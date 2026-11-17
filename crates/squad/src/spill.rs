//! Working memory of the executor (`work_mem`) and temporary files.
//!
//! Operators that buffer rows — sorting, hash aggregation, hash DISTINCT,
//! hash joins, set operations, window functions and materialized CTEs —
//! estimate the memory their buffers hold and keep it below the
//! statement's `work_mem` (hash tables: twice that, like PostgreSQL's
//! default `hash_mem_multiplier`). What does not fit is written to
//! temporary files in the database's temporary directory, in the row
//! encoding of [`crate::encoding`]. A temporary file is deleted when the
//! value owning it is dropped, which happens when the operator finishes,
//! when the statement fails and when it is cancelled. Files left behind by
//! a crash are removed when the database is opened again.
//!
//! Buffers that cannot be split (a single window partition, the state of
//! one aggregate group) fail with SQLSTATE 53200 when they outgrow the
//! budget, instead of exhausting the process's memory.

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use crate::encoding::{decode_row, encode_row_into};
use crate::exec::ExecCx;
use crate::expr::Row;
use crate::types::{compare, Value};
use crate::{sqlstate, Error, Result};

/// `work_mem` when neither the configuration nor the session sets it:
/// 4 MB, as in PostgreSQL.
pub const DEFAULT_WORK_MEM: usize = 4 << 20;
/// Smallest `work_mem` accepted (64 kB), as in PostgreSQL.
pub const MIN_WORK_MEM: usize = 64 << 10;
/// Largest `work_mem` accepted (2147483647 kB), as in PostgreSQL.
pub const MAX_WORK_MEM: usize = (i32::MAX as usize).saturating_mul(1024);

/// File names of temporary files start with this.
pub(crate) const PREFIX: &str = "squad_tmp_";
/// Buffer of a temporary file being written or read.
const IO_BUF: usize = 16 << 10;
/// Partitions a hash operator splits its input into when it spills.
pub(crate) const PARTITIONS: usize = 16;
/// How often a partition may be split again (16^6 partitions in all).
pub(crate) const MAX_LEVEL: u32 = 6;
/// Bookkeeping per buffered entry beyond the values themselves (vector
/// slots, hash table entries).
pub(crate) const ENTRY_OVERHEAD: usize = 48;

// ----------------------------------------------------------- size estimates

/// Estimated bytes a value occupies in memory, including its heap data.
pub(crate) fn value_size(v: &Value) -> usize {
    std::mem::size_of::<Value>() + heap_size(v)
}

fn heap_size(v: &Value) -> usize {
    match v {
        Value::Text(s) | Value::Json(s) | Value::Enum(_, s) => s.capacity(),
        Value::Bytea(b) => b.capacity(),
        Value::Array(items) | Value::Vector(items) | Value::Record(items) => {
            items.capacity() * std::mem::size_of::<Value>() + items.iter().map(heap_size).sum::<usize>()
        }
        Value::Jsonb(j) => std::mem::size_of::<serde_json::Value>() + json_size(j),
        _ => 0,
    }
}

pub(crate) fn json_size(j: &serde_json::Value) -> usize {
    use serde_json::Value as J;
    match j {
        J::String(s) => s.capacity(),
        J::Array(items) => items.capacity() * std::mem::size_of::<J>() + items.iter().map(json_size).sum::<usize>(),
        J::Object(map) => {
            map.iter().map(|(k, v)| k.capacity() + std::mem::size_of::<J>() + ENTRY_OVERHEAD + json_size(v)).sum()
        }
        _ => 0,
    }
}

/// Estimated bytes a row occupies in memory.
pub(crate) fn row_size(row: &[Value]) -> usize {
    std::mem::size_of::<Row>() + row.iter().map(value_size).sum::<usize>()
}

/// `work_mem` as `SHOW` prints it: in the largest unit that divides it.
pub fn format_memory(bytes: usize) -> String {
    let kb = bytes / 1024;
    if kb > 0 && kb.is_multiple_of(1 << 30) {
        format!("{}TB", kb >> 30)
    } else if kb > 0 && kb.is_multiple_of(1 << 20) {
        format!("{}GB", kb >> 20)
    } else if kb > 0 && kb.is_multiple_of(1 << 10) {
        format!("{}MB", kb >> 10)
    } else {
        format!("{kb}kB")
    }
}

/// Parses a `work_mem` value as PostgreSQL does: a number with an
/// optional unit `B`, `kB`, `MB`, `GB` or `TB`; a bare number is kB.
pub(crate) fn parse_memory(name: &str, value: &str) -> Result<usize> {
    let invalid = || {
        Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("invalid value for parameter \"{name}\": \"{value}\""))
            .with_hint("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".")
    };
    let v = value.trim();
    let split = v.find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+')).unwrap_or(v.len());
    let (num, unit) = v.split_at(split);
    let n: f64 = num.parse().map_err(|_| invalid())?;
    let factor = match unit.trim() {
        "B" => 1.0 / 1024.0,
        "" | "kB" => 1.0,
        "MB" => 1024.0,
        "GB" => 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(invalid()),
    };
    let kb = (n * factor).round();
    let (min, max) = ((MIN_WORK_MEM / 1024) as f64, i32::MAX as f64);
    if !(min..=max).contains(&kb) {
        return Err(Error::new(
            sqlstate::INVALID_PARAMETER_VALUE,
            format!(
                "{} kB is outside the valid range for parameter \"{name}\" ({} .. {})",
                kb as i64, min as i64, max as i64
            ),
        ));
    }
    Ok(kb as usize * 1024)
}

/// Parses a `work_mem` value as `SET work_mem` accepts it (`'64MB'`,
/// `'512kB'`, a bare number of kB) into bytes.
pub fn parse_work_mem(value: &str) -> Result<usize> {
    parse_memory("work_mem", value)
}

/// The error of an operator that cannot keep a buffer within its budget.
pub(crate) fn out_of_memory(what: &str, work_mem: usize) -> Error {
    Error::new(
        sqlstate::OUT_OF_MEMORY,
        format!("out of memory: {what} does not fit into work_mem ({})", format_memory(work_mem)),
    )
    .with_hint("Increase work_mem, for example with SET work_mem = '256MB'.")
}

fn io_error(e: io::Error, what: &str) -> Error {
    let code = if e.kind() == io::ErrorKind::StorageFull { sqlstate::DISK_FULL } else { sqlstate::IO_ERROR };
    Error::new(code, format!("could not {what} temporary file: {e}"))
}

// ------------------------------------------------------------ temp space

/// The directory temporary files of one database go to.
pub struct TempSpace {
    dir: PathBuf,
    /// A directory of its own under the system's temporary directory,
    /// removed when the database closes.
    private: bool,
    created: Mutex<bool>,
    next: AtomicU64,
}

impl TempSpace {
    /// A new directory under the system's temporary directory, created
    /// when the first file is needed (for databases without a directory).
    pub(crate) fn private() -> TempSpace {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("squad-tmp-{}-{n}", std::process::id()));
        TempSpace { dir, private: true, created: Mutex::new(false), next: AtomicU64::new(0) }
    }

    /// Files in `dir`, which belongs to one open database. Files a previous
    /// process left there are removed.
    pub(crate) fn in_dir(dir: PathBuf) -> Result<TempSpace> {
        let space = TempSpace { dir, private: false, created: Mutex::new(false), next: AtomicU64::new(0) };
        space.remove_leftovers().map_err(|e| io_error(e, "remove old"))?;
        Ok(space)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn remove_leftovers(&self) -> io::Result<()> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with(PREFIX) {
                match fs::remove_file(entry.path()) {
                    Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Creates a new temporary file.
    pub(crate) fn create(&self) -> Result<SpillWriter> {
        {
            let mut created = self.created.lock().unwrap_or_else(|e| e.into_inner());
            if !*created {
                fs::create_dir_all(&self.dir).map_err(|e| io_error(e, "create directory for"))?;
                *created = true;
            }
        }
        let n = self.next.fetch_add(1, AtomicOrdering::Relaxed);
        let path = self.dir.join(format!("{PREFIX}{}_{n}", std::process::id()));
        let file = OpenOptions::new().write(true).create_new(true).open(&path).map_err(|e| io_error(e, "create"))?;
        Ok(SpillWriter {
            file: TempFile { path },
            out: BufWriter::with_capacity(IO_BUF, file),
            buf: Vec::new(),
            rows: 0,
            bytes: 0,
        })
    }
}

impl Drop for TempSpace {
    fn drop(&mut self) {
        if self.private && *self.created.get_mut().unwrap_or_else(|e| e.into_inner()) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

/// A temporary file, deleted when dropped.
struct TempFile {
    path: PathBuf,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Rows being written to a temporary file.
pub(crate) struct SpillWriter {
    file: TempFile,
    out: BufWriter<File>,
    buf: Vec<u8>,
    rows: u64,
    bytes: u64,
}

impl SpillWriter {
    pub(crate) fn write(&mut self, row: &[Value]) -> Result<()> {
        self.buf.clear();
        encode_row_into(&mut self.buf, row);
        let len = u32::try_from(self.buf.len())
            .map_err(|_| Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "row too large for a temporary file"))?;
        self.out.write_all(&len.to_le_bytes()).map_err(|e| io_error(e, "write to"))?;
        self.out.write_all(&self.buf).map_err(|e| io_error(e, "write to"))?;
        self.rows += 1;
        self.bytes += 4 + self.buf.len() as u64;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Rc<SpillFile>> {
        self.out.flush().map_err(|e| io_error(e, "write to"))?;
        Ok(Rc::new(SpillFile { file: self.file, rows: self.rows, bytes: self.bytes }))
    }
}

/// A finished temporary file of rows. It is deleted when the last
/// reference (including readers) goes away.
pub(crate) struct SpillFile {
    file: TempFile,
    rows: u64,
    bytes: u64,
}

impl SpillFile {
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn reader(self: &Rc<Self>) -> Result<SpillReader> {
        let file = File::open(&self.file.path).map_err(|e| io_error(e, "open"))?;
        Ok(SpillReader {
            input: BufReader::with_capacity(IO_BUF, file),
            remaining: self.rows,
            buf: Vec::new(),
            _file: self.clone(),
        })
    }
}

/// Reads the rows of a temporary file in the order they were written.
pub(crate) struct SpillReader {
    input: BufReader<File>,
    remaining: u64,
    buf: Vec<u8>,
    _file: Rc<SpillFile>,
}

impl SpillReader {
    pub(crate) fn next_row(&mut self) -> Result<Option<Row>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut len = [0u8; 4];
        self.input.read_exact(&mut len).map_err(|e| io_error(e, "read from"))?;
        self.buf.resize(u32::from_le_bytes(len) as usize, 0);
        self.input.read_exact(&mut self.buf).map_err(|e| io_error(e, "read from"))?;
        self.remaining -= 1;
        decode_row(&self.buf).map(Some)
    }
}

// ------------------------------------------------------------ partitions

/// The partition (`0..PARTITIONS`) of a hash key at a partitioning level;
/// each level splits by different hash bits.
pub(crate) fn partition_of(key: &[u8], level: u32) -> usize {
    let mut h = DefaultHasher::new();
    level.hash(&mut h);
    key.hash(&mut h);
    (h.finish() % PARTITIONS as u64) as usize
}

/// Temporary files for the partitions of a spilling hash operator,
/// created when the first row of a partition arrives.
pub(crate) struct Partitions {
    writers: Vec<Option<SpillWriter>>,
    space: Arc<TempSpace>,
}

impl Partitions {
    pub(crate) fn new(cx: &ExecCx) -> Partitions {
        Partitions { writers: (0..PARTITIONS).map(|_| None).collect(), space: cx.temp_space() }
    }

    pub(crate) fn write(&mut self, part: usize, row: &[Value]) -> Result<()> {
        if self.writers[part].is_none() {
            self.writers[part] = Some(self.space.create()?);
        }
        self.writers[part].as_mut().unwrap().write(row)
    }

    /// The files of all partitions, `None` for those without rows.
    pub(crate) fn finish(self) -> Result<Vec<Option<Rc<SpillFile>>>> {
        self.writers.into_iter().map(|w| w.map(SpillWriter::finish).transpose()).collect()
    }
}

// --------------------------------------------------------------- sorting

/// Direction of one sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SortOrder {
    pub desc: bool,
    pub nulls_first: bool,
}

impl SortOrder {
    pub(crate) const ASC: SortOrder = SortOrder { desc: false, nulls_first: false };
}

/// Compares two values under a sort direction.
pub(crate) fn compare_ordered(x: &Value, y: &Value, o: SortOrder) -> Ordering {
    match (x.is_null(), y.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if o.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if o.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let ord = compare(x, y);
            if o.desc {
                ord.reverse()
            } else {
                ord
            }
        }
    }
}

/// Compares two rows of key values.
pub(crate) fn compare_rows(a: &[Value], b: &[Value], orders: &[SortOrder]) -> Ordering {
    for ((x, y), o) in a.iter().zip(b).zip(orders) {
        let ord = compare_ordered(x, y, *o);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// What a sort did, for `EXPLAIN ANALYZE`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SortStats {
    pub peak_memory: usize,
    pub disk: u64,
    pub runs: usize,
}

impl SortStats {
    pub(crate) fn describe(&self) -> String {
        if self.runs == 0 {
            format!("Sort Method: quicksort  Memory: {}kB", self.peak_memory.div_ceil(1024))
        } else {
            format!("Sort Method: external merge  Disk: {}kB", self.disk.div_ceil(1024))
        }
    }
}

/// An external merge sort of `(keys, row)` pairs. Pairs are sorted in
/// memory while they fit into `work_mem`; beyond that, each sorted run is
/// written to a temporary file and the runs are merged. The sort is
/// stable: pairs with equal keys keep the order they were pushed in.
pub(crate) struct Sorter {
    orders: Rc<[SortOrder]>,
    budget: usize,
    space: Arc<TempSpace>,
    mem: Vec<(Vec<Value>, Row)>,
    used: usize,
    runs: Vec<Rc<SpillFile>>,
    stats: SortStats,
}

impl Sorter {
    pub(crate) fn new(orders: Vec<SortOrder>, cx: &ExecCx) -> Sorter {
        Sorter {
            orders: orders.into(),
            budget: cx.work_mem(),
            space: cx.temp_space(),
            mem: Vec::new(),
            used: 0,
            runs: Vec::new(),
            stats: SortStats::default(),
        }
    }

    pub(crate) fn push(&mut self, keys: Vec<Value>, row: Row, cx: &mut ExecCx) -> Result<()> {
        self.used += row_size(&keys) + row_size(&row) + ENTRY_OVERHEAD;
        self.mem.push((keys, row));
        self.stats.peak_memory = self.stats.peak_memory.max(self.used);
        if self.used > self.budget {
            self.spill_run(cx)?;
        }
        Ok(())
    }

    fn sort_mem(&mut self) {
        let orders = self.orders.clone();
        self.mem.sort_by(|a, b| compare_rows(&a.0, &b.0, &orders));
    }

    fn spill_run(&mut self, cx: &mut ExecCx) -> Result<()> {
        self.sort_mem();
        let mut w = self.space.create()?;
        for (mut keys, row) in self.mem.drain(..) {
            cx.check_interrupts()?;
            keys.extend(row);
            w.write(&keys)?;
        }
        let run = w.finish()?;
        self.stats.disk += run.bytes();
        self.stats.runs += 1;
        self.runs.push(run);
        self.used = 0;
        // Keep the vector's capacity bounded by what was accounted for.
        self.mem.shrink_to(self.mem.capacity().min(1 << 16));
        Ok(())
    }

    /// Sorts what was pushed and returns the pairs in order.
    pub(crate) fn finish(mut self, cx: &mut ExecCx) -> Result<(Sorted, SortStats)> {
        if self.runs.is_empty() {
            self.sort_mem();
            return Ok((Sorted::Mem(std::mem::take(&mut self.mem).into_iter()), self.stats));
        }
        if !self.mem.is_empty() {
            self.spill_run(cx)?;
        }
        self.mem = Vec::new();
        let width = self.orders.len();
        // Each run being merged needs a read buffer and its current row.
        // At least 8, so that a tiny budget does not mean many passes.
        let fan_in = (self.budget / (2 * IO_BUF)).clamp(8, 256);
        let mut runs = std::mem::take(&mut self.runs);
        while runs.len() > fan_in {
            let mut merged = Vec::with_capacity(runs.len().div_ceil(fan_in));
            for group in runs.chunks(fan_in) {
                if group.len() == 1 {
                    merged.push(group[0].clone());
                    continue;
                }
                let mut m = Merger::new(group, width, self.orders.clone())?;
                let mut w = self.space.create()?;
                while let Some((mut keys, row)) = m.next()? {
                    cx.check_interrupts()?;
                    keys.extend(row);
                    w.write(&keys)?;
                }
                merged.push(w.finish()?);
            }
            runs = merged;
        }
        let merger = Merger::new(&runs, width, self.orders.clone())?;
        Ok((Sorted::Merge(merger), self.stats))
    }
}

/// The output of a [`Sorter`].
pub(crate) enum Sorted {
    Mem(std::vec::IntoIter<(Vec<Value>, Row)>),
    Merge(Merger),
}

impl Sorted {
    pub(crate) fn next(&mut self) -> Result<Option<(Vec<Value>, Row)>> {
        match self {
            Sorted::Mem(it) => Ok(it.next()),
            Sorted::Merge(m) => m.next(),
        }
    }
}

struct Head {
    keys: Vec<Value>,
    row: Row,
    run: usize,
    orders: Rc<[SortOrder]>,
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Head {
    /// Reversed, so that the max-heap yields the smallest pair first; ties
    /// go to the earlier run, which keeps the sort stable.
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rows(&other.keys, &self.keys, &self.orders).then_with(|| other.run.cmp(&self.run))
    }
}

/// k-way merge of sorted runs.
pub(crate) struct Merger {
    readers: Vec<SpillReader>,
    heap: BinaryHeap<Head>,
    width: usize,
    orders: Rc<[SortOrder]>,
}

impl Merger {
    fn new(runs: &[Rc<SpillFile>], width: usize, orders: Rc<[SortOrder]>) -> Result<Merger> {
        let mut m = Merger {
            readers: runs.iter().map(|r| r.reader()).collect::<Result<_>>()?,
            heap: BinaryHeap::with_capacity(runs.len()),
            width,
            orders,
        };
        for run in 0..m.readers.len() {
            m.refill(run)?;
        }
        Ok(m)
    }

    fn refill(&mut self, run: usize) -> Result<()> {
        if let Some(mut keys) = self.readers[run].next_row()? {
            let row = keys.split_off(self.width);
            self.heap.push(Head { keys, row, run, orders: self.orders.clone() });
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<(Vec<Value>, Row)>> {
        let Some(head) = self.heap.pop() else { return Ok(None) };
        self.refill(head.run)?;
        Ok(Some((head.keys, head.row)))
    }
}

// ------------------------------------------------------------- row store

/// Rows kept for reading, possibly several times: in memory while they fit
/// into `work_mem`, otherwise in a temporary file.
pub(crate) struct RowStore {
    mem: Vec<Row>,
    used: usize,
    budget: usize,
    file: Option<SpillWriter>,
}

impl RowStore {
    pub(crate) fn new(cx: &ExecCx) -> RowStore {
        RowStore { mem: Vec::new(), used: 0, budget: cx.work_mem(), file: None }
    }

    pub(crate) fn push(&mut self, row: Row, cx: &ExecCx) -> Result<()> {
        if let Some(f) = &mut self.file {
            return f.write(&row);
        }
        self.used += row_size(&row) + std::mem::size_of::<Row>();
        self.mem.push(row);
        if self.used > self.budget {
            let mut w = cx.temp_space().create()?;
            for r in self.mem.drain(..) {
                w.write(&r)?;
            }
            self.mem = Vec::new();
            self.file = Some(w);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Rc<Stored>> {
        Ok(Rc::new(match self.file {
            Some(w) => Stored::Disk(w.finish()?),
            None => Stored::Mem(self.mem),
        }))
    }
}

/// Rows collected by a [`RowStore`].
pub(crate) enum Stored {
    Mem(Vec<Row>),
    Disk(Rc<SpillFile>),
}

impl Stored {
    pub(crate) fn empty() -> Rc<Stored> {
        Rc::new(Stored::Mem(Vec::new()))
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            Stored::Mem(rows) => rows.len() as u64,
            Stored::Disk(f) => f.rows(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn iter(self: &Rc<Self>) -> Result<StoredIter> {
        Ok(match &**self {
            Stored::Mem(_) => StoredIter::Mem(self.clone(), 0),
            Stored::Disk(f) => StoredIter::Disk(f.reader()?),
        })
    }
}

/// A reader of [`Stored`] rows.
pub(crate) enum StoredIter {
    Mem(Rc<Stored>, usize),
    Disk(SpillReader),
}

impl StoredIter {
    pub(crate) fn next_row(&mut self) -> Result<Option<Row>> {
        match self {
            StoredIter::Mem(rows, pos) => {
                let Stored::Mem(rows) = &**rows else { unreachable!() };
                *pos += 1;
                Ok(rows.get(*pos - 1).cloned())
            }
            StoredIter::Disk(r) => r.next_row(),
        }
    }

    /// Like [`next_row`](StoredIter::next_row), without copying rows held
    /// in memory.
    pub(crate) fn next_cow(&mut self) -> Result<Option<std::borrow::Cow<'_, [Value]>>> {
        use std::borrow::Cow;
        match self {
            StoredIter::Mem(rows, pos) => {
                let Stored::Mem(rows) = &**rows else { unreachable!() };
                *pos += 1;
                Ok(rows.get(*pos - 1).map(|r| Cow::Borrowed(r.as_slice())))
            }
            StoredIter::Disk(r) => Ok(r.next_row()?.map(Cow::Owned)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_values() {
        assert_eq!(parse_memory("work_mem", "4MB").unwrap(), 4 << 20);
        assert_eq!(parse_memory("work_mem", "64").unwrap(), 64 << 10);
        assert_eq!(parse_memory("work_mem", " 1GB ").unwrap(), 1 << 30);
        assert_eq!(parse_memory("work_mem", "65536B").unwrap(), 64 << 10);
        assert!(parse_memory("work_mem", "63kB").is_err());
        assert!(parse_memory("work_mem", "4 mb").is_err());
        assert!(parse_memory("work_mem", "lots").is_err());
        assert_eq!(format_memory(4 << 20), "4MB");
        assert_eq!(format_memory(65 << 10), "65kB");
        assert_eq!(format_memory(1 << 30), "1GB");
        assert_eq!(format_memory(1025 << 10), "1025kB");
    }

    #[test]
    fn partitions_spread_keys() {
        let mut seen = [0usize; PARTITIONS];
        for i in 0..1600u32 {
            seen[partition_of(&i.to_be_bytes(), 0)] += 1;
        }
        assert!(seen.iter().all(|&n| n > 50), "{seen:?}");
        // Another level splits one partition again.
        let first: Vec<u32> = (0..1600u32).filter(|i| partition_of(&i.to_be_bytes(), 0) == 0).collect();
        let sub: std::collections::HashSet<usize> = first.iter().map(|i| partition_of(&i.to_be_bytes(), 1)).collect();
        assert!(sub.len() > 8);
    }
}
