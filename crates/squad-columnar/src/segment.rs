//! Immutable column segments: writer, reader, statistics, bloom filters.
//!
//! ```text
//! "SQCOL001"
//! chunk*                      column chunks and bloom filters, any order
//! footer                      schema, sort key, row group directory, stats
//! footer_len:u32 footer_crc:u32
//! "SQCOL001"
//!
//! column chunk = crc:u32 compression:u8 raw_len:u32 data
//!                (crc covers everything after it; compression 0 = none,
//!                 1 = LZ4 block; data decodes to an encoded column, see
//!                 `encoding`)
//! bloom chunk  = crc:u32 hashes:u8 bits
//! ```
//!
//! A reader fetches the tail of the file once to get the footer, and then
//! only the chunks of the columns and row groups a query needs.

use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

use crate::encoding::{decode_column, encode_column};
use crate::json;
use crate::types::{cmp_datum, cmp_f64, Batch, Column, ColumnBuilder, ColumnData, ColumnType, Datum, Field, Schema};
use crate::util::{corrupt, hash_bytes, hash_u64, Cur, FastSet, Put};
use crate::{Error, Result};

const MAGIC: &[u8; 8] = b"SQCOL001";
const VERSION: u32 = 1;
/// Longest byte string stored verbatim as a min/max bound.
const BOUND_LEN: usize = 64;
/// How much of the file's end is fetched to find the footer.
const TAIL_READ: u64 = 64 << 10;

/// Random-access byte source of a segment: a file, a buffer in memory or
/// an object in an object store.
pub trait ChunkSource: Send + Sync {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns exactly `len` bytes starting at `offset`.
    fn read(&self, offset: u64, len: u64) -> io::Result<Vec<u8>>;
    /// Announces ranges (offset, length) that are about to be read, so
    /// that a remote source can fetch them together. Optional.
    fn prefetch(&self, _ranges: &[(u64, u64)]) {}
}

impl ChunkSource for Vec<u8> {
    fn len(&self) -> u64 {
        <[u8]>::len(self) as u64
    }
    fn read(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let end = offset.checked_add(len).filter(|e| *e <= <[u8]>::len(self) as u64);
        match end {
            Some(end) => Ok(self[offset as usize..end as usize].to_vec()),
            None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond the end of the segment")),
        }
    }
}

/// A segment in a local file, read with positioned reads.
pub struct FileSource {
    file: std::fs::File,
    len: u64,
}

impl FileSource {
    pub fn open(path: &std::path::Path) -> io::Result<FileSource> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        Ok(FileSource { file, len })
    }
}

impl ChunkSource for FileSource {
    fn len(&self) -> u64 {
        self.len
    }
    fn read(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset.checked_add(len).is_none_or(|e| e > self.len) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond the end of the segment"));
        }
        let mut buf = vec![0u8; len as usize];
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::read_exact_at(&self.file, &mut buf, offset)?;
        }
        #[cfg(windows)]
        {
            let mut done = 0;
            while done < buf.len() {
                let n = std::os::windows::fs::FileExt::seek_read(&self.file, &mut buf[done..], offset + done as u64)?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
                }
                done += n;
            }
        }
        Ok(buf)
    }
}

impl ChunkSource for Arc<Vec<u8>> {
    fn len(&self) -> u64 {
        ChunkSource::len(&**self)
    }
    fn read(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        (**self).read(offset, len)
    }
}

/// Summary of the values of a column in a row group or a segment.
///
/// `min` and `max` are bounds: every non-null value lies between them
/// (long byte strings are shortened). `Datum::Null` means no bound is
/// known, or that there are no non-null values.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    pub min: Datum,
    pub max: Datum,
    pub rows: u64,
    pub null_count: u64,
    /// Number of distinct non-null values (per row group exact; per
    /// segment the largest row group value, a lower bound).
    pub distinct: u64,
}

impl ColumnStats {
    pub fn all_null(&self) -> bool {
        self.null_count == self.rows
    }

    /// Combines the statistics of two disjoint sets of rows.
    pub fn merge(&self, other: &ColumnStats) -> ColumnStats {
        let bound = |a: &Datum, b: &Datum, want: Ordering| -> Datum {
            match (self.all_null(), other.all_null()) {
                (true, _) => b.clone(),
                (_, true) => a.clone(),
                _ if a.is_null() || b.is_null() => Datum::Null,
                _ if cmp_datum(a, b) == want => a.clone(),
                _ => b.clone(),
            }
        };
        ColumnStats {
            min: bound(&self.min, &other.min, Ordering::Less),
            max: bound(&self.max, &other.max, Ordering::Greater),
            rows: self.rows + other.rows,
            null_count: self.null_count + other.null_count,
            distinct: self.distinct.max(other.distinct),
        }
    }
}

/// A bloom filter over the hashes of a column's values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    bits: Vec<u8>,
    hashes: u8,
}

impl Bloom {
    fn with_capacity(distinct: usize) -> Bloom {
        // Ten bits per value and seven probes: about 1 % false positives.
        let bytes = (distinct * 10).div_ceil(8).clamp(8, 1 << 20);
        Bloom { bits: vec![0; bytes], hashes: 7 }
    }

    fn insert(&mut self, hash: u64) {
        let n = self.bits.len() as u64 * 8;
        let (h1, h2) = (hash, (hash >> 32) | 1);
        for i in 0..self.hashes as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % n;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// False means the value is certainly absent.
    pub fn may_contain_hash(&self, hash: u64) -> bool {
        let n = self.bits.len() as u64 * 8;
        if n == 0 {
            return true;
        }
        let (h1, h2) = (hash, (hash >> 32) | 1);
        (0..self.hashes as u64).all(|i| {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % n;
            self.bits[(bit / 8) as usize] >> (bit % 8) & 1 == 1
        })
    }

    /// False means no row holds `value`. Values of other types than the
    /// filter was built for may match.
    pub fn may_contain(&self, value: &Datum) -> bool {
        match value {
            Datum::Int(v) => self.may_contain_hash(hash_u64(*v as u64)),
            Datum::Bytes(b) => self.may_contain_hash(hash_bytes(b)),
            _ => true,
        }
    }
}

fn upper_bound(v: &[u8]) -> Datum {
    if v.len() <= BOUND_LEN {
        return Datum::Bytes(v.to_vec());
    }
    // Shorten and round up: the smallest string above every string with
    // this prefix.
    let mut b = v[..BOUND_LEN].to_vec();
    while let Some(last) = b.pop() {
        if last != 0xFF {
            b.push(last + 1);
            return Datum::Bytes(b);
        }
    }
    Datum::Null
}

/// Statistics and, for integer and byte columns, a bloom filter. For
/// JSON columns the bloom filter holds the documents' top-level keys and
/// key/value pairs (see [`crate::json`]).
pub(crate) fn compute_stats(col: &Column, ty: ColumnType) -> (ColumnStats, Option<Bloom>) {
    let rows = col.len();
    let valid = |i: usize| !col.is_null(i);
    let mut stats = ColumnStats {
        min: Datum::Null,
        max: Datum::Null,
        rows: rows as u64,
        null_count: col.null_count() as u64,
        distinct: 0,
    };
    let mut bloom = None;
    match &col.data {
        ColumnData::Bool(v) => {
            let (mut any_t, mut any_f) = (false, false);
            for (i, b) in v.iter().enumerate() {
                if valid(i) {
                    any_t |= *b;
                    any_f |= !*b;
                }
            }
            if any_t || any_f {
                stats.min = Datum::Bool(!any_f);
                stats.max = Datum::Bool(any_t);
            }
            stats.distinct = any_t as u64 + any_f as u64;
        }
        ColumnData::Int64(v) => {
            let mut set: FastSet<i64> = FastSet::default();
            let (mut min, mut max) = (i64::MAX, i64::MIN);
            for (i, x) in v.iter().enumerate() {
                if valid(i) {
                    min = min.min(*x);
                    max = max.max(*x);
                    set.insert(*x);
                }
            }
            if !set.is_empty() {
                stats.min = Datum::Int(min);
                stats.max = Datum::Int(max);
                let mut b = Bloom::with_capacity(set.len());
                set.iter().for_each(|x| b.insert(hash_u64(*x as u64)));
                bloom = Some(b);
            }
            stats.distinct = set.len() as u64;
        }
        ColumnData::Float64(v) => {
            let mut set: FastSet<u64> = FastSet::default();
            let mut bounds: Option<(f64, f64)> = None;
            for (i, x) in v.iter().enumerate() {
                if valid(i) {
                    set.insert(x.to_bits());
                    bounds = Some(match bounds {
                        None => (*x, *x),
                        Some((lo, hi)) => (
                            if cmp_f64(*x, lo) == Ordering::Less { *x } else { lo },
                            if cmp_f64(*x, hi) == Ordering::Greater { *x } else { hi },
                        ),
                    });
                }
            }
            if let Some((lo, hi)) = bounds {
                stats.min = Datum::Float(lo);
                stats.max = Datum::Float(hi);
            }
            stats.distinct = set.len() as u64;
        }
        ColumnData::Decimal(v) => {
            let mut set: FastSet<i128> = FastSet::default();
            let (mut min, mut max) = (i128::MAX, i128::MIN);
            for (i, x) in v.iter().enumerate() {
                if valid(i) {
                    min = min.min(*x);
                    max = max.max(*x);
                    set.insert(*x);
                }
            }
            if !set.is_empty() {
                stats.min = Datum::Decimal(min);
                stats.max = Datum::Decimal(max);
            }
            stats.distinct = set.len() as u64;
        }
        ColumnData::Bytes(b) => {
            let mut set: FastSet<u64> = FastSet::default();
            let (mut min, mut max): (Option<&[u8]>, Option<&[u8]>) = (None, None);
            for i in 0..rows {
                if valid(i) {
                    let x = b.get(i);
                    if min.is_none_or(|m| x < m) {
                        min = Some(x);
                    }
                    if max.is_none_or(|m| x > m) {
                        max = Some(x);
                    }
                    set.insert(hash_bytes(x));
                }
            }
            if let (Some(lo), Some(hi)) = (min, max) {
                stats.min = Datum::Bytes(lo[..lo.len().min(BOUND_LEN)].to_vec());
                stats.max = upper_bound(hi);
                if ty == ColumnType::Json {
                    let mut entries: FastSet<u64> = FastSet::default();
                    let docs: FastSet<&[u8]> = (0..rows).filter(|i| valid(*i)).map(|i| b.get(i)).collect();
                    for doc in docs {
                        json::for_each_field(doc, |k, v| {
                            entries.insert(json::json_key_hash(k));
                            if let Some(v) = v {
                                entries.insert(json::json_pair_hash(k, v));
                            }
                        });
                    }
                    let mut f = Bloom::with_capacity(entries.len());
                    entries.iter().for_each(|h| f.insert(*h));
                    bloom = Some(f);
                } else {
                    let mut f = Bloom::with_capacity(set.len());
                    set.iter().for_each(|h| f.insert(*h));
                    bloom = Some(f);
                }
            }
            stats.distinct = set.len() as u64;
        }
    }
    (stats, bloom)
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    /// Rows per row group.
    pub row_group_rows: usize,
    /// Columns to sort the segment by (ascending, NULLs last). Zone maps
    /// prune well only on sorted or clustered columns.
    pub sort_key: Vec<usize>,
    /// Apply LZ4 to chunks where it saves at least a tenth.
    pub compress: bool,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions { row_group_rows: 65_536, sort_key: Vec::new(), compress: true }
    }
}

/// Collects batches and writes them as one segment.
///
/// Batches are kept as they are given (shared, not copied). `finish`
/// sorts by the sort key without concatenating them first: it computes
/// the order over all batches and then gathers one row group at a time,
/// so it needs the batches plus about two row groups of memory.
pub struct SegmentWriter {
    schema: Arc<Schema>,
    options: WriterOptions,
    batches: Vec<Arc<Batch>>,
}

impl SegmentWriter {
    pub fn new(schema: Arc<Schema>, options: WriterOptions) -> SegmentWriter {
        SegmentWriter { schema, options, batches: Vec::new() }
    }

    fn check(&self, batch: &Batch) -> Result<()> {
        if batch.schema.columns.len() != self.schema.columns.len()
            || batch.schema.columns.iter().zip(&self.schema.columns).any(|(a, b)| a.ty != b.ty)
        {
            return Err(Error::Type("batch schema does not match the segment schema".into()));
        }
        Ok(())
    }

    /// Adds a copy of `batch`.
    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        self.check(batch)?;
        self.batches.push(Arc::new(batch.clone()));
        Ok(())
    }

    /// Adds a batch without copying it.
    pub fn write_shared(&mut self, batch: Arc<Batch>) -> Result<()> {
        self.check(&batch)?;
        self.batches.push(batch);
        Ok(())
    }

    /// Rows buffered so far.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Encodes everything written and returns the segment's bytes.
    pub fn finish(self) -> Result<Vec<u8>> {
        let SegmentWriter { schema, options, batches } = self;
        if options.sort_key.iter().any(|c| *c >= schema.len()) {
            return Err(Error::Invalid("sort key names a column that does not exist".into()));
        }
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if rows > u32::MAX as usize {
            return Err(Error::Invalid("a segment holds at most 2^32 - 1 rows".into()));
        }
        let group = options.row_group_rows.max(1);
        let order = sort_order(&batches, &options.sort_key);
        let mut out = SegmentStreamWriter::new(schema.clone(), options)?;
        match order {
            None => {
                for b in &batches {
                    out.write_batch(b)?;
                }
            }
            Some(order) => {
                for part in order.chunks(group) {
                    out.write_batch(&gather(&schema, &batches, part))?;
                }
            }
        }
        out.finish()
    }
}

/// The rows of `batches` (taken in sequence) ordered by `keys`, as
/// (batch, row) pairs; `None` when they already are in that order. Ties
/// keep the sequence order.
fn sort_order(batches: &[Arc<Batch>], keys: &[usize]) -> Option<Vec<(u32, u32)>> {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if keys.is_empty() || rows < 2 {
        return None;
    }
    let ints: Option<Vec<Vec<&Vec<i64>>>> = batches
        .iter()
        .map(|b| {
            keys.iter()
                .map(|&k| match (&b.columns[k].data, &b.columns[k].validity) {
                    (ColumnData::Int64(v), None) => Some(v),
                    _ => None,
                })
                .collect()
        })
        .collect();
    let order: Vec<(u32, u32)> = match ints {
        // The common case — integer ids and timestamps — sorts plain
        // tuples instead of going through a comparator.
        Some(ints) if keys.len() <= 2 => {
            let mut tuples: Vec<(i64, i64, u32, u32)> = Vec::with_capacity(rows);
            for (bi, cols) in ints.iter().enumerate() {
                let n = batches[bi].num_rows();
                for r in 0..n {
                    tuples.push((cols[0][r], cols.get(1).map_or(0, |v| v[r]), bi as u32, r as u32));
                }
            }
            tuples.sort_unstable();
            tuples.into_iter().map(|t| (t.2, t.3)).collect()
        }
        _ => {
            let mut order: Vec<(u32, u32)> = Vec::with_capacity(rows);
            for (bi, b) in batches.iter().enumerate() {
                order.extend((0..b.num_rows() as u32).map(|r| (bi as u32, r)));
            }
            order.sort_unstable_by(|&(ba, ra), &(bb, rb)| {
                for &k in keys {
                    let o = crate::table::cmp_cells(
                        &batches[ba as usize].columns[k],
                        ra as usize,
                        &batches[bb as usize].columns[k],
                        rb as usize,
                    );
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                (ba, ra).cmp(&(bb, rb))
            });
            order
        }
    };
    if order.windows(2).all(|w| w[0] < w[1]) {
        None
    } else {
        Some(order)
    }
}

/// The rows `part` names, in that order, as one batch.
fn gather(schema: &Arc<Schema>, batches: &[Arc<Batch>], part: &[(u32, u32)]) -> Batch {
    // Rows needed from each batch, in the order they appear in `part`.
    let mut wanted: Vec<Vec<u32>> = vec![Vec::new(); batches.len()];
    let mut slot: Vec<(u32, u32)> = Vec::with_capacity(part.len());
    for &(b, r) in part {
        slot.push((b, wanted[b as usize].len() as u32));
        wanted[b as usize].push(r);
    }
    let mut pieces = Vec::new();
    let mut offset = vec![0u32; batches.len()];
    let mut at = 0u32;
    for (b, rows) in wanted.iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        offset[b] = at;
        at += rows.len() as u32;
        pieces.push(batches[b].take(rows));
    }
    let combined = if pieces.len() == 1 { pieces.pop().unwrap() } else { Batch::concat(schema.clone(), &pieces) };
    drop(pieces);
    let indices: Vec<u32> = slot.iter().map(|&(b, k)| offset[b as usize] + k).collect();
    if indices.windows(2).all(|w| w[0] < w[1]) {
        combined
    } else {
        combined.take(&indices)
    }
}

/// The part of a segment written so far: chunks, and the directory
/// entries of the row groups they belong to.
struct SegmentBody {
    schema: Arc<Schema>,
    options: WriterOptions,
    out: Out,
    groups: Vec<u8>,
    ngroups: u32,
    rows: u64,
}

/// Where the bytes of a segment being written go.
enum Out {
    Memory(Vec<u8>),
    File { writer: io::BufWriter<std::fs::File>, len: u64 },
}

impl Out {
    fn len(&self) -> u64 {
        match self {
            Out::Memory(v) => v.len() as u64,
            Out::File { len, .. } => *len,
        }
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Out::Memory(v) => v.extend_from_slice(bytes),
            Out::File { writer, len } => {
                io::Write::write_all(writer, bytes).map_err(Error::Io)?;
                *len += bytes.len() as u64;
            }
        }
        Ok(())
    }
}

impl SegmentBody {
    fn with_out(schema: Arc<Schema>, options: WriterOptions, out: Out) -> SegmentBody {
        SegmentBody { schema, options, out, groups: Vec::new(), ngroups: 0, rows: 0 }
    }

    /// Writes one row group.
    fn write_group(&mut self, columns: &[Column]) -> Result<()> {
        let n = columns.first().map_or(0, Column::len);
        if n == 0 {
            return Ok(());
        }
        if self.rows + n as u64 > u32::MAX as u64 {
            return Err(Error::Invalid("a segment holds at most 2^32 - 1 rows".into()));
        }
        let (out, footer) = (&mut self.out, &mut self.groups);
        footer.put_u32(n as u32);
        for (col, field) in columns.iter().zip(&self.schema.columns) {
            let (stats, bloom) = compute_stats(col, field.ty);
            let raw = encode_column(col);
            let (comp, data) = match self.options.compress.then(|| lz4_flex::block::compress(&raw)) {
                Some(c) if c.len() * 10 <= raw.len() * 9 => (1u8, c),
                _ => (0u8, raw.clone()),
            };
            let mut chunk = Vec::with_capacity(data.len() + 5);
            chunk.put_u8(comp);
            chunk.put_u32(raw.len() as u32);
            chunk.extend_from_slice(&data);
            footer.put_u64(out.len());
            footer.put_u32(chunk.len() as u32 + 4);
            out.put(&crc32c::crc32c(&chunk).to_le_bytes())?;
            out.put(&chunk)?;
            match bloom {
                Some(b) => {
                    let mut chunk = vec![b.hashes];
                    chunk.extend_from_slice(&b.bits);
                    footer.put_u64(out.len());
                    footer.put_u32(chunk.len() as u32 + 4);
                    out.put(&crc32c::crc32c(&chunk).to_le_bytes())?;
                    out.put(&chunk)?;
                }
                None => {
                    footer.put_u64(0);
                    footer.put_u32(0);
                }
            }
            footer.put_datum(&stats.min);
            footer.put_datum(&stats.max);
            footer.put_u64(stats.null_count);
            footer.put_u64(stats.distinct);
        }
        self.ngroups += 1;
        self.rows += n as u64;
        Ok(())
    }

    /// Bytes written so far.
    fn len(&self) -> usize {
        self.out.len() as usize + self.groups.len()
    }

    /// Writes the footer; returns where the bytes went and their length.
    fn finish_out(self) -> Result<(Out, u64)> {
        let SegmentBody { schema, options, mut out, groups, ngroups, rows } = self;
        let mut footer = Vec::with_capacity(groups.len() + 64 * schema.len());
        footer.put_u32(VERSION);
        footer.put_u32(schema.len() as u32);
        for f in &schema.columns {
            footer.put_bytes(f.name.as_bytes());
            let (tag, scale) = f.ty.tag();
            footer.put_u8(tag);
            footer.put_u8(scale);
            footer.put_u8(f.nullable as u8);
        }
        footer.put_u32(options.sort_key.len() as u32);
        options.sort_key.iter().for_each(|c| footer.put_u32(*c as u32));
        footer.put_u64(rows);
        footer.put_u32(ngroups);
        footer.extend_from_slice(&groups);
        out.put(&footer)?;
        out.put(&(footer.len() as u32).to_le_bytes())?;
        out.put(&crc32c::crc32c(&footer).to_le_bytes())?;
        out.put(MAGIC)?;
        let len = out.len();
        Ok((out, len))
    }

    fn finish(self) -> Result<Vec<u8>> {
        match self.finish_out()? {
            (Out::Memory(v), _) => Ok(v),
            (Out::File { .. }, _) => Err(Error::Invalid("segment was written to a file".into())),
        }
    }
}

/// Writes a segment from batches that arrive already in their final
/// order, holding at most one row group of decoded values at a time.
/// `options.sort_key` is recorded in the footer but not applied.
pub struct SegmentStreamWriter {
    body: SegmentBody,
    pending: Vec<Column>,
}

impl SegmentStreamWriter {
    pub fn new(schema: Arc<Schema>, options: WriterOptions) -> Result<SegmentStreamWriter> {
        SegmentStreamWriter::with_out(schema, options, Out::Memory(MAGIC.to_vec()))
    }

    /// Writes the segment into `file` (created empty by the caller)
    /// instead of memory; [`finish_file`](SegmentStreamWriter::finish_file)
    /// completes it. Memory then holds one row group and the footer.
    pub fn to_file(schema: Arc<Schema>, options: WriterOptions, file: std::fs::File) -> Result<SegmentStreamWriter> {
        let mut out = Out::File { writer: io::BufWriter::with_capacity(1 << 20, file), len: 0 };
        out.put(MAGIC)?;
        SegmentStreamWriter::with_out(schema, options, out)
    }

    fn with_out(schema: Arc<Schema>, options: WriterOptions, out: Out) -> Result<SegmentStreamWriter> {
        if options.sort_key.iter().any(|c| *c >= schema.len()) {
            return Err(Error::Invalid("sort key names a column that does not exist".into()));
        }
        let pending = schema.columns.iter().map(|f| ColumnBuilder::new(f.ty).finish()).collect();
        Ok(SegmentStreamWriter { body: SegmentBody::with_out(schema, options, out), pending })
    }

    /// Appends rows; full row groups are encoded right away.
    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        let schema = &self.body.schema;
        if batch.columns.len() != schema.len()
            || batch.schema.columns.iter().zip(&schema.columns).any(|(a, b)| a.ty != b.ty)
        {
            return Err(Error::Type("batch schema does not match the segment schema".into()));
        }
        let group = self.body.options.row_group_rows.max(1);
        let mut start = 0;
        let n = batch.num_rows();
        while start < n {
            let have = self.pending.first().map_or(0, Column::len);
            let take = (group - have).min(n - start);
            if have == 0 && take == group && start == 0 && n == group {
                // A whole row group in one piece.
                self.body.write_group(&batch.columns)?;
                return Ok(());
            }
            let indices: Vec<u32> = (start as u32..(start + take) as u32).collect();
            for (p, c) in self.pending.iter_mut().zip(&batch.columns) {
                p.append(&c.take(&indices));
            }
            start += take;
            if have + take == group {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let empty: Vec<Column> = self.body.schema.columns.iter().map(|f| ColumnBuilder::new(f.ty).finish()).collect();
        let full = std::mem::replace(&mut self.pending, empty);
        self.body.write_group(&full)
    }

    /// Rows written so far.
    pub fn num_rows(&self) -> u64 {
        self.body.rows + self.pending.first().map_or(0, Column::len) as u64
    }

    /// Encoded bytes so far (excluding a partial row group).
    pub fn num_bytes(&self) -> usize {
        self.body.len()
    }

    pub fn finish(mut self) -> Result<Vec<u8>> {
        if self.pending.first().is_some_and(|c| !c.is_empty()) {
            self.flush()?;
        }
        self.body.finish()
    }

    /// Completes a segment started with
    /// [`to_file`](SegmentStreamWriter::to_file): writes the footer,
    /// flushes and syncs the file and returns the segment's length.
    pub fn finish_file(mut self) -> Result<u64> {
        if self.pending.first().is_some_and(|c| !c.is_empty()) {
            self.flush()?;
        }
        match self.body.finish_out()? {
            (Out::File { writer, len }, _) => {
                let file = writer.into_inner().map_err(|e| Error::Io(e.into_error()))?;
                file.sync_all().map_err(Error::Io)?;
                Ok(len)
            }
            (Out::Memory(_), _) => Err(Error::Invalid("segment was written to memory".into())),
        }
    }
}

/// Location and statistics of one column chunk.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    offset: u64,
    len: u32,
    bloom_offset: u64,
    bloom_len: u32,
    pub stats: ColumnStats,
    /// Set for a column the segment does not store (see
    /// [`Segment::project`]): every row holds this value.
    constant: Option<Datum>,
}

impl ChunkMeta {
    /// Encoded size of the chunk in bytes.
    pub fn bytes(&self) -> u64 {
        self.len as u64
    }
    pub fn has_bloom(&self) -> bool {
        self.bloom_len > 0
    }
}

#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub rows: u32,
    pub columns: Vec<ChunkMeta>,
}

/// Where a column of a [`Segment::project`]ed segment comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectedColumn {
    /// The segment's own column at this position.
    Stored(usize),
    /// Not stored: every row has this value.
    Constant(Datum),
}

/// An open segment: metadata in memory, data fetched on demand.
pub struct Segment {
    source: Arc<dyn ChunkSource>,
    schema: Arc<Schema>,
    sort_key: Vec<usize>,
    rows: u64,
    groups: Vec<RowGroupMeta>,
    stats: Vec<ColumnStats>,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment").field("rows", &self.rows).field("row_groups", &self.groups.len()).finish()
    }
}

fn io_err(e: io::Error) -> Error {
    Error::Io(e)
}

impl Segment {
    /// Approximate memory the opened segment holds (its schema, row
    /// group directory and statistics), not counting data read through
    /// its source.
    pub fn memory_bytes(&self) -> u64 {
        fn datum(d: &Datum) -> u64 {
            match d {
                Datum::Bytes(b) => b.capacity() as u64,
                _ => 0,
            }
        }
        fn stats(s: &ColumnStats) -> u64 {
            datum(&s.min) + datum(&s.max)
        }
        let mut n = std::mem::size_of::<Segment>() as u64;
        n += self.schema.columns.iter().map(|f| (std::mem::size_of::<Field>() + f.name.len()) as u64).sum::<u64>();
        n += self.stats.iter().map(|s| std::mem::size_of::<ColumnStats>() as u64 + stats(s)).sum::<u64>();
        for g in &self.groups {
            n += std::mem::size_of::<RowGroupMeta>() as u64;
            n += g.columns.iter().map(|c| std::mem::size_of::<ChunkMeta>() as u64 + stats(&c.stats)).sum::<u64>();
        }
        n
    }

    /// Opens a segment by reading and verifying its footer.
    pub fn open(source: Arc<dyn ChunkSource>) -> Result<Segment> {
        let len = source.len();
        if len < 32 {
            return Err(corrupt("segment is too short"));
        }
        let tail_len = len.min(TAIL_READ);
        let tail = source.read(len - tail_len, tail_len).map_err(io_err)?;
        if tail.len() as u64 != tail_len {
            return Err(corrupt("short read of the segment tail"));
        }
        let t = tail.len();
        if &tail[t - 8..] != MAGIC {
            return Err(corrupt("not a squad column segment (bad magic)"));
        }
        let footer_len = u32::from_le_bytes(tail[t - 16..t - 12].try_into().unwrap()) as u64;
        let footer_crc = u32::from_le_bytes(tail[t - 12..t - 8].try_into().unwrap());
        if footer_len > len - 24 {
            return Err(corrupt("footer length exceeds the segment"));
        }
        let footer: Vec<u8> = if footer_len + 16 <= tail_len {
            tail[t - 16 - footer_len as usize..t - 16].to_vec()
        } else {
            let f = source.read(len - 16 - footer_len, footer_len).map_err(io_err)?;
            if f.len() as u64 != footer_len {
                return Err(corrupt("short read of the footer"));
            }
            f
        };
        if crc32c::crc32c(&footer) != footer_crc {
            return Err(corrupt("footer checksum mismatch"));
        }
        let data_end = len - 16 - footer_len;
        let mut cur = Cur::new(&footer);
        if cur.u32()? != VERSION {
            return Err(corrupt("unsupported segment format version"));
        }
        let ncols = cur.u32()? as usize;
        if ncols > footer.len() {
            return Err(corrupt("column count exceeds the footer"));
        }
        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let name = cur.string()?;
            let (tag, scale, nullable) = (cur.u8()?, cur.u8()?, cur.u8()?);
            let ty = ColumnType::from_tag(tag, scale).ok_or_else(|| corrupt("unknown column type"))?;
            columns.push(Field { name, ty, nullable: nullable != 0 });
        }
        let nsort = cur.u32()? as usize;
        if nsort > ncols {
            return Err(corrupt("sort key longer than the schema"));
        }
        let mut sort_key = Vec::with_capacity(nsort);
        for _ in 0..nsort {
            let c = cur.u32()? as usize;
            if c >= ncols {
                return Err(corrupt("sort key column out of range"));
            }
            sort_key.push(c);
        }
        let rows = cur.u64()?;
        let ngroups = cur.u32()? as usize;
        if ngroups > footer.len() {
            return Err(corrupt("row group count exceeds the footer"));
        }
        let mut groups = Vec::with_capacity(ngroups);
        let mut total = 0u64;
        let check = |offset: u64, len: u32| -> Result<()> {
            if len != 0 && (offset < 8 || offset + len as u64 > data_end || len < 4) {
                return Err(corrupt("chunk lies outside the segment"));
            }
            Ok(())
        };
        for _ in 0..ngroups {
            let group_rows = cur.u32()?;
            total += group_rows as u64;
            let mut chunks = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let (offset, len, bloom_offset, bloom_len) = (cur.u64()?, cur.u32()?, cur.u64()?, cur.u32()?);
                check(offset, len)?;
                check(bloom_offset, bloom_len)?;
                if len == 0 {
                    return Err(corrupt("empty column chunk"));
                }
                let (min, max) = (cur.datum()?, cur.datum()?);
                let (null_count, distinct) = (cur.u64()?, cur.u64()?);
                if null_count > group_rows as u64 {
                    return Err(corrupt("null count exceeds the row count"));
                }
                chunks.push(ChunkMeta {
                    offset,
                    len,
                    bloom_offset,
                    bloom_len,
                    stats: ColumnStats { min, max, rows: group_rows as u64, null_count, distinct },
                    constant: None,
                });
            }
            groups.push(RowGroupMeta { rows: group_rows, columns: chunks });
        }
        if total != rows || cur.remaining() != 0 {
            return Err(corrupt("footer is inconsistent"));
        }
        let stats = (0..ncols)
            .map(|c| {
                let mut it = groups.iter().map(|g| &g.columns[c].stats);
                match it.next() {
                    Some(first) => it.fold(first.clone(), |acc, s| acc.merge(s)),
                    None => ColumnStats { min: Datum::Null, max: Datum::Null, rows: 0, null_count: 0, distinct: 0 },
                }
            })
            .collect();
        Ok(Segment { source, schema: Arc::new(Schema::new(columns)), sort_key, rows, groups, stats })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
    pub fn sort_key(&self) -> &[usize] {
        &self.sort_key
    }
    pub fn num_rows(&self) -> u64 {
        self.rows
    }
    pub fn num_row_groups(&self) -> usize {
        self.groups.len()
    }
    pub fn row_group(&self, i: usize) -> &RowGroupMeta {
        &self.groups[i]
    }
    /// Statistics of a column over the whole segment.
    pub fn column_stats(&self, column: usize) -> &ColumnStats {
        &self.stats[column]
    }
    /// Size of the segment in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.source.len()
    }

    fn read_chunk(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let bytes = self.source.read(offset, len as u64).map_err(io_err)?;
        if bytes.len() != len as usize || bytes.len() < 4 {
            return Err(corrupt("short read of a chunk"));
        }
        let crc = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        if crc32c::crc32c(&bytes[4..]) != crc {
            return Err(corrupt("chunk checksum mismatch"));
        }
        Ok(bytes)
    }

    /// Reads and decodes one column of one row group.
    pub fn read_column(&self, row_group: usize, column: usize) -> Result<Column> {
        let group = &self.groups[row_group];
        let meta = &group.columns[column];
        if let Some(value) = &meta.constant {
            return Column::constant(self.schema.columns[column].ty, value, group.rows as usize);
        }
        let bytes = self.read_chunk(meta.offset, meta.len)?;
        let mut cur = Cur::new(&bytes[4..]);
        let comp = cur.u8()?;
        let raw_len = cur.u32()? as usize;
        let data = cur.rest();
        let raw;
        let payload: &[u8] = match comp {
            0 => data,
            1 => {
                // Guard against absurd sizes from a damaged header that
                // happens to pass the checksum of a crafted file.
                if raw_len > (group.rows as usize).saturating_mul(64).max(1 << 20).saturating_add(data.len() * 256) {
                    return Err(corrupt("implausible uncompressed chunk size"));
                }
                raw = lz4_flex::block::decompress(data, raw_len).map_err(|_| corrupt("chunk does not decompress"))?;
                &raw
            }
            _ => return Err(corrupt("unknown chunk compression")),
        };
        if payload.len() != raw_len {
            return Err(corrupt("chunk length mismatch"));
        }
        let col = decode_column(payload, self.schema.columns[column].ty, group.rows as usize)?;
        if col.len() != group.rows as usize {
            return Err(corrupt("chunk holds a different number of rows than its row group"));
        }
        Ok(col)
    }

    /// The bloom filter of a column chunk, if it has one.
    pub fn bloom(&self, row_group: usize, column: usize) -> Result<Option<Bloom>> {
        let meta = &self.groups[row_group].columns[column];
        if meta.bloom_len == 0 {
            return Ok(None);
        }
        let bytes = self.read_chunk(meta.bloom_offset, meta.bloom_len)?;
        if bytes.len() < 5 {
            return Err(corrupt("bloom filter chunk is too short"));
        }
        Ok(Some(Bloom { hashes: bytes[4].clamp(1, 16), bits: bytes[5..].to_vec() }))
    }

    /// Reads the given columns of a row group as a batch with the matching
    /// sub-schema.
    pub fn read_row_group(&self, row_group: usize, columns: &[usize]) -> Result<Batch> {
        let schema = Schema::new(columns.iter().map(|&c| self.schema.columns[c].clone()).collect());
        let cols = columns.iter().map(|&c| self.read_column(row_group, c)).collect::<Result<Vec<_>>>()?;
        Batch::new(Arc::new(schema), cols)
    }

    /// The same rows with the columns of `schema`: column `i` is stored
    /// column `columns[i]` or, where that is a constant, that value in
    /// every row. Nothing is read; data is still fetched on demand from
    /// the same source. Used to read segments written before columns
    /// were added to, dropped from or reordered in a table.
    pub fn project(&self, schema: Arc<Schema>, columns: &[ProjectedColumn]) -> Result<Segment> {
        if columns.len() != schema.len() {
            return Err(Error::Invalid("projection does not match its schema".into()));
        }
        for (spec, field) in columns.iter().zip(&schema.columns) {
            if let ProjectedColumn::Stored(c) = spec {
                let stored = self.schema.columns.get(*c).ok_or_else(|| Error::Invalid(format!("no column {c}")))?;
                if !stored.ty.reads_as(&field.ty) {
                    return Err(Error::Type(format!(
                        "segment column {} of type {:?} cannot be read as {:?}",
                        stored.name, stored.ty, field.ty
                    )));
                }
            }
        }
        let constant_stats = |value: &Datum, rows: u64| ColumnStats {
            min: value.clone(),
            max: value.clone(),
            rows,
            null_count: if value.is_null() { rows } else { 0 },
            distinct: u64::from(!value.is_null() && rows > 0),
        };
        let groups = self
            .groups
            .iter()
            .map(|g| RowGroupMeta {
                rows: g.rows,
                columns: columns
                    .iter()
                    .zip(&schema.columns)
                    .map(|(spec, field)| match spec {
                        ProjectedColumn::Stored(c) => {
                            let mut meta = g.columns[*c].clone();
                            // A text bloom filter is not a JSON one.
                            if self.schema.columns[*c].ty != field.ty {
                                meta.bloom_len = 0;
                            }
                            meta
                        }
                        ProjectedColumn::Constant(v) => ChunkMeta {
                            offset: 0,
                            len: 0,
                            bloom_offset: 0,
                            bloom_len: 0,
                            stats: constant_stats(v, g.rows as u64),
                            constant: Some(v.clone()),
                        },
                    })
                    .collect(),
            })
            .collect();
        let stats = columns
            .iter()
            .map(|spec| match spec {
                ProjectedColumn::Stored(c) => self.stats[*c].clone(),
                ProjectedColumn::Constant(v) => constant_stats(v, self.rows),
            })
            .collect();
        let sort_key = self
            .sort_key
            .iter()
            .map_while(|k| columns.iter().position(|spec| *spec == ProjectedColumn::Stored(*k)))
            .collect();
        Ok(Segment { source: self.source.clone(), schema, sort_key, rows: self.rows, groups, stats })
    }

    /// Byte ranges (offset, length) of the stored chunks of the given
    /// columns in a row group, and of their bloom filters if `blooms`.
    pub fn chunk_ranges(&self, row_group: usize, columns: &[usize], blooms: bool) -> Vec<(u64, u64)> {
        let group = &self.groups[row_group];
        let mut out = Vec::new();
        for &c in columns {
            let m = &group.columns[c];
            if m.constant.is_none() {
                out.push((m.offset, m.len as u64));
                if blooms && m.bloom_len > 0 {
                    out.push((m.bloom_offset, m.bloom_len as u64));
                }
            }
        }
        out
    }

    /// Byte range of the bloom filter of a column chunk, if it has one.
    pub fn bloom_range(&self, row_group: usize, column: usize) -> Option<(u64, u64)> {
        let m = &self.groups[row_group].columns[column];
        (m.bloom_len > 0).then_some((m.bloom_offset, m.bloom_len as u64))
    }

    /// The byte source the segment reads from.
    pub fn source(&self) -> &Arc<dyn ChunkSource> {
        &self.source
    }

    /// Reads the whole segment.
    pub fn read_all(&self) -> Result<Batch> {
        let all: Vec<usize> = (0..self.schema.len()).collect();
        let batches = (0..self.groups.len()).map(|g| self.read_row_group(g, &all)).collect::<Result<Vec<_>>>()?;
        Ok(Batch::concat(self.schema.clone(), &batches))
    }
}
