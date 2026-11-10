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
use crate::types::{cmp_datum, cmp_f64, Batch, Column, ColumnData, ColumnType, Datum, Field, Schema};
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

/// Statistics and, for integer and byte columns, a bloom filter.
pub(crate) fn compute_stats(col: &Column) -> (ColumnStats, Option<Bloom>) {
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
                let mut f = Bloom::with_capacity(set.len());
                set.iter().for_each(|h| f.insert(*h));
                bloom = Some(f);
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
pub struct SegmentWriter {
    schema: Arc<Schema>,
    options: WriterOptions,
    batches: Vec<Batch>,
}

fn cmp_cell(col: &Column, a: usize, b: usize) -> Ordering {
    match (col.is_null(a), col.is_null(b)) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    match &col.data {
        ColumnData::Bool(v) => v[a].cmp(&v[b]),
        ColumnData::Int64(v) => v[a].cmp(&v[b]),
        ColumnData::Float64(v) => cmp_f64(v[a], v[b]),
        ColumnData::Decimal(v) => v[a].cmp(&v[b]),
        ColumnData::Bytes(v) => v.get(a).cmp(v.get(b)),
    }
}

impl SegmentWriter {
    pub fn new(schema: Arc<Schema>, options: WriterOptions) -> SegmentWriter {
        SegmentWriter { schema, options, batches: Vec::new() }
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        if batch.schema.columns.len() != self.schema.columns.len()
            || batch.schema.columns.iter().zip(&self.schema.columns).any(|(a, b)| a.ty != b.ty)
        {
            return Err(Error::Type("batch schema does not match the segment schema".into()));
        }
        self.batches.push(batch.clone());
        Ok(())
    }

    /// Rows buffered so far.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(Batch::num_rows).sum()
    }

    /// Encodes everything written and returns the segment's bytes.
    pub fn finish(self) -> Result<Vec<u8>> {
        let SegmentWriter { schema, options, batches } = self;
        if options.sort_key.iter().any(|c| *c >= schema.len()) {
            return Err(Error::Invalid("sort key names a column that does not exist".into()));
        }
        let mut all = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            Batch::concat(schema.clone(), &batches)
        };
        let rows = all.num_rows();
        if rows > u32::MAX as usize {
            return Err(Error::Invalid("a segment holds at most 2^32 - 1 rows".into()));
        }
        if !options.sort_key.is_empty() && rows > 1 {
            let keys: Vec<&Column> = options.sort_key.iter().map(|&c| &all.columns[c]).collect();
            let ints: Vec<&Vec<i64>> = keys
                .iter()
                .filter_map(|k| match (&k.data, &k.validity) {
                    (ColumnData::Int64(v), None) => Some(v),
                    _ => None,
                })
                .collect();
            let order: Vec<u32> = if ints.len() == keys.len() && keys.len() <= 2 {
                // The common case — integer ids and timestamps — sorts
                // plain tuples instead of going through a comparator.
                let second = ints.get(1);
                let mut tuples: Vec<(i64, i64, u32)> =
                    (0..rows).map(|r| (ints[0][r], second.map_or(0, |v| v[r]), r as u32)).collect();
                tuples.sort_unstable();
                tuples.into_iter().map(|t| t.2).collect()
            } else {
                let mut order: Vec<u32> = (0..rows as u32).collect();
                order.sort_unstable_by(|&a, &b| {
                    for k in &keys {
                        let o = cmp_cell(k, a as usize, b as usize);
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    a.cmp(&b)
                });
                order
            };
            if order.windows(2).any(|w| w[0] > w[1]) {
                all = all.take(&order);
            }
        }

        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        let mut footer = Vec::new();
        footer.put_u32(VERSION);
        footer.put_u32(schema.len() as u32);
        for f in &schema.columns {
            footer.put_bytes(f.name.as_bytes());
            let (tag, scale) = match f.ty {
                ColumnType::Bool => (0, 0),
                ColumnType::Int64 => (1, 0),
                ColumnType::Float64 => (2, 0),
                ColumnType::Decimal { scale } => (3, scale),
                ColumnType::Bytes => (4, 0),
                ColumnType::Utf8 => (5, 0),
            };
            footer.put_u8(tag);
            footer.put_u8(scale);
            footer.put_u8(f.nullable as u8);
        }
        footer.put_u32(options.sort_key.len() as u32);
        options.sort_key.iter().for_each(|c| footer.put_u32(*c as u32));
        footer.put_u64(rows as u64);
        let group_rows = options.row_group_rows.max(1);
        let groups = rows.div_ceil(group_rows);
        footer.put_u32(groups as u32);
        for g in 0..groups {
            let (start, end) = (g * group_rows, ((g + 1) * group_rows).min(rows));
            let indices: Vec<u32> = (start as u32..end as u32).collect();
            footer.put_u32((end - start) as u32);
            for col in &all.columns {
                let part = if groups == 1 { col.clone() } else { col.take(&indices) };
                let (stats, bloom) = compute_stats(&part);
                let raw = encode_column(&part);
                let (comp, data) = match options.compress.then(|| lz4_flex::block::compress(&raw)) {
                    Some(c) if c.len() * 10 <= raw.len() * 9 => (1u8, c),
                    _ => (0u8, raw.clone()),
                };
                let mut chunk = Vec::with_capacity(data.len() + 5);
                chunk.put_u8(comp);
                chunk.put_u32(raw.len() as u32);
                chunk.extend_from_slice(&data);
                footer.put_u64(out.len() as u64);
                footer.put_u32(chunk.len() as u32 + 4);
                out.put_u32(crc32c::crc32c(&chunk));
                out.extend_from_slice(&chunk);
                match bloom {
                    Some(b) => {
                        let mut chunk = vec![b.hashes];
                        chunk.extend_from_slice(&b.bits);
                        footer.put_u64(out.len() as u64);
                        footer.put_u32(chunk.len() as u32 + 4);
                        out.put_u32(crc32c::crc32c(&chunk));
                        out.extend_from_slice(&chunk);
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
        }
        out.extend_from_slice(&footer);
        out.put_u32(footer.len() as u32);
        out.put_u32(crc32c::crc32c(&footer));
        out.extend_from_slice(MAGIC);
        Ok(out)
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
            let ty = match tag {
                0 => ColumnType::Bool,
                1 => ColumnType::Int64,
                2 => ColumnType::Float64,
                3 => ColumnType::Decimal { scale },
                4 => ColumnType::Bytes,
                5 => ColumnType::Utf8,
                _ => return Err(corrupt("unknown column type")),
            };
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

    /// Reads the whole segment.
    pub fn read_all(&self) -> Result<Batch> {
        let all: Vec<usize> = (0..self.schema.len()).collect();
        let batches = (0..self.groups.len()).map(|g| self.read_row_group(g, &all)).collect::<Result<Vec<_>>>()?;
        Ok(Batch::concat(self.schema.clone(), &batches))
    }
}
