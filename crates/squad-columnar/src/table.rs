//! Table-level helpers: the manifest of a table's segments, compaction
//! and retention.

use std::sync::Arc;

use std::cmp::Ordering;

use crate::segment::{ColumnStats, Segment, SegmentStreamWriter, SegmentWriter, WriterOptions};
use crate::types::{cmp_datum, cmp_f64, Batch, Column, ColumnData, ColumnType, Datum, Field, Schema};
use crate::util::{corrupt, Cur, Put};
use crate::{Error, Result};

const MANIFEST_MAGIC_V1: &[u8; 8] = b"SQMANI01";
/// Version 2 adds the column ids of every segment.
const MANIFEST_MAGIC: &[u8; 8] = b"SQMANI02";

/// One segment of a table as recorded in the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMeta {
    /// Name under which the segment is stored (file name, object key).
    pub id: String,
    pub rows: u64,
    pub bytes: u64,
    /// Statistics per column over the whole segment.
    pub stats: Vec<ColumnStats>,
    /// Caller-defined identity of every column of the segment, in segment
    /// order (squad: the stable column ids of the table at the time the
    /// segment was written). Empty for segments of manifests written
    /// before ids were recorded.
    pub columns: Vec<u32>,
}

impl SegmentMeta {
    /// Describes an open segment.
    pub fn describe(id: impl Into<String>, segment: &Segment) -> SegmentMeta {
        SegmentMeta {
            id: id.into(),
            rows: segment.num_rows(),
            bytes: segment.size_bytes(),
            stats: (0..segment.schema().len()).map(|c| segment.column_stats(c).clone()).collect(),
            columns: Vec::new(),
        }
    }

    /// Like [`describe`](SegmentMeta::describe), recording column ids.
    pub fn with_columns(mut self, columns: Vec<u32>) -> SegmentMeta {
        self.columns = columns;
        self
    }
}

/// The list of segments that make up a table, with enough statistics to
/// decide which segments a query or a retention run has to open.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Manifest {
    pub schema: Schema,
    pub sort_key: Vec<usize>,
    pub segments: Vec<SegmentMeta>,
}

impl Manifest {
    pub fn new(schema: Schema, sort_key: Vec<usize>) -> Manifest {
        Manifest { schema, sort_key, segments: Vec::new() }
    }

    pub fn num_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.rows).sum()
    }

    /// Serializes the manifest; the result ends with a checksum.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MANIFEST_MAGIC);
        out.put_u32(self.schema.len() as u32);
        for f in &self.schema.columns {
            out.put_bytes(f.name.as_bytes());
            let (tag, scale) = f.ty.tag();
            out.put_u8(tag);
            out.put_u8(scale);
            out.put_u8(f.nullable as u8);
        }
        out.put_u32(self.sort_key.len() as u32);
        self.sort_key.iter().for_each(|c| out.put_u32(*c as u32));
        out.put_u32(self.segments.len() as u32);
        for s in &self.segments {
            out.put_bytes(s.id.as_bytes());
            out.put_u64(s.rows);
            out.put_u64(s.bytes);
            out.put_u32(s.stats.len() as u32);
            for st in &s.stats {
                out.put_datum(&st.min);
                out.put_datum(&st.max);
                out.put_u64(st.rows);
                out.put_u64(st.null_count);
                out.put_u64(st.distinct);
            }
            out.put_u32(s.columns.len() as u32);
            s.columns.iter().for_each(|c| out.put_u32(*c));
        }
        let crc = crc32c::crc32c(&out);
        out.put_u32(crc);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Manifest> {
        if bytes.len() < 12 || (&bytes[..8] != MANIFEST_MAGIC && &bytes[..8] != MANIFEST_MAGIC_V1) {
            return Err(corrupt("not a squad table manifest"));
        }
        let v2 = &bytes[..8] == MANIFEST_MAGIC;
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return Err(corrupt("manifest checksum mismatch"));
        }
        let mut cur = Cur::new(&body[8..]);
        let ncols = cur.u32()? as usize;
        let mut columns = Vec::new();
        for _ in 0..ncols {
            let name = cur.string()?;
            let (tag, scale, nullable) = (cur.u8()?, cur.u8()?, cur.u8()?);
            let ty = ColumnType::from_tag(tag, scale).ok_or_else(|| corrupt("unknown column type in manifest"))?;
            columns.push(Field { name, ty, nullable: nullable != 0 });
        }
        let nsort = cur.u32()? as usize;
        let mut sort_key = Vec::new();
        for _ in 0..nsort {
            sort_key.push(cur.u32()? as usize);
        }
        let nsegs = cur.u32()? as usize;
        let mut segments = Vec::new();
        for _ in 0..nsegs {
            let id = cur.string()?;
            let (rows, size) = (cur.u64()?, cur.u64()?);
            let nstats = cur.u32()? as usize;
            let mut stats = Vec::new();
            for _ in 0..nstats {
                let (min, max) = (cur.datum()?, cur.datum()?);
                stats.push(ColumnStats { min, max, rows: cur.u64()?, null_count: cur.u64()?, distinct: cur.u64()? });
            }
            let mut columns = Vec::new();
            if v2 {
                let n = cur.u32()? as usize;
                if n > body.len() {
                    return Err(corrupt("column id count exceeds the manifest"));
                }
                for _ in 0..n {
                    columns.push(cur.u32()?);
                }
            }
            segments.push(SegmentMeta { id, rows, bytes: size, stats, columns });
        }
        if cur.remaining() != 0 {
            return Err(corrupt("trailing bytes in manifest"));
        }
        Ok(Manifest { schema: Schema::new(columns), sort_key, segments })
    }

    /// Segments in which every row has `column < cutoff`: they can be
    /// dropped as a whole by a retention policy on that column. Segments
    /// with NULLs in the column, or without known bounds, are kept.
    pub fn expired(&self, column: usize, cutoff: &Datum) -> Vec<&SegmentMeta> {
        self.segments
            .iter()
            .filter(|s| match s.stats.get(column) {
                Some(st) => {
                    st.rows > 0
                        && st.null_count == 0
                        && !st.max.is_null()
                        && cmp_datum(&st.max, cutoff) == std::cmp::Ordering::Less
                }
                None => false,
            })
            .collect()
    }

    /// Candidates for compaction: the smallest segments whose combined
    /// row count stays within `target_rows`, if there are at least
    /// `min_segments` of them.
    pub fn compaction_candidates(&self, target_rows: u64, min_segments: usize) -> Vec<&SegmentMeta> {
        self.merge_candidates(target_rows, u64::MAX, usize::MAX, min_segments)
    }

    /// Like [`compaction_candidates`](Manifest::compaction_candidates),
    /// also bounding the combined size in bytes and the number of
    /// segments picked.
    pub fn merge_candidates(
        &self,
        target_rows: u64,
        max_bytes: u64,
        max_segments: usize,
        min_segments: usize,
    ) -> Vec<&SegmentMeta> {
        let mut small: Vec<&SegmentMeta> =
            self.segments.iter().filter(|s| s.rows < target_rows / 2 && s.bytes < max_bytes / 2).collect();
        small.sort_by_key(|s| s.rows);
        let mut picked = Vec::new();
        let (mut rows, mut bytes) = (0, 0);
        for s in small {
            if rows + s.rows > target_rows || bytes + s.bytes > max_bytes || picked.len() >= max_segments {
                break;
            }
            rows += s.rows;
            bytes += s.bytes;
            picked.push(s);
        }
        if picked.len() >= min_segments.max(2) {
            picked
        } else {
            Vec::new()
        }
    }
}

/// Merges segments into one. The rows are sorted by `options.sort_key`
/// (if any) across all inputs, so the result has the same clustering a
/// freshly written segment would have.
///
/// The inputs are read into memory together; compact sets of segments
/// whose combined size fits.
pub fn compact(segments: &[Arc<Segment>], options: WriterOptions) -> Result<Vec<u8>> {
    let first = segments.first().ok_or_else(|| Error::Invalid("nothing to compact".into()))?;
    let mut writer = SegmentWriter::new(first.schema().clone(), options);
    for s in segments {
        writer.write_batch(&s.read_all()?)?;
    }
    writer.finish()
}

/// Merges segments that are each sorted by `options.sort_key` into one
/// segment sorted by it, streaming: at most one decoded row group per
/// input and one output row group are held at a time, plus the encoded
/// output. The inputs must have the same schema (see
/// [`Segment::project`]). Inputs that are not sorted by the key still
/// produce a correct segment, only one that is less well clustered.
pub fn merge(segments: &[Arc<Segment>], options: WriterOptions) -> Result<Vec<u8>> {
    let first = segments.first().ok_or_else(|| Error::Invalid("nothing to merge".into()))?;
    let schema = first.schema().clone();
    if segments.iter().any(|s| {
        s.schema().len() != schema.len() || s.schema().columns.iter().zip(&schema.columns).any(|(a, b)| a.ty != b.ty)
    }) {
        return Err(Error::Type("segments to merge differ in schema".into()));
    }
    let keys = options.sort_key.clone();
    let all: Vec<usize> = (0..schema.len()).collect();
    let mut writer = SegmentStreamWriter::new(schema, options)?;
    // Without a sort key the inputs are simply concatenated.
    if keys.is_empty() {
        for s in segments {
            for g in 0..s.num_row_groups() {
                writer.write_batch(&s.read_row_group(g, &all)?)?;
            }
        }
        return writer.finish();
    }
    struct Input<'a> {
        segment: &'a Segment,
        group: usize,
        batch: Option<Batch>,
        row: usize,
    }
    impl Input<'_> {
        /// Makes sure `batch` holds an unread row; false when exhausted.
        fn ready(&mut self, all: &[usize]) -> Result<bool> {
            loop {
                if let Some(b) = &self.batch {
                    if self.row < b.num_rows() {
                        return Ok(true);
                    }
                }
                if self.group >= self.segment.num_row_groups() {
                    self.batch = None;
                    return Ok(false);
                }
                self.batch = Some(self.segment.read_row_group(self.group, all)?);
                self.group += 1;
                self.row = 0;
            }
        }
    }
    let mut inputs: Vec<Input> = segments.iter().map(|s| Input { segment: s, group: 0, batch: None, row: 0 }).collect();
    let mut live: Vec<usize> = Vec::new();
    for (i, input) in inputs.iter_mut().enumerate() {
        if input.ready(&all)? {
            live.push(i);
        }
    }
    while !live.is_empty() {
        // The input with the smallest head; ties go to the earlier input.
        let mut best = live[0];
        for &i in &live[1..] {
            let (a, b) = (&inputs[i], &inputs[best]);
            if cmp_rows(a.batch.as_ref().unwrap(), a.row, b.batch.as_ref().unwrap(), b.row, &keys) == Ordering::Less {
                best = i;
            }
        }
        // Take a run from it: rows up to the smallest head of the others.
        let (end, len) = {
            let input = &inputs[best];
            let batch = input.batch.as_ref().unwrap();
            let n = batch.num_rows();
            let mut end = n;
            for &o in live.iter().filter(|&&o| o != best) {
                let other = &inputs[o];
                let (ob, orow) = (other.batch.as_ref().unwrap(), other.row);
                // First row of `best` that sorts after the other's head
                // (equal rows stay with the earlier input).
                let after = |r: usize| {
                    let c = cmp_rows(batch, r, ob, orow, &keys);
                    c == Ordering::Greater || (c == Ordering::Equal && o < best)
                };
                let (mut lo, mut hi) = (input.row, end);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if after(mid) {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                end = lo;
            }
            (end.max(input.row + 1), n)
        };
        let input = &mut inputs[best];
        let batch = input.batch.as_ref().unwrap();
        if input.row == 0 && end == len {
            writer.write_batch(batch)?;
        } else {
            let indices: Vec<u32> = (input.row as u32..end as u32).collect();
            writer.write_batch(&batch.take(&indices))?;
        }
        input.row = end;
        if !input.ready(&all)? {
            live.retain(|&i| i != best);
        }
    }
    writer.finish()
}

/// Compares two rows by the key columns, NULLs last.
fn cmp_rows(a: &Batch, ra: usize, b: &Batch, rb: usize, keys: &[usize]) -> Ordering {
    for &k in keys {
        let o = cmp_cells(&a.columns[k], ra, &b.columns[k], rb);
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

fn cmp_cells(a: &Column, ra: usize, b: &Column, rb: usize) -> Ordering {
    match (a.is_null(ra), b.is_null(rb)) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    match (&a.data, &b.data) {
        (ColumnData::Bool(x), ColumnData::Bool(y)) => x[ra].cmp(&y[rb]),
        (ColumnData::Int64(x), ColumnData::Int64(y)) => x[ra].cmp(&y[rb]),
        (ColumnData::Float64(x), ColumnData::Float64(y)) => cmp_f64(x[ra], y[rb]),
        (ColumnData::Decimal(x), ColumnData::Decimal(y)) => x[ra].cmp(&y[rb]),
        (ColumnData::Bytes(x), ColumnData::Bytes(y)) => x.get(ra).cmp(y.get(rb)),
        _ => cmp_datum(&a.datum(ra), &b.datum(rb)),
    }
}
