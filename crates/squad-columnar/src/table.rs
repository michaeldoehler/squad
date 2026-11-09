//! Table-level helpers: the manifest of a table's segments, compaction
//! and retention.

use std::sync::Arc;

use crate::segment::{ColumnStats, Segment, SegmentWriter, WriterOptions};
use crate::types::{cmp_datum, ColumnType, Datum, Field, Schema};
use crate::util::{corrupt, Cur, Put};
use crate::{Error, Result};

const MANIFEST_MAGIC: &[u8; 8] = b"SQMANI01";

/// One segment of a table as recorded in the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMeta {
    /// Name under which the segment is stored (file name, object key).
    pub id: String,
    pub rows: u64,
    pub bytes: u64,
    /// Statistics per column over the whole segment.
    pub stats: Vec<ColumnStats>,
}

impl SegmentMeta {
    /// Describes an open segment.
    pub fn describe(id: impl Into<String>, segment: &Segment) -> SegmentMeta {
        SegmentMeta {
            id: id.into(),
            rows: segment.num_rows(),
            bytes: segment.size_bytes(),
            stats: (0..segment.schema().len()).map(|c| segment.column_stats(c).clone()).collect(),
        }
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
            let (tag, scale) = match f.ty {
                ColumnType::Bool => (0, 0),
                ColumnType::Int64 => (1, 0),
                ColumnType::Float64 => (2, 0),
                ColumnType::Decimal { scale } => (3, scale),
                ColumnType::Bytes => (4, 0),
                ColumnType::Utf8 => (5, 0),
            };
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
        }
        let crc = crc32c::crc32c(&out);
        out.put_u32(crc);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Manifest> {
        if bytes.len() < 12 || &bytes[..8] != MANIFEST_MAGIC {
            return Err(corrupt("not a squad table manifest"));
        }
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
            let ty = match tag {
                0 => ColumnType::Bool,
                1 => ColumnType::Int64,
                2 => ColumnType::Float64,
                3 => ColumnType::Decimal { scale },
                4 => ColumnType::Bytes,
                5 => ColumnType::Utf8,
                _ => return Err(corrupt("unknown column type in manifest")),
            };
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
            segments.push(SegmentMeta { id, rows, bytes: size, stats });
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
        let mut small: Vec<&SegmentMeta> = self.segments.iter().filter(|s| s.rows < target_rows / 2).collect();
        small.sort_by_key(|s| s.rows);
        let mut picked = Vec::new();
        let mut rows = 0;
        for s in small {
            if rows + s.rows > target_rows {
                break;
            }
            rows += s.rows;
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
