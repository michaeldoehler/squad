//! Column-oriented storage and vectorized aggregation for squad.
//!
//! Row stores are built for looking up and changing individual rows.
//! Analytical queries — "events per day for this project over the last
//! month", "unique users by country", "p95 duration per endpoint" — read
//! a few columns of very many rows, and are orders of magnitude faster on
//! data that is stored column by column, compressed, with statistics that
//! let a query skip what it does not need.
//!
//! This crate provides that:
//!
//! * [`SegmentWriter`] turns batches of rows into an immutable, compressed,
//!   checksummed **segment**; [`Segment`] reads one back through any
//!   [`ChunkSource`] (memory, a file, an object store), fetching only the
//!   columns and row groups a query touches.
//! * [`scan`] and [`aggregate`] run filters and grouped aggregations over
//!   a [`TableSource`]: any number of segments plus in-memory batches of
//!   rows that have not been written to a segment yet.
//! * [`Manifest`], [`compact`] and [`Manifest::expired`] are the building
//!   blocks for managing a table's segments.
//!
//! ```
//! use std::sync::Arc;
//! use squad_columnar::*;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("day", ColumnType::Int64, false),
//!     Field::new("page", ColumnType::Utf8, false),
//!     Field::new("ms", ColumnType::Int64, true),
//! ]));
//! let rows = vec![
//!     vec![Datum::Int(1), Datum::text("/"), Datum::Int(120)],
//!     vec![Datum::Int(1), Datum::text("/pricing"), Datum::Int(80)],
//!     vec![Datum::Int(2), Datum::text("/"), Datum::Null],
//! ];
//! let mut writer = SegmentWriter::new(schema.clone(), WriterOptions::default());
//! writer.write_batch(&Batch::from_rows(schema.clone(), &rows)?)?;
//! let segment = Segment::open(Arc::new(writer.finish()?))?;
//!
//! let mut table = TableSource::new(schema);
//! table.segments.push(Arc::new(segment));
//! let result = aggregate(&table, &AggregateRequest {
//!     predicate: Some(Predicate::Cmp { column: 1, op: CmpOp::Eq, value: Datum::text("/") }),
//!     group_by: vec![GroupKey::Column(0)],
//!     aggregates: vec![Aggregate::CountStar, Aggregate::Avg(2)],
//!     order_by: vec![OrderBy { column: 0, descending: false }],
//!     ..Default::default()
//! })?;
//! assert_eq!(result.to_rows(), vec![
//!     vec![Datum::Int(1), Datum::Int(1), Datum::Float(120.0)],
//!     vec![Datum::Int(2), Datum::Int(1), Datum::Null],
//! ]);
//! # Ok::<(), squad_columnar::Error>(())
//! ```

mod agg;
mod encoding;
mod exec;
mod json;
mod predicate;
mod segment;
mod sketch;
mod table;
mod types;
mod util;

pub use agg::{aggregate, floor_time, Aggregate, AggregateRequest, GroupKey, OrderBy, TimeUnit};
pub use encoding::{decode_batch, encode_batch};
pub use exec::{scan, Scan, ScanRequest, TableSource};
pub use json::{field_text as json_field_text, for_each_field as json_for_each_field, json_key_hash, json_pair_hash};
pub use predicate::{CmpOp, Predicate};
pub use segment::{
    Bloom, ChunkMeta, ChunkSource, ColumnStats, FileSource, ProjectedColumn, RowGroupMeta, Segment,
    SegmentStreamWriter, SegmentWriter, WriterOptions,
};
pub use sketch::{Hll, QuantileSketch};
pub use table::{compact, merge, merge_to_file, Manifest, SegmentMeta};
pub use types::{
    cmp_datum, cmp_f64, Batch, BytesColumn, BytesData, Column, ColumnBuilder, ColumnData, ColumnType, Datum, Field,
    Schema,
};
pub use util::{hash_bytes, hash_u64};

/// Errors of the columnar engine.
#[derive(Debug)]
pub enum Error {
    /// Reading from a [`ChunkSource`] failed.
    Io(std::io::Error),
    /// Stored data failed a checksum or structural check.
    Corrupt(String),
    /// A value or column has the wrong type for the operation.
    Type(String),
    /// The request is invalid.
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corrupt(m) => write!(f, "corrupt column data: {m}"),
            Error::Type(m) => write!(f, "type error: {m}"),
            Error::Invalid(m) => write!(f, "invalid request: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
