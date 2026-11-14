//! Columnar tables: `CREATE TABLE … USING columnar`.
//!
//! A columnar table has two parts. New rows are inserted into the row
//! store — transactional, durable through the write-ahead log and
//! immediately visible. This part is the *delta*. Each insert statement
//! stores its rows as one or a few *blocks*: one key-value entry holding
//! the rows already in columnar form, so that a thousand-row insert costs
//! one entry rather than a thousand. Compaction moves delta blocks into
//! immutable column-oriented *segments* (see the `squad-columnar` crate),
//! which live in the database's object store: a local directory, memory,
//! or an S3-compatible bucket. The list of segments, the *manifest*, is a
//! versioned record in the key-value store, so a compaction is one atomic
//! transaction: readers see either the rows in the delta or the same rows
//! in a segment, never both or neither.
//!
//! Compaction runs on a background thread per database (see
//! [`Compactor`]) once a table's delta holds enough rows or blocks, or is
//! old enough. Segments are read in ranges — the footer, then only the
//! column chunks a query needs — through a byte-bounded LRU cache.
//!
//! Every column of a columnar table has a stable id. Segments and blocks
//! record the ids of the columns they hold, so columns can be added,
//! dropped and renamed without rewriting stored data: a column a segment
//! does not hold reads as the value it had when it was added (NULL or its
//! default).
//!
//! Queries read both parts. Aggregations that the column engine can
//! express run there, vectorized and in parallel; everything else scans
//! the segments as rows through the ordinary executor.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rust_decimal::Decimal;
use squad_columnar as cs;
use squad_kv::{Cursor, LockMode, WaitPolicy};

use crate::agg::numeric_div;
use crate::catalog::{object_prefix, Column, Table};
use crate::exec::{ExecCx, Node};
use crate::expr::{Expr, Row};
use crate::plan::{AggCall, AggFunc, ScanPlan};
use crate::types::{FnCtx, Type, Value};
use crate::value::{format_value, parse_value};
use crate::{sqlstate, Error, Result};

// -------------------------------------------------------------------
// Configuration
// -------------------------------------------------------------------

/// Settings of columnar tables, part of [`Config`](crate::Config).
#[derive(Debug, Clone)]
pub struct ColumnarConfig {
    /// Memory for cached segment data (column chunks and footers) and
    /// decoded delta blocks, in bytes.
    pub cache_bytes: u64,
    /// Run compaction on a background thread. When false, the delta is
    /// only compacted by `VACUUM`.
    pub background_compaction: bool,
    /// Rows in a table's delta that start a compaction.
    pub compact_rows: u64,
    /// Blocks (insert statements) in a table's delta that start a
    /// compaction, so that many single-row inserts are compacted early.
    pub compact_blocks: u64,
    /// Rows in the delta older than this are compacted even if the other
    /// thresholds are not reached.
    pub compact_age: Duration,
    /// Upper bound of the size of a segment produced by merging smaller
    /// ones; also bounds the memory a merge needs for its output.
    pub merge_max_bytes: u64,
    /// `VACUUM` deletes segment objects that no manifest names only once
    /// they are older than this, in case a writer that is not this
    /// process is still about to commit them.
    pub orphan_grace: Duration,
}

impl Default for ColumnarConfig {
    fn default() -> Self {
        ColumnarConfig {
            cache_bytes: 512 << 20,
            background_compaction: true,
            compact_rows: 65_536,
            compact_blocks: 512,
            compact_age: Duration::from_secs(10),
            merge_max_bytes: 256 << 20,
            orphan_grace: Duration::from_secs(3600),
        }
    }
}

/// Segments smaller than half of this are merged with each other.
const TARGET_SEGMENT_ROWS: u64 = 4_000_000;
const MERGE_MIN_SEGMENTS: usize = 8;
const MERGE_MAX_SEGMENTS: usize = 32;
/// Most rows one compaction moves out of the delta; the rest waits for
/// the next run. Bounds the memory of a compaction.
pub(crate) const COMPACT_MAX_ROWS: usize = 1_000_000;
/// Rows of one delta block.
const BLOCK_ROWS: usize = 65_536;
/// Rows of one `INSERT` from which they are written straight into
/// segments instead of passing through the delta.
pub(crate) const BULK_INSERT_ROWS: usize = 50_000;
const BULK_SEGMENT_ROWS: usize = 1_000_000;

impl From<cs::Error> for Error {
    fn from(e: cs::Error) -> Error {
        let code = match &e {
            // A segment object that is gone: see `open_segments`.
            cs::Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => sqlstate::DATA_CORRUPTED,
            cs::Error::Io(_) => sqlstate::IO_ERROR,
            cs::Error::Corrupt(_) => sqlstate::DATA_CORRUPTED,
            _ => sqlstate::INTERNAL_ERROR,
        };
        Error::new(code, format!("columnar storage: {e}"))
    }
}

impl From<squad_object::Error> for Error {
    fn from(e: squad_object::Error) -> Error {
        Error::new(sqlstate::IO_ERROR, e.to_string())
    }
}

fn is_missing(e: &squad_object::Error) -> bool {
    match e {
        squad_object::Error::Remote { status: 404, .. } => true,
        squad_object::Error::Io(io) => io.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

// -------------------------------------------------------------------
// Keys and identities
// -------------------------------------------------------------------

/// Key of a table's manifest in the key-value store.
pub(crate) fn manifest_key(table_id: u32) -> Vec<u8> {
    let mut k = b"\x00\x00\x00\x00columnar/".to_vec();
    k.extend_from_slice(&table_id.to_be_bytes());
    k
}

fn object_key(table_id: u32, segment: &str) -> String {
    format!("columnar/{table_id}/{segment}.seg")
}

/// Delta blocks live in the table's key range after its row keys (which
/// start with 0 or 1, the NULL marker of the row id): `prefix 2 id`.
const BLOCK_TAG: u8 = 2;

fn block_key(table_id: u32, block: u64) -> Vec<u8> {
    let mut k = object_prefix(table_id).to_vec();
    k.push(BLOCK_TAG);
    k.extend_from_slice(&block.to_be_bytes());
    k
}

fn block_id(key: &[u8]) -> Option<u64> {
    key.get(5..13).map(|b| u64::from_be_bytes(b.try_into().unwrap()))
}

/// A new segment id: creation time in milliseconds and randomness. The
/// time lets `VACUUM` tell recent unreferenced objects (perhaps about to
/// be committed by another writer) from old leftovers.
fn new_segment_id() -> String {
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis());
    format!("{millis:x}-{:016x}", rand::random::<u64>())
}

/// Creation time of a segment id made by [`new_segment_id`]; `None` for
/// ids of older versions.
fn segment_created(id: &str) -> Option<std::time::SystemTime> {
    let (millis, _) = id.split_once('-')?;
    let millis = u64::from_str_radix(millis, 16).ok()?;
    Some(std::time::UNIX_EPOCH + Duration::from_millis(millis))
}

/// How a SQL column is represented in a segment.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Bool,
    /// Integers, dates (days) and times/timestamps (microseconds).
    Int,
    Float,
    Decimal(u8),
    Text,
    Bytes,
    /// `jsonb`, as its text representation; fields can be filtered and
    /// grouped by in the column engine.
    Json,
    /// Any other type, stored as its text representation. Such columns
    /// can be read back but not filtered or aggregated by the column
    /// engine.
    AsText,
}

fn kind(col: &Column) -> Kind {
    match &col.ty {
        Type::Bool => Kind::Bool,
        Type::Int2
        | Type::Int4
        | Type::Int8
        | Type::Oid
        | Type::Date
        | Type::Time
        | Type::Timestamp
        | Type::TimestampTz => Kind::Int,
        Type::Float4 | Type::Float8 => Kind::Float,
        Type::Numeric if col.typmod >= 4 => Kind::Decimal(((col.typmod - 4) & 0xffff).min(28) as u8),
        Type::Text | Type::Enum(_) | Type::Char => Kind::Text,
        Type::Uuid | Type::Bytea => Kind::Bytes,
        Type::Jsonb => Kind::Json,
        _ => Kind::AsText,
    }
}

fn column_type(col: &Column) -> cs::ColumnType {
    match kind(col) {
        Kind::Bool => cs::ColumnType::Bool,
        Kind::Int => cs::ColumnType::Int64,
        Kind::Float => cs::ColumnType::Float64,
        Kind::Decimal(scale) => cs::ColumnType::Decimal { scale },
        Kind::Text | Kind::AsText => cs::ColumnType::Utf8,
        Kind::Json => cs::ColumnType::Json,
        Kind::Bytes => cs::ColumnType::Bytes,
    }
}

/// Columns stored in segments: all but the hidden row id.
fn stored(table: &Table) -> impl Iterator<Item = (usize, &Column)> {
    table.columns.iter().enumerate().filter(|(_, c)| !c.hidden)
}

/// Stable ids of the stored columns, in table order. Tables created
/// before ids were recorded use their positions.
pub(crate) fn column_ids(table: &Table) -> Vec<u32> {
    match &table.columnar {
        Some(c) if !c.column_ids.is_empty() => c.column_ids.clone(),
        _ => (0..stored(table).count() as u32).collect(),
    }
}

/// Fixed formatting context for values kept in the catalog.
fn catalog_fn_ctx() -> FnCtx {
    FnCtx::default()
}

/// The shape of a table as segments and blocks are mapped onto it.
pub(crate) struct Layout {
    pub(crate) schema: Arc<cs::Schema>,
    ids: Vec<u32>,
    /// Per stored column: the value of rows written before it existed.
    missing: Vec<cs::Datum>,
    /// Hash of all of the above, for caching projected segments.
    signature: u64,
}

impl Layout {
    pub(crate) fn of(table: &Table) -> Result<Arc<Layout>> {
        let fields: Vec<cs::Field> =
            stored(table).map(|(_, c)| cs::Field::new(c.name.clone(), column_type(c), true)).collect();
        let ids = column_ids(table);
        if ids.len() != fields.len() {
            return Err(Error::new(sqlstate::DATA_CORRUPTED, "columnar column ids do not match the table"));
        }
        let texts = table.columnar.as_ref().map(|c| c.missing.clone()).unwrap_or_default();
        let ctx = catalog_fn_ctx();
        let mut missing = Vec::with_capacity(ids.len());
        for (i, (_, col)) in stored(table).enumerate() {
            missing.push(match texts.get(i).cloned().flatten() {
                None => cs::Datum::Null,
                Some(text) => {
                    let v = parse_value(&text, &col.ty, &ctx)?;
                    to_datum_with(&v, col, &ctx).unwrap_or(cs::Datum::Null)
                }
            });
        }
        let signature = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            ids.hash(&mut h);
            fields.iter().for_each(|f| f.ty.hash(&mut h));
            format!("{missing:?}").hash(&mut h);
            h.finish()
        };
        Ok(Arc::new(Layout { schema: Arc::new(cs::Schema::new(fields)), ids, missing, signature }))
    }

    /// Schema of blocks: like the table's, with column ids as names.
    fn block_schema(&self) -> Arc<cs::Schema> {
        Arc::new(cs::Schema::new(
            self.schema
                .columns
                .iter()
                .zip(&self.ids)
                .map(|(f, id)| cs::Field::new(id.to_string(), f.ty, true))
                .collect(),
        ))
    }

    /// How to read data holding columns `ids` (with types `types`) in this
    /// layout; `None` if it already has exactly this layout.
    fn mapping(&self, ids: &[u32], types: &[cs::ColumnType]) -> Result<Option<Vec<cs::ProjectedColumn>>> {
        let ids: Vec<u32> = if ids.is_empty() { (0..types.len() as u32).collect() } else { ids.to_vec() };
        if ids == self.ids && types.iter().zip(&self.schema.columns).all(|(a, f)| *a == f.ty) {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(self.ids.len());
        for (i, id) in self.ids.iter().enumerate() {
            match ids.iter().position(|x| x == id) {
                Some(p) if types.get(p).is_some_and(|t| t.reads_as(&self.schema.columns[i].ty)) => {
                    out.push(cs::ProjectedColumn::Stored(p))
                }
                Some(_) => {
                    return Err(Error::new(sqlstate::DATA_CORRUPTED, "stored column has an unexpected type"));
                }
                None => out.push(cs::ProjectedColumn::Constant(self.missing[i].clone())),
            }
        }
        Ok(Some(out))
    }

    /// Statistics of the table's columns for a segment described in the
    /// manifest; columns the segment lacks hold their missing value.
    fn segment_stats(&self, meta: &cs::SegmentMeta) -> Vec<Option<cs::ColumnStats>> {
        let ids: Vec<u32> =
            if meta.columns.is_empty() { (0..meta.stats.len() as u32).collect() } else { meta.columns.clone() };
        self.ids
            .iter()
            .enumerate()
            .map(|(i, id)| match ids.iter().position(|x| x == id) {
                Some(p) => meta.stats.get(p).cloned(),
                None => {
                    let v = &self.missing[i];
                    Some(cs::ColumnStats {
                        min: v.clone(),
                        max: v.clone(),
                        rows: meta.rows,
                        null_count: if v.is_null() { meta.rows } else { 0 },
                        distinct: u64::from(!v.is_null()),
                    })
                }
            })
            .collect()
    }

    /// A block's batch (columns named by id) in this layout; the block
    /// itself if it has this layout already.
    fn adopt(&self, block: &Arc<cs::Batch>) -> Result<Arc<cs::Batch>> {
        let same = block.schema.columns.len() == self.ids.len()
            && block
                .schema
                .columns
                .iter()
                .zip(&self.ids)
                .zip(&self.schema.columns)
                .all(|((f, id), want)| f.ty == want.ty && f.name.parse::<u32>().ok() == Some(*id));
        if same {
            return Ok(block.clone());
        }
        let rows = block.num_rows();
        let mut columns = Vec::with_capacity(self.ids.len());
        for (i, id) in self.ids.iter().enumerate() {
            let ty = self.schema.columns[i].ty;
            let found = block.schema.columns.iter().position(|f| f.name.parse::<u32>().ok() == Some(*id));
            columns.push(match found {
                Some(p) if block.schema.columns[p].ty.reads_as(&ty) => block.columns[p].clone(),
                Some(_) => return Err(Error::new(sqlstate::DATA_CORRUPTED, "delta column has an unexpected type")),
                None => cs::Column::constant(ty, &self.missing[i], rows)?,
            });
        }
        Ok(Arc::new(cs::Batch::new(self.schema.clone(), columns)?))
    }
}

// -------------------------------------------------------------------
// Values
// -------------------------------------------------------------------

fn to_datum(v: &Value, col: &Column, cx: &ExecCx) -> Option<cs::Datum> {
    to_datum_with(v, col, &cx.fn_ctx)
}

fn to_datum_with(v: &Value, col: &Column, ctx: &FnCtx) -> Option<cs::Datum> {
    if v.is_null() {
        return Some(cs::Datum::Null);
    }
    Some(match (kind(col), v) {
        (Kind::Bool, Value::Bool(b)) => cs::Datum::Bool(*b),
        (Kind::Int, Value::Date(d)) => cs::Datum::Int(*d as i64),
        (Kind::Int, Value::Time(t) | Value::Timestamp(t) | Value::TimestampTz(t)) => cs::Datum::Int(*t),
        (Kind::Int, v) => cs::Datum::Int(v.as_i64()?),
        (Kind::Float, Value::Float4(f)) => cs::Datum::Float(*f as f64),
        (Kind::Float, Value::Float8(f)) => cs::Datum::Float(*f),
        (Kind::Decimal(scale), v) => {
            let mut d = v.as_decimal()?;
            if d.scale() > scale as u32 {
                // Only exact values can be compared in the column engine.
                if d.round_dp(scale as u32) != d {
                    return None;
                }
            }
            d.rescale(scale as u32);
            cs::Datum::Decimal(d.mantissa())
        }
        (Kind::Text, v) => cs::Datum::text(v.as_str()?),
        (Kind::Bytes, Value::Uuid(u)) => cs::Datum::Bytes(u.to_vec()),
        (Kind::Bytes, Value::Bytea(b)) => cs::Datum::Bytes(b.clone()),
        (Kind::Json | Kind::AsText, v) => cs::Datum::text(format_value(v, ctx)),
        _ => return None,
    })
}

fn from_datum(d: cs::Datum, col: &Column, cx: &ExecCx) -> Result<Value> {
    let bad = || Error::new(sqlstate::DATA_CORRUPTED, format!("segment value does not fit column \"{}\"", col.name));
    Ok(match (d, &col.ty) {
        (cs::Datum::Null, _) => Value::Null,
        (cs::Datum::Bool(b), _) => Value::Bool(b),
        (cs::Datum::Int(i), Type::Int2) => Value::Int2(i as i16),
        (cs::Datum::Int(i), Type::Int4) => Value::Int4(i as i32),
        (cs::Datum::Int(i), Type::Int8 | Type::Oid) => Value::Int8(i),
        (cs::Datum::Int(i), Type::Date) => Value::Date(i as i32),
        (cs::Datum::Int(i), Type::Time) => Value::Time(i),
        (cs::Datum::Int(i), Type::Timestamp) => Value::Timestamp(i),
        (cs::Datum::Int(i), Type::TimestampTz) => Value::TimestampTz(i),
        (cs::Datum::Float(f), Type::Float4) => Value::Float4(f as f32),
        (cs::Datum::Float(f), _) => Value::Float8(f),
        (cs::Datum::Decimal(m), _) => match kind(col) {
            Kind::Decimal(scale) => {
                Value::Numeric(Decimal::try_from_i128_with_scale(m, scale as u32).map_err(|_| bad())?)
            }
            _ => return Err(bad()),
        },
        (cs::Datum::Bytes(b), ty) => match (kind(col), ty) {
            (Kind::Bytes, Type::Uuid) => Value::Uuid(b.try_into().map_err(|_| bad())?),
            (Kind::Bytes, _) => Value::Bytea(b),
            (Kind::Text, Type::Enum(name)) => {
                let text = String::from_utf8(b).map_err(|_| bad())?;
                let ty = cx.catalog.enums.values().find(|e| e.name == *name).ok_or_else(bad)?;
                crate::expr::enum_value(ty, &text)?
            }
            (Kind::Text, _) => Value::Text(String::from_utf8(b).map_err(|_| bad())?),
            (Kind::Json | Kind::AsText, ty) => {
                parse_value(std::str::from_utf8(&b).map_err(|_| bad())?, ty, &cx.fn_ctx)?
            }
            _ => return Err(bad()),
        },
        _ => return Err(bad()),
    })
}

/// Rows of the table (all columns, hidden ones included) as a batch in
/// the given schema (the table's stored columns in order).
fn rows_to_batch(table: &Table, schema: Arc<cs::Schema>, rows: &[Row], cx: &ExecCx) -> Result<cs::Batch> {
    let cols: Vec<(usize, &Column)> = stored(table).collect();
    let mut builders: Vec<cs::ColumnBuilder> = schema.columns.iter().map(|f| cs::ColumnBuilder::new(f.ty)).collect();
    for row in rows {
        for ((i, col), b) in cols.iter().zip(builders.iter_mut()) {
            let d = to_datum(&row[*i], col, cx).ok_or_else(|| {
                Error::internal(format!("value of column \"{}\" cannot be stored in a segment", col.name))
            })?;
            b.push(&d)?;
        }
    }
    Ok(cs::Batch::new(schema, builders.into_iter().map(cs::ColumnBuilder::finish).collect())?)
}

/// Converts a batch in the table's layout into table rows.
fn batch_rows(table: &Table, batch: &cs::Batch, out: &mut Vec<Row>, cx: &ExecCx) -> Result<()> {
    out.reserve(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut data = batch.row(i).into_iter();
        let mut row = Vec::with_capacity(table.columns.len());
        for col in &table.columns {
            row.push(match col.hidden {
                // Rows in segments and blocks have no row id.
                true => Value::Null,
                false => from_datum(data.next().unwrap_or(cs::Datum::Null), col, cx)?,
            });
        }
        out.push(row);
    }
    Ok(())
}

fn batch_bytes(b: &cs::Batch) -> u64 {
    let col = |c: &cs::Column| -> u64 {
        let data = match &c.data {
            cs::ColumnData::Bool(v) => v.len() as u64,
            cs::ColumnData::Int64(v) => v.len() as u64 * 8,
            cs::ColumnData::Float64(v) => v.len() as u64 * 8,
            cs::ColumnData::Decimal(v) => v.len() as u64 * 16,
            cs::ColumnData::Bytes(cs::BytesColumn::Plain(d)) => d.data.len() as u64 + d.offsets.len() as u64 * 4,
            cs::ColumnData::Bytes(cs::BytesColumn::Dict { codes, dict }) => {
                codes.len() as u64 * 4 + dict.data.len() as u64 + dict.offsets.len() as u64 * 4
            }
        };
        data + c.validity.as_ref().map_or(0, |v| v.len() as u64)
    };
    b.columns.iter().map(col).sum::<u64>() + 64
}

// -------------------------------------------------------------------
// Caches
// -------------------------------------------------------------------

/// A map with least-recently-used eviction by a byte budget.
struct Lru<K, V> {
    map: HashMap<K, (V, u64, u64)>,
    order: BTreeMap<u64, K>,
    tick: u64,
    bytes: u64,
    limit: u64,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new(limit: u64) -> Self {
        Lru { map: HashMap::new(), order: BTreeMap::new(), tick: 0, bytes: 0, limit }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        self.tick += 1;
        let tick = self.tick;
        let entry = self.map.get_mut(key)?;
        let old = std::mem::replace(&mut entry.2, tick);
        let value = entry.0.clone();
        if let Some(k) = self.order.remove(&old) {
            self.order.insert(tick, k);
        }
        Some(value)
    }

    fn insert(&mut self, key: K, value: V, size: u64) {
        if size > self.limit {
            return;
        }
        self.remove(&key);
        while self.bytes + size > self.limit {
            let Some((_, victim)) = self.order.pop_first() else { break };
            if let Some((_, n, _)) = self.map.remove(&victim) {
                self.bytes -= n;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, key.clone());
        self.map.insert(key, (value, size, self.tick));
        self.bytes += size;
    }

    fn remove(&mut self, key: &K) {
        if let Some((_, n, tick)) = self.map.remove(key) {
            self.order.remove(&tick);
            self.bytes -= n;
        }
    }
}

const CACHE_SHARDS: usize = 16;

type ChunkKey = (Arc<str>, u64);

/// Ranges of segment objects, cached by bytes, with concurrent misses of
/// the same range fetched once.
struct ChunkCache {
    shards: Vec<Mutex<Lru<ChunkKey, Arc<Vec<u8>>>>>,
    /// Ranges being fetched; later readers wait for the first.
    inflight: Mutex<HashMap<ChunkKey, Arc<Flight>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Default)]
struct Flight {
    done: Mutex<Option<std::result::Result<Arc<Vec<u8>>, String>>>,
    cv: Condvar,
}

impl ChunkCache {
    fn new(bytes: u64) -> ChunkCache {
        let per = (bytes / CACHE_SHARDS as u64).max(1 << 20);
        ChunkCache {
            shards: (0..CACHE_SHARDS).map(|_| Mutex::new(Lru::new(per))).collect(),
            inflight: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: &ChunkKey) -> &Mutex<Lru<ChunkKey, Arc<Vec<u8>>>> {
        let h = cs::hash_bytes(key.0.as_bytes()) ^ cs::hash_u64(key.1);
        &self.shards[(h % CACHE_SHARDS as u64) as usize]
    }

    fn cached(&self, key: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        self.shard(key).lock().get(key)
    }

    fn put(&self, key: ChunkKey, data: Arc<Vec<u8>>) {
        let size = data.len() as u64 + 64;
        self.shard(&key).lock().insert(key, data, size);
    }

    /// Registers a fetch of `key` unless it is cached or being fetched.
    fn claim(&self, key: &ChunkKey) -> Option<Arc<Flight>> {
        let mut inflight = self.inflight.lock();
        if inflight.contains_key(key) || self.cached(key).is_some() {
            return None;
        }
        let f = Arc::new(Flight::default());
        inflight.insert(key.clone(), f.clone());
        Some(f)
    }

    /// Ends a fetch registered by [`claim`](ChunkCache::claim).
    fn complete(&self, key: &ChunkKey, flight: &Flight, result: &std::io::Result<Arc<Vec<u8>>>) {
        if let Ok(data) = result {
            self.put(key.clone(), data.clone());
        }
        *flight.done.lock() = Some(result.as_ref().map(Arc::clone).map_err(|e| e.to_string()));
        flight.cv.notify_all();
        self.inflight.lock().remove(key);
    }

    /// The bytes of `key`, fetched with `fetch` on a miss. Concurrent
    /// misses of one key wait for a single fetch.
    fn get(&self, key: ChunkKey, fetch: impl FnOnce() -> std::io::Result<Vec<u8>>) -> std::io::Result<Arc<Vec<u8>>> {
        loop {
            if let Some(hit) = self.cached(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(hit);
            }
            if let Some(flight) = self.claim(&key) {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let result = fetch().map(Arc::new);
                self.complete(&key, &flight, &result);
                return result;
            }
            let flight = self.inflight.lock().get(&key).cloned();
            let Some(flight) = flight else { continue };
            let mut done = flight.done.lock();
            while done.is_none() {
                flight.cv.wait(&mut done);
            }
            if let Some(Ok(data)) = done.as_ref() {
                return Ok(data.clone());
            }
            // The other fetch failed (perhaps a combined read ahead):
            // fetch on our own.
            drop(done);
            self.misses.fetch_add(1, Ordering::Relaxed);
            return fetch().map(Arc::new);
        }
    }
}

/// Neighbouring ranges fetched together: start, end and the ranges.
type Span = (u64, u64, Vec<(u64, u64)>);

/// Opened segments by object key and layout signature.
type SegmentLru = Lru<(Arc<str>, u64), Arc<cs::Segment>>;

/// A segment object read in ranges through the chunk cache.
struct ObjectSource {
    store: Arc<dyn squad_object::ObjectStore>,
    key: Arc<str>,
    len: u64,
    cache: Arc<ChunkCache>,
    /// Fetch announced ranges ahead, in parallel (remote stores), up to
    /// this many bytes per announcement; 0 for local stores, where each
    /// read is cheap.
    prefetch_bytes: u64,
}

fn io_error(key: &str, e: squad_object::Error) -> std::io::Error {
    if is_missing(&e) {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("segment object {key} is missing"))
    } else {
        std::io::Error::other(e.to_string())
    }
}

impl cs::ChunkSource for ObjectSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read(&self, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
        let key = (self.key.clone(), offset);
        let data =
            self.cache.get(key, || self.store.get_range(&self.key, offset, len).map_err(|e| io_error(&self.key, e)))?;
        let have = data.as_slice().len() as u64;
        if have == len {
            return Ok(data.as_ref().clone());
        }
        // A cached range of another length at the same offset (the tail
        // read of a footer): serve from it if it covers the request.
        if have > len {
            return Ok(data[..len as usize].to_vec());
        }
        self.store.get_range(&self.key, offset, len).map_err(|e| io_error(&self.key, e))
    }

    /// Fetches the missing ranges in a few requests: neighbouring ranges
    /// are fetched together, and separate requests run in parallel.
    fn prefetch(&self, ranges: &[(u64, u64)]) {
        if self.prefetch_bytes == 0 {
            return;
        }
        let mut missing: Vec<(u64, u64)> =
            ranges.iter().copied().filter(|(o, _)| self.cache.cached(&(self.key.clone(), *o)).is_none()).collect();
        if missing.len() < 2 {
            return;
        }
        // More than the cache should hold at once is read as needed.
        let mut budget = self.prefetch_bytes;
        missing.retain(|(_, l)| {
            let keep = *l <= budget;
            budget = budget.saturating_sub(*l);
            keep
        });
        missing.sort_unstable();
        // Spans of ranges with gaps of at most 64 KiB between them.
        let mut spans: Vec<Span> = Vec::new();
        for (o, l) in missing {
            match spans.last_mut() {
                Some((start, end, parts)) if o <= *end + (64 << 10) && o + l - *start <= 64 << 20 => {
                    *end = (*end).max(o + l);
                    parts.push((o, l));
                }
                _ => spans.push((o, o + l, vec![(o, l)])),
            }
        }
        let fetch = |(start, end, parts): &Span| {
            // Ranges another reader is already fetching are left to it.
            let claimed: Vec<((u64, u64), Arc<Flight>)> =
                parts.iter().filter_map(|p| self.cache.claim(&(self.key.clone(), p.0)).map(|f| (*p, f))).collect();
            let (Some(first), Some(last)) = (claimed.first(), claimed.last()) else { return };
            let (start, end) = (first.0 .0, (last.0 .0 + last.0 .1).min(*end).max(*start));
            let data = self.store.get_range(&self.key, start, end - start);
            for ((o, l), flight) in &claimed {
                let key = (self.key.clone(), *o);
                let result = match &data {
                    Ok(data) => {
                        let at = (o - start) as usize;
                        Ok(Arc::new(data[at..at + *l as usize].to_vec()))
                    }
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                };
                self.cache.complete(&key, flight, &result);
            }
        };
        if spans.len() == 1 {
            fetch(&spans[0]);
            return;
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..spans.len().min(8) {
                scope.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(span) = spans.get(i) else { break };
                    fetch(span);
                });
            }
        });
    }
}

/// Columnar state shared by all sessions of a database.
pub(crate) struct ColumnarState {
    pub(crate) config: Mutex<ColumnarConfig>,
    chunks: Mutex<Arc<ChunkCache>>,
    /// Opened (and projected) segments by object key and layout.
    segments: Mutex<SegmentLru>,
    /// Decoded delta blocks by (table, block id). Block ids are never
    /// reused, so entries never go stale.
    blocks: Mutex<Lru<(u32, u64), Arc<cs::Batch>>>,
    /// Replaced segment objects waiting for old snapshots to end, with
    /// the timestamp of the commit that replaced them.
    retired: Mutex<Vec<(u64, String)>>,
    retired_count: std::sync::atomic::AtomicUsize,
    /// Segment objects written by transactions that have not ended yet.
    pending: Mutex<HashMap<String, usize>>,
    pub(crate) compactor: Compactor,
    /// Segments live in a remote store: announced reads are fetched
    /// ahead in parallel.
    remote: AtomicBool,
}

impl Default for ColumnarState {
    fn default() -> Self {
        ColumnarState::new(ColumnarConfig::default())
    }
}

impl ColumnarState {
    pub(crate) fn new(config: ColumnarConfig) -> ColumnarState {
        let bytes = config.cache_bytes;
        ColumnarState {
            chunks: Mutex::new(Arc::new(ChunkCache::new(bytes))),
            segments: Mutex::new(Lru::new(4096)),
            blocks: Mutex::new(Lru::new((bytes / 4).max(16 << 20))),
            retired: Mutex::new(Vec::new()),
            retired_count: Default::default(),
            pending: Mutex::new(HashMap::new()),
            compactor: Compactor::default(),
            remote: AtomicBool::new(false),
            config: Mutex::new(config),
        }
    }

    /// Applies new settings; caches start empty.
    pub(crate) fn configure(&self, config: ColumnarConfig) {
        let bytes = config.cache_bytes;
        *self.chunks.lock() = Arc::new(ChunkCache::new(bytes));
        *self.blocks.lock() = Lru::new((bytes / 4).max(16 << 20));
        *self.config.lock() = config;
    }

    /// Declares whether segments are in a remote store (where reads
    /// are fetched ahead and in parallel) or local.
    pub(crate) fn set_remote(&self, remote: bool) {
        self.remote.store(remote, Ordering::Relaxed);
    }

    pub(crate) fn config(&self) -> ColumnarConfig {
        self.config.lock().clone()
    }

    /// Hits and misses of the chunk cache since the database was opened.
    pub(crate) fn cache_counters(&self) -> (u64, u64) {
        let c = self.chunks.lock().clone();
        (c.hits.load(Ordering::Relaxed), c.misses.load(Ordering::Relaxed))
    }

    /// Queues segment objects that a commit at `ts` replaced. They are
    /// deleted by [`sweep`](ColumnarState::sweep) once no snapshot can
    /// still read a manifest that names them.
    pub(crate) fn retire(&self, ts: u64, keys: Vec<String>) {
        let mut retired = self.retired.lock();
        retired.extend(keys.into_iter().map(|k| (ts, k)));
        self.retired_count.store(retired.len(), Ordering::Release);
    }

    /// True if segment objects wait to be deleted.
    pub(crate) fn has_retired(&self) -> bool {
        self.retired_count.load(Ordering::Acquire) > 0
    }

    /// Deletes retired segment objects that no snapshot at or after
    /// `horizon` can name.
    pub(crate) fn sweep(&self, horizon: u64, store: &dyn squad_object::ObjectStore) {
        let due: Vec<String> = {
            let mut retired = self.retired.lock();
            if retired.is_empty() {
                return;
            }
            let (due, keep): (Vec<_>, Vec<_>) = retired.drain(..).partition(|(ts, _)| *ts <= horizon);
            *retired = keep;
            self.retired_count.store(retired.len(), Ordering::Release);
            due.into_iter().map(|(_, k)| k).collect()
        };
        for key in due {
            let _ = store.delete(&key);
        }
    }

    /// Registers objects about to be written by a transaction.
    fn pending_add(&self, key: &str) {
        *self.pending.lock().entry(key.to_string()).or_default() += 1;
    }

    /// The transaction that wrote these objects has ended.
    pub(crate) fn pending_done(&self, keys: &[String]) {
        let mut pending = self.pending.lock();
        for k in keys {
            if let Some(n) = pending.get_mut(k) {
                *n -= 1;
                if *n == 0 {
                    pending.remove(k);
                }
            }
        }
    }

    fn open_segment(
        &self,
        store: &Arc<dyn squad_object::ObjectStore>,
        key: &str,
        meta: &cs::SegmentMeta,
        layout: &Layout,
    ) -> Result<Arc<cs::Segment>> {
        let key: Arc<str> = Arc::from(key);
        // The manifest does not record column types; the segment does.
        let cached = self.segments.lock().get(&(key.clone(), 0));
        let raw = match cached {
            Some(s) => s,
            None => {
                let source = ObjectSource {
                    prefetch_bytes: if self.remote.load(Ordering::Relaxed) {
                        (self.config.lock().cache_bytes / 8).max(1 << 20)
                    } else {
                        0
                    },
                    store: store.clone(),
                    key: key.clone(),
                    len: meta.bytes,
                    cache: self.chunks.lock().clone(),
                };
                let seg = Arc::new(cs::Segment::open(Arc::new(source))?);
                self.segments.lock().insert((key.clone(), 0), seg.clone(), 1);
                seg
            }
        };
        let types: Vec<cs::ColumnType> = raw.schema().columns.iter().map(|f| f.ty).collect();
        match layout.mapping(&meta.columns, &types)? {
            None => Ok(raw),
            Some(map) => {
                let sig = layout.signature | 1;
                let cached = self.segments.lock().get(&(key.clone(), sig));
                if let Some(s) = cached {
                    return Ok(s);
                }
                let projected = Arc::new(raw.project(layout.schema.clone(), &map)?);
                self.segments.lock().insert((key, sig), projected.clone(), 1);
                Ok(projected)
            }
        }
    }
}

// -------------------------------------------------------------------
// Reading
// -------------------------------------------------------------------

fn load_manifest(table: &Table, cx: &ExecCx) -> Result<cs::Manifest> {
    match cx.txn.get(&manifest_key(table.id))? {
        Some(bytes) => Ok(cs::Manifest::from_bytes(&bytes)?),
        None => Ok(cs::Manifest::new(cs::Schema::default(), Vec::new())),
    }
}

/// Opens the segments of the manifest whose statistics do not rule out
/// `prune`, in the table's layout.
fn open_segments(
    table: &Table,
    layout: &Layout,
    manifest: &cs::Manifest,
    prune: Option<&cs::Predicate>,
    cx: &ExecCx,
) -> Result<Vec<Arc<cs::Segment>>> {
    let store = cx.shared.objects();
    let mut out = Vec::new();
    for s in &manifest.segments {
        if let Some(p) = prune {
            let stats = layout.segment_stats(s);
            if !p.may_match(&|c| stats.get(c).and_then(Option::as_ref)) {
                continue;
            }
        }
        match cx.shared.columnar.open_segment(&store, &object_key(table.id, &s.id), s, layout) {
            Ok(seg) => out.push(seg),
            Err(e) => return Err(replica_conflict(e, cx)),
        }
    }
    Ok(out)
}

/// A replica reads a manifest the writer has moved past: the writer
/// deletes a segment as soon as its own state stops naming it. The
/// statement can be repeated once the replica caught up.
fn replica_conflict(e: Error, cx: &ExecCx) -> Error {
    if e.code() == sqlstate::DATA_CORRUPTED && cx.shared.in_recovery() {
        Error::new(
            sqlstate::SERIALIZATION_FAILURE,
            format!("canceling statement due to conflict with recovery: {}", e.message()),
        )
    } else {
        e
    }
}

fn delta_range(table_id: u32, lo_tag: u8, hi_tag: u8) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let mut lo = object_prefix(table_id).to_vec();
    lo.push(lo_tag);
    let mut hi = object_prefix(table_id).to_vec();
    hi.push(hi_tag);
    (Bound::Included(lo), Bound::Excluded(hi))
}

/// One part of the delta.
struct DeltaPart {
    /// `None` for rows of the old one-row-per-key format.
    block: Option<u64>,
    /// Row keys of such rows (to delete them).
    row_keys: Vec<Vec<u8>>,
    batch: Arc<cs::Batch>,
}

/// Reads the delta: rows stored one per key by older versions, then the
/// blocks, oldest first, until `max_rows` rows are collected.
fn read_delta(table: &Table, layout: &Layout, max_rows: usize, cx: &mut ExecCx) -> Result<Vec<DeltaPart>> {
    let mut parts = Vec::new();
    let mut rows = 0usize;
    // Rows of the old format: keys `prefix 0|1 …`.
    let (lo, hi) = delta_range(table.id, 0, BLOCK_TAG);
    let mut cursor = Cursor::new(cx.txn, lo, hi, false).with_chunk(64);
    let mut legacy = Vec::new();
    let mut keys = Vec::new();
    while let Some((k, v)) = cursor.next(cx.txn)? {
        legacy.push(crate::exec::decode_table_row(table, &v)?);
        keys.push(k);
    }
    if !legacy.is_empty() {
        rows += legacy.len();
        let batch = rows_to_batch(table, layout.schema.clone(), &legacy, cx)?;
        parts.push(DeltaPart { block: None, row_keys: keys, batch: Arc::new(batch) });
    }
    let (lo, hi) = delta_range(table.id, BLOCK_TAG, BLOCK_TAG + 1);
    let mut cursor = Cursor::new(cx.txn, lo, hi, false).with_chunk(16);
    while rows < max_rows {
        let Some((k, v)) = cursor.next(cx.txn)? else { break };
        let id = block_id(&k).ok_or_else(|| Error::new(sqlstate::DATA_CORRUPTED, "malformed delta block key"))?;
        let cache_key = (table.id, id);
        let cached = cx.shared.columnar.blocks.lock().get(&cache_key);
        let raw = match cached {
            Some(b) => b,
            None => {
                let b = Arc::new(cs::decode_batch(&v)?);
                cx.shared.columnar.blocks.lock().insert(cache_key, b.clone(), batch_bytes(&b));
                b
            }
        };
        rows += raw.num_rows();
        parts.push(DeltaPart { block: Some(id), row_keys: Vec::new(), batch: layout.adopt(&raw)? });
    }
    Ok(parts)
}

/// Positions of the stored columns: table column index → segment column.
fn segment_positions(table: &Table) -> Vec<Option<usize>> {
    let mut next = 0;
    table
        .columns
        .iter()
        .map(|c| {
            if c.hidden {
                None
            } else {
                next += 1;
                Some(next - 1)
            }
        })
        .collect()
}

/// A plain reference to a table column, possibly under a widening
/// integer cast.
fn column_of(e: &Expr) -> Option<usize> {
    match e {
        Expr::Column(i) => Some(*i),
        Expr::Cast(inner, Type::Int4 | Type::Int8) => match **inner {
            Expr::Column(i) => Some(i),
            _ => None,
        },
        _ => None,
    }
}

/// `jsonb_column ->> 'constant key'`: the column and the key.
fn json_field<'e>(e: &'e Expr, table: &Table, pos: &[Option<usize>]) -> Option<(usize, &'e str)> {
    match e {
        Expr::Call(f, args) if &*f.name == "->>" && args.len() == 2 => match (&args[0], &args[1]) {
            (Expr::Column(i), Expr::Const(Value::Text(key))) if kind(&table.columns[*i]) == Kind::Json => {
                Some((pos[*i]?, key.as_str()))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Translates a filter over the table row into a predicate over segment
/// columns. `None` means the filter (or part of it) has no exact
/// equivalent; the caller then falls back to evaluating it row by row.
fn predicate(e: &Expr, table: &Table, pos: &[Option<usize>], cx: &mut ExecCx) -> Result<Option<cs::Predicate>> {
    let list = |items: &[Expr], cx: &mut ExecCx| -> Result<Option<Vec<cs::Predicate>>> {
        let mut out = Vec::with_capacity(items.len());
        for i in items {
            match predicate(i, table, pos, cx)? {
                Some(p) => out.push(p),
                None => return Ok(None),
            }
        }
        Ok(Some(out))
    };
    // A constant side of a comparison: evaluated now, it may use parameters.
    let constant = |e: &Expr, col: &Column, cx: &mut ExecCx| -> Result<Option<cs::Datum>> {
        if e.uses_row() || e.is_volatile() {
            return Ok(None);
        }
        let v = e.eval(&[], cx)?;
        // Only values of the column's own type family compare the same
        // way in both engines.
        let same_family = match (&col.ty, &v) {
            (_, Value::Null) => true,
            (Type::Int2 | Type::Int4 | Type::Int8 | Type::Oid, v) => v.as_i64().is_some(),
            (Type::Date, Value::Date(_)) | (Type::Time, Value::Time(_)) => true,
            (Type::Timestamp, Value::Timestamp(_)) | (Type::TimestampTz, Value::TimestampTz(_)) => true,
            (Type::Float4 | Type::Float8, Value::Float4(_) | Value::Float8(_)) => true,
            (Type::Numeric, Value::Numeric(_)) => true,
            (Type::Text | Type::Char, Value::Text(_)) => true,
            (Type::Bool, Value::Bool(_)) | (Type::Uuid, Value::Uuid(_)) | (Type::Bytea, Value::Bytea(_)) => true,
            _ => false,
        };
        Ok(if same_family { to_datum(&v, col, cx) } else { None })
    };
    // The text constant compared with a JSON field.
    let text_constant = |e: &Expr, cx: &mut ExecCx| -> Result<Option<Option<Vec<u8>>>> {
        if e.uses_row() || e.is_volatile() {
            return Ok(None);
        }
        Ok(match e.eval(&[], cx)? {
            Value::Null => Some(None),
            Value::Text(t) => Some(Some(t.into_bytes())),
            _ => None,
        })
    };
    let target = |e: &Expr| -> Option<(usize, &Column)> {
        let i = column_of(e)?;
        let col = &table.columns[i];
        (!matches!(kind(col), Kind::AsText | Kind::Json)).then_some((pos[i]?, col))
    };
    Ok(match e {
        Expr::Const(Value::Bool(b)) => Some(cs::Predicate::Const(Some(*b))),
        Expr::Const(Value::Null) => Some(cs::Predicate::Const(None)),
        Expr::And(items) => list(items, cx)?.map(cs::Predicate::And),
        Expr::Or(items) => list(items, cx)?.map(cs::Predicate::Or),
        Expr::Not(inner) => predicate(inner, table, pos, cx)?.map(|p| cs::Predicate::Not(Box::new(p))),
        Expr::IsNull(inner, negated) => match json_field(inner, table, pos) {
            Some((column, key)) => Some(cs::Predicate::JsonIsNull { column, key: key.to_string(), negated: *negated }),
            None => target(inner).map(|(column, _)| cs::Predicate::IsNull { column, negated: *negated }),
        },
        Expr::Column(i) if table.columns[*i].ty == Type::Bool => pos[*i].map(cs::Predicate::BoolColumn),
        Expr::InList(inner, items, negated) => {
            if let Some((column, key)) = json_field(inner, table, pos) {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    match text_constant(item, cx)? {
                        Some(Some(v)) => values.push(v),
                        _ => return Ok(None),
                    }
                }
                let p = cs::Predicate::JsonIn { column, key: key.to_string(), values };
                return Ok(Some(if *negated { cs::Predicate::Not(Box::new(p)) } else { p }));
            }
            let Some((column, col)) = target(inner) else { return Ok(None) };
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                match constant(item, col, cx)? {
                    Some(cs::Datum::Null) | None => return Ok(None),
                    Some(d) => values.push(d),
                }
            }
            let p = cs::Predicate::In { column, values };
            Some(if *negated { cs::Predicate::Not(Box::new(p)) } else { p })
        }
        Expr::Call(f, args) if args.len() == 2 => {
            let op = match &*f.name {
                "=" => Some(cs::CmpOp::Eq),
                "<>" => Some(cs::CmpOp::Ne),
                "<" => Some(cs::CmpOp::Lt),
                "<=" => Some(cs::CmpOp::Le),
                ">" => Some(cs::CmpOp::Gt),
                ">=" => Some(cs::CmpOp::Ge),
                _ => None,
            };
            match (op, &*f.name) {
                (Some(op), _) => {
                    // `doc ->> 'key' = 'text'` (either side).
                    if matches!(op, cs::CmpOp::Eq | cs::CmpOp::Ne) {
                        for (field_side, const_side) in [(&args[0], &args[1]), (&args[1], &args[0])] {
                            if let Some((column, key)) = json_field(field_side, table, pos) {
                                return Ok(match text_constant(const_side, cx)? {
                                    Some(None) => Some(cs::Predicate::Const(None)),
                                    Some(Some(v)) => {
                                        let p = cs::Predicate::JsonIn { column, key: key.to_string(), values: vec![v] };
                                        Some(if op == cs::CmpOp::Ne { cs::Predicate::Not(Box::new(p)) } else { p })
                                    }
                                    None => None,
                                });
                            }
                        }
                    }
                    let flipped = match op {
                        cs::CmpOp::Lt => cs::CmpOp::Gt,
                        cs::CmpOp::Le => cs::CmpOp::Ge,
                        cs::CmpOp::Gt => cs::CmpOp::Lt,
                        cs::CmpOp::Ge => cs::CmpOp::Le,
                        other => other,
                    };
                    for (col_side, const_side, op) in [(&args[0], &args[1], op), (&args[1], &args[0], flipped)] {
                        if let Some((column, col)) = target(col_side) {
                            return Ok(match constant(const_side, col, cx)? {
                                Some(cs::Datum::Null) => Some(cs::Predicate::Const(None)),
                                Some(value) => Some(cs::Predicate::Cmp { column, op, value }),
                                None => None,
                            });
                        }
                    }
                    None
                }
                (None, name @ ("~~" | "~~*" | "!~~" | "!~~*")) => {
                    let Some((column, col)) = target(&args[0]) else { return Ok(None) };
                    if kind(col) != Kind::Text {
                        return Ok(None);
                    }
                    match constant(&args[1], col, cx)? {
                        Some(cs::Datum::Null) => Some(cs::Predicate::Const(None)),
                        Some(d) => {
                            let pattern = d.as_str().unwrap_or_default().to_string();
                            // A backslash escape has its own rules; leave
                            // such patterns to the row-by-row path.
                            if pattern.contains('\\') {
                                return Ok(None);
                            }
                            let p = cs::Predicate::Like { column, pattern, case_insensitive: name.ends_with('*') };
                            Some(if name.starts_with('!') { cs::Predicate::Not(Box::new(p)) } else { p })
                        }
                        None => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    })
}

/// The parts of a conjunction that could be translated, for pruning only.
fn pruning_predicate(e: &Expr, table: &Table, pos: &[Option<usize>], cx: &mut ExecCx) -> Result<Option<cs::Predicate>> {
    if let Expr::And(items) = e {
        let mut out = Vec::new();
        for i in items {
            if let Some(p) = pruning_predicate(i, table, pos, cx)? {
                out.push(p);
            }
        }
        return Ok(if out.is_empty() { None } else { Some(cs::Predicate::And(out)) });
    }
    predicate(e, table, pos, cx)
}

/// What a scan reads: delta batches, then segments.
enum Part {
    Delta(Arc<cs::Batch>),
    Segment(Arc<cs::Segment>),
}

/// Streams the rows of a columnar table — delta and segments — through
/// the ordinary executor, skipping data whose statistics rule out the
/// filter.
pub(crate) struct ColumnarScan<'p> {
    plan: &'p ScanPlan,
    layout: Arc<Layout>,
    parts: Vec<Part>,
    prune: Option<cs::Predicate>,
    /// The whole filter as a predicate of the column engine, if it has an
    /// exact equivalent: the engine then filters vectorized, using bloom
    /// filters, and only matching rows are converted.
    exact: Option<cs::Predicate>,
    projection: Vec<usize>,
    /// (part, row group) to read next.
    next: (usize, usize),
    pending: std::vec::IntoIter<Row>,
}

impl<'p> ColumnarScan<'p> {
    pub(crate) fn open(plan: &'p ScanPlan, cx: &mut ExecCx) -> Result<ColumnarScan<'p>> {
        let table = &plan.table;
        let layout = Layout::of(table)?;
        let manifest = load_manifest(table, cx)?;
        let pos = segment_positions(table);
        let (prune, exact) = match &plan.filter {
            Some(f) => (pruning_predicate(f, table, &pos, cx)?, predicate(f, table, &pos, cx)?),
            None => (None, None),
        };
        let mut parts: Vec<Part> =
            read_delta(table, &layout, usize::MAX, cx)?.into_iter().map(|d| Part::Delta(d.batch)).collect();
        // Whole segments that cannot match are not even opened.
        parts.extend(open_segments(table, &layout, &manifest, prune.as_ref(), cx)?.into_iter().map(Part::Segment));
        let projection = (0..layout.schema.len()).collect();
        Ok(ColumnarScan {
            plan,
            layout,
            parts,
            prune,
            exact,
            projection,
            next: (0, 0),
            pending: Vec::new().into_iter(),
        })
    }
}

impl Node for ColumnarScan<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        let table = &self.plan.table;
        loop {
            for row in self.pending.by_ref() {
                match &self.plan.filter {
                    Some(f) if !f.test(&row, cx)? => continue,
                    _ => return Ok(Some(row)),
                }
            }
            let (s, g) = self.next;
            let Some(part) = self.parts.get(s) else { return Ok(None) };
            if let Some(exact) = &self.exact {
                // One part at a time through the column engine.
                self.next = (s + 1, 0);
                let mut source = cs::TableSource::new(self.layout.schema.clone());
                match part {
                    Part::Delta(b) => source.delta.push(b.clone()),
                    Part::Segment(seg) => source.segments.push(seg.clone()),
                }
                let request = cs::ScanRequest {
                    projection: self.projection.clone(),
                    predicate: Some(exact.clone()),
                    limit: None,
                };
                let mut rows = Vec::new();
                for batch in cs::scan(&source, &request)? {
                    batch_rows(table, &batch.map_err(|e| replica_conflict(e.into(), cx))?, &mut rows, cx)?;
                }
                self.pending = rows.into_iter();
                continue;
            }
            let segment = match part {
                Part::Delta(b) => {
                    self.next = (s + 1, 0);
                    let mut rows = Vec::new();
                    batch_rows(table, b, &mut rows, cx)?;
                    self.pending = rows.into_iter();
                    continue;
                }
                Part::Segment(seg) => seg,
            };
            if g >= segment.num_row_groups() {
                self.next = (s + 1, 0);
                continue;
            }
            self.next = (s, g + 1);
            let meta = segment.row_group(g);
            if self.prune.as_ref().is_some_and(|p| !p.may_match(&|c| meta.columns.get(c).map(|m| &m.stats))) {
                continue;
            }
            let ranges = segment.chunk_ranges(g, &self.projection, false);
            segment.source().prefetch(&ranges);
            let batch = segment.read_row_group(g, &self.projection).map_err(|e| replica_conflict(e.into(), cx))?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            batch_rows(table, &batch, &mut rows, cx)?;
            self.pending = rows.into_iter();
        }
    }
}

/// What one requested aggregate turns into for the column engine.
enum Mapped {
    /// Result column `i` of the request, converted with the given rule.
    Direct(usize),
    /// Average from a sum and a count column.
    Avg(usize, usize),
}

/// Runs `SELECT groups, aggs FROM table WHERE filter GROUP BY groups` in
/// the column engine. Returns `None` if some part cannot be expressed
/// there; the caller then runs the ordinary plan.
pub(crate) fn aggregate(
    plan: &ScanPlan,
    groups: &[Expr],
    aggs: &[AggCall],
    cx: &mut ExecCx,
) -> Result<Option<Vec<Row>>> {
    let table = &plan.table;
    let pos = segment_positions(table);
    let filter = match &plan.filter {
        Some(f) => match predicate(f, table, &pos, cx)? {
            Some(p) => Some(p),
            None => return Ok(None),
        },
        None => None,
    };

    // Group keys: plain columns, date_trunc over a timestamp column, or a
    // field of a jsonb column.
    enum Key<'a> {
        Column(&'a Column),
        Bucket(&'a Column),
        JsonField,
    }
    let mut keys = Vec::with_capacity(groups.len());
    let mut key_info = Vec::with_capacity(groups.len());
    for g in groups {
        if let Some((column, key)) = json_field(g, table, &pos) {
            keys.push(cs::GroupKey::JsonField { column, key: key.to_string() });
            key_info.push(Key::JsonField);
            continue;
        }
        match g {
            Expr::Column(i) => {
                let col = &table.columns[*i];
                let (Some(p), true) = (pos[*i], !matches!(kind(col), Kind::AsText | Kind::Json)) else {
                    return Ok(None);
                };
                keys.push(cs::GroupKey::Column(p));
                key_info.push(Key::Column(col));
            }
            Expr::Call(f, args) if &*f.name == "date_trunc" && args.len() == 2 => {
                let (Expr::Const(Value::Text(unit)), Some(i)) = (&args[0], column_of(&args[1])) else {
                    return Ok(None);
                };
                let col = &table.columns[i];
                // Buckets of timestamptz follow the session time zone;
                // only UTC has fixed boundaries.
                let fixed = match col.ty {
                    Type::Timestamp => true,
                    Type::TimestampTz => cx.fn_ctx.tz == chrono_tz::UTC,
                    _ => false,
                };
                let unit = match unit.to_ascii_lowercase().as_str() {
                    "second" => cs::TimeUnit::Second,
                    "minute" => cs::TimeUnit::Minute,
                    "hour" => cs::TimeUnit::Hour,
                    "day" => cs::TimeUnit::Day,
                    "week" => cs::TimeUnit::Week,
                    "month" => cs::TimeUnit::Month,
                    "quarter" => cs::TimeUnit::Quarter,
                    "year" => cs::TimeUnit::Year,
                    _ => return Ok(None),
                };
                let (Some(p), true) = (pos[i], fixed) else { return Ok(None) };
                keys.push(cs::GroupKey::TimeBucket {
                    column: p,
                    unit,
                    utc_offset_micros: 0,
                    epoch_unix_micros: crate::types::PG_EPOCH_UNIX_MICROS,
                });
                key_info.push(Key::Bucket(col));
            }
            _ => return Ok(None),
        }
    }

    let mut requests = Vec::new();
    let mut mapped = Vec::with_capacity(aggs.len());
    for a in aggs {
        if a.filter.is_some() || !a.order_by.is_empty() {
            return Ok(None);
        }
        let column = match a.args.first() {
            Some(e) => match column_of(e).and_then(|i| pos[i].map(|p| (p, &table.columns[i]))) {
                Some(found) => Some(found),
                None => return Ok(None),
            },
            None => None,
        };
        let numeric = |col: &Column| {
            matches!(col.ty, Type::Int2 | Type::Int4 | Type::Int8 | Type::Float4 | Type::Float8)
                || matches!(kind(col), Kind::Decimal(_))
        };
        let plain = |col: &Column| !matches!(kind(col), Kind::AsText | Kind::Json);
        let mut push = |agg: cs::Aggregate| {
            requests.push(agg);
            requests.len() - 1
        };
        let m = match (&a.func, column, a.distinct) {
            (AggFunc::CountStar, _, _) => Mapped::Direct(push(cs::Aggregate::CountStar)),
            (AggFunc::Count, Some((p, _)), false) => Mapped::Direct(push(cs::Aggregate::Count(p))),
            (AggFunc::Count, Some((p, col)), true) if plain(col) => {
                Mapped::Direct(push(cs::Aggregate::CountDistinct(p)))
            }
            (AggFunc::ApproxCountDistinct, Some((p, col)), _) if plain(col) => {
                Mapped::Direct(push(cs::Aggregate::ApproxCountDistinct(p)))
            }
            (AggFunc::ApproxPercentile(q), Some((p, col)), false) if numeric(col) => {
                Mapped::Direct(push(cs::Aggregate::ApproxPercentile { column: p, percentile: *q }))
            }
            (AggFunc::Sum, Some((p, col)), false) if numeric(col) => Mapped::Direct(push(cs::Aggregate::Sum(p))),
            (AggFunc::Avg, Some((p, col)), false) if numeric(col) => {
                Mapped::Avg(push(cs::Aggregate::Sum(p)), push(cs::Aggregate::Count(p)))
            }
            (AggFunc::Min, Some((p, col)), _) if plain(col) => Mapped::Direct(push(cs::Aggregate::Min(p))),
            (AggFunc::Max, Some((p, col)), _) if plain(col) => Mapped::Direct(push(cs::Aggregate::Max(p))),
            _ => return Ok(None),
        };
        mapped.push((m, a, column.map(|c| c.1)));
    }

    let layout = Layout::of(table)?;
    let manifest = load_manifest(table, cx)?;
    let mut source = cs::TableSource::new(layout.schema.clone());
    // Segments the manifest's statistics rule out are not opened.
    source.segments = open_segments(table, &layout, &manifest, filter.as_ref(), cx)?;
    source.delta = read_delta(table, &layout, usize::MAX, cx)?.into_iter().map(|d| d.batch).collect();
    let request = cs::AggregateRequest {
        predicate: filter,
        group_by: keys,
        aggregates: requests,
        // Threads cost more than they save on small inputs.
        parallelism: std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min((source.num_rows() / 200_000) as usize + 1),
        ..Default::default()
    };
    let result = cs::aggregate(&source, &request).map_err(|e| replica_conflict(e.into(), cx))?;

    let nkeys = groups.len();
    let mut out = Vec::with_capacity(result.num_rows());
    for r in 0..result.num_rows() {
        let data = result.row(r);
        let mut row = Vec::with_capacity(nkeys + aggs.len());
        for (k, info) in key_info.iter().enumerate() {
            row.push(match info {
                Key::Column(c) | Key::Bucket(c) => from_datum(data[k].clone(), c, cx)?,
                Key::JsonField => match &data[k] {
                    cs::Datum::Bytes(b) => Value::Text(String::from_utf8_lossy(b).into_owned()),
                    _ => Value::Null,
                },
            });
        }
        let int_of = |d: &cs::Datum| -> Option<i128> {
            match d {
                cs::Datum::Int(i) => Some(*i as i128),
                cs::Datum::Decimal(m) => Some(*m),
                _ => None,
            }
        };
        // A sum arrives as a decimal at the column's scale (0 for integers).
        let sum_value = |d: &cs::Datum, col: &Column, ret: &Type| -> Result<Value> {
            let scale = match kind(col) {
                Kind::Decimal(s) => s as u32,
                _ => 0,
            };
            Ok(match (d, ret) {
                (cs::Datum::Null, _) => Value::Null,
                (cs::Datum::Float(f), Type::Float4) => Value::Float4(*f as f32),
                (cs::Datum::Float(f), _) => Value::Float8(*f),
                (d, Type::Int8) => Value::Int8(
                    int_of(d).and_then(|i| i64::try_from(i).ok()).ok_or_else(|| Error::out_of_range("bigint"))?,
                ),
                (d, _) => Value::Numeric(
                    int_of(d)
                        .and_then(|m| Decimal::try_from_i128_with_scale(m, scale).ok())
                        .ok_or_else(|| Error::out_of_range("numeric"))?,
                ),
            })
        };
        for (m, call, col) in &mapped {
            let v = match m {
                Mapped::Direct(i) => {
                    let d = &data[nkeys + i];
                    match (&call.func, col) {
                        (AggFunc::Sum, Some(col)) => sum_value(d, col, &call.ret)?,
                        (AggFunc::Min | AggFunc::Max, Some(col)) => from_datum(d.clone(), col, cx)?,
                        (AggFunc::ApproxPercentile(_), _) => match d {
                            cs::Datum::Float(f) => Value::Float8(*f),
                            _ => Value::Null,
                        },
                        _ => match d {
                            cs::Datum::Int(i) => Value::Int8(*i),
                            cs::Datum::Null => Value::Null,
                            _ => return Err(Error::internal("unexpected aggregate result")),
                        },
                    }
                }
                Mapped::Avg(sum, count) => {
                    let n = match &data[nkeys + count] {
                        cs::Datum::Int(n) => *n,
                        _ => 0,
                    };
                    let col = col.ok_or_else(|| Error::internal("average without column"))?;
                    if n == 0 {
                        Value::Null
                    } else {
                        match sum_value(&data[nkeys + sum], col, &Type::Numeric)? {
                            Value::Float8(f) => Value::Float8(f / n as f64),
                            Value::Null => Value::Null,
                            total => numeric_div()(&cx.fn_ctx, &[total, Value::Numeric(Decimal::from(n))])?,
                        }
                    }
                }
            };
            row.push(v);
        }
        out.push(row);
    }
    Ok(Some(out))
}

// -------------------------------------------------------------------
// Writing
// -------------------------------------------------------------------

fn writer_options(table: &Table) -> cs::WriterOptions {
    let pos = segment_positions(table);
    let sort_key =
        table.columnar.as_ref().map(|c| c.order_by.iter().filter_map(|i| pos[*i]).collect()).unwrap_or_default();
    cs::WriterOptions { sort_key, ..Default::default() }
}

/// Stores segment bytes as a new object and adds it to the manifest.
fn store_segment(
    table: &Table,
    layout: &Layout,
    manifest: &mut cs::Manifest,
    bytes: Vec<u8>,
    cx: &mut ExecCx,
) -> Result<()> {
    let id = new_segment_id();
    let key = object_key(table.id, &id);
    // The object is durable before the manifest that names it commits. If
    // the transaction fails, the object is an unreferenced leftover that
    // the transaction's end or `VACUUM` deletes.
    cx.shared.columnar.pending_add(&key);
    cx.written_objects.push(key.clone());
    cx.shared.objects().put(&key, &bytes)?;
    let segment = cs::Segment::open(Arc::new(bytes))?;
    manifest.segments.push(cs::SegmentMeta::describe(id, &segment).with_columns(layout.ids.clone()));
    Ok(())
}

/// Writes rows as one new segment and adds it to the manifest.
fn add_segment(
    table: &Table,
    layout: &Layout,
    manifest: &mut cs::Manifest,
    batches: &[&cs::Batch],
    cx: &mut ExecCx,
) -> Result<()> {
    let mut writer = cs::SegmentWriter::new(layout.schema.clone(), writer_options(table));
    for b in batches {
        writer.write_batch(b)?;
    }
    store_segment(table, layout, manifest, writer.finish()?, cx)
}

fn save_manifest(table: &Table, manifest: &cs::Manifest, cx: &mut ExecCx) -> Result<()> {
    cx.txn.put_blind(&manifest_key(table.id), &manifest.to_bytes())?;
    Ok(())
}

/// Serializes changes to a table's segments and returns the manifest as
/// the previous holder of the lock left it. Statements that also read
/// the delta must see it as of the same moment: in Read Committed the
/// snapshot moves forward to after the lock was granted; in the stricter
/// levels a change since the transaction's snapshot is a serialization
/// failure.
fn lock_segments(table: &Table, cx: &mut ExecCx) -> Result<cs::Manifest> {
    let wait = WaitPolicy::Wait(cx.session.lock_timeout);
    let key = manifest_key(table.id);
    cx.txn.lock(&key, LockMode::Exclusive, wait)?;
    let current = cx.txn.get_current(&key)?;
    if cx.txn.isolation() == squad_kv::Isolation::ReadCommitted {
        cx.txn.refresh_snapshot();
    } else if cx.txn.get(&key)? != current {
        return Err(Error::new(
            sqlstate::SERIALIZATION_FAILURE,
            "could not serialize access due to concurrent update of a columnar table",
        ));
    }
    match current {
        Some(bytes) => Ok(cs::Manifest::from_bytes(&bytes)?),
        None => Ok(cs::Manifest::new(cs::Schema::default(), Vec::new())),
    }
}

/// Stores rows (shaped like the table) in the delta as blocks.
pub(crate) fn append_rows(table: &Table, rows: &[Row], cx: &mut ExecCx) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let layout = Layout::of(table)?;
    let schema = layout.block_schema();
    for chunk in rows.chunks(BLOCK_ROWS) {
        let batch = rows_to_batch(table, schema.clone(), chunk, cx)?;
        let id = cx.shared.next_rowid(table.id)? as u64;
        cx.txn.put_blind(&block_key(table.id, id), &cs::encode_batch(&batch))?;
        let entry = cx.columnar_inserted.entry(table.id).or_default();
        entry.0 += chunk.len() as u64;
        entry.1 += 1;
    }
    Ok(())
}

/// Stores the rows of a large insert as new segments. The segments become
/// visible when the inserting transaction commits its manifest change,
/// so the insert is as atomic as any other.
pub(crate) fn bulk_insert(table: &Table, rows: &[Row], cx: &mut ExecCx) -> Result<()> {
    let layout = Layout::of(table)?;
    let mut manifest = lock_segments(table, cx)?;
    for chunk in rows.chunks(BULK_SEGMENT_ROWS) {
        let batch = rows_to_batch(table, layout.schema.clone(), chunk, cx)?;
        add_segment(table, &layout, &mut manifest, &[&batch], cx)?;
    }
    save_manifest(table, &manifest, cx)
}

/// Moves delta blocks (up to a bounded number of rows) into a new
/// segment. Returns the number of rows moved. Must run in a Read
/// Committed transaction of its own that holds the table's shared lock.
pub(crate) fn compact_delta(table: &Arc<Table>, cx: &mut ExecCx) -> Result<u64> {
    let layout = Layout::of(table)?;
    let mut manifest = lock_segments(table, cx)?;
    // With the lock held, the refreshed snapshot shows exactly the delta
    // that no earlier compaction has moved.
    let parts = read_delta(table, &layout, COMPACT_MAX_ROWS, cx)?;
    let rows: usize = parts.iter().map(|p| p.batch.num_rows()).sum();
    if rows == 0 {
        return Ok(0);
    }
    let batches: Vec<&cs::Batch> = parts.iter().map(|p| &*p.batch).collect();
    add_segment(table, &layout, &mut manifest, &batches, cx)?;
    for p in &parts {
        match p.block {
            Some(id) => cx.txn.delete_blind(&block_key(table.id, id))?,
            None => {
                for k in &p.row_keys {
                    cx.txn.delete_blind(k)?;
                }
            }
        }
    }
    save_manifest(table, &manifest, cx)?;
    Ok(rows as u64)
}

/// Merges small segments into one, streaming. Returns the number of
/// segments merged (0 if there was nothing worth merging).
pub(crate) fn merge_segments(table: &Arc<Table>, cx: &mut ExecCx) -> Result<usize> {
    let layout = Layout::of(table)?;
    let mut manifest = lock_segments(table, cx)?;
    let max_bytes = cx.shared.columnar.config().merge_max_bytes;
    let small: Vec<cs::SegmentMeta> = manifest
        .merge_candidates(TARGET_SEGMENT_ROWS, max_bytes, MERGE_MAX_SEGMENTS, MERGE_MIN_SEGMENTS)
        .into_iter()
        .cloned()
        .collect();
    if small.is_empty() {
        return Ok(0);
    }
    let picked = cs::Manifest { segments: small.clone(), ..Default::default() };
    let inputs = open_segments(table, &layout, &picked, None, cx)?;
    let merged = cs::merge(&inputs, writer_options(table))?;
    manifest.segments.retain(|s| !small.iter().any(|m| m.id == s.id));
    store_segment(table, &layout, &mut manifest, merged, cx)?;
    cx.dropped_objects.extend(small.iter().map(|s| object_key(table.id, &s.id)));
    save_manifest(table, &manifest, cx)?;
    Ok(small.len())
}

/// `DELETE` on a columnar table: matching delta rows are deleted (a block
/// holding some is replaced by one without them), and every segment that
/// holds matching rows is rewritten without them, one row group at a time.
pub(crate) fn delete(plan: &ScanPlan, cx: &mut ExecCx) -> Result<u64> {
    let table = &plan.table;
    let layout = Layout::of(table)?;
    let mut manifest = lock_segments(table, cx)?;
    let pos = segment_positions(table);
    let (prune, exact) = match &plan.filter {
        Some(f) => (pruning_predicate(f, table, &pos, cx)?, predicate(f, table, &pos, cx)?),
        None => (None, None),
    };
    // Which rows of a batch match: through the column engine where the
    // filter allows, else row by row.
    let matching = |batch: &cs::Batch, cx: &mut ExecCx| -> Result<Vec<bool>> {
        let n = batch.num_rows();
        match (&plan.filter, &exact) {
            (None, _) => Ok(vec![true; n]),
            (Some(_), Some(p)) => {
                let mut source = cs::TableSource::new(layout.schema.clone());
                let mut indexed = batch.clone();
                // Row numbers ride along as an extra column.
                let mut fields = layout.schema.columns.clone();
                fields.push(cs::Field::new("#row", cs::ColumnType::Int64, false));
                indexed.columns.push(cs::Column::new(cs::ColumnData::Int64((0..n as i64).collect()), None));
                indexed.schema = Arc::new(cs::Schema::new(fields));
                source.schema = indexed.schema.clone();
                source.delta.push(Arc::new(indexed));
                let request =
                    cs::ScanRequest { projection: vec![layout.schema.len()], predicate: Some(p.clone()), limit: None };
                let mut out = vec![false; n];
                for b in cs::scan(&source, &request)? {
                    if let cs::ColumnData::Int64(v) = &b?.columns[0].data {
                        v.iter().for_each(|i| out[*i as usize] = true);
                    }
                }
                Ok(out)
            }
            (Some(f), None) => {
                let mut rows = Vec::new();
                batch_rows(table, batch, &mut rows, cx)?;
                rows.iter().map(|r| f.test(r, cx)).collect()
            }
        }
    };
    let mut count = 0u64;
    let block_schema = layout.block_schema();
    for part in read_delta(table, &layout, usize::MAX, cx)? {
        let hits = matching(&part.batch, cx)?;
        let n = hits.iter().filter(|h| **h).count();
        if n == 0 {
            continue;
        }
        count += n as u64;
        match part.block {
            None => {
                for (k, hit) in part.row_keys.iter().zip(&hits) {
                    if *hit {
                        cx.txn.delete_blind(k)?;
                    }
                }
            }
            Some(id) => {
                cx.txn.delete_blind(&block_key(table.id, id))?;
                let keep: Vec<u32> = (0..hits.len() as u32).filter(|i| !hits[*i as usize]).collect();
                if !keep.is_empty() {
                    let mut rest = part.batch.take(&keep);
                    rest.schema = block_schema.clone();
                    let new_id = cx.shared.next_rowid(table.id)? as u64;
                    cx.txn.put_blind(&block_key(table.id, new_id), &cs::encode_batch(&rest))?;
                }
            }
        }
    }
    let candidates: Vec<cs::SegmentMeta> = manifest
        .segments
        .iter()
        .filter(|s| {
            let stats = layout.segment_stats(s);
            prune.as_ref().is_none_or(|p| p.may_match(&|c| stats.get(c).and_then(Option::as_ref)))
        })
        .cloned()
        .collect();
    let mut changed = false;
    let all: Vec<usize> = (0..layout.schema.len()).collect();
    for meta in candidates {
        let one = cs::Manifest { segments: vec![meta.clone()], ..Default::default() };
        let Some(segment) = open_segments(table, &layout, &one, None, cx)?.pop() else { continue };
        let mut writer = cs::SegmentStreamWriter::new(layout.schema.clone(), writer_options(table))?;
        let mut removed = 0u64;
        for g in 0..segment.num_row_groups() {
            let rg = segment.row_group(g);
            if prune.as_ref().is_some_and(|p| !p.may_match(&|c| rg.columns.get(c).map(|m| &m.stats))) {
                writer.write_batch(&segment.read_row_group(g, &all)?)?;
                continue;
            }
            segment.source().prefetch(&segment.chunk_ranges(g, &all, false));
            let batch = segment.read_row_group(g, &all)?;
            let hits = matching(&batch, cx)?;
            let keep: Vec<u32> = (0..hits.len() as u32).filter(|i| !hits[*i as usize]).collect();
            removed += (hits.len() - keep.len()) as u64;
            if keep.len() == hits.len() {
                writer.write_batch(&batch)?;
            } else if !keep.is_empty() {
                writer.write_batch(&batch.take(&keep))?;
            }
        }
        if removed == 0 {
            continue;
        }
        count += removed;
        changed = true;
        manifest.segments.retain(|s| s.id != meta.id);
        if writer.num_rows() > 0 {
            store_segment(table, &layout, &mut manifest, writer.finish()?, cx)?;
        }
        cx.dropped_objects.push(object_key(table.id, &meta.id));
    }
    if changed {
        save_manifest(table, &manifest, cx)?;
    }
    Ok(count)
}

/// Removes all segments of a table (TRUNCATE, DROP TABLE). The objects
/// are deleted once the transaction has committed.
pub(crate) fn clear(table: &Table, txn: &mut squad_kv::Txn, dropped: &mut Vec<String>) -> Result<()> {
    if let Some(bytes) = txn.get(&manifest_key(table.id))? {
        let manifest = cs::Manifest::from_bytes(&bytes)?;
        dropped.extend(manifest.segments.iter().map(|s| object_key(table.id, &s.id)));
        txn.delete_blind(&manifest_key(table.id))?;
    }
    Ok(())
}

/// Rewrites delta rows of the old one-row-per-key format as a block, so
/// that they no longer depend on the column positions a schema change
/// is about to move. Runs in the altering transaction.
pub(crate) fn convert_legacy_delta(table: &Table, cx: &mut ExecCx) -> Result<()> {
    let layout = Layout::of(table)?;
    let (lo, hi) = delta_range(table.id, 0, BLOCK_TAG);
    let mut cursor = Cursor::new(cx.txn, lo, hi, false).with_chunk(64);
    let mut rows = Vec::new();
    let mut keys = Vec::new();
    while let Some((k, v)) = cursor.next(cx.txn)? {
        rows.push(crate::exec::decode_table_row(table, &v)?);
        keys.push(k);
    }
    if rows.is_empty() {
        return Ok(());
    }
    for k in &keys {
        cx.txn.delete_blind(k)?;
    }
    let schema = layout.block_schema();
    for chunk in rows.chunks(BLOCK_ROWS) {
        let batch = rows_to_batch(table, schema.clone(), chunk, cx)?;
        let id = cx.shared.next_rowid(table.id)? as u64;
        cx.txn.put_blind(&block_key(table.id, id), &cs::encode_batch(&batch))?;
    }
    Ok(())
}

/// True if the table holds any row (delta or segments).
pub(crate) fn has_rows(table: &Table, cx: &mut ExecCx) -> Result<bool> {
    if load_manifest(table, cx)?.num_rows() > 0 {
        return Ok(true);
    }
    let (lo, _) = delta_range(table.id, 0, 0);
    let (_, hi) = delta_range(table.id, BLOCK_TAG + 1, BLOCK_TAG + 1);
    let mut cursor = Cursor::new(cx.txn, lo, hi, false).with_chunk(1);
    Ok(cursor.next(cx.txn)?.is_some())
}

/// The text a value of `col` has in the catalog (missing values of added
/// columns).
pub(crate) fn catalog_text(v: &Value) -> Option<String> {
    (!v.is_null()).then(|| format_value(v, &catalog_fn_ctx()))
}

/// Deletes segment objects of the given tables (all tables if `None`)
/// that no manifest names, no transaction is writing and no old snapshot
/// may still read, and that are older than the grace period. Returns the
/// number of objects deleted.
pub(crate) fn remove_orphans(shared: &crate::session::Shared, tables: Option<&[u32]>) -> Result<u64> {
    let store = shared.objects();
    let grace = shared.columnar.config().orphan_grace;
    let state = &shared.columnar;
    // The order matters: list first, then pending writes, then the
    // committed manifests, then retired objects. An object written before
    // the listing is either still pending, or its transaction ended
    // before the pending set was read — committed (then the manifest read
    // afterwards names it, or a later commit replaced it and it is
    // retired) or rolled back (then it is garbage).
    let prefixes: Vec<String> = match tables {
        Some(ids) => ids.iter().map(|id| format!("columnar/{id}/")).collect(),
        None => vec!["columnar/".to_string()],
    };
    let mut listed = Vec::new();
    for p in &prefixes {
        listed.extend(store.list(p)?);
    }
    let pending: HashSet<String> = state.pending.lock().keys().cloned().collect();
    let mut referenced: HashSet<String> = HashSet::new();
    let txn = shared.store.begin(squad_kv::Isolation::ReadCommitted);
    let mut table_ids: HashSet<u32> = HashSet::new();
    for meta in &listed {
        if let Some(id) =
            meta.key.strip_prefix("columnar/").and_then(|r| r.split('/').next()).and_then(|t| t.parse().ok())
        {
            table_ids.insert(id);
        }
    }
    for id in &table_ids {
        if let Some(bytes) = txn.get(&manifest_key(*id))? {
            let manifest = cs::Manifest::from_bytes(&bytes)?;
            referenced.extend(manifest.segments.iter().map(|s| object_key(*id, &s.id)));
        }
    }
    drop(txn);
    let retired: HashSet<String> = state.retired.lock().iter().map(|(_, k)| k.clone()).collect();
    let now = std::time::SystemTime::now();
    let mut removed = 0;
    for meta in listed {
        let key = &meta.key;
        if referenced.contains(key) || pending.contains(key) || retired.contains(key) {
            continue;
        }
        let Some(rest) = key.strip_prefix("columnar/") else { continue };
        let Some((table, file)) = rest.split_once('/') else { continue };
        if table.parse::<u32>().is_err() {
            continue;
        }
        let Some(id) = file.strip_suffix(".seg") else { continue };
        // Objects named by ids with a time are kept during the grace
        // period; older ids carry no time and are as old as the version
        // that wrote them.
        if let Some(created) = segment_created(id) {
            if now.duration_since(created).unwrap_or_default() < grace {
                continue;
            }
        }
        store.delete(key)?;
        removed += 1;
    }
    Ok(removed)
}

// -------------------------------------------------------------------
// Background compaction
// -------------------------------------------------------------------

/// What the delta of a table collected since its last compaction.
#[derive(Debug, Clone, Copy)]
struct DeltaStats {
    rows: u64,
    blocks: u64,
    since: Instant,
}

#[derive(Default)]
struct CompactorState {
    tables: HashMap<u32, DeltaStats>,
    /// Tables a compaction is running for.
    busy: HashSet<u32>,
    /// Compactions finished, for tests and statistics.
    runs: u64,
}

/// The background compaction worker of a database: one thread that
/// compacts a table's delta when it holds `compact_rows` rows or
/// `compact_blocks` blocks or its oldest rows are `compact_age` old, and
/// merges small segments afterwards.
#[derive(Default)]
pub(crate) struct Compactor {
    state: Mutex<CompactorState>,
    wake: Condvar,
    stop: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Signalled whenever a run finishes.
    finished: Condvar,
}

impl Compactor {
    /// Records rows a committed transaction added to tables' deltas.
    pub(crate) fn note_inserted(&self, config: &ColumnarConfig, added: &HashMap<u32, (u64, u64)>) {
        let mut state = self.state.lock();
        let mut wake = false;
        for (table, (rows, blocks)) in added {
            let entry = state.tables.entry(*table).or_insert(DeltaStats { rows: 0, blocks: 0, since: Instant::now() });
            let first = entry.rows == 0 && entry.blocks == 0;
            entry.rows += rows;
            entry.blocks += blocks;
            wake |= first || entry.rows >= config.compact_rows || entry.blocks >= config.compact_blocks;
        }
        if wake {
            self.wake.notify_one();
        }
    }

    /// Forgets what is known about deltas (they are recounted as they
    /// grow); used when a replica becomes the writer.
    pub(crate) fn reset(&self) {
        self.state.lock().tables.clear();
    }

    /// Marks a table as due right away (a delta found at startup).
    pub(crate) fn mark_due(&self, table: u32) {
        let mut state = self.state.lock();
        let since = Instant::now().checked_sub(Duration::from_secs(86_400)).unwrap_or_else(Instant::now);
        state.tables.insert(table, DeltaStats { rows: 1, blocks: 1, since });
        self.wake.notify_one();
    }

    /// Waits until no table is due or busy, or the timeout passes.
    /// Returns true if the compactor is idle.
    pub(crate) fn wait_idle(&self, config: &ColumnarConfig, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        loop {
            let due = state.tables.values().any(|s| s.rows > 0 || s.blocks > 0);
            if !due && state.busy.is_empty() {
                return true;
            }
            let _ = config;
            if self.finished.wait_until(&mut state, deadline).timed_out() {
                return false;
            }
        }
    }

    /// Starts the worker thread for the database behind `shared`.
    pub(crate) fn start(&self, shared: &Arc<crate::session::Shared>) {
        let shared = shared.clone();
        let handle = std::thread::Builder::new()
            .name("squad-compactor".into())
            .spawn(move || run_compactor(shared))
            .expect("spawn compaction thread");
        *self.thread.lock() = Some(handle);
    }

    /// Stops the worker and waits for it. A compaction in progress is
    /// finished first or rolled back; no data is lost either way, rows
    /// not yet compacted stay in the delta.
    pub(crate) fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        {
            let _state = self.state.lock();
            self.wake.notify_all();
        }
        let handle = self.thread.lock().take();
        if let Some(h) = handle {
            if h.thread().id() != std::thread::current().id() {
                let _ = h.join();
            }
        }
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }
}

/// The worker loop. It holds the database open; the database's last
/// handle stops it (see [`Compactor::shutdown`]) before closing.
fn run_compactor(shared: Arc<crate::session::Shared>) {
    let compactor = &shared.columnar.compactor;
    // Deltas left by an earlier run of the process are compacted soon.
    let catalog = shared.catalog.read().clone();
    for t in catalog.tables.values().filter(|t| t.columnar.is_some()) {
        compactor.mark_due(t.id);
    }
    drop(catalog);
    loop {
        if compactor.stopping() {
            return;
        }
        let config = shared.columnar.config();
        // Pick the due tables, or sleep until the oldest delta is due.
        let due: Vec<u32> = {
            let mut state = compactor.state.lock();
            let now = Instant::now();
            let due: Vec<u32> = state
                .tables
                .iter()
                .filter(|(_, s)| {
                    (s.rows > 0 || s.blocks > 0)
                        && (s.rows >= config.compact_rows
                            || s.blocks >= config.compact_blocks
                            || now.duration_since(s.since) >= config.compact_age)
                })
                .map(|(t, _)| *t)
                .collect();
            if due.is_empty() {
                let next = state
                    .tables
                    .values()
                    .filter(|s| s.rows > 0 || s.blocks > 0)
                    .map(|s| (s.since + config.compact_age).saturating_duration_since(now))
                    .min()
                    .unwrap_or(Duration::from_secs(3600));
                if !compactor.stopping() {
                    let _ = compactor.wake.wait_for(&mut state, next.max(Duration::from_millis(5)));
                }
                continue;
            }
            for t in &due {
                state.tables.remove(t);
                state.busy.insert(*t);
            }
            due
        };
        for table in due {
            let result = if !config.background_compaction || shared.in_recovery() || compactor.stopping() {
                Ok(())
            } else {
                let mut session = crate::session::Session::internal(shared.clone());
                session.compact_background(table).map(|_| ())
            };
            let mut state = compactor.state.lock();
            state.busy.remove(&table);
            state.runs += 1;
            if result.is_err() && !compactor.stopping() {
                // Retried once the age threshold has passed again.
                state.tables.entry(table).or_insert(DeltaStats { rows: 0, blocks: 1, since: Instant::now() });
            }
            compactor.finished.notify_all();
        }
        if shared.columnar.has_retired() {
            shared.columnar.sweep(shared.store.oldest_snapshot(), &*shared.objects());
        }
    }
}
