//! Columnar tables: `CREATE TABLE … USING columnar`.
//!
//! A columnar table has two parts. New rows are inserted into the row
//! store like rows of any other table — transactional, durable through the
//! write-ahead log and immediately visible. This part is the *delta*.
//! Compaction moves delta rows into immutable column-oriented *segments*
//! (see the `squad-columnar` crate), which live in the database's object
//! store: a local directory, memory, or an S3-compatible bucket. The list
//! of segments, the *manifest*, is a versioned record in the key-value
//! store, so a compaction is one atomic transaction: readers see either
//! the rows in the delta or the same rows in a segment, never both or
//! neither.
//!
//! Queries read both parts. Aggregations that the column engine can
//! express run there, vectorized and in parallel; everything else scans
//! the segments as rows through the ordinary executor.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rust_decimal::Decimal;
use squad_columnar as cs;
use squad_kv::{LockMode, WaitPolicy};

use crate::agg::numeric_div;
use crate::catalog::{Column, Table};
use crate::exec::{ExecCx, Node};
use crate::expr::{Expr, Row};
use crate::plan::{AggCall, AggFunc, ScanPlan};
use crate::types::{Type, Value};
use crate::value::{format_value, parse_value};
use crate::{sqlstate, Error, Result};

/// Rows in the delta that trigger an automatic compaction.
pub(crate) const AUTO_COMPACT_ROWS: u64 = 100_000;
/// Segments smaller than half of this are merged with each other.
const TARGET_SEGMENT_ROWS: u64 = 4_000_000;
const MERGE_MIN_SEGMENTS: usize = 8;

impl From<cs::Error> for Error {
    fn from(e: cs::Error) -> Error {
        let code = match &e {
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

/// Key of a table's manifest in the key-value store.
pub(crate) fn manifest_key(table_id: u32) -> Vec<u8> {
    let mut k = b"\x00\x00\x00\x00columnar/".to_vec();
    k.extend_from_slice(&table_id.to_be_bytes());
    k
}

fn object_key(table_id: u32, segment: &str) -> String {
    format!("columnar/{table_id}/{segment}.seg")
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
    /// Any other type, stored as its text representation. Such columns
    /// can be read back but not filtered or aggregated by the column
    /// engine.
    AsText,
}

fn kind(col: &Column) -> Kind {
    match &col.ty {
        Type::Bool => Kind::Bool,
        Type::Int2 | Type::Int4 | Type::Int8 | Type::Oid | Type::Date | Type::Time | Type::Timestamp | Type::TimestampTz => {
            Kind::Int
        }
        Type::Float4 | Type::Float8 => Kind::Float,
        Type::Numeric if col.typmod >= 4 => Kind::Decimal(((col.typmod - 4) & 0xffff).min(28) as u8),
        Type::Text | Type::Enum(_) | Type::Char => Kind::Text,
        Type::Uuid | Type::Bytea => Kind::Bytes,
        _ => Kind::AsText,
    }
}

/// Columns stored in segments: all but the hidden row id.
fn stored(table: &Table) -> impl Iterator<Item = (usize, &Column)> {
    table.columns.iter().enumerate().filter(|(_, c)| !c.hidden)
}

pub(crate) fn schema(table: &Table) -> Arc<cs::Schema> {
    let fields = stored(table)
        .map(|(_, c)| {
            let ty = match kind(c) {
                Kind::Bool => cs::ColumnType::Bool,
                Kind::Int => cs::ColumnType::Int64,
                Kind::Float => cs::ColumnType::Float64,
                Kind::Decimal(scale) => cs::ColumnType::Decimal { scale },
                Kind::Text | Kind::AsText => cs::ColumnType::Utf8,
                Kind::Bytes => cs::ColumnType::Bytes,
            };
            cs::Field::new(c.name.clone(), ty, true)
        })
        .collect();
    Arc::new(cs::Schema::new(fields))
}

fn to_datum(v: &Value, col: &Column, cx: &ExecCx) -> Option<cs::Datum> {
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
        (Kind::AsText, v) => cs::Datum::text(format_value(v, &cx.fn_ctx)),
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
            Kind::Decimal(scale) => Value::Numeric(Decimal::try_from_i128_with_scale(m, scale as u32).map_err(|_| bad())?),
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
            (Kind::AsText, ty) => parse_value(std::str::from_utf8(&b).map_err(|_| bad())?, ty, &cx.fn_ctx)?,
            _ => return Err(bad()),
        },
        _ => return Err(bad()),
    })
}

/// Rows of the table (all columns, hidden ones included) as a batch of
/// the stored columns.
fn rows_to_batch(table: &Table, rows: &[Row], cx: &ExecCx) -> Result<cs::Batch> {
    let cols: Vec<(usize, &Column)> = stored(table).collect();
    let mut data = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out = Vec::with_capacity(cols.len());
        for (i, col) in &cols {
            out.push(to_datum(&row[*i], col, cx).ok_or_else(|| {
                Error::internal(format!("value of column \"{}\" cannot be stored in a segment", col.name))
            })?);
        }
        data.push(out);
    }
    Ok(cs::Batch::from_rows(schema(table), &data)?)
}

/// Opened segments, kept in memory. Segments are immutable, so entries
/// never go stale; the cache is bounded by bytes.
#[derive(Default)]
pub(crate) struct SegmentCache {
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    map: HashMap<String, (Arc<cs::Segment>, u64)>,
    order: Vec<String>,
    bytes: u64,
}

/// Memory the segment cache may hold.
const CACHE_BYTES: u64 = 2 << 30;

impl SegmentCache {
    fn get(&self, key: &str, store: &dyn squad_object::ObjectStore) -> Result<Arc<cs::Segment>> {
        if let Some((seg, _)) = self.inner.lock().map.get(key) {
            return Ok(seg.clone());
        }
        let bytes = store
            .get(key)?
            .ok_or_else(|| Error::new(sqlstate::DATA_CORRUPTED, format!("segment object {key} is missing")))?;
        let size = bytes.len() as u64;
        let segment = Arc::new(cs::Segment::open(Arc::new(bytes))?);
        let mut inner = self.inner.lock();
        while inner.bytes + size > CACHE_BYTES && !inner.order.is_empty() {
            let victim = inner.order.remove(0);
            if let Some((_, n)) = inner.map.remove(&victim) {
                inner.bytes -= n;
            }
        }
        inner.bytes += size;
        inner.order.push(key.to_string());
        inner.map.insert(key.to_string(), (segment.clone(), size));
        Ok(segment)
    }

    pub(crate) fn forget(&self, key: &str) {
        let mut inner = self.inner.lock();
        if let Some((_, n)) = inner.map.remove(key) {
            inner.bytes -= n;
            inner.order.retain(|k| k != key);
        }
    }
}

fn load_manifest(table: &Table, cx: &ExecCx) -> Result<cs::Manifest> {
    match cx.txn.get(&manifest_key(table.id))? {
        Some(bytes) => Ok(cs::Manifest::from_bytes(&bytes)?),
        None => {
            let order_by = table.columnar.as_ref().map(|c| c.order_by.clone()).unwrap_or_default();
            Ok(cs::Manifest::new((*schema(table)).clone(), order_by))
        }
    }
}

fn open_segments(table: &Table, manifest: &cs::Manifest, cx: &ExecCx) -> Result<Vec<Arc<cs::Segment>>> {
    let store = cx.shared.objects();
    manifest.segments.iter().map(|s| cx.shared.segments.get(&object_key(table.id, &s.id), &*store)).collect()
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
    let target = |e: &Expr| -> Option<(usize, &Column)> {
        let i = column_of(e)?;
        let col = &table.columns[i];
        (kind(col) != Kind::AsText).then_some((pos[i]?, col))
    };
    Ok(match e {
        Expr::Const(Value::Bool(b)) => Some(cs::Predicate::Const(Some(*b))),
        Expr::Const(Value::Null) => Some(cs::Predicate::Const(None)),
        Expr::And(items) => list(items, cx)?.map(cs::Predicate::And),
        Expr::Or(items) => list(items, cx)?.map(cs::Predicate::Or),
        Expr::Not(inner) => predicate(inner, table, pos, cx)?.map(|p| cs::Predicate::Not(Box::new(p))),
        Expr::IsNull(inner, negated) => target(inner).map(|(column, _)| cs::Predicate::IsNull { column, negated: *negated }),
        Expr::Column(i) if table.columns[*i].ty == Type::Bool => pos[*i].map(cs::Predicate::BoolColumn),
        Expr::InList(inner, items, negated) => {
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

/// Streams the rows of a table's segments through the ordinary executor,
/// skipping row groups whose statistics rule out the filter.
pub(crate) struct SegmentRows<'p> {
    plan: &'p ScanPlan,
    segments: Vec<Arc<cs::Segment>>,
    prune: Option<cs::Predicate>,
    projection: Vec<usize>,
    /// (segment, row group) to read next.
    next: (usize, usize),
    pending: std::vec::IntoIter<Row>,
}

impl<'p> SegmentRows<'p> {
    pub(crate) fn open(plan: &'p ScanPlan, cx: &mut ExecCx) -> Result<SegmentRows<'p>> {
        let table = &plan.table;
        let manifest = load_manifest(table, cx)?;
        let pos = segment_positions(table);
        let prune = match &plan.filter {
            Some(f) => pruning_predicate(f, table, &pos, cx)?,
            None => None,
        };
        // Whole segments that cannot match are not even opened.
        let mut kept = cs::Manifest::new(manifest.schema.clone(), manifest.sort_key.clone());
        for s in manifest.segments {
            if prune.as_ref().is_none_or(|p| p.may_match(&|c| s.stats.get(c))) {
                kept.segments.push(s);
            }
        }
        let segments = open_segments(table, &kept, cx)?;
        let projection = (0..stored(table).count()).collect();
        Ok(SegmentRows { plan, segments, prune, projection, next: (0, 0), pending: Vec::new().into_iter() })
    }
}

impl Node for SegmentRows<'_> {
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
            let Some(segment) = self.segments.get(s) else { return Ok(None) };
            if g >= segment.num_row_groups() {
                self.next = (s + 1, 0);
                continue;
            }
            self.next = (s, g + 1);
            let meta = segment.row_group(g);
            if self.prune.as_ref().is_some_and(|p| !p.may_match(&|c| meta.columns.get(c).map(|m| &m.stats))) {
                continue;
            }
            let batch = segment.read_row_group(g, &self.projection)?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            for i in 0..batch.num_rows() {
                let mut data = batch.row(i).into_iter();
                let mut row = Vec::with_capacity(table.columns.len());
                for col in &table.columns {
                    row.push(match col.hidden {
                        // Rows in segments have no row id.
                        true => Value::Null,
                        false => from_datum(data.next().unwrap_or(cs::Datum::Null), col, cx)?,
                    });
                }
                rows.push(row);
            }
            self.pending = rows.into_iter();
        }
    }
}

/// Delta rows of a table that pass the scan's filter.
fn delta_rows(plan: &ScanPlan, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let mut node = crate::exec::row_store_scan(plan, cx)?;
    let mut rows = Vec::new();
    while let Some(r) = node.next(cx)? {
        rows.push(r);
    }
    Ok(rows)
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
pub(crate) fn aggregate(plan: &ScanPlan, groups: &[Expr], aggs: &[AggCall], cx: &mut ExecCx) -> Result<Option<Vec<Row>>> {
    let table = &plan.table;
    let pos = segment_positions(table);
    let filter = match &plan.filter {
        Some(f) => match predicate(f, table, &pos, cx)? {
            Some(p) => Some(p),
            None => return Ok(None),
        },
        None => None,
    };

    // Group keys: plain columns, or date_trunc over a timestamp column.
    enum Key<'a> {
        Column(&'a Column),
        Bucket(&'a Column),
    }
    let mut keys = Vec::with_capacity(groups.len());
    let mut key_info = Vec::with_capacity(groups.len());
    for g in groups {
        match g {
            Expr::Column(i) => {
                let col = &table.columns[*i];
                let (Some(p), true) = (pos[*i], kind(col) != Kind::AsText) else { return Ok(None) };
                keys.push(cs::GroupKey::Column(p));
                key_info.push(Key::Column(col));
            }
            Expr::Call(f, args) if &*f.name == "date_trunc" && args.len() == 2 => {
                let (Expr::Const(Value::Text(unit)), Some(i)) = (&args[0], column_of(&args[1])) else { return Ok(None) };
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
        let mut push = |agg: cs::Aggregate| {
            requests.push(agg);
            requests.len() - 1
        };
        let m = match (&a.func, column, a.distinct) {
            (AggFunc::CountStar, _, _) => Mapped::Direct(push(cs::Aggregate::CountStar)),
            (AggFunc::Count, Some((p, _)), false) => Mapped::Direct(push(cs::Aggregate::Count(p))),
            (AggFunc::Count, Some((p, col)), true) if kind(col) != Kind::AsText => {
                Mapped::Direct(push(cs::Aggregate::CountDistinct(p)))
            }
            (AggFunc::ApproxCountDistinct, Some((p, col)), _) if kind(col) != Kind::AsText => {
                Mapped::Direct(push(cs::Aggregate::ApproxCountDistinct(p)))
            }
            (AggFunc::ApproxPercentile(q), Some((p, col)), false) if numeric(col) => {
                Mapped::Direct(push(cs::Aggregate::ApproxPercentile { column: p, percentile: *q }))
            }
            (AggFunc::Sum, Some((p, col)), false) if numeric(col) => Mapped::Direct(push(cs::Aggregate::Sum(p))),
            (AggFunc::Avg, Some((p, col)), false) if numeric(col) => {
                Mapped::Avg(push(cs::Aggregate::Sum(p)), push(cs::Aggregate::Count(p)))
            }
            (AggFunc::Min, Some((p, col)), _) if kind(col) != Kind::AsText => Mapped::Direct(push(cs::Aggregate::Min(p))),
            (AggFunc::Max, Some((p, col)), _) if kind(col) != Kind::AsText => Mapped::Direct(push(cs::Aggregate::Max(p))),
            _ => return Ok(None),
        };
        mapped.push((m, a, column.map(|c| c.1)));
    }

    let manifest = load_manifest(table, cx)?;
    let mut source = cs::TableSource::new(schema(table));
    source.segments = open_segments(table, &manifest, cx)?;
    let delta = delta_rows(plan, cx)?;
    if !delta.is_empty() {
        source.delta.push(rows_to_batch(table, &delta, cx)?);
    }
    let request = cs::AggregateRequest {
        predicate: filter,
        group_by: keys,
        aggregates: requests,
        parallelism: std::thread::available_parallelism().map_or(1, |n| n.get()),
        ..Default::default()
    };
    let result = cs::aggregate(&source, &request)?;

    let nkeys = groups.len();
    let mut out = Vec::with_capacity(result.num_rows());
    for r in 0..result.num_rows() {
        let data = result.row(r);
        let mut row = Vec::with_capacity(nkeys + aggs.len());
        for (k, info) in key_info.iter().enumerate() {
            let col = match info {
                Key::Column(c) | Key::Bucket(c) => c,
            };
            row.push(from_datum(data[k].clone(), col, cx)?);
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
                (d, Type::Int8) => {
                    Value::Int8(int_of(d).and_then(|i| i64::try_from(i).ok()).ok_or_else(|| Error::out_of_range("bigint"))?)
                }
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

fn new_segment_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn writer_options(table: &Table) -> cs::WriterOptions {
    let pos = segment_positions(table);
    let sort_key = table.columnar.as_ref().map(|c| c.order_by.iter().filter_map(|i| pos[*i]).collect()).unwrap_or_default();
    cs::WriterOptions { sort_key, ..Default::default() }
}

/// Writes rows as one new segment and adds it to the manifest.
fn add_segment(table: &Table, manifest: &mut cs::Manifest, batch: &cs::Batch, cx: &mut ExecCx) -> Result<()> {
    let mut writer = cs::SegmentWriter::new(schema(table), writer_options(table));
    writer.write_batch(batch)?;
    store_segment(table, manifest, writer.finish()?, cx)
}

fn store_segment(table: &Table, manifest: &mut cs::Manifest, bytes: Vec<u8>, cx: &mut ExecCx) -> Result<()> {
    let id = new_segment_id();
    let key = object_key(table.id, &id);
    // The object is durable before the manifest that names it commits. If
    // the transaction fails, the object is an unreferenced leftover.
    cx.shared.objects().put(&key, &bytes)?;
    let segment = cs::Segment::open(Arc::new(bytes))?;
    manifest.segments.push(cs::SegmentMeta::describe(id, &segment));
    Ok(())
}

fn save_manifest(table: &Table, manifest: &cs::Manifest, cx: &mut ExecCx) -> Result<()> {
    cx.txn.put_blind(&manifest_key(table.id), &manifest.to_bytes())?;
    Ok(())
}

/// Serializes changes to a table's segments. Callers take a fresh snapshot
/// afterwards, so they see what the previous holder committed.
fn lock_segments(table: &Table, cx: &mut ExecCx) -> Result<()> {
    let wait = WaitPolicy::Wait(cx.session.lock_timeout);
    cx.txn.lock(&manifest_key(table.id), LockMode::Exclusive, wait)?;
    cx.txn.refresh_snapshot();
    Ok(())
}

/// Moves all delta rows into a new segment and merges small segments.
/// Returns the number of rows moved. Must run in a Read Committed
/// transaction of its own.
pub(crate) fn compact(table: &Arc<Table>, cx: &mut ExecCx) -> Result<u64> {
    lock_segments(table, cx)?;
    let plan = ScanPlan::full(table.clone());
    let rows = delta_rows(&plan, cx)?;
    let mut manifest = load_manifest(table, cx)?;
    if !rows.is_empty() {
        let batch = rows_to_batch(table, &rows, cx)?;
        add_segment(table, &mut manifest, &batch, cx)?;
        for row in &rows {
            cx.txn.delete_blind(&crate::exec::row_key(table, row))?;
        }
    }
    // Many small segments cost a read each per query: merge them.
    let small: Vec<String> =
        manifest.compaction_candidates(TARGET_SEGMENT_ROWS, MERGE_MIN_SEGMENTS).iter().map(|s| s.id.clone()).collect();
    if !small.is_empty() {
        let store = cx.shared.objects();
        let inputs = small
            .iter()
            .map(|id| cx.shared.segments.get(&object_key(table.id, id), &*store))
            .collect::<Result<Vec<_>>>()?;
        let merged = cs::compact(&inputs, writer_options(table))?;
        manifest.segments.retain(|s| !small.contains(&s.id));
        store_segment(table, &mut manifest, merged, cx)?;
        cx.dropped_objects.extend(small.iter().map(|id| object_key(table.id, id)));
    }
    if !rows.is_empty() || !small.is_empty() {
        save_manifest(table, &manifest, cx)?;
    }
    Ok(rows.len() as u64)
}

/// `DELETE` on a columnar table: delta rows are deleted as usual, and
/// every segment that holds matching rows is rewritten without them.
pub(crate) fn delete(plan: &ScanPlan, cx: &mut ExecCx) -> Result<u64> {
    let table = &plan.table;
    lock_segments(table, cx)?;
    let mut count = 0u64;
    for row in delta_rows(plan, cx)? {
        cx.txn.delete_blind(&crate::exec::row_key(table, &row))?;
        count += 1;
    }
    let mut manifest = load_manifest(table, cx)?;
    let pos = segment_positions(table);
    let prune = match &plan.filter {
        Some(f) => pruning_predicate(f, table, &pos, cx)?,
        None => None,
    };
    let candidates: Vec<String> = manifest
        .segments
        .iter()
        .filter(|s| prune.as_ref().is_none_or(|p| p.may_match(&|c| s.stats.get(c))))
        .map(|s| s.id.clone())
        .collect();
    let mut changed = false;
    for id in candidates {
        let mut one = cs::Manifest::new(manifest.schema.clone(), manifest.sort_key.clone());
        one.segments.extend(manifest.segments.iter().filter(|s| s.id == id).cloned());
        let unfiltered = ScanPlan::full(table.clone());
        let mut reader = SegmentRows {
            plan: &unfiltered,
            segments: open_segments(table, &one, cx)?,
            prune: None,
            projection: (0..stored(table).count()).collect(),
            next: (0, 0),
            pending: Vec::new().into_iter(),
        };
        let mut keep = Vec::new();
        let mut removed = 0u64;
        while let Some(row) = reader.next(cx)? {
            let matches = match &plan.filter {
                Some(f) => f.test(&row, cx)?,
                None => true,
            };
            if matches {
                removed += 1;
            } else {
                keep.push(row);
            }
        }
        if removed == 0 {
            continue;
        }
        count += removed;
        changed = true;
        manifest.segments.retain(|s| s.id != id);
        if !keep.is_empty() {
            let batch = rows_to_batch(table, &keep, cx)?;
            add_segment(table, &mut manifest, &batch, cx)?;
        }
        cx.dropped_objects.push(object_key(table.id, &id));
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
