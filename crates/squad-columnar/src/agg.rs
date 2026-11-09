//! Vectorized, parallel hash aggregation.
//!
//! Every row group (and every delta batch) is one unit of work. Worker
//! threads pull units, filter them and fold the selected rows into their
//! own partial result; the partials are merged at the end. All aggregate
//! states are mergeable, which is also what a distributed execution
//! needs.
//!
//! Memory: a partial holds one state per group it has seen. Counts, sums
//! and extremes are a few bytes per group; exact distinct counts hold
//! every distinct value (or its 128-bit hash) per group; the approximate
//! aggregates are bounded per group (HyperLogLog 4 KiB, quantile sketch a
//! few KiB for typical value ranges).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::exec::{Loaded, TableSource};
use crate::predicate::{selection, Predicate};
use crate::sketch::{Hll, QuantileSketch};
use crate::types::{
    cmp_datum, cmp_f64, Batch, BytesColumn, Column, ColumnBuilder, ColumnData, ColumnType, Datum, Field, Schema,
};
use crate::util::{hash_bytes, hash_u64, FastMap, FastSet};
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    /// ISO week, starting on Monday.
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupKey {
    Column(usize),
    /// Start of the calendar period containing a timestamp.
    ///
    /// The column holds microseconds. `epoch_unix_micros` is the Unix time
    /// of the column's zero (0 for Unix timestamps, 946 684 800 000 000
    /// for timestamps counted from 2000-01-01 as PostgreSQL stores them).
    /// `utc_offset_micros` shifts period boundaries to a time zone with
    /// that fixed offset from UTC. The result is in the column's own
    /// representation.
    TimeBucket {
        column: usize,
        unit: TimeUnit,
        utc_offset_micros: i64,
        epoch_unix_micros: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    /// `count(*)` → Int64
    CountStar,
    /// `count(column)` → Int64
    Count(usize),
    /// `count(DISTINCT column)` → Int64. Exact; byte strings are told
    /// apart by a 128-bit hash.
    CountDistinct(usize),
    /// `sum(column)`: Int64 → Decimal with scale 0 (never overflows 64
    /// bits), Float64 → Float64, Decimal → Decimal of the same scale.
    Sum(usize),
    /// `min(column)` → the column's type
    Min(usize),
    /// `max(column)` → the column's type
    Max(usize),
    /// `avg(column)` → Float64
    Avg(usize),
    /// Approximate `count(DISTINCT column)` → Int64 (HyperLogLog).
    ApproxCountDistinct(usize),
    /// Approximate continuous percentile → Float64. `percentile` is
    /// between 0 and 1; the relative error is at most 1 %.
    ApproxPercentile { column: usize, percentile: f64 },
}

/// Sort key over the result: `column` counts the group keys first, then
/// the aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderBy {
    pub column: usize,
    pub descending: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AggregateRequest {
    pub predicate: Option<Predicate>,
    pub group_by: Vec<GroupKey>,
    pub aggregates: Vec<Aggregate>,
    /// Filter over the result (columns: group keys, then aggregates).
    pub having: Option<Predicate>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    /// Worker threads; 0 or 1 runs on the calling thread.
    pub parallelism: usize,
}

const DAY: i64 = 86_400_000_000;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + (m <= 2) as i64, m)
}

/// Start of the period of `unit` containing Unix time `t` (microseconds).
pub fn floor_time(t: i64, unit: TimeUnit) -> i64 {
    let fixed = |size: i64| t.div_euclid(size).wrapping_mul(size);
    match unit {
        TimeUnit::Second => fixed(1_000_000),
        TimeUnit::Minute => fixed(60_000_000),
        TimeUnit::Hour => fixed(3_600_000_000),
        TimeUnit::Day => fixed(DAY),
        // 1970-01-01 was a Thursday.
        TimeUnit::Week => ((t.div_euclid(DAY) + 3).div_euclid(7) * 7 - 3).wrapping_mul(DAY),
        TimeUnit::Month | TimeUnit::Quarter | TimeUnit::Year => {
            let (y, m) = civil_from_days(t.div_euclid(DAY));
            let m = match unit {
                TimeUnit::Month => m,
                TimeUnit::Quarter => (m - 1) / 3 * 3 + 1,
                _ => 1,
            };
            days_from_civil(y, m, 1).wrapping_mul(DAY)
        }
    }
}

/// Maps the distinct values of one group key to dense ids.
#[derive(Default)]
struct IdTable {
    values: Vec<Datum>,
    null: Option<u32>,
    ints: FastMap<i64, u32>,
    wide: FastMap<i128, u32>,
    bytes: FastMap<Vec<u8>, u32>,
}

fn float_key(f: f64) -> i128 {
    let f = if f.is_nan() {
        f64::NAN
    } else if f == 0.0 {
        0.0
    } else {
        f
    };
    f.to_bits() as i128
}

impl IdTable {
    fn next(&mut self, d: Datum) -> u32 {
        self.values.push(d);
        self.values.len() as u32 - 1
    }
    fn null_id(&mut self) -> u32 {
        match self.null {
            Some(id) => id,
            None => {
                let id = self.next(Datum::Null);
                self.null = Some(id);
                id
            }
        }
    }
    #[inline]
    fn int_id(&mut self, v: i64, as_bool: bool) -> u32 {
        if let Some(id) = self.ints.get(&v) {
            return *id;
        }
        let id = self.next(if as_bool { Datum::Bool(v != 0) } else { Datum::Int(v) });
        self.ints.insert(v, id);
        id
    }
    fn wide_id(&mut self, key: i128, d: impl FnOnce() -> Datum) -> u32 {
        if let Some(id) = self.wide.get(&key) {
            return *id;
        }
        let id = self.next(d());
        self.wide.insert(key, id);
        id
    }
    fn bytes_id(&mut self, v: &[u8]) -> u32 {
        if let Some(id) = self.bytes.get(v) {
            return *id;
        }
        let id = self.next(Datum::Bytes(v.to_vec()));
        self.bytes.insert(v.to_vec(), id);
        id
    }
    fn datum_id(&mut self, d: &Datum) -> u32 {
        match d {
            Datum::Null => self.null_id(),
            Datum::Int(v) => self.int_id(*v, false),
            Datum::Bool(b) => self.int_id(*b as i64, true),
            Datum::Float(f) => self.wide_id(float_key(*f), || Datum::Float(*f)),
            Datum::Decimal(v) => self.wide_id(*v, || Datum::Decimal(*v)),
            Datum::Bytes(b) => self.bytes_id(b),
        }
    }

    /// Ids of the selected rows of one unit, appended to `out`.
    fn ids(&mut self, key: &GroupKey, l: &Loaded, out: &mut Vec<u32>) -> Result<()> {
        let (column, bucket) = match key {
            GroupKey::Column(c) => (*c, None),
            GroupKey::TimeBucket { column, unit, utc_offset_micros, epoch_unix_micros } => {
                (*column, Some((*unit, utc_offset_micros.wrapping_add(*epoch_unix_micros))))
            }
        };
        let col = l.columns[column].as_ref().expect("group key column was loaded");
        let n = l.selected();
        let row_at = |k: usize| l.selection.as_ref().map_or(k, |s| s[k] as usize);
        out.reserve(n);
        match &col.data {
            ColumnData::Int64(v) => {
                // Consecutive rows often share a key (sorted or clustered
                // data), so remember the last lookup.
                let mut last: Option<(i64, u32)> = None;
                for k in 0..n {
                    let r = row_at(k);
                    if col.is_null(r) {
                        out.push(self.null_id());
                        continue;
                    }
                    let x = match bucket {
                        Some((unit, shift)) => floor_time(v[r].wrapping_add(shift), unit).wrapping_sub(shift),
                        None => v[r],
                    };
                    let id = match last {
                        Some((lx, id)) if lx == x => id,
                        _ => {
                            let id = self.int_id(x, false);
                            last = Some((x, id));
                            id
                        }
                    };
                    out.push(id);
                }
            }
            _ if bucket.is_some() => {
                return Err(Error::Type("time buckets need an Int64 column of microseconds".into()))
            }
            ColumnData::Bool(v) => {
                for k in 0..n {
                    let r = row_at(k);
                    let id = if col.is_null(r) { self.null_id() } else { self.int_id(v[r] as i64, true) };
                    out.push(id);
                }
            }
            ColumnData::Float64(v) => {
                for k in 0..n {
                    let r = row_at(k);
                    let id = if col.is_null(r) {
                        self.null_id()
                    } else {
                        self.wide_id(float_key(v[r]), || Datum::Float(v[r]))
                    };
                    out.push(id);
                }
            }
            ColumnData::Decimal(v) => {
                for k in 0..n {
                    let r = row_at(k);
                    let id = if col.is_null(r) { self.null_id() } else { self.wide_id(v[r], || Datum::Decimal(v[r])) };
                    out.push(id);
                }
            }
            ColumnData::Bytes(BytesColumn::Dict { codes, dict }) => {
                // Resolve each dictionary entry once per unit.
                let mut entry_ids = vec![u32::MAX; dict.len()];
                for k in 0..n {
                    let r = row_at(k);
                    if col.is_null(r) {
                        out.push(self.null_id());
                        continue;
                    }
                    let code = codes[r] as usize;
                    if entry_ids[code] == u32::MAX {
                        entry_ids[code] = self.bytes_id(dict.get(code));
                    }
                    out.push(entry_ids[code]);
                }
            }
            ColumnData::Bytes(b) => {
                for k in 0..n {
                    let r = row_at(k);
                    let id = if col.is_null(r) { self.null_id() } else { self.bytes_id(b.get(r)) };
                    out.push(id);
                }
            }
        }
        Ok(())
    }
}

/// Calls `f(k, row)` for every selected row whose value in `col` is not
/// NULL; `k` is the position within the selection.
#[inline]
fn each_valid(l: &Loaded, col: &Column, mut f: impl FnMut(usize, usize)) {
    match (&l.selection, &col.validity) {
        (None, None) => (0..l.rows).for_each(|r| f(r, r)),
        (None, Some(v)) => (0..l.rows).filter(|r| v[*r]).for_each(|r| f(r, r)),
        (Some(s), None) => s.iter().enumerate().for_each(|(k, r)| f(k, *r as usize)),
        (Some(s), Some(v)) => {
            s.iter().enumerate().filter(|(_, r)| v[**r as usize]).for_each(|(k, r)| f(k, *r as usize))
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Bool,
    Int,
    Float,
    Decimal(u8),
    Bytes,
}

fn kind_of(ty: ColumnType) -> Kind {
    match ty {
        ColumnType::Bool => Kind::Bool,
        ColumnType::Int64 => Kind::Int,
        ColumnType::Float64 => Kind::Float,
        ColumnType::Decimal { scale } => Kind::Decimal(scale),
        ColumnType::Bytes | ColumnType::Utf8 => Kind::Bytes,
    }
}

/// Per-group state of one aggregate, stored column-wise.
enum State {
    Count(Vec<i64>),
    /// Sum of integers or decimals with the number of inputs.
    SumWide {
        sum: Vec<i128>,
        n: Vec<u64>,
        overflow: bool,
    },
    SumFloat {
        sum: Vec<f64>,
        n: Vec<u64>,
    },
    ExtremeInt {
        v: Vec<i64>,
        seen: Vec<bool>,
    },
    ExtremeWide {
        v: Vec<i128>,
        seen: Vec<bool>,
    },
    ExtremeFloat {
        v: Vec<f64>,
        seen: Vec<bool>,
    },
    ExtremeBytes {
        v: Vec<Option<Vec<u8>>>,
    },
    DistinctInt(Vec<FastSet<i64>>),
    DistinctWide(Vec<FastSet<u128>>),
    Hll(Vec<Hll>),
    Quantile(Vec<QuantileSketch>),
}

struct AggSpec {
    column: Option<usize>,
    kind: Kind,
    /// Type of the input column.
    ty: ColumnType,
    /// For extremes: true = max.
    max: bool,
    agg: Aggregate,
}

fn fnv(data: &[u8]) -> u64 {
    data.iter().fold(0xCBF2_9CE4_8422_2325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x0100_0000_01B3))
}

/// Identity of a value for exact distinct counting.
fn wide_identity(col: &Column, r: usize) -> u128 {
    match &col.data {
        ColumnData::Bool(v) => v[r] as u128,
        ColumnData::Int64(v) => v[r] as u64 as u128,
        ColumnData::Float64(v) => float_key(v[r]) as u128,
        ColumnData::Decimal(v) => v[r] as u128,
        ColumnData::Bytes(b) => {
            let x = b.get(r);
            ((hash_bytes(x) as u128) << 64) | fnv(x) as u128
        }
    }
}

fn value_hash(col: &Column, r: usize) -> u64 {
    match &col.data {
        ColumnData::Bool(v) => hash_u64(v[r] as u64),
        ColumnData::Int64(v) => hash_u64(v[r] as u64),
        ColumnData::Float64(v) => hash_u64(float_key(v[r]) as u64),
        ColumnData::Decimal(v) => hash_bytes(&v[r].to_le_bytes()),
        ColumnData::Bytes(b) => hash_bytes(b.get(r)),
    }
}

fn numeric_at(col: &Column, r: usize, scale: f64) -> f64 {
    match &col.data {
        ColumnData::Int64(v) => v[r] as f64,
        ColumnData::Float64(v) => v[r],
        ColumnData::Decimal(v) => v[r] as f64 / scale,
        _ => f64::NAN,
    }
}

impl AggSpec {
    fn new(agg: &Aggregate, schema: &Schema) -> Result<(AggSpec, State)> {
        let column = match agg {
            Aggregate::CountStar => None,
            Aggregate::Count(c)
            | Aggregate::CountDistinct(c)
            | Aggregate::Sum(c)
            | Aggregate::Min(c)
            | Aggregate::Max(c)
            | Aggregate::Avg(c)
            | Aggregate::ApproxCountDistinct(c)
            | Aggregate::ApproxPercentile { column: c, .. } => Some(*c),
        };
        let ty = match column {
            Some(c) => schema.columns.get(c).ok_or_else(|| Error::Invalid(format!("column {c} does not exist")))?.ty,
            None => ColumnType::Int64,
        };
        let kind = kind_of(ty);
        let numeric = matches!(kind, Kind::Int | Kind::Float | Kind::Decimal(_));
        let not_numeric = |what: &str| Error::Type(format!("{what} needs a numeric column"));
        let state = match agg {
            Aggregate::CountStar | Aggregate::Count(_) => State::Count(Vec::new()),
            Aggregate::CountDistinct(_) => match kind {
                Kind::Int => State::DistinctInt(Vec::new()),
                _ => State::DistinctWide(Vec::new()),
            },
            Aggregate::Sum(_) | Aggregate::Avg(_) => match kind {
                Kind::Int | Kind::Decimal(_) => State::SumWide { sum: Vec::new(), n: Vec::new(), overflow: false },
                Kind::Float => State::SumFloat { sum: Vec::new(), n: Vec::new() },
                _ => return Err(not_numeric("sum/avg")),
            },
            Aggregate::Min(_) | Aggregate::Max(_) => match kind {
                Kind::Int | Kind::Bool => State::ExtremeInt { v: Vec::new(), seen: Vec::new() },
                Kind::Decimal(_) => State::ExtremeWide { v: Vec::new(), seen: Vec::new() },
                Kind::Float => State::ExtremeFloat { v: Vec::new(), seen: Vec::new() },
                Kind::Bytes => State::ExtremeBytes { v: Vec::new() },
            },
            Aggregate::ApproxCountDistinct(_) => State::Hll(Vec::new()),
            Aggregate::ApproxPercentile { percentile, .. } => {
                if !numeric {
                    return Err(not_numeric("approx_percentile"));
                }
                if !(0.0..=1.0).contains(percentile) {
                    return Err(Error::Invalid("percentile must be between 0 and 1".into()));
                }
                State::Quantile(Vec::new())
            }
        };
        Ok((AggSpec { column, kind, ty, max: matches!(agg, Aggregate::Max(_)), agg: agg.clone() }, state))
    }

    fn result_type(&self) -> ColumnType {
        match &self.agg {
            Aggregate::CountStar
            | Aggregate::Count(_)
            | Aggregate::CountDistinct(_)
            | Aggregate::ApproxCountDistinct(_) => ColumnType::Int64,
            Aggregate::Sum(_) => match self.kind {
                Kind::Float => ColumnType::Float64,
                Kind::Decimal(scale) => ColumnType::Decimal { scale },
                _ => ColumnType::Decimal { scale: 0 },
            },
            Aggregate::Min(_) | Aggregate::Max(_) => self.ty,
            Aggregate::Avg(_) | Aggregate::ApproxPercentile { .. } => ColumnType::Float64,
        }
    }

    fn scale(&self) -> f64 {
        match self.kind {
            Kind::Decimal(s) => 10f64.powi(s as i32),
            _ => 1.0,
        }
    }
}

impl State {
    fn resize(&mut self, groups: usize) {
        match self {
            State::Count(c) => c.resize(groups, 0),
            State::SumWide { sum, n, .. } => {
                sum.resize(groups, 0);
                n.resize(groups, 0);
            }
            State::SumFloat { sum, n } => {
                sum.resize(groups, 0.0);
                n.resize(groups, 0);
            }
            State::ExtremeInt { v, seen } => {
                v.resize(groups, 0);
                seen.resize(groups, false);
            }
            State::ExtremeWide { v, seen } => {
                v.resize(groups, 0);
                seen.resize(groups, false);
            }
            State::ExtremeFloat { v, seen } => {
                v.resize(groups, 0.0);
                seen.resize(groups, false);
            }
            State::ExtremeBytes { v } => v.resize(groups, None),
            State::DistinctInt(s) => s.resize_with(groups, FastSet::default),
            State::DistinctWide(s) => s.resize_with(groups, FastSet::default),
            State::Hll(s) => s.resize_with(groups, Hll::default),
            State::Quantile(s) => s.resize_with(groups, QuantileSketch::default),
        }
    }

    /// Folds the selected rows of a unit into the state. `gids[k]` is the
    /// group of the k-th selected row; `None` means a single group.
    fn update(&mut self, spec: &AggSpec, l: &Loaded, gids: Option<&[u32]>) {
        let gid = |k: usize| gids.map_or(0, |g| g[k] as usize);
        let col = spec.column.map(|c| l.columns[c].as_ref().expect("aggregate column was loaded"));
        match self {
            State::Count(counts) => match (col, gids) {
                (None, None) => counts[0] += l.selected() as i64,
                (None, Some(g)) => g.iter().for_each(|g| counts[*g as usize] += 1),
                (Some(c), None) if c.validity.is_none() => counts[0] += l.selected() as i64,
                (Some(c), _) => each_valid(l, c, |k, _| counts[gid(k)] += 1),
            },
            State::SumWide { sum, n, overflow } => {
                let c = col.unwrap();
                match &c.data {
                    ColumnData::Int64(v) => {
                        if gids.is_none() && l.selection.is_none() && c.validity.is_none() {
                            // 64-bit values cannot overflow 128 bits.
                            sum[0] += v.iter().map(|x| *x as i128).sum::<i128>();
                            n[0] += v.len() as u64;
                        } else {
                            each_valid(l, c, |k, r| {
                                let g = gid(k);
                                sum[g] += v[r] as i128;
                                n[g] += 1;
                            });
                        }
                    }
                    ColumnData::Decimal(v) => each_valid(l, c, |k, r| {
                        let g = gid(k);
                        match sum[g].checked_add(v[r]) {
                            Some(s) => sum[g] = s,
                            None => *overflow = true,
                        }
                        n[g] += 1;
                    }),
                    _ => {}
                }
            }
            State::SumFloat { sum, n } => {
                let c = col.unwrap();
                if let ColumnData::Float64(v) = &c.data {
                    each_valid(l, c, |k, r| {
                        let g = gid(k);
                        sum[g] += v[r];
                        n[g] += 1;
                    });
                }
            }
            State::ExtremeInt { v: best, seen } => {
                let c = col.unwrap();
                let mut fold = |g: usize, x: i64| {
                    if !seen[g] || (x > best[g]) == spec.max && x != best[g] {
                        best[g] = x;
                        seen[g] = true;
                    }
                };
                match &c.data {
                    ColumnData::Int64(v) => each_valid(l, c, |k, r| fold(gid(k), v[r])),
                    ColumnData::Bool(v) => each_valid(l, c, |k, r| fold(gid(k), v[r] as i64)),
                    _ => {}
                }
            }
            State::ExtremeWide { v: best, seen } => {
                let c = col.unwrap();
                if let ColumnData::Decimal(v) = &c.data {
                    each_valid(l, c, |k, r| {
                        let g = gid(k);
                        if !seen[g] || (v[r] > best[g]) == spec.max && v[r] != best[g] {
                            best[g] = v[r];
                            seen[g] = true;
                        }
                    });
                }
            }
            State::ExtremeFloat { v: best, seen } => {
                let c = col.unwrap();
                let want = if spec.max { Ordering::Greater } else { Ordering::Less };
                if let ColumnData::Float64(v) = &c.data {
                    each_valid(l, c, |k, r| {
                        let g = gid(k);
                        if !seen[g] || cmp_f64(v[r], best[g]) == want {
                            best[g] = v[r];
                            seen[g] = true;
                        }
                    });
                }
            }
            State::ExtremeBytes { v: best } => {
                let c = col.unwrap();
                if let ColumnData::Bytes(b) = &c.data {
                    each_valid(l, c, |k, r| {
                        let (g, x) = (gid(k), b.get(r));
                        let better = match &best[g] {
                            None => true,
                            Some(cur) => (x > cur.as_slice()) == spec.max && x != cur.as_slice(),
                        };
                        if better {
                            best[g] = Some(x.to_vec());
                        }
                    });
                }
            }
            State::DistinctInt(sets) => {
                let c = col.unwrap();
                if let ColumnData::Int64(v) = &c.data {
                    each_valid(l, c, |k, r| {
                        sets[gid(k)].insert(v[r]);
                    });
                }
            }
            State::DistinctWide(sets) => {
                let c = col.unwrap();
                each_valid(l, c, |k, r| {
                    sets[gid(k)].insert(wide_identity(c, r));
                });
            }
            State::Hll(sketches) => {
                let c = col.unwrap();
                each_valid(l, c, |k, r| sketches[gid(k)].add_hash(value_hash(c, r)));
            }
            State::Quantile(sketches) => {
                let c = col.unwrap();
                let scale = spec.scale();
                match &c.data {
                    ColumnData::Int64(v) => each_valid(l, c, |k, r| sketches[gid(k)].add_int(v[r])),
                    _ => each_valid(l, c, |k, r| sketches[gid(k)].add(numeric_at(c, r, scale))),
                }
            }
        }
    }

    /// Merges the state of another partial; `map[g]` is the group in
    /// `self` of the other's group `g`.
    fn merge(&mut self, spec: &AggSpec, other: State, map: &[u32]) {
        let at = |g: usize| map[g] as usize;
        match (self, other) {
            (State::Count(a), State::Count(b)) => b.iter().enumerate().for_each(|(g, c)| a[at(g)] += c),
            (State::SumWide { sum, n, overflow }, State::SumWide { sum: s2, n: n2, overflow: o2 }) => {
                *overflow |= o2;
                for g in 0..s2.len() {
                    match sum[at(g)].checked_add(s2[g]) {
                        Some(s) => sum[at(g)] = s,
                        None => *overflow = true,
                    }
                    n[at(g)] += n2[g];
                }
            }
            (State::SumFloat { sum, n }, State::SumFloat { sum: s2, n: n2 }) => {
                for g in 0..s2.len() {
                    sum[at(g)] += s2[g];
                    n[at(g)] += n2[g];
                }
            }
            (State::ExtremeInt { v, seen }, State::ExtremeInt { v: v2, seen: s2 }) => {
                for g in 0..v2.len() {
                    let t = at(g);
                    if s2[g] && (!seen[t] || (v2[g] > v[t]) == spec.max && v2[g] != v[t]) {
                        v[t] = v2[g];
                        seen[t] = true;
                    }
                }
            }
            (State::ExtremeWide { v, seen }, State::ExtremeWide { v: v2, seen: s2 }) => {
                for g in 0..v2.len() {
                    let t = at(g);
                    if s2[g] && (!seen[t] || (v2[g] > v[t]) == spec.max && v2[g] != v[t]) {
                        v[t] = v2[g];
                        seen[t] = true;
                    }
                }
            }
            (State::ExtremeFloat { v, seen }, State::ExtremeFloat { v: v2, seen: s2 }) => {
                let want = if spec.max { Ordering::Greater } else { Ordering::Less };
                for g in 0..v2.len() {
                    let t = at(g);
                    if s2[g] && (!seen[t] || cmp_f64(v2[g], v[t]) == want) {
                        v[t] = v2[g];
                        seen[t] = true;
                    }
                }
            }
            (State::ExtremeBytes { v }, State::ExtremeBytes { v: v2 }) => {
                for (g, x) in v2.into_iter().enumerate() {
                    let Some(x) = x else { continue };
                    let t = at(g);
                    let better = match &v[t] {
                        None => true,
                        Some(cur) => (x > *cur) == spec.max && x != *cur,
                    };
                    if better {
                        v[t] = Some(x);
                    }
                }
            }
            (State::DistinctInt(a), State::DistinctInt(b)) => {
                b.into_iter().enumerate().for_each(|(g, s)| a[at(g)].extend(s));
            }
            (State::DistinctWide(a), State::DistinctWide(b)) => {
                b.into_iter().enumerate().for_each(|(g, s)| a[at(g)].extend(s));
            }
            (State::Hll(a), State::Hll(b)) => b.iter().enumerate().for_each(|(g, s)| a[at(g)].merge(s)),
            (State::Quantile(a), State::Quantile(b)) => b.iter().enumerate().for_each(|(g, s)| a[at(g)].merge(s)),
            _ => unreachable!("partials have the same aggregate states"),
        }
    }

    fn finish(&self, spec: &AggSpec, groups: usize) -> Result<Column> {
        let mut b = ColumnBuilder::new(spec.result_type());
        let avg = matches!(spec.agg, Aggregate::Avg(_));
        let scale = spec.scale();
        for g in 0..groups {
            let d = match self {
                State::Count(c) => Datum::Int(c[g]),
                State::SumWide { overflow: true, .. } => return Err(Error::Invalid("numeric overflow in sum".into())),
                State::SumWide { sum, n, .. } => match (n[g], avg) {
                    (0, _) => Datum::Null,
                    (count, true) => Datum::Float(sum[g] as f64 / scale / count as f64),
                    (_, false) => Datum::Decimal(sum[g]),
                },
                State::SumFloat { sum, n } => match (n[g], avg) {
                    (0, _) => Datum::Null,
                    (count, true) => Datum::Float(sum[g] / count as f64),
                    (_, false) => Datum::Float(sum[g]),
                },
                State::ExtremeInt { v, seen } => match (seen[g], spec.kind) {
                    (false, _) => Datum::Null,
                    (true, Kind::Bool) => Datum::Bool(v[g] != 0),
                    (true, _) => Datum::Int(v[g]),
                },
                State::ExtremeWide { v, seen } => {
                    if seen[g] {
                        Datum::Decimal(v[g])
                    } else {
                        Datum::Null
                    }
                }
                State::ExtremeFloat { v, seen } => {
                    if seen[g] {
                        Datum::Float(v[g])
                    } else {
                        Datum::Null
                    }
                }
                State::ExtremeBytes { v } => v[g].clone().map_or(Datum::Null, Datum::Bytes),
                State::DistinctInt(s) => Datum::Int(s[g].len() as i64),
                State::DistinctWide(s) => Datum::Int(s[g].len() as i64),
                State::Hll(s) => Datum::Int(s[g].estimate() as i64),
                State::Quantile(s) => {
                    let Aggregate::ApproxPercentile { percentile, .. } = &spec.agg else { unreachable!() };
                    s[g].quantile(*percentile).map_or(Datum::Null, Datum::Float)
                }
            };
            b.push(&d)?;
        }
        Ok(b.finish())
    }
}

/// The result of one worker.
struct Partial {
    tables: Vec<IdTable>,
    /// For two keys: `(id0 << 32 | id1)` → group.
    pairs: FastMap<u64, u32>,
    /// For more keys: ids → group.
    tuples: HashMap<Vec<u32>, u32>,
    /// Key ids of each group, `keys` per group.
    group_ids: Vec<u32>,
    groups: usize,
    states: Vec<State>,
}

impl Partial {
    fn new(nkeys: usize, states: Vec<State>) -> Partial {
        let mut p = Partial {
            tables: (0..nkeys).map(|_| IdTable::default()).collect(),
            pairs: FastMap::default(),
            tuples: HashMap::new(),
            group_ids: Vec::new(),
            groups: if nkeys == 0 { 1 } else { 0 },
            states,
        };
        p.states.iter_mut().for_each(|s| s.resize(p.groups));
        p
    }

    fn group_of(&mut self, ids: &[u32]) -> u32 {
        let next = self.groups as u32;
        let g = match ids {
            [a, b] => *self.pairs.entry((*a as u64) << 32 | *b as u64).or_insert(next),
            _ => match self.tuples.get(ids) {
                Some(g) => *g,
                None => {
                    self.tuples.insert(ids.to_vec(), next);
                    next
                }
            },
        };
        if g == next {
            self.group_ids.extend_from_slice(ids);
            self.groups += 1;
        }
        g
    }

    fn consume(&mut self, req: &AggregateRequest, specs: &[AggSpec], l: &Loaded) -> Result<()> {
        let nkeys = req.group_by.len();
        let n = l.selected();
        let gids: Option<Vec<u32>> = match nkeys {
            0 => None,
            1 => {
                let mut ids = Vec::new();
                self.tables[0].ids(&req.group_by[0], l, &mut ids)?;
                self.groups = self.tables[0].values.len();
                Some(ids)
            }
            _ => {
                let mut per_key = Vec::with_capacity(nkeys);
                for (t, key) in self.tables.iter_mut().zip(&req.group_by) {
                    let mut ids = Vec::new();
                    t.ids(key, l, &mut ids)?;
                    per_key.push(ids);
                }
                let mut gids = Vec::with_capacity(n);
                let mut tuple = vec![0u32; nkeys];
                let mut last: Option<u32> = None;
                for k in 0..n {
                    let same = last.is_some() && per_key.iter().zip(&tuple).all(|(ids, t)| ids[k] == *t);
                    if !same {
                        for (t, ids) in tuple.iter_mut().zip(&per_key) {
                            *t = ids[k];
                        }
                        last = Some(self.group_of(&tuple));
                    }
                    gids.push(last.unwrap());
                }
                Some(gids)
            }
        };
        for (state, spec) in self.states.iter_mut().zip(specs) {
            state.resize(self.groups);
            state.update(spec, l, gids.as_deref());
        }
        Ok(())
    }

    fn key(&self, group: usize, nkeys: usize) -> Vec<Datum> {
        match nkeys {
            0 => Vec::new(),
            1 => vec![self.tables[0].values[group].clone()],
            _ => {
                (0..nkeys).map(|k| self.tables[k].values[self.group_ids[group * nkeys + k] as usize].clone()).collect()
            }
        }
    }

    fn merge(&mut self, other: Partial, specs: &[AggSpec], nkeys: usize) {
        let map: Vec<u32> = (0..other.groups)
            .map(|g| {
                let key = other.key(g, nkeys);
                match nkeys {
                    0 => 0,
                    1 => {
                        let id = self.tables[0].datum_id(&key[0]);
                        self.groups = self.tables[0].values.len();
                        id
                    }
                    _ => {
                        let ids: Vec<u32> =
                            key.iter().zip(self.tables.iter_mut()).map(|(d, t)| t.datum_id(d)).collect();
                        self.group_of(&ids)
                    }
                }
            })
            .collect();
        for ((mine, theirs), spec) in self.states.iter_mut().zip(other.states).zip(specs) {
            mine.resize(self.groups);
            mine.merge(spec, theirs, &map);
        }
    }
}

/// Runs an aggregation and returns one row per group: the group keys in
/// request order, then the aggregates. Without `order_by` the row order
/// is unspecified. Without group keys there is exactly one row.
pub fn aggregate(source: &TableSource, request: &AggregateRequest) -> Result<Batch> {
    source.check()?;
    let schema = &source.schema;
    let nkeys = request.group_by.len();
    let mut needed: Vec<usize> = Vec::new();
    let mut key_fields = Vec::with_capacity(nkeys);
    for key in &request.group_by {
        let c = match key {
            GroupKey::Column(c) | GroupKey::TimeBucket { column: c, .. } => *c,
        };
        let field = schema.columns.get(c).ok_or_else(|| Error::Invalid(format!("column {c} does not exist")))?;
        if matches!(key, GroupKey::TimeBucket { .. }) && field.ty != ColumnType::Int64 {
            return Err(Error::Type("time buckets need an Int64 column of microseconds".into()));
        }
        key_fields.push(Field::new(field.name.clone(), field.ty, true));
        if !needed.contains(&c) {
            needed.push(c);
        }
    }
    let mut specs = Vec::with_capacity(request.aggregates.len());
    // Several percentiles of one column share a sketch: `owners` are the
    // aggregates that have a state, `state_of[i]` is the state aggregate
    // `i` reads its result from.
    let mut owners: Vec<usize> = Vec::new();
    let mut state_of = Vec::with_capacity(request.aggregates.len());
    for (i, a) in request.aggregates.iter().enumerate() {
        let (spec, _) = AggSpec::new(a, schema)?;
        if let Some(c) = spec.column {
            if !needed.contains(&c) {
                needed.push(c);
            }
        }
        let shared = match a {
            Aggregate::ApproxPercentile { column, .. } => owners.iter().position(|o| {
                matches!(&request.aggregates[*o], Aggregate::ApproxPercentile { column: other, .. } if other == column)
            }),
            _ => None,
        };
        state_of.push(shared.unwrap_or_else(|| {
            owners.push(i);
            owners.len() - 1
        }));
        specs.push(spec);
    }
    let make_states = || -> Result<Vec<State>> {
        owners.iter().map(|o| AggSpec::new(&request.aggregates[*o], schema).map(|(_, s)| s)).collect()
    };
    let state_specs: Vec<AggSpec> =
        owners.iter().map(|o| AggSpec::new(&request.aggregates[*o], schema).map(|(s, _)| s)).collect::<Result<_>>()?;

    let units = source.units(request.predicate.as_ref());
    let threads = request.parallelism.clamp(1, units.len().max(1));
    let next = AtomicUsize::new(0);
    let work = || -> Result<Partial> {
        let mut partial = Partial::new(nkeys, make_states()?);
        loop {
            let i = next.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(unit) = units.get(i) else { break };
            if let Some(loaded) = source.load(*unit, request.predicate.as_ref(), &needed)? {
                partial.consume(request, &state_specs, &loaded)?;
            }
        }
        Ok(partial)
    };
    let mut partials: Vec<Result<Partial>> = if threads <= 1 {
        vec![work()]
    } else {
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads).map(|_| scope.spawn(work)).collect();
            handles.into_iter().map(|h| h.join().expect("aggregation worker panicked")).collect()
        })
    };
    let mut total = partials.remove(0)?;
    for p in partials {
        total.merge(p?, &state_specs, nkeys);
    }

    // Assemble the result batch.
    let groups = total.groups;
    let mut fields = key_fields;
    let mut columns = Vec::with_capacity(nkeys + specs.len());
    for (k, field) in fields.iter().enumerate() {
        let mut b = ColumnBuilder::new(field.ty);
        for g in 0..groups {
            let d = match nkeys {
                1 => &total.tables[0].values[g],
                _ => &total.tables[k].values[total.group_ids[g * nkeys + k] as usize],
            };
            b.push(d)?;
        }
        columns.push(b.finish());
    }
    for (i, spec) in specs.iter().enumerate() {
        fields.push(Field::new(format!("agg{i}"), spec.result_type(), true));
        columns.push(total.states[state_of[i]].finish(spec, groups)?);
    }
    let mut result = Batch::new(Arc::new(Schema::new(fields)), columns)?;

    if let Some(having) = &request.having {
        let cols: Vec<Option<Column>> = result.columns.iter().cloned().map(Some).collect();
        if let Some(sel) = selection(&having.evaluate(&cols, result.num_rows())?) {
            result = result.take(&sel);
        }
    }
    if !request.order_by.is_empty() || request.limit.is_some() {
        if let Some(bad) = request.order_by.iter().find(|o| o.column >= result.columns.len()) {
            return Err(Error::Invalid(format!("ORDER BY column {} does not exist in the result", bad.column)));
        }
        let mut order: Vec<u32> = (0..result.num_rows() as u32).collect();
        if !request.order_by.is_empty() {
            let keys: Vec<Vec<Datum>> = request
                .order_by
                .iter()
                .map(|o| (0..result.num_rows()).map(|r| result.columns[o.column].datum(r)).collect())
                .collect();
            order.sort_by(|&a, &b| {
                for (o, k) in request.order_by.iter().zip(&keys) {
                    // NULLs last ascending, first descending, as in PostgreSQL.
                    let ord = cmp_datum(&k[a as usize], &k[b as usize]);
                    let ord = if o.descending { ord.reverse() } else { ord };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                a.cmp(&b)
            });
        }
        if let Some(limit) = request.limit {
            order.truncate(limit);
        }
        result = result.take(&order);
    }
    Ok(result)
}
