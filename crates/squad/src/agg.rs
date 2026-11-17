//! Aggregation and window functions.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use crate::encoding::{encode_key, encode_key_tuple};
use crate::exec::{compare_keys, sort_orders, ExecCx, Node, SortedNode};
use crate::expr::{Expr, Row};
use crate::plan::{AggCall, AggFunc, Frame, FrameBound, FrameUnit, Plan, WindowCall, WindowFunc};
use crate::spill::{
    json_size, out_of_memory, partition_of, row_size, value_size, Partitions, RowStore, SortOrder, Sorter, SpillFile,
    Stored, ENTRY_OVERHEAD, MAX_LEVEL,
};
use crate::types::{compare, Interval, Type, Value};
use crate::value::{self, FuncImpl};
use crate::{Error, Result};
use std::rc::Rc;

type Json = serde_json::Value;

pub(crate) fn numeric_div() -> FuncImpl {
    static DIV: OnceLock<FuncImpl> = OnceLock::new();
    *DIV.get_or_init(|| {
        value::resolve_operator("/", Some(&Type::Numeric), &Type::Numeric).expect("numeric division").eval
    })
}

fn to_json(v: &Value, cx: &ExecCx) -> Result<Json> {
    match v {
        Value::Null => Ok(Json::Null),
        Value::Jsonb(j) => Ok((**j).clone()),
        other => {
            let f = value::resolve_function("to_jsonb", &[other.type_of()])?;
            match (f.eval)(&cx.fn_ctx, std::slice::from_ref(other))? {
                Value::Jsonb(j) => Ok(*j),
                _ => Ok(Json::Null),
            }
        }
    }
}

/// Running state of one aggregate.
#[derive(Clone)]
enum Acc {
    Count(i64),
    SumInt(i128),
    SumNumeric(Decimal),
    SumFloat(f64),
    SumInterval(Interval),
    Extreme(Option<Value>),
    Bool(Option<bool>),
    Bits(Option<i64>),
    Text(Option<String>),
    Array(Vec<Value>),
    JsonArray(Vec<Json>),
    JsonObject(serde_json::Map<String, Json>),
    /// count, sum, sum of squares
    /// Count, sum and sum of squares as floats, plus the exact sum and
    /// sum of squares for integer and numeric input while they fit.
    Moments(f64, f64, f64, Option<(Decimal, Decimal)>),
}

#[derive(Clone)]
struct Runner<'p> {
    call: &'p AggCall,
    func: &'p AggFunc,
    acc: Acc,
    /// Number of non-null inputs accumulated.
    n: i64,
    distinct: Option<HashSet<Vec<u8>>>,
    /// Buffered `(sort key, arguments)` for `agg(x ORDER BY ...)`.
    ordered: Vec<(Vec<Value>, Vec<Value>)>,
}

fn out_of_range(what: &str) -> Error {
    Error::out_of_range(what)
}

impl<'p> Runner<'p> {
    fn new(call: &'p AggCall, func: &'p AggFunc) -> Runner<'p> {
        let acc = match func {
            AggFunc::CountStar | AggFunc::Count | AggFunc::ApproxCountDistinct => Acc::Count(0),
            AggFunc::ApproxPercentile(_) => Acc::Array(Vec::new()),
            AggFunc::Sum | AggFunc::Avg => match call.ret {
                Type::Float4 | Type::Float8 => Acc::SumFloat(0.0),
                Type::Interval => Acc::SumInterval(Interval::default()),
                // Integer inputs are summed exactly; `ret` only says how
                // the total is reported.
                _ => Acc::SumInt(0),
            },
            AggFunc::Min | AggFunc::Max => Acc::Extreme(None),
            AggFunc::BoolAnd | AggFunc::BoolOr => Acc::Bool(None),
            AggFunc::BitAnd | AggFunc::BitOr => Acc::Bits(None),
            AggFunc::StringAgg => Acc::Text(None),
            AggFunc::ArrayAgg => Acc::Array(Vec::new()),
            AggFunc::JsonAgg { .. } => Acc::JsonArray(Vec::new()),
            AggFunc::JsonObjectAgg { .. } => Acc::JsonObject(serde_json::Map::new()),
            AggFunc::Variance { .. } => {
                Acc::Moments(0.0, 0.0, 0.0, (call.ret == Type::Numeric).then_some((Decimal::ZERO, Decimal::ZERO)))
            }
        };
        // Outside columnar tables an "approximate" distinct count is exact.
        let distinct = call.distinct || matches!(func, AggFunc::ApproxCountDistinct);
        Runner { call, func, acc, n: 0, distinct: distinct.then(HashSet::new), ordered: Vec::new() }
    }

    /// Feeds one input row. Returns by how many bytes the state grew
    /// (an estimate; zero for states of fixed size).
    fn feed(&mut self, row: &[Value], cx: &mut ExecCx) -> Result<usize> {
        if let Some(f) = &self.call.filter {
            if !f.test(row, cx)? {
                return Ok(0);
            }
        }
        let mut args = Vec::with_capacity(self.call.args.len());
        for a in &self.call.args {
            args.push(a.eval(row, cx)?);
        }
        if !self.call.order_by.is_empty() {
            let mut key = Vec::with_capacity(self.call.order_by.len());
            for k in &self.call.order_by {
                key.push(k.expr.eval(row, cx)?);
            }
            let size = row_size(&key) + row_size(&args) + ENTRY_OVERHEAD;
            self.ordered.push((key, args));
            return Ok(size);
        }
        self.accumulate(args, cx)
    }

    fn accumulate(&mut self, args: Vec<Value>, cx: &mut ExecCx) -> Result<usize> {
        let skip_null = !matches!(
            self.func,
            AggFunc::CountStar | AggFunc::ArrayAgg | AggFunc::JsonAgg { .. } | AggFunc::JsonObjectAgg { .. }
        );
        if skip_null && args.first().is_some_and(Value::is_null) {
            return Ok(0);
        }
        let mut grown = 0;
        if let Some(seen) = &mut self.distinct {
            let key = encode_key_tuple(&args);
            grown += key.len() + ENTRY_OVERHEAD;
            if !seen.insert(key) {
                return Ok(0);
            }
        }
        self.n += 1;
        let mut args = args.into_iter();
        let first = args.next().unwrap_or(Value::Null);
        match &mut self.acc {
            Acc::Count(c) => *c += 1,
            Acc::SumInt(sum) => match first {
                Value::Numeric(d) => {
                    self.acc = Acc::SumNumeric(
                        Decimal::from_i128_with_scale(*sum, 0).checked_add(d).ok_or_else(|| out_of_range("numeric"))?,
                    )
                }
                v => *sum += v.as_i64().ok_or_else(|| Error::internal("sum over non-numeric value"))? as i128,
            },
            Acc::SumNumeric(sum) => {
                let d = first.as_decimal().ok_or_else(|| Error::internal("sum over non-numeric value"))?;
                *sum = sum.checked_add(d).ok_or_else(|| out_of_range("numeric"))?;
            }
            Acc::SumFloat(sum) => {
                *sum += first.as_f64().ok_or_else(|| Error::internal("sum over non-numeric value"))?
            }
            Acc::SumInterval(sum) => {
                if let Value::Interval(i) = first {
                    sum.months += i.months;
                    sum.days += i.days;
                    sum.micros += i.micros;
                }
            }
            Acc::Extreme(best) => {
                let want = if matches!(self.func, AggFunc::Max) { Ordering::Greater } else { Ordering::Less };
                if best.as_ref().is_none_or(|b| compare(&first, b) == want) {
                    *best = Some(first);
                }
            }
            Acc::Bool(state) => {
                let b = first.as_bool().unwrap_or(false);
                let and = matches!(self.func, AggFunc::BoolAnd);
                *state = Some(match *state {
                    None => b,
                    Some(s) if and => s && b,
                    Some(s) => s || b,
                });
            }
            Acc::Bits(state) => {
                let b = first.as_i64().unwrap_or(0);
                let and = matches!(self.func, AggFunc::BitAnd);
                *state = Some(match *state {
                    None => b,
                    Some(s) if and => s & b,
                    Some(s) => s | b,
                });
            }
            Acc::Text(state) => {
                let piece = first.as_str().unwrap_or_default();
                grown += piece.len();
                match state {
                    None => *state = Some(piece.to_string()),
                    Some(s) => {
                        if let Some(Value::Text(sep)) = args.next() {
                            grown += sep.len();
                            s.push_str(&sep);
                        }
                        s.push_str(piece);
                    }
                }
            }
            Acc::Array(items) => {
                grown += value_size(&first);
                items.push(first)
            }
            Acc::JsonArray(items) => {
                let j = to_json(&first, cx)?;
                grown += std::mem::size_of::<Json>() + json_size(&j);
                items.push(j)
            }
            Acc::JsonObject(map) => {
                let Value::Text(key) = first else {
                    return Err(Error::new(crate::sqlstate::NULL_VALUE_NOT_ALLOWED, "field name must not be null"));
                };
                let v = to_json(&args.next().unwrap_or(Value::Null), cx)?;
                grown += key.len() + std::mem::size_of::<Json>() + ENTRY_OVERHEAD + json_size(&v);
                map.insert(key, v);
            }
            Acc::Moments(n, sum, sq, exact) => {
                let x = first.as_f64().ok_or_else(|| Error::internal("variance over non-numeric value"))?;
                *n += 1.0;
                *sum += x;
                *sq += x * x;
                if let Some((s, q)) = exact {
                    let next =
                        first.as_decimal().and_then(|d| Some((s.checked_add(d)?, q.checked_add(d.checked_mul(d)?)?)));
                    *exact = next;
                }
            }
        }
        Ok(grown)
    }

    /// The state as a value, to be written to a temporary file.
    fn save(self) -> Value {
        let tagged = |tag: i16, mut rest: Vec<Value>| {
            rest.insert(0, Value::Int2(tag));
            Value::Record(rest)
        };
        let acc = match self.acc {
            Acc::Count(c) => tagged(0, vec![Value::Int8(c)]),
            Acc::SumInt(s) => tagged(1, vec![Value::Bytea(s.to_le_bytes().to_vec())]),
            Acc::SumNumeric(d) => tagged(2, vec![Value::Numeric(d)]),
            Acc::SumFloat(f) => tagged(3, vec![Value::Float8(f)]),
            Acc::SumInterval(i) => tagged(4, vec![Value::Interval(i)]),
            Acc::Extreme(v) => tagged(5, vec![v.unwrap_or(Value::Null)]),
            Acc::Bool(b) => tagged(6, vec![b.map_or(Value::Null, Value::Bool)]),
            Acc::Bits(b) => tagged(7, vec![b.map_or(Value::Null, Value::Int8)]),
            Acc::Text(t) => tagged(8, vec![t.map_or(Value::Null, Value::Text)]),
            Acc::Array(items) => tagged(9, vec![Value::Array(items)]),
            Acc::JsonArray(items) => {
                tagged(10, vec![Value::Array(items.into_iter().map(|j| Value::Jsonb(Box::new(j))).collect())])
            }
            Acc::JsonObject(map) => tagged(11, vec![Value::Jsonb(Box::new(Json::Object(map)))]),
            Acc::Moments(n, s, q, exact) => {
                let (es, eq) = match exact {
                    Some((a, b)) => (Value::Numeric(a), Value::Numeric(b)),
                    None => (Value::Null, Value::Null),
                };
                tagged(12, vec![Value::Float8(n), Value::Float8(s), Value::Float8(q), es, eq])
            }
        };
        let distinct = match self.distinct {
            None => Value::Null,
            Some(set) => Value::Array(set.into_iter().map(Value::Bytea).collect()),
        };
        let ordered = Value::Array(
            self.ordered.into_iter().map(|(k, a)| Value::Record(vec![Value::Record(k), Value::Record(a)])).collect(),
        );
        Value::Record(vec![acc, Value::Int8(self.n), distinct, ordered])
    }

    /// The inverse of [`save`](Runner::save).
    fn restore(call: &'p AggCall, func: &'p AggFunc, saved: Value) -> Result<Runner<'p>> {
        let bad = || Error::internal("malformed aggregate state in a temporary file");
        let Value::Record(parts) = saved else { return Err(bad()) };
        let [acc, n, distinct, ordered]: [Value; 4] = parts.try_into().map_err(|_| bad())?;
        let Value::Record(acc) = acc else { return Err(bad()) };
        let mut acc = acc.into_iter();
        let (Some(Value::Int2(tag)), Some(first)) = (acc.next(), acc.next()) else { return Err(bad()) };
        let opt = |v: Value| if v.is_null() { None } else { Some(v) };
        let acc = match (tag, first) {
            (0, Value::Int8(c)) => Acc::Count(c),
            (1, Value::Bytea(b)) => Acc::SumInt(i128::from_le_bytes(b.try_into().map_err(|_| bad())?)),
            (2, Value::Numeric(d)) => Acc::SumNumeric(d),
            (3, Value::Float8(f)) => Acc::SumFloat(f),
            (4, Value::Interval(i)) => Acc::SumInterval(i),
            (5, v) => Acc::Extreme(opt(v)),
            (6, v) => Acc::Bool(opt(v).and_then(|v| v.as_bool())),
            (7, v) => Acc::Bits(opt(v).and_then(|v| v.as_i64())),
            (8, Value::Text(t)) => Acc::Text(Some(t)),
            (8, Value::Null) => Acc::Text(None),
            (9, Value::Array(items)) => Acc::Array(items),
            (10, Value::Array(items)) => Acc::JsonArray(
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::Jsonb(j) => Ok(*j),
                        _ => Err(bad()),
                    })
                    .collect::<Result<_>>()?,
            ),
            (11, Value::Jsonb(j)) => match *j {
                Json::Object(map) => Acc::JsonObject(map),
                _ => return Err(bad()),
            },
            (12, Value::Float8(cnt)) => {
                let (Some(Value::Float8(s)), Some(Value::Float8(q)), Some(es), Some(eq)) =
                    (acc.next(), acc.next(), acc.next(), acc.next())
                else {
                    return Err(bad());
                };
                let exact = match (es, eq) {
                    (Value::Numeric(a), Value::Numeric(b)) => Some((a, b)),
                    _ => None,
                };
                Acc::Moments(cnt, s, q, exact)
            }
            _ => return Err(bad()),
        };
        let Value::Int8(n) = n else { return Err(bad()) };
        let distinct = match distinct {
            Value::Null => None,
            Value::Array(keys) => Some(
                keys.into_iter()
                    .map(|k| match k {
                        Value::Bytea(b) => Ok(b),
                        _ => Err(bad()),
                    })
                    .collect::<Result<_>>()?,
            ),
            _ => return Err(bad()),
        };
        let Value::Array(ordered) = ordered else { return Err(bad()) };
        let ordered = ordered
            .into_iter()
            .map(|pair| match pair {
                Value::Record(mut kv) if kv.len() == 2 => match (kv.pop(), kv.pop()) {
                    (Some(Value::Record(a)), Some(Value::Record(k))) => Ok((k, a)),
                    _ => Err(bad()),
                },
                _ => Err(bad()),
            })
            .collect::<Result<_>>()?;
        Ok(Runner { call, func, acc, n, distinct, ordered })
    }

    fn finish(mut self, cx: &mut ExecCx) -> Result<Value> {
        if !self.ordered.is_empty() {
            let keys = &self.call.order_by;
            let mut buffered = std::mem::take(&mut self.ordered);
            buffered.sort_by(|a, b| compare_keys(&a.0, &b.0, keys));
            for (_, args) in buffered {
                self.accumulate(args, cx)?;
            }
        }
        let ret = &self.call.ret;
        let avg = matches!(self.func, AggFunc::Avg);
        Ok(match self.acc {
            Acc::Count(c) => Value::Int8(c),
            _ if self.n == 0
                && !matches!(
                    self.func,
                    AggFunc::ArrayAgg | AggFunc::JsonAgg { .. } | AggFunc::JsonObjectAgg { .. }
                ) =>
            {
                Value::Null
            }
            Acc::SumInt(sum) if avg => {
                let total = Decimal::from_i128_with_scale(sum, 0);
                numeric_div()(&cx.fn_ctx, &[Value::Numeric(total), Value::Numeric(Decimal::from(self.n))])?
            }
            Acc::SumInt(sum) => match ret {
                Type::Int8 => Value::Int8(i64::try_from(sum).map_err(|_| out_of_range("bigint"))?),
                _ => Value::Numeric(Decimal::try_from_i128_with_scale(sum, 0).map_err(|_| out_of_range("numeric"))?),
            },
            Acc::SumNumeric(sum) if avg => {
                numeric_div()(&cx.fn_ctx, &[Value::Numeric(sum), Value::Numeric(Decimal::from(self.n))])?
            }
            Acc::SumNumeric(sum) => Value::Numeric(sum),
            Acc::SumFloat(sum) => {
                let v = if avg { sum / self.n as f64 } else { sum };
                match ret {
                    Type::Float4 => Value::Float4(v as f32),
                    _ => Value::Float8(v),
                }
            }
            Acc::SumInterval(sum) if avg => {
                let n = self.n as i128;
                let total = sum.cmp_key() / n;
                let days = total / 86_400_000_000;
                Value::Interval(Interval {
                    months: (days / 30) as i32,
                    days: (days % 30) as i32,
                    micros: (total % 86_400_000_000) as i64,
                })
            }
            Acc::SumInterval(sum) => Value::Interval(sum),
            Acc::Extreme(v) => v.unwrap_or(Value::Null),
            Acc::Bool(b) => b.map_or(Value::Null, Value::Bool),
            Acc::Bits(b) => match (b, ret) {
                (None, _) => Value::Null,
                (Some(b), Type::Int2) => Value::Int2(b as i16),
                (Some(b), Type::Int4) => Value::Int4(b as i32),
                (Some(b), _) => Value::Int8(b),
            },
            Acc::Text(s) => s.map_or(Value::Null, Value::Text),
            Acc::Array(mut items) if matches!(self.func, AggFunc::ApproxPercentile(_)) => {
                let AggFunc::ApproxPercentile(p) = self.func else { unreachable!() };
                if items.is_empty() {
                    return Ok(Value::Null);
                }
                // Exact here: the value at the percentile's rank.
                items.sort_by(compare);
                let rank = ((items.len() as f64 * p).ceil() as usize).clamp(1, items.len());
                items[rank - 1].as_f64().map_or(Value::Null, Value::Float8)
            }
            Acc::Array(items) if self.n == 0 && items.is_empty() => Value::Null,
            Acc::Array(items) => Value::Array(items),
            Acc::JsonArray(items) if items.is_empty() => Value::Null,
            Acc::JsonArray(items) => json_result(Json::Array(items), ret),
            Acc::JsonObject(map) if map.is_empty() && self.n == 0 => Value::Null,
            Acc::JsonObject(map) => json_result(Json::Object(map), ret),
            Acc::Moments(n, sum, sq, exact) => {
                let AggFunc::Variance { sample, stddev } = self.func else { unreachable!() };
                let denom = if *sample { n - 1.0 } else { n };
                if denom <= 0.0 {
                    return Ok(Value::Null);
                }
                // Exact arithmetic, as Postgres does for numeric input:
                // (N * sum(x^2) - sum(x)^2) / (N * (N - 1)).
                let count = Decimal::from(self.n);
                let exact = exact.and_then(|(s, q)| {
                    let numerator = count.checked_mul(q)?.checked_sub(s.checked_mul(s)?)?;
                    let divisor = count.checked_mul(if *sample { count - Decimal::ONE } else { count })?;
                    Some((numerator.max(Decimal::ZERO), divisor))
                });
                if let Some((numerator, divisor)) = exact {
                    if numerator.is_zero() {
                        return Ok(Value::Numeric(Decimal::ZERO));
                    }
                    let var = numeric_div()(&cx.fn_ctx, &[Value::Numeric(numerator), Value::Numeric(divisor)])?;
                    let Value::Numeric(v) = var else { return Ok(var) };
                    if !*stddev {
                        return Ok(Value::Numeric(v));
                    }
                    // The square root is reported at the variance's scale.
                    let sqrt = value::resolve_function("sqrt", &[Type::Numeric])?;
                    return Ok(match (sqrt.eval)(&cx.fn_ctx, &[Value::Numeric(v)])? {
                        Value::Numeric(root) => {
                            let mut r = root.round_dp_with_strategy(
                                v.scale(),
                                rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                            );
                            r.rescale(v.scale());
                            Value::Numeric(r)
                        }
                        other => other,
                    });
                }
                let var = ((sq - sum * sum / n) / denom).max(0.0);
                let v = if *stddev { var.sqrt() } else { var };
                match ret {
                    Type::Numeric => Value::Numeric(
                        Decimal::from_f64(v).map(|d| d.round_dp(16)).ok_or_else(|| out_of_range("numeric"))?,
                    ),
                    _ => Value::Float8(v),
                }
            }
        })
    }
}

fn json_result(tree: Json, ret: &Type) -> Value {
    match (ret, tree) {
        // `json` aggregates print with the spacing Postgres uses.
        (Type::Json, Json::Array(items)) => {
            Value::Json(format!("[{}]", items.iter().map(Json::to_string).collect::<Vec<_>>().join(", ")))
        }
        (Type::Json, Json::Object(map)) => {
            let fields: Vec<String> = map.iter().map(|(k, v)| format!("{} : {v}", Json::String(k.clone()))).collect();
            Value::Json(format!("{{ {} }}", fields.join(", ")))
        }
        (Type::Json, tree) => Value::Json(tree.to_string()),
        (_, tree) => Value::Jsonb(Box::new(tree)),
    }
}

// ------------------------------------------------- ordered-set aggregates
//
// `percentile_cont`, `percentile_disc` and `mode` with WITHIN GROUP are
// planned as `array_agg(x ORDER BY x) FILTER (WHERE x IS NOT NULL)` and a
// final function over the sorted values and the direct argument, which is
// evaluated once per group like any expression over the grouped row.

fn percentile(v: &Value) -> Result<Option<f64>> {
    let Some(p) = v.as_f64() else { return Ok(None) };
    if !(0.0..=1.0).contains(&p) {
        return Err(Error::new(
            crate::sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!(
                "percentile value {} is not between 0 and 1",
                value::format_value(&Value::Float8(p), &Default::default())
            ),
        ));
    }
    Ok(Some(p))
}

/// Applies `one` to the percentile argument: a number, or an array of
/// them giving an array of results. Without input rows the result is
/// NULL; a single percentile is checked even then, as in PostgreSQL.
fn ordered_set(c: &crate::FnCtx, a: &[Value], one: fn(&crate::FnCtx, &[Value], f64) -> Result<Value>) -> Result<Value> {
    let values: &[Value] = match &a[1] {
        Value::Array(v) => v,
        _ => &[],
    };
    match &a[0] {
        Value::Null => Ok(Value::Null),
        Value::Array(ps) => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut out = Vec::with_capacity(ps.len());
            for p in ps {
                out.push(match percentile(p)? {
                    Some(p) => one(c, values, p)?,
                    None => Value::Null,
                });
            }
            Ok(Value::Array(out))
        }
        p => match percentile(p)? {
            Some(p) if !values.is_empty() => one(c, values, p),
            _ => Ok(Value::Null),
        },
    }
}

fn interval_op(op: &str, right: Type) -> FuncImpl {
    value::resolve_operator(op, Some(&Type::Interval), &right).expect("interval operator").eval
}

/// Linear interpolation between neighbouring values, as PostgreSQL's
/// `percentile_cont`: `lo + (hi - lo) * fraction`.
fn cont(c: &crate::FnCtx, values: &[Value], p: f64) -> Result<Value> {
    let pos = p * (values.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    if lo == hi {
        return Ok(values[lo].clone());
    }
    let t = pos - lo as f64;
    Ok(match (&values[lo], &values[hi]) {
        (Value::Float8(x), Value::Float8(y)) => Value::Float8(x + (y - x) * t),
        (x @ Value::Interval(_), y @ Value::Interval(_)) => {
            static OPS: OnceLock<[FuncImpl; 3]> = OnceLock::new();
            let [sub, mul, add] = OPS.get_or_init(|| {
                [interval_op("-", Type::Interval), interval_op("*", Type::Float8), interval_op("+", Type::Interval)]
            });
            let diff = sub(c, &[y.clone(), x.clone()])?;
            let step = mul(c, &[diff, Value::Float8(t)])?;
            add(c, &[x.clone(), step])?
        }
        _ => return Err(Error::internal("percentile_cont over values that are not float8 or interval")),
    })
}

/// The first value whose position in the sorted input is at least the
/// fraction of the input.
fn disc(_: &crate::FnCtx, values: &[Value], p: f64) -> Result<Value> {
    let row = ((p * values.len() as f64).ceil() as usize).clamp(1, values.len());
    Ok(values[row - 1].clone())
}

pub(crate) fn percentile_cont_final(c: &crate::FnCtx, a: &[Value]) -> Result<Value> {
    ordered_set(c, a, cont)
}

pub(crate) fn percentile_disc_final(c: &crate::FnCtx, a: &[Value]) -> Result<Value> {
    ordered_set(c, a, disc)
}

/// The most frequent value; among equally frequent ones the first in
/// sort order.
pub(crate) fn mode_final(_: &crate::FnCtx, a: &[Value]) -> Result<Value> {
    let Value::Array(values) = &a[0] else { return Ok(Value::Null) };
    let mut best: Option<(&Value, usize)> = None;
    let mut i = 0;
    while i < values.len() {
        let mut j = i + 1;
        while j < values.len() && compare(&values[i], &values[j]) == Ordering::Equal {
            j += 1;
        }
        if best.is_none_or(|(_, n)| j - i > n) {
            best = Some((&values[i], j - i));
        }
        i = j;
    }
    Ok(best.map_or(Value::Null, |(v, _)| v.clone()))
}

/// One group of a hash aggregation.
struct Group<'p> {
    key: Vec<u8>,
    vals: Row,
    runners: Vec<Runner<'p>>,
    /// Estimated bytes of key, values and states.
    size: usize,
}

/// An input record of an aggregation pass: an input row, or a group's
/// state saved to a temporary file when the hash table was evicted.
enum Record {
    Row(Row),
    State(Row, Vec<Value>),
}

/// Where an aggregation pass reads from.
enum Source<'p> {
    Node(Box<dyn Node + 'p>),
    File(crate::spill::SpillReader),
}

impl Source<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Record>> {
        match self {
            Source::Node(n) => Ok(n.next(cx)?.map(Record::Row)),
            Source::File(f) => {
                let Some(mut rec) = f.next_row()? else { return Ok(None) };
                let bad = || Error::internal("malformed aggregation record in a temporary file");
                if rec.is_empty() {
                    return Err(bad());
                }
                match rec.remove(0) {
                    Value::Bool(false) => Ok(Some(Record::Row(rec))),
                    Value::Bool(true) => match (rec.pop(), rec.pop()) {
                        (Some(Value::Record(states)), Some(Value::Record(vals))) => {
                            Ok(Some(Record::State(vals, states)))
                        }
                        _ => Err(bad()),
                    },
                    _ => Err(bad()),
                }
            }
        }
    }
}

/// How a pass treats groups it has not seen yet.
#[derive(PartialEq, Eq)]
enum Mode {
    /// Add them to the hash table.
    Normal,
    /// The table is full: rows of new groups go to partitions on disk,
    /// rows of groups in the table are still aggregated.
    NoNewGroups,
    /// The table was evicted to disk because its states kept growing:
    /// every record goes to the partitions.
    Evicted,
}

/// Hash aggregation. Groups are kept in a hash table of at most
/// `2 × work_mem`. Once it holds `work_mem`, rows of new groups are written
/// to partitions on disk (by a hash of the group key) while the groups in
/// the table keep aggregating; if their states grow past the limit, they
/// are written to the partitions too. Each partition is then aggregated
/// the same way, one after the other.
struct AggNode<'p> {
    plan: &'p Plan,
    groups: &'p [Expr],
    aggs: &'p [AggCall],
    ready: std::vec::IntoIter<Row>,
    pending: Vec<(Rc<SpillFile>, u32)>,
    batches: usize,
    peak: usize,
    disk: u64,
}

pub(crate) fn aggregate<'p>(
    plan: &'p Plan,
    input: &'p Plan,
    groups: &'p [Expr],
    aggs: &'p [AggCall],
    cx: &mut ExecCx,
) -> Result<Box<dyn Node + 'p>> {
    let source = Source::Node(crate::exec::open(input, cx)?);
    let mut node = AggNode {
        plan,
        groups,
        aggs,
        ready: Vec::new().into_iter(),
        pending: Vec::new(),
        batches: 1,
        peak: 0,
        disk: 0,
    };
    node.pass(source, 0, cx)?;
    Ok(Box::new(node))
}

impl<'p> AggNode<'p> {
    fn new_group(&self, key: Vec<u8>, vals: Row, saved: Option<Vec<Value>>) -> Result<Group<'p>> {
        let runners = match saved {
            None => self.aggs.iter().map(|a| Runner::new(a, &a.func)).collect(),
            Some(states) => {
                if states.len() != self.aggs.len() {
                    return Err(Error::internal("aggregate state does not match the aggregates"));
                }
                let mut runners = Vec::with_capacity(states.len());
                for (a, s) in self.aggs.iter().zip(states) {
                    let size = value_size(&s);
                    runners.push((Runner::restore(a, &a.func, s)?, size));
                }
                let size = runners.iter().map(|r| r.1).sum::<usize>();
                let mut g = self.group_of(key, vals, runners.into_iter().map(|r| r.0).collect());
                g.size += size;
                return Ok(g);
            }
        };
        Ok(self.group_of(key, vals, runners))
    }

    fn group_of(&self, key: Vec<u8>, vals: Row, runners: Vec<Runner<'p>>) -> Group<'p> {
        let size = key.len() + row_size(&vals) + runners.len() * std::mem::size_of::<Runner>() + 2 * ENTRY_OVERHEAD;
        Group { key, vals, runners, size }
    }

    /// Aggregates the records of `source` at partitioning `level`: the
    /// finished groups go to `ready`, spilled partitions to `pending`.
    fn pass(&mut self, mut source: Source<'p>, level: u32, cx: &mut ExecCx) -> Result<()> {
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut table: Vec<Group<'p>> = Vec::new();
        if self.groups.is_empty() {
            table.push(self.new_group(Vec::new(), Vec::new(), None)?);
        }
        let mut used = table.iter().map(|g| g.size).sum::<usize>();
        let mut mode = Mode::Normal;
        let mut parts: Option<Partitions> = None;
        while let Some(rec) = source.next(cx)? {
            cx.check_interrupts()?;
            let slot = if self.groups.is_empty() {
                0
            } else {
                let mut key = Vec::with_capacity(16);
                let vals = match &rec {
                    Record::Row(row) => {
                        let mut vals = Vec::with_capacity(self.groups.len());
                        for g in self.groups {
                            let v = g.eval(row, cx)?;
                            encode_key(&mut key, &v);
                            vals.push(v);
                        }
                        Some(vals)
                    }
                    Record::State(vals, _) => {
                        for v in vals {
                            encode_key(&mut key, v);
                        }
                        None
                    }
                };
                match index.get(&key) {
                    Some(&i) => {
                        if matches!(rec, Record::State(..)) {
                            return Err(Error::internal("aggregate state of a group that is in memory"));
                        }
                        i
                    }
                    None if mode == Mode::Normal => {
                        let group = match rec {
                            Record::Row(_) => self.new_group(key.clone(), vals.unwrap_or_default(), None)?,
                            Record::State(vals, states) => {
                                let g = self.new_group(key.clone(), vals, Some(states))?;
                                used += g.size;
                                index.insert(key, table.len());
                                table.push(g);
                                self.check_memory(&mut used, &mut mode, level, &mut table, &mut index, &mut parts, cx)?;
                                continue;
                            }
                        };
                        used += group.size;
                        index.insert(key, table.len());
                        table.push(group);
                        table.len() - 1
                    }
                    None => {
                        let p = parts.get_or_insert_with(|| Partitions::new(cx));
                        p.write(partition_of(&key, level), &record_row(rec))?;
                        continue;
                    }
                }
            };
            let Record::Row(row) = rec else {
                return Err(Error::internal("aggregate state without GROUP BY"));
            };
            let group = &mut table[slot];
            let mut grown = 0;
            for r in &mut group.runners {
                grown += r.feed(&row, cx)?;
            }
            group.size += grown;
            used += grown;
            self.check_memory(&mut used, &mut mode, level, &mut table, &mut index, &mut parts, cx)?;
        }
        drop(source);
        let mut out = Vec::with_capacity(table.len());
        for g in table {
            let mut vals = g.vals;
            for r in g.runners {
                vals.push(r.finish(cx)?);
            }
            out.push(vals);
        }
        self.ready = out.into_iter();
        if let Some(parts) = parts {
            let files = parts.finish()?;
            self.disk += files.iter().flatten().map(|f| f.bytes()).sum::<u64>();
            self.pending.extend(files.into_iter().flatten().map(|f| (f, level + 1)));
        }
        Ok(())
    }

    /// Reacts to the table's memory use: stop taking new groups at
    /// `work_mem`, write the table out at twice that.
    #[allow(clippy::too_many_arguments)]
    fn check_memory(
        &mut self,
        used: &mut usize,
        mode: &mut Mode,
        level: u32,
        table: &mut Vec<Group<'p>>,
        index: &mut HashMap<Vec<u8>, usize>,
        parts: &mut Option<Partitions>,
        cx: &mut ExecCx,
    ) -> Result<()> {
        self.peak = self.peak.max(*used);
        let (work_mem, hash_mem) = (cx.work_mem(), cx.hash_mem());
        if *used > work_mem && *mode == Mode::Normal && !self.groups.is_empty() {
            *mode = Mode::NoNewGroups;
        }
        if *used <= hash_mem {
            return Ok(());
        }
        if self.groups.is_empty() || level >= MAX_LEVEL || table.iter().any(|g| g.size > work_mem) {
            return Err(out_of_memory("the state of an aggregate", work_mem));
        }
        let p = parts.get_or_insert_with(|| Partitions::new(cx));
        for g in table.drain(..) {
            cx.check_interrupts()?;
            let states: Vec<Value> = g.runners.into_iter().map(Runner::save).collect();
            p.write(partition_of(&g.key, level), &[Value::Bool(true), Value::Record(g.vals), Value::Record(states)])?;
        }
        index.clear();
        *used = 0;
        *mode = Mode::Evicted;
        Ok(())
    }
}

/// A record as it is written to a partition.
fn record_row(rec: Record) -> Row {
    match rec {
        Record::Row(row) => {
            let mut out = Vec::with_capacity(row.len() + 1);
            out.push(Value::Bool(false));
            out.extend(row);
            out
        }
        Record::State(v, states) => vec![Value::Bool(true), Value::Record(v), Value::Record(states)],
    }
}

impl Node for AggNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.ready.next() {
                return Ok(Some(r));
            }
            let Some((file, level)) = self.pending.pop() else {
                let mut line = format!("Batches: {}  Memory Usage: {}kB", self.batches, self.peak.div_ceil(1024));
                if self.disk > 0 {
                    line.push_str(&format!("  Disk Usage: {}kB", self.disk.div_ceil(1024)));
                }
                cx.note(self.plan, line);
                return Ok(None);
            };
            self.batches += 1;
            let source = Source::File(file.reader()?);
            drop(file);
            self.pass(source, level, cx)?;
        }
    }
}

/// Positions `[start, end)` of the frame of the row at `pos` within its
/// partition.
#[allow(clippy::too_many_arguments)]
fn frame_bounds(
    frame: &Frame,
    pos: usize,
    n: usize,
    peers: &[(usize, usize)],
    order_vals: &[&Vec<Value>],
    offsets: (Option<&Value>, Option<&Value>),
    desc: bool,
    cx: &mut ExecCx,
) -> Result<(usize, usize)> {
    let as_count = |v: Option<&Value>| -> Result<usize> {
        match v.and_then(Value::as_i64) {
            Some(k) if k >= 0 => Ok(k as usize),
            _ => Err(Error::new("22013", "frame starting or ending offset must not be null or negative")),
        }
    };
    let (peer_start, peer_end) = peers[pos];
    // Index of the peer group containing a position.
    let group_of = |p: usize| peers[..=p].iter().map(|g| g.0).collect::<HashSet<_>>().len() - 1;
    let group_starts: Vec<usize> = {
        let mut s: Vec<usize> = peers.iter().map(|g| g.0).collect();
        s.dedup();
        s
    };
    let range_pos = |offset: Option<&Value>, preceding: bool, is_start: bool, cx: &mut ExecCx| -> Result<usize> {
        let off = offset.ok_or_else(|| Error::internal("frame offset missing"))?;
        let cur = &order_vals[pos][0];
        if cur.is_null() {
            return Ok(if is_start { peer_start } else { peer_end });
        }
        // Moving "preceding" means towards smaller values when ascending.
        let subtract = preceding != desc;
        let op = if subtract { "-" } else { "+" };
        let f = value::resolve_operator(op, Some(&cur.type_of()), &off.type_of())?;
        let target = (f.eval)(&cx.fn_ctx, &[cur.clone(), off.clone()])?;
        // NULLs are peers of each other only and sit at one end of the
        // sorted partition: leading NULLs precede every frame of a
        // non-null row, trailing ones follow it.
        let nulls_first = order_vals.first().is_some_and(|v| v[0].is_null());
        let before = |v: &Value| -> bool {
            if v.is_null() {
                return nulls_first;
            }
            let o = compare(v, &target);
            let o = if desc { o.reverse() } else { o };
            if is_start {
                o == Ordering::Less
            } else {
                o != Ordering::Greater
            }
        };
        Ok(order_vals.iter().take_while(|v| before(&v[0])).count())
    };
    let start = match (&frame.start, frame.unit) {
        (FrameBound::UnboundedPreceding, _) => 0,
        (FrameBound::CurrentRow, FrameUnit::Rows) => pos,
        (FrameBound::CurrentRow, _) => peer_start,
        (FrameBound::Preceding(_), FrameUnit::Rows) => pos.saturating_sub(as_count(offsets.0)?),
        (FrameBound::Following(_), FrameUnit::Rows) => (pos + as_count(offsets.0)?).min(n),
        (FrameBound::Preceding(_), FrameUnit::Groups) => {
            group_starts[group_of(pos).saturating_sub(as_count(offsets.0)?)]
        }
        (FrameBound::Following(_), FrameUnit::Groups) => {
            group_starts.get(group_of(pos) + as_count(offsets.0)?).copied().unwrap_or(n)
        }
        (FrameBound::Preceding(_), FrameUnit::Range) => range_pos(offsets.0, true, true, cx)?,
        (FrameBound::Following(_), FrameUnit::Range) => range_pos(offsets.0, false, true, cx)?,
        (FrameBound::UnboundedFollowing, _) => n,
    };
    let end = match (&frame.end, frame.unit) {
        (FrameBound::UnboundedFollowing, _) => n,
        (FrameBound::CurrentRow, FrameUnit::Rows) => pos + 1,
        (FrameBound::CurrentRow, _) => peer_end,
        (FrameBound::Preceding(_), FrameUnit::Rows) => (pos + 1).saturating_sub(as_count(offsets.1)?),
        (FrameBound::Following(_), FrameUnit::Rows) => (pos + 1 + as_count(offsets.1)?).min(n),
        (FrameBound::Preceding(_), FrameUnit::Groups) => {
            let k = as_count(offsets.1)?;
            let g = group_of(pos);
            if k > g {
                0
            } else {
                group_starts.get(g - k + 1).copied().unwrap_or(n)
            }
        }
        (FrameBound::Following(_), FrameUnit::Groups) => {
            group_starts.get(group_of(pos) + as_count(offsets.1)? + 1).copied().unwrap_or(n)
        }
        (FrameBound::Preceding(_), FrameUnit::Range) => range_pos(offsets.1, true, false, cx)?,
        (FrameBound::Following(_), FrameUnit::Range) => range_pos(offsets.1, false, false, cx)?,
        (FrameBound::UnboundedPreceding, _) => 0,
    };
    Ok((start.min(n), end.min(n).max(start.min(n))))
}

fn window_partition(
    call: &WindowCall,
    rows: &[Row],
    part: &[usize],
    order_vals: &[Vec<Value>],
    cx: &mut ExecCx,
) -> Result<Vec<Value>> {
    let n = part.len();
    let ordered: Vec<&Vec<Value>> = part.iter().map(|&i| &order_vals[i]).collect();
    // Peer groups: rows equal under the window ordering.
    let mut peers = vec![(0usize, 0usize); n];
    let mut start = 0;
    for i in 0..=n {
        if i == n || (i > start && compare_keys(ordered[i], ordered[start], &call.order_by) != Ordering::Equal) {
            for p in &mut peers[start..i] {
                *p = (start, i);
            }
            start = i;
        }
    }
    let arg = |k: usize, pos: usize, cx: &mut ExecCx| -> Result<Value> {
        match call.args.get(k) {
            Some(e) => e.eval(&rows[part[pos]], cx),
            None => Ok(Value::Null),
        }
    };
    let frame_offsets = |pos: usize, cx: &mut ExecCx| -> Result<(Option<Value>, Option<Value>)> {
        let eval = |b: &FrameBound, cx: &mut ExecCx| -> Result<Option<Value>> {
            match b {
                FrameBound::Preceding(e) | FrameBound::Following(e) => Ok(Some(e.eval(&rows[part[pos]], cx)?)),
                _ => Ok(None),
            }
        };
        Ok((eval(&call.frame.start, cx)?, eval(&call.frame.end, cx)?))
    };
    let desc = call.order_by.first().is_some_and(|k| k.desc);
    let bounds = |pos: usize, cx: &mut ExecCx| -> Result<(usize, usize)> {
        let (a, b) = frame_offsets(pos, cx)?;
        frame_bounds(&call.frame, pos, n, &peers, &ordered, (a.as_ref(), b.as_ref()), desc, cx)
    };

    let mut out = Vec::with_capacity(n);
    match &call.func {
        WindowFunc::RowNumber => out.extend((1..=n as i64).map(Value::Int8)),
        WindowFunc::Rank => out.extend(peers.iter().map(|p| Value::Int8(p.0 as i64 + 1))),
        WindowFunc::DenseRank => {
            let mut rank = 0;
            for (i, p) in peers.iter().enumerate() {
                if p.0 == i {
                    rank += 1;
                }
                out.push(Value::Int8(rank));
            }
        }
        WindowFunc::PercentRank => {
            out.extend(peers.iter().map(|p| Value::Float8(if n > 1 { p.0 as f64 / (n - 1) as f64 } else { 0.0 })))
        }
        WindowFunc::CumeDist => out.extend(peers.iter().map(|p| Value::Float8(p.1 as f64 / n as f64))),
        WindowFunc::Ntile => {
            for pos in 0..n {
                let buckets = match arg(0, pos, cx)?.as_i64() {
                    Some(b) if b > 0 => b as usize,
                    None => {
                        out.push(Value::Null);
                        continue;
                    }
                    _ => return Err(Error::new("22014", "argument of ntile must be greater than zero")),
                };
                let (size, extra) = (n / buckets, n % buckets);
                // The first `extra` buckets hold one row more.
                let big = extra * (size + 1);
                let bucket = if pos < big { pos / (size + 1) } else { extra + (pos - big) / size.max(1) };
                out.push(Value::Int4(bucket as i32 + 1));
            }
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            for pos in 0..n {
                let off = match call.args.get(1) {
                    Some(_) => match arg(1, pos, cx)?.as_i64() {
                        Some(o) => o,
                        None => {
                            out.push(Value::Null);
                            continue;
                        }
                    },
                    None => 1,
                };
                let off = if matches!(call.func, WindowFunc::Lag) { -off } else { off };
                let target = pos as i64 + off;
                out.push(if target >= 0 && (target as usize) < n {
                    arg(0, target as usize, cx)?
                } else {
                    arg(2, pos, cx)?
                });
            }
        }
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => {
            for pos in 0..n {
                let (s, e) = bounds(pos, cx)?;
                let target = match call.func {
                    WindowFunc::FirstValue => (s < e).then_some(s),
                    WindowFunc::LastValue => (s < e).then(|| e - 1),
                    _ => match arg(1, pos, cx)?.as_i64() {
                        Some(k) if k >= 1 => Some(s + k as usize - 1).filter(|t| *t < e),
                        Some(_) => return Err(Error::new("22016", "argument of nth_value must be greater than zero")),
                        None => None,
                    },
                };
                out.push(match target {
                    Some(t) => arg(0, t, cx)?,
                    None => Value::Null,
                });
            }
        }
        WindowFunc::Agg(func) => {
            let proto = AggCall {
                func: func.clone(),
                args: call.args.clone(),
                distinct: false,
                filter: call.filter.clone(),
                order_by: Vec::new(),
                ret: call.ret.clone(),
            };
            let growing = matches!(call.frame.start, FrameBound::UnboundedPreceding);
            let mut running = Runner::new(&proto, func);
            let mut fed = 0;
            for pos in 0..n {
                let (s, e) = bounds(pos, cx)?;
                if growing && e >= fed {
                    // The frame only ever grows: extend the running state.
                    for p in fed..e {
                        running.feed(&rows[part[p]], cx)?;
                    }
                    fed = e;
                    out.push(running.clone().finish(cx)?);
                } else {
                    let mut r = Runner::new(&proto, func);
                    for p in s..e {
                        r.feed(&rows[part[p]], cx)?;
                    }
                    out.push(r.finish(cx)?);
                }
            }
        }
    }
    Ok(out)
}

/// Window functions. While the input fits into `work_mem` it is processed
/// in memory. Otherwise it goes to a temporary file and each window call
/// sorts it by partition (external sort), computes one partition at a time
/// in memory, and a final sort restores the input order. A single
/// partition larger than `work_mem` fails with SQLSTATE 53200.
pub(crate) fn window<'p>(
    plan: &'p Plan,
    input: &'p Plan,
    calls: &'p [WindowCall],
    cx: &mut ExecCx,
) -> Result<Box<dyn Node + 'p>> {
    let budget = cx.work_mem();
    let mut node = crate::exec::open(input, cx)?;
    let mut rows = Vec::new();
    let mut used = 0;
    let mut spilled: Option<RowStore> = None;
    while let Some(r) = node.next(cx)? {
        cx.check_interrupts()?;
        if let Some(s) = &mut spilled {
            s.push(r, cx)?;
            continue;
        }
        used += row_size(&r) + ENTRY_OVERHEAD;
        rows.push(r);
        if used > budget {
            let mut s = RowStore::new(cx);
            for r in std::mem::take(&mut rows) {
                s.push(r, cx)?;
            }
            spilled = Some(s);
        }
    }
    drop(node);
    match spilled {
        None => {
            cx.note(plan, format!("Storage: Memory  Maximum Storage: {}kB", used.div_ceil(1024)));
            Ok(crate::exec::rows(window_in_memory(rows, calls, cx)?))
        }
        Some(s) => window_spilled(plan, s.finish()?, calls, cx),
    }
}

fn window_spilled<'p>(
    plan: &'p Plan,
    input: Rc<Stored>,
    calls: &'p [WindowCall],
    cx: &mut ExecCx,
) -> Result<Box<dyn Node + 'p>> {
    let budget = cx.work_mem();
    let mut disk = 0;
    let mut current = input;
    // Each row carries its input position as a last column.
    let mut numbered = false;
    for call in calls {
        let mut orders = vec![SortOrder::ASC];
        orders.extend(sort_orders(&call.order_by));
        orders.push(SortOrder::ASC);
        let mut sorter = Sorter::new(orders, cx);
        let mut it = current.iter()?;
        let mut seq = 0i64;
        while let Some(mut row) = it.next_row()? {
            cx.check_interrupts()?;
            if !numbered {
                row.push(Value::Int8(seq));
                seq += 1;
            }
            let mut part = Vec::new();
            for p in &call.partition_by {
                encode_key(&mut part, &p.eval(&row, cx)?);
            }
            let mut keys = Vec::with_capacity(call.order_by.len() + 2);
            keys.push(Value::Bytea(part));
            for k in &call.order_by {
                keys.push(k.expr.eval(&row, cx)?);
            }
            keys.push(row.last().cloned().unwrap_or(Value::Null));
            sorter.push(keys, row, cx)?;
        }
        drop(it);
        numbered = true;
        let (mut sorted, stats) = sorter.finish(cx)?;
        disk += stats.disk;
        let mut next = RowStore::new(cx);
        let mut part_rows: Vec<Row> = Vec::new();
        let mut part_vals: Vec<Vec<Value>> = Vec::new();
        let mut part_key: Option<Value> = None;
        let mut used = 0;
        loop {
            let item = sorted.next()?;
            let boundary = match (&item, &part_key) {
                (Some((k, _)), Some(pk)) => compare(&k[0], pk) != Ordering::Equal,
                _ => true,
            };
            if boundary && !part_rows.is_empty() {
                let idx: Vec<usize> = (0..part_rows.len()).collect();
                let vals = window_partition(call, &part_rows, &idx, &part_vals, cx)?;
                for (mut row, v) in std::mem::take(&mut part_rows).into_iter().zip(vals) {
                    let seq = row.pop().unwrap_or(Value::Null);
                    row.push(v);
                    row.push(seq);
                    next.push(row, cx)?;
                }
                part_vals.clear();
                used = 0;
            }
            let Some((mut keys, row)) = item else { break };
            cx.check_interrupts()?;
            keys.pop();
            part_key = Some(keys.remove(0));
            used += row_size(&row) + row_size(&keys) + ENTRY_OVERHEAD;
            if used > budget {
                return Err(out_of_memory("a window partition", budget));
            }
            part_rows.push(row);
            part_vals.push(keys);
        }
        current = next.finish()?;
    }
    // Back to the input order.
    let mut sorter = Sorter::new(vec![SortOrder::ASC], cx);
    let mut it = current.iter()?;
    while let Some(mut row) = it.next_row()? {
        cx.check_interrupts()?;
        let seq = row.pop().unwrap_or(Value::Null);
        sorter.push(vec![seq], row, cx)?;
    }
    drop(it);
    drop(current);
    let (sorted, stats) = sorter.finish(cx)?;
    disk += stats.disk;
    cx.note(plan, format!("Storage: Disk  Maximum Storage: {}kB", disk.div_ceil(1024)));
    Ok(Box::new(SortedNode(sorted)))
}

fn window_in_memory(mut rows: Vec<Row>, calls: &[WindowCall], cx: &mut ExecCx) -> Result<Vec<Row>> {
    let n = rows.len();
    let mut extra: Vec<Vec<Value>> = vec![Vec::with_capacity(calls.len()); n];
    for call in calls {
        let mut part_keys = Vec::with_capacity(n);
        let mut order_vals = Vec::with_capacity(n);
        for row in &rows {
            let mut key = Vec::new();
            for p in &call.partition_by {
                encode_key(&mut key, &p.eval(row, cx)?);
            }
            part_keys.push(key);
            let mut vals = Vec::with_capacity(call.order_by.len());
            for k in &call.order_by {
                vals.push(k.expr.eval(row, cx)?);
            }
            order_vals.push(vals);
        }
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by(|&a, &b| {
            part_keys[a].cmp(&part_keys[b]).then_with(|| compare_keys(&order_vals[a], &order_vals[b], &call.order_by))
        });
        let mut start = 0;
        while start < n {
            let mut end = start + 1;
            while end < n && part_keys[idx[end]] == part_keys[idx[start]] {
                end += 1;
            }
            let part = &idx[start..end];
            let vals = window_partition(call, &rows, part, &order_vals, cx)?;
            for (&row, v) in part.iter().zip(vals) {
                extra[row].push(v);
            }
            start = end;
        }
    }
    for (row, e) in rows.iter_mut().zip(extra) {
        row.extend(e);
    }
    let _ = ToPrimitive::to_f64(&Decimal::ZERO);
    Ok(rows)
}
