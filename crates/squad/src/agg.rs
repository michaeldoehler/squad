//! Aggregation and window functions.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use crate::encoding::{encode_key, encode_key_tuple};
use crate::exec::{collect, compare_keys, ExecCx};
use crate::expr::{Expr, Row};
use crate::plan::{AggCall, AggFunc, Frame, FrameBound, FrameUnit, Plan, WindowCall, WindowFunc};
use crate::types::{compare, Interval, Type, Value};
use crate::value::{self, FuncImpl};
use crate::{Error, Result};

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

    /// Feeds one input row.
    fn feed(&mut self, row: &[Value], cx: &mut ExecCx) -> Result<()> {
        if let Some(f) = &self.call.filter {
            if !f.test(row, cx)? {
                return Ok(());
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
            self.ordered.push((key, args));
            return Ok(());
        }
        self.accumulate(args, cx)
    }

    fn accumulate(&mut self, args: Vec<Value>, cx: &mut ExecCx) -> Result<()> {
        let skip_null = !matches!(
            self.func,
            AggFunc::CountStar | AggFunc::ArrayAgg | AggFunc::JsonAgg { .. } | AggFunc::JsonObjectAgg { .. }
        );
        if skip_null && args.first().is_some_and(Value::is_null) {
            return Ok(());
        }
        if let Some(seen) = &mut self.distinct {
            if !seen.insert(encode_key_tuple(&args)) {
                return Ok(());
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
                match state {
                    None => *state = Some(piece.to_string()),
                    Some(s) => {
                        if let Some(Value::Text(sep)) = args.next() {
                            s.push_str(&sep);
                        }
                        s.push_str(piece);
                    }
                }
            }
            Acc::Array(items) => items.push(first),
            Acc::JsonArray(items) => items.push(to_json(&first, cx)?),
            Acc::JsonObject(map) => {
                let Value::Text(key) = first else {
                    return Err(Error::new(crate::sqlstate::NULL_VALUE_NOT_ALLOWED, "field name must not be null"));
                };
                let v = args.next().unwrap_or(Value::Null);
                map.insert(key, to_json(&v, cx)?);
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
        Ok(())
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

pub(crate) fn aggregate(input: &Plan, groups: &[Expr], aggs: &[AggCall], cx: &mut ExecCx) -> Result<Vec<Row>> {
    let mut node = crate::exec::open(input, cx)?;
    let fresh = || aggs.iter().map(|a| Runner::new(a, &a.func)).collect::<Vec<_>>();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut states: Vec<(Row, Vec<Runner>)> = Vec::new();
    if groups.is_empty() {
        states.push((Vec::new(), fresh()));
    }
    while let Some(row) = node.next(cx)? {
        let slot = if groups.is_empty() {
            0
        } else {
            let mut vals = Vec::with_capacity(groups.len());
            let mut key = Vec::with_capacity(16);
            for g in groups {
                let v = g.eval(&row, cx)?;
                encode_key(&mut key, &v);
                vals.push(v);
            }
            match index.get(&key) {
                Some(i) => *i,
                None => {
                    index.insert(key, states.len());
                    states.push((vals, fresh()));
                    states.len() - 1
                }
            }
        };
        for r in &mut states[slot].1 {
            r.feed(&row, cx)?;
        }
    }
    drop(node);
    let mut out = Vec::with_capacity(states.len());
    for (mut vals, runners) in states {
        for r in runners {
            vals.push(r.finish(cx)?);
        }
        out.push(vals);
    }
    Ok(out)
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

pub(crate) fn window(input: &Plan, calls: &[WindowCall], cx: &mut ExecCx) -> Result<Vec<Row>> {
    let mut rows = collect(input, cx)?;
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
