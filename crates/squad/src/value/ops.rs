//! Operators.

use std::cmp::Ordering;

use rust_decimal::Decimal;

use super::cast::to_text;
use super::datetime as dt;
use super::registry::{FuncImpl, Registry, Ret, P};
use super::{f_json, numeric, pattern};
use crate::error::sqlstate;
use crate::types::{compare, Interval};
use crate::{Error, FnCtx, Result, Type, Value};

pub(crate) fn bad_arg() -> Error {
    Error::internal("unexpected argument type in built-in function")
}

fn float_overflow() -> Error {
    Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow")
}

// ------------------------------------------------------------ comparisons

macro_rules! cmp_ops {
    ($eq:ident, $ne:ident, $lt:ident, $le:ident, $gt:ident, $ge:ident, $variant:path) => {
        cmp_ops!(@one $eq, $variant, |o| o == Ordering::Equal);
        cmp_ops!(@one $ne, $variant, |o| o != Ordering::Equal);
        cmp_ops!(@one $lt, $variant, |o| o == Ordering::Less);
        cmp_ops!(@one $le, $variant, |o| o != Ordering::Greater);
        cmp_ops!(@one $gt, $variant, |o| o == Ordering::Greater);
        cmp_ops!(@one $ge, $variant, |o| o != Ordering::Less);
    };
    (@one $name:ident, $variant:path, $test:expr) => {
        fn $name(_: &FnCtx, a: &[Value]) -> Result<Value> {
            let o = match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => x.cmp(y),
                (x, y) => compare(x, y),
            };
            let test: fn(Ordering) -> bool = $test;
            Ok(Value::Bool(test(o)))
        }
    };
}

cmp_ops!(i2_eq, i2_ne, i2_lt, i2_le, i2_gt, i2_ge, Value::Int2);
cmp_ops!(i4_eq, i4_ne, i4_lt, i4_le, i4_gt, i4_ge, Value::Int4);
cmp_ops!(i8_eq, i8_ne, i8_lt, i8_le, i8_gt, i8_ge, Value::Int8);
cmp_ops!(text_eq, text_ne, text_lt, text_le, text_gt, text_ge, Value::Text);
cmp_ops!(bool_eq, bool_ne, bool_lt, bool_le, bool_gt, bool_ge, Value::Bool);
cmp_ops!(date_eq, date_ne, date_lt, date_le, date_gt, date_ge, Value::Date);
cmp_ops!(ts_eq, ts_ne, ts_lt, ts_le, ts_gt, ts_ge, Value::Timestamp);
cmp_ops!(tstz_eq, tstz_ne, tstz_lt, tstz_le, tstz_gt, tstz_ge, Value::TimestampTz);
cmp_ops!(uuid_eq, uuid_ne, uuid_lt, uuid_le, uuid_gt, uuid_ge, Value::Uuid);

/// Ordering of two values for the generic comparison operators. Rows
/// compare field by field as SQL prescribes: the first pair of fields
/// that differs decides, and a NULL field met before that makes the
/// result unknown (`None`).
fn gen_order(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Record(x), Value::Record(y)) => {
            for (i, j) in x.iter().zip(y) {
                if i.is_null() || j.is_null() {
                    return None;
                }
                match gen_order(i, j)? {
                    Ordering::Equal => {}
                    other => return Some(other),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        _ => Some(compare(a, b)),
    }
}

/// Row equality: false as soon as one pair of fields differs, unknown if
/// no pair differs but some field is NULL.
fn gen_equal(a: &Value, b: &Value) -> Option<bool> {
    match (a, b) {
        (Value::Record(x), Value::Record(y)) if x.len() == y.len() => {
            let mut unknown = false;
            for (i, j) in x.iter().zip(y) {
                if i.is_null() || j.is_null() {
                    unknown = true;
                } else {
                    match gen_equal(i, j) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
            }
            if unknown {
                None
            } else {
                Some(true)
            }
        }
        _ => Some(compare(a, b) == Ordering::Equal),
    }
}

fn tri(b: Option<bool>) -> Result<Value> {
    Ok(b.map_or(Value::Null, Value::Bool))
}

fn gen_eq(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_equal(&a[0], &a[1]))
}
fn gen_ne(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_equal(&a[0], &a[1]).map(|e| !e))
}
fn gen_lt(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_order(&a[0], &a[1]).map(|o| o == Ordering::Less))
}
fn gen_le(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_order(&a[0], &a[1]).map(|o| o != Ordering::Greater))
}
fn gen_gt(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_order(&a[0], &a[1]).map(|o| o == Ordering::Greater))
}
fn gen_ge(_: &FnCtx, a: &[Value]) -> Result<Value> {
    tri(gen_order(&a[0], &a[1]).map(|o| o != Ordering::Less))
}

const GENERIC: [(&str, FuncImpl); 6] =
    [("=", gen_eq), ("<>", gen_ne), ("<", gen_lt), ("<=", gen_le), (">", gen_gt), (">=", gen_ge)];

/// Comparison through the generic total order, for types without a
/// specialized implementation (enums, records).
pub fn generic_comparison(op: &str) -> FuncImpl {
    GENERIC.iter().find(|(n, _)| *n == op).map_or(gen_eq as FuncImpl, |(_, f)| *f)
}

// ------------------------------------------------------------- arithmetic

macro_rules! int_arith {
    ($variant:path, $ty:ty, $name:literal, $add:ident, $sub:ident, $mul:ident, $div:ident, $rem:ident, $neg:ident, $abs:ident,
     $and:ident, $or:ident, $xor:ident, $not:ident, $shl:ident, $shr:ident) => {
        fn $add(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => x.checked_add(*y).map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        fn $sub(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => x.checked_sub(*y).map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        fn $mul(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => x.checked_mul(*y).map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        fn $div(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(_), $variant(0)) => Err(Error::division_by_zero()),
                ($variant(x), $variant(y)) => x.checked_div(*y).map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        pub(crate) fn $rem(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(_), $variant(0)) => Err(Error::division_by_zero()),
                ($variant(x), $variant(y)) => Ok($variant(x.checked_rem(*y).unwrap_or(0))),
                _ => Err(bad_arg()),
            }
        }
        fn $neg(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match &a[0] {
                $variant(x) => x.checked_neg().map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        pub(crate) fn $abs(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match &a[0] {
                $variant(x) => x.checked_abs().map($variant).ok_or_else(|| Error::out_of_range($name)),
                _ => Err(bad_arg()),
            }
        }
        fn $and(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => Ok($variant(x & y)),
                _ => Err(bad_arg()),
            }
        }
        fn $or(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => Ok($variant(x | y)),
                _ => Err(bad_arg()),
            }
        }
        fn $xor(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => Ok($variant(x ^ y)),
                _ => Err(bad_arg()),
            }
        }
        fn $not(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match &a[0] {
                $variant(x) => Ok($variant(!x)),
                _ => Err(bad_arg()),
            }
        }
        fn $shl(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), Value::Int4(n)) => Ok($variant(x.wrapping_shl(*n as u32))),
                _ => Err(bad_arg()),
            }
        }
        fn $shr(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), Value::Int4(n)) => Ok($variant(x.wrapping_shr(*n as u32))),
                _ => Err(bad_arg()),
            }
        }
    };
}

int_arith!(
    Value::Int2,
    i16,
    "smallint",
    i2_add,
    i2_sub,
    i2_mul,
    i2_div,
    i2_rem,
    i2_neg,
    i2_abs,
    i2_and,
    i2_or,
    i2_xor,
    i2_not,
    i2_shl,
    i2_shr
);
int_arith!(
    Value::Int4,
    i32,
    "integer",
    i4_add,
    i4_sub,
    i4_mul,
    i4_div,
    i4_rem,
    i4_neg,
    i4_abs,
    i4_and,
    i4_or,
    i4_xor,
    i4_not,
    i4_shl,
    i4_shr
);
int_arith!(
    Value::Int8,
    i64,
    "bigint",
    i8_add,
    i8_sub,
    i8_mul,
    i8_div,
    i8_rem,
    i8_neg,
    i8_abs,
    i8_and,
    i8_or,
    i8_xor,
    i8_not,
    i8_shl,
    i8_shr
);

macro_rules! float_arith {
    ($variant:path, $add:ident, $sub:ident, $mul:ident, $div:ident, $neg:ident, $abs:ident) => {
        fn $add(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => {
                    let r = x + y;
                    if r.is_infinite() && x.is_finite() && y.is_finite() {
                        return Err(float_overflow());
                    }
                    Ok($variant(r))
                }
                _ => Err(bad_arg()),
            }
        }
        fn $sub(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => {
                    let r = x - y;
                    if r.is_infinite() && x.is_finite() && y.is_finite() {
                        return Err(float_overflow());
                    }
                    Ok($variant(r))
                }
                _ => Err(bad_arg()),
            }
        }
        fn $mul(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => {
                    let r = x * y;
                    if r.is_infinite() && x.is_finite() && y.is_finite() {
                        return Err(float_overflow());
                    }
                    Ok($variant(r))
                }
                _ => Err(bad_arg()),
            }
        }
        fn $div(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match (&a[0], &a[1]) {
                ($variant(x), $variant(y)) => {
                    if *y == 0.0 && !x.is_nan() {
                        return Err(Error::division_by_zero());
                    }
                    let r = x / y;
                    if r.is_infinite() && x.is_finite() {
                        return Err(float_overflow());
                    }
                    Ok($variant(r))
                }
                _ => Err(bad_arg()),
            }
        }
        fn $neg(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match &a[0] {
                $variant(x) => Ok($variant(-x)),
                _ => Err(bad_arg()),
            }
        }
        pub(crate) fn $abs(_: &FnCtx, a: &[Value]) -> Result<Value> {
            match &a[0] {
                $variant(x) => Ok($variant(x.abs())),
                _ => Err(bad_arg()),
            }
        }
    };
}

float_arith!(Value::Float4, f4_add, f4_sub, f4_mul, f4_div, f4_neg, f4_abs);
float_arith!(Value::Float8, f8_add, f8_sub, f8_mul, f8_div, f8_neg, f8_abs);

fn num2(a: &[Value]) -> Result<(Decimal, Decimal)> {
    match (&a[0], &a[1]) {
        (Value::Numeric(x), Value::Numeric(y)) => Ok((*x, *y)),
        _ => Err(bad_arg()),
    }
}

fn num_add(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = num2(a)?;
    // The result carries the larger of the two scales, also when one
    // operand is zero.
    let scale = x.scale().max(y.scale());
    x.checked_add(y).map(|d| Value::Numeric(numeric::rescale(d, scale))).ok_or_else(numeric::overflow)
}
fn num_sub(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = num2(a)?;
    let scale = x.scale().max(y.scale());
    x.checked_sub(y).map(|d| Value::Numeric(numeric::rescale(d, scale))).ok_or_else(numeric::overflow)
}
fn num_mul(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = num2(a)?;
    let scale = (x.scale() + y.scale()).min(numeric::MAX_SCALE);
    x.checked_mul(y).map(|d| Value::Numeric(numeric::rescale(d, scale))).ok_or_else(numeric::overflow)
}
fn num_div(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = num2(a)?;
    numeric::div(x, y).map(Value::Numeric)
}
pub(crate) fn num_rem(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = num2(a)?;
    if y.is_zero() {
        return Err(Error::division_by_zero());
    }
    let scale = x.scale().max(y.scale());
    x.checked_rem(y).map(|d| Value::Numeric(numeric::rescale(d, scale))).ok_or_else(numeric::overflow)
}
fn num_neg(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Numeric(x) => Ok(Value::Numeric(numeric::norm(-*x))),
        _ => Err(bad_arg()),
    }
}
pub(crate) fn num_abs(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Numeric(x) => Ok(Value::Numeric(x.abs())),
        _ => Err(bad_arg()),
    }
}
fn identity(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(a[0].clone())
}

// -------------------------------------------------------------- date/time

fn iv(v: &Value) -> Result<&Interval> {
    match v {
        Value::Interval(i) => Ok(i),
        _ => Err(bad_arg()),
    }
}

fn date_out_of_range() -> Error {
    Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "date out of range")
}

fn date_add_int(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Date(d), Value::Int4(n)) => {
            if *d == dt::DATE_INF || *d == dt::DATE_NINF {
                return Ok(Value::Date(*d));
            }
            d.checked_add(*n).map(Value::Date).ok_or_else(date_out_of_range)
        }
        _ => Err(bad_arg()),
    }
}
fn int_add_date(c: &FnCtx, a: &[Value]) -> Result<Value> {
    date_add_int(c, &[a[1].clone(), a[0].clone()])
}
fn date_sub_int(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Date(d), Value::Int4(n)) => {
            if *d == dt::DATE_INF || *d == dt::DATE_NINF {
                return Ok(Value::Date(*d));
            }
            d.checked_sub(*n).map(Value::Date).ok_or_else(date_out_of_range)
        }
        _ => Err(bad_arg()),
    }
}
fn date_sub_date(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Date(x), Value::Date(y)) => {
            if [*x, *y].iter().any(|d| *d == dt::DATE_INF || *d == dt::DATE_NINF) {
                return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "cannot subtract infinite dates"));
            }
            x.checked_sub(*y).map(Value::Int4).ok_or_else(date_out_of_range)
        }
        _ => Err(bad_arg()),
    }
}
fn date_add_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Date(d) => Ok(Value::Timestamp(dt::ts_add_interval(dt::date_to_ts(*d), iv(&a[1])?)?)),
        _ => Err(bad_arg()),
    }
}
fn iv_add_date(c: &FnCtx, a: &[Value]) -> Result<Value> {
    date_add_iv(c, &[a[1].clone(), a[0].clone()])
}
fn date_sub_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Date(d) => {
            Ok(Value::Timestamp(dt::ts_add_interval(dt::date_to_ts(*d), &dt::neg_interval(iv(&a[1])?)?)?))
        }
        _ => Err(bad_arg()),
    }
}
fn date_add_time(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Date(d), Value::Time(t)) | (Value::Time(t), Value::Date(d)) => {
            Ok(Value::Timestamp(dt::date_to_ts(*d).saturating_add(*t)))
        }
        _ => Err(bad_arg()),
    }
}
fn ts_add_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Timestamp(t), Value::Interval(i)) | (Value::Interval(i), Value::Timestamp(t)) => {
            Ok(Value::Timestamp(dt::ts_add_interval(*t, i)?))
        }
        _ => Err(bad_arg()),
    }
}
fn ts_sub_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Timestamp(t) => Ok(Value::Timestamp(dt::ts_add_interval(*t, &dt::neg_interval(iv(&a[1])?)?)?)),
        _ => Err(bad_arg()),
    }
}
fn tstz_add_iv(c: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::TimestampTz(t), Value::Interval(i)) | (Value::Interval(i), Value::TimestampTz(t)) => {
            Ok(Value::TimestampTz(dt::tstz_add_interval(*t, i, &dt::session_zone(c))?))
        }
        _ => Err(bad_arg()),
    }
}
fn tstz_sub_iv(c: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::TimestampTz(t) => {
            Ok(Value::TimestampTz(dt::tstz_add_interval(*t, &dt::neg_interval(iv(&a[1])?)?, &dt::session_zone(c))?))
        }
        _ => Err(bad_arg()),
    }
}
fn ts_sub_ts(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Timestamp(x), Value::Timestamp(y)) | (Value::TimestampTz(x), Value::TimestampTz(y)) => {
            Ok(Value::Interval(dt::ts_diff(*x, *y)?))
        }
        _ => Err(bad_arg()),
    }
}
fn time_add_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Time(t), Value::Interval(i)) | (Value::Interval(i), Value::Time(t)) => {
            Ok(Value::Time((t + i.micros).rem_euclid(dt::USECS_PER_DAY)))
        }
        _ => Err(bad_arg()),
    }
}
fn time_sub_iv(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Time(t), Value::Interval(i)) => Ok(Value::Time((t - i.micros).rem_euclid(dt::USECS_PER_DAY))),
        _ => Err(bad_arg()),
    }
}
fn time_sub_time(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Time(x), Value::Time(y)) => Ok(Value::Interval(Interval { months: 0, days: 0, micros: x - y })),
        _ => Err(bad_arg()),
    }
}
fn iv_range() -> Error {
    Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "interval out of range")
}
fn iv_add(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (iv(&a[0])?, iv(&a[1])?);
    Ok(Value::Interval(Interval {
        months: x.months.checked_add(y.months).ok_or_else(iv_range)?,
        days: x.days.checked_add(y.days).ok_or_else(iv_range)?,
        micros: x.micros.checked_add(y.micros).ok_or_else(iv_range)?,
    }))
}
fn iv_sub(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (iv(&a[0])?, iv(&a[1])?);
    Ok(Value::Interval(Interval {
        months: x.months.checked_sub(y.months).ok_or_else(iv_range)?,
        days: x.days.checked_sub(y.days).ok_or_else(iv_range)?,
        micros: x.micros.checked_sub(y.micros).ok_or_else(iv_range)?,
    }))
}
fn iv_neg(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Interval(dt::neg_interval(iv(&a[0])?)?))
}
fn iv_mul(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Interval(i), Value::Float8(f)) | (Value::Float8(f), Value::Interval(i)) => {
            Ok(Value::Interval(dt::interval_mul(i, *f)?))
        }
        _ => Err(bad_arg()),
    }
}
fn iv_div(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Interval(_), Value::Float8(f)) if *f == 0.0 => Err(Error::division_by_zero()),
        (Value::Interval(i), Value::Float8(f)) => Ok(Value::Interval(dt::interval_mul(i, 1.0 / *f)?)),
        _ => Err(bad_arg()),
    }
}

// ---------------------------------------------------------------- strings

fn text_cat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Text(x), Value::Text(y)) => {
            let mut s = String::with_capacity(x.len() + y.len());
            s.push_str(x);
            s.push_str(y);
            Ok(Value::Text(s))
        }
        _ => Err(bad_arg()),
    }
}
fn any_cat(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Text(format!("{}{}", to_text(&a[0], c), to_text(&a[1], c))))
}
fn bytea_cat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Bytea(x), Value::Bytea(y)) => Ok(Value::Bytea([x.as_slice(), y.as_slice()].concat())),
        _ => Err(bad_arg()),
    }
}

fn two_text(a: &[Value]) -> Result<(&str, &str)> {
    match (&a[0], &a[1]) {
        (Value::Text(x), Value::Text(y)) => Ok((x, y)),
        _ => Err(bad_arg()),
    }
}

fn like(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    pattern::like_match(t, p).map(Value::Bool)
}
fn not_like(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    pattern::like_match(t, p).map(|b| Value::Bool(!b))
}
fn ilike(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    pattern::like_match(&t.to_lowercase(), &p.to_lowercase()).map(Value::Bool)
}
fn not_ilike(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    pattern::like_match(&t.to_lowercase(), &p.to_lowercase()).map(|b| Value::Bool(!b))
}
fn re_match(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    Ok(Value::Bool(pattern::regex(p, "")?.is_match(t)))
}
fn re_imatch(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    Ok(Value::Bool(pattern::regex(p, "i")?.is_match(t)))
}
fn re_not_match(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    Ok(Value::Bool(!pattern::regex(p, "")?.is_match(t)))
}
fn re_not_imatch(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    Ok(Value::Bool(!pattern::regex(p, "i")?.is_match(t)))
}
fn starts_with_op(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (t, p) = two_text(a)?;
    Ok(Value::Bool(t.starts_with(p)))
}

// ----------------------------------------------------------------- arrays

fn items(v: &Value) -> Result<&[Value]> {
    match v {
        Value::Array(i) => Ok(i),
        _ => Err(bad_arg()),
    }
}

fn arr_cat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(match (&a[0], &a[1]) {
        (Value::Null, Value::Null) => Value::Null,
        (Value::Array(x), Value::Null) | (Value::Null, Value::Array(x)) => Value::Array(x.clone()),
        (Value::Array(x), Value::Array(y)) => Value::Array(x.iter().chain(y).cloned().collect()),
        _ => return Err(bad_arg()),
    })
}
pub(crate) fn arr_append(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut out = match &a[0] {
        Value::Null => Vec::new(),
        Value::Array(x) => x.clone(),
        _ => return Err(bad_arg()),
    };
    out.push(a[1].clone());
    Ok(Value::Array(out))
}
pub(crate) fn arr_prepend(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut out = vec![a[0].clone()];
    match &a[1] {
        Value::Null => {}
        Value::Array(x) => out.extend(x.iter().cloned()),
        _ => return Err(bad_arg()),
    }
    Ok(Value::Array(out))
}
fn has(haystack: &[Value], needle: &Value) -> bool {
    !needle.is_null() && haystack.iter().any(|h| !h.is_null() && compare(h, needle) == Ordering::Equal)
}
fn arr_contains(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (items(&a[0])?, items(&a[1])?);
    Ok(Value::Bool(y.iter().all(|e| has(x, e))))
}
fn arr_contained(c: &FnCtx, a: &[Value]) -> Result<Value> {
    arr_contains(c, &[a[1].clone(), a[0].clone()])
}
fn arr_overlap(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (items(&a[0])?, items(&a[1])?);
    Ok(Value::Bool(y.iter().any(|e| has(x, e))))
}

// ------------------------------------------------------------ registration

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    let b = || Ret::T(Bool);

    // Comparisons.
    type Six = [FuncImpl; 6];
    let generic: Six = [gen_eq, gen_ne, gen_lt, gen_le, gen_gt, gen_ge];
    let specialized: Vec<(Type, Six)> = vec![
        (Bool, [bool_eq, bool_ne, bool_lt, bool_le, bool_gt, bool_ge]),
        (Int2, [i2_eq, i2_ne, i2_lt, i2_le, i2_gt, i2_ge]),
        (Int4, [i4_eq, i4_ne, i4_lt, i4_le, i4_gt, i4_ge]),
        (Int8, [i8_eq, i8_ne, i8_lt, i8_le, i8_gt, i8_ge]),
        (Text, [text_eq, text_ne, text_lt, text_le, text_gt, text_ge]),
        (Date, [date_eq, date_ne, date_lt, date_le, date_gt, date_ge]),
        (Timestamp, [ts_eq, ts_ne, ts_lt, ts_le, ts_gt, ts_ge]),
        (TimestampTz, [tstz_eq, tstz_ne, tstz_lt, tstz_le, tstz_gt, tstz_ge]),
        (Uuid, [uuid_eq, uuid_ne, uuid_lt, uuid_le, uuid_gt, uuid_ge]),
        (Float4, generic),
        (Float8, generic),
        (Numeric, generic),
        (Bytea, generic),
        (Time, generic),
        (Interval, generic),
        (Jsonb, generic),
    ];
    const NAMES: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];
    for (ty, fns) in specialized {
        for (name, f) in NAMES.iter().zip(fns) {
            r.op(name, &[t(ty.clone()), t(ty.clone())], b(), f);
        }
    }
    for (name, f) in NAMES.iter().zip(generic) {
        r.op(name, &[P::Array, P::Array], b(), f);
        r.op(name, &[t(Float4), t(Float8)], b(), f).coerce(&[Float8, Float8]);
        r.op(name, &[t(Float8), t(Float4)], b(), f).coerce(&[Float8, Float8]);
    }

    // Arithmetic.
    type Five = [(&'static str, FuncImpl); 5];
    let ints: Vec<(Type, Five)> = vec![
        (Int2, [("+", i2_add), ("-", i2_sub), ("*", i2_mul), ("/", i2_div), ("%", i2_rem)]),
        (Int4, [("+", i4_add), ("-", i4_sub), ("*", i4_mul), ("/", i4_div), ("%", i4_rem)]),
        (Int8, [("+", i8_add), ("-", i8_sub), ("*", i8_mul), ("/", i8_div), ("%", i8_rem)]),
        (Numeric, [("+", num_add), ("-", num_sub), ("*", num_mul), ("/", num_div), ("%", num_rem)]),
    ];
    for (ty, fns) in ints {
        for (name, f) in fns {
            r.op(name, &[t(ty.clone()), t(ty.clone())], Ret::T(ty.clone()), f);
        }
    }
    type Four = [(&'static str, FuncImpl); 4];
    let floats: Vec<(Type, Four)> = vec![
        (Float4, [("+", f4_add), ("-", f4_sub), ("*", f4_mul), ("/", f4_div)]),
        (Float8, [("+", f8_add), ("-", f8_sub), ("*", f8_mul), ("/", f8_div)]),
    ];
    for (ty, fns) in floats {
        for (name, f) in fns {
            r.op(name, &[t(ty.clone()), t(ty.clone())], Ret::T(ty.clone()), f);
            if ty == Float8 {
                r.op(name, &[t(Float4), t(Float8)], Ret::T(Float8), f).coerce(&[Float8, Float8]);
                r.op(name, &[t(Float8), t(Float4)], Ret::T(Float8), f).coerce(&[Float8, Float8]);
            }
        }
    }
    r.op("^", &[t(Float8), t(Float8)], Ret::T(Float8), super::f_math::f8_power);
    r.op("^", &[t(Numeric), t(Numeric)], Ret::T(Numeric), super::f_math::num_power);
    r.op("|/", &[t(Float8)], Ret::T(Float8), super::f_math::f8_sqrt);
    r.op("||/", &[t(Float8)], Ret::T(Float8), super::f_math::f8_cbrt);
    let unary: Vec<(Type, FuncImpl, FuncImpl)> = vec![
        (Int2, i2_neg, i2_abs),
        (Int4, i4_neg, i4_abs),
        (Int8, i8_neg, i8_abs),
        (Float4, f4_neg, f4_abs),
        (Float8, f8_neg, f8_abs),
        (Numeric, num_neg, num_abs),
    ];
    for (ty, neg, abs) in unary {
        r.op("-", &[t(ty.clone())], Ret::T(ty.clone()), neg);
        r.op("+", &[t(ty.clone())], Ret::T(ty.clone()), identity);
        r.op("@", &[t(ty.clone())], Ret::T(ty.clone()), abs);
    }
    type Bits = (Type, [FuncImpl; 6]);
    let bits: Vec<Bits> = vec![
        (Int2, [i2_and, i2_or, i2_xor, i2_not, i2_shl, i2_shr]),
        (Int4, [i4_and, i4_or, i4_xor, i4_not, i4_shl, i4_shr]),
        (Int8, [i8_and, i8_or, i8_xor, i8_not, i8_shl, i8_shr]),
    ];
    for (ty, [and, or, xor, not, shl, shr]) in bits {
        let p = || t(ty.clone());
        r.op("&", &[p(), p()], Ret::T(ty.clone()), and);
        r.op("|", &[p(), p()], Ret::T(ty.clone()), or);
        r.op("#", &[p(), p()], Ret::T(ty.clone()), xor);
        r.op("~", &[p()], Ret::T(ty.clone()), not);
        r.op("<<", &[p(), t(Int4)], Ret::T(ty.clone()), shl);
        r.op(">>", &[p(), t(Int4)], Ret::T(ty.clone()), shr);
    }

    // Date/time arithmetic.
    r.op("+", &[t(Date), t(Int4)], Ret::T(Date), date_add_int);
    r.op("+", &[t(Int4), t(Date)], Ret::T(Date), int_add_date);
    r.op("-", &[t(Date), t(Int4)], Ret::T(Date), date_sub_int);
    r.op("-", &[t(Date), t(Date)], Ret::T(Int4), date_sub_date);
    r.op("+", &[t(Date), t(Interval)], Ret::T(Timestamp), date_add_iv);
    r.op("+", &[t(Interval), t(Date)], Ret::T(Timestamp), iv_add_date);
    r.op("-", &[t(Date), t(Interval)], Ret::T(Timestamp), date_sub_iv);
    r.op("+", &[t(Date), t(Time)], Ret::T(Timestamp), date_add_time);
    r.op("+", &[t(Time), t(Date)], Ret::T(Timestamp), date_add_time);
    r.op("+", &[t(Timestamp), t(Interval)], Ret::T(Timestamp), ts_add_iv);
    r.op("+", &[t(Interval), t(Timestamp)], Ret::T(Timestamp), ts_add_iv);
    r.op("-", &[t(Timestamp), t(Interval)], Ret::T(Timestamp), ts_sub_iv);
    r.op("-", &[t(Timestamp), t(Timestamp)], Ret::T(Interval), ts_sub_ts);
    r.op("+", &[t(TimestampTz), t(Interval)], Ret::T(TimestampTz), tstz_add_iv).stable();
    r.op("+", &[t(Interval), t(TimestampTz)], Ret::T(TimestampTz), tstz_add_iv).stable();
    r.op("-", &[t(TimestampTz), t(Interval)], Ret::T(TimestampTz), tstz_sub_iv).stable();
    r.op("-", &[t(TimestampTz), t(TimestampTz)], Ret::T(Interval), ts_sub_ts);
    r.op("+", &[t(Time), t(Interval)], Ret::T(Time), time_add_iv);
    r.op("+", &[t(Interval), t(Time)], Ret::T(Time), time_add_iv);
    r.op("-", &[t(Time), t(Interval)], Ret::T(Time), time_sub_iv);
    r.op("-", &[t(Time), t(Time)], Ret::T(Interval), time_sub_time);
    r.op("+", &[t(Interval), t(Interval)], Ret::T(Interval), iv_add);
    r.op("-", &[t(Interval), t(Interval)], Ret::T(Interval), iv_sub);
    r.op("-", &[t(Interval)], Ret::T(Interval), iv_neg);
    r.op("+", &[t(Interval)], Ret::T(Interval), identity);
    r.op("*", &[t(Interval), t(Float8)], Ret::T(Interval), iv_mul);
    r.op("*", &[t(Float8), t(Interval)], Ret::T(Interval), iv_mul);
    r.op("/", &[t(Interval), t(Float8)], Ret::T(Interval), iv_div);

    // Strings.
    r.op("||", &[t(Text), t(Text)], Ret::T(Text), text_cat);
    r.op("||", &[t(Text), P::NonArray], Ret::T(Text), any_cat).stable();
    r.op("||", &[P::NonArray, t(Text)], Ret::T(Text), any_cat).stable();
    r.op("||", &[t(Bytea), t(Bytea)], Ret::T(Bytea), bytea_cat);
    r.op("~~", &[t(Text), t(Text)], b(), like);
    r.op("!~~", &[t(Text), t(Text)], b(), not_like);
    r.op("~~*", &[t(Text), t(Text)], b(), ilike);
    r.op("!~~*", &[t(Text), t(Text)], b(), not_ilike);
    r.op("~", &[t(Text), t(Text)], b(), re_match);
    r.op("~*", &[t(Text), t(Text)], b(), re_imatch);
    r.op("!~", &[t(Text), t(Text)], b(), re_not_match);
    r.op("!~*", &[t(Text), t(Text)], b(), re_not_imatch);
    r.op("^@", &[t(Text), t(Text)], b(), starts_with_op);

    // Arrays.
    r.op("||", &[P::Array, P::Array], Ret::Array, arr_cat).nonstrict();
    r.op("||", &[P::Array, P::Elem], Ret::Array, arr_append).nonstrict();
    r.op("||", &[P::Elem, P::Array], Ret::Array, arr_prepend).nonstrict();
    r.op("@>", &[P::Array, P::Array], b(), arr_contains);
    r.op("<@", &[P::Array, P::Array], b(), arr_contained);
    r.op("&&", &[P::Array, P::Array], b(), arr_overlap);

    f_json::register_ops(r);
}
