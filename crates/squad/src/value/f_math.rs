//! Mathematical functions.

use rand::Rng;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::numeric;
use super::ops::{self, bad_arg};
use super::registry::{Registry, Ret, P};
use crate::error::sqlstate;
use crate::{Error, FnCtx, Result, Type, Value};

fn f8(v: &Value) -> Result<f64> {
    match v {
        Value::Float8(f) => Ok(*f),
        _ => Err(bad_arg()),
    }
}
fn num(v: &Value) -> Result<Decimal> {
    match v {
        Value::Numeric(d) => Ok(*d),
        _ => Err(bad_arg()),
    }
}
fn i4(v: &Value) -> Result<i32> {
    match v {
        Value::Int4(i) => Ok(*i),
        _ => Err(bad_arg()),
    }
}

fn check(r: f64, inputs_finite: bool) -> Result<Value> {
    if r.is_infinite() && inputs_finite {
        return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow"));
    }
    Ok(Value::Float8(r))
}

fn power_err(msg: &str) -> Error {
    Error::new(sqlstate::INVALID_ARGUMENT_FOR_POWER, msg)
}
fn log_err(msg: &str) -> Error {
    Error::new(sqlstate::INVALID_ARGUMENT_FOR_LOG, msg)
}

pub(crate) fn f8_power(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (f8(&a[0])?, f8(&a[1])?);
    if x == 0.0 && y < 0.0 {
        return Err(power_err("zero raised to a negative power is undefined"));
    }
    if x < 0.0 && y.fract() != 0.0 && y.is_finite() {
        return Err(power_err("a negative number raised to a non-integer power yields a complex result"));
    }
    check(x.powf(y), x.is_finite() && y.is_finite())
}
pub(crate) fn f8_sqrt(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = f8(&a[0])?;
    if x < 0.0 {
        return Err(power_err("cannot take square root of a negative number"));
    }
    Ok(Value::Float8(x.sqrt()))
}
pub(crate) fn f8_cbrt(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Float8(f8(&a[0])?.cbrt()))
}

macro_rules! f8_fn {
    ($name:ident, |$x:ident| $body:expr) => {
        fn $name(_: &FnCtx, a: &[Value]) -> Result<Value> {
            let $x = f8(&a[0])?;
            Ok(Value::Float8($body))
        }
    };
}
f8_fn!(f8_ceil, |x| x.ceil());
f8_fn!(f8_floor, |x| x.floor());
f8_fn!(f8_round, |x| x.round_ties_even());
f8_fn!(f8_trunc, |x| x.trunc());
f8_fn!(f8_sign, |x| if x > 0.0 {
    1.0
} else if x < 0.0 {
    -1.0
} else {
    0.0
});
f8_fn!(f8_degrees, |x| x.to_degrees());
f8_fn!(f8_radians, |x| x.to_radians());
f8_fn!(f8_sin, |x| x.sin());
f8_fn!(f8_cos, |x| x.cos());
f8_fn!(f8_tan, |x| x.tan());
f8_fn!(f8_cot, |x| 1.0 / x.tan());
f8_fn!(f8_atan, |x| x.atan());
f8_fn!(f8_sinh, |x| x.sinh());
f8_fn!(f8_cosh, |x| x.cosh());
f8_fn!(f8_tanh, |x| x.tanh());

fn f8_asin(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = f8(&a[0])?;
    if !(-1.0..=1.0).contains(&x) {
        return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "input is out of range"));
    }
    Ok(Value::Float8(x.asin()))
}
fn f8_acos(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = f8(&a[0])?;
    if !(-1.0..=1.0).contains(&x) {
        return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "input is out of range"));
    }
    Ok(Value::Float8(x.acos()))
}
fn f8_atan2(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Float8(f8(&a[0])?.atan2(f8(&a[1])?)))
}
fn f8_exp(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = f8(&a[0])?;
    check(x.exp(), x.is_finite())
}
fn ln_arg(x: f64) -> Result<f64> {
    if x == 0.0 {
        Err(log_err("cannot take logarithm of zero"))
    } else if x < 0.0 {
        Err(log_err("cannot take logarithm of a negative number"))
    } else {
        Ok(x)
    }
}
fn f8_ln(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Float8(ln_arg(f8(&a[0])?)?.ln()))
}
fn f8_log10(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Float8(ln_arg(f8(&a[0])?)?.log10()))
}
fn f8_pi(_: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::Float8(std::f64::consts::PI))
}
fn random(_: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::Float8(rand::thread_rng().gen::<f64>()))
}

// ----------------------------------------------------------------- numeric

fn num_ceil(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::norm(num(&a[0])?.ceil())))
}
fn num_floor(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::norm(num(&a[0])?.floor())))
}
fn num_round(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::round(num(&a[0])?, 0)))
}
fn num_round2(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::round(num(&a[0])?, i4(&a[1])?)))
}
fn num_trunc(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::trunc(num(&a[0])?, 0)))
}
fn num_trunc2(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::trunc(num(&a[0])?, i4(&a[1])?)))
}
fn num_sign(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let d = num(&a[0])?;
    Ok(Value::Numeric(if d.is_zero() {
        Decimal::ZERO
    } else if d.is_sign_negative() {
        Decimal::NEGATIVE_ONE
    } else {
        Decimal::ONE
    }))
}

fn dec_exp(x: Decimal) -> Option<Decimal> {
    if x > Decimal::from(66) {
        return None;
    }
    if x < Decimal::from(-70) {
        return Some(Decimal::ZERO);
    }
    let mut r = x;
    let mut halvings = 0;
    let limit = Decimal::new(1, 1);
    while r.abs() > limit {
        r /= Decimal::TWO;
        halvings += 1;
    }
    let mut term = Decimal::ONE;
    let mut sum = Decimal::ONE;
    for i in 1..60 {
        term = term * r / Decimal::from(i);
        if term.is_zero() {
            break;
        }
        sum += term;
    }
    for _ in 0..halvings {
        sum = sum.checked_mul(sum)?;
    }
    Some(sum)
}

fn dec_ln(x: Decimal) -> Option<Decimal> {
    let mut y = Decimal::from_f64(x.to_f64()?.ln())?;
    for _ in 0..3 {
        let e = dec_exp(y)?;
        y += Decimal::TWO * (x - e) / (x + e);
    }
    Some(y)
}

fn dec_sqrt(x: Decimal) -> Option<Decimal> {
    if x.is_zero() {
        return Some(Decimal::ZERO);
    }
    let mut y = Decimal::from_f64(x.to_f64()?.sqrt())?;
    if y.is_zero() {
        return Some(y);
    }
    for _ in 0..4 {
        y = (y + x / y) / Decimal::TWO;
    }
    Some(y)
}

/// Decimal weight (position of the leading digit) of a value, truncated
/// toward zero like the C cast Postgres uses.
fn dweight(v: f64) -> i32 {
    if v == 0.0 || !v.is_finite() {
        0
    } else {
        v.abs().log10().trunc() as i32
    }
}

fn finish(d: Decimal, rscale: i32) -> Value {
    Value::Numeric(numeric::rescale(d, rscale.clamp(0, numeric::MAX_SCALE as i32) as u32))
}

fn num_sqrt(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = num(&a[0])?;
    if x.is_sign_negative() && !x.is_zero() {
        return Err(power_err("cannot take square root of a negative number"));
    }
    let (w, _) = numeric::weight_and_first_digit(&x);
    let sweight = (w + 1) * 4 / 2 - 1;
    let r = dec_sqrt(x).ok_or_else(numeric::overflow)?;
    Ok(finish(r, (16 - sweight).max(x.scale() as i32)))
}

fn num_ln_arg(x: Decimal) -> Result<Decimal> {
    if x.is_zero() {
        Err(log_err("cannot take logarithm of zero"))
    } else if x.is_sign_negative() {
        Err(log_err("cannot take logarithm of a negative number"))
    } else {
        Ok(x)
    }
}

fn num_ln(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = num_ln_arg(num(&a[0])?)?;
    let r = dec_ln(x).ok_or_else(numeric::overflow)?;
    Ok(finish(r, (16 - dweight(r.to_f64().unwrap_or(0.0))).max(x.scale() as i32)))
}

fn num_exp(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let x = num(&a[0])?;
    let r = dec_exp(x).ok_or_else(numeric::overflow)?;
    let w = (x.to_f64().unwrap_or(0.0) * std::f64::consts::LOG10_E).trunc() as i32;
    Ok(finish(r, (16 - w).max(x.scale() as i32)))
}

fn num_log_base(base: Decimal, x: Decimal) -> Result<Value> {
    let (b, x) = (num_ln_arg(base)?, num_ln_arg(x)?);
    let lb = dec_ln(b).ok_or_else(numeric::overflow)?;
    if lb.is_zero() {
        return Err(Error::division_by_zero());
    }
    let lx = dec_ln(x).ok_or_else(numeric::overflow)?;
    let r = lx / lb;
    let result_dweight = dweight(lx.to_f64().unwrap_or(0.0)) - dweight(lb.to_f64().unwrap_or(0.0));
    let rscale = (16 - result_dweight).max(b.scale() as i32).max(x.scale() as i32);
    Ok(finish(r, rscale))
}
fn num_log10(_: &FnCtx, a: &[Value]) -> Result<Value> {
    num_log_base(Decimal::TEN, num(&a[0])?)
}
fn num_log2(_: &FnCtx, a: &[Value]) -> Result<Value> {
    num_log_base(num(&a[0])?, num(&a[1])?)
}

pub(crate) fn num_power(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (num(&a[0])?, num(&a[1])?);
    if x.is_zero() && y.is_sign_negative() && !y.is_zero() {
        return Err(power_err("zero raised to a negative power is undefined"));
    }
    let y_is_int = y.fract().is_zero();
    if x.is_sign_negative() && !x.is_zero() && !y_is_int {
        return Err(power_err("a negative number raised to a non-integer power yields a complex result"));
    }
    let r = if y_is_int && y.abs() <= Decimal::from(4096) {
        // Exact repeated squaring for integer exponents.
        let mut e = y.abs().to_u32().unwrap_or(0);
        let mut base = x;
        let mut acc = Decimal::ONE;
        while e > 0 {
            if e & 1 == 1 {
                acc = acc.checked_mul(base).ok_or_else(numeric::overflow)?;
            }
            e >>= 1;
            if e > 0 {
                base = base.checked_mul(base).ok_or_else(numeric::overflow)?;
            }
        }
        if y.is_sign_negative() {
            Decimal::ONE.checked_div(acc).ok_or_else(numeric::overflow)?
        } else {
            acc
        }
    } else if x.is_zero() {
        Decimal::ZERO
    } else {
        let l = dec_ln(x).ok_or_else(numeric::overflow)?;
        dec_exp(l.checked_mul(y).ok_or_else(numeric::overflow)?).ok_or_else(numeric::overflow)?
    };
    let rscale = (16 - dweight(r.to_f64().unwrap_or(0.0))).max(x.scale() as i32).max(y.scale() as i32);
    Ok(finish(r, rscale))
}

fn num_div_trunc(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (num(&a[0])?, num(&a[1])?);
    if y.is_zero() {
        return Err(Error::division_by_zero());
    }
    Ok(Value::Numeric(numeric::norm(x.checked_div(y).ok_or_else(numeric::overflow)?.trunc())))
}
fn num_scale(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Int4(num(&a[0])?.scale() as i32))
}
fn num_min_scale(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Int4(num(&a[0])?.normalize().scale() as i32))
}
fn num_trim_scale(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Numeric(numeric::norm(num(&a[0])?.normalize())))
}

fn gcd_i64(mut a: i64, mut b: i64) -> Option<i64> {
    while b != 0 {
        let r = a.checked_rem(b)?;
        a = b;
        b = r;
    }
    a.checked_abs()
}
fn gcd(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match (&a[0], &a[1]) {
        (Value::Int4(x), Value::Int4(y)) => gcd_i64(*x as i64, *y as i64)
            .and_then(|g| i32::try_from(g).ok())
            .map(Value::Int4)
            .ok_or_else(|| Error::out_of_range("integer")),
        (Value::Int8(x), Value::Int8(y)) => {
            gcd_i64(*x, *y).map(Value::Int8).ok_or_else(|| Error::out_of_range("bigint"))
        }
        _ => Err(bad_arg()),
    }
}
fn lcm(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let l = |x: i64, y: i64| -> Option<i64> {
        if x == 0 || y == 0 {
            return Some(0);
        }
        (x / gcd_i64(x, y)?).checked_mul(y)?.checked_abs()
    };
    match (&a[0], &a[1]) {
        (Value::Int4(x), Value::Int4(y)) => l(*x as i64, *y as i64)
            .and_then(|g| i32::try_from(g).ok())
            .map(Value::Int4)
            .ok_or_else(|| Error::out_of_range("integer")),
        (Value::Int8(x), Value::Int8(y)) => l(*x, *y).map(Value::Int8).ok_or_else(|| Error::out_of_range("bigint")),
        _ => Err(bad_arg()),
    }
}

fn width_bucket(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let count = i4(&a[3])?;
    let (op, lo, hi) = match (&a[0], &a[1], &a[2]) {
        (Value::Float8(o), Value::Float8(l), Value::Float8(h)) => (*o, *l, *h),
        (Value::Numeric(o), Value::Numeric(l), Value::Numeric(h)) => {
            (o.to_f64().unwrap_or(0.0), l.to_f64().unwrap_or(0.0), h.to_f64().unwrap_or(0.0))
        }
        _ => return Err(bad_arg()),
    };
    let e = |m: &str| Error::new("2201G", m.to_string());
    if count <= 0 {
        return Err(e("count must be greater than zero"));
    }
    if lo == hi {
        return Err(e("lower bound cannot equal upper bound"));
    }
    let bucket = if lo < hi {
        if op < lo {
            0
        } else if op >= hi {
            count as i64 + 1
        } else {
            ((op - lo) / (hi - lo) * count as f64).floor() as i64 + 1
        }
    } else if op > lo {
        0
    } else if op <= hi {
        count as i64 + 1
    } else {
        ((lo - op) / (lo - hi) * count as f64).floor() as i64 + 1
    };
    i32::try_from(bucket).map(Value::Int4).map_err(|_| Error::out_of_range("integer"))
}

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    let f = || Ret::T(Float8);
    let n = || Ret::T(Numeric);

    r.f("abs", &[t(Int2)], Ret::T(Int2), ops::i2_abs);
    r.f("abs", &[t(Int4)], Ret::T(Int4), ops::i4_abs);
    r.f("abs", &[t(Int8)], Ret::T(Int8), ops::i8_abs);
    r.f("abs", &[t(Float4)], Ret::T(Float4), ops::f4_abs);
    r.f("abs", &[t(Float8)], f(), ops::f8_abs);
    r.f("abs", &[t(Numeric)], n(), ops::num_abs);

    for name in ["ceil", "ceiling"] {
        r.f(name, &[t(Float8)], f(), f8_ceil);
        r.f(name, &[t(Numeric)], n(), num_ceil);
    }
    r.f("floor", &[t(Float8)], f(), f8_floor);
    r.f("floor", &[t(Numeric)], n(), num_floor);
    r.f("round", &[t(Float8)], f(), f8_round);
    r.f("round", &[t(Numeric)], n(), num_round);
    r.f("round", &[t(Numeric), t(Int4)], n(), num_round2);
    r.f("trunc", &[t(Float8)], f(), f8_trunc);
    r.f("trunc", &[t(Numeric)], n(), num_trunc);
    r.f("trunc", &[t(Numeric), t(Int4)], n(), num_trunc2);
    r.f("sign", &[t(Float8)], f(), f8_sign);
    r.f("sign", &[t(Numeric)], n(), num_sign);
    r.f("sqrt", &[t(Float8)], f(), f8_sqrt);
    r.f("sqrt", &[t(Numeric)], n(), num_sqrt);
    r.f("cbrt", &[t(Float8)], f(), f8_cbrt);
    for name in ["power", "pow"] {
        r.f(name, &[t(Float8), t(Float8)], f(), f8_power);
        r.f(name, &[t(Numeric), t(Numeric)], n(), num_power);
    }
    r.f("exp", &[t(Float8)], f(), f8_exp);
    r.f("exp", &[t(Numeric)], n(), num_exp);
    r.f("ln", &[t(Float8)], f(), f8_ln);
    r.f("ln", &[t(Numeric)], n(), num_ln);
    for name in ["log", "log10"] {
        r.f(name, &[t(Float8)], f(), f8_log10);
        r.f(name, &[t(Numeric)], n(), num_log10);
    }
    r.f("log", &[t(Numeric), t(Numeric)], n(), num_log2);
    r.f("mod", &[t(Int2), t(Int2)], Ret::T(Int2), ops::i2_rem);
    r.f("mod", &[t(Int4), t(Int4)], Ret::T(Int4), ops::i4_rem);
    r.f("mod", &[t(Int8), t(Int8)], Ret::T(Int8), ops::i8_rem);
    r.f("mod", &[t(Numeric), t(Numeric)], n(), ops::num_rem);
    r.f("div", &[t(Numeric), t(Numeric)], n(), num_div_trunc);
    r.f("gcd", &[t(Int4), t(Int4)], Ret::T(Int4), gcd);
    r.f("gcd", &[t(Int8), t(Int8)], Ret::T(Int8), gcd);
    r.f("lcm", &[t(Int4), t(Int4)], Ret::T(Int4), lcm);
    r.f("lcm", &[t(Int8), t(Int8)], Ret::T(Int8), lcm);
    r.f("pi", &[], f(), f8_pi);
    r.f("degrees", &[t(Float8)], f(), f8_degrees);
    r.f("radians", &[t(Float8)], f(), f8_radians);
    r.f("sin", &[t(Float8)], f(), f8_sin);
    r.f("cos", &[t(Float8)], f(), f8_cos);
    r.f("tan", &[t(Float8)], f(), f8_tan);
    r.f("cot", &[t(Float8)], f(), f8_cot);
    r.f("asin", &[t(Float8)], f(), f8_asin);
    r.f("acos", &[t(Float8)], f(), f8_acos);
    r.f("atan", &[t(Float8)], f(), f8_atan);
    r.f("atan2", &[t(Float8), t(Float8)], f(), f8_atan2);
    r.f("sinh", &[t(Float8)], f(), f8_sinh);
    r.f("cosh", &[t(Float8)], f(), f8_cosh);
    r.f("tanh", &[t(Float8)], f(), f8_tanh);
    r.f("random", &[], f(), random).volatile();
    r.f("width_bucket", &[t(Float8), t(Float8), t(Float8), t(Int4)], Ret::T(Int4), width_bucket);
    r.f("width_bucket", &[t(Numeric), t(Numeric), t(Numeric), t(Int4)], Ret::T(Int4), width_bucket);
    r.f("scale", &[t(Numeric)], Ret::T(Int4), num_scale);
    r.f("min_scale", &[t(Numeric)], Ret::T(Int4), num_min_scale);
    r.f("trim_scale", &[t(Numeric)], n(), num_trim_scale);
}
