//! Helpers for `numeric`, backed by [`rust_decimal::Decimal`].

use std::str::FromStr;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};

use crate::error::sqlstate;
use crate::{Error, Result};

pub const MAX_SCALE: u32 = 28;

pub fn overflow() -> Error {
    Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "value overflows numeric format")
}

/// Removes the sign of a zero so that it prints as `0`, not `-0`.
pub fn norm(mut d: Decimal) -> Decimal {
    if d.is_zero() {
        d.set_sign_positive(true);
    }
    d
}

pub fn parse_numeric(input: &str) -> Result<Decimal> {
    let s = input.trim();
    let bad = || Error::invalid_text("numeric", input);
    if s.is_empty() {
        return Err(bad());
    }
    let lower = s.to_ascii_lowercase();
    if matches!(lower.trim_start_matches(['+', '-']), "nan" | "infinity" | "inf") {
        return Err(Error::new(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("numeric value \"{s}\" (NaN and infinity) is not supported"),
        ));
    }
    let cleaned: String;
    let s = if s.contains('_') {
        cleaned = s.replace('_', "");
        &cleaned
    } else {
        s
    };
    let s = s.strip_prefix('+').unwrap_or(s);
    if !s.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E')) {
        return Err(bad());
    }
    let d = if lower.contains('e') {
        // Decimal::from_scientific does not keep the written scale, so
        // shift the mantissa by hand.
        let (mant, exp) = lower.trim_start_matches('+').split_once('e').ok_or_else(bad)?;
        let exp: i32 = exp.parse().map_err(|_| bad())?;
        let mut d = Decimal::from_str(mant).map_err(|_| bad())?;
        if exp > 0 {
            for _ in 0..exp {
                d = d.checked_mul(Decimal::TEN).ok_or_else(overflow)?;
            }
            let scale = d.scale().saturating_sub(exp as u32).min(d.scale());
            d = d.round_dp(scale);
        } else if exp < 0 {
            let new_scale = d.scale() as i64 - exp as i64;
            if new_scale > MAX_SCALE as i64 {
                let mut x = d;
                for _ in 0..(-exp) {
                    x /= Decimal::TEN;
                }
                d = x;
            } else {
                d.set_scale(new_scale as u32).map_err(|_| overflow())?;
            }
        }
        d
    } else {
        Decimal::from_str(s).map_err(|e| match e {
            rust_decimal::Error::ExceedsMaximumPossibleValue | rust_decimal::Error::LessThanMinimumPossibleValue => {
                overflow()
            }
            _ => bad(),
        })?
    };
    Ok(norm(d))
}

/// Postgres converts floats to numeric through their 15 (6 for `real`)
/// significant-digit decimal representation.
pub fn from_float(v: f64, digits: usize) -> Result<Decimal> {
    if !v.is_finite() {
        return Err(Error::new(sqlstate::FEATURE_NOT_SUPPORTED, "cannot convert infinity or NaN to numeric"));
    }
    let s = format!("{:.*e}", digits - 1, v);
    let (mant, exp) = s.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    let mant = mant.trim_end_matches('0').trim_end_matches('.');
    parse_numeric(&format!("{mant}e{exp}")).or_else(|_| Decimal::from_f64(v).map(norm).ok_or_else(overflow))
}

/// Base-10000 weight and leading digit as used by Postgres to pick the
/// scale of a quotient.
pub fn weight_and_first_digit(d: &Decimal) -> (i32, i32) {
    if d.is_zero() {
        return (0, 0);
    }
    let m = d.mantissa().unsigned_abs();
    let digits = m.to_string();
    let int_len = digits.len() as i32 - d.scale() as i32; // decimal digits before the point (may be <= 0)
    let weight = (int_len - 1).div_euclid(4);
    // Decimal position (power of ten) of the most significant digit.
    let msd_pow = int_len - 1;
    let group_low_pow = weight * 4;
    let take = (msd_pow - group_low_pow + 1) as usize;
    let mut first: i32 = 0;
    for i in 0..take {
        first = first * 10 + digits.as_bytes().get(i).map_or(0, |b| (b - b'0') as i32);
    }
    (weight, first)
}

/// Result scale Postgres chooses for `a / b` (`select_div_scale`).
pub fn div_scale(a: &Decimal, b: &Decimal) -> u32 {
    let (w1, f1) = weight_and_first_digit(a);
    let (w2, f2) = weight_and_first_digit(b);
    let mut qweight = w1 - w2;
    if f1 <= f2 {
        qweight -= 1;
    }
    let rscale = (16 - qweight * 4).max(a.scale() as i32).max(b.scale() as i32).max(0);
    (rscale as u32).min(MAX_SCALE)
}

pub fn rescale(d: Decimal, scale: u32) -> Decimal {
    let mut r = d.round_dp_with_strategy(scale, RoundingStrategy::MidpointAwayFromZero);
    // Pad with zeros when the mantissa has room; otherwise keep fewer digits.
    r.rescale(scale);
    norm(r)
}

pub fn div(a: Decimal, b: Decimal) -> Result<Decimal> {
    if b.is_zero() {
        return Err(Error::division_by_zero());
    }
    let scale = div_scale(&a, &b);
    let q = a.checked_div(b).ok_or_else(overflow)?;
    Ok(rescale(q, scale))
}

pub fn round(d: Decimal, scale: i32) -> Decimal {
    if scale >= 0 {
        let mut r = d.round_dp_with_strategy(scale as u32, RoundingStrategy::MidpointAwayFromZero);
        if (scale as u32) <= MAX_SCALE {
            r.rescale(scale as u32);
        }
        norm(r)
    } else {
        match pow10(-scale) {
            Some(p) => norm(((d / p).round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero) * p).trunc()),
            None => Decimal::ZERO,
        }
    }
}

pub fn trunc(d: Decimal, scale: i32) -> Decimal {
    if scale >= 0 {
        let mut r = d.round_dp_with_strategy(scale as u32, RoundingStrategy::ToZero);
        if (scale as u32) <= MAX_SCALE {
            r.rescale(scale as u32);
        }
        norm(r)
    } else {
        match pow10(-scale) {
            Some(p) => norm(((d / p).trunc() * p).trunc()),
            None => Decimal::ZERO,
        }
    }
}

pub fn pow10(n: i32) -> Option<Decimal> {
    if !(0..=28).contains(&n) {
        return None;
    }
    Some(Decimal::from_i128_with_scale(10i128.pow(n as u32), 0))
}

/// Rounds half away from zero to an integer, as numeric-to-integer casts do.
pub fn to_i64(d: &Decimal) -> Option<i64> {
    d.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero).to_i64()
}
