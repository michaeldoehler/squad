//! Date/time functions.

use rust_decimal::prelude::ToPrimitive;

use super::datetime::{self as dt, Source, Tm, Zone};
use super::f_string::{int, txt};
use super::ops::bad_arg;
use super::registry::{Registry, Ret, P};
use super::to_char;
use crate::error::sqlstate;
use crate::types::{Interval, PG_EPOCH_UNIX_MICROS};
use crate::{Error, FnCtx, Result, Type, Value};

fn now(c: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(c.txn_time))
}
fn statement_timestamp(c: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(c.stmt_time))
}
fn clock_timestamp(_: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(chrono::Utc::now().timestamp_micros() - PG_EPOCH_UNIX_MICROS))
}
fn local_now(c: &FnCtx) -> i64 {
    dt::session_zone(c).utc_to_local(c.txn_time)
}
fn round_precision(ts: i64, a: &[Value]) -> Result<i64> {
    Ok(match a.first() {
        Some(v) => {
            let p = int(v)?.clamp(0, 6) as u32;
            let unit = 10i64.pow(6 - p);
            (ts as f64 / unit as f64).round() as i64 * unit
        }
        None => ts,
    })
}
fn current_timestamp(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(round_precision(c.txn_time, a)?))
}
fn current_date(c: &FnCtx, _: &[Value]) -> Result<Value> {
    Ok(Value::Date(dt::ts_to_date(local_now(c))))
}
fn localtime(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Time(round_precision(local_now(c), a)?.rem_euclid(dt::USECS_PER_DAY)))
}
fn localtimestamp(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Timestamp(round_precision(local_now(c), a)?))
}

fn source(v: &Value, c: &FnCtx) -> Result<(Source, &'static str)> {
    Ok(match v {
        Value::Timestamp(t) => (Source::Ts(*t, None), "timestamp without time zone"),
        Value::TimestampTz(t) => {
            let off = dt::session_zone(c).offset_at_utc(*t);
            (Source::Ts(t.saturating_add(off as i64 * dt::USECS_PER_SEC), Some(off)), "timestamp with time zone")
        }
        Value::Date(d) => (Source::Date(*d), "date"),
        Value::Time(t) => (Source::Time(*t), "time without time zone"),
        Value::Interval(i) => (Source::Interval(*i), "interval"),
        _ => return Err(bad_arg()),
    })
}

fn infinite_extract(field: &str, v: &Value) -> Option<Result<Value>> {
    let inf = match v {
        Value::Timestamp(t) | Value::TimestampTz(t) if !dt::is_finite_ts(*t) => *t > 0,
        Value::Date(d) if *d == dt::DATE_INF || *d == dt::DATE_NINF => *d > 0,
        _ => return None,
    };
    let f = field.trim().to_ascii_lowercase();
    Some(match f.as_str() {
        "epoch" | "year" | "decade" | "century" | "millennium" | "julian" | "isoyear" => {
            Ok(Value::Float8(if inf { f64::INFINITY } else { f64::NEG_INFINITY }))
        }
        _ => Ok(Value::Null),
    })
}

fn extract(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let field = txt(&a[0])?;
    if let Some(r) = infinite_extract(field, &a[1]) {
        return match r? {
            Value::Float8(_) => Err(Error::unsupported("extract of an infinite date/time value as numeric")),
            v => Ok(v),
        };
    }
    let (src, name) = source(&a[1], c)?;
    dt::extract(field, &src, name).map(Value::Numeric)
}
fn date_part(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let field = txt(&a[0])?;
    if let Some(r) = infinite_extract(field, &a[1]) {
        return r;
    }
    let (src, name) = source(&a[1], c)?;
    let d = dt::extract(field, &src, name)?;
    Ok(Value::Float8(d.to_f64().unwrap_or(f64::NAN)))
}

fn date_trunc(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let field = txt(&a[0])?;
    Ok(match &a[1] {
        Value::Timestamp(t) => Value::Timestamp(dt::trunc_ts(field, *t, "timestamp without time zone")?),
        Value::TimestampTz(t) => {
            let zone = match a.get(2) {
                Some(z) => {
                    dt::parse_zone(txt(z)?, true).ok_or_else(|| dt::zone_not_recognized(txt(z).unwrap_or("")))?
                }
                None => dt::session_zone(c),
            };
            let local = dt::trunc_ts(field, zone.utc_to_local(*t), "timestamp with time zone")?;
            Value::TimestampTz(zone.local_to_utc(local))
        }
        Value::Interval(i) => Value::Interval(dt::trunc_interval(field, i)?),
        _ => return Err(bad_arg()),
    })
}

fn age(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let zone = dt::session_zone(c);
    let local = |v: &Value| -> Result<i64> {
        match v {
            Value::Timestamp(t) => Ok(*t),
            Value::TimestampTz(t) => Ok(zone.utc_to_local(*t)),
            _ => Err(bad_arg()),
        }
    };
    let (x, y) = if a.len() == 2 {
        (local(&a[0])?, local(&a[1])?)
    } else {
        (dt::date_to_ts(dt::ts_to_date(local_now(c))), local(&a[0])?)
    };
    Ok(Value::Interval(dt::age(x, y)))
}

fn fields_of(v: &Value, c: &FnCtx) -> Result<to_char::Fields> {
    let infinite = || Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "cannot format an infinite timestamp");
    Ok(match v {
        Value::Timestamp(t) => {
            if !dt::is_finite_ts(*t) {
                return Err(infinite());
            }
            let tm = Tm::from_ts(*t);
            to_char::Fields { tm, zone: None, hours: tm.hour as i64 }
        }
        Value::TimestampTz(t) => {
            if !dt::is_finite_ts(*t) {
                return Err(infinite());
            }
            let zone = dt::session_zone(c);
            let off = zone.offset_at_utc(*t);
            let tm = Tm::from_ts(*t + off as i64 * dt::USECS_PER_SEC);
            to_char::Fields { tm, zone: Some((off, zone.abbrev_at_utc(*t))), hours: tm.hour as i64 }
        }
        Value::Interval(i) => {
            let t = i.micros;
            let tm = Tm {
                year: (i.months / 12) as i64,
                month: (i.months % 12).unsigned_abs().max(1),
                day: i.days.unsigned_abs().max(1),
                hour: (t.abs() / dt::USECS_PER_HOUR % 24) as u32,
                minute: (t.abs() / dt::USECS_PER_MIN % 60) as u32,
                second: (t.abs() / dt::USECS_PER_SEC % 60) as u32,
                micro: (t.abs() % dt::USECS_PER_SEC) as u32,
            };
            to_char::Fields { tm, zone: None, hours: t / dt::USECS_PER_HOUR }
        }
        _ => return Err(bad_arg()),
    })
}
fn to_char_dt(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Text(to_char::datetime_to_char(&fields_of(&a[0], c)?, txt(&a[1])?)))
}
fn to_char_num(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let d = match &a[0] {
        Value::Float4(f) => super::numeric::from_float(*f as f64, 6)?,
        Value::Float8(f) => super::numeric::from_float(*f, 15)?,
        v => v.as_decimal().ok_or_else(bad_arg)?,
    };
    to_char::numeric_to_char(d, txt(&a[1])?).map(Value::Text)
}

fn to_timestamp_epoch(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let secs = match &a[0] {
        Value::Float8(f) => *f,
        _ => return Err(bad_arg()),
    };
    if secs.is_infinite() {
        return Ok(Value::TimestampTz(if secs > 0.0 { dt::TS_INF } else { dt::TS_NINF }));
    }
    if secs.is_nan() {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "timestamp cannot be NaN"));
    }
    let micros = (secs * 1e6).round();
    if micros.abs() > 9.0e18 {
        return Err(dt::ts_out_of_range());
    }
    Ok(Value::TimestampTz(micros as i64 - PG_EPOCH_UNIX_MICROS))
}
fn to_timestamp_fmt(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let p = to_char::parse_with_format(txt(&a[0])?, txt(&a[1])?)?;
    let (days, time) = p.resolve()?;
    let local = days * dt::USECS_PER_DAY + time;
    let zone = p.tz_offset.map_or_else(|| dt::session_zone(c), Zone::Fixed);
    Ok(Value::TimestampTz(zone.local_to_utc(local)))
}
fn to_date(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let p = to_char::parse_with_format(txt(&a[0])?, txt(&a[1])?)?;
    Ok(Value::Date(p.resolve()?.0 as i32))
}
fn to_number(_: &FnCtx, a: &[Value]) -> Result<Value> {
    to_char::to_number(txt(&a[0])?, txt(&a[1])?).map(Value::Numeric)
}

fn field_range(what: &str) -> Error {
    Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, format!("{what} field value out of range"))
}
fn make_date_parts(y: i64, m: i64, d: i64) -> Result<i64> {
    if y == 0 || !(1..=12).contains(&m) || d < 1 {
        return Err(field_range("date"));
    }
    let year = if y < 0 { y + 1 } else { y };
    if d as u32 > dt::days_in_month(year, m as u32) {
        return Err(field_range("date"));
    }
    Ok(dt::days_from_civil(year, m as u32, d as u32))
}
fn make_time_parts(h: i64, mi: i64, sec: f64) -> Result<i64> {
    if !(0..=24).contains(&h)
        || !(0..=59).contains(&mi)
        || !(0.0..=60.0).contains(&sec)
        || (h == 24 && (mi > 0 || sec > 0.0))
    {
        return Err(field_range("time"));
    }
    Ok(h * dt::USECS_PER_HOUR + mi * dt::USECS_PER_MIN + (sec * 1e6).round() as i64)
}
fn f8(v: &Value) -> Result<f64> {
    v.as_f64().ok_or_else(bad_arg)
}
fn make_date(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Date(make_date_parts(int(&a[0])?, int(&a[1])?, int(&a[2])?)? as i32))
}
fn make_time(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Time(make_time_parts(int(&a[0])?, int(&a[1])?, f8(&a[2])?)?))
}
fn make_timestamp(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let days = make_date_parts(int(&a[0])?, int(&a[1])?, int(&a[2])?)?;
    Ok(Value::Timestamp(days * dt::USECS_PER_DAY + make_time_parts(int(&a[3])?, int(&a[4])?, f8(&a[5])?)?))
}
fn make_timestamptz(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let days = make_date_parts(int(&a[0])?, int(&a[1])?, int(&a[2])?)?;
    let local = days * dt::USECS_PER_DAY + make_time_parts(int(&a[3])?, int(&a[4])?, f8(&a[5])?)?;
    let zone = match a.get(6) {
        Some(z) => dt::parse_zone(txt(z)?, true).ok_or_else(|| dt::zone_not_recognized(txt(z).unwrap_or("")))?,
        None => dt::session_zone(c),
    };
    Ok(Value::TimestampTz(zone.local_to_utc(local)))
}
fn make_interval(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let get = |i: usize| -> Result<i64> { a.get(i).map_or(Ok(0), int) };
    let secs = a.get(6).map_or(Ok(0.0), f8)?;
    let e = || Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "interval out of range");
    let months = get(0)?.checked_mul(12).and_then(|m| m.checked_add(get(1).ok()?)).ok_or_else(e)?;
    let days = get(2)?.checked_mul(7).and_then(|d| d.checked_add(get(3).ok()?)).ok_or_else(e)?;
    let micros = get(4)? as f64 * 3.6e9 + get(5)? as f64 * 6e7 + (secs * 1e6).round();
    Ok(Value::Interval(Interval {
        months: i32::try_from(months).map_err(|_| e())?,
        days: i32::try_from(days).map_err(|_| e())?,
        micros: if micros.abs() < 9.2e18 { micros as i64 } else { return Err(e()) },
    }))
}

fn timezone(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let name = txt(&a[0])?;
    let zone = dt::parse_zone(name, true).ok_or_else(|| dt::zone_not_recognized(name))?;
    Ok(match &a[1] {
        Value::Timestamp(t) => Value::TimestampTz(zone.local_to_utc(*t)),
        Value::TimestampTz(t) => Value::Timestamp(zone.utc_to_local(*t)),
        _ => return Err(bad_arg()),
    })
}
fn iv(v: &Value) -> Result<&Interval> {
    match v {
        Value::Interval(i) => Ok(i),
        _ => Err(bad_arg()),
    }
}
fn justify_days(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Interval(dt::justify_days(iv(&a[0])?)))
}
fn justify_hours(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Interval(dt::justify_hours(iv(&a[0])?)))
}
fn justify_interval(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let i = iv(&a[0])?;
    // Fold time into days and days into months, then fix mixed signs.
    let total_days = i.days as i64 + i.micros / dt::USECS_PER_DAY;
    let mut micros = i.micros % dt::USECS_PER_DAY;
    let mut months = i.months as i64 + total_days / 30;
    let mut days = total_days % 30;
    if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
        days += 30;
        months -= 1;
    } else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
        days -= 30;
        months += 1;
    }
    if days > 0 && micros < 0 {
        micros += dt::USECS_PER_DAY;
        days -= 1;
    } else if days < 0 && micros > 0 {
        micros -= dt::USECS_PER_DAY;
        days += 1;
    }
    Ok(Value::Interval(Interval { months: months as i32, days: days as i32, micros }))
}
fn isfinite(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Bool(match &a[0] {
        Value::Timestamp(t) | Value::TimestampTz(t) => dt::is_finite_ts(*t),
        Value::Date(d) => *d != dt::DATE_INF && *d != dt::DATE_NINF,
        Value::Interval(_) => true,
        _ => return Err(bad_arg()),
    }))
}
fn date_bin(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let stride = iv(&a[0])?;
    if stride.months != 0 {
        return Err(Error::unsupported("timestamps cannot be binned into intervals containing months or years; this"));
    }
    let width = stride.days as i64 * dt::USECS_PER_DAY + stride.micros;
    if width <= 0 {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "stride must be greater than zero"));
    }
    let bin = |t: i64, origin: i64| origin + (t - origin).div_euclid(width) * width;
    Ok(match (&a[1], &a[2]) {
        (Value::Timestamp(t), Value::Timestamp(o)) => Value::Timestamp(bin(*t, *o)),
        (Value::TimestampTz(t), Value::TimestampTz(o)) => Value::TimestampTz(bin(*t, *o)),
        _ => return Err(bad_arg()),
    })
}

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;

    for name in ["now", "transaction_timestamp"] {
        r.f(name, &[], Ret::T(TimestampTz), now).stable();
    }
    r.f("current_timestamp", &[], Ret::T(TimestampTz), current_timestamp).stable();
    r.f("current_timestamp", &[t(Int4)], Ret::T(TimestampTz), current_timestamp).stable();
    r.f("statement_timestamp", &[], Ret::T(TimestampTz), statement_timestamp).stable();
    r.f("clock_timestamp", &[], Ret::T(TimestampTz), clock_timestamp).volatile();
    r.f("current_date", &[], Ret::T(Date), current_date).stable();
    for name in ["localtime", "current_time"] {
        r.f(name, &[], Ret::T(Time), localtime).stable();
        r.f(name, &[t(Int4)], Ret::T(Time), localtime).stable();
    }
    r.f("localtimestamp", &[], Ret::T(Timestamp), localtimestamp).stable();
    r.f("localtimestamp", &[t(Int4)], Ret::T(Timestamp), localtimestamp).stable();

    for src in [Timestamp, TimestampTz, Date, Time, Interval] {
        let stable = src == TimestampTz;
        let s = r.f("extract", &[t(Text), t(src.clone())], Ret::T(Numeric), extract);
        if stable {
            s.stable();
        }
        let s = r.f("date_part", &[t(Text), t(src.clone())], Ret::T(Float8), date_part);
        if stable {
            s.stable();
        }
    }
    r.f("date_trunc", &[t(Text), t(Timestamp)], Ret::T(Timestamp), date_trunc);
    r.f("date_trunc", &[t(Text), t(TimestampTz)], Ret::T(TimestampTz), date_trunc).stable();
    r.f("date_trunc", &[t(Text), t(TimestampTz), t(Text)], Ret::T(TimestampTz), date_trunc);
    r.f("date_trunc", &[t(Text), t(Interval)], Ret::T(Interval), date_trunc);
    r.f("age", &[t(Timestamp), t(Timestamp)], Ret::T(Interval), age);
    r.f("age", &[t(TimestampTz), t(TimestampTz)], Ret::T(Interval), age).stable();
    r.f("age", &[t(Timestamp)], Ret::T(Interval), age).stable();
    r.f("age", &[t(TimestampTz)], Ret::T(Interval), age).stable();

    r.f("to_char", &[t(Timestamp), t(Text)], Ret::T(Text), to_char_dt).stable();
    r.f("to_char", &[t(TimestampTz), t(Text)], Ret::T(Text), to_char_dt).stable();
    r.f("to_char", &[t(Interval), t(Text)], Ret::T(Text), to_char_dt).stable();
    for ty in [Int4, Int8, Float4, Float8, Numeric] {
        r.f("to_char", &[t(ty), t(Text)], Ret::T(Text), to_char_num).stable();
    }
    r.f("to_timestamp", &[t(Float8)], Ret::T(TimestampTz), to_timestamp_epoch);
    r.f("to_timestamp", &[t(Text), t(Text)], Ret::T(TimestampTz), to_timestamp_fmt).stable();
    r.f("to_date", &[t(Text), t(Text)], Ret::T(Date), to_date).stable();
    r.f("to_number", &[t(Text), t(Text)], Ret::T(Numeric), to_number).stable();

    r.f("make_date", &[t(Int4), t(Int4), t(Int4)], Ret::T(Date), make_date);
    r.f("make_time", &[t(Int4), t(Int4), t(Float8)], Ret::T(Time), make_time);
    r.f("make_timestamp", &[t(Int4), t(Int4), t(Int4), t(Int4), t(Int4), t(Float8)], Ret::T(Timestamp), make_timestamp);
    r.f(
        "make_timestamptz",
        &[t(Int4), t(Int4), t(Int4), t(Int4), t(Int4), t(Float8)],
        Ret::T(TimestampTz),
        make_timestamptz,
    )
    .stable();
    r.f(
        "make_timestamptz",
        &[t(Int4), t(Int4), t(Int4), t(Int4), t(Int4), t(Float8), t(Text)],
        Ret::T(TimestampTz),
        make_timestamptz,
    );
    let mut params: Vec<P> = Vec::new();
    r.f("make_interval", &params, Ret::T(Interval), make_interval);
    for i in 0..7 {
        params.push(if i == 6 { t(Float8) } else { t(Int4) });
        r.f("make_interval", &params, Ret::T(Interval), make_interval);
    }
    r.f("timezone", &[t(Text), t(Timestamp)], Ret::T(TimestampTz), timezone);
    r.f("timezone", &[t(Text), t(TimestampTz)], Ret::T(Timestamp), timezone);
    r.f("justify_days", &[t(Interval)], Ret::T(Interval), justify_days);
    r.f("justify_hours", &[t(Interval)], Ret::T(Interval), justify_hours);
    r.f("justify_interval", &[t(Interval)], Ret::T(Interval), justify_interval);
    for ty in [Timestamp, TimestampTz, Date, Interval] {
        r.f("isfinite", &[t(ty)], Ret::T(Bool), isfinite);
    }
    r.f("date_bin", &[t(Interval), t(Timestamp), t(Timestamp)], Ret::T(Timestamp), date_bin);
    r.f("date_bin", &[t(Interval), t(TimestampTz), t(TimestampTz)], Ret::T(TimestampTz), date_bin);
}
