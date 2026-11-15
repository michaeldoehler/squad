//! Cast rules (`pg_cast`), runtime conversion and common-type selection.

use rust_decimal::Decimal;

use super::datetime as dt;
use super::{io, numeric};
use crate::error::sqlstate;
use crate::{Error, FnCtx, Result, Type, Value};

/// The context in which a cast is requested; later variants allow more.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CastContext {
    Implicit,
    Assignment,
    Explicit,
}

/// Most permissive context in which `from -> to` is *not* allowed is one
/// below the returned value; `None` means no cast exists.
fn cast_level(from: &Type, to: &Type) -> Option<CastContext> {
    use CastContext::*;
    use Type::*;
    if from == to {
        return Some(Implicit);
    }
    Some(match (from, to) {
        (Unknown, _) => Implicit,
        (_, Unknown) => return None,
        (Int2, Int4 | Int8 | Float4 | Float8 | Numeric) => Implicit,
        (Int4, Int8 | Float4 | Float8 | Numeric) => Implicit,
        (Int4, Int2) => Assignment,
        (Int8, Float4 | Float8 | Numeric) => Implicit,
        (Int8, Int2 | Int4) => Assignment,
        (Float4, Float8) => Implicit,
        (Float4, Int2 | Int4 | Int8 | Numeric) => Assignment,
        (Float8, Float4 | Int2 | Int4 | Int8 | Numeric) => Assignment,
        (Numeric, Float4 | Float8) => Implicit,
        (Numeric, Int2 | Int4 | Int8) => Assignment,
        (Int4, Bool) | (Bool, Int4) => Explicit,
        (Date, Timestamp | TimestampTz) => Implicit,
        (Timestamp, TimestampTz) => Implicit,
        (Timestamp, Date | Time) => Assignment,
        (TimestampTz, Date | Time | Timestamp) => Assignment,
        (Time, Interval) => Implicit,
        (Interval, Time) => Assignment,
        (Json, Jsonb) | (Jsonb, Json) => Assignment,
        (Jsonb, Bool | Int2 | Int4 | Int8 | Float4 | Float8 | Numeric) => Explicit,
        (Array(a), Array(b)) => return cast_level(a, b),
        // A vector is binary coercible to an array of its element type.
        (Int2Vector, Array(b)) => return cast_level(&Int2, b),
        (OidVector, Array(b)) => return cast_level(&Int8, b),
        // I/O conversion casts.
        (Text, Void | Record) => return None,
        (Text, _) => Explicit,
        (Void, Text) => return None,
        (_, Text) => Assignment,
        _ => return None,
    })
}

/// Whether Postgres allows casting `from` to `to` in the given context.
pub fn can_cast(from: &Type, to: &Type, ctx: CastContext) -> bool {
    cast_level(from, to).is_some_and(|needed| needed <= ctx)
}

fn cannot_cast(from: &Value, to: &Type) -> Error {
    Error::new(sqlstate::CANNOT_COERCE, format!("cannot cast type {} to {}", from.type_of().name(), to.name()))
}

fn int_range(v: i64, to: &Type) -> Result<Value> {
    match to {
        Type::Int2 => i16::try_from(v).map(Value::Int2).map_err(|_| Error::out_of_range("smallint")),
        Type::Int4 => i32::try_from(v).map(Value::Int4).map_err(|_| Error::out_of_range("integer")),
        _ => Ok(Value::Int8(v)),
    }
}

fn int_type_name(to: &Type) -> &'static str {
    match to {
        Type::Int2 => "smallint",
        Type::Int4 => "integer",
        _ => "bigint",
    }
}

fn float_to_int(f: f64, to: &Type) -> Result<Value> {
    let r = f.round_ties_even();
    // i64::MAX is not representable; anything >= 2^63 overflows.
    if !r.is_finite() || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r) {
        return Err(Error::out_of_range(int_type_name(to)));
    }
    int_range(r as i64, to)
}

fn json_scalar_err(j: &serde_json::Value, to: &Type) -> Error {
    let kind = match j {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "numeric",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    };
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("cannot cast jsonb {kind} to type {}", to.name()))
}

pub fn json_number_to_decimal(n: &serde_json::Number) -> Result<Decimal> {
    if let Some(i) = n.as_i64() {
        Ok(Decimal::from(i))
    } else if let Some(u) = n.as_u64() {
        Ok(Decimal::from(u))
    } else {
        numeric::parse_numeric(&n.to_string()).or_else(|_| numeric::from_float(n.as_f64().unwrap_or(0.0), 15))
    }
}

/// `value::text` — like the output function except for booleans.
pub fn to_text(v: &Value, ctx: &FnCtx) -> String {
    match v {
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        v => io::format_value(v, ctx),
    }
}

/// Converts a value to `to`. NULL stays NULL.
pub fn cast(v: Value, to: &Type, ctx: &FnCtx) -> Result<Value> {
    use Value as V;
    let zone = || dt::session_zone(ctx);
    Ok(match (v, to) {
        (V::Null, _) => V::Null,
        (v, Type::Unknown | Type::Record | Type::Void) => v,
        (V::Text(s), Type::Text) => V::Text(s),
        (V::Text(s), Type::Enum(_)) => {
            let _ = s;
            return Err(Error::internal("casts to enum types must be handled by the caller"));
        }
        (V::Text(s), t) => io::parse_value(&s, t, ctx)?,
        (V::Enum(_, s), Type::Text) => V::Text(s),
        (v @ V::Enum(..), Type::Enum(_)) => v,
        (v, Type::Text) => V::Text(to_text(&v, ctx)),

        (v @ V::Bool(_), Type::Bool) => v,
        (V::Int2(i), Type::Bool) => V::Bool(i != 0),
        (V::Int4(i), Type::Bool) => V::Bool(i != 0),
        (V::Int8(i), Type::Bool) => V::Bool(i != 0),
        (V::Bool(b), Type::Int2 | Type::Int4 | Type::Int8) => int_range(b as i64, to)?,

        (V::Int2(i), Type::Int2 | Type::Int4 | Type::Int8) => int_range(i as i64, to)?,
        (V::Int4(i), Type::Int2 | Type::Int4 | Type::Int8) => int_range(i as i64, to)?,
        (V::Int8(i), Type::Int2 | Type::Int4 | Type::Int8) => int_range(i, to)?,
        (V::Float4(f), Type::Int2 | Type::Int4 | Type::Int8) => float_to_int(f as f64, to)?,
        (V::Float8(f), Type::Int2 | Type::Int4 | Type::Int8) => float_to_int(f, to)?,
        (V::Numeric(d), Type::Int2 | Type::Int4 | Type::Int8) => {
            int_range(numeric::to_i64(&d).ok_or_else(|| Error::out_of_range(int_type_name(to)))?, to)?
        }

        (V::Int2(i), Type::Float4) => V::Float4(i as f32),
        (V::Int4(i), Type::Float4) => V::Float4(i as f32),
        (V::Int8(i), Type::Float4) => V::Float4(i as f32),
        (v @ V::Float4(_), Type::Float4) => v,
        (V::Float8(f), Type::Float4) => {
            let r = f as f32;
            if r.is_infinite() && f.is_finite() {
                return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow"));
            }
            V::Float4(r)
        }
        (V::Numeric(d), Type::Float4) => V::Float4(d.to_string().parse::<f32>().unwrap_or(f32::NAN)),
        (V::Int2(i), Type::Float8) => V::Float8(i as f64),
        (V::Int4(i), Type::Float8) => V::Float8(i as f64),
        (V::Int8(i), Type::Float8) => V::Float8(i as f64),
        (V::Float4(f), Type::Float8) => V::Float8(f as f64),
        (v @ V::Float8(_), Type::Float8) => v,
        (V::Numeric(d), Type::Float8) => V::Float8(d.to_string().parse::<f64>().unwrap_or(f64::NAN)),

        (V::Int2(i), Type::Numeric) => V::Numeric(Decimal::from(i)),
        (V::Int4(i), Type::Numeric) => V::Numeric(Decimal::from(i)),
        (V::Int8(i), Type::Numeric) => V::Numeric(Decimal::from(i)),
        (V::Float4(f), Type::Numeric) => V::Numeric(numeric::from_float(f as f64, 6)?),
        (V::Float8(f), Type::Numeric) => V::Numeric(numeric::from_float(f, 15)?),
        (v @ V::Numeric(_), Type::Numeric) => v,

        (v @ V::Bytea(_), Type::Bytea) => v,
        (v @ V::Uuid(_), Type::Uuid) => v,

        (v @ V::Date(_), Type::Date) => v,
        (V::Timestamp(t), Type::Date) => V::Date(dt::ts_to_date(t)),
        (V::TimestampTz(t), Type::Date) => V::Date(dt::ts_to_date(zone().utc_to_local(t))),
        (v @ V::Time(_), Type::Time) => v,
        (V::Timestamp(t), Type::Time) if dt::is_finite_ts(t) => V::Time(t.rem_euclid(dt::USECS_PER_DAY)),
        (V::TimestampTz(t), Type::Time) if dt::is_finite_ts(t) => {
            V::Time(zone().utc_to_local(t).rem_euclid(dt::USECS_PER_DAY))
        }
        (V::Interval(iv), Type::Time) => V::Time(iv.micros.rem_euclid(dt::USECS_PER_DAY)),
        (V::Date(d), Type::Timestamp) => V::Timestamp(dt::date_to_ts(d)),
        (v @ V::Timestamp(_), Type::Timestamp) => v,
        (V::TimestampTz(t), Type::Timestamp) => V::Timestamp(zone().utc_to_local(t)),
        (V::Date(d), Type::TimestampTz) => V::TimestampTz(zone().local_to_utc(dt::date_to_ts(d))),
        (V::Timestamp(t), Type::TimestampTz) => V::TimestampTz(zone().local_to_utc(t)),
        (v @ V::TimestampTz(_), Type::TimestampTz) => v,
        (v @ V::Interval(_), Type::Interval) => v,
        (V::Time(t), Type::Interval) => V::Interval(crate::types::Interval { months: 0, days: 0, micros: t }),

        (v @ V::Json(_), Type::Json) => v,
        (V::Jsonb(j), Type::Json) => V::Json(io::fmt_jsonb(&j)),
        (V::Json(s), Type::Jsonb) => V::Jsonb(Box::new(io::parse_json(&s, "jsonb")?)),
        (v @ V::Jsonb(_), Type::Jsonb) => v,
        (V::Jsonb(j), Type::Bool) => match *j {
            serde_json::Value::Bool(b) => V::Bool(b),
            ref other => return Err(json_scalar_err(other, to)),
        },
        (V::Jsonb(j), Type::Int2 | Type::Int4 | Type::Int8 | Type::Float4 | Type::Float8 | Type::Numeric) => {
            match &*j {
                serde_json::Value::Number(n) => cast(V::Numeric(json_number_to_decimal(n)?), to, ctx)?,
                other => return Err(json_scalar_err(other, to)),
            }
        }

        (V::Array(items) | V::Vector(items), Type::Array(elem)) => {
            V::Array(items.into_iter().map(|i| cast(i, elem, ctx)).collect::<Result<_>>()?)
        }
        (v @ V::Vector(_), Type::Int2Vector | Type::OidVector) => v,
        (v, Type::Enum(_)) => return Err(cannot_cast(&v, to)),
        (v, _) => return Err(cannot_cast(&v, to)),
    })
}

/// Type category as in `pg_type.typcategory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Bool,
    Numeric,
    String,
    DateTime,
    Timespan,
    Array,
    Enum,
    Unknown,
    /// User-defined and everything else; only identical types match.
    Other,
}

pub fn category(t: &Type) -> Category {
    match t {
        Type::Bool => Category::Bool,
        Type::Int2 | Type::Int4 | Type::Int8 | Type::Float4 | Type::Float8 | Type::Numeric => Category::Numeric,
        Type::Text => Category::String,
        Type::Date | Type::Time | Type::Timestamp | Type::TimestampTz => Category::DateTime,
        Type::Interval => Category::Timespan,
        Type::Array(_) | Type::Int2Vector | Type::OidVector => Category::Array,
        Type::Enum(_) => Category::Enum,
        Type::Unknown => Category::Unknown,
        _ => Category::Other,
    }
}

pub fn is_preferred(t: &Type) -> bool {
    matches!(t, Type::Bool | Type::Float8 | Type::Text | Type::TimestampTz | Type::Interval)
}

/// Postgres' `select_common_type` for CASE, COALESCE, VALUES, UNION,
/// ARRAY[] and GREATEST/LEAST.
pub fn common_type(types: &[Type], context: &str) -> Result<Type> {
    let mut known = types.iter().filter(|t| **t != Type::Unknown);
    let Some(first) = known.next() else { return Ok(Type::Text) };
    let mut best = first.clone();
    let mismatch = |a: &Type, b: &Type| {
        Error::new(
            sqlstate::DATATYPE_MISMATCH,
            format!("{context} types {} and {} cannot be matched", a.name(), b.name()),
        )
    };
    for t in known {
        if *t == best {
            continue;
        }
        if category(t) != category(&best) {
            return Err(mismatch(&best, t));
        }
        if let (Type::Array(a), Type::Array(b)) = (&best, t) {
            best = common_type(&[(**a).clone(), (**b).clone()], context)?.array_of();
            continue;
        }
        let best_to_t = can_cast(&best, t, CastContext::Implicit);
        let t_to_best = can_cast(t, &best, CastContext::Implicit);
        if !best_to_t && !t_to_best {
            return Err(mismatch(&best, t));
        }
        if !is_preferred(&best) && best_to_t && (is_preferred(t) || !t_to_best) {
            best = t.clone();
        }
    }
    Ok(best)
}
