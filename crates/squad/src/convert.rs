//! Conversions between Rust types and SQL values.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use rust_decimal::Decimal;

use crate::types::{Value, PG_EPOCH_UNIX_DAYS, PG_EPOCH_UNIX_MICROS};
use crate::{sqlstate, Error, Result};

/// A Rust value that can be passed as a statement parameter.
pub trait ToSql {
    fn to_value(&self) -> Value;
}

/// A Rust type a column value can be converted to.
pub trait FromSql: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

fn mismatch<T>(value: &Value) -> Error {
    Error::new(
        sqlstate::DATATYPE_MISMATCH,
        format!("cannot convert SQL value of type {} to Rust type {}", value.type_of(), std::any::type_name::<T>()),
    )
}

fn unexpected_null<T>() -> Error {
    Error::new(
        sqlstate::NULL_VALUE_NOT_ALLOWED,
        format!("unexpected NULL when converting to {}; use Option<T>", std::any::type_name::<T>()),
    )
}

impl ToSql for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl FromSql for Value {
    fn from_value(value: &Value) -> Result<Value> {
        Ok(value.clone())
    }
}

impl<T: ToSql + ?Sized> ToSql for &T {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_value(value: &Value) -> Result<Option<T>> {
        match value {
            Value::Null => Ok(None),
            v => T::from_value(v).map(Some),
        }
    }
}

macro_rules! simple_to_sql {
    ($($t:ty => $variant:ident),*) => {$(
        impl ToSql for $t {
            fn to_value(&self) -> Value {
                Value::$variant(self.clone().into())
            }
        }
    )*};
}
simple_to_sql!(bool => Bool, i16 => Int2, i32 => Int4, i64 => Int8, f32 => Float4, f64 => Float8, Decimal => Numeric, String => Text, Vec<u8> => Bytea);

impl ToSql for str {
    fn to_value(&self) -> Value {
        Value::Text(self.to_string())
    }
}

impl ToSql for [u8] {
    fn to_value(&self) -> Value {
        Value::Bytea(self.to_vec())
    }
}

impl ToSql for serde_json::Value {
    fn to_value(&self) -> Value {
        Value::Jsonb(Box::new(self.clone()))
    }
}

impl ToSql for NaiveDate {
    fn to_value(&self) -> Value {
        let days = self.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        Value::Date(days as i32 - PG_EPOCH_UNIX_DAYS)
    }
}

impl ToSql for NaiveTime {
    fn to_value(&self) -> Value {
        Value::Time(self.num_seconds_from_midnight() as i64 * 1_000_000 + (self.nanosecond() / 1000) as i64)
    }
}

impl ToSql for NaiveDateTime {
    fn to_value(&self) -> Value {
        Value::Timestamp(self.and_utc().timestamp_micros() - PG_EPOCH_UNIX_MICROS)
    }
}

impl<Tz: TimeZone> ToSql for DateTime<Tz> {
    fn to_value(&self) -> Value {
        Value::TimestampTz(self.timestamp_micros() - PG_EPOCH_UNIX_MICROS)
    }
}

impl<T: ToSql> ToSql for Vec<T> {
    fn to_value(&self) -> Value {
        Value::Array(self.iter().map(ToSql::to_value).collect())
    }
}

impl FromSql for bool {
    fn from_value(value: &Value) -> Result<bool> {
        match value {
            Value::Bool(b) => Ok(*b),
            Value::Null => Err(unexpected_null::<bool>()),
            v => Err(mismatch::<bool>(v)),
        }
    }
}

macro_rules! int_from_sql {
    ($($t:ty),*) => {$(
        impl FromSql for $t {
            fn from_value(value: &Value) -> Result<$t> {
                match value {
                    Value::Null => Err(unexpected_null::<$t>()),
                    v => v
                        .as_i64()
                        .ok_or_else(|| mismatch::<$t>(v))
                        .and_then(|i| <$t>::try_from(i).map_err(|_| Error::out_of_range(std::any::type_name::<$t>()))),
                }
            }
        }
    )*};
}
int_from_sql!(i16, i32, i64, u32, u64, usize);

impl FromSql for f64 {
    fn from_value(value: &Value) -> Result<f64> {
        match value {
            Value::Null => Err(unexpected_null::<f64>()),
            v => v.as_f64().ok_or_else(|| mismatch::<f64>(v)),
        }
    }
}

impl FromSql for f32 {
    fn from_value(value: &Value) -> Result<f32> {
        match value {
            Value::Null => Err(unexpected_null::<f32>()),
            Value::Float4(f) => Ok(*f),
            v => v.as_f64().map(|f| f as f32).ok_or_else(|| mismatch::<f32>(v)),
        }
    }
}

impl FromSql for Decimal {
    fn from_value(value: &Value) -> Result<Decimal> {
        match value {
            Value::Null => Err(unexpected_null::<Decimal>()),
            v => v.as_decimal().ok_or_else(|| mismatch::<Decimal>(v)),
        }
    }
}

impl FromSql for String {
    fn from_value(value: &Value) -> Result<String> {
        match value {
            Value::Null => Err(unexpected_null::<String>()),
            Value::Text(s) | Value::Json(s) | Value::Enum(_, s) => Ok(s.clone()),
            Value::Jsonb(j) => Ok(j.to_string()),
            v => Err(mismatch::<String>(v)),
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_value(value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Null => Err(unexpected_null::<Vec<u8>>()),
            Value::Bytea(b) => Ok(b.clone()),
            Value::Uuid(u) => Ok(u.to_vec()),
            v => Err(mismatch::<Vec<u8>>(v)),
        }
    }
}

impl FromSql for [u8; 16] {
    fn from_value(value: &Value) -> Result<[u8; 16]> {
        match value {
            Value::Null => Err(unexpected_null::<[u8; 16]>()),
            Value::Uuid(u) => Ok(*u),
            v => Err(mismatch::<[u8; 16]>(v)),
        }
    }
}

impl FromSql for serde_json::Value {
    fn from_value(value: &Value) -> Result<serde_json::Value> {
        match value {
            Value::Null => Ok(serde_json::Value::Null),
            Value::Jsonb(j) => Ok((**j).clone()),
            Value::Json(s) => {
                serde_json::from_str(s).map_err(|e| Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, e.to_string()))
            }
            v => Err(mismatch::<serde_json::Value>(v)),
        }
    }
}

impl FromSql for NaiveDate {
    fn from_value(value: &Value) -> Result<NaiveDate> {
        match value {
            Value::Null => Err(unexpected_null::<NaiveDate>()),
            Value::Date(d) => NaiveDate::from_ymd_opt(2000, 1, 1)
                .and_then(|e| e.checked_add_signed(chrono::Duration::days(*d as i64)))
                .ok_or_else(|| Error::out_of_range("date")),
            v => Err(mismatch::<NaiveDate>(v)),
        }
    }
}

impl FromSql for NaiveTime {
    fn from_value(value: &Value) -> Result<NaiveTime> {
        match value {
            Value::Null => Err(unexpected_null::<NaiveTime>()),
            Value::Time(t) => {
                NaiveTime::from_num_seconds_from_midnight_opt((*t / 1_000_000) as u32, ((*t % 1_000_000) * 1000) as u32)
                    .ok_or_else(|| Error::out_of_range("time"))
            }
            v => Err(mismatch::<NaiveTime>(v)),
        }
    }
}

impl FromSql for NaiveDateTime {
    fn from_value(value: &Value) -> Result<NaiveDateTime> {
        match value {
            Value::Null => Err(unexpected_null::<NaiveDateTime>()),
            Value::Timestamp(t) | Value::TimestampTz(t) => {
                DateTime::<Utc>::from_timestamp_micros(*t + PG_EPOCH_UNIX_MICROS)
                    .map(|d| d.naive_utc())
                    .ok_or_else(|| Error::out_of_range("timestamp"))
            }
            v => Err(mismatch::<NaiveDateTime>(v)),
        }
    }
}

impl FromSql for DateTime<Utc> {
    fn from_value(value: &Value) -> Result<DateTime<Utc>> {
        match value {
            Value::Null => Err(unexpected_null::<DateTime<Utc>>()),
            Value::TimestampTz(t) | Value::Timestamp(t) => {
                DateTime::<Utc>::from_timestamp_micros(*t + PG_EPOCH_UNIX_MICROS)
                    .ok_or_else(|| Error::out_of_range("timestamp"))
            }
            v => Err(mismatch::<DateTime<Utc>>(v)),
        }
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_value(value: &Value) -> Result<Vec<T>> {
        match value {
            Value::Null => Err(unexpected_null::<Vec<T>>()),
            Value::Array(items) | Value::Vector(items) => items.iter().map(T::from_value).collect(),
            v => Err(mismatch::<Vec<T>>(v)),
        }
    }
}

impl ToSql for [u8; 16] {
    /// Sixteen bytes are a `uuid`.
    fn to_value(&self) -> Value {
        Value::Uuid(*self)
    }
}
