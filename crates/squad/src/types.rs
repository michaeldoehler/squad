//! SQL data types and runtime values.

use std::cmp::Ordering;
use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A SQL data type.
///
/// `varchar(n)`, `char(n)` and `name` are represented as [`Type::Text`];
/// the declared form is kept in the column definition for reporting.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
    /// One-dimensional array of the element type.
    Array(Box<Type>),
    /// User-defined enum, identified by its type name.
    Enum(String),
    /// Row type of an anonymous record (`ROW(...)`, whole-row references).
    Record,
    /// An untyped string literal or NULL whose type is decided by context.
    Unknown,
    Void,
    /// Object identifier, as found in the system catalogs. Values are
    /// stored and computed as `int8`; the type only affects how a column
    /// is described and encoded on the wire (4 bytes, type OID 26).
    Oid,
    /// The single-byte `"char"` type of the system catalogs. Values are
    /// `text`; the type affects only description and wire encoding.
    Char,
    /// `int2vector` of the system catalogs (`pg_index.indkey`): a
    /// one-dimensional `smallint` array with lower bound 0, written as
    /// space-separated numbers. Values are [`Value::Vector`].
    Int2Vector,
    /// `oidvector` (`pg_index.indclass`, `pg_proc.proargtypes`): like
    /// `int2vector` with `oid` elements, which are computed as `int8`.
    OidVector,
}

impl Type {
    /// Postgres type OID. Enums report the OID assigned in the catalog via
    /// the session; here they fall back to `text`.
    pub fn oid(&self) -> u32 {
        match self {
            Type::Bool => 16,
            Type::Bytea => 17,
            Type::Int8 => 20,
            Type::Int2 => 21,
            Type::Int4 => 23,
            Type::Text | Type::Enum(_) => 25,
            Type::Json => 114,
            Type::Float4 => 700,
            Type::Float8 => 701,
            Type::Unknown => 705,
            Type::Date => 1082,
            Type::Time => 1083,
            Type::Timestamp => 1114,
            Type::TimestampTz => 1184,
            Type::Interval => 1186,
            Type::Numeric => 1700,
            Type::Uuid => 2950,
            Type::Jsonb => 3802,
            Type::Record => 2249,
            Type::Void => 2278,
            Type::Oid => 26,
            Type::Char => 18,
            Type::Int2Vector => 22,
            Type::OidVector => 30,
            Type::Array(elem) => match **elem {
                Type::Oid => 1028,
                Type::Char => 1002,
                Type::Bool => 1000,
                Type::Bytea => 1001,
                Type::Int2 => 1005,
                Type::Int4 => 1007,
                Type::Text | Type::Enum(_) | Type::Unknown => 1009,
                Type::Int8 => 1016,
                Type::Float4 => 1021,
                Type::Float8 => 1022,
                Type::Timestamp => 1115,
                Type::Date => 1182,
                Type::Time => 1183,
                Type::TimestampTz => 1185,
                Type::Interval => 1187,
                Type::Numeric => 1231,
                Type::Uuid => 2951,
                Type::Json => 199,
                Type::Jsonb => 3807,
                Type::Int2Vector => 1006,
                Type::OidVector => 1013,
                _ => 2277,
            },
        }
    }

    /// The type for a Postgres type OID, for the built-in types.
    pub fn from_oid(oid: u32) -> Option<Type> {
        let scalar = |oid| {
            Some(match oid {
                16 => Type::Bool,
                17 => Type::Bytea,
                18 => Type::Char,
                22 => Type::Int2Vector,
                26 => Type::Oid,
                30 => Type::OidVector,
                20 => Type::Int8,
                21 => Type::Int2,
                23 => Type::Int4,
                25 | 1042 | 1043 | 19 => Type::Text,
                114 => Type::Json,
                700 => Type::Float4,
                701 => Type::Float8,
                705 => Type::Unknown,
                1082 => Type::Date,
                1083 => Type::Time,
                1114 => Type::Timestamp,
                1184 => Type::TimestampTz,
                1186 => Type::Interval,
                1700 => Type::Numeric,
                2950 => Type::Uuid,
                3802 => Type::Jsonb,
                _ => return None,
            })
        };
        scalar(oid).or_else(|| {
            let elem = match oid {
                1000 => 16,
                1002 => 18,
                1028 => 26,
                1001 => 17,
                1005 => 21,
                1007 => 23,
                1009 | 1015 | 1014 => 25,
                1016 => 20,
                1021 => 700,
                1022 => 701,
                1115 => 1114,
                1182 => 1082,
                1183 => 1083,
                1185 => 1184,
                1187 => 1186,
                1231 => 1700,
                2951 => 2950,
                199 => 114,
                3807 => 3802,
                _ => return None,
            };
            scalar(elem).map(|t| Type::Array(Box::new(t)))
        })
    }

    /// Name as printed by `format_type` / in error messages.
    pub fn name(&self) -> String {
        match self {
            Type::Bool => "boolean".into(),
            Type::Int2 => "smallint".into(),
            Type::Int4 => "integer".into(),
            Type::Int8 => "bigint".into(),
            Type::Float4 => "real".into(),
            Type::Float8 => "double precision".into(),
            Type::Numeric => "numeric".into(),
            Type::Text => "text".into(),
            Type::Bytea => "bytea".into(),
            Type::Date => "date".into(),
            Type::Time => "time without time zone".into(),
            Type::Timestamp => "timestamp without time zone".into(),
            Type::TimestampTz => "timestamp with time zone".into(),
            Type::Interval => "interval".into(),
            Type::Uuid => "uuid".into(),
            Type::Json => "json".into(),
            Type::Jsonb => "jsonb".into(),
            Type::Array(e) => format!("{}[]", e.name()),
            Type::Enum(n) => n.clone(),
            Type::Record => "record".into(),
            Type::Unknown => "unknown".into(),
            Type::Void => "void".into(),
            Type::Oid => "oid".into(),
            Type::Char => "\"char\"".into(),
            Type::Int2Vector => "int2vector".into(),
            Type::OidVector => "oidvector".into(),
        }
    }

    /// The type values of this type are computed as: the catalog-only
    /// types `oid` and `"char"` behave like `int8` and `text`.
    pub fn base(&self) -> Type {
        match self {
            Type::Oid => Type::Int8,
            Type::Char => Type::Text,
            Type::Array(elem) => Type::Array(Box::new(elem.base())),
            other => other.clone(),
        }
    }

    /// Resolves a built-in type name (as written in SQL or as found in
    /// `pg_type.typname`) to a type. Enum names are resolved by the catalog.
    pub fn from_name(name: &str) -> Option<Type> {
        Some(match name.to_ascii_lowercase().as_str() {
            "bool" | "boolean" => Type::Bool,
            "int2" | "smallint" | "smallserial" | "serial2" => Type::Int2,
            "int4" | "int" | "integer" | "serial" | "serial4" => Type::Int4,
            "oid" => Type::Oid,
            "int8" | "bigint" | "bigserial" | "serial8" | "regclass" | "regtype" | "regproc" | "xid" => Type::Int8,
            "float4" | "real" => Type::Float4,
            "float8" | "double precision" | "float" => Type::Float8,
            "numeric" | "decimal" => Type::Numeric,
            "text" | "varchar" | "character varying" | "bpchar" | "char" | "character" | "name" | "citext" => {
                Type::Text
            }
            "bytea" => Type::Bytea,
            "date" => Type::Date,
            "time" | "time without time zone" => Type::Time,
            "timestamp" | "timestamp without time zone" => Type::Timestamp,
            "timestamptz" | "timestamp with time zone" => Type::TimestampTz,
            "interval" => Type::Interval,
            "uuid" => Type::Uuid,
            "json" => Type::Json,
            "jsonb" => Type::Jsonb,
            "record" => Type::Record,
            "unknown" => Type::Unknown,
            "void" => Type::Void,
            "int2vector" => Type::Int2Vector,
            "oidvector" => Type::OidVector,
            _ => return None,
        })
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Type::Int2 | Type::Int4 | Type::Int8 | Type::Float4 | Type::Float8 | Type::Numeric)
    }
    pub fn is_integer(&self) -> bool {
        matches!(self, Type::Int2 | Type::Int4 | Type::Int8)
    }
    pub fn is_string(&self) -> bool {
        matches!(self, Type::Text | Type::Unknown)
    }
    pub fn is_datetime(&self) -> bool {
        matches!(self, Type::Date | Type::Time | Type::Timestamp | Type::TimestampTz | Type::Interval)
    }
    pub fn array_of(self) -> Type {
        Type::Array(Box::new(self))
    }
    pub fn element(&self) -> Option<&Type> {
        match self {
            Type::Array(e) => Some(e),
            Type::Int2Vector => Some(&Type::Int2),
            Type::OidVector => Some(&Type::Int8),
            _ => None,
        }
    }

    /// `int2vector` or `oidvector`.
    pub fn is_vector(&self) -> bool {
        matches!(self, Type::Int2Vector | Type::OidVector)
    }

    /// The element type of a vector type as expressions compute it
    /// (`oid` elements are `int8`), for vector types only.
    pub fn vector_elem(&self) -> Option<Type> {
        match self {
            Type::Int2Vector => Some(Type::Int2),
            Type::OidVector => Some(Type::Int8),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// A time span, with the three independent fields Postgres keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    /// Total length assuming 30-day months and 24-hour days; this is the
    /// value Postgres compares and sorts intervals by.
    pub fn cmp_key(&self) -> i128 {
        (self.months as i128 * 30 + self.days as i128) * 86_400_000_000 + self.micros as i128
    }
}

/// Microseconds between 1970-01-01 and 2000-01-01, the Postgres epoch.
pub const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
/// Days between 1970-01-01 and 2000-01-01.
pub const PG_EPOCH_UNIX_DAYS: i32 = 10_957;

/// A runtime SQL value.
///
/// Date and time values use the Postgres binary representation: days
/// (`Date`) or microseconds (`Timestamp`, `TimestampTz`) since 2000-01-01,
/// timestamps with time zone in UTC, and microseconds since midnight for
/// `Time`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Numeric(Decimal),
    Text(String),
    Bytea(Vec<u8>),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    TimestampTz(i64),
    Interval(Interval),
    Uuid([u8; 16]),
    /// `json` keeps the input text verbatim.
    Json(String),
    Jsonb(Box<serde_json::Value>),
    Array(Vec<Value>),
    /// An `int2vector` or `oidvector`: a one-dimensional array without
    /// NULLs whose subscripts start at 0 and whose text form is the
    /// elements separated by spaces. Elements are `Int2` or `Int8` (oid).
    Vector(Vec<Value>),
    /// Enum value: position in the type's label list and the label.
    Enum(u32, String),
    /// A composite value (`ROW(...)`).
    Record(Vec<Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn text(s: impl Into<String>) -> Value {
        Value::Text(s.into())
    }

    /// The type of a non-null value; `Unknown` for NULL. Array element and
    /// enum type names are not recoverable from the value alone.
    pub fn type_of(&self) -> Type {
        match self {
            Value::Null => Type::Unknown,
            Value::Bool(_) => Type::Bool,
            Value::Int2(_) => Type::Int2,
            Value::Int4(_) => Type::Int4,
            Value::Int8(_) => Type::Int8,
            Value::Float4(_) => Type::Float4,
            Value::Float8(_) => Type::Float8,
            Value::Numeric(_) => Type::Numeric,
            Value::Text(_) => Type::Text,
            Value::Bytea(_) => Type::Bytea,
            Value::Date(_) => Type::Date,
            Value::Time(_) => Type::Time,
            Value::Timestamp(_) => Type::Timestamp,
            Value::TimestampTz(_) => Type::TimestampTz,
            Value::Interval(_) => Type::Interval,
            Value::Uuid(_) => Type::Uuid,
            Value::Json(_) => Type::Json,
            Value::Jsonb(_) => Type::Jsonb,
            Value::Array(items) => {
                Type::Array(Box::new(items.iter().find(|v| !v.is_null()).map_or(Type::Unknown, Value::type_of)))
            }
            Value::Vector(items) => match items.first() {
                Some(Value::Int8(_)) => Type::OidVector,
                _ => Type::Int2Vector,
            },
            Value::Enum(..) => Type::Text,
            Value::Record(_) => Type::Record,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Integer value of any integer-typed value.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int2(v) => Some(*v as i64),
            Value::Int4(v) => Some(*v as i64),
            Value::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        use rust_decimal::prelude::ToPrimitive;
        match self {
            Value::Int2(v) => Some(*v as f64),
            Value::Int4(v) => Some(*v as f64),
            Value::Int8(v) => Some(*v as f64),
            Value::Float4(v) => Some(*v as f64),
            Value::Float8(v) => Some(*v),
            Value::Numeric(d) => d.to_f64(),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        use rust_decimal::prelude::FromPrimitive;
        match self {
            Value::Int2(v) => Some(Decimal::from(*v)),
            Value::Int4(v) => Some(Decimal::from(*v)),
            Value::Int8(v) => Some(Decimal::from(*v)),
            Value::Numeric(d) => Some(*d),
            Value::Float4(v) => Decimal::from_f32(*v),
            Value::Float8(v) => Decimal::from_f64(*v),
            _ => None,
        }
    }

    /// Elements and lower bound of an array or vector value.
    pub fn array_items(&self) -> Option<(&[Value], i32)> {
        match self {
            Value::Array(items) => Some((items, 1)),
            Value::Vector(items) => Some((items, 0)),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) | Value::Enum(_, s) | Value::Json(s) => Some(s),
            _ => None,
        }
    }
}

fn cmp_f64(a: f64, b: f64) -> Ordering {
    // Postgres: NaN equals NaN and sorts above every other value.
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap(),
    }
}

/// Total order of two non-null values of the same type (numeric types may
/// be mixed). NULL sorts above everything, matching `ORDER BY ... ASC`
/// defaults; SQL comparison operators handle NULL before calling this.
pub fn compare(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Greater,
        (_, Null) => Ordering::Less,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int2(x), Int2(y)) => x.cmp(y),
        (Int4(x), Int4(y)) => x.cmp(y),
        (Int8(x), Int8(y)) => x.cmp(y),
        (Float4(x), Float4(y)) => cmp_f64(*x as f64, *y as f64),
        (Float8(x), Float8(y)) => cmp_f64(*x, *y),
        (Numeric(x), Numeric(y)) => x.cmp(y),
        (Text(x), Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Bytea(x), Bytea(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        (Time(x), Time(y)) => x.cmp(y),
        (Timestamp(x), Timestamp(y)) | (TimestampTz(x), TimestampTz(y)) => x.cmp(y),
        (Timestamp(x), TimestampTz(y)) | (TimestampTz(x), Timestamp(y)) => x.cmp(y),
        (Date(x), Timestamp(y)) | (Date(x), TimestampTz(y)) => (*x as i64 * 86_400_000_000).cmp(y),
        (Timestamp(x), Date(y)) | (TimestampTz(x), Date(y)) => x.cmp(&(*y as i64 * 86_400_000_000)),
        (Interval(x), Interval(y)) => x.cmp_key().cmp(&y.cmp_key()),
        (Uuid(x), Uuid(y)) => x.cmp(y),
        (Json(x), Json(y)) => x.cmp(y),
        (Jsonb(x), Jsonb(y)) => cmp_json(x, y),
        (Enum(x, _), Enum(y, _)) => x.cmp(y),
        (Enum(_, x), Text(y)) | (Text(x), Enum(_, y)) => x.as_bytes().cmp(y.as_bytes()),
        (Array(x) | Vector(x), Array(y) | Vector(y)) | (Record(x), Record(y)) => {
            for (i, j) in x.iter().zip(y.iter()) {
                let o = compare(i, j);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => {
            // Mixed numeric types: integers exactly, otherwise via decimal
            // and finally via float.
            if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                return x.cmp(&y);
            }
            let float = |v: &Value| matches!(v, Float4(_) | Float8(_));
            if !float(a) && !float(b) {
                if let (Some(x), Some(y)) = (a.as_decimal(), b.as_decimal()) {
                    return x.cmp(&y);
                }
            }
            match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => cmp_f64(x, y),
                _ => rank(a).cmp(&rank(b)),
            }
        }
    }
}

fn rank(v: &Value) -> u8 {
    match v {
        Value::Null => 255,
        Value::Bool(_) => 1,
        Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Float4(_) | Value::Float8(_) | Value::Numeric(_) => 2,
        Value::Text(_) | Value::Enum(..) => 3,
        Value::Bytea(_) => 4,
        Value::Date(_) | Value::Timestamp(_) | Value::TimestampTz(_) => 5,
        Value::Time(_) => 6,
        Value::Interval(_) => 7,
        Value::Uuid(_) => 8,
        Value::Json(_) | Value::Jsonb(_) => 9,
        Value::Array(_) | Value::Vector(_) => 10,
        Value::Record(_) => 11,
    }
}

/// jsonb ordering as documented by Postgres:
/// Object > Array > Boolean > Number > String > Null.
pub fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
    use serde_json::Value as J;
    let rank = |v: &J| match v {
        J::Null => 0,
        J::String(_) => 1,
        J::Number(_) => 2,
        J::Bool(_) => 3,
        J::Array(_) => 4,
        J::Object(_) => 5,
    };
    match (a, b) {
        (J::String(x), J::String(y)) => x.cmp(y),
        (J::Number(x), J::Number(y)) => cmp_f64(x.as_f64().unwrap_or(f64::NAN), y.as_f64().unwrap_or(f64::NAN)),
        (J::Bool(x), J::Bool(y)) => x.cmp(y),
        (J::Array(x), J::Array(y)) => x.len().cmp(&y.len()).then_with(|| {
            x.iter().zip(y).map(|(i, j)| cmp_json(i, j)).find(|o| *o != Ordering::Equal).unwrap_or(Ordering::Equal)
        }),
        (J::Object(x), J::Object(y)) => x.len().cmp(&y.len()).then_with(|| {
            x.iter()
                .zip(y)
                .map(|((k1, v1), (k2, v2))| k1.cmp(k2).then_with(|| cmp_json(v1, v2)))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        }),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Session state that scalar functions may read.
#[derive(Debug, Clone)]
pub struct FnCtx {
    /// Start of the current transaction (`now()`), microseconds since the
    /// Postgres epoch, UTC.
    pub txn_time: i64,
    /// Start of the current statement (`statement_timestamp()`).
    pub stmt_time: i64,
    /// Session time zone (`SET TIME ZONE`).
    pub tz: chrono_tz::Tz,
}

impl Default for FnCtx {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp_micros() - PG_EPOCH_UNIX_MICROS;
        FnCtx { txn_time: now, stmt_time: now, tz: chrono_tz::UTC }
    }
}
