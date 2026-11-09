//! Byte encodings of values.
//!
//! * [`encode_key`] produces an order-preserving ("memcomparable") encoding
//!   used for primary keys, index keys and hash/group keys: comparing two
//!   encodings bytewise gives the same result as comparing the values.
//! * [`encode_row`] / [`decode_row`] are the compact self-describing format
//!   rows are stored in.

use rust_decimal::Decimal;

use crate::types::{Interval, Value};
use crate::{Error, Result};

fn push_escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn push_f64(out: &mut Vec<u8>, f: f64) {
    // NaN sorts above everything, -0 equals +0.
    let f = if f.is_nan() {
        f64::NAN
    } else if f == 0.0 {
        0.0
    } else {
        f
    };
    let bits = f.to_bits();
    let bits = if bits >> 63 == 0 { bits ^ (1 << 63) } else { !bits };
    out.extend_from_slice(&bits.to_be_bytes());
}

fn push_decimal(out: &mut Vec<u8>, d: &Decimal) {
    let d = d.normalize();
    if d.is_zero() {
        out.push(0x80);
        return;
    }
    let neg = d.is_sign_negative();
    let digits = d.mantissa().unsigned_abs().to_string();
    // Position of the decimal point relative to the first digit.
    let exp = (digits.len() as i32 - d.scale() as i32 + 0x4000) as u16;
    let start = out.len();
    out.push(0xC0);
    out.extend_from_slice(&exp.to_be_bytes());
    out.extend(digits.bytes().map(|b| b - b'0' + 1));
    out.push(0);
    if neg {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

/// Appends the order-preserving encoding of `v`.
///
/// Integers are widened to 64 bits and floats to `f64`, so values of
/// different integer (or float) widths encode identically. NULL sorts
/// first.
pub fn encode_key(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => {
            out.push(0);
            return;
        }
        _ => out.push(1),
    }
    match v {
        Value::Null => unreachable!(),
        Value::Bool(b) => out.push(*b as u8),
        Value::Int2(i) => out.extend_from_slice(&((*i as i64) ^ i64::MIN).to_be_bytes()),
        Value::Int4(i) => out.extend_from_slice(&((*i as i64) ^ i64::MIN).to_be_bytes()),
        Value::Int8(i) => out.extend_from_slice(&(*i ^ i64::MIN).to_be_bytes()),
        Value::Float4(f) => push_f64(out, *f as f64),
        Value::Float8(f) => push_f64(out, *f),
        Value::Numeric(d) => push_decimal(out, d),
        Value::Text(s) | Value::Json(s) => push_escaped(out, s.as_bytes()),
        Value::Bytea(b) => push_escaped(out, b),
        Value::Date(d) => out.extend_from_slice(&((*d as i64 * 86_400_000_000) ^ i64::MIN).to_be_bytes()),
        Value::Time(t) | Value::Timestamp(t) | Value::TimestampTz(t) => {
            out.extend_from_slice(&(*t ^ i64::MIN).to_be_bytes())
        }
        Value::Interval(i) => out.extend_from_slice(&(i.cmp_key() ^ i128::MIN).to_be_bytes()),
        Value::Uuid(u) => out.extend_from_slice(u),
        Value::Enum(ord, _) => out.extend_from_slice(&ord.to_be_bytes()),
        Value::Jsonb(j) => push_escaped(out, j.to_string().as_bytes()),
        Value::Array(items) | Value::Record(items) => {
            for item in items {
                out.push(1);
                encode_key(out, item);
            }
            out.push(0);
        }
    }
}

/// Order-preserving encoding of a tuple of values.
pub fn encode_key_tuple<'a>(values: impl IntoIterator<Item = &'a Value>) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    for v in values {
        encode_key(&mut out, v);
    }
    out
}

fn push_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn push_bytes(out: &mut Vec<u8>, b: &[u8]) {
    push_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0),
        Value::Bool(false) => out.push(1),
        Value::Bool(true) => out.push(2),
        Value::Int2(i) => {
            out.push(3);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Int4(i) => {
            out.push(4);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Int8(i) => {
            out.push(5);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float4(f) => {
            out.push(6);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Float8(f) => {
            out.push(7);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Numeric(d) => {
            out.push(8);
            out.extend_from_slice(&d.serialize());
        }
        Value::Text(s) => {
            out.push(9);
            push_bytes(out, s.as_bytes());
        }
        Value::Bytea(b) => {
            out.push(10);
            push_bytes(out, b);
        }
        Value::Date(d) => {
            out.push(11);
            out.extend_from_slice(&d.to_le_bytes());
        }
        Value::Time(t) => {
            out.push(12);
            out.extend_from_slice(&t.to_le_bytes());
        }
        Value::Timestamp(t) => {
            out.push(13);
            out.extend_from_slice(&t.to_le_bytes());
        }
        Value::TimestampTz(t) => {
            out.push(14);
            out.extend_from_slice(&t.to_le_bytes());
        }
        Value::Interval(i) => {
            out.push(15);
            out.extend_from_slice(&i.months.to_le_bytes());
            out.extend_from_slice(&i.days.to_le_bytes());
            out.extend_from_slice(&i.micros.to_le_bytes());
        }
        Value::Uuid(u) => {
            out.push(16);
            out.extend_from_slice(u);
        }
        Value::Json(s) => {
            out.push(17);
            push_bytes(out, s.as_bytes());
        }
        Value::Jsonb(j) => {
            out.push(18);
            push_bytes(out, j.to_string().as_bytes());
        }
        Value::Array(items) => {
            out.push(19);
            push_varint(out, items.len() as u64);
            items.iter().for_each(|i| encode_value(out, i));
        }
        Value::Enum(ord, label) => {
            out.push(20);
            push_varint(out, *ord as u64);
            push_bytes(out, label.as_bytes());
        }
        Value::Record(items) => {
            out.push(21);
            push_varint(out, items.len() as u64);
            items.iter().for_each(|i| encode_value(out, i));
        }
    }
}

/// Serializes a row for storage.
pub fn encode_row(row: &[Value]) -> Vec<u8> {
    let mut out = Vec::with_capacity(row.len() * 9 + 2);
    push_varint(&mut out, row.len() as u64);
    for v in row {
        encode_value(&mut out, v);
    }
    out
}

struct Reader<'a> {
    buf: &'a [u8],
}

fn corrupt() -> Error {
    Error::new(crate::sqlstate::DATA_CORRUPTED, "stored row is malformed")
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(corrupt());
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Ok(a)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn varint(&mut self) -> Result<u64> {
        let mut n = 0u64;
        let mut shift = 0;
        loop {
            let b = self.take(1)?[0];
            n |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(n);
            }
            shift += 7;
            if shift > 63 {
                return Err(corrupt());
            }
        }
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.varint()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| corrupt())
    }
    fn value(&mut self) -> Result<Value> {
        Ok(match self.take(1)?[0] {
            0 => Value::Null,
            1 => Value::Bool(false),
            2 => Value::Bool(true),
            3 => Value::Int2(i16::from_le_bytes(self.array()?)),
            4 => Value::Int4(i32::from_le_bytes(self.array()?)),
            5 => Value::Int8(i64::from_le_bytes(self.array()?)),
            6 => Value::Float4(f32::from_le_bytes(self.array()?)),
            7 => Value::Float8(f64::from_le_bytes(self.array()?)),
            8 => Value::Numeric(Decimal::deserialize(self.array()?)),
            9 => Value::Text(self.string()?),
            10 => Value::Bytea(self.bytes()?.to_vec()),
            11 => Value::Date(i32::from_le_bytes(self.array()?)),
            12 => Value::Time(i64::from_le_bytes(self.array()?)),
            13 => Value::Timestamp(i64::from_le_bytes(self.array()?)),
            14 => Value::TimestampTz(i64::from_le_bytes(self.array()?)),
            15 => Value::Interval(Interval {
                months: i32::from_le_bytes(self.array()?),
                days: i32::from_le_bytes(self.array()?),
                micros: i64::from_le_bytes(self.array()?),
            }),
            16 => Value::Uuid(self.array()?),
            17 => Value::Json(self.string()?),
            18 => Value::Jsonb(Box::new(serde_json::from_slice(self.bytes()?).map_err(|_| corrupt())?)),
            t @ (19 | 21) => {
                let n = self.varint()? as usize;
                let mut items = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    items.push(self.value()?);
                }
                if t == 19 {
                    Value::Array(items)
                } else {
                    Value::Record(items)
                }
            }
            20 => {
                let ord = self.varint()? as u32;
                Value::Enum(ord, self.string()?)
            }
            _ => return Err(corrupt()),
        })
    }
}

/// Deserializes a stored row.
pub fn decode_row(buf: &[u8]) -> Result<Vec<Value>> {
    let mut r = Reader { buf };
    let n = r.varint()? as usize;
    let mut row = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        row.push(r.value()?);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::compare;
    use std::str::FromStr;

    fn key(v: &Value) -> Vec<u8> {
        encode_key_tuple([v])
    }

    #[test]
    fn key_order_matches_value_order() {
        let groups: Vec<Vec<Value>> = vec![
            [i64::MIN, -5, -1, 0, 1, 7, 255, 256, i64::MAX].iter().map(|i| Value::Int8(*i)).collect(),
            [f64::NEG_INFINITY, -1e300, -1.5, -0.0, 1e-300, 2.0, f64::INFINITY, f64::NAN]
                .iter()
                .map(|f| Value::Float8(*f))
                .collect(),
            [
                "-1000000",
                "-12.5",
                "-12.49",
                "-0.001",
                "0",
                "0.0001",
                "0.001",
                "0.5",
                "1",
                "1.01",
                "9.99",
                "10",
                "100.5",
                "79228162514264337593543950335",
            ]
            .iter()
            .map(|s| Value::Numeric(Decimal::from_str(s).unwrap()))
            .collect(),
            ["", "\0", "\0a", "a", "a\0", "a\0b", "ab", "b"].iter().map(|s| Value::text(*s)).collect(),
            vec![
                Value::Array(vec![]),
                Value::Array(vec![Value::Int4(1)]),
                Value::Array(vec![Value::Int4(1), Value::Int4(0)]),
                Value::Array(vec![Value::Int4(2)]),
            ],
        ];
        for g in groups {
            for a in &g {
                for b in &g {
                    assert_eq!(compare(a, b), key(a).cmp(&key(b)), "{a:?} vs {b:?}");
                }
            }
        }
        assert_eq!(key(&Value::Int2(7)), key(&Value::Int8(7)));
        assert_eq!(
            key(&Value::Numeric(Decimal::from_str("1.50").unwrap())),
            key(&Value::Numeric(Decimal::from_str("1.5").unwrap()))
        );
        assert!(key(&Value::Null) < key(&Value::Bool(false)));
    }

    #[test]
    fn row_roundtrip() {
        let row = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int2(-3),
            Value::Int4(70000),
            Value::Int8(i64::MIN),
            Value::Float4(1.5),
            Value::Float8(-2.25),
            Value::Numeric(Decimal::from_str("123.4500").unwrap()),
            Value::text("héllo"),
            Value::Bytea(vec![0, 1, 255]),
            Value::Date(-1),
            Value::Time(1),
            Value::Timestamp(2),
            Value::TimestampTz(3),
            Value::Interval(Interval { months: 1, days: -2, micros: 3 }),
            Value::Uuid([7; 16]),
            Value::Json("{\"a\" : 1}".into()),
            Value::Jsonb(Box::new(serde_json::json!({"a": [1, "x", null]}))),
            Value::Array(vec![Value::Int4(1), Value::Null]),
            Value::Enum(2, "happy".into()),
            Value::Record(vec![Value::Int4(1), Value::text("x")]),
        ];
        assert_eq!(decode_row(&encode_row(&row)).unwrap(), row);
        assert!(decode_row(&[5, 9]).is_err());
    }
}
