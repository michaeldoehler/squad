//! Postgres binary wire format (the types' send/receive functions).

use rust_decimal::Decimal;

use super::io;
use crate::error::sqlstate;
use crate::types::Interval;
use crate::{Error, Result, Type, Value};

fn bad(ty: &Type) -> Error {
    Error::new(sqlstate::INVALID_BINARY_REPRESENTATION, format!("incorrect binary data format for type {}", ty.name()))
}

fn encode_numeric(d: &Decimal, out: &mut Vec<u8>) {
    let scale = d.scale() as usize;
    let digits = d.mantissa().unsigned_abs().to_string();
    let digits =
        if digits.len() <= scale { format!("{}{digits}", "0".repeat(scale - digits.len() + 1)) } else { digits };
    let (int, frac) = digits.split_at(digits.len() - scale);
    let int_padded = format!("{}{int}", "0".repeat((4 - int.len() % 4) % 4));
    let frac_padded = format!("{frac}{}", "0".repeat((4 - frac.len() % 4) % 4));
    let mut groups: Vec<i16> = Vec::new();
    for chunk in int_padded.as_bytes().chunks(4).chain(frac_padded.as_bytes().chunks(4)) {
        groups.push(chunk.iter().fold(0i16, |acc, b| acc * 10 + (b - b'0') as i16));
    }
    let mut weight = (int_padded.len() / 4) as i16 - 1;
    let mut start = 0;
    while start < groups.len() && groups[start] == 0 {
        start += 1;
        weight -= 1;
    }
    let mut end = groups.len();
    while end > start && groups[end - 1] == 0 {
        end -= 1;
    }
    let groups = &groups[start..end];
    if groups.is_empty() {
        weight = 0;
    }
    out.extend_from_slice(&(groups.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&(if d.is_sign_negative() && !d.is_zero() { 0x4000u16 } else { 0 }).to_be_bytes());
    out.extend_from_slice(&(scale as i16).to_be_bytes());
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
}

fn decode_numeric(b: &[u8]) -> Result<Decimal> {
    let e = || bad(&Type::Numeric);
    if b.len() < 8 {
        return Err(e());
    }
    let i16_at = |i: usize| i16::from_be_bytes([b[i], b[i + 1]]);
    let n = i16_at(0) as usize;
    let weight = i16_at(2) as i32;
    let sign = u16::from_be_bytes([b[4], b[5]]);
    let dscale = i16_at(6) as u32;
    if b.len() != 8 + n * 2 || n > 64 {
        return Err(e());
    }
    if sign != 0 && sign != 0x4000 {
        return Err(Error::unsupported("numeric NaN and infinity"));
    }
    let group = |g: usize| format!("{:04}", i16_at(8 + g * 2));
    let mut s = String::new();
    if sign == 0x4000 {
        s.push('-');
    }
    if weight >= 0 {
        for g in 0..=(weight as usize) {
            s.push_str(&if g < n { group(g) } else { "0000".into() });
        }
        s.push('.');
        for g in (weight as usize + 1)..n {
            s.push_str(&group(g));
        }
    } else {
        s.push_str("0.");
        s.push_str(&"0000".repeat((-1 - weight) as usize));
        for g in 0..n {
            s.push_str(&group(g));
        }
    }
    if s.ends_with('.') {
        s.push('0');
    }
    let d = super::numeric::parse_numeric(&s)?;
    Ok(super::numeric::rescale(d, dscale.min(super::numeric::MAX_SCALE)))
}

/// Appends the binary representation of a non-null value.
pub fn encode_binary(v: &Value, ty: &Type, out: &mut Vec<u8>) -> Result<()> {
    match v {
        Value::Null => return Err(Error::internal("NULL has no binary representation")),
        Value::Bool(b) => out.push(*b as u8),
        Value::Int2(i) => out.extend_from_slice(&i.to_be_bytes()),
                Value::Int4(i) => out.extend_from_slice(&i.to_be_bytes()),
        // An `oid` travels as four unsigned bytes.
        Value::Int8(i) if *ty == Type::Oid => out.extend_from_slice(&(*i as u32).to_be_bytes()),
        Value::Int8(i) => out.extend_from_slice(&i.to_be_bytes()),
        // `"char"` is a single byte.
        Value::Text(s) if *ty == Type::Char => out.push(s.as_bytes().first().copied().unwrap_or(0)),
        Value::Float4(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Float8(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Numeric(d) => encode_numeric(d, out),
        Value::Text(s) | Value::Json(s) | Value::Enum(_, s) => out.extend_from_slice(s.as_bytes()),
        Value::Bytea(b) => out.extend_from_slice(b),
        Value::Date(d) => out.extend_from_slice(&d.to_be_bytes()),
        Value::Time(t) | Value::Timestamp(t) | Value::TimestampTz(t) => out.extend_from_slice(&t.to_be_bytes()),
        Value::Interval(iv) => {
            out.extend_from_slice(&iv.micros.to_be_bytes());
            out.extend_from_slice(&iv.days.to_be_bytes());
            out.extend_from_slice(&iv.months.to_be_bytes());
        }
        Value::Uuid(u) => out.extend_from_slice(u),
        Value::Jsonb(j) => {
            out.push(1);
            out.extend_from_slice(io::fmt_jsonb(j).as_bytes());
        }
        Value::Array(items) => {
            let elem = match ty {
                Type::Array(e) => (**e).clone(),
                _ => v.type_of().element().cloned().unwrap_or(Type::Text),
            };
            let elem = if elem == Type::Unknown { Type::Text } else { elem };
            let has_null = items.iter().any(Value::is_null);
            out.extend_from_slice(&(if items.is_empty() { 0i32 } else { 1 }).to_be_bytes());
            out.extend_from_slice(&(has_null as i32).to_be_bytes());
            out.extend_from_slice(&elem.oid().to_be_bytes());
            if !items.is_empty() {
                out.extend_from_slice(&(items.len() as i32).to_be_bytes());
                out.extend_from_slice(&1i32.to_be_bytes());
                for item in items {
                    if item.is_null() {
                        out.extend_from_slice(&(-1i32).to_be_bytes());
                        continue;
                    }
                    let at = out.len();
                    out.extend_from_slice(&[0; 4]);
                    encode_binary(item, &elem, out)?;
                    let len = (out.len() - at - 4) as i32;
                    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
                }
            }
        }
        Value::Record(items) => {
            out.extend_from_slice(&(items.len() as i32).to_be_bytes());
            for item in items {
                let t = item.type_of();
                let t = if t == Type::Unknown { Type::Text } else { t };
                out.extend_from_slice(&t.oid().to_be_bytes());
                if item.is_null() {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                    continue;
                }
                let at = out.len();
                out.extend_from_slice(&[0; 4]);
                encode_binary(item, &t, out)?;
                let len = (out.len() - at - 4) as i32;
                out[at..at + 4].copy_from_slice(&len.to_be_bytes());
            }
        }
    }
    Ok(())
}

/// Parses the binary representation of a value of type `ty`. Enum values
/// come back as [`Value::Text`] labels for the caller to resolve.
pub fn decode_binary(b: &[u8], ty: &Type) -> Result<Value> {
    let e = || bad(ty);
    fn arr<const N: usize>(b: &[u8], e: impl Fn() -> Error) -> Result<[u8; N]> {
        b.try_into().map_err(|_| e())
    }
    let utf8 = |b: &[u8]| {
        std::str::from_utf8(b).map(str::to_string).map_err(|_| {
            Error::new(sqlstate::CHARACTER_NOT_IN_REPERTOIRE, "invalid byte sequence for encoding \"UTF8\"")
        })
    };
    Ok(match ty {
        Type::Bool => Value::Bool(*b.first().filter(|_| b.len() == 1).ok_or_else(e)? != 0),
        Type::Int2 => Value::Int2(i16::from_be_bytes(arr(b, e)?)),
        Type::Int4 => Value::Int4(i32::from_be_bytes(arr(b, e)?)),
                Type::Int8 => Value::Int8(i64::from_be_bytes(arr(b, e)?)),
        Type::Oid => Value::Int8(u32::from_be_bytes(arr(b, e)?) as i64),
        Type::Char => match b {
            [0] | [] => Value::Text(String::new()),
            [c] => Value::Text((*c as char).to_string()),
            _ => return Err(e()),
        },
        Type::Float4 => Value::Float4(f32::from_be_bytes(arr(b, e)?)),
        Type::Float8 => Value::Float8(f64::from_be_bytes(arr(b, e)?)),
        Type::Numeric => Value::Numeric(decode_numeric(b)?),
        Type::Text | Type::Unknown | Type::Enum(_) => Value::Text(utf8(b)?),
        Type::Bytea => Value::Bytea(b.to_vec()),
        Type::Date => Value::Date(i32::from_be_bytes(arr(b, e)?)),
        Type::Time => Value::Time(i64::from_be_bytes(arr(b, e)?)),
        Type::Timestamp => Value::Timestamp(i64::from_be_bytes(arr(b, e)?)),
        Type::TimestampTz => Value::TimestampTz(i64::from_be_bytes(arr(b, e)?)),
        Type::Interval => {
            if b.len() != 16 {
                return Err(e());
            }
            Value::Interval(Interval {
                micros: i64::from_be_bytes(arr(&b[..8], e)?),
                days: i32::from_be_bytes(arr(&b[8..12], e)?),
                months: i32::from_be_bytes(arr(&b[12..], e)?),
            })
        }
        Type::Uuid => Value::Uuid(arr(b, e)?),
        Type::Json => {
            let s = utf8(b)?;
            io::parse_json(&s, "json")?;
            Value::Json(s)
        }
        Type::Jsonb => {
            if b.first() != Some(&1) {
                return Err(Error::new(sqlstate::INVALID_BINARY_REPRESENTATION, "unsupported jsonb version number"));
            }
            Value::Jsonb(Box::new(io::parse_json(&utf8(&b[1..])?, "jsonb")?))
        }
        Type::Array(elem) => {
            if b.len() < 12 {
                return Err(e());
            }
            let i32_at = |i: usize| -> Result<i32> { Ok(i32::from_be_bytes(arr(b.get(i..i + 4).ok_or_else(e)?, e)?)) };
            let ndim = i32_at(0)?;
            if ndim == 0 {
                return Ok(Value::Array(Vec::new()));
            }
            if ndim != 1 {
                return Err(Error::unsupported("multidimensional arrays"));
            }
            let n = i32_at(12)?;
            let mut pos = 20;
            let mut items = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n {
                let len = i32_at(pos)?;
                pos += 4;
                if len < 0 {
                    items.push(Value::Null);
                } else {
                    let data = b.get(pos..pos + len as usize).ok_or_else(e)?;
                    items.push(decode_binary(data, elem)?);
                    pos += len as usize;
                }
            }
            Value::Array(items)
        }
        Type::Record | Type::Void => return Err(Error::unsupported(format!("binary input of type {}", ty.name()))),
    })
}
