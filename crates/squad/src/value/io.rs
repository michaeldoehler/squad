//! Text input and output functions of all types.

use rust_decimal::Decimal;

use super::datetime as dt;
use super::numeric;
use crate::error::sqlstate;
use crate::{Error, FnCtx, Result, Type, Value};

// ------------------------------------------------------------------ input

pub fn parse_bool(input: &str) -> Result<bool> {
    let s = input.trim().to_ascii_lowercase();
    let is_prefix = |full: &str, min: usize| s.len() >= min && full.starts_with(s.as_str());
    if is_prefix("true", 1) || is_prefix("yes", 1) || s == "on" || s == "1" {
        Ok(true)
    } else if is_prefix("false", 1) || is_prefix("no", 1) || is_prefix("off", 2) || s == "0" {
        Ok(false)
    } else {
        Err(Error::invalid_text("boolean", input))
    }
}

fn parse_int(input: &str, type_name: &str, min: i128, max: i128) -> Result<i128> {
    let bad = || Error::invalid_text(type_name, input);
    let s = input.trim();
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        Some(_) => (false, s),
        None => return Err(bad()),
    };
    let (radix, digits) = match body.get(..2).map(|p| p.to_ascii_lowercase()).as_deref() {
        Some("0x") => (16, &body[2..]),
        Some("0o") => (8, &body[2..]),
        Some("0b") => (2, &body[2..]),
        _ => (10, body),
    };
    if digits.is_empty() || digits.starts_with('_') && radix == 10 || digits.ends_with('_') || digits.contains("__") {
        return Err(bad());
    }
    let mut v: i128 = 0;
    for c in digits.chars() {
        if c == '_' {
            continue;
        }
        let d = c.to_digit(radix).ok_or_else(bad)? as i128;
        v = v * radix as i128 + d;
        if v > u64::MAX as i128 {
            // Still validate the remaining characters for the right error.
            if !digits.chars().all(|c| c == '_' || c.is_digit(radix)) {
                return Err(bad());
            }
            break;
        }
    }
    let v = if neg { -v } else { v };
    if v < min || v > max {
        return Err(Error::new(
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("value \"{input}\" is out of range for type {type_name}"),
        ));
    }
    Ok(v)
}

pub fn parse_i16(s: &str) -> Result<i16> {
    parse_int(s, "smallint", i16::MIN as i128, i16::MAX as i128).map(|v| v as i16)
}
pub fn parse_i32(s: &str) -> Result<i32> {
    parse_int(s, "integer", i32::MIN as i128, i32::MAX as i128).map(|v| v as i32)
}
pub fn parse_i64(s: &str) -> Result<i64> {
    parse_int(s, "bigint", i64::MIN as i128, i64::MAX as i128).map(|v| v as i64)
}

fn parse_float(input: &str, type_name: &str, single: bool) -> Result<f64> {
    let s = input.trim();
    let lower = s.to_ascii_lowercase();
    let word = lower.trim_start_matches(['+', '-']);
    let special = matches!(word, "nan" | "inf" | "infinity");
    let ok_chars = s.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E'));
    if s.is_empty() || (!special && !ok_chars) {
        return Err(Error::invalid_text(type_name, input));
    }
    let v: f64 = lower.parse().map_err(|_| Error::invalid_text(type_name, input))?;
    let overflow = if single { (v as f32).is_infinite() } else { v.is_infinite() };
    if overflow && !special {
        return Err(Error::new(
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("\"{input}\" is out of range for type {type_name}"),
        ));
    }
    Ok(v)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if bytes.len() % 2 != 0 {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "invalid hexadecimal data: odd number of digits"));
    }
    bytes
        .chunks(2)
        .map(|p| match (hex_val(p[0]), hex_val(p[1])) {
            (Some(h), Some(l)) => Ok(h << 4 | l),
            (h, _) => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                format!("invalid hexadecimal digit: \"{}\"", (if h.is_none() { p[0] } else { p[1] }) as char),
            )),
        })
        .collect()
}

pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 15) as usize] as char);
    }
    s
}

pub fn parse_bytea(input: &str) -> Result<Vec<u8>> {
    if let Some(hex) = input.strip_prefix("\\x") {
        return decode_hex(hex);
    }
    let b = input.as_bytes();
    let bad = || Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, "invalid input syntax for type bytea");
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
        } else if b.get(i + 1) == Some(&b'\\') {
            out.push(b'\\');
            i += 2;
        } else if i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) && b[i + 1] <= b'3' {
            out.push((b[i + 1] - b'0') << 6 | (b[i + 2] - b'0') << 3 | (b[i + 3] - b'0'));
            i += 4;
        } else {
            return Err(bad());
        }
    }
    Ok(out)
}

pub fn parse_uuid(input: &str) -> Result<[u8; 16]> {
    let bad = || Error::invalid_text("uuid", input);
    let s = input.trim();
    let s = match (s.strip_prefix('{'), s.ends_with('}')) {
        (Some(inner), true) => &inner[..inner.len() - 1],
        (None, false) => s,
        _ => return Err(bad()),
    };
    let mut out = [0u8; 16];
    let mut n = 0;
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'-' {
            // Hyphens are allowed after each group of four digits.
            if n == 0 || n % 2 != 0 || i + 1 == b.len() {
                return Err(bad());
            }
            i += 1;
            continue;
        }
        let (h, l) = (hex_val(b[i]).ok_or_else(bad)?, b.get(i + 1).and_then(|c| hex_val(*c)).ok_or_else(bad)?);
        if n >= 16 {
            return Err(bad());
        }
        out[n] = h << 4 | l;
        n += 1;
        i += 2;
    }
    if n != 16 {
        return Err(bad());
    }
    Ok(out)
}

pub fn parse_json(input: &str, type_name: &str) -> Result<serde_json::Value> {
    serde_json::from_str(input).map_err(|e| {
        Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, format!("invalid input syntax for type {type_name}"))
            .with_detail(e.to_string())
    })
}

fn malformed_array(input: &str) -> Error {
    Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, format!("malformed array literal: \"{input}\""))
}

/// Splits an array literal into its raw items; `None` is an unquoted NULL.
pub fn split_array_literal(input: &str) -> Result<Vec<Option<String>>> {
    let bad = || malformed_array(input);
    let mut s = input.trim();
    if s.starts_with('[') {
        // Explicit dimensions, e.g. '[1:3]={1,2,3}'.
        let eq = s.find('=').ok_or_else(bad)?;
        s = s[eq + 1..].trim_start();
    }
    let inner = s.strip_prefix('{').and_then(|r| r.strip_suffix('}')).ok_or_else(bad)?;
    let mut items = Vec::new();
    let mut chars = inner.chars().peekable();
    if inner.trim().is_empty() {
        return Ok(items);
    }
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.peek() {
            Some('"') => {
                chars.next();
                let mut item = String::new();
                loop {
                    match chars.next().ok_or_else(bad)? {
                        '\\' => item.push(chars.next().ok_or_else(bad)?),
                        '"' => break,
                        c => item.push(c),
                    }
                }
                items.push(Some(item));
                while chars.peek().is_some_and(|c| c.is_whitespace()) {
                    chars.next();
                }
            }
            Some('{') => {
                return Err(Error::unsupported("multidimensional arrays"));
            }
            _ => {
                let mut item = String::new();
                while let Some(&c) = chars.peek() {
                    match c {
                        ',' => break,
                        '\\' => {
                            chars.next();
                            item.push(chars.next().ok_or_else(bad)?);
                        }
                        '"' | '{' | '}' => return Err(bad()),
                        _ => {
                            item.push(c);
                            chars.next();
                        }
                    }
                }
                let item = item.trim_end();
                if item.is_empty() {
                    return Err(bad());
                }
                items.push(if item.eq_ignore_ascii_case("null") { None } else { Some(item.to_string()) });
            }
        }
        match chars.next() {
            None => break,
            Some(',') => {}
            Some(_) => return Err(bad()),
        }
    }
    Ok(items)
}

/// Text input function of `ty`.
pub fn parse_value(text: &str, ty: &Type, ctx: &FnCtx) -> Result<Value> {
    Ok(match ty {
        Type::Bool => Value::Bool(parse_bool(text)?),
        Type::Int2 => Value::Int2(parse_i16(text)?),
        Type::Int4 => Value::Int4(parse_i32(text)?),
                Type::Int8 => Value::Int8(parse_i64(text)?),
        Type::Oid => match parse_i64(text) {
            Ok(v) if (0..=u32::MAX as i64).contains(&v) => Value::Int8(v),
            _ => return Err(Error::invalid_text("oid", text)),
        },
        Type::Char => Value::Text(text.chars().take(1).collect()),
        Type::Float4 => Value::Float4(parse_float(text, "real", true)? as f32),
        Type::Float8 => Value::Float8(parse_float(text, "double precision", false)?),
        Type::Numeric => Value::Numeric(numeric::parse_numeric(text)?),
        Type::Text | Type::Unknown => Value::Text(text.to_string()),
        Type::Bytea => Value::Bytea(parse_bytea(text)?),
        Type::Date => Value::Date(dt::parse_date(text, ctx)?),
        Type::Time => Value::Time(dt::parse_time(text, ctx)?),
        Type::Timestamp => Value::Timestamp(dt::parse_timestamp(text, ctx)?),
        Type::TimestampTz => Value::TimestampTz(dt::parse_timestamptz(text, ctx)?),
        Type::Interval => Value::Interval(dt::parse_interval(text)?),
        Type::Uuid => Value::Uuid(parse_uuid(text)?),
        Type::Json => {
            parse_json(text, "json")?;
            Value::Json(text.to_string())
        }
        Type::Jsonb => Value::Jsonb(Box::new(parse_json(text, "jsonb")?)),
        Type::Array(elem) => {
            let items = split_array_literal(text)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(match item {
                    None => Value::Null,
                    // Enum labels are resolved by the caller, who knows the catalog.
                    Some(s) if matches!(**elem, Type::Enum(_)) => Value::Text(s),
                    Some(s) => parse_value(&s, elem, ctx)?,
                });
            }
            Value::Array(out)
        }
        Type::Enum(_) | Type::Record | Type::Void => {
            return Err(Error::internal(format!("text input of type {} must be handled by the caller", ty.name())))
        }
    })
}

// ----------------------------------------------------------------- output

/// Splits Rust's scientific notation into digits and decimal exponent.
fn split_sci(s: &str) -> (String, i32) {
    let (mant, exp) = s.trim_start_matches('-').split_once('e').unwrap();
    (mant.chars().filter(char::is_ascii_digit).collect(), exp.parse().unwrap())
}

/// Rust and Postgres both print the shortest digit string that reads back
/// as the same float, but Postgres never picks a string lying exactly on
/// the boundary to the neighbouring float. That only happens for large
/// integers; in that case more digits are needed.
fn on_boundary(digits: &str, exp: i32, mantissa: u64, exp2: i32) -> bool {
    if exp2 <= 0 || exp2 > 64 {
        return false;
    }
    let value = (mantissa as u128) << exp2;
    let ulp = 1u128 << exp2;
    let shift = exp - (digits.len() as i32 - 1);
    if !(0..=38).contains(&shift) {
        return false;
    }
    let decimal = digits.parse::<u128>().ok().and_then(|d| d.checked_mul(10u128.pow(shift as u32)));
    decimal.is_some_and(|d| d != value && d.abs_diff(value) * 2 == ulp)
}

/// Shortest round-trip digits and exponent the way Postgres (Ryu) picks
/// them. `exact(n)` formats the value with `n` digits after the leading
/// one, correctly rounded; `reads_back` tests a candidate.
fn shortest_digits(
    shortest: String,
    exact: impl Fn(usize) -> String,
    reads_back: impl Fn(&str) -> bool,
    mantissa: u64,
    exp2: i32,
) -> (String, i32) {
    let (mut digits, mut exp) = split_sci(&shortest);
    while on_boundary(&digits, exp, mantissa, exp2) && digits.len() < 20 {
        (digits, exp) = split_sci(&exact(digits.len()));
    }
    // When the value lies exactly halfway between two candidates of this
    // length, Postgres rounds to the even digit, Rust away from zero.
    let n = digits.len();
    let (longer, longer_exp) = split_sci(&exact(n));
    let last_odd = (digits.as_bytes()[n - 1] - b'0') % 2 == 1;
    let halfway = last_odd && longer_exp == exp && longer.ends_with('5') && {
        let (tail, _) = split_sci(&exact(n + 40));
        tail[n + 1..].bytes().all(|b| b == b'0')
    };
    if halfway {
        let down = &longer[..n];
        let candidate = format!("{}.{}e{}", &down[..1], &down[1..], exp);
        if reads_back(&candidate) {
            digits = down.to_string();
        }
    }
    let trimmed = digits.trim_end_matches('0');
    (if trimmed.is_empty() { "0".to_string() } else { trimmed.to_string() }, exp)
}

/// Lays the digits out the way Postgres does: plain notation while the
/// exponent is inside `[min_exp, max_exp)`, scientific otherwise.
fn render_float(negative: bool, digits: &str, exp: i32, min_exp: i32, max_exp: i32) -> String {
    let mut out = String::with_capacity(digits.len() + 8);
    if negative {
        out.push('-');
    }
    if exp < min_exp || exp >= max_exp {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push_str(&format!("e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs()));
    } else if exp >= 0 {
        let int_len = exp as usize + 1;
        if digits.len() <= int_len {
            out.push_str(digits);
            out.push_str(&"0".repeat(int_len - digits.len()));
        } else {
            out.push_str(&digits[..int_len]);
            out.push('.');
            out.push_str(&digits[int_len..]);
        }
    } else {
        out.push_str("0.");
        out.push_str(&"0".repeat((-exp - 1) as usize));
        out.push_str(digits);
    }
    out
}

pub fn fmt_float8(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else if v == 0.0 {
        if v.is_sign_negative() { "-0" } else { "0" }.into()
    } else {
        let bits = v.to_bits();
        let mantissa = (bits & ((1 << 52) - 1)) | (1 << 52);
        let (digits, exp) = shortest_digits(
            format!("{v:e}"),
            |n| format!("{v:.n$e}"),
            |s| s.parse::<f64>().is_ok_and(|p| p == v.abs()),
            mantissa,
            ((bits >> 52) & 0x7ff) as i32 - 1075,
        );
        render_float(v < 0.0, &digits, exp, -4, 15)
    }
}

pub fn fmt_float4(v: f32) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else if v == 0.0 {
        if v.is_sign_negative() { "-0" } else { "0" }.into()
    } else {
        let bits = v.to_bits();
        let mantissa = ((bits & ((1 << 23) - 1)) | (1 << 23)) as u64;
        let (digits, exp) = shortest_digits(
            format!("{v:e}"),
            |n| format!("{v:.n$e}"),
            |s| s.parse::<f32>().is_ok_and(|p| p == v.abs()),
            mantissa,
            ((bits >> 23) & 0xff) as i32 - 150,
        );
        render_float(v < 0.0, &digits, exp, -4, 6)
    }
}

pub fn fmt_numeric(d: &Decimal) -> String {
    numeric::norm(*d).to_string()
}

pub fn fmt_uuid(u: &[u8; 16]) -> String {
    let h = encode_hex(u);
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

fn write_json_number(n: &serde_json::Number, out: &mut String) {
    if n.is_f64() {
        let f = n.as_f64().unwrap_or(0.0);
        out.push_str(&format!("{f}"));
    } else {
        out.push_str(&n.to_string());
    }
}

/// jsonb output: keys ordered by length then bytes, `": "` and `", "`
/// separators.
pub fn write_jsonb(v: &serde_json::Value, out: &mut String) {
    use serde_json::Value as J;
    match v {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Number(n) => write_json_number(n, out),
        J::String(s) => out.push_str(&serde_json::to_string(s).unwrap_or_default()),
        J::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_jsonb(item, out);
            }
            out.push(']');
        }
        J::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.as_bytes().cmp(b.as_bytes())));
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&serde_json::to_string(k).unwrap_or_default());
                out.push_str(": ");
                write_jsonb(&map[k], out);
            }
            out.push('}');
        }
    }
}

pub fn fmt_jsonb(v: &serde_json::Value) -> String {
    let mut s = String::new();
    write_jsonb(v, &mut s);
    s
}

fn push_array_elem(out: &mut String, s: &str) {
    let needs_quote = s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.chars().any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace());
    if !needs_quote {
        out.push_str(s);
        return;
    }
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

pub fn fmt_array(items: &[Value], ctx: &FnCtx) -> String {
    let mut out = String::from("{");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Array(inner) => out.push_str(&fmt_array(inner, ctx)),
            v => push_array_elem(&mut out, &format_value(v, ctx)),
        }
    }
    out.push('}');
    out
}

pub fn fmt_record(items: &[Value], ctx: &FnCtx) -> String {
    let mut out = String::from("(");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if item.is_null() {
            continue;
        }
        let s = format_value(item, ctx);
        let needs_quote =
            s.is_empty() || s.chars().any(|c| matches!(c, '(' | ')' | ',' | '"' | '\\') || c.is_whitespace());
        if needs_quote {
            out.push('"');
            for c in s.chars() {
                if c == '"' || c == '\\' {
                    out.push(c);
                }
                out.push(c);
            }
            out.push('"');
        } else {
            out.push_str(&s);
        }
    }
    out.push(')');
    out
}

/// Text output function.
pub fn format_value(v: &Value, ctx: &FnCtx) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "t" } else { "f" }.into(),
        Value::Int2(i) => i.to_string(),
        Value::Int4(i) => i.to_string(),
        Value::Int8(i) => i.to_string(),
        Value::Float4(f) => fmt_float4(*f),
        Value::Float8(f) => fmt_float8(*f),
        Value::Numeric(d) => fmt_numeric(d),
        Value::Text(s) | Value::Json(s) | Value::Enum(_, s) => s.clone(),
        Value::Bytea(b) => format!("\\x{}", encode_hex(b)),
        Value::Date(d) => dt::fmt_date(*d),
        Value::Time(t) => dt::fmt_time(*t),
        Value::Timestamp(t) => dt::fmt_timestamp(*t),
        Value::TimestampTz(t) => dt::fmt_timestamptz(*t, &dt::session_zone(ctx)),
        Value::Interval(iv) => dt::fmt_interval(iv),
        Value::Uuid(u) => fmt_uuid(u),
        Value::Jsonb(j) => fmt_jsonb(j),
        Value::Array(items) => fmt_array(items, ctx),
        Value::Record(items) => fmt_record(items, ctx),
    }
}
