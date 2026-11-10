//! String and binary-string functions.

use super::io;
use super::ops::bad_arg;
use super::pattern;
use super::registry::{Registry, Ret, P};
use crate::error::sqlstate;
use crate::{Error, FnCtx, Result, Type, Value};

pub(crate) fn txt(v: &Value) -> Result<&str> {
    match v {
        Value::Text(s) => Ok(s),
        _ => Err(bad_arg()),
    }
}
pub(crate) fn int(v: &Value) -> Result<i64> {
    v.as_i64().ok_or_else(bad_arg)
}
fn bytes(v: &Value) -> Result<&[u8]> {
    match v {
        Value::Bytea(b) => Ok(b),
        _ => Err(bad_arg()),
    }
}
fn text(s: impl Into<String>) -> Result<Value> {
    Ok(Value::Text(s.into()))
}
fn i32v(n: usize) -> Result<Value> {
    Ok(Value::Int4(n as i32))
}

fn length(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Text(s) => i32v(if s.is_ascii() { s.len() } else { s.chars().count() }),
        Value::Bytea(b) => i32v(b.len()),
        _ => Err(bad_arg()),
    }
}
fn octet_length(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Text(s) => i32v(s.len()),
        Value::Bytea(b) => i32v(b.len()),
        _ => Err(bad_arg()),
    }
}
fn bit_length(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Text(s) => i32v(s.len() * 8),
        Value::Bytea(b) => i32v(b.len() * 8),
        _ => Err(bad_arg()),
    }
}
fn lower(_: &FnCtx, a: &[Value]) -> Result<Value> {
    text(txt(&a[0])?.to_lowercase())
}
fn upper(_: &FnCtx, a: &[Value]) -> Result<Value> {
    text(txt(&a[0])?.to_uppercase())
}
fn initcap(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut out = String::new();
    let mut in_word = false;
    for c in txt(&a[0])?.chars() {
        if c.is_alphanumeric() {
            if in_word {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            in_word = true;
        } else {
            in_word = false;
            out.push(c);
        }
    }
    text(out)
}

fn trim_impl(a: &[Value], left: bool, right: bool) -> Result<Value> {
    let s = txt(&a[0])?;
    let set: Vec<char> = match a.get(1) {
        Some(v) => txt(v)?.chars().collect(),
        None => vec![' '],
    };
    let mut out = s;
    if left {
        out = out.trim_start_matches(|c| set.contains(&c));
    }
    if right {
        out = out.trim_end_matches(|c| set.contains(&c));
    }
    text(out)
}
fn btrim(_: &FnCtx, a: &[Value]) -> Result<Value> {
    trim_impl(a, true, true)
}
fn ltrim(_: &FnCtx, a: &[Value]) -> Result<Value> {
    trim_impl(a, true, false)
}
fn rtrim(_: &FnCtx, a: &[Value]) -> Result<Value> {
    trim_impl(a, false, true)
}

/// Characters `start..start+len` (1-based, as SQL counts) of `s`.
fn substr_chars(s: &str, start: i64, len: Option<i64>) -> Result<String> {
    if let Some(l) = len {
        if l < 0 {
            return Err(Error::new(sqlstate::SUBSTRING_ERROR, "negative substring length not allowed"));
        }
    }
    let end = len.map(|l| start.saturating_add(l));
    let from = start.max(1) as usize - 1;
    let iter = s.chars().skip(from);
    Ok(match end {
        Some(e) => iter.take((e.max(1) as usize - 1).saturating_sub(from)).collect(),
        None => iter.collect(),
    })
}
fn substring(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let len = match a.get(2) {
        Some(v) => Some(int(v)?),
        None => None,
    };
    text(substr_chars(txt(&a[0])?, int(&a[1])?, len)?)
}
fn substring_regex(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let re = pattern::regex(txt(&a[1])?, "")?;
    Ok(match re.captures(txt(&a[0])?) {
        Some(c) => {
            let m = if c.len() > 1 { c.get(1) } else { c.get(0) };
            m.map_or(Value::Null, |m| Value::Text(m.as_str().to_string()))
        }
        None => Value::Null,
    })
}
fn strpos(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, sub) = (txt(&a[0])?, txt(&a[1])?);
    i32v(s.find(sub).map_or(0, |i| s[..i].chars().count() + 1))
}
fn replace(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, from, to) = (txt(&a[0])?, txt(&a[1])?, txt(&a[2])?);
    text(if from.is_empty() { s.to_string() } else { s.replace(from, to) })
}
fn translate(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let from: Vec<char> = txt(&a[1])?.chars().collect();
    let to: Vec<char> = txt(&a[2])?.chars().collect();
    let mut out = String::new();
    for c in txt(&a[0])?.chars() {
        match from.iter().position(|f| *f == c) {
            Some(i) => {
                if let Some(r) = to.get(i) {
                    out.push(*r);
                }
            }
            None => out.push(c),
        }
    }
    text(out)
}
fn concat(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut out = String::new();
    for v in a.iter().filter(|v| !v.is_null()) {
        match v {
            Value::Text(s) => out.push_str(s),
            other => out.push_str(&io::format_value(other, c)),
        }
    }
    text(out)
}
fn concat_ws(c: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    let sep = txt(&a[0])?;
    let parts: Vec<String> = a[1..].iter().filter(|v| !v.is_null()).map(|v| io::format_value(v, c)).collect();
    text(parts.join(sep))
}
fn left(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, n) = (txt(&a[0])?, int(&a[1])?);
    let len = s.chars().count() as i64;
    let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
    text(s.chars().take(take as usize).collect::<String>())
}
fn right(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, n) = (txt(&a[0])?, int(&a[1])?);
    let len = s.chars().count() as i64;
    let skip = if n >= 0 { (len - n).max(0) } else { (-n).min(len) };
    text(s.chars().skip(skip as usize).collect::<String>())
}
fn pad(a: &[Value], left: bool) -> Result<Value> {
    let (s, n) = (txt(&a[0])?, int(&a[1])?.max(0) as usize);
    let fill: Vec<char> = match a.get(2) {
        Some(v) => txt(v)?.chars().collect(),
        None => vec![' '],
    };
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= n || fill.is_empty() {
        return text(chars.into_iter().take(n).collect::<String>());
    }
    let padding: String = fill.iter().cycle().take(n - chars.len()).collect();
    text(if left { format!("{padding}{s}") } else { format!("{s}{padding}") })
}
fn lpad(_: &FnCtx, a: &[Value]) -> Result<Value> {
    pad(a, true)
}
fn rpad(_: &FnCtx, a: &[Value]) -> Result<Value> {
    pad(a, false)
}
fn repeat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, n) = (txt(&a[0])?, int(&a[1])?.max(0) as usize);
    if s.len().saturating_mul(n) > 1 << 30 {
        return Err(Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "requested length too large"));
    }
    text(s.repeat(n))
}
fn reverse(_: &FnCtx, a: &[Value]) -> Result<Value> {
    text(txt(&a[0])?.chars().rev().collect::<String>())
}
fn split_part(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, sep, n) = (txt(&a[0])?, txt(&a[1])?, int(&a[2])?);
    if n == 0 {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "field position must not be zero"));
    }
    if sep.is_empty() {
        return text(if n == 1 || n == -1 { s } else { "" });
    }
    let parts: Vec<&str> = s.split(sep).collect();
    let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
    text(if idx >= 0 { parts.get(idx as usize).copied().unwrap_or("") } else { "" })
}
fn starts_with(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Bool(txt(&a[0])?.starts_with(txt(&a[1])?)))
}
fn ascii(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Int4(txt(&a[0])?.chars().next().map_or(0, |c| c as i32)))
}
fn chr(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let n = int(&a[0])?;
    if n == 0 {
        return Err(Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "null character not permitted"));
    }
    u32::try_from(n)
        .ok()
        .and_then(char::from_u32)
        .map(|c| Value::Text(c.to_string()))
        .ok_or_else(|| Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "requested character too large for encoding"))
}

/// MD5 digest (RFC 1321).
pub fn md5_digest(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14,
        20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6,
        10, 15, 21,
    ];
    let k: Vec<u32> = (0..64).map(|i| ((i as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32).collect();
    let mut msg = input.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((input.len() as u64).wrapping_mul(8)).to_le_bytes());
    let (mut a0, mut b0, mut c0, mut d0) = (0x67452301u32, 0xefcdab89u32, 0x98badcfeu32, 0x10325476u32);
    for chunk in msg.chunks(64) {
        let m: Vec<u32> = chunk.chunks(4).map(|w| u32::from_le_bytes([w[0], w[1], w[2], w[3]])).collect();
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    for (i, v) in [a0, b0, c0, d0].iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_le_bytes());
    }
    out
}
fn md5(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let data = match &a[0] {
        Value::Text(s) => s.as_bytes(),
        Value::Bytea(b) => b.as_slice(),
        _ => return Err(bad_arg()),
    };
    text(io::encode_hex(&md5_digest(data)))
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::new();
    let mut line = 0;
    for chunk in data.chunks(3) {
        let n =
            (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
        line += 4;
        // Postgres breaks base64 output into 76-character lines.
        if line >= 76 {
            out.push('\n');
            line = 0;
        }
    }
    if out.ends_with('\n') {
        out.pop();
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    let bad = |m: &str| Error::new(sqlstate::INVALID_PARAMETER_VALUE, m.to_string());
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut n = 0;
    let mut pad = 0;
    for c in s.bytes() {
        if c.is_ascii_whitespace() {
            continue;
        }
        let v = if c == b'=' {
            pad += 1;
            0
        } else {
            if pad > 0 {
                return Err(bad("unexpected \"=\" while decoding base64 sequence"));
            }
            B64.iter()
                .position(|b| *b == c)
                .ok_or_else(|| bad(&format!("invalid symbol \"{}\" found while decoding base64 sequence", c as char)))?
                as u32
        };
        buf = buf << 6 | v;
        n += 1;
        if n == 4 {
            out.extend_from_slice(&[(buf >> 16) as u8, (buf >> 8) as u8, buf as u8]);
            buf = 0;
            n = 0;
        }
    }
    if n != 0 || pad > 2 {
        return Err(bad("invalid base64 end sequence"));
    }
    out.truncate(out.len() - pad);
    Ok(out)
}

fn unknown_encoding(name: &str) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("unrecognized encoding: \"{name}\""))
}
fn encode(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let data = bytes(&a[0])?;
    let fmt = txt(&a[1])?;
    text(match fmt.to_ascii_lowercase().as_str() {
        "hex" => io::encode_hex(data),
        "base64" => base64_encode(data),
        "escape" => {
            let mut out = String::new();
            for &b in data {
                match b {
                    b'\\' => out.push_str("\\\\"),
                    0 | 128..=255 => out.push_str(&format!("\\{b:03o}")),
                    _ => out.push(b as char),
                }
            }
            out
        }
        _ => return Err(unknown_encoding(fmt)),
    })
}
fn decode(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, fmt) = (txt(&a[0])?, txt(&a[1])?);
    Ok(Value::Bytea(match fmt.to_ascii_lowercase().as_str() {
        "hex" => io::decode_hex(s)?,
        "base64" => base64_decode(s)?,
        "escape" => io::parse_bytea(s)?,
        _ => return Err(unknown_encoding(fmt)),
    }))
}

const RESERVED: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
];

pub fn quote_ident_str(s: &str) -> String {
    let simple = !s.is_empty()
        && s.bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_lowercase() || b == b'_' || (i > 0 && (b.is_ascii_digit() || b == b'$')))
        && !RESERVED.contains(&s);
    if simple {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}
pub fn quote_literal_str(s: &str) -> String {
    let body = s.replace('\'', "''");
    if s.contains('\\') {
        format!("E'{}'", body.replace('\\', "\\\\"))
    } else {
        format!("'{body}'")
    }
}
fn quote_ident(_: &FnCtx, a: &[Value]) -> Result<Value> {
    text(quote_ident_str(txt(&a[0])?))
}
fn quote_literal(c: &FnCtx, a: &[Value]) -> Result<Value> {
    text(quote_literal_str(&super::cast::to_text(&a[0], c)))
}
fn quote_nullable(c: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return text("NULL");
    }
    quote_literal(c, a)
}
fn to_hex(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &a[0] {
        Value::Int4(i) => text(format!("{:x}", *i as u32)),
        Value::Int8(i) => text(format!("{:x}", *i as u64)),
        _ => Err(bad_arg()),
    }
}

fn format(c: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    let fmt = txt(&a[0])?;
    let args = &a[1..];
    let bad = |m: &str| Error::new(sqlstate::INVALID_PARAMETER_VALUE, m.to_string());
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let mut next_arg = 0usize;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let read_num = |chars: &mut std::iter::Peekable<std::str::Chars<'_>>| -> Option<usize> {
            let mut s = String::new();
            while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                s.push(*d);
                chars.next();
            }
            s.parse().ok()
        };
        let mut position = None;
        let mut width: Option<i64> = None;
        let mut left_align = false;
        let first = read_num(&mut chars);
        if let Some(n) = first {
            if chars.peek() == Some(&'$') {
                chars.next();
                if n == 0 {
                    return Err(bad("format specifies argument 0, but arguments are numbered from 1"));
                }
                position = Some(n);
            } else {
                width = Some(n as i64);
            }
        }
        if width.is_none() {
            if chars.peek() == Some(&'-') {
                chars.next();
                left_align = true;
            }
            if chars.peek() == Some(&'*') {
                chars.next();
                let arg = args.get(next_arg).ok_or_else(|| bad("too few arguments for format()"))?;
                next_arg += 1;
                let w = if arg.is_null() {
                    0
                } else {
                    arg.as_i64().ok_or_else(|| bad("width argument must be an integer"))?
                };
                left_align |= w < 0;
                width = Some(w.abs());
            } else if let Some(n) = read_num(&mut chars) {
                width = Some(n as i64);
            }
        }
        let kind = chars.next().ok_or_else(|| bad("unterminated format() type specifier"))?;
        let idx = match position {
            Some(p) => {
                next_arg = p;
                p - 1
            }
            None => {
                next_arg += 1;
                next_arg - 1
            }
        };
        let arg = args.get(idx).ok_or_else(|| bad("too few arguments for format()"))?;
        let piece = match kind {
            's' => {
                if arg.is_null() {
                    String::new()
                } else {
                    io::format_value(arg, c)
                }
            }
            'I' => {
                if arg.is_null() {
                    return Err(Error::new(
                        sqlstate::NULL_VALUE_NOT_ALLOWED,
                        "null values cannot be formatted as an SQL identifier",
                    ));
                }
                quote_ident_str(&io::format_value(arg, c))
            }
            'L' => {
                if arg.is_null() {
                    "NULL".to_string()
                } else {
                    quote_literal_str(&io::format_value(arg, c))
                }
            }
            other => {
                return Err(bad(&format!("unrecognized format() type specifier \"{other}\""))
                    .with_hint("For a single \"%\" use \"%%\"."))
            }
        };
        let w = width.unwrap_or(0) as usize;
        let len = piece.chars().count();
        if len >= w {
            out.push_str(&piece);
        } else if left_align {
            out.push_str(&piece);
            out.push_str(&" ".repeat(w - len));
        } else {
            out.push_str(&" ".repeat(w - len));
            out.push_str(&piece);
        }
    }
    text(out)
}

/// Converts a Postgres replacement string (`\1`, `\&`) to the regex
/// crate's syntax.
fn replacement(rep: &str) -> String {
    let mut out = String::new();
    let mut it = rep.chars();
    while let Some(c) = it.next() {
        match c {
            '$' => out.push_str("$$"),
            '\\' => match it.next() {
                Some(d) if d.is_ascii_digit() => out.push_str(&format!("${{{d}}}")),
                Some('&') => out.push_str("${0}"),
                Some(other) => out.push(other),
                None => out.push('\\'),
            },
            _ => out.push(c),
        }
    }
    out
}
fn regexp_replace(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let flags = match a.get(3) {
        Some(v) => txt(v)?,
        None => "",
    };
    let re = pattern::regex(txt(&a[1])?, flags)?;
    let rep = replacement(txt(&a[2])?);
    let s = txt(&a[0])?;
    text(
        if flags.contains('g') { re.replace_all(s, rep.as_str()) } else { re.replacen(s, 1, rep.as_str()) }
            .into_owned(),
    )
}
fn regexp_match(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let flags = match a.get(2) {
        Some(v) => txt(v)?,
        None => "",
    };
    let re = pattern::regex(txt(&a[1])?, flags)?;
    Ok(match re.captures(txt(&a[0])?) {
        None => Value::Null,
        Some(c) => {
            let group = |i: usize| c.get(i).map_or(Value::Null, |m| Value::Text(m.as_str().to_string()));
            Value::Array(if c.len() > 1 { (1..c.len()).map(group).collect() } else { vec![group(0)] })
        }
    })
}
fn regexp_like(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let flags = match a.get(2) {
        Some(v) => txt(v)?,
        None => "",
    };
    Ok(Value::Bool(pattern::regex(txt(&a[1])?, flags)?.is_match(txt(&a[0])?)))
}
fn regexp_count(_: &FnCtx, a: &[Value]) -> Result<Value> {
    i32v(pattern::regex(txt(&a[1])?, "")?.find_iter(txt(&a[0])?).count())
}
fn regexp_split_to_array(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let flags = match a.get(2) {
        Some(v) => txt(v)?,
        None => "",
    };
    let re = pattern::regex(txt(&a[1])?, flags)?;
    Ok(Value::Array(re.split(txt(&a[0])?).map(|p| Value::Text(p.to_string())).collect()))
}

fn overlay(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (s, r, start) = (txt(&a[0])?, txt(&a[1])?, int(&a[2])?);
    let len = match a.get(3) {
        Some(v) => int(v)?,
        None => r.chars().count() as i64,
    };
    if start <= 0 {
        return Err(Error::new(sqlstate::SUBSTRING_ERROR, "negative substring length not allowed"));
    }
    let head = substr_chars(s, 1, Some(start - 1))?;
    let tail = substr_chars(s, start + len, None)?;
    text(format!("{head}{r}{tail}"))
}
fn like_escape(_: &FnCtx, a: &[Value]) -> Result<Value> {
    text(pattern::like_escape(txt(&a[0])?, txt(&a[1])?)?)
}
fn similar_to_escape(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let esc = match a.get(1) {
        Some(v) => Some(txt(v)?),
        None => None,
    };
    text(pattern::similar_to_escape(txt(&a[0])?, esc)?)
}
fn string_to_array(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    let s = txt(&a[0])?;
    let null_str = match a.get(2) {
        Some(Value::Text(n)) => Some(n.as_str()),
        _ => None,
    };
    let mk = |p: &str| if Some(p) == null_str { Value::Null } else { Value::Text(p.to_string()) };
    Ok(Value::Array(match &a[1] {
        Value::Null => s.chars().map(|c| mk(&c.to_string())).collect(),
        Value::Text(sep) if sep.is_empty() => vec![mk(s)],
        Value::Text(_) if s.is_empty() => Vec::new(),
        Value::Text(sep) => s.split(sep.as_str()).map(mk).collect(),
        _ => return Err(bad_arg()),
    }))
}

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    let tx = || Ret::T(Text);
    let i = || Ret::T(Int4);

    for name in ["length", "char_length", "character_length"] {
        r.f(name, &[t(Text)], i(), length);
    }
    r.f("length", &[t(Bytea)], i(), length);
    r.f("octet_length", &[t(Text)], i(), octet_length);
    r.f("octet_length", &[t(Bytea)], i(), octet_length);
    r.f("bit_length", &[t(Text)], i(), bit_length);
    r.f("bit_length", &[t(Bytea)], i(), bit_length);
    r.f("lower", &[t(Text)], tx(), lower);
    r.f("upper", &[t(Text)], tx(), upper);
    r.f("initcap", &[t(Text)], tx(), initcap);
    for (name, f) in [("btrim", btrim as super::registry::FuncImpl), ("ltrim", ltrim), ("rtrim", rtrim)] {
        r.f(name, &[t(Text)], tx(), f);
        r.f(name, &[t(Text), t(Text)], tx(), f);
    }
    for name in ["substring", "substr"] {
        r.f(name, &[t(Text), t(Int4)], tx(), substring);
        r.f(name, &[t(Text), t(Int4), t(Int4)], tx(), substring);
    }
    r.f("substring", &[t(Text), t(Text)], tx(), substring_regex);
    r.f("position", &[t(Text), t(Text)], i(), strpos);
    r.f("strpos", &[t(Text), t(Text)], i(), strpos);
    r.f("replace", &[t(Text), t(Text), t(Text)], tx(), replace);
    r.f("translate", &[t(Text), t(Text), t(Text)], tx(), translate);
    r.f("concat", &[P::Any], tx(), concat).variadic().nonstrict().stable();
    r.f("concat_ws", &[t(Text), P::Any], tx(), concat_ws).variadic().nonstrict().stable();
    r.f("left", &[t(Text), t(Int4)], tx(), left);
    r.f("right", &[t(Text), t(Int4)], tx(), right);
    r.f("lpad", &[t(Text), t(Int4)], tx(), lpad);
    r.f("lpad", &[t(Text), t(Int4), t(Text)], tx(), lpad);
    r.f("rpad", &[t(Text), t(Int4)], tx(), rpad);
    r.f("rpad", &[t(Text), t(Int4), t(Text)], tx(), rpad);
    r.f("repeat", &[t(Text), t(Int4)], tx(), repeat);
    r.f("reverse", &[t(Text)], tx(), reverse);
    r.f("split_part", &[t(Text), t(Text), t(Int4)], tx(), split_part);
    r.f("starts_with", &[t(Text), t(Text)], Ret::T(Bool), starts_with);
    r.f("ascii", &[t(Text)], i(), ascii);
    r.f("chr", &[t(Int4)], tx(), chr);
    r.f("md5", &[t(Text)], tx(), md5);
    r.f("md5", &[t(Bytea)], tx(), md5);
    r.f("encode", &[t(Bytea), t(Text)], tx(), encode);
    r.f("decode", &[t(Text), t(Text)], Ret::T(Bytea), decode);
    r.f("format", &[t(Text)], tx(), format).nonstrict().stable();
    r.f("format", &[t(Text), P::Any], tx(), format).variadic().nonstrict().stable();
    r.f("quote_ident", &[t(Text)], tx(), quote_ident);
    r.f("quote_literal", &[t(Text)], tx(), quote_literal);
    r.f("quote_literal", &[P::Elem], tx(), quote_literal).stable();
    r.f("quote_nullable", &[t(Text)], tx(), quote_nullable).nonstrict();
    r.f("quote_nullable", &[P::Elem], tx(), quote_nullable).nonstrict().stable();
    r.f("to_hex", &[t(Int4)], tx(), to_hex);
    r.f("to_hex", &[t(Int8)], tx(), to_hex);
    r.f("regexp_replace", &[t(Text), t(Text), t(Text)], tx(), regexp_replace);
    r.f("regexp_replace", &[t(Text), t(Text), t(Text), t(Text)], tx(), regexp_replace);
    r.f("regexp_match", &[t(Text), t(Text)], Ret::T(Text.array_of()), regexp_match);
    r.f("regexp_match", &[t(Text), t(Text), t(Text)], Ret::T(Text.array_of()), regexp_match);
    r.f("regexp_like", &[t(Text), t(Text)], Ret::T(Bool), regexp_like);
    r.f("regexp_like", &[t(Text), t(Text), t(Text)], Ret::T(Bool), regexp_like);
    r.f("regexp_count", &[t(Text), t(Text)], i(), regexp_count);
    r.f("regexp_split_to_array", &[t(Text), t(Text)], Ret::T(Text.array_of()), regexp_split_to_array);
    r.f("regexp_split_to_array", &[t(Text), t(Text), t(Text)], Ret::T(Text.array_of()), regexp_split_to_array);
    r.f("overlay", &[t(Text), t(Text), t(Int4)], tx(), overlay);
    r.f("overlay", &[t(Text), t(Text), t(Int4), t(Int4)], tx(), overlay);
    r.f("like_escape", &[t(Text), t(Text)], tx(), like_escape);
    r.f("similar_to_escape", &[t(Text)], tx(), similar_to_escape);
    r.f("similar_to_escape", &[t(Text), t(Text)], tx(), similar_to_escape);
    r.f("string_to_array", &[t(Text), t(Text)], Ret::T(Text.array_of()), string_to_array).nonstrict();
    r.f("string_to_array", &[t(Text), t(Text), t(Text)], Ret::T(Text.array_of()), string_to_array).nonstrict();
}
