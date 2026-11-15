//! JSON and JSONB functions and operators.

use std::cmp::Ordering;

use serde_json::Value as J;

use super::datetime as dt;
use super::f_string::txt;
use super::io;
use super::ops::bad_arg;
use super::registry::{Registry, Ret, P};
use crate::error::sqlstate;
use crate::types::cmp_json;
use crate::{Error, FnCtx, Result, Type, Value};

fn invalid(msg: impl Into<String>) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, msg)
}

fn jb(v: &Value) -> Result<&J> {
    match v {
        Value::Jsonb(j) => Ok(j),
        _ => Err(bad_arg()),
    }
}
/// Parses either flavor into a tree.
fn tree(v: &Value) -> Result<std::borrow::Cow<'_, J>> {
    match v {
        Value::Jsonb(j) => Ok(std::borrow::Cow::Borrowed(j)),
        Value::Json(s) => Ok(std::borrow::Cow::Owned(io::parse_json(s, "json")?)),
        _ => Err(bad_arg()),
    }
}
fn jsonb(j: J) -> Value {
    Value::Jsonb(Box::new(j))
}
/// Wraps a sub-tree in the same flavor as `like`.
fn same_flavor(like: &Value, j: J) -> Value {
    match like {
        Value::Json(_) => Value::Json(serde_json::to_string(&j).unwrap_or_default()),
        _ => jsonb(j),
    }
}
fn text_of(like: &Value, j: &J) -> Value {
    match j {
        J::Null => Value::Null,
        J::String(s) => Value::Text(s.clone()),
        other => Value::Text(match like {
            Value::Json(_) => serde_json::to_string(other).unwrap_or_default(),
            _ => io::fmt_jsonb(other),
        }),
    }
}

fn iso_timestamp(ts: i64, offset: Option<i32>) -> String {
    let s = match offset {
        Some(off) => {
            let local = dt::fmt_timestamp(ts.saturating_add(off as i64 * dt::USECS_PER_SEC));
            let a = off.abs();
            format!("{local}{}{:02}:{:02}", if off < 0 { '-' } else { '+' }, a / 3600, a / 60 % 60)
        }
        None => dt::fmt_timestamp(ts),
    };
    s.replacen(' ', "T", 1)
}

/// Converts any SQL value to a JSON tree (`to_jsonb`).
pub fn to_tree(v: &Value, c: &FnCtx) -> Result<J> {
    Ok(match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int2(i) => J::from(*i),
        Value::Int4(i) => J::from(*i),
        Value::Int8(i) => J::from(*i),
        Value::Float4(f) if f.is_finite() => serde_json::from_str(&io::fmt_float4(*f)).unwrap_or(J::Null),
        Value::Float8(f) if f.is_finite() => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        Value::Float4(_) | Value::Float8(_) => J::String(io::format_value(v, c)),
        Value::Numeric(d) => serde_json::from_str(&io::fmt_numeric(d)).unwrap_or(J::Null),
        Value::Json(s) => io::parse_json(s, "json")?,
        Value::Jsonb(j) => (**j).clone(),
        Value::Timestamp(t) if dt::is_finite_ts(*t) => J::String(iso_timestamp(*t, None)),
        Value::TimestampTz(t) if dt::is_finite_ts(*t) => {
            J::String(iso_timestamp(*t, Some(dt::session_zone(c).offset_at_utc(*t))))
        }
        Value::Array(items) | Value::Vector(items) => {
            J::Array(items.iter().map(|i| to_tree(i, c)).collect::<Result<_>>()?)
        }
        Value::Record(items) => {
            let mut m = serde_json::Map::new();
            for (i, item) in items.iter().enumerate() {
                m.insert(format!("f{}", i + 1), to_tree(item, c)?);
            }
            J::Object(m)
        }
        other => J::String(io::format_value(other, c)),
    })
}

/// Writes any SQL value as compact JSON text (`to_json`), keeping field
/// order and numeric formatting.
fn write_json(v: &Value, c: &FnCtx, out: &mut String) -> Result<()> {
    match v {
        Value::Numeric(d) => out.push_str(&io::fmt_numeric(d)),
        Value::Float4(f) if f.is_finite() => out.push_str(&io::fmt_float4(*f)),
        Value::Float8(f) if f.is_finite() => out.push_str(&io::fmt_float8(*f)),
        Value::Json(s) => out.push_str(s),
        Value::Array(items) | Value::Vector(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, c, out)?;
            }
            out.push(']');
        }
        Value::Record(items) => {
            out.push('{');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&format!("\"f{}\":", i + 1));
                write_json(item, c, out)?;
            }
            out.push('}');
        }
        Value::Jsonb(j) => out.push_str(&io::fmt_jsonb(j)),
        other => out.push_str(&serde_json::to_string(&to_tree(other, c)?).unwrap_or_default()),
    }
    Ok(())
}

fn to_json(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut s = String::new();
    write_json(&a[0], c, &mut s)?;
    Ok(Value::Json(s))
}
fn to_jsonb(c: &FnCtx, a: &[Value]) -> Result<Value> {
    to_tree(&a[0], c).map(jsonb)
}

fn key_text(v: &Value, c: &FnCtx, pos: usize) -> Result<String> {
    if v.is_null() {
        return Err(Error::new(sqlstate::NULL_VALUE_NOT_ALLOWED, format!("argument {pos}: key must not be null")));
    }
    Ok(super::cast::to_text(v, c))
}
fn check_pairs(a: &[Value]) -> Result<()> {
    if !a.len().is_multiple_of(2) {
        return Err(invalid("argument list must have even number of elements")
            .with_hint("The arguments of json_build_object() must consist of alternating keys and values."));
    }
    Ok(())
}
fn json_build_object(c: &FnCtx, a: &[Value]) -> Result<Value> {
    check_pairs(a)?;
    let mut s = String::from("{");
    for (i, pair) in a.chunks(2).enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&serde_json::to_string(&key_text(&pair[0], c, i * 2 + 1)?).unwrap_or_default());
        s.push_str(" : ");
        write_json(&pair[1], c, &mut s)?;
    }
    s.push('}');
    Ok(Value::Json(s))
}
fn jsonb_build_object(c: &FnCtx, a: &[Value]) -> Result<Value> {
    check_pairs(a)?;
    let mut m = serde_json::Map::new();
    for (i, pair) in a.chunks(2).enumerate() {
        m.insert(key_text(&pair[0], c, i * 2 + 1)?, to_tree(&pair[1], c)?);
    }
    Ok(jsonb(J::Object(m)))
}
fn json_build_array(c: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut s = String::from("[");
    for (i, v) in a.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        write_json(v, c, &mut s)?;
    }
    s.push(']');
    Ok(Value::Json(s))
}
fn jsonb_build_array(c: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(jsonb(J::Array(a.iter().map(|v| to_tree(v, c)).collect::<Result<_>>()?)))
}
fn typeof_(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Text(
        match &*tree(&a[0])? {
            J::Null => "null",
            J::Bool(_) => "boolean",
            J::Number(_) => "number",
            J::String(_) => "string",
            J::Array(_) => "array",
            J::Object(_) => "object",
        }
        .into(),
    ))
}
fn array_length(_: &FnCtx, a: &[Value]) -> Result<Value> {
    match &*tree(&a[0])? {
        J::Array(items) => Ok(Value::Int4(items.len() as i32)),
        J::Object(_) => Err(invalid("cannot get array length of a non-array")),
        _ => Err(invalid("cannot get array length of a scalar")),
    }
}

fn step<'a>(j: &'a J, key: &str) -> Option<&'a J> {
    match j {
        J::Object(m) => m.get(key),
        J::Array(items) => {
            let i: i64 = key.trim().parse().ok()?;
            let idx = if i < 0 { items.len() as i64 + i } else { i };
            items.get(usize::try_from(idx).ok()?)
        }
        _ => None,
    }
}
fn walk<'a>(j: &'a J, path: &[Value]) -> Option<&'a J> {
    let mut cur = j;
    for p in path {
        cur = step(cur, p.as_str()?)?;
    }
    Some(cur)
}
fn path_of(v: &Value) -> Result<&[Value]> {
    match v {
        Value::Array(items) => Ok(items),
        _ => Err(bad_arg()),
    }
}
fn extract_path(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let t = tree(&a[0])?;
    Ok(walk(&t, &a[1..]).map_or(Value::Null, |j| same_flavor(&a[0], j.clone())))
}
fn extract_path_text(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let t = tree(&a[0])?;
    Ok(walk(&t, &a[1..]).map_or(Value::Null, |j| text_of(&a[0], j)))
}
fn op_path(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let t = tree(&a[0])?;
    Ok(walk(&t, path_of(&a[1])?).map_or(Value::Null, |j| same_flavor(&a[0], j.clone())))
}
fn op_path_text(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let t = tree(&a[0])?;
    Ok(walk(&t, path_of(&a[1])?).map_or(Value::Null, |j| text_of(&a[0], j)))
}
fn field<'a>(j: &'a J, key: &Value) -> Option<&'a J> {
    match (j, key) {
        (J::Object(m), Value::Text(k)) => m.get(k.as_str()),
        (J::Array(items), Value::Int4(i)) => {
            let idx = if *i < 0 { items.len() as i64 + *i as i64 } else { *i as i64 };
            items.get(usize::try_from(idx).ok()?)
        }
        _ => None,
    }
}
fn op_get(_: &FnCtx, a: &[Value]) -> Result<Value> {
    // jsonb is the hot path; avoid cloning the whole document.
    if let Value::Jsonb(j) = &a[0] {
        // Postgres stores a top-level jsonb scalar as a one-element array,
        // so element 0 (or -1) of a scalar is the scalar itself.
        if let (false, Value::Int4(0 | -1)) = (j.is_array() || j.is_object(), &a[1]) {
            return Ok(a[0].clone());
        }
        return Ok(field(j, &a[1]).map_or(Value::Null, |v| jsonb(v.clone())));
    }
    let t = tree(&a[0])?;
    Ok(field(&t, &a[1]).map_or(Value::Null, |j| same_flavor(&a[0], j.clone())))
}
fn op_get_text(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if let Value::Jsonb(j) = &a[0] {
        return Ok(field(j, &a[1]).map_or(Value::Null, |v| text_of(&a[0], v)));
    }
    let t = tree(&a[0])?;
    Ok(field(&t, &a[1]).map_or(Value::Null, |j| text_of(&a[0], j)))
}

fn json_eq(a: &J, b: &J) -> bool {
    cmp_json(a, b) == Ordering::Equal
}
fn contains(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Object(x), J::Object(y)) => y.iter().all(|(k, v)| x.get(k).is_some_and(|xv| contains(xv, v))),
        (J::Array(x), J::Array(y)) => y.iter().all(|v| x.iter().any(|xv| contains(xv, v))),
        (J::Array(_), J::Object(_)) | (J::Object(_), _) => false,
        _ => json_eq(a, b),
    }
}
fn top_contains(a: &J, b: &J) -> bool {
    match (a, b) {
        // An array contains a bare scalar only at the top level.
        (J::Array(x), y) if !y.is_array() && !y.is_object() => x.iter().any(|v| json_eq(v, y)),
        _ => contains(a, b),
    }
}
fn op_contains(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Bool(top_contains(jb(&a[0])?, jb(&a[1])?)))
}
fn op_contained(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Bool(top_contains(jb(&a[1])?, jb(&a[0])?)))
}
fn exists(j: &J, key: &str) -> bool {
    match j {
        J::Object(m) => m.contains_key(key),
        J::Array(items) => items.iter().any(|v| v.as_str() == Some(key)),
        J::String(s) => s == key,
        _ => false,
    }
}
fn op_exists(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Bool(exists(jb(&a[0])?, txt(&a[1])?)))
}
fn op_exists_any(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let j = jb(&a[0])?;
    Ok(Value::Bool(path_of(&a[1])?.iter().any(|k| k.as_str().is_some_and(|k| exists(j, k)))))
}
fn op_exists_all(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let j = jb(&a[0])?;
    Ok(Value::Bool(path_of(&a[1])?.iter().all(|k| k.as_str().is_some_and(|k| exists(j, k)))))
}
fn op_concat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let (x, y) = (jb(&a[0])?, jb(&a[1])?);
    Ok(jsonb(match (x, y) {
        (J::Object(m), J::Object(n)) => {
            let mut out = m.clone();
            for (k, v) in n {
                out.insert(k.clone(), v.clone());
            }
            J::Object(out)
        }
        (J::Array(m), J::Array(n)) => J::Array(m.iter().chain(n).cloned().collect()),
        (J::Array(m), other) => {
            let mut out = m.clone();
            out.push(other.clone());
            J::Array(out)
        }
        (other, J::Array(n)) => {
            let mut out = vec![other.clone()];
            out.extend(n.iter().cloned());
            J::Array(out)
        }
        (x, y) => J::Array(vec![x.clone(), y.clone()]),
    }))
}
fn delete_keys(j: &J, keys: &[&str]) -> Result<J> {
    Ok(match j {
        J::Object(m) => J::Object(
            m.iter().filter(|(k, _)| !keys.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect(),
        ),
        J::Array(items) => {
            J::Array(items.iter().filter(|v| !v.as_str().is_some_and(|s| keys.contains(&s))).cloned().collect())
        }
        _ => return Err(invalid("cannot delete from scalar")),
    })
}
fn op_delete(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let j = jb(&a[0])?;
    Ok(jsonb(match &a[1] {
        Value::Text(k) => delete_keys(j, &[k.as_str()])?,
        Value::Array(keys) => delete_keys(j, &keys.iter().filter_map(Value::as_str).collect::<Vec<_>>())?,
        Value::Int4(i) => match j {
            J::Array(items) => {
                let idx = if *i < 0 { items.len() as i64 + *i as i64 } else { *i as i64 };
                let mut out = items.clone();
                if idx >= 0 && (idx as usize) < out.len() {
                    out.remove(idx as usize);
                }
                J::Array(out)
            }
            J::Object(_) => return Err(invalid("cannot delete from object using integer index")),
            _ => return Err(invalid("cannot delete from scalar")),
        },
        _ => return Err(bad_arg()),
    }))
}

fn array_index(len: usize, key: &str) -> Result<i64> {
    let i: i64 = key.trim().parse().map_err(|_| {
        Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, format!("path element is not an integer: \"{key}\""))
    })?;
    Ok(if i < 0 { len as i64 + i } else { i })
}

enum Edit<'a> {
    Set { value: &'a J, create: bool },
    Insert { value: &'a J, after: bool },
    Delete,
}

fn edit(j: &mut J, path: &[&str], op: &Edit<'_>) -> Result<()> {
    let Some((key, rest)) = path.split_first() else { return Ok(()) };
    match j {
        J::Object(m) => {
            if !rest.is_empty() {
                if let Some(child) = m.get_mut(*key) {
                    edit(child, rest, op)?;
                }
                return Ok(());
            }
            match op {
                Edit::Set { value, create } => {
                    if *create || m.contains_key(*key) {
                        m.insert((*key).to_string(), (*value).clone());
                    }
                }
                Edit::Insert { value, .. } => {
                    if m.contains_key(*key) {
                        return Err(invalid("cannot replace existing key")
                            .with_hint("Try using the function jsonb_set to replace key value."));
                    }
                    m.insert((*key).to_string(), (*value).clone());
                }
                Edit::Delete => {
                    m.remove(*key);
                }
            }
        }
        J::Array(items) => {
            let idx = array_index(items.len(), key)?;
            if !rest.is_empty() {
                if idx >= 0 && (idx as usize) < items.len() {
                    edit(&mut items[idx as usize], rest, op)?;
                }
                return Ok(());
            }
            let in_range = idx >= 0 && (idx as usize) < items.len();
            match op {
                Edit::Set { value, create } => {
                    if in_range {
                        items[idx as usize] = (*value).clone();
                    } else if *create {
                        if idx < 0 {
                            items.insert(0, (*value).clone());
                        } else {
                            items.push((*value).clone());
                        }
                    }
                }
                Edit::Insert { value, after } => {
                    let pos = if idx < 0 {
                        0
                    } else if !in_range {
                        items.len()
                    } else if *after {
                        idx as usize + 1
                    } else {
                        idx as usize
                    };
                    items.insert(pos, (*value).clone());
                }
                Edit::Delete => {
                    if in_range {
                        items.remove(idx as usize);
                    }
                }
            }
        }
        _ => {
            if matches!(op, Edit::Delete) && rest.is_empty() {
                return Err(invalid("cannot delete path in scalar"));
            }
        }
    }
    Ok(())
}

fn text_path(v: &Value) -> Result<Vec<&str>> {
    path_of(v)?
        .iter()
        .enumerate()
        .map(|(i, p)| {
            p.as_str().ok_or_else(|| {
                Error::new(sqlstate::NULL_VALUE_NOT_ALLOWED, format!("path element at position {} is null", i + 1))
            })
        })
        .collect()
}
/// Path-based edits apply to objects and arrays only.
fn reject_scalar(j: &J, what: &str) -> Result<()> {
    if j.is_object() || j.is_array() {
        return Ok(());
    }
    Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("cannot {what} in scalar")))
}
fn jsonb_set(_: &FnCtx, a: &[Value]) -> Result<Value> {
    reject_scalar(jb(&a[0])?, "set path")?;
    let mut j = jb(&a[0])?.clone();
    let create = a.get(3).and_then(Value::as_bool).unwrap_or(true);
    edit(&mut j, &text_path(&a[1])?, &Edit::Set { value: jb(&a[2])?, create })?;
    Ok(jsonb(j))
}
fn jsonb_insert(_: &FnCtx, a: &[Value]) -> Result<Value> {
    reject_scalar(jb(&a[0])?, "set path")?;
    let mut j = jb(&a[0])?.clone();
    let after = a.get(3).and_then(Value::as_bool).unwrap_or(false);
    edit(&mut j, &text_path(&a[1])?, &Edit::Insert { value: jb(&a[2])?, after })?;
    Ok(jsonb(j))
}
fn op_delete_path(_: &FnCtx, a: &[Value]) -> Result<Value> {
    reject_scalar(jb(&a[0])?, "delete path")?;
    let mut j = jb(&a[0])?.clone();
    edit(&mut j, &text_path(&a[1])?, &Edit::Delete)?;
    Ok(jsonb(j))
}
fn strip(j: &J) -> J {
    match j {
        J::Object(m) => J::Object(m.iter().filter(|(_, v)| !v.is_null()).map(|(k, v)| (k.clone(), strip(v))).collect()),
        J::Array(items) => J::Array(items.iter().map(strip).collect()),
        other => other.clone(),
    }
}
fn strip_nulls(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let t = tree(&a[0])?;
    Ok(same_flavor(&a[0], strip(&t)))
}
fn pretty(j: &J, indent: usize, out: &mut String) {
    let pad = |n: usize, out: &mut String| out.push_str(&"    ".repeat(n));
    match j {
        J::Array(items) if !items.is_empty() => {
            out.push_str("[\n");
            for (i, item) in items.iter().enumerate() {
                pad(indent + 1, out);
                pretty(item, indent + 1, out);
                out.push_str(if i + 1 < items.len() { ",\n" } else { "\n" });
            }
            pad(indent, out);
            out.push(']');
        }
        J::Object(m) if !m.is_empty() => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.as_bytes().cmp(b.as_bytes())));
            out.push_str("{\n");
            for (i, k) in keys.iter().enumerate() {
                pad(indent + 1, out);
                out.push_str(&serde_json::to_string(k).unwrap_or_default());
                out.push_str(": ");
                pretty(&m[*k], indent + 1, out);
                out.push_str(if i + 1 < keys.len() { ",\n" } else { "\n" });
            }
            pad(indent, out);
            out.push('}');
        }
        J::Array(_) => {
            out.push_str("[\n");
            pad(indent, out);
            out.push(']');
        }
        J::Object(_) => {
            out.push_str("{\n");
            pad(indent, out);
            out.push('}');
        }
        other => io::write_jsonb(other, out),
    }
}
fn jsonb_pretty(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let mut s = String::new();
    pretty(jb(&a[0])?, 0, &mut s);
    Ok(Value::Text(s))
}

pub fn register_ops(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    for flavor in [Jsonb, Json] {
        let f = || t(flavor.clone());
        r.op("->", &[f(), t(Text)], Ret::T(flavor.clone()), op_get);
        r.op("->", &[f(), t(Int4)], Ret::T(flavor.clone()), op_get);
        r.op("->>", &[f(), t(Text)], Ret::T(Text), op_get_text);
        r.op("->>", &[f(), t(Int4)], Ret::T(Text), op_get_text);
        r.op("#>", &[f(), t(Text.array_of())], Ret::T(flavor.clone()), op_path);
        r.op("#>>", &[f(), t(Text.array_of())], Ret::T(Text), op_path_text);
    }
    r.op("@>", &[t(Jsonb), t(Jsonb)], Ret::T(Bool), op_contains);
    r.op("<@", &[t(Jsonb), t(Jsonb)], Ret::T(Bool), op_contained);
    r.op("?", &[t(Jsonb), t(Text)], Ret::T(Bool), op_exists);
    r.op("?|", &[t(Jsonb), t(Text.array_of())], Ret::T(Bool), op_exists_any);
    r.op("?&", &[t(Jsonb), t(Text.array_of())], Ret::T(Bool), op_exists_all);
    r.op("||", &[t(Jsonb), t(Jsonb)], Ret::T(Jsonb), op_concat);
    r.op("-", &[t(Jsonb), t(Text)], Ret::T(Jsonb), op_delete);
    r.op("-", &[t(Jsonb), t(Int4)], Ret::T(Jsonb), op_delete);
    r.op("-", &[t(Jsonb), t(Text.array_of())], Ret::T(Jsonb), op_delete);
    r.op("#-", &[t(Jsonb), t(Text.array_of())], Ret::T(Jsonb), op_delete_path);
}

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    r.f("to_json", &[P::Elem], Ret::T(Json), to_json).stable();
    r.f("to_jsonb", &[P::Elem], Ret::T(Jsonb), to_jsonb).stable();
    r.f("row_to_json", &[t(Record)], Ret::T(Json), to_json).stable();
    r.f("array_to_json", &[P::Array], Ret::T(Json), to_json).stable();
    r.f("json_build_object", &[], Ret::T(Json), json_build_object).stable();
    r.f("json_build_object", &[P::Any], Ret::T(Json), json_build_object).variadic().nonstrict().stable();
    r.f("jsonb_build_object", &[], Ret::T(Jsonb), jsonb_build_object).stable();
    r.f("jsonb_build_object", &[P::Any], Ret::T(Jsonb), jsonb_build_object).variadic().nonstrict().stable();
    r.f("json_build_array", &[], Ret::T(Json), json_build_array).stable();
    r.f("json_build_array", &[P::Any], Ret::T(Json), json_build_array).variadic().nonstrict().stable();
    r.f("jsonb_build_array", &[], Ret::T(Jsonb), jsonb_build_array).stable();
    r.f("jsonb_build_array", &[P::Any], Ret::T(Jsonb), jsonb_build_array).variadic().nonstrict().stable();
    r.f("json_typeof", &[t(Json)], Ret::T(Text), typeof_);
    r.f("jsonb_typeof", &[t(Jsonb)], Ret::T(Text), typeof_);
    r.f("json_array_length", &[t(Json)], Ret::T(Int4), array_length);
    r.f("jsonb_array_length", &[t(Jsonb)], Ret::T(Int4), array_length);
    r.f("json_extract_path", &[t(Json), t(Text)], Ret::T(Json), extract_path).variadic();
    r.f("jsonb_extract_path", &[t(Jsonb), t(Text)], Ret::T(Jsonb), extract_path).variadic();
    r.f("json_extract_path_text", &[t(Json), t(Text)], Ret::T(Text), extract_path_text).variadic();
    r.f("jsonb_extract_path_text", &[t(Jsonb), t(Text)], Ret::T(Text), extract_path_text).variadic();
    r.f("jsonb_set", &[t(Jsonb), t(Text.array_of()), t(Jsonb)], Ret::T(Jsonb), jsonb_set);
    r.f("jsonb_set", &[t(Jsonb), t(Text.array_of()), t(Jsonb), t(Bool)], Ret::T(Jsonb), jsonb_set);
    r.f("jsonb_insert", &[t(Jsonb), t(Text.array_of()), t(Jsonb)], Ret::T(Jsonb), jsonb_insert);
    r.f("jsonb_insert", &[t(Jsonb), t(Text.array_of()), t(Jsonb), t(Bool)], Ret::T(Jsonb), jsonb_insert);
    r.f("jsonb_strip_nulls", &[t(Jsonb)], Ret::T(Jsonb), strip_nulls);
    r.f("json_strip_nulls", &[t(Json)], Ret::T(Json), strip_nulls);
    r.f("jsonb_pretty", &[t(Jsonb)], Ret::T(Text), jsonb_pretty);
}
