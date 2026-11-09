//! Array functions (one-dimensional arrays, lower bound 1).

use std::cmp::Ordering;

use super::f_string::{int, txt};
use super::io;
use super::ops::{self, bad_arg};
use super::registry::{Registry, Ret, P};
use crate::error::sqlstate;
use crate::types::compare;
use crate::{Error, FnCtx, Result, Type, Value};

fn items(v: &Value) -> Result<&[Value]> {
    match v {
        Value::Array(i) => Ok(i),
        _ => Err(bad_arg()),
    }
}

fn dim_len(a: &[Value]) -> Result<Value> {
    let arr = items(&a[0])?;
    Ok(if int(&a[1])? == 1 && !arr.is_empty() { Value::Int4(arr.len() as i32) } else { Value::Null })
}
fn array_length(_: &FnCtx, a: &[Value]) -> Result<Value> {
    dim_len(a)
}
fn array_upper(_: &FnCtx, a: &[Value]) -> Result<Value> {
    dim_len(a)
}
fn array_lower(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let arr = items(&a[0])?;
    Ok(if int(&a[1])? == 1 && !arr.is_empty() { Value::Int4(1) } else { Value::Null })
}
fn cardinality(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(Value::Int4(items(&a[0])?.len() as i32))
}
fn array_ndims(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(if items(&a[0])?.is_empty() { Value::Null } else { Value::Int4(1) })
}
fn array_dims(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let arr = items(&a[0])?;
    Ok(if arr.is_empty() { Value::Null } else { Value::Text(format!("[1:{}]", arr.len())) })
}
fn array_cat(_: &FnCtx, a: &[Value]) -> Result<Value> {
    Ok(match (&a[0], &a[1]) {
        (Value::Null, Value::Null) => Value::Null,
        (Value::Array(x), Value::Null) | (Value::Null, Value::Array(x)) => Value::Array(x.clone()),
        (Value::Array(x), Value::Array(y)) => Value::Array(x.iter().chain(y).cloned().collect()),
        _ => return Err(bad_arg()),
    })
}
fn same(a: &Value, b: &Value) -> bool {
    match (a.is_null(), b.is_null()) {
        (true, true) => true,
        (false, false) => compare(a, b) == Ordering::Equal,
        _ => false,
    }
}
fn array_position(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    let arr = items(&a[0])?;
    let start = match a.get(2) {
        Some(Value::Null) => {
            return Err(Error::new(sqlstate::NULL_VALUE_NOT_ALLOWED, "initial position must not be null"))
        }
        Some(v) => (int(v)?.max(1) - 1) as usize,
        None => 0,
    };
    Ok(arr
        .iter()
        .enumerate()
        .skip(start)
        .find(|(_, v)| same(v, &a[1]))
        .map_or(Value::Null, |(i, _)| Value::Int4(i as i32 + 1)))
}
fn array_positions(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    Ok(Value::Array(
        items(&a[0])?
            .iter()
            .enumerate()
            .filter(|(_, v)| same(v, &a[1]))
            .map(|(i, _)| Value::Int4(i as i32 + 1))
            .collect(),
    ))
}
fn array_remove(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    Ok(Value::Array(items(&a[0])?.iter().filter(|v| !same(v, &a[1])).cloned().collect()))
}
fn array_replace(_: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() {
        return Ok(Value::Null);
    }
    Ok(Value::Array(items(&a[0])?.iter().map(|v| if same(v, &a[1]) { a[2].clone() } else { v.clone() }).collect()))
}
fn array_to_string(c: &FnCtx, a: &[Value]) -> Result<Value> {
    if a[0].is_null() || a[1].is_null() {
        return Ok(Value::Null);
    }
    let sep = txt(&a[1])?;
    let null_str = match a.get(2) {
        Some(Value::Text(s)) => Some(s.as_str()),
        _ => None,
    };
    let mut parts: Vec<String> = Vec::new();
    for v in items(&a[0])? {
        match (v, null_str) {
            (Value::Null, Some(n)) => parts.push(n.to_string()),
            (Value::Null, None) => {}
            (v, _) => parts.push(io::format_value(v, c)),
        }
    }
    Ok(Value::Text(parts.join(sep)))
}
fn trim_array(_: &FnCtx, a: &[Value]) -> Result<Value> {
    let arr = items(&a[0])?;
    let n = int(&a[1])?;
    if n < 0 || n as usize > arr.len() {
        return Err(Error::new(
            sqlstate::ARRAY_SUBSCRIPT_ERROR,
            format!("number of elements to trim must be between 0 and {}", arr.len()),
        ));
    }
    Ok(Value::Array(arr[..arr.len() - n as usize].to_vec()))
}

pub fn register(r: &mut Registry) {
    use Type::*;
    let t = P::T;
    r.f("array_length", &[P::Array, t(Int4)], Ret::T(Int4), array_length);
    r.f("array_upper", &[P::Array, t(Int4)], Ret::T(Int4), array_upper);
    r.f("array_lower", &[P::Array, t(Int4)], Ret::T(Int4), array_lower);
    r.f("cardinality", &[P::Array], Ret::T(Int4), cardinality);
    r.f("array_ndims", &[P::Array], Ret::T(Int4), array_ndims);
    r.f("array_dims", &[P::Array], Ret::T(Text), array_dims);
    r.f("array_append", &[P::Array, P::Elem], Ret::Array, ops::arr_append).nonstrict();
    r.f("array_prepend", &[P::Elem, P::Array], Ret::Array, ops::arr_prepend).nonstrict();
    r.f("array_cat", &[P::Array, P::Array], Ret::Array, array_cat).nonstrict();
    r.f("array_position", &[P::Array, P::Elem], Ret::T(Int4), array_position).nonstrict();
    r.f("array_position", &[P::Array, P::Elem, t(Int4)], Ret::T(Int4), array_position).nonstrict();
    r.f("array_positions", &[P::Array, P::Elem], Ret::T(Int4.array_of()), array_positions).nonstrict();
    r.f("array_remove", &[P::Array, P::Elem], Ret::Array, array_remove).nonstrict();
    r.f("array_replace", &[P::Array, P::Elem, P::Elem], Ret::Array, array_replace).nonstrict();
    r.f("array_to_string", &[P::Array, t(Text)], Ret::T(Text), array_to_string).nonstrict().stable();
    r.f("array_to_string", &[P::Array, t(Text), t(Text)], Ret::T(Text), array_to_string).nonstrict().stable();
    r.f("trim_array", &[P::Array, t(Int4)], Ret::Array, trim_array);
}
