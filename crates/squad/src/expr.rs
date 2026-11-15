//! Bound (type-checked) expressions and their evaluation.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::catalog::EnumType;
use crate::exec::ExecCx;
use crate::plan::Plan;
use crate::types::{compare, Type, Value};
use crate::value::{self, FuncImpl};
use crate::{sqlstate, Error, Result};

pub type Row = Vec<Value>;

/// A resolved scalar function or operator.
#[derive(Clone)]
pub struct FuncRef {
    pub name: Arc<str>,
    pub eval: FuncImpl,
    pub strict: bool,
    /// Result type, kept for access-path selection.
    pub ret: Type,
}

impl PartialEq for FuncRef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && std::ptr::fn_addr_eq(self.eval, other.eval)
    }
}

impl std::fmt::Debug for FuncRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

/// Functions that need the session or the catalog rather than just their
/// arguments.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionFn {
    NextVal,
    CurrVal,
    SetVal,
    LastVal,
    CurrentSchema,
    CurrentSchemas,
    CurrentUser,
    CurrentDatabase,
    Version,
    CurrentSetting,
    SetConfig,
    /// Next value of a table's hidden row id counter.
    RowId(u32),
    TxidCurrent,
    BackendPid,
    /// Catalog helper functions (`pg_get_expr`, `format_type`, ...), by name.
    Catalog(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubKind {
    /// `(SELECT x)`: one column, at most one row.
    Scalar,
    Exists,
    /// `ARRAY(SELECT x)`
    Array,
    /// `left op ANY/ALL (SELECT ...)`; `op == None` is equality with row
    /// support (`IN`).
    Quantified {
        left: Vec<Expr>,
        op: Option<FuncRef>,
        all: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SubqueryExpr {
    /// Slot in the per-execution cache for uncorrelated subqueries.
    pub id: usize,
    pub plan: Arc<Plan>,
    pub kind: SubKind,
    pub correlated: bool,
}

impl PartialEq for SubqueryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Const(Value),
    /// Column of the current input row.
    Column(usize),
    /// Column of an enclosing query's current row; depth 1 is the nearest.
    Outer(usize, usize),
    /// Column of the outer row of the innermost index nested-loop join.
    NlLeft(usize),
    /// `$n`, zero-based.
    Param(usize),
    Call(FuncRef, Vec<Expr>),
    Cast(Box<Expr>, Type),
    EnumCast(Box<Expr>, Arc<EnumType>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>, bool),
    BoolTest(Box<Expr>, BoolTest),
    /// `a IS DISTINCT FROM b`; the flag negates.
    Distinct(Box<Expr>, Box<Expr>, bool),
    Case(Vec<(Expr, Expr)>, Option<Box<Expr>>),
    Coalesce(Vec<Expr>),
    /// GREATEST (true) / LEAST (false)
    MinMax(Vec<Expr>, bool),
    /// `x [NOT] IN (list)`
    InList(Box<Expr>, Vec<Expr>, bool),
    /// `left op ANY/ALL (array)`
    AnyAll {
        left: Box<Expr>,
        op: FuncRef,
        right: Box<Expr>,
        all: bool,
    },
    Array(Vec<Expr>),
    Subscript(Box<Expr>, Box<Expr>),
    Slice(Box<Expr>, Option<Box<Expr>>, Option<Box<Expr>>),
    Row(Vec<Expr>),
    Field(Box<Expr>, usize),
    Subquery(Box<SubqueryExpr>),
    Session(SessionFn, Vec<Expr>),
    /// Placeholder for the n-th aggregate of the query being planned.
    AggRef(usize),
    /// Placeholder for the n-th window function of the query being planned.
    WindowRef(usize),
    /// Placeholder for the n-th set-returning function in the select list.
    SrfRef(usize),
}

fn bool_value(b: Option<bool>) -> Value {
    b.map_or(Value::Null, Value::Bool)
}

/// SQL equality with three-valued logic, descending into rows.
pub fn sql_eq(a: &Value, b: &Value) -> Option<bool> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Record(x), Value::Record(y)) if x.len() == y.len() => {
            let mut unknown = false;
            for (i, j) in x.iter().zip(y) {
                match sql_eq(i, j) {
                    Some(false) => return Some(false),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            if unknown {
                None
            } else {
                Some(true)
            }
        }
        _ => Some(compare(a, b) == Ordering::Equal),
    }
}

impl Expr {
    pub fn null() -> Expr {
        Expr::Const(Value::Null)
    }

    pub fn is_const(&self) -> bool {
        matches!(self, Expr::Const(_))
    }

    /// Calls `f` on this expression and all sub-expressions (not descending
    /// into subquery plans).
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        self.for_each_child(&mut |c| c.walk(f));
    }

    /// Calls `f` on the direct sub-expressions.
    pub fn for_each_child(&self, f: &mut dyn FnMut(&Expr)) {
        match self {
            Expr::Const(_)
            | Expr::Column(_)
            | Expr::Outer(..)
            | Expr::NlLeft(_)
            | Expr::Param(_)
            | Expr::AggRef(_)
            | Expr::WindowRef(_)
            | Expr::SrfRef(_) => {}
            Expr::Call(_, args)
            | Expr::And(args)
            | Expr::Or(args)
            | Expr::Coalesce(args)
            | Expr::MinMax(args, _)
            | Expr::Array(args)
            | Expr::Row(args)
            | Expr::Session(_, args) => args.iter().for_each(f),
            Expr::Cast(e, _)
            | Expr::EnumCast(e, _)
            | Expr::Not(e)
            | Expr::IsNull(e, _)
            | Expr::BoolTest(e, _)
            | Expr::Field(e, _) => f(e),
            Expr::Distinct(a, b, _) | Expr::Subscript(a, b) => {
                f(a);
                f(b);
            }
            Expr::Case(whens, els) => {
                for (w, t) in whens {
                    f(w);
                    f(t);
                }
                if let Some(e) = els {
                    f(e);
                }
            }
            Expr::InList(e, list, _) => {
                f(e);
                list.iter().for_each(f);
            }
            Expr::AnyAll { left, right, .. } => {
                f(left);
                f(right);
            }
            Expr::Slice(e, lo, hi) => {
                f(e);
                if let Some(lo) = lo {
                    f(lo);
                }
                if let Some(hi) = hi {
                    f(hi);
                }
            }
            Expr::Subquery(s) => {
                if let SubKind::Quantified { left, .. } = &s.kind {
                    left.iter().for_each(f);
                }
            }
        }
    }

    /// Rebuilds the expression bottom-up: `f` may replace any node. `f` is
    /// called on a node before its children; returning `Some` stops the
    /// descent there.
    pub fn rewrite(self, f: &mut dyn FnMut(&Expr) -> Result<Option<Expr>>) -> Result<Expr> {
        if let Some(r) = f(&self)? {
            return Ok(r);
        }
        let list = |v: Vec<Expr>, f: &mut dyn FnMut(&Expr) -> Result<Option<Expr>>| -> Result<Vec<Expr>> {
            v.into_iter().map(|e| e.rewrite(f)).collect()
        };
        let bx = |e: Box<Expr>, f: &mut dyn FnMut(&Expr) -> Result<Option<Expr>>| -> Result<Box<Expr>> {
            Ok(Box::new(e.rewrite(f)?))
        };
        Ok(match self {
            e @ (Expr::Const(_)
            | Expr::Column(_)
            | Expr::Outer(..)
            | Expr::NlLeft(_)
            | Expr::Param(_)
            | Expr::AggRef(_)
            | Expr::WindowRef(_)
            | Expr::SrfRef(_)) => e,
            Expr::Call(func, args) => Expr::Call(func, list(args, f)?),
            Expr::And(args) => Expr::And(list(args, f)?),
            Expr::Or(args) => Expr::Or(list(args, f)?),
            Expr::Coalesce(args) => Expr::Coalesce(list(args, f)?),
            Expr::MinMax(args, g) => Expr::MinMax(list(args, f)?, g),
            Expr::Array(args) => Expr::Array(list(args, f)?),
            Expr::Row(args) => Expr::Row(list(args, f)?),
            Expr::Session(s, args) => Expr::Session(s, list(args, f)?),
            Expr::Cast(e, t) => Expr::Cast(bx(e, f)?, t),
            Expr::EnumCast(e, t) => Expr::EnumCast(bx(e, f)?, t),
            Expr::Not(e) => Expr::Not(bx(e, f)?),
            Expr::IsNull(e, n) => Expr::IsNull(bx(e, f)?, n),
            Expr::BoolTest(e, t) => Expr::BoolTest(bx(e, f)?, t),
            Expr::Field(e, i) => Expr::Field(bx(e, f)?, i),
            Expr::Distinct(a, b, n) => Expr::Distinct(bx(a, f)?, bx(b, f)?, n),
            Expr::Subscript(a, b) => Expr::Subscript(bx(a, f)?, bx(b, f)?),
            Expr::Case(whens, els) => Expr::Case(
                whens.into_iter().map(|(w, t)| Ok((w.rewrite(f)?, t.rewrite(f)?))).collect::<Result<_>>()?,
                els.map(|e| bx(e, f)).transpose()?,
            ),
            Expr::InList(e, l, n) => Expr::InList(bx(e, f)?, list(l, f)?, n),
            Expr::AnyAll { left, op, right, all } => Expr::AnyAll { left: bx(left, f)?, op, right: bx(right, f)?, all },
            Expr::Slice(e, lo, hi) => {
                Expr::Slice(bx(e, f)?, lo.map(|e| bx(e, f)).transpose()?, hi.map(|e| bx(e, f)).transpose()?)
            }
            Expr::Subquery(mut s) => {
                if let SubKind::Quantified { left, op, all } = s.kind {
                    s.kind = SubKind::Quantified { left: list(left, f)?, op, all };
                }
                Expr::Subquery(s)
            }
        })
    }

    /// True if the expression reads the current input row (directly, not
    /// through an outer reference).
    pub fn uses_row(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| match e {
            Expr::Column(_) | Expr::AggRef(_) | Expr::WindowRef(_) | Expr::SrfRef(_) => found = true,
            // A correlated subquery reads this row through Outer(1, _).
            Expr::Subquery(s) if s.correlated => found = true,
            _ => {}
        });
        found
    }

    /// True if evaluating twice may give different results or has effects.
    pub fn is_volatile(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| match e {
            // Stable in PostgreSQL's terms: the same value throughout a
            // statement, so they can bound an index scan or be compared
            // by the column engine. Policies use `current_setting`.
            Expr::Session(
                SessionFn::CurrentSetting
                | SessionFn::CurrentUser
                | SessionFn::CurrentSchema
                | SessionFn::CurrentSchemas
                | SessionFn::CurrentDatabase
                | SessionFn::Version
                | SessionFn::BackendPid,
                _,
            ) => {}
            Expr::Session(..) | Expr::Subquery(_) => found = true,
            Expr::Call(f, _)
                if matches!(&*f.name, "random" | "gen_random_uuid" | "uuid_generate_v4" | "clock_timestamp") =>
            {
                found = true
            }
            _ => {}
        });
        found
    }

    /// Adds `delta` to every `Column` index.
    pub fn shift_columns(self, delta: usize) -> Expr {
        self.rewrite(&mut |e| Ok(if let Expr::Column(i) = e { Some(Expr::Column(i + delta)) } else { None }))
            .expect("infallible")
    }

    pub fn eval(&self, row: &[Value], cx: &mut ExecCx) -> Result<Value> {
        Ok(match self {
            Expr::Const(v) => v.clone(),
            Expr::Column(i) => row[*i].clone(),
            Expr::Outer(depth, i) => cx.outer[cx.outer.len() - depth][*i].clone(),
            Expr::NlLeft(i) => cx.nl_left.last().expect("index join without outer row")[*i].clone(),
            Expr::Param(i) => match cx.params.get(*i) {
                Some(v) => v.clone(),
                None => {
                    return Err(Error::new(sqlstate::UNDEFINED_PARAMETER, format!("there is no parameter ${}", i + 1)))
                }
            },
            Expr::Call(func, args) => {
                // Small inline buffer avoids a heap allocation for the
                // common unary/binary case.
                let mut buf: [Value; 2] = [Value::Null, Value::Null];
                if args.len() <= 2 {
                    for (slot, a) in buf.iter_mut().zip(args) {
                        *slot = a.eval(row, cx)?;
                        if func.strict && slot.is_null() {
                            return Ok(Value::Null);
                        }
                    }
                    (func.eval)(&cx.fn_ctx, &buf[..args.len()])?
                } else {
                    let mut vals = Vec::with_capacity(args.len());
                    for a in args {
                        let v = a.eval(row, cx)?;
                        if func.strict && v.is_null() {
                            return Ok(Value::Null);
                        }
                        vals.push(v);
                    }
                    (func.eval)(&cx.fn_ctx, &vals)?
                }
            }
            Expr::Cast(e, ty) => value::cast(e.eval(row, cx)?, ty, &cx.fn_ctx)?,
            Expr::EnumCast(e, ty) => match e.eval(row, cx)? {
                Value::Null => Value::Null,
                v @ Value::Enum(..) => v,
                v => enum_value(ty, v.as_str().unwrap_or_default())?,
            },
            Expr::And(args) => {
                let mut unknown = false;
                for a in args {
                    match a.eval(row, cx)? {
                        Value::Bool(false) => return Ok(Value::Bool(false)),
                        Value::Bool(true) => {}
                        _ => unknown = true,
                    }
                }
                bool_value(if unknown { None } else { Some(true) })
            }
            Expr::Or(args) => {
                let mut unknown = false;
                for a in args {
                    match a.eval(row, cx)? {
                        Value::Bool(true) => return Ok(Value::Bool(true)),
                        Value::Bool(false) => {}
                        _ => unknown = true,
                    }
                }
                bool_value(if unknown { None } else { Some(false) })
            }
            Expr::Not(e) => bool_value(e.eval(row, cx)?.as_bool().map(|b| !b)),
            Expr::IsNull(e, negated) => {
                let v = e.eval(row, cx)?;
                // A row is NULL when all fields are, NOT NULL when none is.
                let is = match &v {
                    Value::Record(fields) if *negated => return Ok(Value::Bool(fields.iter().all(|f| !f.is_null()))),
                    Value::Record(fields) => fields.iter().all(Value::is_null),
                    v => v.is_null(),
                };
                Value::Bool(is != *negated)
            }
            Expr::BoolTest(e, test) => {
                let v = e.eval(row, cx)?.as_bool();
                Value::Bool(match test {
                    BoolTest::IsTrue => v == Some(true),
                    BoolTest::IsNotTrue => v != Some(true),
                    BoolTest::IsFalse => v == Some(false),
                    BoolTest::IsNotFalse => v != Some(false),
                    BoolTest::IsUnknown => v.is_none(),
                    BoolTest::IsNotUnknown => v.is_some(),
                })
            }
            Expr::Distinct(a, b, negated) => {
                let (a, b) = (a.eval(row, cx)?, b.eval(row, cx)?);
                let distinct = match (a.is_null(), b.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    _ => compare(&a, &b) != Ordering::Equal,
                };
                Value::Bool(distinct != *negated)
            }
            Expr::Case(whens, els) => {
                for (cond, result) in whens {
                    if cond.eval(row, cx)? == Value::Bool(true) {
                        return result.eval(row, cx);
                    }
                }
                match els {
                    Some(e) => e.eval(row, cx)?,
                    None => Value::Null,
                }
            }
            Expr::Coalesce(args) => {
                for a in args {
                    let v = a.eval(row, cx)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                }
                Value::Null
            }
            Expr::MinMax(args, greatest) => {
                let mut best = Value::Null;
                for a in args {
                    let v = a.eval(row, cx)?;
                    if v.is_null() {
                        continue;
                    }
                    let better = best.is_null() || (compare(&v, &best) == Ordering::Greater) == *greatest;
                    if better && (best.is_null() || compare(&v, &best) != Ordering::Equal) {
                        best = v;
                    }
                }
                best
            }
            Expr::InList(e, list, negated) => {
                let v = e.eval(row, cx)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                let mut unknown = false;
                for item in list {
                    match sql_eq(&v, &item.eval(row, cx)?) {
                        Some(true) => return Ok(Value::Bool(!negated)),
                        None => unknown = true,
                        Some(false) => {}
                    }
                }
                bool_value(if unknown { None } else { Some(*negated) })
            }
            Expr::AnyAll { left, op, right, all } => {
                let l = left.eval(row, cx)?;
                let items = match right.eval(row, cx)? {
                    Value::Null => return Ok(Value::Null),
                    Value::Array(items) | Value::Vector(items) => items,
                    other => return Err(Error::internal(format!("ANY/ALL over non-array {other:?}"))),
                };
                quantified(items.iter(), *all, |item| {
                    if l.is_null() || item.is_null() {
                        return Ok(None);
                    }
                    Ok((op.eval)(&cx.fn_ctx, &[l.clone(), item.clone()])?.as_bool())
                })?
            }
            Expr::Array(items) => Value::Array(items.iter().map(|e| e.eval(row, cx)).collect::<Result<_>>()?),
            Expr::Subscript(arr, idx) => match (arr.eval(row, cx)?, idx.eval(row, cx)?) {
                (Value::Array(items), idx) => match idx.as_i64() {
                    Some(i) if i >= 1 => items.into_iter().nth(i as usize - 1).unwrap_or(Value::Null),
                    _ => Value::Null,
                },
                // Vector subscripts start at 0.
                (Value::Vector(items), idx) => match idx.as_i64() {
                    Some(i) if i >= 0 => items.into_iter().nth(i as usize).unwrap_or(Value::Null),
                    _ => Value::Null,
                },
                (Value::Jsonb(j), idx) => {
                    let found = match (&*j, &idx) {
                        (serde_json::Value::Object(m), Value::Text(k)) => m.get(k.as_str()).cloned(),
                        (serde_json::Value::Array(a), i) => i.as_i64().and_then(|i| {
                            let i = if i < 0 { a.len() as i64 + i } else { i };
                            a.get(usize::try_from(i).ok()?).cloned()
                        }),
                        _ => None,
                    };
                    found.map_or(Value::Null, |v| Value::Jsonb(Box::new(v)))
                }
                _ => Value::Null,
            },
            Expr::Slice(arr, lo, hi) => {
                // The result is an array with lower bound 1 also for a
                // vector (lower bound 0), as in PostgreSQL.
                let (items, lower) = match arr.eval(row, cx)? {
                    Value::Array(items) => (items, 1),
                    Value::Vector(items) => (items, 0),
                    _ => return Ok(Value::Null),
                };
                let bound = |e: &Option<Box<Expr>>, default: i64, cx: &mut ExecCx| -> Result<Option<i64>> {
                    match e {
                        None => Ok(Some(default)),
                        Some(e) => Ok(e.eval(row, cx)?.as_i64().map(|b| b - lower + 1)),
                    }
                };
                let (Some(lo), Some(hi)) = (bound(lo, 1, cx)?, bound(hi, items.len() as i64, cx)?) else {
                    return Ok(Value::Null);
                };
                let lo = lo.max(1) as usize;
                let hi = (hi.max(0) as usize).min(items.len());
                Value::Array(if lo > hi { Vec::new() } else { items[lo - 1..hi].to_vec() })
            }
            Expr::Row(items) => Value::Record(items.iter().map(|e| e.eval(row, cx)).collect::<Result<_>>()?),
            Expr::Field(e, i) => match e.eval(row, cx)? {
                Value::Record(mut fields) if *i < fields.len() => fields.swap_remove(*i),
                _ => Value::Null,
            },
            Expr::Subquery(sub) => cx.eval_subquery(sub, row)?,
            Expr::Session(func, args) => {
                let vals = args.iter().map(|e| e.eval(row, cx)).collect::<Result<Vec<_>>>()?;
                cx.session_fn(func, vals)?
            }
            Expr::AggRef(_) | Expr::WindowRef(_) | Expr::SrfRef(_) => {
                return Err(Error::internal("unresolved aggregate or window reference"))
            }
        })
    }

    /// Evaluates a predicate: only `true` passes.
    pub fn test(&self, row: &[Value], cx: &mut ExecCx) -> Result<bool> {
        Ok(matches!(self.eval(row, cx)?, Value::Bool(true)))
    }
}

/// Combines per-item comparison results under ANY (`all == false`) or ALL.
pub fn quantified<'a, I>(
    items: impl Iterator<Item = &'a I>,
    all: bool,
    mut test: impl FnMut(&'a I) -> Result<Option<bool>>,
) -> Result<Value>
where
    I: 'a,
{
    let mut unknown = false;
    for item in items {
        match test(item)? {
            Some(b) if b != all => return Ok(Value::Bool(!all)),
            Some(_) => {}
            None => unknown = true,
        }
    }
    Ok(bool_value(if unknown { None } else { Some(all) }))
}

/// Looks up `label` in an enum type.
pub fn enum_value(ty: &EnumType, label: &str) -> Result<Value> {
    match ty.labels.iter().position(|l| l == label) {
        Some(i) => Ok(Value::Enum(i as u32, label.to_string())),
        None => Err(Error::new(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            format!("invalid input value for enum {}: \"{}\"", ty.name, label),
        )),
    }
}
