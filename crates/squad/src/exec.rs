//! Plan execution: a pull-based (Volcano) executor. Scans, filters,
//! projections, limits and nested-loop joins stream; operators that need
//! their whole input (sort, aggregation, window functions, set operations)
//! materialize it.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use squad_kv::{Cursor, Isolation, LockMode, Txn, WaitPolicy};

use crate::catalog::{object_prefix, Catalog, Table};
use crate::encoding::{decode_row, encode_key, encode_key_tuple};
use crate::expr::{quantified, sql_eq, Expr, Row, SubKind, SubqueryExpr};
use crate::plan::{JoinKind, LockWait, Plan, RowLock, ScanPlan, SetOpKind, SortKey, Srf};
use crate::session::{SessionState, Shared};
use crate::types::{compare, FnCtx, Interval, Value};
use crate::{sqlstate, Error, Result};

enum SubCache {
    Value(Value),
    /// Result set of an uncorrelated quantified subquery.
    Set {
        keys: HashSet<Vec<u8>>,
        has_null: bool,
        rows: Rc<Vec<Row>>,
    },
}

/// Everything a running statement needs.
pub struct ExecCx<'a> {
    pub txn: &'a mut Txn,
    pub catalog: &'a Arc<Catalog>,
    pub params: &'a [Value],
    /// Current rows of enclosing queries, innermost last.
    pub outer: Vec<Row>,
    /// Outer rows of index nested-loop joins, innermost last.
    pub nl_left: Vec<Row>,
    pub fn_ctx: FnCtx,
    pub session: &'a mut SessionState,
    pub shared: &'a Arc<Shared>,
    sub_cache: Vec<Option<SubCache>>,
    ctes: HashMap<usize, Rc<Vec<Row>>>,
    /// Rows changed by the data-modifying statements run so far.
        pub rows_affected: u64,
    /// Segment objects made obsolete by this statement.
    pub dropped_objects: Vec<String>,
    /// Rows inserted into columnar tables by this statement.
    pub columnar_inserted: HashMap<u32, u64>,
}

impl<'a> ExecCx<'a> {
    pub fn new(
        txn: &'a mut Txn,
        catalog: &'a Arc<Catalog>,
        params: &'a [Value],
        session: &'a mut SessionState,
        shared: &'a Arc<Shared>,
        fn_ctx: FnCtx,
        subqueries: usize,
    ) -> ExecCx<'a> {
        ExecCx {
            txn,
            catalog,
            params,
            outer: Vec::new(),
            nl_left: Vec::new(),
            fn_ctx,
            session,
            shared,
            sub_cache: (0..subqueries).map(|_| None).collect(),
            ctes: HashMap::new(),
                        rows_affected: 0,
            dropped_objects: Vec::new(),
            columnar_inserted: HashMap::new(),
        }
    }

    pub fn wait_policy(&self) -> WaitPolicy {
        WaitPolicy::Wait(self.session.lock_timeout)
    }

    pub(crate) fn eval_subquery(&mut self, sub: &SubqueryExpr, row: &[Value]) -> Result<Value> {
        if sub.correlated {
            self.outer.push(row.to_vec());
            let result = self.run_subquery(sub, row);
            self.outer.pop();
            return result;
        }
        // Uncorrelated: run once per statement execution.
        if self.sub_cache.len() <= sub.id {
            self.sub_cache.resize_with(sub.id + 1, || None);
        }
        if self.sub_cache[sub.id].is_none() {
            let cache = match &sub.kind {
                SubKind::Quantified { .. } => {
                    let rows = collect(&sub.plan, self)?;
                    let mut keys = HashSet::with_capacity(rows.len());
                    let mut has_null = false;
                    for r in &rows {
                        if r.iter().any(Value::is_null) {
                            has_null = true;
                        } else {
                            keys.insert(encode_key_tuple(r));
                        }
                    }
                    SubCache::Set { keys, has_null, rows: Rc::new(rows) }
                }
                _ => SubCache::Value(self.run_subquery(sub, row)?),
            };
            self.sub_cache[sub.id] = Some(cache);
        }
        match self.sub_cache[sub.id].as_ref().unwrap() {
            SubCache::Value(v) => Ok(v.clone()),
            SubCache::Set { keys, has_null, rows } => {
                let SubKind::Quantified { left, op, all } = &sub.kind else { unreachable!() };
                let (has_null, rows) = (*has_null, rows.clone());
                let mut lvals = Vec::with_capacity(left.len());
                // `keys` borrows self; evaluate the probe first.
                let hit = if op.is_none() && !*all {
                    let mut null = false;
                    for e in left {
                        let v = e.clone().eval(row, self)?;
                        null |= v.is_null();
                        lvals.push(v);
                    }
                    if null {
                        None
                    } else {
                        let SubCache::Set { keys, .. } = self.sub_cache[sub.id].as_ref().unwrap() else {
                            unreachable!()
                        };
                        Some(keys.contains(&encode_key_tuple(&lvals)))
                    }
                } else {
                    let _ = keys;
                    return self.quantified_over(left, op, *all, &rows, row);
                };
                Ok(match hit {
                    Some(true) => Value::Bool(true),
                    // No exact match: unknown if NULLs are involved and
                    // the set is not empty.
                    Some(false) if has_null => Value::Null,
                    Some(false) => Value::Bool(false),
                    None if rows.is_empty() => Value::Bool(false),
                    None => Value::Null,
                })
            }
        }
    }

    fn quantified_over(
        &mut self,
        left: &[Expr],
        op: &Option<crate::expr::FuncRef>,
        all: bool,
        rows: &[Row],
        row: &[Value],
    ) -> Result<Value> {
        let lvals = left.iter().map(|e| e.eval(row, self)).collect::<Result<Vec<_>>>()?;
        let fn_ctx = self.fn_ctx.clone();
        quantified(rows.iter(), all, |r| match op {
            Some(op) => {
                if lvals[0].is_null() || r[0].is_null() {
                    return Ok(None);
                }
                Ok((op.eval)(&fn_ctx, &[lvals[0].clone(), r[0].clone()])?.as_bool())
            }
            None => {
                if lvals.len() == 1 {
                    Ok(sql_eq(&lvals[0], &r[0]))
                } else {
                    Ok(sql_eq(&Value::Record(lvals.clone()), &Value::Record(r.clone())))
                }
            }
        })
    }

    fn run_subquery(&mut self, sub: &SubqueryExpr, row: &[Value]) -> Result<Value> {
        match &sub.kind {
            SubKind::Exists => {
                let mut node = open(&sub.plan, self)?;
                Ok(Value::Bool(node.next(self)?.is_some()))
            }
            SubKind::Scalar => {
                let mut node = open(&sub.plan, self)?;
                let Some(mut first) = node.next(self)? else { return Ok(Value::Null) };
                if node.next(self)?.is_some() {
                    return Err(Error::new(
                        sqlstate::CARDINALITY_VIOLATION,
                        "more than one row returned by a subquery used as an expression",
                    ));
                }
                Ok(first.swap_remove(0))
            }
            SubKind::Array => {
                let rows = collect(&sub.plan, self)?;
                Ok(Value::Array(rows.into_iter().map(|mut r| r.swap_remove(0)).collect()))
            }
            SubKind::Quantified { left, op, all } => {
                let rows = collect(&sub.plan, self)?;
                // The left side belongs to the enclosing query: evaluate it
                // without the row pushed for the subquery.
                let pushed = if sub.correlated { self.outer.pop() } else { None };
                let result = self.quantified_over(left, op, *all, &rows, row);
                if let Some(p) = pushed {
                    self.outer.push(p);
                }
                result
            }
        }
    }
}

/// A running plan node.
pub trait Node {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>>;
}

/// Runs a plan to completion.
pub fn collect(plan: &Plan, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let mut node = open(plan, cx)?;
    let mut rows = Vec::new();
    while let Some(r) = node.next(cx)? {
        rows.push(r);
    }
    Ok(rows)
}

struct VecNode(std::vec::IntoIter<Row>);

impl Node for VecNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        Ok(self.0.next())
    }
}

fn rows(v: Vec<Row>) -> Box<dyn Node> {
    Box::new(VecNode(v.into_iter()))
}

/// Shared rows of a CTE, handed out by cloning.
struct RcNode(Rc<Vec<Row>>, usize);

impl Node for RcNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        self.1 += 1;
        Ok(self.0.get(self.1 - 1).cloned())
    }
}

pub fn open<'p>(plan: &'p Plan, cx: &mut ExecCx) -> Result<Box<dyn Node + 'p>> {
    Ok(match plan {
        Plan::OneRow => rows(vec![Vec::new()]),
        Plan::Values(list) => {
            let mut out = Vec::with_capacity(list.len());
            for exprs in list {
                out.push(exprs.iter().map(|e| e.eval(&[], cx)).collect::<Result<Row>>()?);
            }
            rows(out)
        }
                // A columnar table is its delta rows followed by its segments.
        Plan::Scan(scan) if scan.table.columnar.is_some() => Box::new(ChainNode {
            first: Some(Box::new(ScanNode::open(scan, cx)?)),
            second: Box::new(crate::columnar::SegmentRows::open(scan, cx)?),
        }),
        Plan::Scan(scan) => Box::new(ScanNode::open(scan, cx)?),
        Plan::ColumnarAgg { scan, groups, aggs, fallback } => match crate::columnar::aggregate(scan, groups, aggs, cx)? {
            Some(out) => rows(out),
            None => open(fallback, cx)?,
        },
        Plan::System(name) => rows(crate::system::scan(name, cx)?),
        Plan::Filter(input, pred) => Box::new(FilterNode { input: open(input, cx)?, pred }),
        Plan::Project(input, exprs) => Box::new(ProjectNode { input: open(input, cx)?, exprs }),
        Plan::Limit { input, limit, offset } => {
            let count = |e: &Option<Expr>, what: &str, cx: &mut ExecCx| -> Result<Option<u64>> {
                match e {
                    None => Ok(None),
                    Some(e) => match e.eval(&[], cx)? {
                        Value::Null => Ok(None),
                        v => match v.as_i64() {
                            Some(n) if n >= 0 => Ok(Some(n as u64)),
                            _ => Err(Error::new(
                                if what == "LIMIT" { "2201W" } else { "2201X" },
                                format!("{what} must not be negative"),
                            )),
                        },
                    },
                }
            };
            let limit = count(limit, "LIMIT", cx)?;
            let skip = count(offset, "OFFSET", cx)?.unwrap_or(0);
            Box::new(LimitNode { input: open(input, cx)?, skip, remaining: limit })
        }
        Plan::NestedLoop { left, right, kind, on, lateral, index_join, right_width } => {
            if matches!(kind, JoinKind::Right | JoinKind::Full) {
                rows(nested_loop_outer(left, right, *kind, on.as_ref(), *right_width, cx)?)
            } else {
                let rescan = *lateral || *index_join;
                let inner = if rescan { None } else { Some(Rc::new(collect(right, cx)?)) };
                Box::new(NestedLoopNode {
                    left: open(left, cx)?,
                    right,
                    kind: *kind,
                    on: on.as_ref(),
                    lateral: *lateral,
                    index_join: *index_join,
                    right_width: *right_width,
                    inner,
                    current: None,
                })
            }
        }
        Plan::HashJoin { left, right, kind, left_keys, right_keys, filter, left_width, right_width } => {
            let build = collect(right, cx)?;
            let mut table: HashMap<Vec<u8>, Vec<usize>> = HashMap::with_capacity(build.len());
            for (i, r) in build.iter().enumerate() {
                if let Some(k) = join_key(right_keys, r, cx)? {
                    table.entry(k).or_default().push(i);
                }
            }
            let matched = vec![false; if matches!(kind, JoinKind::Right | JoinKind::Full) { build.len() } else { 0 }];
            Box::new(HashJoinNode {
                left: open(left, cx)?,
                build,
                table,
                kind: *kind,
                left_keys,
                filter: filter.as_ref(),
                left_width: *left_width,
                right_width: *right_width,
                matched,
                pending: Vec::new(),
                drained: false,
            })
        }
        Plan::Aggregate { input, groups, aggs } => rows(crate::agg::aggregate(input, groups, aggs, cx)?),
        Plan::Window { input, calls } => rows(crate::agg::window(input, calls, cx)?),
        Plan::Sort { input, keys } => {
            let input = collect(input, cx)?;
            rows(sort_rows(input, keys, cx)?)
        }
        Plan::Distinct { input, on } => {
            Box::new(DistinctNode { input: open(input, cx)?, on: on.as_deref(), seen: HashSet::new(), last: None })
        }
        Plan::SetOp { kind, all, left, right } => match (kind, all) {
            (SetOpKind::Union, true) => Box::new(ChainNode { first: Some(open(left, cx)?), second: open(right, cx)? }),
            (SetOpKind::Union, false) => Box::new(DistinctNode {
                input: Box::new(ChainNode { first: Some(open(left, cx)?), second: open(right, cx)? }),
                on: None,
                seen: HashSet::new(),
                last: None,
            }),
            _ => {
                let l = collect(left, cx)?;
                let r = collect(right, cx)?;
                let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
                for row in &r {
                    *counts.entry(encode_key_tuple(row)).or_default() += 1;
                }
                let mut emitted: HashSet<Vec<u8>> = HashSet::new();
                let mut out = Vec::new();
                for row in l {
                    let k = encode_key_tuple(&row);
                    let in_right = counts.get_mut(&k).filter(|c| **c > 0);
                    let keep = match (kind, all, in_right) {
                        (SetOpKind::Intersect, true, Some(c)) => {
                            *c -= 1;
                            true
                        }
                        (SetOpKind::Intersect, false, Some(_)) => emitted.insert(k),
                        (SetOpKind::Intersect, _, None) => false,
                        (SetOpKind::Except, true, Some(c)) => {
                            *c -= 1;
                            false
                        }
                        (SetOpKind::Except, true, None) => true,
                        (SetOpKind::Except, false, Some(_)) => false,
                        (SetOpKind::Except, false, None) => emitted.insert(k),
                        (SetOpKind::Union, ..) => unreachable!(),
                    };
                    if keep {
                        out.push(row);
                    }
                }
                rows(out)
            }
        },
        Plan::With { ctes, body } => {
            for cte in ctes {
                let base = collect(&cte.plan, cx)?;
                let result = match &cte.recursive {
                    None => base,
                    Some((step, union_all)) => {
                        let mut seen: HashSet<Vec<u8>> = HashSet::new();
                        let mut dedup = |rows: Vec<Row>| -> Vec<Row> {
                            if *union_all {
                                rows
                            } else {
                                rows.into_iter().filter(|r| seen.insert(encode_key_tuple(r))).collect()
                            }
                        };
                        let mut all = dedup(base);
                        let mut working = all.clone();
                        while !working.is_empty() {
                            cx.ctes.insert(cte.id, Rc::new(working));
                            working = dedup(collect(step, cx)?);
                            all.extend(working.iter().cloned());
                        }
                        all
                    }
                };
                cx.ctes.insert(cte.id, Rc::new(result));
            }
            open(body, cx)?
        }
        Plan::CteScan(id) => {
            let data =
                cx.ctes.get(id).cloned().ok_or_else(|| Error::internal("CTE referenced before it was computed"))?;
            Box::new(RcNode(data, 0))
        }
        Plan::FuncScan { func, args, ordinality } => {
            let vals = args.iter().map(|e| e.eval(&[], cx)).collect::<Result<Vec<_>>>()?;
            let mut out = srf_rows(func, vals, cx)?;
            if *ordinality {
                for (i, r) in out.iter_mut().enumerate() {
                    r.push(Value::Int8(i as i64 + 1));
                }
            }
            rows(out)
        }
        Plan::ProjectSet { input, funcs } => {
            Box::new(ProjectSetNode { input: open(input, cx)?, funcs, pending: Vec::new() })
        }
        Plan::LockRows { input, table, offset, mode, wait, recheck } => Box::new(LockRowsNode {
            input: open(input, cx)?,
            table,
            offset: *offset,
            mode: *mode,
            wait: *wait,
            recheck: recheck.as_ref(),
        }),
        Plan::Insert(p) => rows(crate::dml::insert(p, cx)?),
        Plan::Update(p) => rows(crate::dml::update(p, cx)?),
        Plan::Delete(p) => rows(crate::dml::delete(p, cx)?),
    })
}

/// Scan of the row store part of a table (for a columnar table: its
/// delta).
pub(crate) fn row_store_scan<'p>(plan: &'p ScanPlan, cx: &mut ExecCx) -> Result<Box<dyn Node + 'p>> {
    Ok(Box::new(ScanNode::open(plan, cx)?))
}

/// Smallest byte string greater than every string starting with `key`.
fn successor(mut key: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(last) = key.pop() {
        if last != 0xFF {
            key.push(last + 1);
            return Some(key);
        }
    }
    None
}

/// Storage key of a table row.
pub fn row_key(table: &Table, row: &[Value]) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&object_prefix(table.id));
    for &c in &table.pk {
        encode_key(&mut key, &row[c]);
    }
    key
}

/// Decodes a stored row, padding columns added after it was written.
pub fn decode_table_row(table: &Table, bytes: &[u8]) -> Result<Row> {
    let mut row = decode_row(bytes)?;
    if row.len() < table.columns.len() {
        row.resize(table.columns.len(), Value::Null);
    }
    Ok(row)
}

enum ScanState {
    Done,
    Point(Vec<u8>),
    Range(Cursor),
}

struct ScanNode<'p> {
    plan: &'p ScanPlan,
    state: ScanState,
}

impl<'p> ScanNode<'p> {
    fn open(plan: &'p ScanPlan, cx: &mut ExecCx) -> Result<ScanNode<'p>> {
        let id = plan.index_def().map_or(plan.table.id, |ix| ix.id);
        let mut base = Vec::with_capacity(32);
        base.extend_from_slice(&object_prefix(id));
        for e in &plan.eq {
            let v = e.eval(&[], cx)?;
            if v.is_null() {
                return Ok(ScanNode { plan, state: ScanState::Done });
            }
            encode_key(&mut base, &v);
        }
        if plan.index.is_none() && plan.eq.len() == plan.table.pk.len() {
            return Ok(ScanNode { plan, state: ScanState::Point(base) });
        }
                // `None`: the bound is NULL, so nothing can match. `Some(None)`:
        // the scan is open on that side.
        type KeyBound = Option<(Vec<u8>, bool)>;
        let mut bound = |b: &Option<(Expr, bool)>| -> Result<Option<KeyBound>> {
            match b {
                None => Ok(Some(None)),
                Some((e, inclusive)) => {
                    let v = e.eval(&[], cx)?;
                    if v.is_null() {
                        return Ok(None);
                    }
                    let mut k = base.clone();
                    encode_key(&mut k, &v);
                    Ok(Some(Some((k, *inclusive))))
                }
            }
        };
        let (Some(lower), Some(upper)) = (bound(&plan.lower)?, bound(&plan.upper)?) else {
            return Ok(ScanNode { plan, state: ScanState::Done });
        };
        let lo = match lower {
            Some((k, true)) => Bound::Included(k),
            Some((k, false)) => match successor(k) {
                Some(s) => Bound::Included(s),
                None => return Ok(ScanNode { plan, state: ScanState::Done }),
            },
            // A range predicate never matches NULL, which sorts first.
            None if upper.is_some() => {
                let mut k = base.clone();
                k.push(1);
                Bound::Included(k)
            }
            None => Bound::Included(base.clone()),
        };
        let hi = match upper {
            Some((k, true)) => successor(k).map_or(Bound::Unbounded, Bound::Excluded),
            Some((k, false)) => Bound::Excluded(k),
            None => successor(base).map_or(Bound::Unbounded, Bound::Excluded),
        };
        let cursor = Cursor::new(cx.txn, lo, hi, plan.reverse);
        let cursor = if plan.is_unique_lookup() { cursor.with_chunk(1) } else { cursor };
        Ok(ScanNode { plan, state: ScanState::Range(cursor) })
    }
}

impl Node for ScanNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        let table = &self.plan.table;
        loop {
            let bytes = match &mut self.state {
                ScanState::Done => return Ok(None),
                ScanState::Point(key) => {
                    let v = cx.txn.get(key)?;
                    self.state = ScanState::Done;
                    match v {
                        Some(v) => v,
                        None => return Ok(None),
                    }
                }
                ScanState::Range(cursor) => match cursor.next(cx.txn)? {
                    None => {
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    Some((_, v)) if self.plan.index.is_none() => v,
                    Some((_, pk)) => {
                        // Index entry: the value is the row's key suffix.
                        let mut key = Vec::with_capacity(4 + pk.len());
                        key.extend_from_slice(&object_prefix(table.id));
                        key.extend_from_slice(&pk);
                        match cx.txn.get(&key)? {
                            Some(v) => v,
                            None => continue,
                        }
                    }
                },
            };
            let row = decode_table_row(table, &bytes)?;
            match &self.plan.filter {
                Some(f) if !f.test(&row, cx)? => continue,
                _ => return Ok(Some(row)),
            }
        }
    }
}

struct FilterNode<'p> {
    input: Box<dyn Node + 'p>,
    pred: &'p Expr,
}

impl Node for FilterNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(cx)? {
            if self.pred.test(&row, cx)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct ProjectNode<'p> {
    input: Box<dyn Node + 'p>,
    exprs: &'p [Expr],
}

impl Node for ProjectNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        let Some(row) = self.input.next(cx)? else { return Ok(None) };
        let mut out = Vec::with_capacity(self.exprs.len());
        for e in self.exprs {
            out.push(e.eval(&row, cx)?);
        }
        Ok(Some(out))
    }
}

struct LimitNode<'p> {
    input: Box<dyn Node + 'p>,
    skip: u64,
    remaining: Option<u64>,
}

impl Node for LimitNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        while self.skip > 0 {
            if self.input.next(cx)?.is_none() {
                return Ok(None);
            }
            self.skip -= 1;
        }
        let row = self.input.next(cx)?;
        if let (Some(_), Some(n)) = (&row, &mut self.remaining) {
            *n -= 1;
        }
        Ok(row)
    }
}

struct ChainNode<'p> {
    first: Option<Box<dyn Node + 'p>>,
    second: Box<dyn Node + 'p>,
}

impl Node for ChainNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        if let Some(first) = &mut self.first {
            if let Some(r) = first.next(cx)? {
                return Ok(Some(r));
            }
            self.first = None;
        }
        self.second.next(cx)
    }
}

struct DistinctNode<'p> {
    input: Box<dyn Node + 'p>,
    on: Option<&'p [Expr]>,
    seen: HashSet<Vec<u8>>,
    last: Option<Vec<u8>>,
}

impl Node for DistinctNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(cx)? {
            match self.on {
                None => {
                    if self.seen.insert(encode_key_tuple(&row)) {
                        return Ok(Some(row));
                    }
                }
                Some(on) => {
                    let mut key = Vec::new();
                    for e in on {
                        encode_key(&mut key, &e.eval(&row, cx)?);
                    }
                    if self.last.as_ref() != Some(&key) {
                        self.last = Some(key);
                        return Ok(Some(row));
                    }
                }
            }
        }
        Ok(None)
    }
}

fn concat(left: &[Value], right: &[Value]) -> Row {
    let mut row = Vec::with_capacity(left.len() + right.len());
    row.extend_from_slice(left);
    row.extend_from_slice(right);
    row
}

fn pad(left: &[Value], width: usize) -> Row {
    let mut row = Vec::with_capacity(left.len() + width);
    row.extend_from_slice(left);
    row.resize(left.len() + width, Value::Null);
    row
}

struct NestedLoopNode<'p> {
    left: Box<dyn Node + 'p>,
    right: &'p Plan,
    kind: JoinKind,
    on: Option<&'p Expr>,
    lateral: bool,
    index_join: bool,
    right_width: usize,
    /// Materialized right side when it does not depend on the left row.
    inner: Option<Rc<Vec<Row>>>,
    /// Left row being joined: `(row, right rows, position, matched)`.
    current: Option<(Row, Rc<Vec<Row>>, usize, bool)>,
}

impl Node for NestedLoopNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if self.current.is_none() {
                let Some(left) = self.left.next(cx)? else { return Ok(None) };
                let right = match &self.inner {
                    Some(r) => r.clone(),
                    None => {
                        // Re-run the right side with the left row visible.
                        let stack = if self.index_join { &mut cx.nl_left } else { &mut cx.outer };
                        stack.push(left.clone());
                        let result = collect(self.right, cx);
                        let stack = if self.index_join { &mut cx.nl_left } else { &mut cx.outer };
                        stack.pop();
                        let _ = self.lateral;
                        Rc::new(result?)
                    }
                };
                self.current = Some((left, right, 0, false));
            }
            let (left, right, pos, matched) = self.current.as_mut().unwrap();
            while *pos < right.len() {
                let row = concat(left, &right[*pos]);
                *pos += 1;
                let ok = match self.on {
                    Some(on) => on.test(&row, cx)?,
                    None => true,
                };
                if !ok {
                    continue;
                }
                *matched = true;
                match self.kind {
                    JoinKind::Semi => {
                        let out = std::mem::take(left);
                        self.current = None;
                        return Ok(Some(out));
                    }
                    JoinKind::Anti => break,
                    _ => return Ok(Some(row)),
                }
            }
            let (left, _, _, matched) = self.current.take().unwrap();
            match self.kind {
                JoinKind::Left if !matched => return Ok(Some(pad(&left, self.right_width))),
                JoinKind::Anti if !matched => return Ok(Some(left)),
                _ => {}
            }
        }
    }
}

/// RIGHT and FULL joins without usable equality keys.
fn nested_loop_outer(
    left: &Plan,
    right: &Plan,
    kind: JoinKind,
    on: Option<&Expr>,
    right_width: usize,
    cx: &mut ExecCx,
) -> Result<Vec<Row>> {
    let lrows = collect(left, cx)?;
    let rrows = collect(right, cx)?;
    let left_width = lrows.first().map_or(0, Vec::len);
    let mut rmatched = vec![false; rrows.len()];
    let mut out = Vec::new();
    for l in &lrows {
        let mut matched = false;
        for (i, r) in rrows.iter().enumerate() {
            let row = concat(l, r);
            if match on {
                Some(on) => on.test(&row, cx)?,
                None => true,
            } {
                matched = true;
                rmatched[i] = true;
                out.push(row);
            }
        }
        if !matched && kind == JoinKind::Full {
            out.push(pad(l, right_width));
        }
    }
    // The left width is unknown when the left side is empty; the planner
    // records only the right width, so derive it from the unmatched rows.
    for (i, r) in rrows.iter().enumerate() {
        if !rmatched[i] {
            let mut row = vec![Value::Null; left_width.max(plan_width(left))];
            row.extend_from_slice(r);
            out.push(row);
        }
    }
    Ok(out)
}

/// Number of columns a plan produces, where it is statically known.
fn plan_width(plan: &Plan) -> usize {
    match plan {
        Plan::OneRow => 0,
        Plan::Values(rows) => rows.first().map_or(0, Vec::len),
        Plan::Scan(s) => s.table.columns.len(),
        Plan::Project(_, exprs) => exprs.len(),
        Plan::Filter(input, _)
        | Plan::Sort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Distinct { input, .. }
        | Plan::LockRows { input, .. } => plan_width(input),
        Plan::NestedLoop { left, right_width, kind, .. } => match kind {
            JoinKind::Semi | JoinKind::Anti => plan_width(left),
            _ => plan_width(left) + right_width,
        },
        Plan::HashJoin { left_width, right_width, kind, .. } => match kind {
            JoinKind::Semi | JoinKind::Anti => *left_width,
            _ => left_width + right_width,
        },
                Plan::Aggregate { groups, aggs, .. } | Plan::ColumnarAgg { groups, aggs, .. } => groups.len() + aggs.len(),
        Plan::Window { input, calls } => plan_width(input) + calls.len(),
        Plan::ProjectSet { input, funcs } => plan_width(input) + funcs.len(),
        Plan::SetOp { left, .. } => plan_width(left),
        Plan::With { body, .. } => plan_width(body),
        _ => 0,
    }
}

fn join_key(keys: &[Expr], row: &[Value], cx: &mut ExecCx) -> Result<Option<Vec<u8>>> {
    let mut key = Vec::with_capacity(16);
    for e in keys {
        let v = e.eval(row, cx)?;
        if v.is_null() {
            return Ok(None);
        }
        encode_key(&mut key, &v);
    }
    Ok(Some(key))
}

struct HashJoinNode<'p> {
    left: Box<dyn Node + 'p>,
    build: Vec<Row>,
    table: HashMap<Vec<u8>, Vec<usize>>,
    kind: JoinKind,
    left_keys: &'p [Expr],
    filter: Option<&'p Expr>,
    left_width: usize,
    right_width: usize,
    matched: Vec<bool>,
    /// Output rows of the current probe row, in reverse order.
    pending: Vec<Row>,
    drained: bool,
}

impl Node for HashJoinNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.pending.pop() {
                return Ok(Some(r));
            }
            if self.drained {
                return Ok(None);
            }
            let Some(left) = self.left.next(cx)? else {
                self.drained = true;
                // Unmatched build rows of RIGHT / FULL joins.
                for (i, m) in self.matched.iter().enumerate().rev() {
                    if !m {
                        let mut row = vec![Value::Null; self.left_width];
                        row.extend_from_slice(&self.build[i]);
                        self.pending.push(row);
                    }
                }
                continue;
            };
            let mut any = false;
            if let Some(key) = join_key(self.left_keys, &left, cx)? {
                if let Some(candidates) = self.table.get(&key) {
                    for &i in candidates.iter().rev() {
                        let row = concat(&left, &self.build[i]);
                        if match self.filter {
                            Some(f) => f.test(&row, cx)?,
                            None => true,
                        } {
                            any = true;
                            if let Some(m) = self.matched.get_mut(i) {
                                *m = true;
                            }
                            match self.kind {
                                JoinKind::Semi | JoinKind::Anti => break,
                                _ => self.pending.push(row),
                            }
                        }
                    }
                }
            }
            match (self.kind, any) {
                (JoinKind::Semi, true) | (JoinKind::Anti, false) => return Ok(Some(left)),
                (JoinKind::Left | JoinKind::Full, false) => return Ok(Some(pad(&left, self.right_width))),
                _ => {}
            }
        }
    }
}

/// Compares two rows of pre-computed sort key values.
pub(crate) fn compare_keys(a: &[Value], b: &[Value], keys: &[SortKey]) -> Ordering {
    for ((x, y), k) in a.iter().zip(b).zip(keys) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if k.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if k.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let o = compare(x, y);
                if k.desc {
                    o.reverse()
                } else {
                    o
                }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(crate) fn sort_rows(input: Vec<Row>, keys: &[SortKey], cx: &mut ExecCx) -> Result<Vec<Row>> {
    let mut keyed = Vec::with_capacity(input.len());
    for row in input {
        let mut k = Vec::with_capacity(keys.len());
        for key in keys {
            k.push(key.expr.eval(&row, cx)?);
        }
        keyed.push((k, row));
    }
    keyed.sort_by(|a, b| compare_keys(&a.0, &b.0, keys));
    Ok(keyed.into_iter().map(|(_, r)| r).collect())
}

struct ProjectSetNode<'p> {
    input: Box<dyn Node + 'p>,
    funcs: &'p [(Srf, Vec<Expr>)],
    pending: Vec<Row>,
}

impl Node for ProjectSetNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.pending.pop() {
                return Ok(Some(r));
            }
            let Some(row) = self.input.next(cx)? else { return Ok(None) };
            let mut columns = Vec::with_capacity(self.funcs.len());
            let mut longest = 0;
            for (func, args) in self.funcs {
                let vals = args.iter().map(|e| e.eval(&row, cx)).collect::<Result<Vec<_>>>()?;
                let produced: Vec<Value> =
                    srf_rows(func, vals, cx)?.into_iter().map(|mut r| r.swap_remove(0)).collect();
                longest = longest.max(produced.len());
                columns.push(produced);
            }
            for i in (0..longest).rev() {
                let mut out = row.clone();
                for c in &columns {
                    out.push(c.get(i).cloned().unwrap_or(Value::Null));
                }
                self.pending.push(out);
            }
        }
    }
}

struct LockRowsNode<'p> {
    input: Box<dyn Node + 'p>,
    table: &'p Arc<Table>,
    offset: usize,
    mode: RowLock,
    wait: LockWait,
    recheck: Option<&'p Expr>,
}

impl Node for LockRowsNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        let width = self.table.columns.len();
        while let Some(mut row) = self.input.next(cx)? {
            let slice = &row[self.offset..self.offset + width];
            // The nullable side of an outer join has no row to lock.
            if self.table.pk.iter().any(|&c| slice[c].is_null()) {
                return Ok(Some(row));
            }
            let key = row_key(self.table, slice);
            let mode = match self.mode {
                RowLock::Update => LockMode::Exclusive,
                RowLock::Share => LockMode::Shared,
            };
            let wait = match self.wait {
                LockWait::Block => cx.wait_policy(),
                LockWait::Skip => WaitPolicy::Skip,
                LockWait::Error => WaitPolicy::NoWait,
            };
            if !cx.txn.lock(&key, mode, wait)? {
                continue;
            }
            match refresh_locked_row(cx, self.table, &key)? {
                Refreshed::Unchanged => return Ok(Some(row)),
                Refreshed::Deleted => continue,
                Refreshed::Changed(new) => {
                    row.splice(self.offset..self.offset + width, new);
                    match self.recheck {
                        Some(pred) if !pred.test(&row, cx)? => continue,
                        _ => return Ok(Some(row)),
                    }
                }
            }
        }
        Ok(None)
    }
}

pub(crate) enum Refreshed {
    Unchanged,
    Deleted,
    Changed(Row),
}

/// After a row lock was granted: detects whether another transaction
/// changed the row after this statement's snapshot was taken. Under Read
/// Committed the caller continues with the new version; under the stricter
/// levels this is a serialization failure, as in Postgres.
pub(crate) fn refresh_locked_row(cx: &mut ExecCx, table: &Table, key: &[u8]) -> Result<Refreshed> {
    let Some((value, ts)) = cx.txn.get_latest(key)? else { return Ok(Refreshed::Unchanged) };
    if ts <= cx.txn.snapshot_ts() {
        return Ok(Refreshed::Unchanged);
    }
    if cx.txn.isolation() != Isolation::ReadCommitted {
        return Err(Error::new(sqlstate::SERIALIZATION_FAILURE, "could not serialize access due to concurrent update"));
    }
    match value {
        None => Ok(Refreshed::Deleted),
        Some(bytes) => Ok(Refreshed::Changed(decode_table_row(table, &bytes)?)),
    }
}

fn srf_rows(func: &Srf, args: Vec<Value>, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let one = |v: Value| vec![v];
    Ok(match func {
        Srf::GenerateSeries => {
            if args.iter().any(Value::is_null) {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            match (&args[0], &args[1]) {
                (Value::Timestamp(_) | Value::TimestampTz(_), _) => {
                    let tz = matches!(args[0], Value::TimestampTz(_));
                    let (Value::Timestamp(start) | Value::TimestampTz(start)) = args[0] else { unreachable!() };
                    let (Value::Timestamp(end) | Value::TimestampTz(end)) = args[1] else {
                        return Err(Error::internal("generate_series bounds differ in type"));
                    };
                    let Value::Interval(step) = args[2] else {
                        return Err(Error::internal("generate_series step is not an interval"));
                    };
                    let sign = step.cmp_key().signum();
                    if sign == 0 {
                        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
                    }
                    let plus = crate::value::resolve_operator(
                        "+",
                        Some(&if tz { crate::Type::TimestampTz } else { crate::Type::Timestamp }),
                        &crate::Type::Interval,
                    )?;
                    let mut cur = start;
                    while (sign > 0 && cur <= end) || (sign < 0 && cur >= end) {
                        let v = if tz { Value::TimestampTz(cur) } else { Value::Timestamp(cur) };
                        out.push(one(v.clone()));
                        cur = match (plus.eval)(&cx.fn_ctx, &[v, Value::Interval(Interval { ..step })])? {
                            Value::Timestamp(t) | Value::TimestampTz(t) => t,
                            _ => break,
                        };
                    }
                }
                (Value::Numeric(start), Value::Numeric(end)) => {
                    let step = match args.get(2) {
                        Some(Value::Numeric(s)) => *s,
                        _ => rust_decimal::Decimal::ONE,
                    };
                    if step.is_zero() {
                        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
                    }
                    let mut cur = *start;
                    while (step.is_sign_positive() && cur <= *end) || (step.is_sign_negative() && cur >= *end) {
                        out.push(one(Value::Numeric(cur)));
                        cur += step;
                    }
                }
                (a, b) => {
                    let (Some(start), Some(end)) = (a.as_i64(), b.as_i64()) else {
                        return Err(Error::internal("generate_series over unsupported type"));
                    };
                    let step = args.get(2).and_then(Value::as_i64).unwrap_or(1);
                    if step == 0 {
                        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
                    }
                    let wide = matches!(a, Value::Int8(_));
                    let mut cur = start;
                    while (step > 0 && cur <= end) || (step < 0 && cur >= end) {
                        out.push(one(if wide { Value::Int8(cur) } else { Value::Int4(cur as i32) }));
                        match cur.checked_add(step) {
                            Some(n) => cur = n,
                            None => break,
                        }
                    }
                }
            }
            out
        }
        Srf::Unnest => {
            let lists: Vec<Vec<Value>> = args
                .into_iter()
                .map(|a| match a {
                    Value::Array(items) => items,
                    _ => Vec::new(),
                })
                .collect();
            let n = lists.iter().map(Vec::len).max().unwrap_or(0);
            (0..n).map(|i| lists.iter().map(|l| l.get(i).cloned().unwrap_or(Value::Null)).collect()).collect()
        }
        Srf::JsonArrayElements { text } | Srf::JsonEach { text } => {
            let each = matches!(func, Srf::JsonEach { .. });
            let (tree, is_jsonb) = match &args[0] {
                Value::Null => return Ok(Vec::new()),
                Value::Jsonb(j) => ((**j).clone(), true),
                Value::Json(s) => (
                    serde_json::from_str(s).map_err(|_| {
                        Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, "invalid input syntax for type json")
                    })?,
                    false,
                ),
                _ => return Err(Error::internal("json function over non-json value")),
            };
            let wrap = |v: serde_json::Value| -> Value {
                if *text {
                    match v {
                        serde_json::Value::Null => Value::Null,
                        serde_json::Value::String(s) => Value::Text(s),
                        other => {
                            Value::Text(crate::value::format_value(&Value::Jsonb(Box::new(other)), &FnCtx::default()))
                        }
                    }
                } else if is_jsonb {
                    Value::Jsonb(Box::new(v))
                } else {
                    Value::Json(v.to_string())
                }
            };
            match (tree, each) {
                (serde_json::Value::Array(items), false) => items.into_iter().map(|v| one(wrap(v))).collect(),
                (serde_json::Value::Object(map), true) => {
                    map.into_iter().map(|(k, v)| vec![Value::Text(k), wrap(v)]).collect()
                }
                (_, false) => {
                    return Err(Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        "cannot extract elements from a non-array",
                    ))
                }
                (_, true) => {
                    return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "cannot deconstruct a non-object"))
                }
            }
        }
        Srf::JsonObjectKeys => {
            let tree: serde_json::Value = match &args[0] {
                Value::Null => return Ok(Vec::new()),
                Value::Jsonb(j) => (**j).clone(),
                Value::Json(s) => serde_json::from_str(s).map_err(|_| {
                    Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, "invalid input syntax for type json")
                })?,
                _ => return Err(Error::internal("json function over non-json value")),
            };
            match tree {
                serde_json::Value::Object(map) => map.into_iter().map(|(k, _)| one(Value::Text(k))).collect(),
                _ => {
                    return Err(Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        "cannot call jsonb_object_keys on a non-object",
                    ))
                }
            }
        }
        Srf::RegexpSplitToTable | Srf::StringToTable => {
            if args[0].is_null() {
                return Ok(Vec::new());
            }
            let name =
                if matches!(func, Srf::RegexpSplitToTable) { "regexp_split_to_array" } else { "string_to_array" };
            let types: Vec<crate::Type> = args.iter().map(|_| crate::Type::Text).collect();
            let f = crate::value::resolve_function(name, &types)?;
            match (f.eval)(&cx.fn_ctx, &args)? {
                Value::Array(items) => items.into_iter().map(one).collect(),
                _ => Vec::new(),
            }
        }
        Srf::GenerateSubscripts => match (&args[0], args[1].as_i64()) {
            (Value::Array(items), Some(1)) => (1..=items.len() as i32).map(|i| one(Value::Int4(i))).collect(),
            _ => Vec::new(),
        },
    })
}

#[allow(dead_code)]
fn _assert_types(_: Duration) {}
