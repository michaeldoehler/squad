//! Plan execution: a pull-based (Volcano) executor. Scans, filters,
//! projections, limits and nested-loop joins stream; operators that need
//! their whole input (sort, aggregation, window functions, set operations)
//! materialize it.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use squad_kv::{Cursor, Isolation, LockMode, Txn, WaitPolicy};

use crate::catalog::{object_prefix, Catalog, Table};
use crate::encoding::{decode_row, encode_key, encode_key_tuple};
use crate::expr::{quantified, sql_eq, Expr, Row, SubKind, SubqueryExpr};
use crate::plan::{JoinKind, LockWait, Plan, RowLock, ScanPlan, SetOpKind, SortKey, Srf};
use crate::session::{SessionState, Shared};
use crate::spill::{
    compare_ordered, out_of_memory, partition_of, row_size, Partitions, RowStore, SortOrder, Sorted, Sorter, SpillFile,
    SpillReader, Stored, StoredIter, ENTRY_OVERHEAD, MAX_LEVEL,
};
use crate::types::{FnCtx, Interval, Value};
use crate::{sqlstate, Error, Result};

enum SubCache {
    Value(Value),
    /// Result set of an uncorrelated quantified subquery, with the keys
    /// of its rows hashed while they fit into the hash budget.
    Set {
        keys: Option<HashSet<Vec<u8>>>,
        has_null: bool,
        rows: Rc<Stored>,
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
    ctes: HashMap<usize, Rc<Stored>>,
    /// Rows changed by the data-modifying statements run so far.
    pub rows_affected: u64,
    /// Segment objects made obsolete by this statement.
    pub dropped_objects: Vec<String>,
    /// Rows and delta blocks inserted into columnar tables by this
    /// statement.
    pub columnar_inserted: HashMap<u32, (u64, u64)>,
    /// Segment objects written by this statement; they belong to the
    /// transaction until it ends.
    pub written_objects: Vec<String>,
    /// When `statement_timeout` expires.
    deadline: Option<Instant>,
    /// Calls of [`ExecCx::check_interrupts`] since the last real check.
    ticks: u32,
}

/// How many rows the executor processes between two looks at the cancel
/// flag and the clock.
const INTERRUPT_INTERVAL: u32 = 256;

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
            shared,
            sub_cache: (0..subqueries).map(|_| None).collect(),
            ctes: HashMap::new(),
            rows_affected: 0,
            dropped_objects: Vec::new(),
            columnar_inserted: HashMap::new(),
            written_objects: Vec::new(),
            deadline: session.statement_timeout.map(|d| Instant::now() + d),
            ticks: 0,
            session,
        }
    }

    /// Fails with SQLSTATE 57014 once the statement was cancelled or ran
    /// past `statement_timeout`. Loops of the executor call this once per
    /// row; the flag and the clock are only read every few hundred calls.
    #[inline]
    pub(crate) fn check_interrupts(&mut self) -> Result<()> {
        self.ticks += 1;
        if self.ticks < INTERRUPT_INTERVAL {
            return Ok(());
        }
        self.ticks = 0;
        self.check_interrupts_now()
    }

    #[cold]
    pub(crate) fn check_interrupts_now(&mut self) -> Result<()> {
        if self.session.cancel.take() {
            return Err(self.session.cancel.error());
        }
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(Error::new(sqlstate::QUERY_CANCELED, "canceling statement due to statement timeout"));
        }
        Ok(())
    }

    /// The memory budget of one operator's buffers (`work_mem`).
    pub(crate) fn work_mem(&self) -> usize {
        self.session.work_mem
    }

    /// The budget of hash tables: twice `work_mem`, like PostgreSQL's
    /// default `hash_mem_multiplier`.
    pub(crate) fn hash_mem(&self) -> usize {
        self.work_mem().saturating_mul(2)
    }

    pub(crate) fn temp_space(&self) -> Arc<crate::spill::TempSpace> {
        self.shared.temp_space()
    }

    /// Records a detail about a plan node for `EXPLAIN ANALYZE`.
    pub(crate) fn note(&mut self, plan: &Plan, line: String) {
        if let Some(notes) = &mut self.session.plan_notes {
            notes.entry(plan as *const Plan as usize).or_default().push(line);
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
                    // Beyond the budget the rows stay in a temporary file
                    // and each probe scans them, like PostgreSQL's plain
                    // (not hashed) subplan.
                    let budget = self.hash_mem();
                    let mut node = open(&sub.plan, self)?;
                    let mut rows = RowStore::new(self);
                    let mut keys = Some(HashSet::new());
                    let (mut has_null, mut used) = (false, 0);
                    while let Some(r) = node.next(self)? {
                        self.check_interrupts()?;
                        if r.iter().any(Value::is_null) {
                            has_null = true;
                        } else if let Some(k) = &mut keys {
                            let key = encode_key_tuple(&r);
                            used += key.len() + ENTRY_OVERHEAD;
                            k.insert(key);
                            if used > budget {
                                keys = None;
                            }
                        }
                        rows.push(r, self)?;
                    }
                    drop(node);
                    SubCache::Set { keys, has_null, rows: rows.finish()? }
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
                let hit = if op.is_none() && !*all && keys.is_some() {
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
                        Some(keys.as_ref().is_some_and(|k| k.contains(&encode_key_tuple(&lvals))))
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
        rows: &Rc<Stored>,
        row: &[Value],
    ) -> Result<Value> {
        let lvals = left.iter().map(|e| e.eval(row, self)).collect::<Result<Vec<_>>>()?;
        let fn_ctx = self.fn_ctx.clone();
        let test = |r: &[Value]| -> Result<Option<bool>> {
            match op {
                Some(op) => {
                    if lvals[0].is_null() || r[0].is_null() {
                        return Ok(None);
                    }
                    Ok((op.eval)(&fn_ctx, &[lvals[0].clone(), r[0].clone()])?.as_bool())
                }
                None if lvals.len() == 1 => Ok(sql_eq(&lvals[0], &r[0])),
                None => Ok(sql_eq(&Value::Record(lvals.clone()), &Value::Record(r.to_vec()))),
            }
        };
        if let Stored::Mem(rows) = &**rows {
            return quantified(rows.iter(), all, |r| test(r));
        }
        let mut it = rows.iter()?;
        let mut unknown = false;
        while let Some(r) = it.next_row()? {
            self.check_interrupts()?;
            match test(&r)? {
                Some(b) if b != all => return Ok(Value::Bool(!all)),
                Some(_) => {}
                None => unknown = true,
            }
        }
        // No row decided it: as `quantified` does for rows in memory.
        Ok(if unknown { Value::Null } else { Value::Bool(all) })
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
                let rows = store(&sub.plan, self)?;
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
        cx.check_interrupts()?;
        rows.push(r);
    }
    Ok(rows)
}

/// Runs a plan into a [`RowStore`].
fn store(plan: &Plan, cx: &mut ExecCx) -> Result<Rc<Stored>> {
    let mut node = open(plan, cx)?;
    let mut out = RowStore::new(cx);
    while let Some(r) = node.next(cx)? {
        cx.check_interrupts()?;
        out.push(r, cx)?;
    }
    out.finish()
}

struct VecNode(std::vec::IntoIter<Row>);

impl Node for VecNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        Ok(self.0.next())
    }
}

pub(crate) fn rows<'p>(v: Vec<Row>) -> Box<dyn Node + 'p> {
    Box::new(VecNode(v.into_iter()))
}

/// Shared rows of a CTE, handed out by cloning.
struct StoredNode(StoredIter);

impl Node for StoredNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        self.0.next_row()
    }
}

/// Rows read back from a temporary file.
pub(crate) struct SpillNode(pub(crate) SpillReader);

impl Node for SpillNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        self.0.next_row()
    }
}

/// The output of a sort.
pub(crate) struct SortedNode(pub(crate) Sorted);

impl Node for SortedNode {
    fn next(&mut self, _: &mut ExecCx) -> Result<Option<Row>> {
        Ok(self.0.next()?.map(|(_, row)| row))
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
        // A columnar table is its delta followed by its segments.
        Plan::Scan(scan) if scan.table.columnar.is_some() => Box::new(crate::columnar::ColumnarScan::open(scan, cx)?),
        Plan::Scan(scan) => Box::new(ScanNode::open(scan, cx)?),
        Plan::ColumnarAgg { scan, groups, aggs, fallback } => match crate::columnar::aggregate(scan, groups, aggs, cx)?
        {
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
            let outer_right = matches!(kind, JoinKind::Right | JoinKind::Full);
            // RIGHT and FULL joins read the right side once and remember
            // which of its rows found a partner.
            let rescan = (*lateral || *index_join) && !outer_right;
            let inner = if rescan { None } else { Some(store(right, cx)?) };
            let rmatched = match (&inner, outer_right) {
                (Some(i), true) => vec![false; i.len() as usize],
                _ => Vec::new(),
            };
            Box::new(NestedLoopNode {
                left: open(left, cx)?,
                left_plan: left,
                right,
                kind: *kind,
                on: on.as_ref(),
                index_join: *index_join,
                right_width: *right_width,
                inner,
                current: None,
                rmatched,
                left_width: None,
                tail: None,
            })
        }
        // Without keys every row lands in one bucket: that is a nested
        // loop, which can keep its inner side on disk.
        Plan::HashJoin { left, right, kind, left_keys, filter, left_width, right_width, .. }
            if left_keys.is_empty() =>
        {
            let inner = store(right, cx)?;
            let outer_right = matches!(kind, JoinKind::Right | JoinKind::Full);
            Box::new(NestedLoopNode {
                left: open(left, cx)?,
                left_plan: left,
                right,
                kind: *kind,
                on: filter.as_ref(),
                index_join: false,
                right_width: *right_width,
                rmatched: if outer_right { vec![false; inner.len() as usize] } else { Vec::new() },
                inner: Some(inner),
                current: None,
                left_width: Some(*left_width),
                tail: None,
            })
        }
        Plan::HashJoin { left, right, kind, left_keys, right_keys, filter, left_width, right_width } => {
            Box::new(HashJoinNode::open(
                plan,
                left,
                right,
                *kind,
                left_keys,
                right_keys,
                filter.as_ref(),
                *left_width,
                *right_width,
                cx,
            )?)
        }
        Plan::Aggregate { input, groups, aggs } => crate::agg::aggregate(plan, input, groups, aggs, cx)?,
        Plan::Window { input, calls } => crate::agg::window(plan, input, calls, cx)?,
        Plan::Sort { input, keys } => {
            let mut sorter = Sorter::new(sort_orders(keys), cx);
            let mut node = open(input, cx)?;
            while let Some(row) = node.next(cx)? {
                cx.check_interrupts()?;
                let mut k = Vec::with_capacity(keys.len());
                for key in keys {
                    k.push(key.expr.eval(&row, cx)?);
                }
                sorter.push(k, row, cx)?;
            }
            drop(node);
            let (sorted, stats) = sorter.finish(cx)?;
            cx.note(plan, stats.describe());
            Box::new(SortedNode(sorted))
        }
        Plan::Distinct { input, on } => Box::new(DistinctNode::new(plan, open(input, cx)?, on.as_deref(), cx)),
        Plan::SetOp { kind, all, left, right } => match (kind, all) {
            (SetOpKind::Union, true) => Box::new(ChainNode { first: Some(open(left, cx)?), second: open(right, cx)? }),
            (SetOpKind::Union, false) => {
                let both = Box::new(ChainNode { first: Some(open(left, cx)?), second: open(right, cx)? });
                Box::new(DistinctNode::new(plan, both, None, cx))
            }
            _ => Box::new(SetOpNode::open(plan, *kind, *all, left, right, cx)?),
        },
        Plan::With { ctes, body } => {
            for cte in ctes {
                let result = match &cte.recursive {
                    None => store(&cte.plan, cx)?,
                    Some((step, union_all)) => recursive_cte(cte.id, &cte.plan, step, *union_all, cx)?,
                };
                cx.ctes.insert(cte.id, result);
            }
            open(body, cx)?
        }
        Plan::CteScan(id) => {
            let data =
                cx.ctes.get(id).cloned().ok_or_else(|| Error::internal("CTE referenced before it was computed"))?;
            Box::new(StoredNode(data.iter()?))
        }
        Plan::FuncScan { func: Srf::GenerateSeries, args, ordinality } => {
            let vals = args.iter().map(|e| e.eval(&[], cx)).collect::<Result<Vec<_>>>()?;
            Box::new(SeriesNode { series: Series::new(&vals, cx)?, ordinality: ordinality.then_some(0) })
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
            cx.check_interrupts()?;
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
            cx.check_interrupts()?;
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

/// Memory, batches and disk use of a hash operator, for `EXPLAIN ANALYZE`.
#[derive(Default)]
struct HashStats {
    batches: usize,
    peak: usize,
    disk: u64,
}

impl HashStats {
    fn track(&mut self, used: usize) {
        self.peak = self.peak.max(used);
    }

    fn add_files(&mut self, files: &[Option<Rc<SpillFile>>]) {
        self.disk += files.iter().flatten().map(|f| f.bytes()).sum::<u64>();
    }

    fn describe(&self) -> String {
        let mut s = format!("Batches: {}  Memory Usage: {}kB", self.batches.max(1), self.peak.div_ceil(1024));
        if self.disk > 0 {
            s.push_str(&format!("  Disk Usage: {}kB", self.disk.div_ceil(1024)));
        }
        s
    }
}

fn empty_node<'p>() -> Box<dyn Node + 'p> {
    rows(Vec::new())
}

/// DISTINCT: whole-row (hashed, spilling to partitions beyond its budget)
/// or `DISTINCT ON` over sorted input.
struct DistinctNode<'p> {
    plan: &'p Plan,
    input: Box<dyn Node + 'p>,
    on: Option<&'p [Expr]>,
    seen: HashSet<Vec<u8>>,
    last: Option<Vec<u8>>,
    used: usize,
    budget: usize,
    /// Partitioning level of the current batch.
    level: u32,
    /// Rows of keys not seen before, once `seen` is full.
    spill: Option<Partitions>,
    /// Partitions still to be deduplicated, with their level.
    pending: Vec<(Rc<SpillFile>, u32)>,
    stats: HashStats,
}

impl<'p> DistinctNode<'p> {
    fn new(plan: &'p Plan, input: Box<dyn Node + 'p>, on: Option<&'p [Expr]>, cx: &ExecCx) -> DistinctNode<'p> {
        DistinctNode {
            plan,
            input,
            on,
            seen: HashSet::new(),
            last: None,
            used: 0,
            budget: cx.hash_mem(),
            level: 0,
            spill: None,
            pending: Vec::new(),
            stats: HashStats::default(),
        }
    }

    /// Switches to the next spilled partition; false when there is none.
    fn next_batch(&mut self, cx: &mut ExecCx) -> Result<bool> {
        if let Some(parts) = self.spill.take() {
            let files = parts.finish()?;
            self.stats.add_files(&files);
            self.pending.extend(files.into_iter().flatten().map(|f| (f, self.level + 1)));
        }
        let Some((file, level)) = self.pending.pop() else {
            if self.on.is_none() {
                cx.note(self.plan, self.stats.describe());
            }
            return Ok(false);
        };
        self.input = Box::new(SpillNode(file.reader()?));
        self.level = level;
        self.seen = HashSet::new();
        self.used = 0;
        self.stats.batches += 1;
        Ok(true)
    }
}

impl Node for DistinctNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            let Some(row) = self.input.next(cx)? else {
                if self.on.is_none() && self.next_batch(cx)? {
                    continue;
                }
                return Ok(None);
            };
            match self.on {
                None => {
                    cx.check_interrupts()?;
                    let key = encode_key_tuple(&row);
                    if self.seen.contains(&key) {
                        continue;
                    }
                    if let Some(parts) = &mut self.spill {
                        parts.write(partition_of(&key, self.level), &row)?;
                        continue;
                    }
                    self.used += key.len() + ENTRY_OVERHEAD;
                    self.stats.track(self.used);
                    self.seen.insert(key);
                    if self.used > self.budget {
                        if self.level >= MAX_LEVEL {
                            return Err(out_of_memory("the hash table of DISTINCT", cx.work_mem()));
                        }
                        self.spill = Some(Partitions::new(cx));
                        self.stats.batches = self.stats.batches.max(1);
                    }
                    return Ok(Some(row));
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
    left_plan: &'p Plan,
    right: &'p Plan,
    kind: JoinKind,
    on: Option<&'p Expr>,
    index_join: bool,
    right_width: usize,
    /// Materialized right side when it does not depend on the left row.
    inner: Option<Rc<Stored>>,
    /// Left row being joined: `(row, right rows, position, matched)`.
    current: Option<(Row, StoredIter, usize, bool)>,
    /// RIGHT and FULL joins: which right rows found a partner.
    rmatched: Vec<bool>,
    /// Width of the left rows, once one was seen.
    left_width: Option<usize>,
    /// RIGHT and FULL joins after the left side ended: the right rows
    /// being checked for partners.
    tail: Option<(StoredIter, usize)>,
}

impl Node for NestedLoopNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if let Some((it, pos)) = &mut self.tail {
                while let Some(r) = it.next_row()? {
                    cx.check_interrupts()?;
                    *pos += 1;
                    if !self.rmatched[*pos - 1] {
                        // The left width is unknown when the left side is
                        // empty; the planner records only the right width.
                        let width = self.left_width.unwrap_or(0).max(plan_width(self.left_plan));
                        let mut row = vec![Value::Null; width];
                        row.extend(r);
                        return Ok(Some(row));
                    }
                }
                return Ok(None);
            }
            if self.current.is_none() {
                let Some(left) = self.left.next(cx)? else {
                    if matches!(self.kind, JoinKind::Right | JoinKind::Full) {
                        let inner = self.inner.clone().unwrap_or_else(Stored::empty);
                        self.tail = Some((inner.iter()?, 0));
                        continue;
                    }
                    return Ok(None);
                };
                self.left_width.get_or_insert(left.len());
                let right = match &self.inner {
                    Some(r) => r.clone(),
                    None => {
                        // Re-run the right side with the left row visible.
                        let stack = if self.index_join { &mut cx.nl_left } else { &mut cx.outer };
                        stack.push(left.clone());
                        let result = store(self.right, cx);
                        let stack = if self.index_join { &mut cx.nl_left } else { &mut cx.outer };
                        stack.pop();
                        result?
                    }
                };
                self.current = Some((left, right.iter()?, 0, false));
            }
            let (left, right, pos, matched) = self.current.as_mut().unwrap();
            while let Some(r) = right.next_cow()? {
                cx.check_interrupts()?;
                let row = concat(left, &r);
                *pos += 1;
                let ok = match self.on {
                    Some(on) => on.test(&row, cx)?,
                    None => true,
                };
                if !ok {
                    continue;
                }
                *matched = true;
                if let Some(m) = self.rmatched.get_mut(*pos - 1) {
                    *m = true;
                }
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
                JoinKind::Left | JoinKind::Full if !matched => return Ok(Some(pad(&left, self.right_width))),
                JoinKind::Anti if !matched => return Ok(Some(left)),
                _ => {}
            }
        }
    }
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

/// The partition of a join key; rows whose key has a NULL never match and
/// go to the first partition.
fn key_partition(key: &Option<Vec<u8>>, level: u32) -> usize {
    key.as_ref().map_or(0, |k| partition_of(k, level))
}

/// One pair of partitions of a grace hash join: build rows, probe rows,
/// and the level to split them at if the build rows are still too many.
type JoinBatch = (Option<Rc<SpillFile>>, Option<Rc<SpillFile>>, u32);

/// Hash join. The right side is loaded into a hash table; if it outgrows
/// its budget, both sides are split by a hash of the join key into
/// partitions on disk, which are then joined pair by pair (grace hash
/// join), splitting a pair again where needed.
struct HashJoinNode<'p> {
    plan: &'p Plan,
    left: Box<dyn Node + 'p>,
    build: Vec<Row>,
    table: HashMap<Vec<u8>, Vec<usize>>,
    kind: JoinKind,
    left_keys: &'p [Expr],
    right_keys: &'p [Expr],
    filter: Option<&'p Expr>,
    left_width: usize,
    right_width: usize,
    matched: Vec<bool>,
    /// Output rows of the current probe row, in reverse order.
    pending: Vec<Row>,
    drained: bool,
    budget: usize,
    batches: VecDeque<JoinBatch>,
    /// After partitioning: build rows whose key has a NULL. They match
    /// nothing, so they stay out of the partitions; RIGHT and FULL joins
    /// emit them at the end.
    null_rows: Option<RowStore>,
    null_tail: Option<StoredIter>,
    stats: HashStats,
}

impl<'p> HashJoinNode<'p> {
    /// Sends a build row to its partition, or aside if its key has a NULL.
    fn partition_build_row(&mut self, parts: &mut Partitions, row: Row, cx: &mut ExecCx) -> Result<()> {
        match join_key(self.right_keys, &row, cx)? {
            Some(k) => parts.write(partition_of(&k, 0), &row),
            None if matches!(self.kind, JoinKind::Right | JoinKind::Full) => {
                self.null_rows.get_or_insert_with(|| RowStore::new(cx)).push(row, cx)
            }
            None => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn open(
        plan: &'p Plan,
        left: &'p Plan,
        right: &'p Plan,
        kind: JoinKind,
        left_keys: &'p [Expr],
        right_keys: &'p [Expr],
        filter: Option<&'p Expr>,
        left_width: usize,
        right_width: usize,
        cx: &mut ExecCx,
    ) -> Result<HashJoinNode<'p>> {
        let mut node = HashJoinNode {
            plan,
            left: empty_node(),
            build: Vec::new(),
            table: HashMap::new(),
            kind,
            left_keys,
            right_keys,
            filter,
            left_width,
            right_width,
            matched: Vec::new(),
            pending: Vec::new(),
            drained: true,
            budget: cx.hash_mem(),
            batches: VecDeque::new(),
            null_rows: None,
            null_tail: None,
            stats: HashStats::default(),
        };
        let mut input = open(right, cx)?;
        let mut parts: Option<Partitions> = None;
        let mut used = 0;
        while let Some(r) = input.next(cx)? {
            cx.check_interrupts()?;
            if let Some(p) = &mut parts {
                node.partition_build_row(p, r, cx)?;
                continue;
            }
            used += row_size(&r) + ENTRY_OVERHEAD;
            node.stats.track(used);
            node.build.push(r);
            if used > node.budget {
                let mut p = Partitions::new(cx);
                for r in std::mem::take(&mut node.build) {
                    node.partition_build_row(&mut p, r, cx)?;
                }
                parts = Some(p);
            }
        }
        drop(input);
        match parts {
            None => {
                node.index_build(cx)?;
                node.left = open(left, cx)?;
                node.drained = false;
                node.stats.batches = 1;
            }
            Some(parts) => {
                let build = parts.finish()?;
                let mut probe = Partitions::new(cx);
                let mut input = open(left, cx)?;
                while let Some(r) = input.next(cx)? {
                    cx.check_interrupts()?;
                    let k = join_key(left_keys, &r, cx)?;
                    probe.write(key_partition(&k, 0), &r)?;
                }
                let probe = probe.finish()?;
                node.stats.add_files(&build);
                node.stats.add_files(&probe);
                for (b, p) in build.into_iter().zip(probe) {
                    if b.is_some() || p.is_some() {
                        node.batches.push_back((b, p, 1));
                    }
                }
            }
        }
        Ok(node)
    }

    /// Builds the hash table over `self.build`.
    fn index_build(&mut self, cx: &mut ExecCx) -> Result<()> {
        self.table = HashMap::with_capacity(self.build.len());
        for (i, r) in self.build.iter().enumerate() {
            if let Some(k) = join_key(self.right_keys, r, cx)? {
                self.table.entry(k).or_default().push(i);
            }
        }
        let outer = matches!(self.kind, JoinKind::Right | JoinKind::Full);
        self.matched = vec![false; if outer { self.build.len() } else { 0 }];
        Ok(())
    }

    /// Loads the next pair of partitions; false when all are done.
    fn next_batch(&mut self, cx: &mut ExecCx) -> Result<bool> {
        'batches: while let Some((build, probe, level)) = self.batches.pop_front() {
            self.build = Vec::new();
            self.table = HashMap::new();
            let mut used = 0;
            if let Some(b) = build {
                let mut reader = b.reader()?;
                drop(b);
                while let Some(r) = reader.next_row()? {
                    cx.check_interrupts()?;
                    used += row_size(&r) + ENTRY_OVERHEAD;
                    self.stats.track(used);
                    self.build.push(r);
                    if used <= self.budget {
                        continue;
                    }
                    // Still too large: split this pair again.
                    if level >= MAX_LEVEL {
                        return Err(out_of_memory("the hash table of a hash join", cx.work_mem()));
                    }
                    let mut sub = Partitions::new(cx);
                    for r in std::mem::take(&mut self.build)
                        .into_iter()
                        .map(Ok)
                        .chain(std::iter::from_fn(|| reader.next_row().transpose()))
                    {
                        let r = r?;
                        let k = join_key(self.right_keys, &r, cx)?;
                        sub.write(key_partition(&k, level), &r)?;
                    }
                    let mut psub = Partitions::new(cx);
                    if let Some(p) = probe {
                        let mut reader = p.reader()?;
                        while let Some(r) = reader.next_row()? {
                            cx.check_interrupts()?;
                            let k = join_key(self.left_keys, &r, cx)?;
                            psub.write(key_partition(&k, level), &r)?;
                        }
                    }
                    let (b, p) = (sub.finish()?, psub.finish()?);
                    self.stats.add_files(&b);
                    self.stats.add_files(&p);
                    for (b, p) in b.into_iter().zip(p).rev() {
                        if b.is_some() || p.is_some() {
                            self.batches.push_front((b, p, level + 1));
                        }
                    }
                    continue 'batches;
                }
            }
            self.index_build(cx)?;
            self.left = match probe {
                Some(p) => Box::new(SpillNode(p.reader()?)),
                None => empty_node(),
            };
            self.drained = false;
            self.stats.batches += 1;
            return Ok(true);
        }
        Ok(false)
    }
}

impl Node for HashJoinNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.pending.pop() {
                return Ok(Some(r));
            }
            if let Some(tail) = &mut self.null_tail {
                return Ok(tail.next_row()?.map(|r| {
                    let mut row = vec![Value::Null; self.left_width];
                    row.extend(r);
                    row
                }));
            }
            if self.drained {
                if self.next_batch(cx)? {
                    continue;
                }
                cx.note(self.plan, self.stats.describe());
                if let Some(nulls) = self.null_rows.take() {
                    self.null_tail = Some(nulls.finish()?.iter()?);
                    continue;
                }
                // Free the last batch now rather than when the node goes.
                self.build = Vec::new();
                self.table = HashMap::new();
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
                        cx.check_interrupts()?;
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

/// INTERSECT and EXCEPT (with or without ALL). The right side becomes a
/// table of `key → (count, emitted)`; left rows stream past it. If the
/// table outgrows its budget, it and the rest of the left side are split
/// into partitions by key, which are processed pair by pair.
struct SetOpNode<'p> {
    plan: &'p Plan,
    kind: SetOpKind,
    all: bool,
    left: Box<dyn Node + 'p>,
    state: HashMap<Vec<u8>, (u64, bool)>,
    used: usize,
    budget: usize,
    level: u32,
    /// After spilling: partitions of the state and of the left rows.
    spill: Option<(Partitions, Partitions)>,
    batches: Vec<JoinBatch>,
    stats: HashStats,
}

impl<'p> SetOpNode<'p> {
    fn open(
        plan: &'p Plan,
        kind: SetOpKind,
        all: bool,
        left: &'p Plan,
        right: &'p Plan,
        cx: &mut ExecCx,
    ) -> Result<SetOpNode<'p>> {
        let mut node = SetOpNode {
            plan,
            kind,
            all,
            left: empty_node(),
            state: HashMap::new(),
            used: 0,
            budget: cx.hash_mem(),
            level: 0,
            spill: None,
            batches: Vec::new(),
            stats: HashStats { batches: 1, ..HashStats::default() },
        };
        let mut input = open(right, cx)?;
        while let Some(row) = input.next(cx)? {
            cx.check_interrupts()?;
            let key = encode_key_tuple(&row);
            node.add(key, 1, false, cx)?;
        }
        drop(input);
        node.left = open(left, cx)?;
        Ok(node)
    }

    /// Adds right-side state for a key, spilling when the table is full.
    fn add(&mut self, key: Vec<u8>, count: u64, emitted: bool, cx: &mut ExecCx) -> Result<()> {
        if let Some((parts, _)) = &mut self.spill {
            let part = partition_of(&key, self.level);
            return parts.write(part, &[Value::Bytea(key), Value::Int8(count as i64), Value::Bool(emitted)]);
        }
        match self.state.get_mut(&key) {
            Some(e) => {
                e.0 += count;
                e.1 |= emitted;
            }
            None => {
                self.used += key.len() + ENTRY_OVERHEAD;
                self.stats.track(self.used);
                self.state.insert(key, (count, emitted));
                if self.used > self.budget {
                    self.start_spilling(cx)?;
                }
            }
        }
        Ok(())
    }

    fn start_spilling(&mut self, cx: &mut ExecCx) -> Result<()> {
        if self.level >= MAX_LEVEL {
            return Err(out_of_memory(&format!("the hash table of {:?}", self.kind).to_uppercase(), cx.work_mem()));
        }
        let mut parts = Partitions::new(cx);
        for (key, (count, emitted)) in std::mem::take(&mut self.state) {
            let part = partition_of(&key, self.level);
            parts.write(part, &[Value::Bytea(key), Value::Int8(count as i64), Value::Bool(emitted)])?;
        }
        self.used = 0;
        self.spill = Some((parts, Partitions::new(cx)));
        Ok(())
    }

    /// Ends the current batch and starts the next one; false when done.
    fn next_batch(&mut self, cx: &mut ExecCx) -> Result<bool> {
        if let Some((state, left)) = self.spill.take() {
            let (s, l) = (state.finish()?, left.finish()?);
            self.stats.add_files(&s);
            self.stats.add_files(&l);
            for (s, l) in s.into_iter().zip(l) {
                // Without left rows a partition produces nothing.
                if l.is_some() {
                    self.batches.push((s, l, self.level + 1));
                }
            }
        }
        let Some((state, left, level)) = self.batches.pop() else {
            cx.note(self.plan, self.stats.describe());
            return Ok(false);
        };
        self.state = HashMap::new();
        self.used = 0;
        self.level = level;
        self.stats.batches += 1;
        if let Some(state) = state {
            let mut reader = state.reader()?;
            while let Some(r) = reader.next_row()? {
                cx.check_interrupts()?;
                let mut r = r.into_iter();
                let (Some(Value::Bytea(key)), Some(Value::Int8(count)), Some(Value::Bool(emitted))) =
                    (r.next(), r.next(), r.next())
                else {
                    return Err(Error::internal("malformed set operation state"));
                };
                self.add(key, count as u64, emitted, cx)?;
            }
        }
        self.left = match left {
            Some(l) => Box::new(SpillNode(l.reader()?)),
            None => empty_node(),
        };
        Ok(true)
    }
}

impl Node for SetOpNode<'_> {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        loop {
            let Some(row) = self.left.next(cx)? else {
                if self.next_batch(cx)? {
                    continue;
                }
                return Ok(None);
            };
            cx.check_interrupts()?;
            let key = encode_key_tuple(&row);
            if let Some((_, left)) = &mut self.spill {
                left.write(partition_of(&key, self.level), &row)?;
                continue;
            }
            let entry = self.state.get_mut(&key);
            let keep = match (self.kind, self.all, entry) {
                (SetOpKind::Intersect, true, Some(e)) if e.0 > 0 => {
                    e.0 -= 1;
                    true
                }
                (SetOpKind::Intersect, false, Some(e)) if e.0 > 0 && !e.1 => {
                    e.1 = true;
                    true
                }
                (SetOpKind::Intersect, ..) => false,
                (SetOpKind::Except, true, Some(e)) if e.0 > 0 => {
                    e.0 -= 1;
                    false
                }
                (SetOpKind::Except, true, _) => true,
                (SetOpKind::Except, false, Some(e)) if e.0 > 0 || e.1 => false,
                (SetOpKind::Except, false, Some(e)) => {
                    e.1 = true;
                    true
                }
                (SetOpKind::Except, false, None) => {
                    // A left-only row: remember that it was emitted. The
                    // table may have to spill; the row is emitted anyway.
                    self.add(key, 0, true, cx)?;
                    true
                }
                (SetOpKind::Union, ..) => unreachable!(),
            };
            if keep {
                return Ok(Some(row));
            }
        }
    }
}

/// `WITH RECURSIVE`: runs the recursive term until it adds no rows.
fn recursive_cte(id: usize, base: &Plan, step: &Plan, union_all: bool, cx: &mut ExecCx) -> Result<Rc<Stored>> {
    let budget = cx.hash_mem();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut used = 0;
    let mut all = RowStore::new(cx);
    let mut working = RowStore::new(cx);
    let mut add = |row: Row, all: &mut RowStore, working: &mut RowStore, cx: &mut ExecCx| -> Result<()> {
        cx.check_interrupts()?;
        if !union_all {
            let key = encode_key_tuple(&row);
            if seen.contains(&key) {
                return Ok(());
            }
            used += key.len() + ENTRY_OVERHEAD;
            if used > budget {
                return Err(out_of_memory("the rows a recursive UNION has seen", cx.work_mem()));
            }
            seen.insert(key);
        }
        all.push(row.clone(), cx)?;
        working.push(row, cx)
    };
    let mut node = open(base, cx)?;
    while let Some(r) = node.next(cx)? {
        add(r, &mut all, &mut working, cx)?;
    }
    drop(node);
    loop {
        let rows = working.finish()?;
        if rows.is_empty() {
            break;
        }
        cx.ctes.insert(id, rows);
        working = RowStore::new(cx);
        let mut node = open(step, cx)?;
        while let Some(r) = node.next(cx)? {
            add(r, &mut all, &mut working, cx)?;
        }
    }
    all.finish()
}

/// Compares two rows of pre-computed sort key values.
pub(crate) fn compare_keys(a: &[Value], b: &[Value], keys: &[SortKey]) -> Ordering {
    for ((x, y), k) in a.iter().zip(b).zip(keys) {
        let ord = compare_ordered(x, y, SortOrder { desc: k.desc, nulls_first: k.nulls_first });
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(crate) fn sort_orders(keys: &[SortKey]) -> Vec<SortOrder> {
    keys.iter().map(|k| SortOrder { desc: k.desc, nulls_first: k.nulls_first }).collect()
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

/// The values of `generate_series`, produced one at a time.
enum Series {
    Done,
    Int { cur: i64, end: i64, step: i64, wide: bool },
    Numeric { cur: rust_decimal::Decimal, end: rust_decimal::Decimal, step: rust_decimal::Decimal },
    Time { cur: i64, end: i64, step: Interval, sign: i128, tz: bool, plus: crate::value::FuncImpl },
}

impl Series {
    fn new(args: &[Value], _cx: &mut ExecCx) -> Result<Series> {
        if args.iter().any(Value::is_null) {
            return Ok(Series::Done);
        }
        Ok(match (&args[0], &args[1]) {
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
                )?
                .eval;
                Series::Time { cur: start, end, step, sign, tz, plus }
            }
            (Value::Numeric(start), Value::Numeric(end)) => {
                let step = match args.get(2) {
                    Some(Value::Numeric(s)) => *s,
                    _ => rust_decimal::Decimal::ONE,
                };
                if step.is_zero() {
                    return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
                }
                Series::Numeric { cur: *start, end: *end, step }
            }
            (a, b) => {
                let (Some(start), Some(end)) = (a.as_i64(), b.as_i64()) else {
                    return Err(Error::internal("generate_series over unsupported type"));
                };
                let step = args.get(2).and_then(Value::as_i64).unwrap_or(1);
                if step == 0 {
                    return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
                }
                Series::Int { cur: start, end, step, wide: matches!(a, Value::Int8(_)) }
            }
        })
    }

    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Value>> {
        cx.check_interrupts()?;
        match self {
            Series::Done => Ok(None),
            Series::Int { cur, end, step, wide } => {
                if !((*step > 0 && *cur <= *end) || (*step < 0 && *cur >= *end)) {
                    return Ok(None);
                }
                let v = if *wide { Value::Int8(*cur) } else { Value::Int4(*cur as i32) };
                match cur.checked_add(*step) {
                    Some(n) => *cur = n,
                    None => *self = Series::Done,
                }
                Ok(Some(v))
            }
            Series::Numeric { cur, end, step } => {
                if !((step.is_sign_positive() && *cur <= *end) || (step.is_sign_negative() && *cur >= *end)) {
                    return Ok(None);
                }
                let v = Value::Numeric(*cur);
                *cur += *step;
                Ok(Some(v))
            }
            Series::Time { cur, end, step, sign, tz, plus } => {
                if !((*sign > 0 && *cur <= *end) || (*sign < 0 && *cur >= *end)) {
                    return Ok(None);
                }
                let v = if *tz { Value::TimestampTz(*cur) } else { Value::Timestamp(*cur) };
                match plus(&cx.fn_ctx, &[v.clone(), Value::Interval(Interval { ..*step })])? {
                    Value::Timestamp(t) | Value::TimestampTz(t) => *cur = t,
                    _ => *self = Series::Done,
                }
                Ok(Some(v))
            }
        }
    }
}

/// `generate_series` in FROM, streamed.
struct SeriesNode {
    series: Series,
    /// `WITH ORDINALITY`: the number of the last row.
    ordinality: Option<i64>,
}

impl Node for SeriesNode {
    fn next(&mut self, cx: &mut ExecCx) -> Result<Option<Row>> {
        let Some(v) = self.series.next(cx)? else { return Ok(None) };
        Ok(Some(match &mut self.ordinality {
            Some(n) => {
                *n += 1;
                vec![v, Value::Int8(*n)]
            }
            None => vec![v],
        }))
    }
}

fn srf_rows(func: &Srf, args: Vec<Value>, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let one = |v: Value| vec![v];
    Ok(match func {
        Srf::GenerateSeries => {
            let mut series = Series::new(&args, cx)?;
            let mut out = Vec::new();
            while let Some(v) = series.next(cx)? {
                out.push(one(v));
            }
            out
        }
        Srf::Unnest => {
            let lists: Vec<Vec<Value>> = args
                .into_iter()
                .map(|a| match a {
                    Value::Array(items) | Value::Vector(items) => items,
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
        Srf::GenerateSubscripts => match (args[0].array_items(), args[1].as_i64()) {
            (Some((items, lower)), Some(1)) => {
                (lower..lower + items.len() as i32).map(|i| one(Value::Int4(i))).collect()
            }
            _ => Vec::new(),
        },
    })
}

#[allow(dead_code)]
fn _assert_types(_: Duration) {}
