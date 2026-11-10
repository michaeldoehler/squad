//! Scatter-gather: a SELECT that runs on several shards and is combined
//! on the coordinator.
//!
//! Two shapes exist. A *concat* query returns rows that are complete on
//! their shard (no aggregation, or grouping that includes the
//! distribution column); the coordinator only sorts, removes duplicates
//! and applies LIMIT. A *merge* query aggregates across shards: each
//! shard computes partial aggregates per group, and the coordinator
//! combines them with a second query over the partial rows.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use pg_query::protobuf::{self as pb, a_const::Val, node::Node as N};
use squad::{sqlstate, Error, OutputColumn, Result, Type, Value};

use crate::route::{self, Env};
use crate::shard::Output;
use crate::tree::{self, deparse, deparse_expr, func_name, operand, parse_expr, res_target, select_node, unsupported, Operand};

/// Distinct values the coordinator holds at most for `count(DISTINCT …)`.
pub(crate) const DISTINCT_LIMIT: usize = 1_000_000;

/// Name of the table of partial rows in the coordinator's scratch database.
pub(crate) const PARTIAL_TABLE: &str = "__partial";

pub(crate) enum Scatter {
    Concat(Concat),
    Merge(Merge),
}

pub(crate) struct SortKey {
    index: usize,
    desc: bool,
    nulls_first: bool,
}

pub(crate) struct Concat {
    /// Statement for each shard.
    pub sql: String,
    /// Columns of the result; shards may return more (sort keys).
    visible: usize,
    sort: Vec<SortKey>,
    limit: Option<Operand>,
    offset: Option<Operand>,
    distinct: bool,
}

pub(crate) struct Merge {
    /// Statement for each shard: group values, then partial aggregates.
    pub sql: String,
    pub create_sql: String,
    pub insert_sql: String,
    /// Query over the partial rows that produces the result.
    pub merge_sql: String,
    group_cols: usize,
    /// Partial columns that hold arrays of distinct values.
    distinct_cols: Vec<usize>,
    pub columns: Vec<OutputColumn>,
}

fn string_node(s: &str) -> pb::Node {
    pb::Node { node: Some(N::String(pb::String { sval: s.to_string() })) }
}

fn column(name: &str) -> pb::Node {
    pb::Node { node: Some(N::ColumnRef(pb::ColumnRef { fields: vec![string_node(name)], location: -1 })) }
}

/// The name of a column reference without qualifier.
fn bare_name(n: &pb::Node) -> Option<&str> {
    match n.node.as_ref()? {
        N::ColumnRef(c) if c.fields.len() == 1 => tree::string_of(&c.fields[0]),
        _ => None,
    }
}

/// `ORDER BY 2`, `GROUP BY 1`: the position.
fn ordinal(n: &pb::Node) -> Option<usize> {
    match n.node.as_ref()? {
        N::AConst(c) => match c.val.as_ref()? {
            Val::Ival(i) if i.ival >= 1 => Some(i.ival as usize - 1),
            _ => None,
        },
        _ => None,
    }
}

fn target_value(n: &pb::Node) -> Option<&pb::Node> {
    match n.node.as_ref()? {
        N::ResTarget(t) => t.val.as_deref(),
        _ => None,
    }
}

fn across(what: &str) -> Error {
    unsupported(format!("{what} across shards")).with_hint(
        "Add a condition on the distribution column so that the query reads one shard, or group by the distribution column.",
    )
}

/// Whether an expression holds aggregates or window functions of its own
/// query level.
fn scan(expr: &pb::Node, aggs: &mut bool, windows: &mut bool) -> Result<()> {
    let mut copy = expr.clone();
    tree::walk(&mut copy, &mut |n| match n.node.as_ref() {
        Some(N::FuncCall(f)) => {
            if f.over.is_some() {
                *windows = true;
            } else if tree::is_aggregate(&func_name(f)) {
                *aggs = true;
            }
            Ok(false)
        }
        Some(N::SubLink(_)) => Ok(true),
        _ => Ok(false),
    })
}

pub(crate) fn plan(sel: &pb::SelectStmt, text: &str, env: &Env) -> Result<Scatter> {
    if sel.larg.is_some() || sel.rarg.is_some() {
        return Err(across("UNION, INTERSECT and EXCEPT"));
    }
    if sel.into_clause.is_some() {
        return Err(unsupported("SELECT INTO"));
    }
    if sel.distinct_clause.iter().any(|n| n.node.is_some()) {
        return Err(across("DISTINCT ON"));
    }
    if sel.limit_option == pb::LimitOption::WithTies as i32 {
        return Err(across("FETCH … WITH TIES"));
    }
    let (mut aggs, mut windows) = (false, !sel.window_clause.is_empty());
    for t in &sel.target_list {
        scan(t, &mut aggs, &mut windows)?;
    }
    for s in &sel.sort_clause {
        scan(s, &mut aggs, &mut windows)?;
    }
    if let Some(h) = &sel.having_clause {
        scan(h, &mut aggs, &mut windows)?;
    }
    if windows {
        return Err(across("window functions"));
    }
    let grouped = aggs || !sel.group_clause.is_empty() || sel.having_clause.is_some();
    if !grouped || groups_by_distribution_column(sel, env)? {
        plan_concat(sel, text, env)
    } else {
        plan_merge(sel, text, env)
    }
}

/// A group never spans shards when the distribution column is one of the
/// grouping expressions.
fn groups_by_distribution_column(sel: &pb::SelectStmt, env: &Env) -> Result<bool> {
    for g in &sel.group_clause {
        let expr = match ordinal(g) {
            Some(i) => match sel.target_list.get(i).and_then(target_value) {
                Some(e) => e,
                None => continue,
            },
            None => g,
        };
        if let Some(N::ColumnRef(c)) = expr.node.as_ref() {
            if route::is_dist_column(sel, c, env)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn limit_operand(n: &Option<Box<pb::Node>>, what: &str) -> Result<Option<Operand>> {
    let Some(n) = n else { return Ok(None) };
    if matches!(n.node.as_ref(), Some(N::AConst(c)) if c.isnull) {
        return Ok(None);
    }
    match operand(n) {
        Some(o) => Ok(Some(o)),
        None => Err(unsupported(format!("an expression as {what} across shards; use a constant or a parameter"))),
    }
}

fn plan_concat(sel: &pb::SelectStmt, text: &str, env: &Env) -> Result<Scatter> {
    let out = (env.describe)(text)?;
    let visible = out.len();
    let distinct = !sel.distinct_clause.is_empty();
    let mut shard = sel.clone();
    let mut sort = Vec::with_capacity(sel.sort_clause.len());
    let mut hidden = 0;
    for s in &sel.sort_clause {
        let Some(N::SortBy(sb)) = s.node.as_ref() else { return Err(Error::internal("malformed ORDER BY")) };
        if !sb.use_op.is_empty() || sb.sortby_dir == pb::SortByDir::SortbyUsing as i32 {
            return Err(across("ORDER BY … USING"));
        }
        let desc = sb.sortby_dir == pb::SortByDir::SortbyDesc as i32;
        let nulls_first = match pb::SortByNulls::try_from(sb.sortby_nulls) {
            Ok(pb::SortByNulls::SortbyNullsFirst) => true,
            Ok(pb::SortByNulls::SortbyNullsLast) => false,
            _ => desc,
        };
        let node = sb.node.as_deref().ok_or_else(|| Error::internal("malformed ORDER BY"))?;
        let by_name = || {
            let name = bare_name(node)?;
            let mut hits = out.iter().enumerate().filter(|(_, c)| c.name == name);
            let first = hits.next()?;
            hits.next().is_none().then_some(first.0)
        };
        let index = match ordinal(node).filter(|i| *i < visible).or_else(by_name) {
            Some(i) => i,
            None => {
                if distinct {
                    return Err(unsupported("ORDER BY expressions that are not in the select list of SELECT DISTINCT"));
                }
                shard.target_list.push(res_target(&format!("__sort{hidden}"), node.clone()));
                hidden += 1;
                visible + hidden - 1
            }
        };
        sort.push(SortKey { index, desc, nulls_first });
    }
    let limit = limit_operand(&sel.limit_count, "LIMIT")?;
    let offset = limit_operand(&sel.limit_offset, "OFFSET")?;
    shard.limit_offset = None;
    match (&sel.limit_count, &sel.limit_offset, &limit, &offset) {
        (Some(l), Some(o), Some(_), Some(_)) => {
            let sum = format!("({})::bigint + ({})::bigint", deparse_expr(l)?, deparse_expr(o)?);
            shard.limit_count = Some(Box::new(parse_expr(&sum)?));
        }
        (_, _, None, _) => {
            shard.limit_count = None;
            shard.limit_option = pb::LimitOption::Default as i32;
        }
        _ => {}
    }
    let sql = if hidden == 0 && offset.is_none() { text.to_string() } else { deparse(&select_node(shard))? };
    Ok(Scatter::Concat(Concat { sql, visible, sort, limit, offset, distinct }))
}

fn count_of(op: &Option<Operand>, params: &[Value], what: &str) -> Result<Option<usize>> {
    let Some(op) = op else { return Ok(None) };
    let n = match op.resolve(params)? {
        Value::Null => return Ok(None),
        Value::Int2(i) => *i as i64,
        Value::Int4(i) => *i as i64,
        Value::Int8(i) => *i,
        Value::Text(s) => s.trim().parse().map_err(|_| Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, format!("invalid {what}: \"{s}\"")))?,
        other => return Err(Error::new(sqlstate::DATATYPE_MISMATCH, format!("{what} must be an integer, not {}", other.type_of().name()))),
    };
    if n < 0 {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("{what} must not be negative")));
    }
    Ok(Some(n as usize))
}

impl Concat {
    pub(crate) fn finish(&self, outputs: Vec<Output>, params: &[Value]) -> Result<Output> {
        let mut columns = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for out in outputs {
            if columns.is_empty() {
                columns = out.columns;
            }
            rows.extend(out.rows);
        }
        columns.truncate(self.visible);
        if self.distinct {
            let mut seen = HashSet::new();
            rows.retain(|r| seen.insert(squad::encoding::encode_key_tuple(r.iter().take(self.visible))));
        }
        if !self.sort.is_empty() {
            rows.sort_by(|a, b| {
                for k in &self.sort {
                    let (x, y) = (&a[k.index], &b[k.index]);
                    let ord = match (x.is_null(), y.is_null()) {
                        (true, true) => Ordering::Equal,
                        (true, false) if k.nulls_first => Ordering::Less,
                        (true, false) => Ordering::Greater,
                        (false, true) if k.nulls_first => Ordering::Greater,
                        (false, true) => Ordering::Less,
                        (false, false) if k.desc => squad::types::compare(y, x),
                        (false, false) => squad::types::compare(x, y),
                    };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }
        let offset = count_of(&self.offset, params, "OFFSET")?.unwrap_or(0);
        if offset > 0 {
            rows.drain(..offset.min(rows.len()));
        }
        if let Some(limit) = count_of(&self.limit, params, "LIMIT")? {
            rows.truncate(limit);
        }
        for r in &mut rows {
            r.truncate(self.visible);
        }
        Ok(Output { columns, affected: rows.len() as u64, rows })
    }
}

/// How one aggregate of the query is split.
enum AggKind {
    Count,
    Sum,
    /// `min`, `max`, `bool_and`, `bool_or`, `bit_and`, `bit_or`: the same
    /// function combines partial results.
    Same(&'static str),
    Avg,
    CountDistinct,
}

struct Agg {
    kind: AggKind,
    /// Positions of this aggregate's partial columns.
    partials: Vec<usize>,
}

struct Rewriter<'a> {
    group_texts: &'a [String],
    aggs: Vec<Agg>,
    by_text: HashMap<String, usize>,
    /// Partial aggregate expressions for the shards.
    partials: Vec<pb::Node>,
}

impl Rewriter<'_> {
    fn register(&mut self, node: &pb::Node, f: &pb::FuncCall) -> Result<usize> {
        let text = deparse_expr(node)?;
        if let Some(i) = self.by_text.get(&text) {
            return Ok(*i);
        }
        let name = func_name(f);
        if !f.agg_order.is_empty() || f.agg_within_group {
            return Err(across(&format!("{name} with ORDER BY")));
        }
        let renamed = |to: &str| {
            let mut call = f.clone();
            call.funcname = vec![string_node(to)];
            pb::Node { node: Some(N::FuncCall(Box::new(call))) }
        };
        let (kind, partials) = match name.as_str() {
            "count" if f.agg_distinct => {
                if f.args.len() != 1 {
                    return Err(across("count(DISTINCT …) of several expressions"));
                }
                (AggKind::CountDistinct, vec![renamed("array_agg")])
            }
            "min" | "max" | "bool_and" | "bool_or" | "every" | "bit_and" | "bit_or" => {
                let same = match name.as_str() {
                    "min" => "min",
                    "max" => "max",
                    "bool_or" => "bool_or",
                    "bit_and" => "bit_and",
                    "bit_or" => "bit_or",
                    _ => "bool_and",
                };
                (AggKind::Same(same), vec![node.clone()])
            }
            _ if f.agg_distinct => return Err(across(&format!("{name}(DISTINCT …)"))),
            "count" => (AggKind::Count, vec![node.clone()]),
            "sum" => (AggKind::Sum, vec![node.clone()]),
            "avg" => (AggKind::Avg, vec![renamed("sum"), renamed("count")]),
            "approx_count_distinct" | "approx_percentile" => {
                return Err(unsupported(format!("{name} across shards unless the query groups by the distribution column")).with_hint(
                    "The sketches of different shards cannot be combined yet. Group by the distribution column, or restrict the query to one distribution key.",
                ))
            }
            _ => return Err(across(&format!("aggregate {name}"))),
        };
        let first = self.partials.len();
        self.partials.extend(partials);
        self.aggs.push(Agg { kind, partials: (first..self.partials.len()).collect() });
        self.by_text.insert(text, self.aggs.len() - 1);
        Ok(self.aggs.len() - 1)
    }

    /// Replaces aggregates by `__agg<n>` and grouping expressions by
    /// `__g<n>`; anything else that reads a column is an error.
    fn rewrite(&mut self, expr: &mut pb::Node) -> Result<()> {
        tree::walk(expr, &mut |n| {
            let agg = match n.node.as_ref() {
                Some(N::FuncCall(f)) if f.over.is_some() => return Err(across("window functions")),
                Some(N::FuncCall(f)) if tree::is_aggregate(&func_name(f)) => Some((**f).clone()),
                Some(N::SubLink(_)) => return Err(across("a subquery outside of WHERE in an aggregate query")),
                _ => None,
            };
            if let Some(f) = agg {
                let i = self.register(n, &f)?;
                *n = column(&format!("__agg{i}"));
                return Ok(true);
            }
            let comparable = matches!(
                n.node.as_ref(),
                Some(
                    N::ColumnRef(_)
                        | N::FuncCall(_)
                        | N::AExpr(_)
                        | N::TypeCast(_)
                        | N::CaseExpr(_)
                        | N::CoalesceExpr(_)
                        | N::MinMaxExpr(_)
                        | N::BoolExpr(_)
                        | N::NullTest(_)
                        | N::BooleanTest(_)
                        | N::AIndirection(_)
                )
            );
            if !comparable {
                return Ok(false);
            }
            let text = deparse_expr(n)?;
            if let Some(i) = self.group_texts.iter().position(|g| *g == text) {
                *n = column(&format!("__g{i}"));
                return Ok(true);
            }
            if matches!(n.node.as_ref(), Some(N::ColumnRef(_))) {
                return Err(Error::new(
                    sqlstate::GROUPING_ERROR,
                    format!("column \"{text}\" must appear in the GROUP BY clause or be used in an aggregate function"),
                ));
            }
            Ok(false)
        })
    }
}

fn plan_merge(sel: &pb::SelectStmt, text: &str, env: &Env) -> Result<Scatter> {
    let out = (env.describe)(text)?;
    if out.len() != sel.target_list.len() {
        return Err(across("SELECT * in an aggregate query"));
    }

    // Grouping expressions, with positions and output names resolved.
    let mut groups = Vec::with_capacity(sel.group_clause.len());
    for g in &sel.group_clause {
        if matches!(g.node.as_ref(), Some(N::GroupingSet(_))) {
            return Err(across("GROUPING SETS, ROLLUP and CUBE"));
        }
        let mut expr = g;
        if let Some(i) = ordinal(g) {
            expr = sel
                .target_list
                .get(i)
                .and_then(target_value)
                .ok_or_else(|| Error::new(sqlstate::INVALID_COLUMN_REFERENCE, format!("GROUP BY position {} is not in select list", i + 1)))?;
        } else if let Some(name) = bare_name(g) {
            let alias = sel.target_list.iter().find_map(|t| match t.node.as_ref() {
                Some(N::ResTarget(rt)) if rt.name == name => rt.val.as_deref(),
                _ => None,
            });
            if let Some(val) = alias {
                if !route::is_input_column(sel, name, env)? {
                    expr = val;
                }
            }
        }
        groups.push(expr.clone());
    }
    let group_texts: Vec<String> = groups.iter().map(deparse_expr).collect::<Result<_>>()?;

    let mut rw = Rewriter { group_texts: &group_texts, aggs: Vec::new(), by_text: HashMap::new(), partials: Vec::new() };
    let mut targets = Vec::with_capacity(sel.target_list.len());
    for t in &sel.target_list {
        let mut val = target_value(t).ok_or_else(|| Error::internal("malformed select list"))?.clone();
        rw.rewrite(&mut val)?;
        targets.push(val);
    }
    let mut having = sel.having_clause.as_deref().cloned();
    if let Some(h) = having.as_mut() {
        rw.rewrite(h)?;
    }
    let mut sort = sel.sort_clause.clone();
    for s in &mut sort {
        let Some(N::SortBy(sb)) = s.node.as_mut() else { return Err(Error::internal("malformed ORDER BY")) };
        if !sb.use_op.is_empty() {
            return Err(across("ORDER BY … USING"));
        }
        let Some(node) = sb.node.as_mut() else { continue };
        // An output name or position refers to the merged select list.
        let output = ordinal(node).is_some() || bare_name(node).is_some_and(|n| out.iter().any(|c| c.name == n));
        let is_group = output && deparse_expr(node).is_ok_and(|t| group_texts.contains(&t));
        if !output || is_group {
            rw.rewrite(node)?;
        }
    }
    let Rewriter { aggs, mut partials, .. } = rw;

    // The statement for the shards.
    let shard_sql = |partials: &[pb::Node]| -> Result<String> {
        let mut p = sel.clone();
        p.target_list = groups
            .iter()
            .enumerate()
            .map(|(i, g)| res_target(&format!("__g{i}"), g.clone()))
            .chain(partials.iter().enumerate().map(|(i, e)| res_target(&format!("__a{i}"), e.clone())))
            .collect();
        p.group_clause = groups.clone();
        p.having_clause = None;
        p.sort_clause = Vec::new();
        p.distinct_clause = Vec::new();
        p.limit_count = None;
        p.limit_offset = None;
        p.limit_option = pb::LimitOption::Default as i32;
        deparse(&select_node(p))
    };
    if groups.is_empty() && partials.is_empty() {
        return Err(across("HAVING without aggregates"));
    }
    let mut sql = shard_sql(&partials)?;
    let mut types = (env.describe)(&sql)?;
    // `sum(real)` adds in single precision; averages need double.
    let mut widened = false;
    for agg in &aggs {
        if let AggKind::Avg = agg.kind {
            let at = agg.partials[0];
            if types[groups.len() + at].ty == Type::Float4 {
                if let Some(N::FuncCall(f)) = partials[at].node.as_mut() {
                    if let Some(arg) = f.args.first_mut() {
                        *arg = parse_expr(&format!("({})::float8", deparse_expr(arg)?))?;
                        widened = true;
                    }
                }
            }
        }
    }
    if widened {
        sql = shard_sql(&partials)?;
        types = (env.describe)(&sql)?;
    }

    // The table of partial rows and the expressions that merge them.
    let mut distinct_cols = Vec::new();
    let mut merged = Vec::with_capacity(aggs.len());
    let scratch_type = |ty: &Type| -> Result<String> {
        match ty {
            Type::Unknown => Ok("text".into()),
            Type::Enum(_) | Type::Record | Type::Void => Err(across(&format!("grouping or aggregating values of type {}", ty.name()))),
            Type::Array(e) if matches!(**e, Type::Enum(_) | Type::Record) => {
                Err(across(&format!("grouping or aggregating values of type {}", ty.name())))
            }
            other => Ok(other.base().name()),
        }
    };
    let mut defs = Vec::with_capacity(types.len());
    for (i, c) in types.iter().enumerate() {
        defs.push((if i < groups.len() { format!("__g{i}") } else { format!("__a{}", i - groups.len()) }, scratch_type(&c.ty)?));
    }
    for agg in &aggs {
        let col = |k: usize| format!("__a{}", agg.partials[k]);
        let ty = |k: usize| &types[groups.len() + agg.partials[k]].ty;
        merged.push(match agg.kind {
            AggKind::Count => format!("coalesce(sum({}), 0)::bigint", col(0)),
            AggKind::Sum => format!("sum({})::{}", col(0), scratch_type(ty(0))?),
            AggKind::Same(f) => format!("{f}({})", col(0)),
            AggKind::Avg => match ty(0) {
                Type::Float4 | Type::Float8 => format!("(sum({})::float8 / nullif(sum({}), 0)::float8)", col(0), col(1)),
                Type::Int2 | Type::Int4 | Type::Int8 | Type::Numeric => {
                    format!("(sum({})::numeric / nullif(sum({}), 0)::numeric)", col(0), col(1))
                }
                other => return Err(across(&format!("avg of {}", other.name()))),
            },
            AggKind::CountDistinct => {
                let at = groups.len() + agg.partials[0];
                distinct_cols.push(at);
                defs[at].1 = "bigint".into();
                format!("coalesce(sum({}), 0)::bigint", col(0))
            }
        });
    }
    let merged: Vec<pb::Node> = merged.iter().map(|m| parse_expr(m)).collect::<Result<_>>()?;
    let place = |expr: &mut pb::Node| -> Result<()> {
        tree::walk(expr, &mut |n| {
            if let Some(i) = bare_name(n).and_then(|name| name.strip_prefix("__agg")).and_then(|i| i.parse::<usize>().ok()) {
                if let Some(m) = merged.get(i) {
                    *n = m.clone();
                    return Ok(true);
                }
            }
            Ok(false)
        })
    };

    let mut m = tree::parse_select(&format!("SELECT 1 FROM {PARTIAL_TABLE}"))?;
    m.target_list.clear();
    for (mut val, column) in targets.into_iter().zip(&out) {
        place(&mut val)?;
        let castable = !matches!(column.ty, Type::Unknown | Type::Enum(_) | Type::Record | Type::Void | Type::Oid | Type::Char)
            && !matches!(&column.ty, Type::Array(e) if matches!(**e, Type::Enum(_) | Type::Record));
        if castable {
            val = parse_expr(&format!("({})::{}", deparse_expr(&val)?, column.ty.name()))?;
        }
        m.target_list.push(res_target(&column.name, val));
    }
    m.group_clause = (0..groups.len()).map(|i| column(&format!("__g{i}"))).collect();
    if let Some(mut h) = having {
        place(&mut h)?;
        m.having_clause = Some(Box::new(h));
    }
    for s in &mut sort {
        place(s)?;
    }
    m.sort_clause = sort;
    m.distinct_clause = sel.distinct_clause.clone();
    m.limit_count = sel.limit_count.clone();
    m.limit_offset = sel.limit_offset.clone();
    m.limit_option = sel.limit_option;

    let create_sql = format!(
        "CREATE TABLE {PARTIAL_TABLE} ({})",
        defs.iter().map(|(n, t)| format!("{n} {t}")).collect::<Vec<_>>().join(", ")
    );
    let insert_sql = format!(
        "INSERT INTO {PARTIAL_TABLE} VALUES ({})",
        (1..=defs.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ")
    );
    Ok(Scatter::Merge(Merge {
        sql,
        create_sql,
        insert_sql,
        merge_sql: deparse(&select_node(m))?,
        group_cols: groups.len(),
        distinct_cols,
        columns: out,
    }))
}

impl Merge {
    /// The partial rows of all shards, ready for the scratch table:
    /// arrays of distinct values are replaced by their combined count,
    /// carried by the first row of each group.
    pub(crate) fn partial_rows(&self, outputs: Vec<Output>) -> Result<Vec<Vec<Value>>> {
        let mut rows: Vec<Vec<Value>> = outputs.into_iter().flat_map(|o| o.rows).collect();
        if self.distinct_cols.is_empty() {
            return Ok(rows);
        }
        let keys: Vec<Vec<u8>> = rows.iter().map(|r| squad::encoding::encode_key_tuple(r.iter().take(self.group_cols))).collect();
        let mut held = 0usize;
        for &col in &self.distinct_cols {
            let mut sets: HashMap<&[u8], HashSet<Vec<u8>>> = HashMap::new();
            for (row, key) in rows.iter().zip(&keys) {
                let set = sets.entry(key.as_slice()).or_default();
                if let Value::Array(items) = &row[col] {
                    for v in items.iter().filter(|v| !v.is_null()) {
                        if set.insert(squad::encoding::encode_key_tuple([v])) {
                            held += 1;
                            if held > DISTINCT_LIMIT {
                                return Err(Error::new(
                                    sqlstate::PROGRAM_LIMIT_EXCEEDED,
                                    format!("count(DISTINCT …) across shards is limited to {DISTINCT_LIMIT} distinct values"),
                                )
                                .with_hint("Group by the distribution column, or use approx_count_distinct on one distribution key."));
                            }
                        }
                    }
                }
            }
            let mut first = HashSet::new();
            for (row, key) in rows.iter_mut().zip(&keys) {
                let n = if first.insert(key.as_slice()) { sets.get(key.as_slice()).map_or(0, |s| s.len()) } else { 0 };
                row[col] = Value::Int8(n as i64);
            }
        }
        Ok(rows)
    }
}
