//! Binding of SELECT: FROM, grouping, windows, ordering, set operations
//! and common table expressions.

use pg_query::protobuf::{self as pb, a_const::Val, node::Node as N};

use super::expr::{node_kind, sort_key, srf, Typed};
use super::{name_list, node, opt_node, parse_sql, range_var_name, AggLevel, Binder, CteBinding, Scope};
use crate::catalog::Relation;
use crate::expr::Expr;
use crate::plan::{CteDef, JoinKind, LockWait, Plan, RowLock, ScanPlan, SetOpKind, SortKey};
use crate::types::Type;
use crate::value;
use crate::{sqlstate, Error, Result};

/// A column of a query result.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumn {
    pub name: String,
    pub ty: Type,
}

pub(crate) enum SortTarget {
    /// Refers to the n-th select-list entry.
    Output(usize),
    Expr(Expr),
}

fn int_const(n: &pb::Node) -> Option<i32> {
    match n.node.as_ref()? {
        N::AConst(c) if !c.isnull => match c.val.as_ref()? {
            Val::Ival(i) => Some(i.ival),
            _ => None,
        },
        _ => None,
    }
}

/// Postgres rejects a bare constant other than an output position as a
/// sort key.
fn reject_constant_sort_key(n: &pb::Node, is_position: bool) -> Result<()> {
    if !is_position && matches!(n.node.as_ref(), Some(N::AConst(_))) {
        return Err(Error::syntax("non-integer constant in ORDER BY"));
    }
    Ok(())
}

fn bare_name(n: &pb::Node) -> Option<&str> {
    match n.node.as_ref()? {
        N::ColumnRef(c) if c.fields.len() == 1 => match c.fields[0].node.as_ref()? {
            N::String(s) => Some(&s.sval),
            _ => None,
        },
        _ => None,
    }
}

/// The name of a column reference or function call, looking through casts
/// and subscripts; `None` for anything that only has a fallback name.
fn strong_name(n: &pb::Node) -> Option<String> {
    match n.node.as_ref()? {
        N::ColumnRef(_) | N::FuncCall(_) => Some(target_name(n)).filter(|s| s != "?column?"),
        N::TypeCast(c) => c.arg.as_ref().and_then(|a| strong_name(a)),
        N::CollateClause(c) => c.arg.as_ref().and_then(|a| strong_name(a)),
        N::AIndirection(i) => i.arg.as_ref().and_then(|a| strong_name(a)),
        N::CaseExpr(c) => c.defresult.as_ref().and_then(|d| strong_name(d)),
        _ => None,
    }
}

/// Column name Postgres derives for an unaliased select-list expression.
fn target_name(n: &pb::Node) -> String {
    match n.node.as_ref() {
        Some(N::ColumnRef(c)) => c.fields.last().and_then(|f| match f.node.as_ref() {
            Some(N::String(s)) => Some(s.sval.clone()),
            _ => None,
        }),
        Some(N::FuncCall(f)) => f.funcname.last().and_then(|f| match f.node.as_ref() {
            Some(N::String(s)) => Some(s.sval.clone()),
            _ => None,
        }),
        Some(N::TypeCast(c)) => {
            let inner = c.arg.as_ref().map(|a| target_name(a));
            match inner {
                Some(name) if name != "?column?" => Some(name),
                _ => c.type_name.as_ref().and_then(|t| t.names.last()).and_then(|f| match f.node.as_ref() {
                    Some(N::String(s)) => Some(s.sval.clone()),
                    _ => None,
                }),
            }
        }
        Some(N::CollateClause(c)) => c.arg.as_ref().map(|a| target_name(a)),
        Some(N::AIndirection(i)) => i.arg.as_ref().map(|a| target_name(a)),
        // A CASE takes the name of its ELSE branch if that is a column or
        // a function call (possibly under a cast).
        Some(N::CaseExpr(c)) => {
            Some(c.defresult.as_ref().and_then(|d| strong_name(d)).unwrap_or_else(|| "case".into()))
        }
        Some(N::CoalesceExpr(_)) => Some("coalesce".into()),
        Some(N::AArrayExpr(_)) => Some("array".into()),
        Some(N::RowExpr(_)) => Some("row".into()),
        Some(N::MinMaxExpr(m)) => {
            Some(if m.op == pb::MinMaxOp::IsGreatest as i32 { "greatest" } else { "least" }.into())
        }
        Some(N::AExpr(e)) if e.kind == pb::AExprKind::AexprNullif as i32 => Some("nullif".into()),
        Some(N::SubLink(s)) => match pb::SubLinkType::try_from(s.sub_link_type) {
            Ok(pb::SubLinkType::ExistsSublink) => Some("exists".into()),
            Ok(pb::SubLinkType::ArraySublink) => Some("array".into()),
            Ok(pb::SubLinkType::ExprSublink) => match opt_node(&s.subselect) {
                Some(N::SelectStmt(sel)) => sel.target_list.first().map(|t| match t.node.as_ref() {
                    Some(N::ResTarget(r)) if !r.name.is_empty() => r.name.clone(),
                    Some(N::ResTarget(r)) => r.val.as_ref().map_or("?column?".into(), |v| target_name(v)),
                    _ => "?column?".into(),
                }),
                _ => None,
            },
            _ => None,
        },
        Some(N::SqlvalueFunction(f)) => {
            use pb::SqlValueFunctionOp as Op;
            Some(
                match Op::try_from(f.op).unwrap_or(Op::SqlvalueFunctionOpUndefined) {
                    Op::SvfopCurrentDate => "current_date",
                    Op::SvfopCurrentTime | Op::SvfopCurrentTimeN => "current_time",
                    Op::SvfopCurrentTimestamp | Op::SvfopCurrentTimestampN => "current_timestamp",
                    Op::SvfopLocaltime | Op::SvfopLocaltimeN => "localtime",
                    Op::SvfopLocaltimestamp | Op::SvfopLocaltimestampN => "localtimestamp",
                    Op::SvfopCurrentRole => "current_role",
                    Op::SvfopCurrentUser => "current_user",
                    Op::SvfopUser => "user",
                    Op::SvfopSessionUser => "session_user",
                    Op::SvfopCurrentCatalog => "current_catalog",
                    Op::SvfopCurrentSchema => "current_schema",
                    _ => "?column?",
                }
                .into(),
            )
        }
        _ => None,
    }
    .unwrap_or_else(|| "?column?".into())
}

fn output_scope(out: &[OutputColumn]) -> Scope {
    Scope::relation("", out.iter().map(|c| (c.name.clone(), c.ty.clone(), false)).collect(), None).unqualified()
}

impl Scope {
    /// Joins two FROM scopes, rejecting a table name or alias used twice.
    fn join_distinct(self, other: Scope) -> Result<Scope> {
        // Subqueries without an alias share a placeholder name.
        let clash = other.rels.iter().find(|r| r.alias != "subquery" && self.rels.iter().any(|l| l.alias == r.alias));
        if let Some(r) = clash {
            return Err(Error::new(
                sqlstate::DUPLICATE_ALIAS,
                format!("table name \"{}\" specified more than once", r.alias),
            ));
        }
        Ok(self.join(other))
    }

    fn unqualified(mut self) -> Scope {
        for c in &mut self.cols {
            c.table = None;
        }
        self.rels.clear();
        self
    }
}

impl<'a> Binder<'a> {
    /// Binds a top-level query. Untyped output columns become `text`.
    pub fn bind_query(&mut self, sel: &pb::SelectStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let (plan, mut out) = self.bind_select(sel)?;
        for c in &mut out {
            if c.ty == Type::Unknown {
                c.ty = Type::Text;
            }
        }
        Ok((plan, out))
    }

    /// Binds a subquery that may refer to the columns of `scope`. Returns
    /// whether it actually does (or refers to anything further out).
    pub(crate) fn bind_subquery(
        &mut self,
        sel: &pb::SelectStmt,
        scope: &Scope,
    ) -> Result<(Plan, Vec<OutputColumn>, bool)> {
        self.outer_scopes.push(scope.clone());
        self.correlated.push(false);
        let clause = self.clause;
        let result = self.bind_select(sel);
        self.clause = clause;
        let correlated = self.correlated.pop().unwrap_or(false);
        self.outer_scopes.pop();
        let (plan, out) = result?;
        Ok((plan, out, correlated))
    }

    pub(crate) fn bind_select(&mut self, sel: &pb::SelectStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let cte_mark = self.ctes.len();
        // Expressions of a nested query are top-level within that query.
        let depth = std::mem::take(&mut self.expr_depth);
        self.select_depth += 1;
        let result = (|| {
            let defs = match &sel.with_clause {
                Some(with) => self.bind_with(with)?,
                None => Vec::new(),
            };
            let op = pb::SetOperation::try_from(sel.op).unwrap_or(pb::SetOperation::SetopNone);
            let (plan, out) = if !matches!(op, pb::SetOperation::SetopNone | pb::SetOperation::Undefined) {
                self.bind_set_operation(sel, op)?
            } else if !sel.values_lists.is_empty() {
                self.bind_values(sel)?
            } else {
                self.bind_simple_select(sel)?
            };
            Ok((if defs.is_empty() { plan } else { Plan::With { ctes: defs, body: Box::new(plan) } }, out))
        })();
        self.select_depth -= 1;
        self.expr_depth = depth;
        self.ctes.truncate(cte_mark);
        result
    }

    pub(crate) fn bind_with(&mut self, with: &pb::WithClause) -> Result<Vec<CteDef>> {
        let mut defs = Vec::with_capacity(with.ctes.len());
        for c in &with.ctes {
            let N::CommonTableExpr(cte) = node(c)? else { return Err(Error::internal("malformed WITH")) };
            let query = cte.ctequery.as_ref().ok_or_else(|| Error::internal("CTE without query"))?;
            let col_names = name_list(&cte.aliascolnames)?;
            if with.ctes[..defs.len()]
                .iter()
                .any(|d| matches!(d.node.as_ref(), Some(N::CommonTableExpr(o)) if o.ctename == cte.ctename))
            {
                return Err(Error::new(
                    sqlstate::DUPLICATE_ALIAS,
                    format!("WITH query name \"{}\" specified more than once", cte.ctename),
                ));
            }
            let id = self.next_cte_id;
            self.next_cte_id += 1;
            let rename = |out: &mut Vec<OutputColumn>| {
                for (c, n) in out.iter_mut().zip(&col_names) {
                    c.name = n.clone();
                }
                for c in out.iter_mut() {
                    if c.ty == Type::Unknown {
                        c.ty = Type::Text;
                    }
                }
            };
            let binding = |name: &str, out: &[OutputColumn]| CteBinding {
                name: name.to_string(),
                id,
                cols: out.iter().map(|c| (c.name.clone(), c.ty.clone())).collect(),
            };

            let def = match node(query)? {
                N::SelectStmt(sel)
                    if with.recursive
                        && sel.op == pb::SetOperation::SetopUnion as i32
                        && sel.larg.is_some()
                        && sel.rarg.is_some()
                        && sel.with_clause.is_none() =>
                {
                    let (base, mut out) = self.bind_select(sel.larg.as_ref().unwrap())?;
                    rename(&mut out);
                    self.ctes.push(binding(&cte.ctename, &out));
                    let (step, step_out) = self.bind_select(sel.rarg.as_ref().unwrap())?;
                    if step_out.len() != out.len() {
                        return Err(Error::syntax("each UNION query must have the same number of columns"));
                    }
                    if self.cte_refs.contains(&id) {
                        // The recursive term must produce the base term's types.
                        let mut casts = Vec::with_capacity(out.len());
                        let mut needed = false;
                        for (i, (b, s)) in out.iter().zip(&step_out).enumerate() {
                            if b.ty == s.ty {
                                casts.push(Expr::Column(i));
                            } else if s.ty == Type::Unknown
                                || value::can_cast(&s.ty, &b.ty, value::CastContext::Implicit)
                            {
                                needed = true;
                                casts.push(self.coerce(Expr::Column(i), &s.ty, &b.ty)?);
                            } else {
                                return Err(Error::type_mismatch(format!(
                                    "recursive query \"{}\" column {} has type {} in non-recursive term but type {} overall",
                                    cte.ctename,
                                    i + 1,
                                    b.ty,
                                    s.ty
                                ))
                                .with_hint("Cast the output of the non-recursive term to the correct type."));
                            }
                        }
                        let step = if needed { Plan::Project(Box::new(step), casts) } else { step };
                        CteDef { id, name: cte.ctename.clone(), plan: base, recursive: Some((step, sel.all)) }
                    } else {
                        // Not self-referential after all: an ordinary UNION.
                        let (plan, mut out2) =
                            self.make_set_op(SetOpKind::Union, sel.all, base, out, step, step_out)?;
                        rename(&mut out2);
                        self.ctes.pop();
                        self.ctes.push(binding(&cte.ctename, &out2));
                        CteDef { id, name: cte.ctename.clone(), plan, recursive: None }
                    }
                }
                N::SelectStmt(sel) => {
                    let (plan, mut out) = self.bind_select(sel)?;
                    rename(&mut out);
                    self.ctes.push(binding(&cte.ctename, &out));
                    CteDef { id, name: cte.ctename.clone(), plan, recursive: None }
                }
                stmt @ (N::InsertStmt(_) | N::UpdateStmt(_) | N::DeleteStmt(_)) => {
                    let (plan, mut out) = self.bind_dml(stmt)?;
                    rename(&mut out);
                    self.ctes.push(binding(&cte.ctename, &out));
                    CteDef { id, name: cte.ctename.clone(), plan, recursive: None }
                }
                other => return Err(Error::unsupported(format!("{} in WITH", node_kind(other)))),
            };
            defs.push(def);
        }
        Ok(defs)
    }

    /// Combines two bound queries with UNION / INTERSECT / EXCEPT.
    fn make_set_op(
        &mut self,
        kind: SetOpKind,
        all: bool,
        left: Plan,
        left_out: Vec<OutputColumn>,
        right: Plan,
        right_out: Vec<OutputColumn>,
    ) -> Result<(Plan, Vec<OutputColumn>)> {
        let what = match kind {
            SetOpKind::Union => "UNION",
            SetOpKind::Intersect => "INTERSECT",
            SetOpKind::Except => "EXCEPT",
        };
        if left_out.len() != right_out.len() {
            return Err(Error::syntax(format!("each {what} query must have the same number of columns")));
        }
        let mut out = Vec::with_capacity(left_out.len());
        let (mut lcast, mut rcast) = (Vec::new(), Vec::new());
        let (mut lneed, mut rneed) = (false, false);
        for (i, (l, r)) in left_out.iter().zip(&right_out).enumerate() {
            let ty = if l.ty == r.ty { l.ty.clone() } else { value::common_type(&[l.ty.base(), r.ty.base()], what)? };
            lneed |= l.ty != ty && l.ty != Type::Unknown;
            rneed |= r.ty != ty && r.ty != Type::Unknown;
            lcast.push(if l.ty == Type::Unknown { Expr::Column(i) } else { self.coerce(Expr::Column(i), &l.ty, &ty)? });
            rcast.push(if r.ty == Type::Unknown { Expr::Column(i) } else { self.coerce(Expr::Column(i), &r.ty, &ty)? });
            // Untyped literals only need parsing when the common type is
            // not text; their runtime representation already is text.
            if l.ty == Type::Unknown && ty != Type::Text {
                lcast[i] = self.cast_expr(Expr::Column(i), &Type::Text, &ty)?;
                lneed = true;
            }
            if r.ty == Type::Unknown && ty != Type::Text {
                rcast[i] = self.cast_expr(Expr::Column(i), &Type::Text, &ty)?;
                rneed = true;
            }
            out.push(OutputColumn { name: l.name.clone(), ty });
        }
        let left = if lneed { Plan::Project(Box::new(left), lcast) } else { left };
        let right = if rneed { Plan::Project(Box::new(right), rcast) } else { right };
        Ok((Plan::SetOp { kind, all, left: Box::new(left), right: Box::new(right) }, out))
    }

    fn bind_set_operation(&mut self, sel: &pb::SelectStmt, op: pb::SetOperation) -> Result<(Plan, Vec<OutputColumn>)> {
        let (Some(larg), Some(rarg)) = (&sel.larg, &sel.rarg) else {
            return Err(Error::internal("set operation without operands"));
        };
        let (left, left_out) = self.bind_select(larg)?;
        let (right, right_out) = self.bind_select(rarg)?;
        let kind = match op {
            pb::SetOperation::SetopUnion => SetOpKind::Union,
            pb::SetOperation::SetopIntersect => SetOpKind::Intersect,
            _ => SetOpKind::Except,
        };
        let (plan, out) = self.make_set_op(kind, sel.all, left, left_out, right, right_out)?;
        if !sel.locking_clause.is_empty() {
            return Err(Error::unsupported("FOR UPDATE with UNION/INTERSECT/EXCEPT"));
        }
        let plan = self.finish_output_ordered(plan, &out, sel)?;
        Ok((plan, out))
    }

    fn bind_values(&mut self, sel: &pb::SelectStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let scope = Scope::default();
        let mut rows: Vec<Vec<Typed>> = Vec::with_capacity(sel.values_lists.len());
        for row in &sel.values_lists {
            let N::List(list) = node(row)? else { return Err(Error::internal("malformed VALUES")) };
            let mut bound = Vec::with_capacity(list.items.len());
            for item in &list.items {
                bound.push(self.bind_expr(item, &scope)?);
            }
            if let Some(first) = rows.first() {
                if first.len() != bound.len() {
                    return Err(Error::syntax("VALUES lists must all be the same length"));
                }
            }
            rows.push(bound);
        }
        let width = rows.first().map_or(0, Vec::len);
        let mut out = Vec::with_capacity(width);
        for col in 0..width {
            let types: Vec<Type> = rows.iter().map(|r| r[col].1.clone()).collect();
            // A column of only untyped literals stays untyped so that an
            // enclosing INSERT can give it the target column's type.
            let ty = if types.iter().all(|t| *t == Type::Unknown) {
                Type::Unknown
            } else {
                value::common_type(&types, "VALUES")?
            };
            out.push(OutputColumn { name: format!("column{}", col + 1), ty });
        }
        let mut plan_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut exprs = Vec::with_capacity(width);
            for ((e, t), col) in row.into_iter().zip(&out) {
                exprs.push(self.coerce(e, &t, &col.ty)?);
            }
            plan_rows.push(exprs);
        }
        let plan = self.finish_output_ordered(Plan::Values(plan_rows), &out, sel)?;
        Ok((plan, out))
    }

    /// ORDER BY / LIMIT over a finished result (set operations, VALUES):
    /// sort keys may only name output columns.
    fn finish_output_ordered(&mut self, mut plan: Plan, out: &[OutputColumn], sel: &pb::SelectStmt) -> Result<Plan> {
        if !sel.sort_clause.is_empty() {
            let scope = output_scope(out);
            let mut keys = Vec::with_capacity(sel.sort_clause.len());
            for s in &sel.sort_clause {
                let N::SortBy(sb) = node(s)? else { return Err(Error::internal("malformed ORDER BY")) };
                let target = sb.node.as_ref().ok_or_else(|| Error::internal("ORDER BY without expression"))?;
                reject_constant_sort_key(target, int_const(target).is_some())?;
                let expr = match int_const(target) {
                    Some(i) if i >= 1 && (i as usize) <= out.len() => Expr::Column(i as usize - 1),
                    Some(i) => {
                        return Err(Error::new(
                            sqlstate::INVALID_COLUMN_REFERENCE,
                            format!("ORDER BY position {i} is not in select list"),
                        ))
                    }
                    None => self.bind_expr(target, &scope)?.0,
                };
                keys.push(sort_key(expr, sb)?);
            }
            plan = Plan::Sort { input: Box::new(plan), keys };
        }
        self.bind_limit(plan, sel)
    }

    fn bind_limit(&mut self, plan: Plan, sel: &pb::SelectStmt) -> Result<Plan> {
        if sel.limit_option == pb::LimitOption::WithTies as i32 {
            return Err(Error::unsupported("FETCH FIRST ... WITH TIES"));
        }
        let scope = Scope::default();
        let bind = |b: &mut Self, n: &Option<Box<pb::Node>>, what: &'static str| -> Result<Option<Expr>> {
            let Some(n) = n else { return Ok(None) };
            if matches!(n.node.as_ref(), Some(N::AConst(c)) if c.isnull) {
                return Ok(None);
            }
            b.clause = what;
            let (e, t) = b.bind_expr(n, &scope)?;
            if !t.is_integer() && t != Type::Unknown {
                let e = b.explicit_cast(e, &t, &Type::Int8).map_err(|_| {
                    Error::type_mismatch(format!("argument of {what} must be type bigint, not type {t}"))
                })?;
                return Ok(Some(e));
            }
            Ok(Some(b.coerce(e, &t, &Type::Int8)?))
        };
        let limit = bind(self, &sel.limit_count, "LIMIT")?;
        let offset = bind(self, &sel.limit_offset, "OFFSET")?;
        Ok(if limit.is_some() || offset.is_some() {
            Plan::Limit { input: Box::new(plan), limit, offset }
        } else {
            plan
        })
    }

    fn bind_simple_select(&mut self, sel: &pb::SelectStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        if sel.into_clause.is_some() {
            return Err(Error::unsupported("SELECT INTO"));
        }
        let (plan, scope) = self.bind_from_list(&sel.from_clause)?;

        self.clause = "WHERE";
        let where_pred = sel.where_clause.as_ref().map(|w| self.bind_bool(w, &scope, "WHERE")).transpose()?;
        let mut plan = self.push_filter(plan, where_pred.clone(), &scope.types())?;

        let mut window_defs = Vec::new();
        for w in &sel.window_clause {
            if let N::WindowDef(d) = node(w)? {
                window_defs.push((**d).clone());
            }
        }
        self.agg_levels.push(AggLevel {
            window_defs,
            allow_aggs: true,
            allow_windows: true,
            allow_srfs: true,
            ..Default::default()
        });
        let bound = self.bind_select_clauses(sel, &scope);
        let level = self.agg_levels.pop().expect("aggregate level");
        let (mut targets, names, mut groups, having, mut sorts, distinct_on) = bound?;

        let is_agg = !groups.is_empty() || !level.aggs.is_empty() || having.is_some() || !sel.group_clause.is_empty();
        let plain_distinct = sel.distinct_clause.len() == 1 && sel.distinct_clause[0].node.is_none();
        if !sel.locking_clause.is_empty() && (is_agg || !sel.distinct_clause.is_empty() || !level.windows.is_empty()) {
            return Err(Error::new(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "FOR UPDATE is not allowed with GROUP BY, DISTINCT, aggregate or window functions",
            ));
        }

        let mut windows = level.windows;
        let mut srfs = level.srfs;
        let mut distinct_on = distinct_on;
        let mut width = scope.cols.len();
        if is_agg {
            // Collect every expression evaluated above the aggregation.
            let mut having = having;
            {
                let mut above: Vec<&mut Expr> = targets.iter_mut().map(|t| &mut t.0).collect();
                above.extend(having.iter_mut());
                above.extend(sorts.iter_mut().filter_map(|s| match &mut s.0 {
                    SortTarget::Expr(e) => Some(e),
                    SortTarget::Output(_) => None,
                }));
                for w in &mut windows {
                    above.extend(w.args.iter_mut());
                    above.extend(w.partition_by.iter_mut());
                    above.extend(w.order_by.iter_mut().map(|k| &mut k.expr));
                    above.extend(w.filter.iter_mut());
                }
                for (_, args) in &mut srfs {
                    above.extend(args.iter_mut());
                }
                above.extend(distinct_on.iter_mut().flatten());
                add_dependent_groups(&mut groups, &above, &scope);
                for e in above {
                    *e = post_agg(std::mem::replace(e, Expr::null()), &groups, &scope)?;
                }
            }
            width = groups.len() + level.aggs.len();
            plan = Plan::Aggregate { input: Box::new(plan), groups, aggs: level.aggs };
            if let Some(h) = having {
                plan = Plan::Filter(Box::new(plan), h);
            }
        }

        if !windows.is_empty() {
            let n = windows.len();
            plan = Plan::Window { input: Box::new(plan), calls: windows };
            let base = width;
            let mut fix = |e: &mut Expr| {
                *e = std::mem::replace(e, Expr::null())
                    .rewrite(&mut |n| Ok(if let Expr::WindowRef(i) = n { Some(Expr::Column(base + i)) } else { None }))
                    .expect("infallible");
            };
            targets.iter_mut().for_each(|t| fix(&mut t.0));
            for s in &mut sorts {
                if let SortTarget::Expr(e) = &mut s.0 {
                    fix(e);
                }
            }
            srfs.iter_mut().flat_map(|(_, a)| a.iter_mut()).for_each(&mut fix);
            distinct_on.iter_mut().flatten().for_each(&mut fix);
            width += n;
        }

        if !srfs.is_empty() {
            let n = srfs.len();
            plan = Plan::ProjectSet { input: Box::new(plan), funcs: srfs };
            let base = width;
            let mut fix = |e: &mut Expr| {
                *e = std::mem::replace(e, Expr::null())
                    .rewrite(&mut |n| Ok(if let Expr::SrfRef(i) = n { Some(Expr::Column(base + i)) } else { None }))
                    .expect("infallible");
            };
            targets.iter_mut().for_each(|t| fix(&mut t.0));
            for s in &mut sorts {
                if let SortTarget::Expr(e) = &mut s.0 {
                    fix(e);
                }
            }
            distinct_on.iter_mut().flatten().for_each(&mut fix);
            width += n;
        }
        let _ = width;

        let out: Vec<OutputColumn> =
            targets.iter().zip(names).map(|((_, ty), name)| OutputColumn { name, ty: ty.clone() }).collect();
        let exprs: Vec<Expr> = targets.into_iter().map(|t| t.0).collect();
        let has_limit = sel.limit_count.is_some();

        if plain_distinct {
            plan = Plan::Distinct { input: Box::new(Plan::Project(Box::new(plan), exprs.clone())), on: None };
            if !sorts.is_empty() {
                let mut keys = Vec::with_capacity(sorts.len());
                for (target, proto) in sorts {
                    let idx = match target {
                        SortTarget::Output(i) => i,
                        SortTarget::Expr(e) => exprs.iter().position(|t| *t == e).ok_or_else(|| {
                            Error::new(
                                sqlstate::INVALID_COLUMN_REFERENCE,
                                "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                            )
                        })?,
                    };
                    keys.push(SortKey { expr: Expr::Column(idx), ..proto });
                }
                plan = Plan::Sort { input: Box::new(plan), keys };
            }
            return Ok((self.bind_limit(plan, sel)?, out));
        }

        let mut keys: Vec<SortKey> = sorts
            .into_iter()
            .map(|(target, proto)| SortKey {
                expr: match target {
                    SortTarget::Output(i) => exprs[i].clone(),
                    SortTarget::Expr(e) => e,
                },
                ..proto
            })
            .collect();
        if let Some(on) = &distinct_on {
            // DISTINCT ON needs equal keys adjacent: sort by them first
            // unless ORDER BY already starts with them.
            for (i, e) in on.iter().enumerate() {
                match keys.get(i) {
                    Some(k) if k.expr == *e => {}
                    Some(_) => {
                        return Err(Error::new(
                            sqlstate::INVALID_COLUMN_REFERENCE,
                            "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
                        ))
                    }
                    None => keys.push(SortKey { expr: e.clone(), desc: false, nulls_first: false }),
                }
            }
        }
        if !keys.is_empty() {
            plan = self.sort_or_use_order(plan, keys, has_limit, &scope);
        }
        if let Some(on) = distinct_on {
            plan = Plan::Distinct { input: Box::new(plan), on: Some(on) };
        }
        plan = self.bind_locking(plan, sel, &scope, where_pred)?;
        plan = Plan::Project(Box::new(plan), exprs);
        Ok((self.bind_limit(plan, sel)?, out))
    }

    /// Binds select list, GROUP BY, HAVING, ORDER BY and DISTINCT ON
    /// against the FROM scope.
    #[allow(clippy::type_complexity)]
    pub(crate) fn bind_select_clauses(
        &mut self,
        sel: &pb::SelectStmt,
        scope: &Scope,
    ) -> Result<(Vec<Typed>, Vec<String>, Vec<Expr>, Option<Expr>, Vec<(SortTarget, SortKey)>, Option<Vec<Expr>>)> {
        self.clause = "SELECT";
        let mut targets: Vec<Typed> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        for t in &sel.target_list {
            let N::ResTarget(rt) = node(t)? else { return Err(Error::internal("malformed select list")) };
            let val = rt.val.as_ref().ok_or_else(|| Error::internal("select item without value"))?;
            if let Some(N::ColumnRef(c)) = val.node.as_ref() {
                if matches!(c.fields.last().and_then(|f| f.node.as_ref()), Some(N::AStar(_))) {
                    let qualifier = match c.fields.len() {
                        1 => None,
                        n => Some(super::str_val(&c.fields[n - 2])?),
                    };
                    let before = targets.len();
                    for (i, col) in scope.cols.iter().enumerate() {
                        let take = match qualifier {
                            None => !col.hidden && !col.merged,
                            Some(q) => !col.hidden && col.table.as_deref() == Some(q),
                        };
                        if take {
                            targets.push((Expr::Column(i), col.ty.clone()));
                            names.push(col.name.clone());
                        }
                    }
                    if let (Some(q), true) = (qualifier, targets.len() == before) {
                        return Err(Error::new(
                            sqlstate::UNDEFINED_TABLE,
                            format!("missing FROM-clause entry for table \"{q}\""),
                        ));
                    }
                    if qualifier.is_none() && scope.cols.is_empty() {
                        return Err(Error::syntax("SELECT * with no tables specified is not valid"));
                    }
                    continue;
                }
            }
            let (expr, ty) = self.bind_expr(val, scope)?;
            // A plain column keeps its declared type (`oid`, `"char"`) in
            // the result description.
            let ty = match (&expr, val.node.as_ref()) {
                (Expr::Column(i), _) => scope.cols[*i].ty.clone(),
                // So does an explicit cast to one of those types.
                (_, Some(N::TypeCast(c))) => match c.type_name.as_ref().map(|t| self.resolve_type(t)) {
                    Some(Ok(decl)) if decl.ty.base() == ty && decl.ty != ty => decl.ty,
                    _ => ty,
                },
                _ => ty,
            };
            targets.push((expr, ty));
            names.push(if rt.name.is_empty() { target_name(val) } else { rt.name.clone() });
        }

        // GROUP BY: positions and select-list aliases are allowed.
        self.clause = "GROUP BY";
        let mut groups = Vec::with_capacity(sel.group_clause.len());
        self.agg_levels.last_mut().unwrap().allow_aggs = false;
        self.agg_levels.last_mut().unwrap().allow_windows = false;
        self.agg_levels.last_mut().unwrap().allow_srfs = false;
        for g in &sel.group_clause {
            if let Some(N::GroupingSet(gs)) = g.node.as_ref() {
                if gs.content.is_empty() {
                    continue;
                }
                return Err(Error::unsupported("GROUPING SETS, ROLLUP and CUBE"));
            }
            let expr = if let Some(i) = int_const(g) {
                if i < 1 || i as usize > targets.len() {
                    return Err(Error::new(
                        sqlstate::INVALID_COLUMN_REFERENCE,
                        format!("GROUP BY position {i} is not in select list"),
                    ));
                }
                targets[i as usize - 1].0.clone()
            } else {
                match self.bind_expr(g, scope) {
                    Ok((e, _)) => e,
                    Err(err) if err.code() == sqlstate::UNDEFINED_COLUMN => {
                        // Not an input column: maybe a select-list alias.
                        match bare_name(g).and_then(|n| names.iter().position(|x| x == n)) {
                            Some(i) => targets[i].0.clone(),
                            None => return Err(err),
                        }
                    }
                    Err(err) => return Err(err),
                }
            };
            let mut has_agg = false;
            expr.walk(&mut |e| has_agg |= matches!(e, Expr::AggRef(_) | Expr::WindowRef(_)));
            if has_agg {
                return Err(Error::new(sqlstate::GROUPING_ERROR, "aggregate functions are not allowed in GROUP BY"));
            }
            if !groups.contains(&expr) {
                groups.push(expr);
            }
        }
        self.agg_levels.last_mut().unwrap().allow_aggs = true;

        self.clause = "HAVING";
        let having = sel.having_clause.as_ref().map(|h| self.bind_bool(h, scope, "HAVING")).transpose()?;
        self.agg_levels.last_mut().unwrap().allow_windows = true;

        // ORDER BY: positions and output names first, then expressions.
        self.clause = "ORDER BY";
        let mut sorts = Vec::with_capacity(sel.sort_clause.len());
        for s in &sel.sort_clause {
            let N::SortBy(sb) = node(s)? else { return Err(Error::internal("malformed ORDER BY")) };
            let target = sb.node.as_ref().ok_or_else(|| Error::internal("ORDER BY without expression"))?;
            let by_position = int_const(target);
            reject_constant_sort_key(target, by_position.is_some())?;
            let by_name = bare_name(target).and_then(|n| {
                let mut hits = names.iter().enumerate().filter(|(_, x)| *x == n);
                match (hits.next(), hits.next()) {
                    (Some((i, _)), None) => Some(i),
                    // Several outputs with this name: fine if identical.
                    (Some((i, _)), Some((j, _))) if targets[i].0 == targets[j].0 => Some(i),
                    _ => None,
                }
            });
            let st = if let Some(i) = by_position {
                if i < 1 || i as usize > targets.len() {
                    return Err(Error::new(
                        sqlstate::INVALID_COLUMN_REFERENCE,
                        format!("ORDER BY position {i} is not in select list"),
                    ));
                }
                SortTarget::Output(i as usize - 1)
            } else if let Some(i) = by_name {
                SortTarget::Output(i)
            } else {
                let (e, t) = self.bind_expr(target, scope)?;
                let e = if t == Type::Unknown { self.coerce(e, &t, &Type::Text)? } else { e };
                match targets.iter().position(|x| x.0 == e) {
                    Some(i) => SortTarget::Output(i),
                    None => SortTarget::Expr(e),
                }
            };
            sorts.push((st, sort_key(Expr::null(), sb)?));
        }

        self.clause = "DISTINCT ON";
        let distinct_on = if sel.distinct_clause.iter().any(|d| d.node.is_some()) {
            let mut on = Vec::with_capacity(sel.distinct_clause.len());
            for d in &sel.distinct_clause {
                on.push(match int_const(d) {
                    Some(i) if i >= 1 && (i as usize) <= targets.len() => targets[i as usize - 1].0.clone(),
                    _ => match bare_name(d).and_then(|n| names.iter().position(|x| x == n)) {
                        Some(i) if self.bind_expr(d, scope).is_err() => targets[i].0.clone(),
                        _ => self.bind_expr(d, scope)?.0,
                    },
                });
            }
            Some(on)
        } else {
            None
        };
        self.clause = "SELECT";
        Ok((targets, names, groups, having, sorts, distinct_on))
    }

    fn bind_locking(
        &mut self,
        mut plan: Plan,
        sel: &pb::SelectStmt,
        scope: &Scope,
        recheck: Option<Expr>,
    ) -> Result<Plan> {
        for l in &sel.locking_clause {
            let N::LockingClause(lc) = node(l)? else { continue };
            use pb::{LockClauseStrength as S, LockWaitPolicy as W};
            let mode = match S::try_from(lc.strength).unwrap_or(S::Undefined) {
                S::LcsForupdate | S::LcsFornokeyupdate => RowLock::Update,
                _ => RowLock::Share,
            };
            let wait = match W::try_from(lc.wait_policy).unwrap_or(W::Undefined) {
                W::LockWaitSkip => LockWait::Skip,
                W::LockWaitError => LockWait::Error,
                _ => LockWait::Block,
            };
            let mut names = Vec::new();
            for r in &lc.locked_rels {
                if let N::RangeVar(rv) = node(r)? {
                    names.push(rv.relname.clone());
                }
            }
            for n in &names {
                if !scope.rels.iter().any(|r| r.alias == *n) {
                    return Err(Error::new(
                        sqlstate::UNDEFINED_TABLE,
                        format!("relation \"{n}\" in FOR UPDATE clause not found in FROM clause"),
                    ));
                }
            }
            for rel in &scope.rels {
                if !names.is_empty() && !names.contains(&rel.alias) {
                    continue;
                }
                let Some(table) = &rel.table else {
                    if names.is_empty() {
                        continue;
                    }
                    return Err(Error::unsupported("FOR UPDATE on a relation that is not a base table"));
                };
                plan = Plan::LockRows {
                    input: Box::new(plan),
                    table: table.clone(),
                    offset: rel.start,
                    mode,
                    wait,
                    recheck: recheck.clone(),
                };
            }
        }
        Ok(plan)
    }

    pub(crate) fn bind_from_list(&mut self, items: &[pb::Node]) -> Result<(Plan, Scope)> {
        self.clause = "FROM";
        let mut acc: Option<(Plan, Scope)> = None;
        for item in items {
            acc = Some(match acc {
                None => {
                    let (p, s, _) = self.bind_from_item(item, None)?;
                    (p, s)
                }
                Some((lp, ls)) => {
                    let (rp, rs, lateral) = self.bind_from_item(item, Some(&ls))?;
                    let right_width = rs.cols.len();
                    (
                        Plan::NestedLoop {
                            left: Box::new(lp),
                            right: Box::new(rp),
                            kind: JoinKind::Inner,
                            on: None,
                            lateral,
                            index_join: false,
                            right_width,
                        },
                        ls.join_distinct(rs)?,
                    )
                }
            });
        }
        Ok(acc.unwrap_or((Plan::OneRow, Scope::default())))
    }

    /// Binds one FROM item. `left` is the scope of the items to its left,
    /// which LATERAL subqueries and functions may reference; the returned
    /// flag says whether this item did.
    fn bind_from_item(&mut self, n: &pb::Node, left: Option<&Scope>) -> Result<(Plan, Scope, bool)> {
        match node(n)? {
            N::RangeVar(rv) => {
                let (plan, scope) = self.bind_range_var(rv)?;
                Ok((plan, scope, false))
            }
            N::RangeSubselect(rs) => {
                let Some(N::SelectStmt(sel)) = opt_node(&rs.subquery) else {
                    return Err(Error::internal("subquery in FROM without SELECT"));
                };
                let (plan, out, lateral) = match left {
                    Some(l) if rs.lateral => {
                        let hidden = std::mem::take(&mut self.hidden_levels);
                        let bound = self.bind_subquery(sel, l);
                        self.hidden_levels = hidden;
                        bound?
                    }
                    _ => {
                        let (p, o) = self.bind_select(sel)?;
                        (p, o, false)
                    }
                };
                let alias = rs.alias.as_ref().map_or("subquery", |a| a.aliasname.as_str());
                let cols = out
                    .into_iter()
                    .map(|c| (c.name, if c.ty == Type::Unknown { Type::Text } else { c.ty }, false))
                    .collect();
                let mut scope = Scope::relation(alias, cols, None);
                if let Some(a) = &rs.alias {
                    scope = scope.rename(alias, &name_list(&a.colnames)?);
                }
                Ok((plan, scope, lateral))
            }
            N::RangeFunction(rf) => self.bind_range_function(rf, left),
            N::JoinExpr(j) => {
                // Items to the left of this join (in a comma list) stay
                // visible to LATERAL items inside it, one level further out.
                if let Some(l) = left {
                    self.hidden_levels.push(self.outer_scopes.len());
                    self.outer_scopes.push(l.clone());
                    self.correlated.push(false);
                }
                let result = (|| -> Result<(Plan, Scope)> {
                    let larg = j.larg.as_ref().ok_or_else(|| Error::internal("join without left side"))?;
                    let rarg = j.rarg.as_ref().ok_or_else(|| Error::internal("join without right side"))?;
                    let (lp, ls, _) = self.bind_from_item(larg, None)?;
                    let (rp, rs, lateral) = self.bind_from_item(rarg, Some(&ls))?;
                    let kind = match pb::JoinType::try_from(j.jointype).unwrap_or(pb::JoinType::Undefined) {
                        pb::JoinType::JoinInner => JoinKind::Inner,
                        pb::JoinType::JoinLeft => JoinKind::Left,
                        pb::JoinType::JoinRight => JoinKind::Right,
                        pb::JoinType::JoinFull => JoinKind::Full,
                        other => return Err(Error::unsupported(format!("join type {other:?}"))),
                    };
                    if lateral && matches!(kind, JoinKind::Right | JoinKind::Full) {
                        return Err(Error::new(
                        sqlstate::INVALID_COLUMN_REFERENCE,
                        "invalid reference to FROM-clause entry: the combining JOIN type must be INNER or LEFT for a LATERAL reference",
                    ));
                    }
                    let (left_width, right_width) = (ls.cols.len(), rs.cols.len());

                    // USING / NATURAL: equate same-named columns and hide the
                    // duplicate from unqualified references and `*`.
                    let mut using = name_list(&j.using_clause)?;
                    if j.is_natural {
                        using = ls
                            .cols
                            .iter()
                            .filter(|c| !c.hidden && !c.merged)
                            .filter(|c| rs.cols.iter().any(|r| !r.hidden && !r.merged && r.name == c.name))
                            .map(|c| c.name.clone())
                            .collect();
                    }
                    let mut pairs = Vec::with_capacity(using.len());
                    for name in &using {
                        let find =
                            |s: &Scope, side: &str| -> Result<usize> {
                                let mut hits = s.cols.iter().enumerate().filter(|(_, c)| c.name == *name && !c.merged);
                                match (hits.next(), hits.next()) {
                            (Some((i, _)), None) => Ok(i),
                            (None, _) => Err(Error::new(
                                sqlstate::UNDEFINED_COLUMN,
                                format!("column \"{name}\" specified in USING clause does not exist in {side} table"),
                            )),
                            _ => Err(Error::new(
                                sqlstate::AMBIGUOUS_COLUMN,
                                format!("common column name \"{name}\" appears more than once in {side} table"),
                            )),
                        }
                            };
                        pairs.push((find(&ls, "left")?, find(&rs, "right")?));
                    }
                    let mut scope = ls.join_distinct(rs)?;
                    let mut conds = Vec::new();
                    for (li, ri) in pairs {
                        let ri = left_width + ri;
                        let l = (Expr::Column(li), scope.cols[li].ty.clone());
                        let r = (Expr::Column(ri), scope.cols[ri].ty.clone());
                        conds.push(self.bind_op("=", Some(l), r)?.0);
                        let hide = if kind == JoinKind::Right { li } else { ri };
                        scope.cols[hide].merged = true;
                    }
                    if let Some(q) = &j.quals {
                        self.clause = "JOIN/ON";
                        conds.push(self.bind_bool(q, &scope, "JOIN/ON")?);
                        self.clause = "FROM";
                    }
                    let on = match conds.len() {
                        0 => None,
                        1 => conds.pop(),
                        _ => Some(Expr::And(conds)),
                    };
                    let plan = Plan::NestedLoop {
                        left: Box::new(lp),
                        right: Box::new(rp),
                        kind,
                        on,
                        lateral,
                        index_join: false,
                        right_width,
                    };
                    if let Some(a) = &j.alias {
                        scope = scope.rename(&a.aliasname, &name_list(&a.colnames)?);
                    }
                    Ok((plan, scope))
                })();
                let lateral = if left.is_some() {
                    self.outer_scopes.pop();
                    let level = self.outer_scopes.len();
                    self.hidden_levels.retain(|l| *l != level);
                    self.correlated.pop().unwrap_or(false)
                } else {
                    false
                };
                let (plan, scope) = result?;
                Ok((plan, scope, lateral))
            }
            other => Err(Error::unsupported(format!("{} in FROM", node_kind(other)))),
        }
    }

    fn bind_range_function(&mut self, rf: &pb::RangeFunction, left: Option<&Scope>) -> Result<(Plan, Scope, bool)> {
        if rf.is_rowsfrom && rf.functions.len() != 1 {
            return Err(Error::unsupported("ROWS FROM with several functions"));
        }
        let first = rf.functions.first().ok_or_else(|| Error::internal("function in FROM without call"))?;
        let N::List(list) = node(first)? else { return Err(Error::internal("malformed function in FROM")) };
        let call = list.items.first().ok_or_else(|| Error::internal("malformed function in FROM"))?;
        let empty = Scope::default();

        // Arguments may refer to FROM items on the left (implicit LATERAL).
        let hidden = std::mem::take(&mut self.hidden_levels);
        if let Some(l) = left {
            self.outer_scopes.push(l.clone());
            self.correlated.push(false);
        }
        let bound = (|| -> Result<(Plan, super::expr::Columns, String)> {
            match node(call)? {
                N::FuncCall(f) => {
                    let fname = name_list(&f.funcname)?.pop().unwrap_or_default().to_ascii_lowercase();
                    match srf(&fname) {
                        Some(kind) => {
                            let mut args = Vec::with_capacity(f.args.len());
                            for a in &f.args {
                                args.push(self.bind_expr(a, &empty)?);
                            }
                            let (exprs, cols) = self.bind_srf_args(&kind, &fname, args)?;
                            Ok((Plan::FuncScan { func: kind, args: exprs, ordinality: rf.ordinality }, cols, fname))
                        }
                        None => {
                            let (e, t) = self.bind_expr(call, &empty)?;
                            let t = if t == Type::Unknown { Type::Text } else { t };
                            Ok((Plan::Project(Box::new(Plan::OneRow), vec![e]), vec![(fname.clone(), t)], fname))
                        }
                    }
                }
                _ => {
                    let (e, t) = self.bind_expr(call, &empty)?;
                    Ok((
                        Plan::Project(Box::new(Plan::OneRow), vec![e]),
                        vec![(target_name(call), t)],
                        target_name(call),
                    ))
                }
            }
        })();
        let lateral = if left.is_some() {
            self.outer_scopes.pop();
            self.correlated.pop().unwrap_or(false)
        } else {
            false
        };
        self.hidden_levels = hidden;
        let (plan, mut cols, fname) = bound?;
        if rf.ordinality {
            if !matches!(plan, Plan::FuncScan { .. }) {
                return Err(Error::unsupported("WITH ORDINALITY on a non-set-returning function"));
            }
            cols.push(("ordinality".to_string(), Type::Int8));
        }
        let alias = rf.alias.as_ref().map_or(fname.as_str(), |a| a.aliasname.as_str()).to_string();
        // A single-column function result is named after the alias.
        if let (Some(_), 1) = (&rf.alias, cols.len()) {
            cols[0].0 = alias.clone();
        }
        let mut scope = Scope::relation(&alias, cols.into_iter().map(|(n, t)| (n, t, false)).collect(), None);
        if let Some(a) = &rf.alias {
            scope = scope.rename(&alias, &name_list(&a.colnames)?);
        }
        Ok((plan, scope, lateral))
    }

    fn bind_range_var(&mut self, rv: &pb::RangeVar) -> Result<(Plan, Scope)> {
        let (schema, name) = range_var_name(rv);
        let alias = rv.alias.as_ref().map_or(name, |a| a.aliasname.as_str());
        let col_names = match &rv.alias {
            Some(a) => name_list(&a.colnames)?,
            None => Vec::new(),
        };
        let finish = |scope: Scope| if col_names.is_empty() { scope } else { scope.rename(alias, &col_names) };

        if schema.is_none() {
            if let Some(cte) = self.ctes.iter().rev().find(|c| c.name == name) {
                let cols = cte.cols.iter().map(|(n, t)| (n.clone(), t.clone(), false)).collect();
                let id = cte.id;
                self.cte_refs.push(id);
                return Ok((Plan::CteScan(id), finish(Scope::relation(alias, cols, None))));
            }
        }
        if let Some(cols) = crate::system::table_columns(schema, name) {
            let cols = cols.into_iter().map(|(n, t)| (n.to_string(), t, false)).collect();
            let qualified = format!("{}.{}", schema.unwrap_or("pg_catalog"), name);
            return Ok((Plan::System(qualified), finish(Scope::relation(alias, cols, None))));
        }
        match self.catalog.relation(schema, name, self.search_path) {
            Some(Relation::Table(table)) => {
                self.tables_used.push(table.id);
                let scope = Scope::for_table(&table, alias);
                Ok((Plan::Scan(ScanPlan::full(table)), finish(scope)))
            }
            Some(Relation::View(view)) => {
                if self.view_stack.contains(&view.id) {
                    return Err(Error::new(
                        sqlstate::INVALID_OBJECT_DEFINITION,
                        format!("infinite recursion detected in rules for relation \"{}\"", view.name),
                    ));
                }
                let stmts = parse_sql(&view.sql)?;
                let Some(N::SelectStmt(sel)) =
                    stmts.first().and_then(|s| s.stmt.as_ref()).and_then(|s| s.node.as_ref())
                else {
                    return Err(Error::internal("stored view definition is not a SELECT"));
                };
                // A view body is bound in isolation from the referencing query.
                let hidden = std::mem::take(&mut self.hidden_levels);
                let outer = std::mem::take(&mut self.outer_scopes);
                let correlated = std::mem::take(&mut self.correlated);
                let ctes = std::mem::take(&mut self.ctes);
                let levels = std::mem::take(&mut self.agg_levels);
                self.view_stack.push(view.id);
                let result = self.bind_select(sel);
                self.view_stack.pop();
                self.outer_scopes = outer;
                self.hidden_levels = hidden;
                self.correlated = correlated;
                self.ctes = ctes;
                self.agg_levels = levels;
                let (plan, out) = result?;
                let cols = out
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let name = view.columns.get(i).cloned().unwrap_or(c.name);
                        (name, if c.ty == Type::Unknown { Type::Text } else { c.ty }, false)
                    })
                    .collect();
                Ok((plan, finish(Scope::relation(alias, cols, None))))
            }
            None => Err(Error::new(
                sqlstate::UNDEFINED_TABLE,
                match schema {
                    Some(s) => format!("relation \"{s}.{name}\" does not exist"),
                    None => format!("relation \"{name}\" does not exist"),
                },
            )
            .with_position(rv.location.max(0) as usize + 1)),
        }
    }
}

/// Postgres lets a query select columns that are functionally dependent on
/// the GROUP BY columns: if a table's whole primary key is grouped, its
/// other columns are constant within a group. Such columns are added as
/// extra grouping keys, which does not change the groups.
fn add_dependent_groups(groups: &mut Vec<Expr>, exprs: &[&mut Expr], scope: &Scope) {
    let mut wanted: Vec<usize> = Vec::new();
    for e in exprs {
        collect_ungrouped(e, groups, &mut wanted);
    }
    for col in wanted {
        let rel = &scope.rels[scope.cols[col].rel];
        let Some(table) = &rel.table else { continue };
        if table.rowid {
            continue;
        }
        let covered = table.pk.iter().all(|pk| groups.contains(&Expr::Column(rel.start + pk)));
        if covered && !groups.contains(&Expr::Column(col)) {
            groups.push(Expr::Column(col));
        }
    }
}

fn collect_ungrouped(e: &Expr, groups: &[Expr], out: &mut Vec<usize>) {
    if groups.contains(e) {
        return;
    }
    match e {
        Expr::Column(i) => {
            if !out.contains(i) {
                out.push(*i);
            }
        }
        other => other.for_each_child(&mut |c| collect_ungrouped(c, groups, out)),
    }
}

/// Rewrites an expression bound against the FROM row into one over the
/// aggregate's output row (grouping keys, then aggregates).
fn post_agg(e: Expr, groups: &[Expr], scope: &Scope) -> Result<Expr> {
    e.rewrite(&mut |n| {
        if let Some(i) = groups.iter().position(|g| g == n) {
            return Ok(Some(Expr::Column(i)));
        }
        match n {
            Expr::AggRef(j) => Ok(Some(Expr::Column(groups.len() + j))),
            Expr::Column(i) => {
                let c = &scope.cols[*i];
                let name = match &c.table {
                    Some(t) if !t.is_empty() => format!("{t}.{}", c.name),
                    _ => c.name.clone(),
                };
                Err(Error::new(
                    sqlstate::GROUPING_ERROR,
                    format!("column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function"),
                ))
            }
            Expr::Subquery(s) if s.correlated => Err(Error::unsupported(
                "a correlated subquery above GROUP BY that references ungrouped or grouped columns of the outer query",
            )),
            _ => Ok(None),
        }
    })
}
