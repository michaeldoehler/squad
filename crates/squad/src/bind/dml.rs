//! Binding of INSERT, UPDATE and DELETE.

use std::sync::Arc;

use pg_query::protobuf::{self as pb, node::Node as N};

use super::expr::Typed;
use super::query::OutputColumn;
use super::{node, parse_expr, range_var_name, Binder, DeclaredType, Scope};
use crate::catalog::{Column, Relation, Table};
use crate::expr::{Expr, SessionFn};
use crate::plan::{DeletePlan, InsertPlan, JoinKind, OnConflict, Plan, ScanPlan, UpdatePlan};
use crate::types::{Type, Value};
use crate::value::{self, CastContext};
use crate::{sqlstate, Error, Result};

impl<'a> Binder<'a> {
    /// Binds a data-modifying statement. The output columns are those of
    /// its RETURNING clause (empty without one).
    pub fn bind_dml(&mut self, stmt: &N) -> Result<(Plan, Vec<OutputColumn>)> {
        let with = match stmt {
            N::InsertStmt(s) => &s.with_clause,
            N::UpdateStmt(s) => &s.with_clause,
            N::DeleteStmt(s) => &s.with_clause,
            _ => return Err(Error::internal("not a DML statement")),
        };
        let cte_mark = self.ctes.len();
        let result = (|| {
            let defs = match with {
                Some(w) => self.bind_with(w)?,
                None => Vec::new(),
            };
            let (plan, out) = match stmt {
                N::InsertStmt(s) => self.bind_insert(s)?,
                N::UpdateStmt(s) => self.bind_update(s)?,
                N::DeleteStmt(s) => self.bind_delete(s)?,
                _ => unreachable!(),
            };
            Ok((if defs.is_empty() { plan } else { Plan::With { ctes: defs, body: Box::new(plan) } }, out))
        })();
        self.ctes.truncate(cte_mark);
        result
    }

    fn target_table(&mut self, rv: &Option<pb::RangeVar>) -> Result<(Arc<Table>, String)> {
        let rv = rv.as_ref().ok_or_else(|| Error::internal("statement without target relation"))?;
        let (schema, name) = range_var_name(rv);
        let alias = rv.alias.as_ref().map_or(name, |a| a.aliasname.as_str()).to_string();
        match self.catalog.relation(schema, name, self.search_path) {
            Some(Relation::Table(t)) => {
                self.tables_used.push(t.id);
                Ok((t, alias))
            }
            Some(Relation::View(v)) => Err(Error::new(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("cannot modify view \"{}\": updatable views are not supported", v.name),
            )),
            None if crate::system::table_columns(schema, name).is_some() => Err(Error::new(
                sqlstate::INSUFFICIENT_PRIVILEGE,
                format!("permission denied: \"{name}\" is a system catalog"),
            )),
            None => Err(Error::new(sqlstate::UNDEFINED_TABLE, format!("relation \"{name}\" does not exist"))
                .with_position(rv.location.max(0) as usize + 1)),
        }
    }

    /// The expression a column gets when no value is supplied.
    pub(crate) fn default_expr(&mut self, table: &Table, col: usize) -> Result<Expr> {
        let column = &table.columns[col];
        if column.hidden {
            return Ok(Expr::Session(SessionFn::RowId(table.id), vec![]));
        }
        match &column.default {
            None => Ok(Expr::Const(Value::Null)),
            Some(sql) => {
                let n = parse_expr(sql)?;
                let typed = self.bind_expr(&n, &Scope::default())?;
                self.coerce_assign(typed, column)
            }
        }
    }

    /// Converts a value for storage into `column`: assignment cast plus
    /// length / precision enforcement.
    pub(crate) fn coerce_assign(&mut self, (e, from): Typed, column: &Column) -> Result<Expr> {
        let e = if from == column.ty || from == Type::Unknown {
            self.coerce(e, &from, &column.ty)?
        } else if value::can_cast(&from, &column.ty, CastContext::Assignment) {
            self.cast_expr(e, &from, &column.ty)?
        } else {
            return Err(Error::type_mismatch(format!(
                "column \"{}\" is of type {} but expression is of type {}",
                column.name, column.ty, from
            ))
            .with_hint("You will need to rewrite or cast the expression."));
        };
        let decl = DeclaredType {
            ty: column.ty.clone(),
            decl: String::new(),
            oid: column.decl_oid,
            typmod: column.typmod,
            serial: false,
        };
        Ok(self.apply_typmod(e, &decl, false))
    }

    fn column_of(&self, table: &Table, name: &str) -> Result<usize> {
        table.columns.iter().position(|c| c.name == name && !c.hidden).ok_or_else(|| {
            Error::new(
                sqlstate::UNDEFINED_COLUMN,
                format!("column \"{name}\" of relation \"{}\" does not exist", table.name),
            )
        })
    }

    fn bind_returning(
        &mut self,
        rc: &Option<pb::ReturningClause>,
        scope: &Scope,
    ) -> Result<(Option<Vec<Expr>>, Vec<OutputColumn>)> {
        let Some(rc) = rc else { return Ok((None, Vec::new())) };
        if rc.exprs.is_empty() {
            return Ok((None, Vec::new()));
        }
        self.clause = "RETURNING";
        let select = pb::SelectStmt { target_list: rc.exprs.clone(), ..Default::default() };
        self.agg_levels.push(Default::default());
        let bound = self.bind_select_clauses(&select, scope);
        self.agg_levels.pop();
        let (targets, names, ..) = bound?;
        let out = targets
            .iter()
            .zip(names)
            .map(|((_, t), name)| OutputColumn { name, ty: if *t == Type::Unknown { Type::Text } else { t.clone() } })
            .collect();
        Ok((Some(targets.into_iter().map(|t| t.0).collect()), out))
    }

    fn bind_insert(&mut self, ins: &pb::InsertStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let (table, alias) = self.target_table(&ins.relation)?;
        let explicit_cols = !ins.cols.is_empty();
        let mut targets: Vec<usize> = Vec::new();
        if explicit_cols {
            for c in &ins.cols {
                let N::ResTarget(rt) = node(c)? else { return Err(Error::internal("malformed INSERT column list")) };
                if !rt.indirection.is_empty() {
                    return Err(Error::unsupported("assignment to array elements or fields"));
                }
                let idx = self.column_of(&table, &rt.name)?;
                if targets.contains(&idx) {
                    return Err(Error::new(
                        sqlstate::DUPLICATE_COLUMN,
                        format!("column \"{}\" specified more than once", rt.name),
                    ));
                }
                targets.push(idx);
            }
        } else {
            targets = table.visible_columns().map(|(i, _)| i).collect();
        }
        let overriding = ins.r#override != pb::OverridingKind::OverridingNotSet as i32
            && ins.r#override != pb::OverridingKind::Undefined as i32;

        let too_many = || Error::syntax("INSERT has more expressions than target columns");
        let too_few = || Error::syntax("INSERT has more target columns than expressions");
        let check_identity = |col: &Column, is_default: bool| -> Result<()> {
            if col.identity_always && !is_default && !overriding {
                return Err(Error::new(
                    "428C9",
                    format!("cannot insert a non-DEFAULT value into column \"{}\"", col.name),
                )
                .with_detail(format!("Column \"{}\" is an identity column defined as GENERATED ALWAYS.", col.name))
                .with_hint("Use OVERRIDING SYSTEM VALUE to override."));
            }
            Ok(())
        };

        self.clause = "VALUES";
        let empty = Scope::default();
        let source = match ins.select_stmt.as_ref().map(|s| node(s)).transpose()? {
            None => {
                // DEFAULT VALUES
                let mut row = Vec::with_capacity(table.columns.len());
                for i in 0..table.columns.len() {
                    row.push(self.default_expr(&table, i)?);
                }
                Plan::Values(vec![row])
            }
            Some(N::SelectStmt(sel))
                if !sel.values_lists.is_empty()
                    && sel.sort_clause.is_empty()
                    && sel.limit_count.is_none()
                    && sel.limit_offset.is_none()
                    && sel.with_clause.is_none() =>
            {
                // Plain VALUES: bind every item directly against its
                // target column so literals take the column's type.
                let mut rows = Vec::with_capacity(sel.values_lists.len());
                for list in &sel.values_lists {
                    let N::List(list) = node(list)? else { return Err(Error::internal("malformed VALUES")) };
                    if list.items.len() > targets.len() {
                        return Err(too_many());
                    }
                    if explicit_cols && list.items.len() < targets.len() {
                        return Err(too_few());
                    }
                    let mut row: Vec<Option<Expr>> = vec![None; table.columns.len()];
                    for (item, &col) in list.items.iter().zip(&targets) {
                        let column = &table.columns[col];
                        let is_default = matches!(item.node.as_ref(), Some(N::SetToDefault(_)));
                        check_identity(column, is_default)?;
                        row[col] = Some(if is_default {
                            self.default_expr(&table, col)?
                        } else {
                            let typed = self.bind_expr(item, &empty)?;
                            self.coerce_assign(typed, column)?
                        });
                    }
                    let mut full = Vec::with_capacity(row.len());
                    for (i, e) in row.into_iter().enumerate() {
                        full.push(match e {
                            Some(e) => e,
                            None => self.default_expr(&table, i)?,
                        });
                    }
                    rows.push(full);
                }
                Plan::Values(rows)
            }
            Some(N::SelectStmt(sel)) => {
                let (plan, out) = self.bind_select(sel)?;
                if out.len() > targets.len() {
                    return Err(too_many());
                }
                if explicit_cols && out.len() < targets.len() {
                    return Err(too_few());
                }
                let mut exprs = Vec::with_capacity(table.columns.len());
                for (i, column) in table.columns.iter().enumerate() {
                    exprs.push(match targets.iter().position(|t| *t == i).filter(|p| *p < out.len()) {
                        Some(p) => {
                            check_identity(column, false)?;
                            self.coerce_assign((Expr::Column(p), out[p].ty.clone()), column)?
                        }
                        None => self.default_expr(&table, i)?,
                    });
                }
                Plan::Project(Box::new(plan), exprs)
            }
            Some(_) => return Err(Error::internal("INSERT source is not a query")),
        };

        let scope = Scope::for_table(&table, &alias);
        let on_conflict = match &ins.on_conflict_clause {
            Some(oc) => Some(self.bind_on_conflict(oc, &table, &scope)?),
            None => None,
        };
        let (returning, out) = self.bind_returning(&ins.returning_clause, &scope)?;
        let checks = self.bind_checks(&table)?;
        Ok((Plan::Insert(Box::new(InsertPlan { table, source, on_conflict, checks, returning })), out))
    }

    fn bind_on_conflict(&mut self, oc: &pb::OnConflictClause, table: &Arc<Table>, scope: &Scope) -> Result<OnConflict> {
        // Which unique index arbitrates: `Some(None)` is the primary key.
        let mut target: Option<Option<usize>> = None;
        if let Some(infer) = &oc.infer {
            if !infer.conname.is_empty() {
                let pos =
                    table.indexes.iter().position(|ix| ix.name == infer.conname && ix.unique).ok_or_else(|| {
                        Error::new(
                            sqlstate::UNDEFINED_OBJECT,
                            format!("constraint \"{}\" for table \"{}\" does not exist", infer.conname, table.name),
                        )
                    })?;
                target = Some(if table.indexes[pos].primary { None } else { Some(pos) });
            } else if !infer.index_elems.is_empty() {
                let mut cols = Vec::with_capacity(infer.index_elems.len());
                for e in &infer.index_elems {
                    let N::IndexElem(ie) = node(e)? else {
                        return Err(Error::internal("malformed ON CONFLICT target"));
                    };
                    if ie.name.is_empty() {
                        return Err(Error::unsupported("expressions in ON CONFLICT targets"));
                    }
                    cols.push(self.column_of(table, &ie.name)?);
                }
                let same = |key: &[usize]| key.len() == cols.len() && key.iter().all(|c| cols.contains(c));
                target = if !table.rowid && same(&table.pk) {
                    Some(None)
                } else {
                    match table.indexes.iter().position(|ix| ix.unique && !ix.primary && same(&ix.columns)) {
                        Some(pos) => Some(Some(pos)),
                        None => {
                            return Err(Error::new(
                                sqlstate::INVALID_COLUMN_REFERENCE,
                                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
                            ))
                        }
                    }
                };
            }
        }
        match pb::OnConflictAction::try_from(oc.action).unwrap_or(pb::OnConflictAction::Undefined) {
            pb::OnConflictAction::OnconflictNothing => Ok(OnConflict::Nothing { target }),
            pb::OnConflictAction::OnconflictUpdate => {
                let target = target.ok_or_else(|| {
                    Error::syntax("ON CONFLICT DO UPDATE requires inference specification or constraint name")
                })?;
                // SET and WHERE see the existing row and the proposed row.
                let both = scope.clone().join(Scope::for_table(table, "excluded"));
                self.clause = "ON CONFLICT";
                let set = self.bind_set_list(&oc.target_list, table, &both)?;
                let filter = oc.where_clause.as_ref().map(|w| self.bind_bool(w, &both, "WHERE")).transpose()?;
                Ok(OnConflict::Update { target, set, filter })
            }
            _ => Err(Error::internal("unknown ON CONFLICT action")),
        }
    }

    /// Binds `SET col = expr, ...` of UPDATE and ON CONFLICT DO UPDATE.
    fn bind_set_list(&mut self, list: &[pb::Node], table: &Arc<Table>, scope: &Scope) -> Result<Vec<(usize, Expr)>> {
        let mut set: Vec<(usize, Expr)> = Vec::with_capacity(list.len());
        for item in list {
            let N::ResTarget(rt) = node(item)? else { return Err(Error::internal("malformed SET list")) };
            if !rt.indirection.is_empty() {
                return Err(Error::unsupported("assignment to array elements or fields"));
            }
            let col = self.column_of(table, &rt.name)?;
            if set.iter().any(|(c, _)| *c == col) {
                return Err(Error::syntax(format!("multiple assignments to same column \"{}\"", rt.name)));
            }
            let column = &table.columns[col];
            let val = rt.val.as_ref().ok_or_else(|| Error::internal("SET without value"))?;
            // `(a, b) = (x, y)` arrives as one MultiAssignRef per column.
            let val: &pb::Node = match val.node.as_ref() {
                Some(N::MultiAssignRef(m)) => match m.source.as_ref().and_then(|s| s.node.as_ref()) {
                    Some(N::RowExpr(r)) if r.args.len() == m.ncolumns as usize => &r.args[(m.colno - 1) as usize],
                    Some(N::RowExpr(_)) => {
                        return Err(Error::syntax("number of columns does not match number of values"))
                    }
                    _ => return Err(Error::unsupported("multiple-column assignment from a subquery")),
                },
                _ => val,
            };
            let expr = if matches!(val.node.as_ref(), Some(N::SetToDefault(_))) {
                self.default_expr(table, col)?
            } else {
                if column.identity_always {
                    return Err(Error::new(
                        "428C9",
                        format!("column \"{}\" can only be updated to DEFAULT", column.name),
                    )
                    .with_detail(format!(
                        "Column \"{}\" is an identity column defined as GENERATED ALWAYS.",
                        column.name
                    )));
                }
                let typed = self.bind_expr(val, scope)?;
                self.coerce_assign(typed, column)?
            };
            set.push((col, expr));
        }
        Ok(set)
    }

    /// Scan of the target table joined with the extra FROM / USING items
    /// and filtered by WHERE. Returns the plan, its scope and the bound
    /// predicate.
    fn bind_modify_source(
        &mut self,
        table: &Arc<Table>,
        alias: &str,
        from: &[pb::Node],
        where_clause: &Option<Box<pb::Node>>,
    ) -> Result<(Plan, Scope, Option<Expr>)> {
        let mut scope = Scope::for_table(table, alias);
        let mut plan = Plan::Scan(ScanPlan::full(table.clone()));
        if !from.is_empty() {
            let (fp, fs) = self.bind_from_list(from)?;
            let right_width = fs.cols.len();
            plan = Plan::NestedLoop {
                left: Box::new(plan),
                right: Box::new(fp),
                kind: JoinKind::Inner,
                on: None,
                lateral: false,
                index_join: false,
                right_width,
            };
            scope = scope.join(fs);
        }
        self.clause = "WHERE";
        let pred = match where_clause {
            Some(w) => {
                if matches!(w.node.as_ref(), Some(N::CurrentOfExpr(_))) {
                    return Err(Error::unsupported("WHERE CURRENT OF"));
                }
                Some(self.bind_bool(w, &scope, "WHERE")?)
            }
            None => None,
        };
        let plan = self.push_filter(plan, pred.clone(), &scope.types())?;
        Ok((plan, scope, pred))
    }

    fn bind_update(&mut self, upd: &pb::UpdateStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let (table, alias) = self.target_table(&upd.relation)?;
        let (source, scope, recheck) = self.bind_modify_source(&table, &alias, &upd.from_clause, &upd.where_clause)?;
        self.clause = "UPDATE";
        let set = self.bind_set_list(&upd.target_list, &table, &scope)?;
        let (returning, out) = self.bind_returning(&upd.returning_clause, &scope)?;
        let checks = self.bind_checks(&table)?;
        Ok((Plan::Update(Box::new(UpdatePlan { table, source, set, recheck, checks, returning })), out))
    }

    fn bind_delete(&mut self, del: &pb::DeleteStmt) -> Result<(Plan, Vec<OutputColumn>)> {
        let (table, alias) = self.target_table(&del.relation)?;
        let (source, scope, recheck) = self.bind_modify_source(&table, &alias, &del.using_clause, &del.where_clause)?;
        let (returning, out) = self.bind_returning(&del.returning_clause, &scope)?;
        Ok((Plan::Delete(Box::new(DeletePlan { table, source, recheck, returning })), out))
    }

    fn bind_checks(&mut self, table: &Arc<Table>) -> Result<Vec<(String, Expr)>> {
        table.checks.iter().map(|c| Ok((c.name.clone(), self.bind_check(table, &c.expr)?))).collect()
    }

    /// Binds a stored CHECK constraint against the table's row.
    pub fn bind_check(&mut self, table: &Arc<Table>, sql: &str) -> Result<Expr> {
        let n = parse_expr(sql)?;
        let scope = Scope::for_table(table, &table.name);
        self.clause = "CHECK";
        self.bind_bool(&n, &scope, "CHECK")
    }
}
