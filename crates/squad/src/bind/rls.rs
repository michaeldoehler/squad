//! Row-level security: the policies of a table, turned into a filter on
//! the rows a statement reads and into checks on the rows it writes.
//!
//! The policies are applied while binding, as PostgreSQL's rewriter adds
//! them to the query: a table reference becomes a scan whose filter is
//! the combined `USING` expressions, and `INSERT`/`UPDATE` plans carry the
//! `WITH CHECK` expressions. The filter's conjuncts stay ahead of the
//! statement's own conditions, so those see only rows the policies let
//! through. Policy expressions are evaluated when the statement runs;
//! `current_setting(...)` in them reads the setting of that moment.

use std::sync::Arc;

use super::{parse_expr, Binder, Scope};
use crate::catalog::{PolicyCommand, Table};
use crate::expr::Expr;
use crate::types::Value;
use crate::{sqlstate, Error, Result};

/// Who a statement is bound for, as far as row-level security cares.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSecurityUser {
    /// The login name policies `TO name` match.
    pub user: String,
    /// The login is exempt from the policies of tables without `FORCE ROW
    /// LEVEL SECURITY`, like a table's owner in PostgreSQL.
    pub bypass_rls: bool,
    /// The `row_security` setting. When off, a statement that policies
    /// would filter fails instead.
    pub row_security: bool,
}

/// A `WITH CHECK` condition a new row must meet (`true`; NULL fails).
#[derive(Debug, Clone)]
pub struct RlsCheck {
    /// Over the table row.
    pub expr: Expr,
    /// The policy's name for a restrictive policy; `None` for the
    /// combination of the permissive ones.
    pub policy: Option<String>,
}

impl RlsCheck {
    /// The error for a row that fails this check.
    pub fn violation(&self, table: &Table) -> Error {
        let msg = match &self.policy {
            Some(p) => format!("new row violates row-level security policy \"{p}\" for table \"{}\"", table.name),
            None => format!("new row violates row-level security policy for table \"{}\"", table.name),
        };
        Error::new(sqlstate::INSUFFICIENT_PRIVILEGE, msg)
    }
}

impl<'a> Binder<'a> {
    /// True if the policies of `table` apply to the statement being bound.
    /// With `row_security` off, a statement they would apply to fails.
    pub(crate) fn rls_applies(&self, table: &Table) -> Result<bool> {
        let Some(user) = &self.rls else { return Ok(false) };
        if !table.row_security || (user.bypass_rls && !table.force_row_security) {
            return Ok(false);
        }
        if !user.row_security {
            return Err(Error::new(
                sqlstate::INSUFFICIENT_PRIVILEGE,
                format!("query would be affected by row-level security policy for table \"{}\"", table.name),
            ));
        }
        Ok(true)
    }

    /// Binds a stored policy expression against the table's row, in
    /// isolation from the statement around it.
    fn bind_policy_expr(&mut self, table: &Arc<Table>, sql: &str) -> Result<Expr> {
        if self.rls_stack.contains(&table.id) {
            return Err(Error::new(
                sqlstate::INVALID_OBJECT_DEFINITION,
                format!("infinite recursion detected in policy for relation \"{}\"", table.name),
            ));
        }
        let n = parse_expr(sql)?;
        let hidden = std::mem::take(&mut self.hidden_levels);
        let outer = std::mem::take(&mut self.outer_scopes);
        let correlated = std::mem::take(&mut self.correlated);
        let ctes = std::mem::take(&mut self.ctes);
        let levels = std::mem::take(&mut self.agg_levels);
        let clause = std::mem::replace(&mut self.clause, "policy expressions");
        self.rls_stack.push(table.id);
        let scope = Scope::for_table(table, &table.name);
        let result = self.bind_bool(&n, &scope, "POLICY");
        self.rls_stack.pop();
        self.clause = clause;
        self.outer_scopes = outer;
        self.hidden_levels = hidden;
        self.correlated = correlated;
        self.ctes = ctes;
        self.agg_levels = levels;
        result
    }

    /// Binds a new policy expression to check it (`CREATE POLICY`).
    pub(crate) fn validate_policy_expr(&mut self, table: &Arc<Table>, sql: &str) -> Result<()> {
        self.bind_policy_expr(table, sql).map(|_| ())
    }

    /// The condition existing rows must meet for `command`: the `USING`
    /// expressions of the permissive policies combined with OR, and those
    /// of the restrictive ones with AND. Without a permissive policy
    /// nothing qualifies. The caller has checked [`Binder::rls_applies`].
    pub(crate) fn rls_using(&mut self, table: &Arc<Table>, command: PolicyCommand) -> Result<Expr> {
        let user = self.rls.as_ref().map(|u| u.user.clone()).unwrap_or_default();
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for p in table.policies.iter().filter(|p| p.applies(command, &user)) {
            let Some(sql) = &p.using else { continue };
            let e = self.bind_policy_expr(table, sql)?;
            if p.permissive {
                permissive.push(e);
            } else {
                restrictive.push(e);
            }
        }
        if permissive.is_empty() {
            return Ok(Expr::Const(Value::Bool(false)));
        }
        let mut all = vec![if permissive.len() == 1 { permissive.pop().unwrap() } else { Expr::Or(permissive) }];
        all.extend(restrictive);
        Ok(if all.len() == 1 { all.pop().unwrap() } else { Expr::And(all) })
    }

    /// The conditions new rows must meet for `command` (INSERT or
    /// UPDATE): each policy's `WITH CHECK` expression, or its `USING`
    /// expression if it has none (`use_using` takes the `USING`
    /// expressions in any case, for SELECT policies that a statement
    /// reading the rows must also satisfy).
    pub(crate) fn rls_checks(
        &mut self,
        table: &Arc<Table>,
        command: PolicyCommand,
        use_using: bool,
    ) -> Result<Vec<RlsCheck>> {
        let user = self.rls.as_ref().map(|u| u.user.clone()).unwrap_or_default();
        let mut permissive = Vec::new();
        let mut out = Vec::new();
        for p in table.policies.iter().filter(|p| p.applies(command, &user)) {
            let sql = if use_using { p.using.as_ref() } else { p.check.as_ref().or(p.using.as_ref()) };
            let Some(sql) = sql else { continue };
            let e = self.bind_policy_expr(table, sql)?;
            if p.permissive {
                permissive.push(e);
            } else {
                out.push(RlsCheck { expr: e, policy: Some(p.name.clone()) });
            }
        }
        let combined = match permissive.len() {
            0 => Expr::Const(Value::Bool(false)),
            1 => permissive.pop().unwrap(),
            _ => Expr::Or(permissive),
        };
        out.insert(0, RlsCheck { expr: combined, policy: None });
        Ok(out)
    }
}

/// True if any of the expressions reads a column of the target table,
/// which occupies the first `width` columns of the row (or may, through a
/// correlated subquery). PostgreSQL then also applies the SELECT policies
/// to an UPDATE or DELETE.
pub(crate) fn reads_target(exprs: &[&Expr], width: usize) -> bool {
    let mut found = false;
    for e in exprs {
        e.walk(&mut |n| match n {
            Expr::Column(i) if *i < width => found = true,
            Expr::Subquery(s) if s.correlated => found = true,
            _ => {}
        });
    }
    found
}
