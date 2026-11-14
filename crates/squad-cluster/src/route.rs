//! Statement analysis: which shards a statement must run on.
//!
//! Everything here works on the Postgres parse tree. The result of an
//! analysis does not depend on parameter values; routing positions keep
//! their `$n` and are resolved for each execution.

use std::collections::HashMap;

use pg_query::protobuf::{self as pb, node::Node as N};
use squad::{sqlstate, Error, Result};

use crate::scatter::{self, Scatter};
use crate::tree::{self, conjuncts, operand, string_of, unsupported, Operand};
use crate::{Class, Meta};

/// What the cluster knows while analysing: table classes and the columns
/// of tables.
pub(crate) struct Env<'a> {
    pub meta: &'a Meta,
    pub columns: &'a dyn Fn(&str) -> Result<Vec<String>>,
    /// Describes a statement on one shard (names and types of its result).
    pub describe: &'a dyn Fn(&str) -> Result<Vec<squad::OutputColumn>>,
    pub shard_count: usize,
}

/// One row of a multi-row INSERT: the operand in the distribution column.
pub(crate) struct InsertRouting {
    pub stmt: pb::InsertStmt,
    pub table: String,
    pub keys: Vec<Operand>,
}

/// Sets of distribution keys a statement is restricted to. Each group
/// must resolve to shards; all groups must agree on one shard, or there
/// is exactly one group, which may name several.
pub(crate) type KeyGroups = Vec<(String, Vec<Operand>)>;

pub(crate) enum Plan {
    /// `SELECT squad_distribute('table', 'column')`
    Distribute {
        table: String,
        column: String,
    },
    /// `SELECT squad_reference('table')`
    Reference {
        table: String,
    },
    Begin,
    /// COMMIT, ROLLBACK and savepoint commands, forwarded to the pinned shard.
    TxnControl {
        ends: bool,
    },
    /// Schema changes and session settings: every shard.
    Everywhere {
        ddl: bool,
        dropped: Vec<String>,
    },
    /// Reads that touch no distributed table: any one shard.
    Anywhere,
    /// Writes to a reference table: every shard.
    ReferenceWrite,
    Insert(InsertRouting),
    /// Restricted by distribution keys. `scatter` is how to run a SELECT
    /// when the keys name several shards.
    Keyed {
        groups: KeyGroups,
        write: bool,
        scatter: Option<std::result::Result<Scatter, Error>>,
    },
    /// UPDATE or DELETE of a distributed table without a key: each shard
    /// changes its own rows.
    WriteAll,
    Scatter(std::result::Result<Scatter, Error>),
}

fn relname(rv: &pb::RangeVar) -> String {
    rv.relname.clone()
}

fn is_system(rv: &pb::RangeVar) -> bool {
    matches!(rv.schemaname.as_str(), "pg_catalog" | "information_schema") || rv.relname.starts_with("pg_")
}

/// A table in a FROM clause.
#[derive(Debug, Clone)]
struct Instance {
    alias: String,
    table: String,
    /// Distribution column, for distributed tables.
    dist: Option<String>,
}

/// One query level: its tables, conditions and nested queries.
#[derive(Default)]
struct Level {
    instances: Vec<Instance>,
    conds: Vec<pb::Node>,
    /// USING column lists of joins: `(name, left aliases, right aliases)`.
    using: Vec<(String, Vec<String>, Vec<String>)>,
    nested: Vec<pb::SelectStmt>,
    /// A table that is neither distributed nor reference nor a system
    /// catalog.
    undeclared: Option<String>,
    ctes: Vec<String>,
}

impl Level {
    fn add_table(&mut self, rv: &pb::RangeVar, env: &Env) {
        let table = relname(rv);
        if rv.schemaname.is_empty() && self.ctes.contains(&table) {
            return;
        }
        let alias = rv.alias.as_ref().map_or_else(|| table.clone(), |a| a.aliasname.clone());
        let dist = match env.meta.tables.get(&table) {
            Some(Class::Distributed { column, .. }) => Some(column.clone()),
            Some(Class::Reference) => None,
            None => {
                if !is_system(rv) && crate::META_TABLE != table {
                    self.undeclared.get_or_insert(table.clone());
                }
                None
            }
        };
        self.instances.push(Instance { alias, table, dist });
    }

    fn add_from(&mut self, node: &pb::Node, env: &Env) -> Result<Vec<String>> {
        match node.node.as_ref() {
            Some(N::RangeVar(rv)) => {
                let before = self.instances.len();
                self.add_table(rv, env);
                Ok(self.instances[before..].iter().map(|i| i.alias.clone()).collect())
            }
            Some(N::JoinExpr(j)) => {
                let mut left = match &j.larg {
                    Some(l) => self.add_from(l, env)?,
                    None => Vec::new(),
                };
                let right = match &j.rarg {
                    Some(r) => self.add_from(r, env)?,
                    None => Vec::new(),
                };
                if let Some(q) = &j.quals {
                    conjuncts(q, &mut self.conds);
                    self.collect_nested(q)?;
                }
                for u in &j.using_clause {
                    if let Some(name) = string_of(u) {
                        self.using.push((name.to_string(), left.clone(), right.clone()));
                    }
                }
                left.extend(right);
                Ok(left)
            }
            Some(N::RangeSubselect(s)) => {
                if let Some(N::SelectStmt(sel)) = s.subquery.as_ref().and_then(|q| q.node.as_ref()) {
                    self.nested.push((**sel).clone());
                }
                Ok(Vec::new())
            }
            Some(N::RangeFunction(f)) => {
                for func in &f.functions {
                    self.collect_nested(func)?;
                }
                Ok(Vec::new())
            }
            Some(other) => {
                let dbg = format!("{other:?}");
                Err(unsupported(format!("{} in FROM", dbg.split(['(', ' ', '{']).next().unwrap_or("item"))))
            }
            None => Ok(Vec::new()),
        }
    }

    /// Finds subqueries inside an expression.
    fn collect_nested(&mut self, expr: &pb::Node) -> Result<()> {
        let mut copy = expr.clone();
        let nested = &mut self.nested;
        tree::walk(&mut copy, &mut |n| {
            if let Some(N::SubLink(s)) = n.node.as_mut() {
                if let Some(N::SelectStmt(sel)) = s.subselect.as_ref().and_then(|q| q.node.as_ref()) {
                    nested.push((**sel).clone());
                }
                if let Some(t) = s.testexpr.as_mut() {
                    // The left side of `x IN (SELECT …)` may hold more.
                    let mut inner = Vec::new();
                    tree::walk(t, &mut |m| {
                        if let Some(N::SubLink(s2)) = m.node.as_ref() {
                            if let Some(N::SelectStmt(sel)) = s2.subselect.as_ref().and_then(|q| q.node.as_ref()) {
                                inner.push((**sel).clone());
                            }
                            return Ok(true);
                        }
                        Ok(false)
                    })?;
                    nested.extend(inner);
                }
                return Ok(true);
            }
            Ok(false)
        })
    }

    fn distributed(&self) -> impl Iterator<Item = &Instance> {
        self.instances.iter().filter(|i| i.dist.is_some())
    }

    /// The distributed instance whose distribution column `c` refers to.
    fn dist_instance(&self, c: &pb::ColumnRef, env: &Env) -> Result<Option<usize>> {
        let parts: Vec<&str> = c.fields.iter().filter_map(string_of).collect();
        if parts.len() != c.fields.len() {
            return Ok(None);
        }
        let (qualifier, name) = match parts.as_slice() {
            [name] => (None, *name),
            [.., q, name] => (Some(*q), *name),
            _ => return Ok(None),
        };
        match qualifier {
            Some(q) => Ok(self.instances.iter().position(|i| i.alias == q && i.dist.as_deref() == Some(name))),
            None => {
                // Unqualified: the name must belong to exactly one table
                // of this level, and be that table's distribution column.
                let mut owner = None;
                for (idx, inst) in self.instances.iter().enumerate() {
                    let has =
                        inst.dist.as_deref() == Some(name) || (env.columns)(&inst.table)?.iter().any(|c| c == name);
                    if has {
                        if owner.is_some() {
                            return Ok(None);
                        }
                        owner = Some(idx);
                    }
                }
                Ok(owner.filter(|i| self.instances[*i].dist.as_deref() == Some(name)))
            }
        }
    }
}

/// Outcome of analysing the keys of one query level and everything nested
/// in it.
struct Keys {
    /// Key sets, one per group of co-located tables that has a filter.
    groups: KeyGroups,
    /// Number of groups of distributed tables at the top level without a
    /// key filter.
    open_groups: usize,
    /// A nested query reads distributed tables without a key filter.
    nested_open: bool,
    /// The statement reads distributed tables at all.
    distributed: bool,
    undeclared: Option<String>,
}

fn level_keys(level: &Level, env: &Env, top: bool) -> Result<Keys> {
    let n = level.instances.len();
    // Union-find over instances joined on their distribution columns.
    let mut parent: Vec<usize> = (0..n).collect();
    fn find(p: &mut [usize], mut i: usize) -> usize {
        while p[i] != i {
            p[i] = p[p[i]];
            i = p[i];
        }
        i
    }
    let mut filters: HashMap<usize, Vec<Operand>> = HashMap::new();
    for cond in &level.conds {
        let Some(N::AExpr(e)) = cond.node.as_ref() else { continue };
        let kind = pb::AExprKind::try_from(e.kind).unwrap_or(pb::AExprKind::Undefined);
        let op = e.name.last().and_then(string_of).unwrap_or_default();
        if op != "=" {
            continue;
        }
        let (Some(l), Some(r)) = (&e.lexpr, &e.rexpr) else { continue };
        match kind {
            pb::AExprKind::AexprOp => {
                let col = |x: &pb::Node| match x.node.as_ref() {
                    Some(N::ColumnRef(c)) => level.dist_instance(c, env),
                    _ => Ok(None),
                };
                match (col(l)?, col(r)?) {
                    (Some(a), Some(b)) => {
                        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
                        parent[ra] = rb;
                    }
                    (Some(a), None) => {
                        if let Some(o) = operand(r) {
                            filters.entry(a).or_insert_with(|| vec![o]);
                        }
                    }
                    (None, Some(b)) => {
                        if let Some(o) = operand(l) {
                            filters.entry(b).or_insert_with(|| vec![o]);
                        }
                    }
                    _ => {}
                }
            }
            pb::AExprKind::AexprIn => {
                let Some(N::ColumnRef(c)) = l.node.as_ref() else { continue };
                let (Some(a), Some(N::List(list))) = (level.dist_instance(c, env)?, r.node.as_ref()) else { continue };
                let ops: Option<Vec<Operand>> = list.items.iter().map(operand).collect();
                if let Some(ops) = ops {
                    filters.entry(a).or_insert(ops);
                }
            }
            _ => {}
        }
    }
    for (name, left, right) in &level.using {
        let side = |aliases: &Vec<String>| -> Vec<usize> {
            level
                .instances
                .iter()
                .enumerate()
                .filter(|(_, i)| aliases.contains(&i.alias) && i.dist.as_deref() == Some(name.as_str()))
                .map(|(idx, _)| idx)
                .collect()
        };
        for a in side(left) {
            for b in side(right) {
                let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
                parent[ra] = rb;
            }
        }
    }

    let mut out = Keys {
        groups: Vec::new(),
        open_groups: 0,
        nested_open: false,
        distributed: level.distributed().next().is_some(),
        undeclared: level.undeclared.clone(),
    };
    let mut roots: Vec<usize> = Vec::new();
    for (i, instance) in level.instances.iter().enumerate() {
        if instance.dist.is_none() {
            continue;
        }
        let root = find(&mut parent, i);
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    for root in roots {
        let members: Vec<usize> =
            (0..n).filter(|i| level.instances[*i].dist.is_some() && find(&mut parent, *i) == root).collect();
        match members.iter().find_map(|m| filters.get(m).map(|f| (*m, f))) {
            Some((m, ops)) => out.groups.push((level.instances[m].table.clone(), ops.clone())),
            None => out.open_groups += 1,
        }
    }
    for sel in &level.nested {
        let inner = select_keys(sel, env)?;
        out.distributed |= inner.distributed;
        out.nested_open |= inner.nested_open || inner.open_groups > 0;
        out.groups.extend(inner.groups);
        if out.undeclared.is_none() {
            out.undeclared = inner.undeclared;
        }
    }
    let _ = top;
    Ok(out)
}

fn select_level(sel: &pb::SelectStmt, env: &Env) -> Result<Level> {
    let mut level = Level::default();
    if let Some(with) = &sel.with_clause {
        for c in &with.ctes {
            if let Some(N::CommonTableExpr(cte)) = c.node.as_ref() {
                match cte.ctequery.as_ref().and_then(|q| q.node.as_ref()) {
                    Some(N::SelectStmt(q)) => level.nested.push((**q).clone()),
                    Some(_) => return Err(unsupported("data-modifying statements in WITH")),
                    None => {}
                }
                level.ctes.push(cte.ctename.clone());
            }
        }
    }
    // Set operations: both sides are nested levels.
    if let (Some(l), Some(r)) = (&sel.larg, &sel.rarg) {
        level.nested.push((**l).clone());
        level.nested.push((**r).clone());
        return Ok(level);
    }
    for item in &sel.from_clause {
        level.add_from(item, env)?;
    }
    if let Some(w) = &sel.where_clause {
        conjuncts(w, &mut level.conds);
        level.collect_nested(w)?;
    }
    for t in sel.target_list.iter().chain(&sel.group_clause).chain(&sel.sort_clause).chain(&sel.distinct_clause) {
        if t.node.is_some() {
            level.collect_nested(t)?;
        }
    }
    if let Some(h) = &sel.having_clause {
        level.collect_nested(h)?;
    }
    for list in &sel.values_lists {
        level.collect_nested(list)?;
    }
    Ok(level)
}

fn select_keys(sel: &pb::SelectStmt, env: &Env) -> Result<Keys> {
    let level = select_level(sel, env)?;
    level_keys(&level, env, false)
}

pub(crate) fn undeclared_error(table: &str) -> Error {
    Error::new(
        sqlstate::FEATURE_NOT_SUPPORTED,
        format!("table \"{table}\" is neither distributed nor a reference table"),
    )
    .with_hint(format!(
        "Declare it with SELECT squad_distribute('{table}', '<column>') or SELECT squad_reference('{table}')."
    ))
}

fn missing_key_error(what: &str) -> Error {
    unsupported(what).with_hint(
        "Add a condition on the distribution column (for example project_id = …) for every distributed table, or join them on it.",
    )
}

/// A call `name('a', 'b')` without FROM, as used for the cluster's own
/// functions.
fn cluster_call(sel: &pb::SelectStmt) -> Option<(String, Vec<String>)> {
    if !sel.from_clause.is_empty() || sel.target_list.len() != 1 {
        return None;
    }
    let Some(N::ResTarget(t)) = sel.target_list[0].node.as_ref() else { return None };
    let Some(N::FuncCall(f)) = t.val.as_ref()?.node.as_ref() else { return None };
    let name = tree::func_name(f);
    if !matches!(name.as_str(), "squad_distribute" | "squad_reference") {
        return None;
    }
    let args: Option<Vec<String>> = f
        .args
        .iter()
        .map(|a| match operand(a) {
            Some(Operand::Const(squad::Value::Text(s))) => Some(s),
            _ => None,
        })
        .collect();
    Some((name, args?))
}

pub(crate) fn analyze(stmt: &pb::Node, text: &str, env: &Env) -> Result<Plan> {
    let single = env.shard_count == 1;
    match stmt.node.as_ref().ok_or_else(|| Error::internal("empty statement"))? {
        N::SelectStmt(sel) => {
            if let Some((name, args)) = cluster_call(sel) {
                return match (name.as_str(), args.as_slice()) {
                    ("squad_distribute", [table, column]) => {
                        Ok(Plan::Distribute { table: table.clone(), column: column.clone() })
                    }
                    ("squad_reference", [table]) => Ok(Plan::Reference { table: table.clone() }),
                    _ => Err(Error::new(
                        sqlstate::UNDEFINED_FUNCTION,
                        format!("{name} takes constant text arguments: squad_distribute('table', 'column'), squad_reference('table')"),
                    )),
                };
            }
            if !sel.locking_clause.is_empty() && single {
                return Ok(Plan::Keyed { groups: Vec::new(), write: true, scatter: None });
            }
            let level = select_level(sel, env)?;
            let keys = level_keys(&level, env, true)?;
            if let (Some(t), false) = (&keys.undeclared, single) {
                return Err(undeclared_error(t));
            }
            if !keys.distributed {
                return Ok(Plan::Anywhere);
            }
            let write = !sel.locking_clause.is_empty();
            if keys.open_groups == 0 && !keys.nested_open {
                // Several key values may still name several shards.
                let scatter = if write {
                    Some(Err(missing_key_error("SELECT … FOR UPDATE on several shards")))
                } else {
                    (keys.groups.len() == 1).then(|| scatter::plan(sel, text, env))
                };
                return Ok(Plan::Keyed { groups: keys.groups, write, scatter });
            }
            if single {
                return Ok(Plan::Keyed { groups: Vec::new(), write, scatter: None });
            }
            if write {
                return Err(missing_key_error("SELECT … FOR UPDATE on several shards"));
            }
            if keys.nested_open || keys.open_groups > 1 || !keys.groups.is_empty() {
                return Ok(Plan::Scatter(Err(missing_key_error(if keys.open_groups > 1 {
                    "a join of distributed tables that is not on their distribution columns"
                } else {
                    "a subquery or join that reads distributed tables without a condition on their distribution column"
                }))));
            }
            Ok(Plan::Scatter(scatter::plan(sel, text, env)))
        }
        N::InsertStmt(ins) => {
            let rv = ins.relation.as_ref().ok_or_else(|| Error::internal("INSERT without table"))?;
            let table = relname(rv);
            match env.meta.tables.get(&table) {
                Some(Class::Reference) => Ok(Plan::ReferenceWrite),
                None if single => Ok(Plan::Keyed { groups: Vec::new(), write: true, scatter: None }),
                None => Err(undeclared_error(&table)),
                Some(Class::Distributed { column, .. }) => {
                    if single {
                        return Ok(Plan::Keyed { groups: Vec::new(), write: true, scatter: None });
                    }
                    // Position of the distribution column among the targets.
                    let pos = if ins.cols.is_empty() {
                        (env.columns)(&table)?.iter().position(|c| c == column)
                    } else {
                        ins.cols
                            .iter()
                            .position(|c| matches!(c.node.as_ref(), Some(N::ResTarget(t)) if t.name == *column))
                    };
                    let Some(pos) = pos else {
                        return Err(unsupported(format!(
                            "INSERT into distributed table \"{table}\" without a value for its distribution column \"{column}\""
                        )));
                    };
                    let values = match ins.select_stmt.as_ref().and_then(|s| s.node.as_ref()) {
                        Some(N::SelectStmt(s)) if !s.values_lists.is_empty() && s.with_clause.is_none() => {
                            &s.values_lists
                        }
                        _ => {
                            return Err(unsupported(format!(
                                "INSERT … SELECT and DEFAULT VALUES into distributed table \"{table}\""
                            ))
                            .with_hint("Insert rows with VALUES so that each row can be sent to its shard."))
                        }
                    };
                    let mut keys = Vec::with_capacity(values.len());
                    for row in values {
                        let Some(N::List(list)) = row.node.as_ref() else {
                            return Err(Error::internal("malformed VALUES"));
                        };
                        if matches!(list.items.get(pos).and_then(|n| n.node.as_ref()), Some(N::AConst(c)) if c.isnull) {
                            return Err(Error::new(
                                sqlstate::NOT_NULL_VIOLATION,
                                "the distribution column cannot be null",
                            ));
                        }
                        let key = list.items.get(pos).and_then(operand).ok_or_else(|| {
                            unsupported(format!(
                                "an expression as the value of distribution column \"{column}\"; use a constant or a parameter"
                            ))
                        })?;
                        keys.push(key);
                    }
                    Ok(Plan::Insert(InsertRouting { stmt: (**ins).clone(), table, keys }))
                }
            }
        }
        N::UpdateStmt(_) | N::DeleteStmt(_) => {
            let (rv, extra, where_clause, with, what) = match stmt.node.as_ref() {
                Some(N::UpdateStmt(u)) => (&u.relation, &u.from_clause, &u.where_clause, &u.with_clause, "UPDATE"),
                Some(N::DeleteStmt(d)) => (&d.relation, &d.using_clause, &d.where_clause, &d.with_clause, "DELETE"),
                _ => unreachable!(),
            };
            if with.is_some() && !single {
                return Err(unsupported(format!("WITH in {what}")));
            }
            let rv = rv.as_ref().ok_or_else(|| Error::internal("statement without table"))?;
            let table = relname(rv);
            if single {
                return Ok(Plan::Keyed { groups: Vec::new(), write: true, scatter: None });
            }
            let mut level = Level::default();
            level.add_table(rv, env);
            for item in extra {
                level.add_from(item, env)?;
            }
            if let Some(w) = where_clause {
                conjuncts(w, &mut level.conds);
                level.collect_nested(w)?;
            }
            if let Some(N::UpdateStmt(u)) = stmt.node.as_ref() {
                for t in &u.target_list {
                    level.collect_nested(t)?;
                    // Moving a row to another shard is not possible.
                    if let (Some(N::ResTarget(rt)), Some(Class::Distributed { column, .. })) =
                        (t.node.as_ref(), env.meta.tables.get(&table))
                    {
                        if rt.name == *column {
                            return Err(unsupported(format!("updating distribution column \"{column}\"")));
                        }
                    }
                }
            }
            let keys = level_keys(&level, env, true)?;
            if let Some(t) = &keys.undeclared {
                return Err(undeclared_error(t));
            }
            match env.meta.tables.get(&table) {
                Some(Class::Reference) => {
                    if keys.distributed {
                        return Err(unsupported(format!("{what} of a reference table that reads distributed tables")));
                    }
                    Ok(Plan::ReferenceWrite)
                }
                Some(Class::Distributed { .. }) => {
                    if keys.open_groups == 0 && !keys.nested_open {
                        return Ok(Plan::Keyed { groups: keys.groups, write: true, scatter: None });
                    }
                    if keys.nested_open || keys.open_groups > 1 || !keys.groups.is_empty() {
                        return Err(missing_key_error(&format!("{what} that reads other distributed tables without a condition on their distribution column")));
                    }
                    Ok(Plan::WriteAll)
                }
                None => Err(undeclared_error(&table)),
            }
        }
        N::TransactionStmt(t) => {
            use pb::TransactionStmtKind as K;
            match K::try_from(t.kind).unwrap_or(K::Undefined) {
                K::TransStmtBegin | K::TransStmtStart => Ok(Plan::Begin),
                K::TransStmtCommit | K::TransStmtRollback => Ok(Plan::TxnControl { ends: true }),
                K::TransStmtSavepoint | K::TransStmtRelease | K::TransStmtRollbackTo => {
                    Ok(Plan::TxnControl { ends: false })
                }
                _ => Err(unsupported("two-phase commit")),
            }
        }
        N::VariableShowStmt(_) | N::ExplainStmt(_) => Ok(Plan::Anywhere),
        N::VariableSetStmt(_) | N::DiscardStmt(_) => Ok(Plan::Everywhere { ddl: false, dropped: Vec::new() }),
        N::DropStmt(d) => {
            let mut dropped = Vec::new();
            if d.remove_type == pb::ObjectType::ObjectTable as i32 {
                for o in &d.objects {
                    if let Some(N::List(l)) = o.node.as_ref() {
                        if let Some(name) = l.items.last().and_then(string_of) {
                            dropped.push(name.to_string());
                        }
                    }
                }
            }
            Ok(Plan::Everywhere { ddl: true, dropped })
        }
        N::RenameStmt(r) => {
            let table = r.relation.as_ref().map(relname).unwrap_or_default();
            match env.meta.tables.get(&table) {
                Some(_) if r.rename_type == pb::ObjectType::ObjectTable as i32 => Err(unsupported(format!(
                    "renaming \"{table}\", a distributed or reference table"
                ))
                .with_hint(
                    "The cluster records table classes by name. Create the table under the new name and move the rows.",
                )),
                Some(Class::Distributed { column, .. })
                    if r.rename_type == pb::ObjectType::ObjectColumn as i32 && r.subname == *column =>
                {
                    Err(unsupported(format!("renaming distribution column \"{column}\" of \"{table}\"")))
                }
                _ => Ok(Plan::Everywhere { ddl: true, dropped: Vec::new() }),
            }
        }
        N::AlterTableStmt(a) => {
            let table = a.relation.as_ref().map(relname).unwrap_or_default();
            if let Some(Class::Distributed { column, .. }) = env.meta.tables.get(&table) {
                for cmd in &a.cmds {
                    let Some(N::AlterTableCmd(c)) = cmd.node.as_ref() else { continue };
                    let changes = c.subtype == pb::AlterTableType::AtDropColumn as i32
                        || c.subtype == pb::AlterTableType::AtAlterColumnType as i32;
                    if changes && c.name == *column {
                        return Err(unsupported(format!(
                            "dropping or retyping distribution column \"{column}\" of \"{table}\""
                        )));
                    }
                }
            }
            Ok(Plan::Everywhere { ddl: true, dropped: Vec::new() })
        }
        N::CreateStmt(_)
        | N::IndexStmt(_)
        | N::CreateSeqStmt(_)
        | N::AlterSeqStmt(_)
        | N::CreateEnumStmt(_)
        | N::AlterEnumStmt(_)
        | N::ViewStmt(_)
        | N::CreateSchemaStmt(_)
        | N::TruncateStmt(_)
        | N::VacuumStmt(_)
        | N::CheckPointStmt(_)
        | N::CreateExtensionStmt(_)
        | N::CommentStmt(_)
        | N::GrantStmt(_) => Ok(Plan::Everywhere { ddl: true, dropped: Vec::new() }),
        N::CreateTableAsStmt(_) if single => Ok(Plan::Everywhere { ddl: true, dropped: Vec::new() }),
        N::CreateTableAsStmt(_) => Err(unsupported("CREATE TABLE AS")),
        N::PrepareStmt(_) | N::ExecuteStmt(_) | N::DeallocateStmt(_) => {
            Err(unsupported("SQL-level PREPARE / EXECUTE; use the connection's prepare()"))
        }
        other => {
            let dbg = format!("{other:?}");
            Err(unsupported(format!("statement {}", dbg.split(['(', ' ', '{']).next().unwrap_or("kind"))))
        }
    }
}

/// Whether `c` names the distribution column of a distributed table in
/// the FROM clause of `sel`.
pub(crate) fn is_dist_column(sel: &pb::SelectStmt, c: &pb::ColumnRef, env: &Env) -> Result<bool> {
    let level = select_level(sel, env)?;
    Ok(level.dist_instance(c, env)?.is_some())
}

/// Whether a table in the FROM clause of `sel` has a column `name`.
pub(crate) fn is_input_column(sel: &pb::SelectStmt, name: &str, env: &Env) -> Result<bool> {
    let level = select_level(sel, env)?;
    for inst in &level.instances {
        if (env.columns)(&inst.table)?.iter().any(|c| c == name) {
            return Ok(true);
        }
    }
    Ok(false)
}
