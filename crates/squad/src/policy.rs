//! Row-level security DDL: `CREATE`/`ALTER`/`DROP POLICY` and the
//! `ALTER TABLE ... ROW LEVEL SECURITY` actions. Enforcement is in
//! `bind::rls`.

use std::sync::Arc;

use pg_query::protobuf::{self as pb, node::Node as N};

use crate::bind::{deparse_expr, node, range_var_name, Binder};
use crate::catalog::{Catalog, Policy, PolicyCommand, Table};
use crate::ddl::{done, put_table};
use crate::session::{QueryResult, Session, SessionState};
use crate::{sqlstate, Error, Notice, Result};

/// In PostgreSQL only a table's owner may change its row-level security
/// and policies. squad has no owners: logins that bypass row-level
/// security count as owners, the others may not.
pub(crate) fn require_owner(state: &SessionState, table: &Table) -> Result<()> {
    if state.bypass_rls {
        return Ok(());
    }
    Err(Error::new(sqlstate::INSUFFICIENT_PRIVILEGE, format!("must be owner of table {}", table.name)))
}

/// A login subject to row-level security may not change, drop or
/// truncate a table that has it enabled: that would get around the
/// policies.
pub(crate) fn require_owner_if_protected(state: &SessionState, table: &Table) -> Result<()> {
    if table.row_security {
        require_owner(state, table)?;
    }
    Ok(())
}

/// The table a policy statement names.
fn policy_table(catalog: &Catalog, rv: &pb::RangeVar, path: &[String]) -> Result<Arc<Table>> {
    let (schema, name) = range_var_name(rv);
    if let Some(t) = catalog.table(schema, name, path) {
        return Ok(t.clone());
    }
    if catalog.view(schema, name, path).is_some() {
        return Err(Error::new(sqlstate::WRONG_OBJECT_TYPE, format!("\"{name}\" is not a table")));
    }
    if crate::system::table_columns(schema, name).is_some() {
        return Err(Error::new(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!("permission denied: \"{name}\" is a system catalog"),
        ));
    }
    Err(Error::new(sqlstate::UNDEFINED_TABLE, format!("relation \"{name}\" does not exist")))
}

/// `TO role, ...`: login names, or empty for PUBLIC. `CURRENT_USER`,
/// `CURRENT_ROLE` and `SESSION_USER` stand for the login creating the
/// policy. Names are not checked: there are no roles in SQL.
fn policy_roles(sess: &mut Session, roles: &[pb::Node]) -> Result<Vec<String>> {
    use pb::RoleSpecType as R;
    let mut names = Vec::new();
    let mut public = false;
    for r in roles {
        let N::RoleSpec(spec) = node(r)? else { return Err(Error::internal("malformed role list")) };
        match R::try_from(spec.roletype).unwrap_or(R::Undefined) {
            R::RolespecPublic => public = true,
            R::RolespecCstring => names.push(spec.rolename.clone()),
            _ => names.push(sess.state.user.clone()),
        }
    }
    if public {
        if !names.is_empty() {
            sess.notice(
                Notice::warning("ignoring specified roles other than PUBLIC")
                    .with_hint("All roles are members of the PUBLIC role."),
            );
        }
        return Ok(Vec::new());
    }
    names.dedup();
    Ok(names)
}

/// Checks a policy expression against the table and returns its text.
fn policy_expr(catalog: &Catalog, path: &[String], table: &Arc<Table>, expr: &pb::Node) -> Result<String> {
    let sql = deparse_expr(expr)?;
    let mut binder = Binder::new(catalog, path, Vec::new());
    binder.validate_policy_expr(table, &sql)?;
    if !binder.param_types.is_empty() {
        return Err(Error::new(sqlstate::UNDEFINED_PARAMETER, "there is no parameter $1"));
    }
    Ok(sql)
}

fn command_of(name: &str) -> Result<PolicyCommand> {
    Ok(match name {
        "" | "all" => PolicyCommand::All,
        "select" => PolicyCommand::Select,
        "insert" => PolicyCommand::Insert,
        "update" => PolicyCommand::Update,
        "delete" => PolicyCommand::Delete,
        other => return Err(Error::internal(format!("unrecognized policy command \"{other}\""))),
    })
}

fn undefined_policy(name: &str, table: &str) -> Error {
    Error::new(sqlstate::UNDEFINED_OBJECT, format!("policy \"{name}\" for table \"{table}\" does not exist"))
}

fn duplicate_policy(name: &str, table: &str) -> Error {
    Error::new(sqlstate::DUPLICATE_OBJECT, format!("policy \"{name}\" for table \"{table}\" already exists"))
}

pub(crate) fn create_policy(sess: &mut Session, stmt: &pb::CreatePolicyStmt) -> Result<QueryResult> {
    let rv = stmt.table.as_ref().ok_or_else(|| Error::internal("CREATE POLICY without table"))?;
    let command = command_of(&stmt.cmd_name)?;
    if matches!(command, PolicyCommand::Select | PolicyCommand::Delete) && stmt.with_check.is_some() {
        return Err(Error::syntax("WITH CHECK cannot be applied to SELECT or DELETE"));
    }
    if command == PolicyCommand::Insert && stmt.qual.is_some() {
        return Err(Error::syntax("only WITH CHECK expression allowed for INSERT"));
    }
    let roles = policy_roles(sess, &stmt.roles)?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, txn, _, state| {
        let table = policy_table(catalog, rv, &path)?;
        require_owner(state, &table)?;
        Session::lock_table_exclusive(txn, table.id, squad_kv::WaitPolicy::Wait(state.lock_timeout))?;
        if table.policies.iter().any(|p| p.name == stmt.policy_name) {
            return Err(duplicate_policy(&stmt.policy_name, &table.name));
        }
        let using = stmt.qual.as_ref().map(|q| policy_expr(catalog, &path, &table, q)).transpose()?;
        let check = stmt.with_check.as_ref().map(|q| policy_expr(catalog, &path, &table, q)).transpose()?;
        let mut t = (*table).clone();
        t.policies.push(Policy {
            id: catalog.alloc_id(),
            name: stmt.policy_name.clone(),
            permissive: stmt.permissive,
            command,
            roles,
            using,
            check,
        });
        put_table(catalog, t);
        Ok(())
    })?;
    done("CREATE POLICY")
}

pub(crate) fn alter_policy(sess: &mut Session, stmt: &pb::AlterPolicyStmt) -> Result<QueryResult> {
    let rv = stmt.table.as_ref().ok_or_else(|| Error::internal("ALTER POLICY without table"))?;
    let roles = if stmt.roles.is_empty() { None } else { Some(policy_roles(sess, &stmt.roles)?) };
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, txn, _, state| {
        let table = policy_table(catalog, rv, &path)?;
        require_owner(state, &table)?;
        Session::lock_table_exclusive(txn, table.id, squad_kv::WaitPolicy::Wait(state.lock_timeout))?;
        let mut t = (*table).clone();
        let pos = t
            .policies
            .iter()
            .position(|p| p.name == stmt.policy_name)
            .ok_or_else(|| undefined_policy(&stmt.policy_name, &table.name))?;
        let command = t.policies[pos].command;
        if matches!(command, PolicyCommand::Select | PolicyCommand::Delete) && stmt.with_check.is_some() {
            return Err(Error::syntax("only USING expression allowed for SELECT, DELETE"));
        }
        if command == PolicyCommand::Insert && stmt.qual.is_some() {
            return Err(Error::syntax("only WITH CHECK expression allowed for INSERT"));
        }
        let using = stmt.qual.as_ref().map(|q| policy_expr(catalog, &path, &table, q)).transpose()?;
        let check = stmt.with_check.as_ref().map(|q| policy_expr(catalog, &path, &table, q)).transpose()?;
        let p = &mut t.policies[pos];
        if let Some(roles) = roles.clone() {
            p.roles = roles;
        }
        if using.is_some() {
            p.using = using;
        }
        if check.is_some() {
            p.check = check;
        }
        put_table(catalog, t);
        Ok(())
    })?;
    done("ALTER POLICY")
}

/// `ALTER POLICY name ON table RENAME TO new`.
pub(crate) fn rename_policy(
    catalog: &mut Catalog,
    state: &SessionState,
    rv: &pb::RangeVar,
    path: &[String],
    old: &str,
    new: &str,
) -> Result<()> {
    let table = policy_table(catalog, rv, path)?;
    require_owner(state, &table)?;
    if table.policies.iter().any(|p| p.name == new) {
        return Err(duplicate_policy(new, &table.name));
    }
    let mut t = (*table).clone();
    let p = t.policies.iter_mut().find(|p| p.name == old).ok_or_else(|| undefined_policy(old, &table.name))?;
    p.name = new.to_string();
    put_table(catalog, t);
    Ok(())
}

/// `DROP POLICY [IF EXISTS] name ON table`; `parts` is the table's name
/// followed by the policy's.
pub(crate) fn drop_policy(
    catalog: &mut Catalog,
    state: &mut SessionState,
    parts: &[String],
    path: &[String],
    missing_ok: bool,
) -> Result<()> {
    let Some((policy, table_parts)) = parts.split_last() else {
        return Err(Error::internal("DROP POLICY without name"));
    };
    let (schema, name) = crate::bind::qualified(table_parts);
    let Some(table) = catalog.table(schema, name, path).cloned() else {
        if missing_ok {
            state.notices.push(Notice::info(format!("relation \"{name}\" does not exist, skipping")));
            return Ok(());
        }
        let rv = pb::RangeVar {
            schemaname: schema.unwrap_or_default().to_string(),
            relname: name.to_string(),
            ..Default::default()
        };
        return policy_table(catalog, &rv, path).map(|_| ());
    };
    require_owner(state, &table)?;
    let Some(pos) = table.policies.iter().position(|p| p.name == *policy) else {
        if missing_ok {
            state.notices.push(Notice::info(format!(
                "policy \"{policy}\" for relation \"{}\" does not exist, skipping",
                table.name
            )));
            return Ok(());
        }
        return Err(undefined_policy(policy, &table.name));
    };
    let mut t = (*table).clone();
    t.policies.remove(pos);
    put_table(catalog, t);
    Ok(())
}

/// `ALTER TABLE ... ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY`.
pub(crate) fn set_row_security(
    catalog: &mut Catalog,
    state: &SessionState,
    table: &Table,
    enable: Option<bool>,
    force: Option<bool>,
) -> Result<()> {
    require_owner(state, table)?;
    let mut t = catalog.table_by_id(table.id).map(|t| (**t).clone()).unwrap_or_else(|| table.clone());
    if let Some(e) = enable {
        t.row_security = e;
    }
    if let Some(f) = force {
        t.force_row_security = f;
    }
    put_table(catalog, t);
    Ok(())
}

/// Policies of `table` whose expressions mention `column`. Used to keep
/// columns that policies depend on from being dropped, renamed or
/// changed in type.
pub(crate) fn policies_using_column<'t>(table: &'t Table, column: &str) -> Vec<&'t Policy> {
    if column.is_empty() {
        return Vec::new();
    }
    let mentions = |sql: &str| {
        sql.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .any(|word| word == column || word.trim_matches('"') == column)
    };
    table
        .policies
        .iter()
        .filter(|p| p.using.as_deref().is_some_and(mentions) || p.check.as_deref().is_some_and(mentions))
        .collect()
}
