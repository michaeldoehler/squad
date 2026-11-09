//! Execution of INSERT, UPDATE and DELETE: row and index maintenance and
//! constraint enforcement.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use squad_kv::{Isolation, LockMode};

use crate::catalog::{object_prefix, FkAction, ForeignKey, Index, Table};
use crate::encoding::{encode_key, encode_row};
use crate::exec::{collect, decode_table_row, refresh_locked_row, row_key, ExecCx, Refreshed};
use crate::expr::{Expr, Row};
use crate::plan::{DeletePlan, InsertPlan, OnConflict, UpdatePlan};
use crate::types::Value;
use crate::value::format_value;
use crate::{sqlstate, Error, Result};

fn show(v: &Value, cx: &ExecCx) -> String {
    match v {
        Value::Null => "null".into(),
        v => format_value(v, &cx.fn_ctx),
    }
}

fn show_list(row: &[Value], cols: &[usize], cx: &ExecCx) -> String {
    cols.iter().map(|&c| show(&row[c], cx)).collect::<Vec<_>>().join(", ")
}

fn col_names(table: &Table, cols: &[usize]) -> String {
    cols.iter().map(|&c| table.columns[c].name.as_str()).collect::<Vec<_>>().join(", ")
}

/// Storage key and value of a row's entry in an index. Entries of unique
/// indexes carry only the indexed values, so that equal values collide;
/// if a value is NULL (never equal) or the index is not unique, the row's
/// key is appended to keep entries apart.
pub(crate) fn index_entry(table: &Table, index: &Index, row: &[Value]) -> (Vec<u8>, Vec<u8>, bool) {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&object_prefix(index.id));
    let mut has_null = false;
    for &c in &index.columns {
        has_null |= row[c].is_null();
        encode_key(&mut key, &row[c]);
    }
    let pk = row_key(table, row)[4..].to_vec();
    let enforce = index.unique && (!has_null || index.nulls_not_distinct);
    if !enforce {
        key.extend_from_slice(&pk);
    }
    (key, pk, enforce)
}

fn unique_violation(table: &Table, name: &str, cols: &[usize], row: &[Value], cx: &ExecCx) -> Error {
    Error::new(sqlstate::UNIQUE_VIOLATION, format!("duplicate key value violates unique constraint \"{name}\""))
        .with_detail(format!("Key ({})=({}) already exists.", col_names(table, cols), show_list(row, cols, cx)))
}

fn check_row(table: &Table, checks: &[(String, Expr)], row: &[Value], cx: &mut ExecCx) -> Result<()> {
    for (i, col) in table.columns.iter().enumerate() {
        if col.not_null && row[i].is_null() {
            return Err(Error::new(
                sqlstate::NOT_NULL_VIOLATION,
                format!(
                    "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                    col.name, table.name
                ),
            )
            .with_detail(format!("Failing row contains ({}).", failing_row(table, row, cx))));
        }
    }
    for (name, pred) in checks {
        // A CHECK passes unless it is definitely false.
        if pred.eval(row, cx)? == Value::Bool(false) {
            return Err(Error::new(
                sqlstate::CHECK_VIOLATION,
                format!("new row for relation \"{}\" violates check constraint \"{name}\"", table.name),
            )
            .with_detail(format!("Failing row contains ({}).", failing_row(table, row, cx))));
        }
    }
    Ok(())
}

fn failing_row(table: &Table, row: &[Value], cx: &ExecCx) -> String {
    let cols: Vec<usize> = table.visible_columns().map(|(i, _)| i).collect();
    show_list(row, &cols, cx)
}

/// Which unique key an insert collided with.
enum Conflict {
    Primary,
    Index(usize),
}

/// Locks the row's unique keys and reports the first one already taken.
fn find_conflict(table: &Table, row: &[Value], key: &[u8], skip_pk: bool, cx: &mut ExecCx) -> Result<Option<Conflict>> {
    let wait = cx.wait_policy();
    if !skip_pk {
        cx.txn.lock(key, LockMode::Exclusive, wait)?;
        if cx.txn.get_current(key)?.is_some() {
            return Ok(Some(Conflict::Primary));
        }
    }
    for (i, index) in table.indexes.iter().enumerate() {
        if index.primary || !index.unique {
            continue;
        }
        let (ikey, _, enforce) = index_entry(table, index, row);
        if !enforce {
            continue;
        }
        cx.txn.lock(&ikey, LockMode::Exclusive, wait)?;
        if let Some(owner) = cx.txn.get_current(&ikey)? {
            // An entry pointing at this very row is not a conflict (the
            // row is being updated in place).
            if owner != key[4..] {
                return Ok(Some(Conflict::Index(i)));
            }
        }
    }
    Ok(None)
}

fn conflict_error(table: &Table, conflict: &Conflict, row: &[Value], cx: &ExecCx) -> Error {
    match conflict {
        Conflict::Primary => {
            let name = table.pk_name().map(str::to_string).unwrap_or_else(|| format!("{}_pkey", table.name));
            unique_violation(table, &name, &table.pk, row, cx)
        }
        Conflict::Index(i) => {
            let ix = &table.indexes[*i];
            unique_violation(table, &ix.name, &ix.columns, row, cx)
        }
    }
}

fn write_index_entries(table: &Table, row: &[Value], cx: &mut ExecCx) -> Result<()> {
    for index in table.indexes.iter().filter(|i| !i.primary) {
        let (ikey, pk, _) = index_entry(table, index, row);
        cx.txn.put_blind(&ikey, &pk)?;
    }
    Ok(())
}

fn delete_index_entries(table: &Table, row: &[Value], cx: &mut ExecCx) -> Result<()> {
    for index in table.indexes.iter().filter(|i| !i.primary) {
        let (ikey, _, _) = index_entry(table, index, row);
        cx.txn.delete_blind(&ikey)?;
    }
    Ok(())
}

/// Checks that the rows referenced by this row's foreign keys exist, and
/// share-locks them so they cannot disappear before commit.
fn check_outgoing_fks(
    table: &Table,
    row: &[Value],
    only_changed_from: Option<&[Value]>,
    cx: &mut ExecCx,
) -> Result<()> {
    for fk in &table.foreign_keys {
        if fk.columns.iter().any(|&c| row[c].is_null()) {
            continue;
        }
        if let Some(old) = only_changed_from {
            if fk.columns.iter().all(|&c| crate::expr::sql_eq(&row[c], &old[c]) == Some(true)) {
                continue;
            }
        }
        let parent = cx
            .catalog
            .table_by_id(fk.ref_table)
            .cloned()
            .ok_or_else(|| Error::internal("foreign key references a missing table"))?;
        let mut key = Vec::with_capacity(24);
        let found = if fk.ref_columns == parent.pk {
            key.extend_from_slice(&object_prefix(parent.id));
            for &c in &fk.columns {
                encode_key(&mut key, &row[c]);
            }
            let wait = cx.wait_policy();
            cx.txn.lock(&key, LockMode::Shared, wait)?;
            cx.txn.get_current(&key)?.is_some()
        } else {
            let index = parent
                .indexes
                .iter()
                .find(|ix| ix.unique && ix.columns == fk.ref_columns)
                .ok_or_else(|| Error::internal("foreign key without a matching unique index"))?;
            key.extend_from_slice(&object_prefix(index.id));
            for &c in &fk.columns {
                encode_key(&mut key, &row[c]);
            }
            let wait = cx.wait_policy();
            cx.txn.lock(&key, LockMode::Shared, wait)?;
            match cx.txn.get_current(&key)? {
                Some(pk) => {
                    // Lock the parent row itself as well.
                    let mut rk = object_prefix(parent.id).to_vec();
                    rk.extend_from_slice(&pk);
                    cx.txn.lock(&rk, LockMode::Shared, wait)?;
                    cx.txn.get_current(&rk)?.is_some()
                }
                None => false,
            }
        };
        if !found {
            return Err(Error::new(
                sqlstate::FOREIGN_KEY_VIOLATION,
                format!("insert or update on table \"{}\" violates foreign key constraint \"{}\"", table.name, fk.name),
            )
            .with_detail(format!(
                "Key ({})=({}) is not present in table \"{}\".",
                col_names(table, &fk.columns),
                show_list(row, &fk.columns, cx),
                parent.name
            )));
        }
    }
    Ok(())
}

/// Rows of `child` whose foreign key columns equal `values`. Looks at this
/// transaction's view and at the latest committed state, so that children
/// committed after the statement's snapshot are not missed.
fn referencing_rows(child: &Arc<Table>, fk: &ForeignKey, values: &[Value], cx: &mut ExecCx) -> Result<Vec<Row>> {
    let index = child.indexes.iter().find(|ix| !ix.primary && ix.columns.starts_with(&fk.columns));
    let by_pk = child.pk.starts_with(&fk.columns) && !child.rowid;
    let mut prefix = Vec::with_capacity(24);
    prefix.extend_from_slice(&object_prefix(match (by_pk, index) {
        (true, _) | (false, None) => child.id,
        (false, Some(ix)) => ix.id,
    }));
    let full_scan = !by_pk && index.is_none();
    if !full_scan {
        for v in values {
            encode_key(&mut prefix, v);
        }
    }
    let mut hi = prefix.clone();
    let hi = loop {
        match hi.pop() {
            Some(0xFF) => continue,
            Some(b) => {
                hi.push(b + 1);
                break Bound::Excluded(hi);
            }
            None => break Bound::Unbounded,
        }
    };
    let hi_ref = match &hi {
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        _ => Bound::Unbounded,
    };

    let fresh = cx.shared.store.begin(Isolation::ReadCommitted);
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut out = Vec::new();
    for view in 0..2 {
        let entries = if view == 0 {
            cx.txn.scan(Bound::Included(&prefix), hi_ref, false, usize::MAX)?
        } else {
            fresh.scan(Bound::Included(&prefix), hi_ref, false, usize::MAX)?
        };
        for (k, v) in entries {
            let rk = if by_pk || full_scan {
                k
            } else {
                let mut rk = object_prefix(child.id).to_vec();
                rk.extend_from_slice(&v);
                rk
            };
            if !seen.insert(rk.clone()) {
                continue;
            }
            // Always judge by the newest version of the child row.
            let Some(bytes) = cx.txn.get_current(&rk)? else { continue };
            let row = decode_table_row(child, &bytes)?;
            let matches = fk.columns.iter().zip(values).all(|(&c, v)| crate::expr::sql_eq(&row[c], v) == Some(true));
            if matches {
                out.push(row);
            }
        }
    }
    Ok(out)
}

/// Applies the referential actions of foreign keys pointing at a row that
/// is deleted (`new == None`) or whose referenced columns change.
fn handle_incoming_fks(table: &Arc<Table>, old: &[Value], new: Option<&[Value]>, cx: &mut ExecCx) -> Result<()> {
    let referencing = cx.catalog.referencing(table.id);
    for (child, fk) in referencing {
        if let Some(new) = new {
            if fk.ref_columns.iter().all(|&c| crate::expr::sql_eq(&old[c], &new[c]) == Some(true)) {
                continue;
            }
        }
        let values: Vec<Value> = fk.ref_columns.iter().map(|&c| old[c].clone()).collect();
        if values.iter().any(Value::is_null) {
            continue;
        }
        let rows = referencing_rows(&child, &fk, &values, cx)?;
        if rows.is_empty() {
            continue;
        }
        let action = if new.is_some() { fk.on_update } else { fk.on_delete };
        match action {
            FkAction::NoAction | FkAction::Restrict => {
                return Err(Error::new(
                    sqlstate::FOREIGN_KEY_VIOLATION,
                    format!(
                        "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
                        table.name, fk.name, child.name
                    ),
                )
                .with_detail(format!(
                    "Key ({})=({}) is still referenced from table \"{}\".",
                    col_names(table, &fk.ref_columns),
                    show_list(old, &fk.ref_columns, cx),
                    child.name
                )));
            }
            FkAction::Cascade if new.is_none() => {
                for row in rows {
                    let key = row_key(&child, &row);
                    let wait = cx.wait_policy();
                    cx.txn.lock(&key, LockMode::Exclusive, wait)?;
                    delete_row(&child, &row, cx)?;
                }
            }
            FkAction::Cascade | FkAction::SetNull | FkAction::SetDefault => {
                if action == FkAction::SetDefault {
                    return Err(Error::unsupported("ON DELETE/UPDATE SET DEFAULT"));
                }
                for row in rows {
                    let key = row_key(&child, &row);
                    let wait = cx.wait_policy();
                    cx.txn.lock(&key, LockMode::Exclusive, wait)?;
                    let mut updated = row.clone();
                    for (&c, &r) in fk.columns.iter().zip(&fk.ref_columns) {
                        updated[c] = match (action, new) {
                            (FkAction::Cascade, Some(new)) => new[r].clone(),
                            _ => Value::Null,
                        };
                    }
                    update_row(&child, &[], &row, updated, cx)?;
                }
            }
        }
    }
    Ok(())
}

/// Writes a new row whose unique keys were already verified.
fn store_row(table: &Table, key: &[u8], row: &[Value], cx: &mut ExecCx) -> Result<()> {
    cx.txn.put_blind(key, &encode_row(row))?;
    write_index_entries(table, row, cx)
}

fn delete_row(table: &Arc<Table>, row: &[Value], cx: &mut ExecCx) -> Result<()> {
    handle_incoming_fks(table, row, None, cx)?;
    cx.txn.delete_blind(&row_key(table, row))?;
    delete_index_entries(table, row, cx)
}

/// Replaces `old` by `new`, maintaining indexes and constraints. The
/// caller holds the exclusive lock on the old row.
fn update_row(table: &Arc<Table>, checks: &[(String, Expr)], old: &[Value], new: Row, cx: &mut ExecCx) -> Result<()> {
    check_row(table, checks, &new, cx)?;
    let old_key = row_key(table, old);
    let new_key = row_key(table, &new);
    let moved = old_key != new_key;
    if let Some(conflict) = find_conflict(table, &new, &new_key, !moved, cx)? {
        return Err(conflict_error(table, &conflict, &new, cx));
    }
    check_outgoing_fks(table, &new, Some(old), cx)?;
    handle_incoming_fks(table, old, Some(&new), cx)?;
    if moved {
        cx.txn.delete_blind(&old_key)?;
    }
    // Only touch index entries whose key actually changes.
    for index in table.indexes.iter().filter(|i| !i.primary) {
        let (okey, _, _) = index_entry(table, index, old);
        let (nkey, pk, _) = index_entry(table, index, &new);
        if okey != nkey {
            cx.txn.delete_blind(&okey)?;
            cx.txn.put_blind(&nkey, &pk)?;
        } else if moved {
            cx.txn.put_blind(&nkey, &pk)?;
        }
    }
    cx.txn.put_blind(&new_key, &encode_row(&new))?;
    Ok(())
}

fn returning(exprs: &Option<Vec<Expr>>, row: &[Value], out: &mut Vec<Row>, cx: &mut ExecCx) -> Result<()> {
    if let Some(exprs) = exprs {
        out.push(exprs.iter().map(|e| e.eval(row, cx)).collect::<Result<Row>>()?);
    }
    Ok(())
}

pub(crate) fn insert(plan: &InsertPlan, cx: &mut ExecCx) -> Result<Vec<Row>> {
        let table = &plan.table;
    let mut out = Vec::new();
    // Rows this statement inserted or updated; ON CONFLICT DO UPDATE may
    // not touch any of them again.
    let mut touched: HashSet<Vec<u8>> = HashSet::new();
    let track = matches!(plan.on_conflict, Some(OnConflict::Update { .. }));
    let mut source = crate::exec::open(&plan.source, cx)?;
    // Materialize when the source could observe this statement's writes.
    let buffered = match &plan.source {
        crate::plan::Plan::Values(_) => None,
        _ => {
            let mut rows = Vec::new();
            while let Some(r) = source.next(cx)? {
                rows.push(r);
            }
            Some(rows.into_iter())
        }
    };
        let mut buffered = buffered;
    // Foreign keys are verified when the statement ends, as in Postgres:
    // rows of one statement may reference each other in any order, and
    // other constraint errors take precedence.
    let mut fk_pending: Vec<Row> = Vec::new();
    loop {
        let row = match &mut buffered {
            Some(it) => it.next(),
            None => source.next(cx)?,
        };
        let Some(row) = row else { break };
        check_row(table, &plan.checks, &row, cx)?;
        let key = row_key(table, &row);
        match find_conflict(table, &row, &key, false, cx)? {
                        None => {
                if !table.foreign_keys.is_empty() {
                    fk_pending.push(row.clone());
                }
                store_row(table, &key, &row, cx)?;
                if track {
                    touched.insert(key);
                }
                cx.rows_affected += 1;
                returning(&plan.returning, &row, &mut out, cx)?;
            }
            Some(conflict) => {
                let arbiter = |target: &Option<usize>| match (&conflict, target) {
                    (Conflict::Primary, None) => true,
                    (Conflict::Index(i), Some(t)) => i == t,
                    _ => false,
                };
                match &plan.on_conflict {
                    Some(OnConflict::Nothing { target: None }) => continue,
                    Some(OnConflict::Nothing { target: Some(t) }) if arbiter(t) => continue,
                    Some(OnConflict::Update { target, set, filter }) if arbiter(target) => {
                        // Find and lock the existing row.
                        let existing_key = match &conflict {
                            Conflict::Primary => key.clone(),
                            Conflict::Index(i) => {
                                let (ikey, _, _) = index_entry(table, &table.indexes[*i], &row);
                                let pk = cx
                                    .txn
                                    .get_current(&ikey)?
                                    .ok_or_else(|| Error::internal("conflicting index entry vanished"))?;
                                let mut k = object_prefix(table.id).to_vec();
                                k.extend_from_slice(&pk);
                                k
                            }
                        };
                                                                        if touched.contains(&existing_key) {
                            return Err(Error::new(
                                sqlstate::CARDINALITY_VIOLATION,
                                "ON CONFLICT DO UPDATE command cannot affect row a second time",
                            )
                            .with_hint(
                                "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.",
                            ));
                        }
                        let wait = cx.wait_policy();
                        cx.txn.lock(&existing_key, LockMode::Exclusive, wait)?;
                        let Some(bytes) = cx.txn.get_current(&existing_key)? else {
                            return Err(Error::internal("conflicting row vanished"));
                        };
                        let existing = decode_table_row(table, &bytes)?;
                        let mut both = existing.clone();
                        both.extend(row.iter().cloned());
                        if let Some(f) = filter {
                            if !f.test(&both, cx)? {
                                continue;
                            }
                        }
                        let mut updated = existing.clone();
                        for (col, e) in set {
                            updated[*col] = e.eval(&both, cx)?;
                        }
                                                update_row(table, &plan.checks, &existing, updated.clone(), cx)?;
                        touched.insert(row_key(table, &updated));
                        cx.rows_affected += 1;
                        returning(&plan.returning, &updated, &mut out, cx)?;
                    }
                                        _ => return Err(conflict_error(table, &conflict, &row, cx)),
                }
            }
        }
    }
    for row in &fk_pending {
        check_outgoing_fks(table, row, None, cx)?;
    }
    Ok(out)
}

/// Locks a row found by a scan and brings it up to date. Returns `None`
/// if the row is gone or no longer qualifies.
fn lock_target(
    table: &Arc<Table>,
    mut src: Row,
    recheck: &Option<Expr>,
    done: &mut HashSet<Vec<u8>>,
    cx: &mut ExecCx,
) -> Result<Option<(Vec<u8>, Row)>> {
    let width = table.columns.len();
    let key = row_key(table, &src[..width]);
    // A join may produce the same target row several times.
    if !done.insert(key.clone()) {
        return Ok(None);
    }
    let wait = cx.wait_policy();
    cx.txn.lock(&key, LockMode::Exclusive, wait)?;
    match refresh_locked_row(cx, table, &key)? {
        Refreshed::Unchanged => {}
        Refreshed::Deleted => return Ok(None),
        Refreshed::Changed(new) => {
            src.splice(..width, new);
            if let Some(pred) = recheck {
                if !pred.test(&src, cx)? {
                    return Ok(None);
                }
            }
        }
    }
    // The row may have been deleted earlier in this transaction's
    // statement (e.g. through a cascade).
    if cx.txn.get(&key)?.is_none() {
        return Ok(None);
    }
    Ok(Some((key, src)))
}

pub(crate) fn update(plan: &UpdatePlan, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let table = &plan.table;
    let width = table.columns.len();
    let sources = collect(&plan.source, cx)?;
    let mut done = HashSet::new();
    let mut out = Vec::new();
        // Compute every new row before writing any: the SET expressions (and
    // subqueries in them) must see the table as it was when the statement
    // started, not rows this statement already changed.
    let mut changes = Vec::with_capacity(sources.len());
    for src in sources {
        let Some((_, src)) = lock_target(table, src, &plan.recheck, &mut done, cx)? else { continue };
        let old: Row = src[..width].to_vec();
        let mut new = old.clone();
        for (col, e) in &plan.set {
            new[*col] = e.eval(&src, cx)?;
        }
        changes.push((src, old, new));
    }
    for (mut src, old, new) in changes {
        update_row(table, &plan.checks, &old, new.clone(), cx)?;
        cx.rows_affected += 1;
        if plan.returning.is_some() {
            src.splice(..width, new);
            returning(&plan.returning, &src, &mut out, cx)?;
        }
    }
    Ok(out)
}

pub(crate) fn delete(plan: &DeletePlan, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let table = &plan.table;
    let width = table.columns.len();
    let sources = collect(&plan.source, cx)?;
    let mut done = HashSet::new();
    let mut out = Vec::new();
    for src in sources {
        let Some((_, src)) = lock_target(table, src, &plan.recheck, &mut done, cx)? else { continue };
        delete_row(table, &src[..width], cx)?;
        cx.rows_affected += 1;
        returning(&plan.returning, &src, &mut out, cx)?;
    }
    Ok(out)
}

/// Deletes every row and index entry of a table (TRUNCATE, DROP).
pub(crate) fn clear_table(table: &Table, txn: &mut squad_kv::Txn) -> Result<()> {
    let ids = std::iter::once(table.id).chain(table.indexes.iter().filter(|i| !i.primary).map(|i| i.id));
    for id in ids {
        clear_prefix(&object_prefix(id), txn)?;
    }
    Ok(())
}

pub(crate) fn clear_prefix(prefix: &[u8], txn: &mut squad_kv::Txn) -> Result<()> {
    let mut hi = prefix.to_vec();
    *hi.last_mut().expect("non-empty prefix") += 1;
    loop {
        let chunk = txn.scan(Bound::Included(prefix), Bound::Excluded(&hi), false, 4096)?;
        if chunk.is_empty() {
            return Ok(());
        }
        for (k, _) in chunk {
            txn.delete_blind(&k)?;
        }
    }
}

/// Adds the index entries of all existing rows (CREATE INDEX, new UNIQUE
/// constraints), verifying uniqueness.
pub(crate) fn build_index(table: &Table, index: &Index, cx: &mut ExecCx) -> Result<()> {
    let prefix = object_prefix(table.id);
    let mut hi = prefix.to_vec();
    *hi.last_mut().unwrap() += 1;
    let mut cursor =
        squad_kv::Cursor::new(cx.txn, Bound::Included(prefix.to_vec()), Bound::Excluded(hi), false).with_chunk(1024);
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut entries = Vec::new();
    while let Some((_, bytes)) = cursor.next(cx.txn)? {
        let row = decode_table_row(table, &bytes)?;
        let (ikey, pk, enforce) = index_entry(table, index, &row);
        if enforce && !seen.insert(ikey.clone()) {
            return Err(Error::new(
                sqlstate::UNIQUE_VIOLATION,
                format!("could not create unique index \"{}\"", index.name),
            )
            .with_detail(format!(
                "Key ({})=({}) is duplicated.",
                col_names(table, &index.columns),
                show_list(&row, &index.columns, cx)
            )));
        }
        entries.push((ikey, pk));
    }
    for (k, v) in entries {
        cx.txn.put_blind(&k, &v)?;
    }
    Ok(())
}

/// Verifies a newly added foreign key against the existing rows.
pub(crate) fn validate_foreign_key(table: &Arc<Table>, fk: &ForeignKey, rows: &[Row], cx: &mut ExecCx) -> Result<()> {
    let mut only = (**table).clone();
    only.foreign_keys = vec![fk.clone()];
    for row in rows {
        check_outgoing_fks(&only, row, None, cx)?;
    }
    Ok(())
}
