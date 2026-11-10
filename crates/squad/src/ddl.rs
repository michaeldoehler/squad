//! Utility statements: schema changes, transaction control, settings.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use pg_query::protobuf::{self as pb, a_const::Val, node::Node as N};
use squad_kv::{Isolation, Txn, WaitPolicy};

use crate::bind::{
    deparse_expr, deparse_stmt, name_list, node, qualified, range_var_name, Binder, OutputColumn, Scope,
};
use crate::catalog::{
    object_prefix, Catalog, Check, Column, EnumType, FkAction, ForeignKey, Index, Relation, Sequence, Table, View,
};
use crate::dml::{clear_table, index_entry};
use crate::encoding::encode_row;
use crate::exec::{decode_table_row, row_key};
use crate::expr::Row;
use crate::session::{feature, QueryResult, Session, SessionState};
use crate::types::{Type, Value};
use crate::{sqlstate, Error, Result};

const ROWID_COLUMN: &str = "__rowid";

fn done(tag: &str) -> Result<QueryResult> {
    Ok(QueryResult { tag: tag.to_string(), ..Default::default() })
}

fn text_column(name: &str) -> Vec<OutputColumn> {
    vec![OutputColumn { name: name.to_string(), ty: Type::Text }]
}

/// Result shape (tag, columns) of a utility statement, known at prepare
/// time so that clients can describe it.
pub(crate) fn utility_shape(sess: &mut Session, stmt: &N) -> Result<(&'static str, Vec<OutputColumn>)> {
    Ok(match stmt {
        N::VariableShowStmt(s) => ("SHOW", text_column(&show_column_name(&s.name))),
        N::ExplainStmt(_) => ("EXPLAIN", text_column("QUERY PLAN")),
        N::ExecuteStmt(e) => {
            let target = sess.named.get(&e.name).ok_or_else(|| {
                Error::new(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{}\" does not exist", e.name),
                )
            })?;
            ("EXECUTE", target.columns.clone())
        }
        _ => ("", Vec::new()),
    })
}

fn show_column_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "timezone" => "TimeZone".into(),
        "datestyle" => "DateStyle".into(),
        "transaction_isolation" => "transaction_isolation".into(),
        other => other.to_string(),
    }
}

/// Value of a run-time setting as `SHOW` prints it.
pub(crate) fn show_setting(state: &SessionState, name: &str) -> Result<String> {
    let lower = name.to_ascii_lowercase();
    if let Some(v) = state.settings.get(&lower) {
        return Ok(v.clone());
    }
    Ok(match lower.as_str() {
        "search_path" => "\"$user\", public".to_string(),
        "timezone" => state.tz.name().to_string(),
        "transaction_isolation" | "default_transaction_isolation" => match state.default_isolation {
            Isolation::ReadCommitted => "read committed",
            Isolation::RepeatableRead => "repeatable read",
            Isolation::Serializable => "serializable",
        }
        .to_string(),
        "lock_timeout" => state.lock_timeout.map_or("0".to_string(), |d| format!("{}ms", d.as_millis())),
        "server_version" => crate::SERVER_VERSION.to_string(),
        "server_version_num" => crate::SERVER_VERSION_NUM.to_string(),
        "server_encoding" | "client_encoding" => "UTF8".to_string(),
        "standard_conforming_strings" | "integer_datetimes" | "is_superuser" => "on".to_string(),
        "datestyle" => "ISO, MDY".to_string(),
        "intervalstyle" => "postgres".to_string(),
        "max_identifier_length" => "63".to_string(),
        "statement_timeout" | "idle_in_transaction_session_timeout" => "0".to_string(),
        "application_name" => String::new(),
        "extra_float_digits" => "1".to_string(),
        "client_min_messages" => "notice".to_string(),
        "bytea_output" => "hex".to_string(),
        "transaction_read_only" | "default_transaction_read_only" => "off".to_string(),
        "max_connections" => "100".to_string(),
        "lc_collate" | "lc_ctype" => "C".to_string(),
        "session_authorization" => state.user.clone(),
        _ => {
            return Err(Error::new(
                sqlstate::UNDEFINED_OBJECT,
                format!("unrecognized configuration parameter \"{name}\""),
            ))
        }
    })
}

fn parse_duration_ms(value: &str) -> Result<Option<std::time::Duration>> {
    let v = value.trim();
    let (num, unit) = v.split_at(v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len()));
    let n: u64 = num.parse().map_err(|_| {
        Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("invalid value for a timeout: \"{value}\""))
    })?;
    let ms = match unit.trim() {
        "" | "ms" => n,
        "s" => n * 1000,
        "min" => n * 60_000,
        "h" => n * 3_600_000,
        _ => {
            return Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                format!("invalid value for a timeout: \"{value}\""),
            ))
        }
    };
    Ok((ms > 0).then(|| std::time::Duration::from_millis(ms)))
}

fn parse_isolation(value: &str) -> Result<Isolation> {
    match value.to_ascii_lowercase().as_str() {
        "read committed" | "read uncommitted" => Ok(Isolation::ReadCommitted),
        "repeatable read" => Ok(Isolation::RepeatableRead),
        "serializable" => Ok(Isolation::Serializable),
        other => Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("invalid isolation level \"{other}\""))),
    }
}

/// Applies `SET name = value`; `None` resets to the default.
pub(crate) fn apply_setting(state: &mut SessionState, name: &str, value: Option<&str>) -> Result<()> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "search_path" => {
            state.search_path = match value {
                Some(v) => v
                    .split(',')
                    .map(|s| s.trim().trim_matches('"').to_string())
                    .filter(|s| !s.is_empty() && s != "$user")
                    .collect(),
                None => vec!["public".to_string()],
            };
            // `SHOW search_path` echoes the list as it was set.
            match value {
                Some(v) => state.settings.insert(lower, v.to_string()),
                None => state.settings.remove(&lower),
            };
        }
        "timezone" => {
            state.tz = match value {
                None => chrono_tz::UTC,
                Some(v) if v.eq_ignore_ascii_case("default") || v.eq_ignore_ascii_case("local") => chrono_tz::UTC,
                Some(v) => v.parse().map_err(|_| {
                    Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        format!("invalid value for parameter \"TimeZone\": \"{v}\""),
                    )
                })?,
            };
        }
        "lock_timeout" => state.lock_timeout = value.map(parse_duration_ms).transpose()?.flatten(),
        "default_transaction_isolation" => {
            state.default_isolation = value.map(parse_isolation).transpose()?.unwrap_or_default();
        }
        "client_encoding" => match value {
            Some(v) if !matches!(v.to_ascii_uppercase().as_str(), "UTF8" | "UTF-8" | "UNICODE" | "DEFAULT") => {
                return Err(Error::new(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    format!("client encoding \"{v}\" is not supported; squad only speaks UTF8"),
                ))
            }
            _ => {}
        },
        "server_version" | "server_version_num" | "server_encoding" | "integer_datetimes" | "is_superuser" => {
            return Err(Error::new("55P02", format!("parameter \"{name}\" cannot be changed")))
        }
        _ => match value {
            Some(v) => {
                state.settings.insert(lower, v.to_string());
            }
            None => {
                state.settings.remove(&lower);
            }
        },
    }
    Ok(())
}

fn const_text(n: &pb::Node) -> Option<String> {
    match n.node.as_ref()? {
        N::AConst(c) => match c.val.as_ref()? {
            Val::Sval(s) => Some(s.sval.clone()),
            Val::Ival(i) => Some(i.ival.to_string()),
            Val::Fval(f) => Some(f.fval.clone()),
            Val::Boolval(b) => Some(if b.boolval { "on" } else { "off" }.to_string()),
            Val::Bsval(s) => Some(s.bsval.clone()),
        },
        N::TypeCast(c) => c.arg.as_ref().and_then(|a| const_text(a)),
        N::String(s) => Some(s.sval.clone()),
        N::Integer(i) => Some(i.ival.to_string()),
        N::Float(f) => Some(f.fval.clone()),
        N::Boolean(b) => Some(if b.boolval { "true" } else { "false" }.to_string()),
        _ => None,
    }
}

fn def_elems(nodes: &[pb::Node]) -> Vec<(&str, Option<&pb::Node>)> {
    nodes
        .iter()
        .filter_map(|n| match n.node.as_ref() {
            Some(N::DefElem(d)) => Some((d.defname.as_str(), d.arg.as_deref())),
            _ => None,
        })
        .collect()
}

fn fk_action(code: &str) -> FkAction {
    match code {
        "r" => FkAction::Restrict,
        "c" => FkAction::Cascade,
        "n" => FkAction::SetNull,
        "d" => FkAction::SetDefault,
        _ => FkAction::NoAction,
    }
}

fn duplicate_relation(name: &str) -> Error {
    Error::new(sqlstate::DUPLICATE_TABLE, format!("relation \"{name}\" already exists"))
}

fn undefined_table(name: &str) -> Error {
    Error::new(sqlstate::UNDEFINED_TABLE, format!("relation \"{name}\" does not exist"))
}

fn undefined_column(table: &Table, name: &str) -> Error {
    Error::new(sqlstate::UNDEFINED_COLUMN, format!("column \"{name}\" of relation \"{}\" does not exist", table.name))
}

/// Schema an unqualified new object is created in.
fn creation_schema(catalog: &Catalog, schema: Option<&str>, path: &[String]) -> Result<String> {
    match schema {
        Some(s) if catalog.has_schema(s) => Ok(s.to_string()),
        Some(s) => Err(Error::new(sqlstate::INVALID_SCHEMA_NAME, format!("schema \"{s}\" does not exist"))),
        None => path
            .iter()
            .find(|s| catalog.has_schema(s))
            .cloned()
            .ok_or_else(|| Error::new(sqlstate::INVALID_SCHEMA_NAME, "no schema has been selected to create in")),
    }
}

/// A name for an implicitly named object that is not taken yet.
fn free_name(catalog: &Catalog, schema: &str, base: &str, extra: &[String]) -> String {
    let mut name = base.to_string();
    let mut n = 1;
    while catalog.relation_name_taken(schema, &name) || extra.contains(&name) {
        name = format!("{base}{n}");
        n += 1;
    }
    name
}

fn all_rows(table: &Table, txn: &Txn) -> Result<Vec<Row>> {
    let prefix = object_prefix(table.id);
    let mut hi = prefix.to_vec();
    *hi.last_mut().unwrap() += 1;
    let mut cursor =
        squad_kv::Cursor::new(txn, Bound::Included(prefix.to_vec()), Bound::Excluded(hi), false).with_chunk(1024);
    let mut rows = Vec::new();
    while let Some((_, bytes)) = cursor.next(txn)? {
        rows.push(decode_table_row(table, &bytes)?);
    }
    Ok(rows)
}

/// Stores rows under a (possibly changed) table definition, rebuilding
/// all index entries and verifying unique indexes.
fn write_rows(table: &Table, rows: &[Row], txn: &mut Txn) -> Result<()> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    for row in rows {
        let key = row_key(table, row);
        if !seen.insert(key.clone()) {
            let name = table.pk_name().unwrap_or("primary key").to_string();
            return Err(Error::new(sqlstate::UNIQUE_VIOLATION, format!("could not create unique index \"{name}\""))
                .with_detail("Key is duplicated."));
        }
        txn.put_blind(&key, &encode_row(row))?;
        for index in table.indexes.iter().filter(|i| !i.primary) {
            let (ikey, pk, enforce) = index_entry(table, index, row);
            if enforce && !seen.insert(ikey.clone()) {
                return Err(Error::new(
                    sqlstate::UNIQUE_VIOLATION,
                    format!("could not create unique index \"{}\"", index.name),
                )
                .with_detail("Key is duplicated."));
            }
            txn.put_blind(&ikey, &pk)?;
        }
    }
    Ok(())
}

/// Everything parsed from one column definition.
struct ColumnSpec {
    column: Column,
    primary: Option<String>,
    unique: Vec<(String, bool)>,
    checks: Vec<(String, String)>,
    references: Vec<pb::Constraint>,
    /// Owned sequence to create: `(name, bigint?)`.
    sequence: Option<String>,
}

fn column_spec(
    catalog: &Catalog,
    path: &[String],
    schema: &str,
    table: &str,
    def: &pb::ColumnDef,
    taken: &[String],
) -> Result<ColumnSpec> {
    let binder = Binder::new(catalog, path, Vec::new());
    let tn = def.type_name.as_ref().ok_or_else(|| Error::syntax("column definition without a type"))?;
    let decl = binder.resolve_type(tn)?;
    let mut column = Column {
        name: def.colname.clone(),
        ty: decl.ty.clone(),
        decl: decl.decl.clone(),
        decl_oid: decl.oid,
        typmod: decl.typmod,
        not_null: def.is_not_null,
        default: None,
        identity_always: false,
        hidden: false,
    };
    let mut spec = ColumnSpec {
        column: column.clone(),
        primary: None,
        unique: Vec::new(),
        checks: Vec::new(),
        references: Vec::new(),
        sequence: None,
    };
    let mut sequence = decl.serial;
    if !def.generated.is_empty() {
        return Err(feature("generated columns"));
    }
    for c in &def.constraints {
        let N::Constraint(con) = node(c)? else { continue };
        use pb::ConstrType as C;
        match C::try_from(con.contype).unwrap_or(C::Undefined) {
            C::ConstrNotnull => column.not_null = true,
            C::ConstrNull => column.not_null = false,
            C::ConstrDefault => {
                let e = con.raw_expr.as_ref().ok_or_else(|| Error::syntax("DEFAULT without expression"))?;
                column.default = Some(deparse_expr(e)?);
            }
            C::ConstrIdentity => {
                sequence = true;
                column.not_null = true;
                column.identity_always = con.generated_when == "a";
            }
            C::ConstrGenerated => return Err(feature("generated columns")),
            C::ConstrPrimary => spec.primary = Some(con.conname.clone()),
            C::ConstrUnique => spec.unique.push((con.conname.clone(), con.nulls_not_distinct)),
            C::ConstrCheck => {
                let e = con.raw_expr.as_ref().ok_or_else(|| Error::syntax("CHECK without expression"))?;
                spec.checks.push((con.conname.clone(), deparse_expr(e)?));
            }
            C::ConstrForeign => spec.references.push((**con).clone()),
            C::ConstrAttrDeferrable | C::ConstrAttrDeferred => return Err(feature("deferrable constraints")),
            C::ConstrExclusion => return Err(feature("exclusion constraints")),
            _ => {}
        }
    }
    if sequence {
        if !column.ty.is_integer() {
            return Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "identity column type must be smallint, integer, or bigint",
            ));
        }
        let name = free_name(catalog, schema, &format!("{table}_{}_seq", column.name), taken);
        column.default = Some(format!("nextval('{}'::regclass)", quote_qualified(schema, &name)));
        column.not_null = true;
        spec.sequence = Some(name);
    }
    spec.column = column;
    Ok(spec)
}

fn quote_ident(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !s.chars().next().unwrap().is_ascii_digit();
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

pub(crate) fn quote_qualified(schema: &str, name: &str) -> String {
    if schema == "public" {
        quote_ident(name)
    } else {
        format!("{}.{}", quote_ident(schema), quote_ident(name))
    }
}

fn new_sequence(
    catalog: &mut Catalog,
    schema: &str,
    name: &str,
    max: i64,
    owned_by: Option<(u32, String)>,
) -> Arc<Sequence> {
    let seq = Arc::new(Sequence {
        id: catalog.alloc_id(),
        schema: schema.to_string(),
        name: name.to_string(),
        start: 1,
        increment: 1,
        min: 1,
        max,
        cycle: false,
        owned_by,
    });
    catalog.sequences.insert((schema.to_string(), name.to_string()), seq.clone());
    seq
}

fn int_max(ty: &Type) -> i64 {
    match ty {
        Type::Int2 => i16::MAX as i64,
        Type::Int4 => i32::MAX as i64,
        _ => i64::MAX,
    }
}

fn columns_by_name(table: &Table, names: &[String]) -> Result<Vec<usize>> {
    names
        .iter()
        .map(|n| {
            table.columns.iter().position(|c| c.name == *n && !c.hidden).ok_or_else(|| {
                Error::new(sqlstate::UNDEFINED_COLUMN, format!("column \"{n}\" named in key does not exist"))
            })
        })
        .collect()
}

/// Resolves a FOREIGN KEY / REFERENCES clause against the catalog.
fn foreign_key(
    catalog: &Catalog,
    path: &[String],
    table: &Table,
    con: &pb::Constraint,
    columns: Vec<usize>,
) -> Result<ForeignKey> {
    let rv = con.pktable.as_ref().ok_or_else(|| Error::syntax("REFERENCES without table"))?;
    let (schema, name) = range_var_name(rv);
    // A table may reference itself while it is being created.
    let self_ref = name == table.name && schema.is_none_or(|s| s == table.schema);
    let parent: Arc<Table> = if self_ref {
        Arc::new(table.clone())
    } else {
        catalog.table(schema, name, path).cloned().ok_or_else(|| undefined_table(name))?
    };
    let ref_names = name_list(&con.pk_attrs)?;
    let ref_columns = if ref_names.is_empty() {
        if parent.rowid {
            return Err(Error::new(
                sqlstate::INVALID_FOREIGN_KEY,
                format!("there is no primary key for referenced table \"{}\"", parent.name),
            ));
        }
        parent.pk.clone()
    } else {
        columns_by_name(&parent, &ref_names).map_err(|_| {
            Error::new(sqlstate::UNDEFINED_COLUMN, "column referenced in foreign key constraint does not exist")
        })?
    };
    if ref_columns.len() != columns.len() {
        return Err(Error::new(
            sqlstate::INVALID_FOREIGN_KEY,
            "number of referencing and referenced columns for foreign key disagree",
        ));
    }
    let is_pk = !parent.rowid && parent.pk == ref_columns;
    if !is_pk && !parent.indexes.iter().any(|ix| ix.unique && ix.columns == ref_columns) {
        return Err(Error::new(
            sqlstate::INVALID_FOREIGN_KEY,
            format!("there is no unique constraint matching given keys for referenced table \"{}\"", parent.name),
        ));
    }
    for (&c, &r) in columns.iter().zip(&ref_columns) {
        let (ct, rt) = (&table.columns[c].ty, &parent.columns[r].ty);
        let compatible = ct == rt || (ct.is_integer() && rt.is_integer());
        if !compatible {
            return Err(Error::type_mismatch(format!(
                "foreign key constraint cannot be implemented: key columns \"{}\" and \"{}\" are of incompatible types: {ct} and {rt}",
                table.columns[c].name, parent.columns[r].name
            )));
        }
    }
    let name = if con.conname.is_empty() {
        let cols: Vec<&str> = columns.iter().map(|&c| table.columns[c].name.as_str()).collect();
        format!("{}_{}_fkey", table.name, cols.join("_"))
    } else {
        con.conname.clone()
    };
    Ok(ForeignKey {
        name,
        columns,
        ref_table: parent.id,
        ref_columns,
        on_delete: fk_action(&con.fk_del_action),
        on_update: fk_action(&con.fk_upd_action),
    })
}

fn validate_check(catalog: &Catalog, path: &[String], table: &Table, sql: &str) -> Result<()> {
    let mut binder = Binder::new(catalog, path, Vec::new());
    binder.bind_check(&Arc::new(table.clone()), sql).map(|_| ())
}

fn validate_default(catalog: &Catalog, path: &[String], table: &Table, col: usize) -> Result<()> {
    let mut binder = Binder::new(catalog, path, Vec::new());
    binder.default_expr(table, col).map(|_| ())
}

fn create_table(sess: &mut Session, stmt: &pb::CreateStmt) -> Result<QueryResult> {
    let rv = stmt.relation.as_ref().ok_or_else(|| Error::internal("CREATE TABLE without name"))?;
    if !stmt.inh_relations.is_empty() || stmt.partspec.is_some() || stmt.partbound.is_some() {
        return Err(feature("table inheritance and partitioning"));
    }
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _txn, shared, _| {
        let (schema, name) = range_var_name(rv);
        let schema = creation_schema(catalog, schema, &path)?;
        if catalog.relation_name_taken(&schema, name) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(duplicate_relation(name));
        }
        let id = catalog.alloc_id();
        let mut table = Table {
            id,
            schema: schema.clone(),
            name: name.to_string(),
            columns: Vec::new(),
            pk: Vec::new(),
            rowid: false,
            indexes: Vec::new(),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            columnar: None,
        };
        let mut specs = Vec::new();
        let mut table_constraints: Vec<pb::Constraint> = Vec::new();
        let mut seq_names: Vec<String> = Vec::new();
        for elt in &stmt.table_elts {
            match node(elt)? {
                N::ColumnDef(def) => {
                    if table.columns.iter().any(|c| c.name == def.colname) {
                        return Err(Error::new(
                            sqlstate::DUPLICATE_COLUMN,
                            format!("column \"{}\" specified more than once", def.colname),
                        ));
                    }
                    let spec = column_spec(catalog, &path, &schema, name, def, &seq_names)?;
                    seq_names.extend(spec.sequence.clone());
                    table.columns.push(spec.column.clone());
                    specs.push(spec);
                }
                N::Constraint(c) => table_constraints.push((**c).clone()),
                N::TableLikeClause(_) => return Err(feature("CREATE TABLE ... LIKE")),
                _ => return Err(feature("this table element")),
            }
        }
        if table.columns.is_empty() {
            return Err(feature("tables without columns"));
        }

        let mut taken: Vec<String> = Vec::new();
        let index_name = |catalog: &Catalog, given: &str, default: String, taken: &mut Vec<String>| -> Result<String> {
            let name = if given.is_empty() { free_name(catalog, &schema, &default, taken) } else { given.to_string() };
            if catalog.relation_name_taken(&schema, &name) || taken.contains(&name) {
                return Err(duplicate_relation(&name));
            }
            taken.push(name.clone());
            Ok(name)
        };
        let col_list = |table: &Table, cols: &[usize]| {
            cols.iter().map(|&c| table.columns[c].name.as_str()).collect::<Vec<_>>().join("_")
        };

        // Primary key: from a column constraint or a table constraint.
        let mut pk: Option<(String, Vec<usize>)> = None;
        for (i, spec) in specs.iter().enumerate() {
            if let Some(conname) = &spec.primary {
                if pk.is_some() {
                    return Err(Error::new(
                        sqlstate::INVALID_TABLE_DEFINITION,
                        format!("multiple primary keys for table \"{name}\" are not allowed"),
                    ));
                }
                pk = Some((conname.clone(), vec![i]));
            }
        }
        use pb::ConstrType as C;
        for con in &table_constraints {
            if con.contype == C::ConstrPrimary as i32 {
                if pk.is_some() {
                    return Err(Error::new(
                        sqlstate::INVALID_TABLE_DEFINITION,
                        format!("multiple primary keys for table \"{name}\" are not allowed"),
                    ));
                }
                pk = Some((con.conname.clone(), columns_by_name(&table, &name_list(&con.keys)?)?));
            }
        }
        match pk {
            Some((conname, cols)) => {
                for &c in &cols {
                    table.columns[c].not_null = true;
                }
                let iname = index_name(catalog, &conname, format!("{name}_pkey"), &mut taken)?;
                table.indexes.push(Index {
                    id: catalog.alloc_id(),
                    name: iname,
                    columns: cols.clone(),
                    unique: true,
                    primary: true,
                    constraint: true,
                    nulls_not_distinct: false,
                });
                table.pk = cols;
            }
            None => {
                table.columns.push(rowid_column());
                table.pk = vec![table.columns.len() - 1];
                table.rowid = true;
            }
        }

        // UNIQUE constraints.
        let mut uniques: Vec<(String, Vec<usize>, bool)> = Vec::new();
        for (i, spec) in specs.iter().enumerate() {
            for (conname, nnd) in &spec.unique {
                uniques.push((conname.clone(), vec![i], *nnd));
            }
        }
        for con in &table_constraints {
            if con.contype == C::ConstrUnique as i32 {
                uniques.push((
                    con.conname.clone(),
                    columns_by_name(&table, &name_list(&con.keys)?)?,
                    con.nulls_not_distinct,
                ));
            }
        }
        for (conname, cols, nnd) in uniques {
            let iname = index_name(catalog, &conname, format!("{name}_{}_key", col_list(&table, &cols)), &mut taken)?;
            table.indexes.push(Index {
                id: catalog.alloc_id(),
                name: iname,
                columns: cols,
                unique: true,
                primary: false,
                constraint: true,
                nulls_not_distinct: nnd,
            });
        }

        // CHECK constraints.
        let mut checks: Vec<(String, String, Option<usize>)> = Vec::new();
        for (i, spec) in specs.iter().enumerate() {
            for (conname, sql) in &spec.checks {
                checks.push((conname.clone(), sql.clone(), Some(i)));
            }
        }
        for con in &table_constraints {
            match C::try_from(con.contype).unwrap_or(C::Undefined) {
                C::ConstrCheck => {
                    let e = con.raw_expr.as_ref().ok_or_else(|| Error::syntax("CHECK without expression"))?;
                    checks.push((con.conname.clone(), deparse_expr(e)?, None));
                }
                C::ConstrPrimary | C::ConstrUnique | C::ConstrForeign => {}
                C::ConstrExclusion => return Err(feature("exclusion constraints")),
                _ => return Err(feature("this table constraint")),
            }
        }
        for (conname, sql, col) in checks {
            let cname = if conname.is_empty() {
                let base = match col {
                    Some(c) => format!("{name}_{}_check", table.columns[c].name),
                    None => format!("{name}_check"),
                };
                let mut candidate = base.clone();
                let mut n = 1;
                while table.checks.iter().any(|c| c.name == candidate) {
                    candidate = format!("{base}{n}");
                    n += 1;
                }
                candidate
            } else {
                conname
            };
            validate_check(catalog, &path, &table, &sql)?;
            table.checks.push(Check { name: cname, expr: sql });
        }

        // Sequences of serial / identity columns must exist before the
        // defaults that call them are validated.
        for (i, spec) in specs.iter().enumerate() {
            if let Some(seq_name) = &spec.sequence {
                let seq = new_sequence(
                    catalog,
                    &schema,
                    seq_name,
                    int_max(&table.columns[i].ty),
                    Some((id, table.columns[i].name.clone())),
                );
                shared.reset_counter(seq.id)?;
            }
        }
        for i in 0..table.columns.len() {
            validate_default(catalog, &path, &table, i)?;
        }

        // Foreign keys.
        for (i, spec) in specs.iter().enumerate() {
            for con in &spec.references {
                let fk = foreign_key(catalog, &path, &table, con, vec![i])?;
                table.foreign_keys.push(fk);
            }
        }
        for con in &table_constraints {
            if con.contype == C::ConstrForeign as i32 {
                let cols = columns_by_name(&table, &name_list(&con.fk_attrs)?)?;
                let fk = foreign_key(catalog, &path, &table, con, cols)?;
                table.foreign_keys.push(fk);
            }
        }
        shared.reset_counter(id)?;
        // CREATE TABLE … USING columnar [WITH (order_by = 'a, b')]
        match stmt.access_method.as_str() {
            "" | "heap" => {}
            "columnar" => {
                if !table.rowid || !table.indexes.is_empty() || !table.foreign_keys.is_empty() {
                    return Err(feature("primary key, unique and foreign key constraints on columnar tables"));
                }
                let mut order_by = Vec::new();
                for (option, arg) in def_elems(&stmt.options) {
                    if option != "order_by" {
                        return Err(feature(&format!("columnar table option \"{option}\"")));
                    }
                    for col in arg.and_then(const_text).unwrap_or_default().split(',') {
                        let col = col.trim();
                        order_by.push(table.columns.iter().position(|c| c.name == col && !c.hidden).ok_or_else(|| {
                            Error::new(sqlstate::UNDEFINED_COLUMN, format!("column \"{col}\" named in order_by does not exist"))
                        })?);
                    }
                }
                table.columnar = Some(crate::catalog::Columnar { order_by });
            }
            other => return Err(feature(&format!("table access method \"{other}\""))),
        }
        catalog.tables.insert((schema, name.to_string()), Arc::new(table));
        Ok(())
    })?;
    done("CREATE TABLE")
}

fn rowid_column() -> Column {
    Column {
        name: ROWID_COLUMN.to_string(),
        ty: Type::Int8,
        decl: "bigint".into(),
        decl_oid: 20,
        typmod: -1,
        not_null: true,
        default: None,
        identity_always: false,
        hidden: true,
    }
}

fn find_table(catalog: &Catalog, rv: &pb::RangeVar, path: &[String]) -> Result<Arc<Table>> {
    let (schema, name) = range_var_name(rv);
    catalog.table(schema, name, path).cloned().ok_or_else(|| undefined_table(name))
}

fn put_table(catalog: &mut Catalog, table: Table) {
    catalog.tables.insert((table.schema.clone(), table.name.clone()), Arc::new(table));
}

fn create_index(sess: &mut Session, stmt: &pb::IndexStmt) -> Result<QueryResult> {
    let rv = stmt.relation.as_ref().ok_or_else(|| Error::internal("CREATE INDEX without table"))?;
    if stmt.where_clause.is_some() {
        return Err(feature("partial indexes"));
    }
    if !matches!(stmt.access_method.as_str(), "" | "btree" | "hash") {
        return Err(feature(&format!("index access method \"{}\"", stmt.access_method)));
    }
    let path = sess.state.search_path.clone();
    let wait = WaitPolicy::Wait(sess.state.lock_timeout);
    let built = sess.alter_catalog(|catalog, txn, _, _| {
        let table = find_table(catalog, rv, &path)?;
        if table.columnar.is_some() {
            return Err(feature("indexes on columnar tables"));
        }
        Session::lock_table_exclusive(txn, table.id, wait)?;
        let mut cols = Vec::with_capacity(stmt.index_params.len());
        for p in &stmt.index_params {
            let N::IndexElem(e) = node(p)? else { return Err(Error::internal("malformed index column")) };
            if e.name.is_empty() {
                return Err(feature("expression indexes"));
            }
            cols.push(table.columns.iter().position(|c| c.name == e.name && !c.hidden).ok_or_else(|| {
                Error::new(sqlstate::UNDEFINED_COLUMN, format!("column \"{}\" does not exist", e.name))
            })?);
        }
        let name = if stmt.idxname.is_empty() {
            let list = cols.iter().map(|&c| table.columns[c].name.as_str()).collect::<Vec<_>>().join("_");
            free_name(catalog, &table.schema, &format!("{}_{}_idx", table.name, list), &[])
        } else {
            stmt.idxname.clone()
        };
        if catalog.relation_name_taken(&table.schema, &name) {
            if stmt.if_not_exists {
                return Ok(None);
            }
            return Err(duplicate_relation(&name));
        }
        let index = Index {
            id: catalog.alloc_id(),
            name,
            columns: cols,
            unique: stmt.unique,
            primary: false,
            constraint: false,
            nulls_not_distinct: stmt.nulls_not_distinct,
        };
        let mut t = (*table).clone();
        t.indexes.push(index.clone());
        put_table(catalog, t.clone());
        Ok(Some((t, index)))
    })?;
    if let Some((table, index)) = built {
        sess.with_cx(|cx| crate::dml::build_index(&table, &index, cx))?;
    }
    done("CREATE INDEX")
}

fn view_binds(catalog: &Catalog, path: &[String], view: &View) -> bool {
    let mut search = vec![view.schema.clone()];
    search.extend(path.iter().cloned());
    let Ok(stmts) = crate::bind::parse_sql(&view.sql) else { return false };
    match stmts.first().and_then(|s| s.stmt.as_ref()).and_then(|s| s.node.as_ref()) {
        Some(N::SelectStmt(sel)) => Binder::new(catalog, &search, Vec::new()).bind_query(sel).is_ok(),
        _ => false,
    }
}

/// Removes a relation from the catalog. Views that are defined on it
/// (directly or through other views) block the removal unless `cascade`
/// is set, in which case they are removed as well.
fn remove_with_dependent_views(
    catalog: &mut Catalog,
    path: &[String],
    kind: &str,
    name: &str,
    cascade: bool,
    remove: impl Fn(&mut Catalog),
) -> Result<()> {
    let intact: Vec<Arc<View>> = catalog.views.values().filter(|v| view_binds(catalog, path, v)).cloned().collect();
    remove(catalog);
    loop {
        let broken: Vec<Arc<View>> = intact
            .iter()
            .filter(|v| {
                catalog.views.contains_key(&(v.schema.clone(), v.name.clone())) && !view_binds(catalog, path, v)
            })
            .cloned()
            .collect();
        let Some(first) = broken.first() else { return Ok(()) };
        if !cascade {
            return Err(Error::new(
                sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                format!("cannot drop {kind} {name} because other objects depend on it"),
            )
            .with_detail(format!("view {} depends on {kind} {name}", first.name))
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."));
        }
        for v in broken {
            catalog.views.remove(&(v.schema.clone(), v.name.clone()));
        }
    }
}

fn drop_owned_sequences(catalog: &mut Catalog, table_id: u32, column: Option<&str>, sess_resets: &mut Vec<u32>) {
    let owned: Vec<(String, String)> = catalog
        .sequences
        .iter()
        .filter(|(_, s)| matches!(&s.owned_by, Some((t, c)) if *t == table_id && column.is_none_or(|col| col == c)))
        .map(|(k, _)| k.clone())
        .collect();
    for key in owned {
        if let Some(s) = catalog.sequences.remove(&key) {
            sess_resets.push(s.id);
        }
    }
}

fn drop_objects(sess: &mut Session, stmt: &pb::DropStmt) -> Result<QueryResult> {
    use pb::ObjectType as O;
    let kind = O::try_from(stmt.remove_type).unwrap_or(O::Undefined);
    let cascade = stmt.behavior == pb::DropBehavior::DropCascade as i32;
    let path = sess.state.search_path.clone();
    let wait = WaitPolicy::Wait(sess.state.lock_timeout);
    let tag = match kind {
        O::ObjectTable => "DROP TABLE",
        O::ObjectIndex => "DROP INDEX",
        O::ObjectSequence => "DROP SEQUENCE",
        O::ObjectView => "DROP VIEW",
        O::ObjectType => "DROP TYPE",
        O::ObjectSchema => "DROP SCHEMA",
        O::ObjectExtension => return done("DROP EXTENSION"),
        other => return Err(feature(&format!("DROP for {other:?}"))),
    };
    let mut names: Vec<Vec<String>> = Vec::new();
    for o in &stmt.objects {
        names.push(match node(o)? {
            N::List(l) => name_list(&l.items)?,
            N::String(s) => vec![s.sval.clone()],
            N::TypeName(t) => name_list(&t.names)?,
            _ => return Err(Error::internal("unexpected object name in DROP")),
        });
    }
    let mut dropped_objects = Vec::new();
    let resets = sess.alter_catalog(|catalog, txn, _, _| {
        let mut resets = Vec::new();
        for parts in &names {
            let (schema, name) = qualified(parts);
            let missing = |what: &str| -> Result<()> {
                if stmt.missing_ok {
                    Ok(())
                } else {
                    Err(Error::new(
                        if what == "type" || what == "schema" || what == "index" {
                            sqlstate::UNDEFINED_OBJECT
                        } else {
                            sqlstate::UNDEFINED_TABLE
                        },
                        format!("{what} \"{name}\" does not exist"),
                    ))
                }
            };
            match kind {
                O::ObjectTable => {
                    let Some(table) = catalog.table(schema, name, &path).cloned() else {
                        missing("table")?;
                        continue;
                    };
                    Session::lock_table_exclusive(txn, table.id, wait)?;
                    let dependents: Vec<(Arc<Table>, ForeignKey)> =
                        catalog.referencing(table.id).into_iter().filter(|(t, _)| t.id != table.id).collect();
                    if !dependents.is_empty() && !cascade {
                        let (child, fk) = &dependents[0];
                        return Err(Error::new(
                            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                            format!("cannot drop table {} because other objects depend on it", table.name),
                        )
                        .with_detail(format!(
                            "constraint {} on table {} depends on table {}",
                            fk.name, child.name, table.name
                        ))
                        .with_hint("Use DROP ... CASCADE to drop the dependent objects too."));
                    }
                    for (child, fk) in dependents {
                        let mut c = (*catalog.table_by_id(child.id).cloned().unwrap_or(child)).clone();
                        c.foreign_keys.retain(|f| f.name != fk.name);
                        put_table(catalog, c);
                    }
                    let key = (table.schema.clone(), table.name.clone());
                    remove_with_dependent_views(catalog, &path, "table", &table.name, cascade, |c| {
                        c.tables.remove(&key);
                    })?;
                    clear_table(&table, txn)?;
                    crate::columnar::clear(&table, txn, &mut dropped_objects)?;
                    drop_owned_sequences(catalog, table.id, None, &mut resets);
                    resets.push(table.id);
                }
                O::ObjectIndex => {
                    let Some((table, pos)) = catalog.index(schema, name, &path).map(|(t, p)| (t.clone(), p)) else {
                        missing("index")?;
                        continue;
                    };
                    let index = &table.indexes[pos];
                    if index.constraint {
                        return Err(Error::new(
                            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                            format!(
                                "cannot drop index {} because constraint {} on table {} requires it",
                                index.name, index.name, table.name
                            ),
                        )
                        .with_hint(format!(
                            "You can drop constraint {} on table {} instead.",
                            index.name, table.name
                        )));
                    }
                    Session::lock_table_exclusive(txn, table.id, wait)?;
                    crate::dml::clear_prefix(&object_prefix(index.id), txn)?;
                    let mut t = (*table).clone();
                    t.indexes.remove(pos);
                    put_table(catalog, t);
                }
                O::ObjectSequence => {
                    let Some(seq) = catalog.sequence(schema, name, &path).cloned() else {
                        missing("sequence")?;
                        continue;
                    };
                    if let (Some((table_id, column)), false) = (&seq.owned_by, cascade) {
                        if let Some(t) = catalog.table_by_id(*table_id) {
                            if t.columns.iter().any(|c| c.name == *column && c.default.is_some()) {
                                return Err(Error::new(
                                    sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                                    format!("cannot drop sequence {} because other objects depend on it", seq.name),
                                )
                                .with_detail(format!(
                                    "default value for column {} of table {} depends on sequence {}",
                                    column, t.name, seq.name
                                )));
                            }
                        }
                    }
                    catalog.sequences.remove(&(seq.schema.clone(), seq.name.clone()));
                    resets.push(seq.id);
                }
                O::ObjectView => {
                    let Some(view) = catalog.view(schema, name, &path).cloned() else {
                        missing("view")?;
                        continue;
                    };
                    let key = (view.schema.clone(), view.name.clone());
                    remove_with_dependent_views(catalog, &path, "view", &view.name, cascade, |c| {
                        c.views.remove(&key);
                    })?;
                }
                O::ObjectType => {
                    let Some(ty) = catalog.enum_type(schema, name, &path).cloned() else {
                        missing("type")?;
                        continue;
                    };
                    let user =
                        catalog.tables.values().find(|t| t.columns.iter().any(|c| type_uses_enum(&c.ty, &ty.name)));
                    if let Some(t) = user {
                        return Err(Error::new(
                            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                            format!("cannot drop type {} because other objects depend on it", ty.name),
                        )
                        .with_detail(format!("a column of table {} depends on type {}", t.name, ty.name)));
                    }
                    catalog.enums.remove(&(ty.schema.clone(), ty.name.clone()));
                }
                O::ObjectSchema => {
                    if !catalog.has_schema(name) {
                        missing("schema")?;
                        continue;
                    }
                    let tables: Vec<Arc<Table>> =
                        catalog.tables.values().filter(|t| t.schema == *name).cloned().collect();
                    let populated = !tables.is_empty()
                        || catalog.views.keys().any(|k| k.0 == *name)
                        || catalog.sequences.keys().any(|k| k.0 == *name)
                        || catalog.enums.keys().any(|k| k.0 == *name);
                    if populated && !cascade {
                        return Err(Error::new(
                            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                            format!("cannot drop schema {name} because other objects depend on it"),
                        )
                        .with_hint("Use DROP ... CASCADE to drop the dependent objects too."));
                    }
                    for t in tables {
                        Session::lock_table_exclusive(txn, t.id, wait)?;
                        clear_table(&t, txn)?;
                        crate::columnar::clear(&t, txn, &mut dropped_objects)?;
                        resets.push(t.id);
                    }
                    resets.extend(catalog.sequences.values().filter(|s| s.schema == *name).map(|s| s.id));
                    catalog.tables.retain(|k, _| k.0 != *name);
                    catalog.views.retain(|k, _| k.0 != *name);
                    catalog.sequences.retain(|k, _| k.0 != *name);
                    catalog.enums.retain(|k, _| k.0 != *name);
                    catalog.schemas.retain(|s| s != name);
                }
                _ => unreachable!(),
            }
        }
        Ok(resets)
    })?;
    if let Some(t) = &mut sess.txn {
        t.reset_counters.extend(resets);
        t.dropped_objects.extend(dropped_objects);
    }
    done(tag)
}

fn type_uses_enum(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Enum(n) => n == name,
        Type::Array(e) => type_uses_enum(e, name),
        _ => false,
    }
}

/// Evaluates an expression for every row of a table (ALTER TABLE).
fn eval_per_row(
    sess: &mut Session,
    table: &Arc<Table>,
    rows: &[Row],
    sql: &str,
    target: &Column,
) -> Result<Vec<Value>> {
    let catalog = sess.catalog();
    let path = sess.state.search_path.clone();
    let mut binder = Binder::new(&catalog, &path, Vec::new());
    let n = crate::bind::parse_expr(sql)?;
    let scope = Scope::for_table(table, &table.name);
    let typed = binder.bind_expr(&n, &scope)?;
    let expr = binder.coerce_assign(typed, target)?;
    sess.with_cx(|cx| rows.iter().map(|r| expr.eval(r, cx)).collect())
}

fn alter_table(sess: &mut Session, stmt: &pb::AlterTableStmt) -> Result<QueryResult> {
    let rv = stmt.relation.as_ref().ok_or_else(|| Error::internal("ALTER TABLE without name"))?;
    use pb::ObjectType as O;
    let objtype = O::try_from(stmt.objtype).unwrap_or(O::Undefined);
    if !matches!(objtype, O::ObjectTable) {
        return match objtype {
            O::ObjectIndex | O::ObjectSequence | O::ObjectView => done("ALTER TABLE"),
            other => Err(feature(&format!("ALTER for {other:?}"))),
        };
    }
    let path = sess.state.search_path.clone();
    {
        let catalog = sess.catalog();
        let (schema, name) = range_var_name(rv);
        if catalog.table(schema, name, &path).is_none() {
            if stmt.missing_ok {
                return done("ALTER TABLE");
            }
            return Err(undefined_table(name));
        }
    }
    for cmd in &stmt.cmds {
        let N::AlterTableCmd(cmd) = node(cmd)? else { continue };
        alter_table_cmd(sess, rv, cmd, &path)?;
    }
    done("ALTER TABLE")
}

fn alter_table_cmd(sess: &mut Session, rv: &pb::RangeVar, cmd: &pb::AlterTableCmd, path: &[String]) -> Result<()> {
    use pb::AlterTableType as A;
    let wait = WaitPolicy::Wait(sess.state.lock_timeout);
    let kind = A::try_from(cmd.subtype).unwrap_or(A::Undefined);
    let cascade = cmd.behavior == pb::DropBehavior::DropCascade as i32;

    // Lock the table and read its current definition and rows.
    let table = sess.alter_catalog(|catalog, txn, _, _| {
        let table = find_table(catalog, rv, path)?;
        Session::lock_table_exclusive(txn, table.id, wait)?;
        Ok(table)
    })?;
    if table.columnar.is_some() {
        return Err(feature("ALTER TABLE on columnar tables"));
    }
    let column = |name: &str| {
        table.columns.iter().position(|c| c.name == name && !c.hidden).ok_or_else(|| undefined_column(&table, name))
    };

    match kind {
        A::AtAddColumn => {
            let Some(N::ColumnDef(def)) = cmd.def.as_ref().and_then(|d| d.node.as_ref()) else {
                return Err(Error::internal("ADD COLUMN without definition"));
            };
            if table.columns.iter().any(|c| c.name == def.colname) {
                if cmd.missing_ok {
                    return Ok(());
                }
                return Err(Error::new(
                    sqlstate::DUPLICATE_COLUMN,
                    format!("column \"{}\" of relation \"{}\" already exists", def.colname, table.name),
                ));
            }
            // Define the column (and its sequence) first so the default
            // can be bound, then fill existing rows.
            let (new_table, spec) = sess.alter_catalog(|catalog, _, shared, _| {
                let spec = column_spec(catalog, path, &table.schema, &table.name, def, &[])?;
                if spec.primary.is_some() {
                    return Err(feature("adding a primary key column to an existing table"));
                }
                let mut t = (*table).clone();
                t.columns.push(spec.column.clone());
                let col = t.columns.len() - 1;
                if let Some(seq_name) = &spec.sequence {
                    let seq = new_sequence(
                        catalog,
                        &t.schema,
                        seq_name,
                        int_max(&spec.column.ty),
                        Some((t.id, spec.column.name.clone())),
                    );
                    shared.reset_counter(seq.id)?;
                }
                for (conname, sql) in &spec.checks {
                    let name = if conname.is_empty() {
                        format!("{}_{}_check", t.name, spec.column.name)
                    } else {
                        conname.clone()
                    };
                    validate_check(catalog, path, &t, sql)?;
                    t.checks.push(Check { name, expr: sql.clone() });
                }
                for (conname, nnd) in &spec.unique {
                    let name = if conname.is_empty() {
                        free_name(catalog, &t.schema, &format!("{}_{}_key", t.name, spec.column.name), &[])
                    } else {
                        conname.clone()
                    };
                    t.indexes.push(Index {
                        id: catalog.alloc_id(),
                        name,
                        columns: vec![col],
                        unique: true,
                        primary: false,
                        constraint: true,
                        nulls_not_distinct: *nnd,
                    });
                }
                for con in &spec.references {
                    let fk = foreign_key(catalog, path, &t, con, vec![col])?;
                    t.foreign_keys.push(fk);
                }
                validate_default(catalog, path, &t, col)?;
                put_table(catalog, t.clone());
                Ok((Arc::new(t), spec))
            })?;
            let col = new_table.columns.len() - 1;
            let mut rows = sess.with_cx(|cx| all_rows(&table, cx.txn))?;
            if let Some(sql) = &spec.column.default {
                let values = eval_per_row(sess, &table, &rows, sql, &spec.column)?;
                for (r, v) in rows.iter_mut().zip(values) {
                    r.push(v);
                }
            } else {
                rows.iter_mut().for_each(|r| r.push(Value::Null));
            }
            if spec.column.not_null && rows.iter().any(|r| r[col].is_null()) {
                return Err(Error::new(
                    sqlstate::NOT_NULL_VIOLATION,
                    format!("column \"{}\" of relation \"{}\" contains null values", spec.column.name, table.name),
                ));
            }
            sess.with_cx(|cx| {
                check_rows(&new_table, &rows, cx)?;
                clear_table(&table, cx.txn)?;
                write_rows(&new_table, &rows, cx.txn)
            })
        }
        A::AtDropColumn => {
            let idx = match column(&cmd.name) {
                Ok(i) => i,
                Err(_) if cmd.missing_ok => return Ok(()),
                Err(e) => return Err(e),
            };
            if table.pk.contains(&idx) {
                return Err(feature("dropping a primary key column"));
            }
            let referenced =
                sess.catalog().referencing(table.id).into_iter().any(|(_, fk)| fk.ref_columns.contains(&idx));
            if referenced && !cascade {
                return Err(Error::new(
                    sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                    format!(
                        "cannot drop column {} of table {} because other objects depend on it",
                        cmd.name, table.name
                    ),
                ));
            }
            let mut rows = sess.with_cx(|cx| all_rows(&table, cx.txn))?;
            let (new_table, resets) = sess.alter_catalog(|catalog, _, _, _| {
                let mut t = (*table).clone();
                let shift = |c: usize| if c > idx { c - 1 } else { c };
                t.columns.remove(idx);
                t.pk = t.pk.iter().map(|&c| shift(c)).collect();
                t.indexes.retain(|ix| !ix.columns.contains(&idx));
                for ix in &mut t.indexes {
                    ix.columns = ix.columns.iter().map(|&c| shift(c)).collect();
                }
                t.foreign_keys.retain(|fk| !fk.columns.contains(&idx));
                for fk in &mut t.foreign_keys {
                    fk.columns = fk.columns.iter().map(|&c| shift(c)).collect();
                    if fk.ref_table == t.id {
                        fk.ref_columns = fk.ref_columns.iter().map(|&c| shift(c)).collect();
                    }
                }
                // Checks that no longer bind referenced the column.
                let probe = t.clone();
                t.checks.retain(|c| validate_check(catalog, path, &probe, &c.expr).is_ok());
                let mut resets = Vec::new();
                drop_owned_sequences(catalog, t.id, Some(&cmd.name), &mut resets);
                // Foreign keys of other tables pointing at this one.
                let others: Vec<Arc<Table>> = catalog.tables.values().filter(|o| o.id != t.id).cloned().collect();
                for o in others {
                    if o.foreign_keys.iter().any(|fk| fk.ref_table == t.id) {
                        let mut o = (*o).clone();
                        o.foreign_keys.retain(|fk| fk.ref_table != t.id || !fk.ref_columns.contains(&idx));
                        for fk in o.foreign_keys.iter_mut().filter(|fk| fk.ref_table == t.id) {
                            fk.ref_columns = fk.ref_columns.iter().map(|&c| shift(c)).collect();
                        }
                        put_table(catalog, o);
                    }
                }
                put_table(catalog, t.clone());
                Ok((t, resets))
            })?;
            if let Some(t) = &mut sess.txn {
                t.reset_counters.extend(resets);
            }
            for r in &mut rows {
                r.remove(idx);
            }
            sess.with_cx(|cx| {
                clear_table(&table, cx.txn)?;
                write_rows(&new_table, &rows, cx.txn)
            })
        }
        A::AtColumnDefault => {
            let idx = column(&cmd.name)?;
            let default = cmd.def.as_ref().map(|d| deparse_expr(d)).transpose()?;
            sess.alter_catalog(|catalog, _, _, _| {
                let mut t = (*table).clone();
                t.columns[idx].default = default;
                validate_default(catalog, path, &t, idx)?;
                put_table(catalog, t);
                Ok(())
            })
        }
        A::AtSetNotNull | A::AtDropNotNull => {
            let idx = column(&cmd.name)?;
            let set = kind == A::AtSetNotNull;
            if set {
                let rows = sess.with_cx(|cx| all_rows(&table, cx.txn))?;
                if rows.iter().any(|r| r[idx].is_null()) {
                    return Err(Error::new(
                        sqlstate::NOT_NULL_VIOLATION,
                        format!("column \"{}\" of relation \"{}\" contains null values", cmd.name, table.name),
                    ));
                }
            } else if table.pk.contains(&idx) {
                return Err(Error::new(
                    sqlstate::INVALID_TABLE_DEFINITION,
                    format!("column \"{}\" is in a primary key", cmd.name),
                ));
            }
            sess.alter_catalog(|catalog, _, _, _| {
                let mut t = (*table).clone();
                t.columns[idx].not_null = set;
                put_table(catalog, t);
                Ok(())
            })
        }
        A::AtAlterColumnType => {
            let idx = column(&cmd.name)?;
            let Some(N::ColumnDef(def)) = cmd.def.as_ref().and_then(|d| d.node.as_ref()) else {
                return Err(Error::internal("ALTER COLUMN TYPE without definition"));
            };
            let tn = def.type_name.as_ref().ok_or_else(|| Error::syntax("ALTER COLUMN TYPE without a type"))?;
            let catalog = sess.catalog();
            let decl = Binder::new(&catalog, path, Vec::new()).resolve_type(tn)?;
            let mut target = table.columns[idx].clone();
            target.ty = decl.ty.clone();
            target.decl = decl.decl.clone();
            target.decl_oid = decl.oid;
            target.typmod = decl.typmod;
            let using = match &def.raw_default {
                Some(u) => deparse_expr(u)?,
                None => {
                    // Without USING the conversion must be an assignment cast.
                    let from = &table.columns[idx].ty;
                    if *from != decl.ty
                        && !crate::value::can_cast(from, &decl.ty, crate::value::CastContext::Assignment)
                    {
                        return Err(Error::type_mismatch(format!(
                            "column \"{}\" cannot be cast automatically to type {}",
                            cmd.name, decl.ty
                        ))
                        .with_hint(format!(
                            "You might need to specify \"USING {}::{}\".",
                            quote_ident(&cmd.name),
                            decl.decl
                        )));
                    }
                    format!("{}::{}", quote_ident(&cmd.name), decl.decl)
                }
            };
            let mut rows = sess.with_cx(|cx| all_rows(&table, cx.txn))?;
            let values = eval_per_row(sess, &table, &rows, &using, &target)?;
            for (r, v) in rows.iter_mut().zip(values) {
                r[idx] = v;
            }
            let new_table = sess.alter_catalog(|catalog, _, _, _| {
                let mut t = (*table).clone();
                t.columns[idx] = target.clone();
                // The old default may not fit the new type.
                if validate_default(catalog, path, &t, idx).is_err() {
                    return Err(Error::type_mismatch(format!(
                        "default for column \"{}\" cannot be cast automatically to type {}",
                        cmd.name, target.ty
                    )));
                }
                put_table(catalog, t.clone());
                Ok(t)
            })?;
            sess.with_cx(|cx| {
                clear_table(&table, cx.txn)?;
                write_rows(&new_table, &rows, cx.txn)
            })
        }
        A::AtAddConstraint => {
            let Some(N::Constraint(con)) = cmd.def.as_ref().and_then(|d| d.node.as_ref()) else {
                return Err(Error::internal("ADD CONSTRAINT without definition"));
            };
            use pb::ConstrType as C;
            let rows = sess.with_cx(|cx| all_rows(&table, cx.txn))?;
            match C::try_from(con.contype).unwrap_or(C::Undefined) {
                C::ConstrCheck => {
                    let e = con.raw_expr.as_ref().ok_or_else(|| Error::syntax("CHECK without expression"))?;
                    let sql = deparse_expr(e)?;
                    let name =
                        if con.conname.is_empty() { format!("{}_check", table.name) } else { con.conname.clone() };
                    if table.checks.iter().any(|c| c.name == name) {
                        return Err(Error::new(
                            sqlstate::DUPLICATE_OBJECT,
                            format!("constraint \"{name}\" for relation \"{}\" already exists", table.name),
                        ));
                    }
                    let new_table = sess.alter_catalog(|catalog, _, _, _| {
                        let mut t = (*table).clone();
                        validate_check(catalog, path, &t, &sql)?;
                        t.checks.push(Check { name: name.clone(), expr: sql.clone() });
                        put_table(catalog, t.clone());
                        Ok(Arc::new(t))
                    })?;
                    if !con.skip_validation {
                        sess.with_cx(|cx| check_rows(&new_table, &rows, cx))?;
                    }
                    Ok(())
                }
                C::ConstrUnique | C::ConstrPrimary => {
                    let primary = con.contype == C::ConstrPrimary as i32;
                    let cols = columns_by_name(&table, &name_list(&con.keys)?)?;
                    if primary && !table.rowid {
                        return Err(Error::new(
                            sqlstate::INVALID_TABLE_DEFINITION,
                            format!("multiple primary keys for table \"{}\" are not allowed", table.name),
                        ));
                    }
                    if primary && rows.iter().any(|r| cols.iter().any(|&c| r[c].is_null())) {
                        return Err(Error::new(
                            sqlstate::NOT_NULL_VIOLATION,
                            format!("column of relation \"{}\" contains null values", table.name),
                        ));
                    }
                    let new_table = sess.alter_catalog(|catalog, _, _, _| {
                        let mut t = (*table).clone();
                        let list = cols.iter().map(|&c| t.columns[c].name.as_str()).collect::<Vec<_>>().join("_");
                        let default =
                            if primary { format!("{}_pkey", t.name) } else { format!("{}_{}_key", t.name, list) };
                        let name = if con.conname.is_empty() {
                            free_name(catalog, &t.schema, &default, &[])
                        } else {
                            con.conname.clone()
                        };
                        if catalog.relation_name_taken(&t.schema, &name) {
                            return Err(duplicate_relation(&name));
                        }
                        t.indexes.push(Index {
                            id: catalog.alloc_id(),
                            name,
                            columns: cols.clone(),
                            unique: true,
                            primary,
                            constraint: true,
                            nulls_not_distinct: con.nulls_not_distinct,
                        });
                        if primary {
                            // The declared key replaces the hidden row id.
                            for &c in &cols {
                                t.columns[c].not_null = true;
                            }
                            t.pk = cols.clone();
                            t.rowid = false;
                            t.columns.retain(|c| !c.hidden);
                        }
                        put_table(catalog, t.clone());
                        Ok(t)
                    })?;
                    let mut rows = rows;
                    if primary {
                        let hidden = table.columns.iter().position(|c| c.hidden);
                        if let Some(h) = hidden {
                            rows.iter_mut().for_each(|r| {
                                r.remove(h);
                            });
                        }
                    }
                    sess.with_cx(|cx| {
                        clear_table(&table, cx.txn)?;
                        write_rows(&new_table, &rows, cx.txn)
                    })
                }
                C::ConstrForeign => {
                    let new_table = sess.alter_catalog(|catalog, _, _, _| {
                        let mut t = (*table).clone();
                        let cols = columns_by_name(&t, &name_list(&con.fk_attrs)?)?;
                        let fk = foreign_key(catalog, path, &t, con, cols)?;
                        if t.foreign_keys.iter().any(|f| f.name == fk.name) {
                            return Err(Error::new(
                                sqlstate::DUPLICATE_OBJECT,
                                format!("constraint \"{}\" for relation \"{}\" already exists", fk.name, t.name),
                            ));
                        }
                        t.foreign_keys.push(fk);
                        put_table(catalog, t.clone());
                        Ok(Arc::new(t))
                    })?;
                    if !con.skip_validation {
                        let fk = new_table.foreign_keys.last().cloned().unwrap();
                        sess.with_cx(|cx| crate::dml::validate_foreign_key(&new_table, &fk, &rows, cx))?;
                    }
                    Ok(())
                }
                _ => Err(feature("this kind of constraint")),
            }
        }
        A::AtAddIdentity => {
            // ALTER COLUMN ... ADD GENERATED ... AS IDENTITY, as written by
            // pg_dump: creates the sequence and makes it the default.
            let idx = column(&cmd.name)?;
            let Some(N::Constraint(con)) = cmd.def.as_ref().and_then(|d| d.node.as_ref()) else {
                return Err(Error::internal("ADD IDENTITY without definition"));
            };
            if !table.columns[idx].ty.is_integer() {
                return Err(Error::new(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    "identity column type must be smallint, integer, or bigint",
                ));
            }
            if table.columns[idx].default.is_some() {
                return Err(Error::new(
                    sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("column \"{}\" of relation \"{}\" already has a default value", cmd.name, table.name),
                ));
            }
            // The sequence name may be given explicitly.
            let mut seq_name: Option<Vec<String>> = None;
            let mut options = Vec::new();
            for o in &con.options {
                match o.node.as_ref() {
                    Some(N::DefElem(d)) if d.defname == "sequence_name" => {
                        seq_name = match d.arg.as_ref().and_then(|a| a.node.as_ref()) {
                            Some(N::List(l)) => Some(name_list(&l.items)?),
                            Some(N::RangeVar(rv)) => {
                                let (schema, name) = range_var_name(rv);
                                Some(schema.into_iter().chain([name]).map(str::to_string).collect())
                            }
                            _ => None,
                        };
                    }
                    _ => options.push(o.clone()),
                }
            }
            let always = con.generated_when == "a";
            sess.alter_catalog(|catalog, _, shared, _| {
                let mut t = (*table).clone();
                let (schema, name) = match &seq_name {
                    Some(parts) => {
                        let (schema, name) = qualified(parts);
                        (creation_schema(catalog, schema, path)?, name.to_string())
                    }
                    None => {
                        let base = format!("{}_{}_seq", t.name, cmd.name);
                        (t.schema.clone(), free_name(catalog, &t.schema, &base, &[]))
                    }
                };
                if catalog.relation_name_taken(&schema, &name) {
                    return Err(duplicate_relation(&name));
                }
                let mut seq = Sequence {
                    id: catalog.alloc_id(),
                    schema: schema.clone(),
                    name: name.clone(),
                    start: 1,
                    increment: 1,
                    min: 1,
                    max: int_max(&t.columns[idx].ty),
                    cycle: false,
                    owned_by: Some((t.id, cmd.name.clone())),
                };
                sequence_options(&mut seq, &options, &mut None)?;
                shared.reset_counter(seq.id)?;
                catalog.sequences.insert((schema.clone(), name.clone()), Arc::new(seq));
                t.columns[idx].default = Some(format!("nextval('{}'::regclass)", quote_qualified(&schema, &name)));
                t.columns[idx].not_null = true;
                t.columns[idx].identity_always = always;
                put_table(catalog, t);
                Ok(())
            })
        }
        A::AtDropConstraint => {
            let name = &cmd.name;
            let is_index = table.indexes.iter().position(|ix| ix.constraint && ix.name == *name);
            let exists = is_index.is_some()
                || table.checks.iter().any(|c| c.name == *name)
                || table.foreign_keys.iter().any(|f| f.name == *name);
            if !exists {
                if cmd.missing_ok {
                    return Ok(());
                }
                return Err(Error::new(
                    sqlstate::UNDEFINED_OBJECT,
                    format!("constraint \"{name}\" of relation \"{}\" does not exist", table.name),
                ));
            }
            if let Some(pos) = is_index {
                if table.indexes[pos].primary {
                    return Err(feature("dropping a primary key constraint"));
                }
                let cols = table.indexes[pos].columns.clone();
                let used = sess.catalog().referencing(table.id).into_iter().any(|(_, fk)| fk.ref_columns == cols);
                if used && !cascade {
                    return Err(Error::new(
                        sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                        format!(
                            "cannot drop constraint {name} on table {} because other objects depend on it",
                            table.name
                        ),
                    ));
                }
            }
            sess.alter_catalog(|catalog, txn, _, _| {
                let mut t = (*table).clone();
                if let Some(pos) = is_index {
                    crate::dml::clear_prefix(&object_prefix(t.indexes[pos].id), txn)?;
                    t.indexes.remove(pos);
                }
                t.checks.retain(|c| c.name != *name);
                t.foreign_keys.retain(|f| f.name != *name);
                put_table(catalog, t);
                Ok(())
            })
        }
        A::AtChangeOwner
        | A::AtSetRelOptions
        | A::AtResetRelOptions
        | A::AtClusterOn
        | A::AtSetLogged
        | A::AtSetUnLogged
        | A::AtEnableRowSecurity
        | A::AtDisableRowSecurity
        | A::AtSetStatistics
        | A::AtSetStorage
        | A::AtReplicaIdentity
        | A::AtValidateConstraint
        | A::AtEnableTrig
        | A::AtDisableTrig
        | A::AtEnableTrigAll
        | A::AtDisableTrigAll => Ok(()),
        other => Err(feature(&format!("ALTER TABLE action {other:?}"))),
    }
}

/// Verifies NOT NULL and CHECK constraints for existing rows.
fn check_rows(table: &Arc<Table>, rows: &[Row], cx: &mut crate::exec::ExecCx) -> Result<()> {
    let catalog = cx.catalog.clone();
    let path = cx.session.search_path.clone();
    let mut binder = Binder::new(&catalog, &path, Vec::new());
    let mut checks = Vec::with_capacity(table.checks.len());
    for c in &table.checks {
        checks.push((c.name.clone(), binder.bind_check(table, &c.expr)?));
    }
    for row in rows {
        for (name, pred) in &checks {
            if pred.eval(row, cx)? == Value::Bool(false) {
                return Err(Error::new(
                    sqlstate::CHECK_VIOLATION,
                    format!("check constraint \"{name}\" of relation \"{}\" is violated by some row", table.name),
                ));
            }
        }
    }
    Ok(())
}

fn rename(sess: &mut Session, stmt: &pb::RenameStmt) -> Result<QueryResult> {
    use pb::ObjectType as O;
    let kind = O::try_from(stmt.rename_type).unwrap_or(O::Undefined);
    let path = sess.state.search_path.clone();
    let wait = WaitPolicy::Wait(sess.state.lock_timeout);
    let new = stmt.newname.clone();
    let tag = match kind {
        O::ObjectIndex => "ALTER INDEX",
        O::ObjectSequence => "ALTER SEQUENCE",
        O::ObjectView => "ALTER VIEW",
        O::ObjectSchema => "ALTER SCHEMA",
        O::ObjectType => "ALTER TYPE",
        _ => "ALTER TABLE",
    };
    sess.alter_catalog(|catalog, txn, _, _| {
        let rv = stmt.relation.as_ref();
        let missing = |name: &str| -> Result<()> {
            if stmt.missing_ok {
                Ok(())
            } else {
                Err(undefined_table(name))
            }
        };
        match kind {
            O::ObjectTable => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME without relation"))?;
                let (schema, name) = range_var_name(rv);
                // ALTER TABLE ... RENAME also applies to views, sequences
                // and indexes.
                if let Some(t) = catalog.table(schema, name, &path).cloned() {
                    Session::lock_table_exclusive(txn, t.id, wait)?;
                    if catalog.relation_name_taken(&t.schema, &new) {
                        return Err(duplicate_relation(&new));
                    }
                    catalog.tables.remove(&(t.schema.clone(), t.name.clone()));
                    let mut t = (*t).clone();
                    t.name = new.clone();
                    put_table(catalog, t);
                } else if let Some(v) = catalog.view(schema, name, &path).cloned() {
                    catalog.views.remove(&(v.schema.clone(), v.name.clone()));
                    let mut v = (*v).clone();
                    v.name = new.clone();
                    catalog.views.insert((v.schema.clone(), v.name.clone()), Arc::new(v));
                } else {
                    missing(name)?;
                }
            }
            O::ObjectColumn => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME COLUMN without relation"))?;
                let (schema, name) = range_var_name(rv);
                let Some(t) = catalog.table(schema, name, &path).cloned() else { return missing(name) };
                Session::lock_table_exclusive(txn, t.id, wait)?;
                let idx = t
                    .columns
                    .iter()
                    .position(|c| c.name == stmt.subname && !c.hidden)
                    .ok_or_else(|| undefined_column(&t, &stmt.subname))?;
                if t.columns.iter().any(|c| c.name == new) {
                    return Err(Error::new(
                        sqlstate::DUPLICATE_COLUMN,
                        format!("column \"{new}\" of relation \"{}\" already exists", t.name),
                    ));
                }
                let mut t = (*t).clone();
                t.columns[idx].name = new.clone();
                let owned: Vec<(String, String)> = catalog
                    .sequences
                    .iter()
                    .filter(|(_, s)| matches!(&s.owned_by, Some((id, c)) if *id == t.id && *c == stmt.subname))
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in owned {
                    let mut s = (*catalog.sequences[&key]).clone();
                    s.owned_by = Some((t.id, new.clone()));
                    catalog.sequences.insert(key, Arc::new(s));
                }
                put_table(catalog, t);
            }
            O::ObjectIndex => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME without relation"))?;
                let (schema, name) = range_var_name(rv);
                let Some((t, pos)) = catalog.index(schema, name, &path).map(|(t, p)| (t.clone(), p)) else {
                    return missing(name);
                };
                if catalog.relation_name_taken(&t.schema, &new) {
                    return Err(duplicate_relation(&new));
                }
                let mut t = (*t).clone();
                t.indexes[pos].name = new.clone();
                put_table(catalog, t);
            }
            O::ObjectSequence => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME without relation"))?;
                let (schema, name) = range_var_name(rv);
                let Some(s) = catalog.sequence(schema, name, &path).cloned() else { return missing(name) };
                catalog.sequences.remove(&(s.schema.clone(), s.name.clone()));
                let mut s = (*s).clone();
                s.name = new.clone();
                catalog.sequences.insert((s.schema.clone(), s.name.clone()), Arc::new(s));
            }
            O::ObjectView => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME without relation"))?;
                let (schema, name) = range_var_name(rv);
                let Some(v) = catalog.view(schema, name, &path).cloned() else { return missing(name) };
                catalog.views.remove(&(v.schema.clone(), v.name.clone()));
                let mut v = (*v).clone();
                v.name = new.clone();
                catalog.views.insert((v.schema.clone(), v.name.clone()), Arc::new(v));
            }
            O::ObjectTabconstraint => {
                let rv = rv.ok_or_else(|| Error::internal("RENAME CONSTRAINT without relation"))?;
                let (schema, name) = range_var_name(rv);
                let Some(t) = catalog.table(schema, name, &path).cloned() else { return missing(name) };
                let mut t = (*t).clone();
                let mut found = false;
                for ix in t.indexes.iter_mut().filter(|ix| ix.name == stmt.subname) {
                    ix.name = new.clone();
                    found = true;
                }
                for c in t.checks.iter_mut().filter(|c| c.name == stmt.subname) {
                    c.name = new.clone();
                    found = true;
                }
                for f in t.foreign_keys.iter_mut().filter(|f| f.name == stmt.subname) {
                    f.name = new.clone();
                    found = true;
                }
                if !found {
                    return Err(Error::new(
                        sqlstate::UNDEFINED_OBJECT,
                        format!("constraint \"{}\" for table \"{}\" does not exist", stmt.subname, t.name),
                    ));
                }
                put_table(catalog, t);
            }
            other => return Err(feature(&format!("RENAME for {other:?}"))),
        }
        Ok(())
    })?;
    done(tag)
}

fn sequence_options(seq: &mut Sequence, options: &[pb::Node], restart: &mut Option<i64>) -> Result<()> {
    let int = |n: Option<&pb::Node>| -> Result<i64> {
        n.and_then(const_text)
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| Error::syntax("sequence option requires an integer value"))
    };
    let mut explicit_start = false;
    for (name, arg) in def_elems(options) {
        match name {
            "increment" => {
                seq.increment = int(arg)?;
                if seq.increment == 0 {
                    return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "INCREMENT must not be zero"));
                }
            }
            "minvalue" => {
                if arg.is_some() {
                    seq.min = int(arg)?;
                }
            }
            "maxvalue" => {
                if arg.is_some() {
                    seq.max = int(arg)?;
                }
            }
            "start" => {
                seq.start = int(arg)?;
                explicit_start = true;
            }
            "restart" => *restart = Some(if arg.is_some() { int(arg)? } else { seq.start }),
            "cycle" => seq.cycle = arg.and_then(const_text).is_none_or(|v| v != "false" && v != "0" && v != "off"),
            "cache" | "owned_by" | "as" | "sequence_name" => {}
            other => return Err(feature(&format!("sequence option {other}"))),
        }
    }
    if seq.increment < 0 && !explicit_start && seq.start == 1 {
        if seq.max == i64::MAX {
            seq.max = -1;
        }
        if seq.min == 1 {
            seq.min = i64::MIN;
        }
        seq.start = seq.max;
    }
    if !explicit_start && seq.increment > 0 && seq.start < seq.min {
        seq.start = seq.min;
    }
    if seq.min > seq.max {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "MINVALUE must be less than MAXVALUE"));
    }
    if seq.start < seq.min || seq.start > seq.max {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "START value is outside the sequence's range"));
    }
    Ok(())
}

fn create_sequence(sess: &mut Session, stmt: &pb::CreateSeqStmt) -> Result<QueryResult> {
    let rv = stmt.sequence.as_ref().ok_or_else(|| Error::internal("CREATE SEQUENCE without name"))?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _, shared, _| {
        let (schema, name) = range_var_name(rv);
        let schema = creation_schema(catalog, schema, &path)?;
        if catalog.relation_name_taken(&schema, name) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(duplicate_relation(name));
        }
        let mut seq = Sequence {
            id: catalog.alloc_id(),
            schema: schema.clone(),
            name: name.to_string(),
            start: 1,
            increment: 1,
            min: 1,
            max: i64::MAX,
            cycle: false,
            owned_by: None,
        };
        sequence_options(&mut seq, &stmt.options, &mut None)?;
        shared.reset_counter(seq.id)?;
        catalog.sequences.insert((schema, name.to_string()), Arc::new(seq));
        Ok(())
    })?;
    done("CREATE SEQUENCE")
}

fn alter_sequence(sess: &mut Session, stmt: &pb::AlterSeqStmt) -> Result<QueryResult> {
    let rv = stmt.sequence.as_ref().ok_or_else(|| Error::internal("ALTER SEQUENCE without name"))?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _, shared, _| {
        let (schema, name) = range_var_name(rv);
        let Some(old) = catalog.sequence(schema, name, &path).cloned() else {
            if stmt.missing_ok {
                return Ok(());
            }
            return Err(undefined_table(name));
        };
        let mut seq = (*old).clone();
        let mut restart = None;
        sequence_options(&mut seq, &stmt.options, &mut restart)?;
        if let Some(v) = restart {
            shared.setval(&seq, v, false)?;
        }
        catalog.sequences.insert((seq.schema.clone(), seq.name.clone()), Arc::new(seq));
        Ok(())
    })?;
    done("ALTER SEQUENCE")
}

fn create_enum(sess: &mut Session, stmt: &pb::CreateEnumStmt) -> Result<QueryResult> {
    let parts = name_list(&stmt.type_name)?;
    let labels = name_list(&stmt.vals)?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _, _, _| {
        let (schema, name) = qualified(&parts);
        let schema = creation_schema(catalog, schema, &path)?;
        if catalog.enums.contains_key(&(schema.clone(), name.to_string())) || Type::from_name(name).is_some() {
            return Err(Error::new(sqlstate::DUPLICATE_OBJECT, format!("type \"{name}\" already exists")));
        }
        let mut seen = HashSet::new();
        if let Some(dup) = labels.iter().find(|l| !seen.insert(l.as_str())) {
            return Err(Error::new(sqlstate::DUPLICATE_OBJECT, format!("enum label \"{dup}\" used more than once")));
        }
        let ty =
            EnumType { id: catalog.alloc_id(), schema: schema.clone(), name: name.to_string(), labels: labels.clone() };
        catalog.enums.insert((schema, name.to_string()), Arc::new(ty));
        Ok(())
    })?;
    done("CREATE TYPE")
}

fn alter_enum(sess: &mut Session, stmt: &pb::AlterEnumStmt) -> Result<QueryResult> {
    let parts = name_list(&stmt.type_name)?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _, _, _| {
        let (schema, name) = qualified(&parts);
        let old = catalog
            .enum_type(schema, name, &path)
            .cloned()
            .ok_or_else(|| Error::new(sqlstate::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist")))?;
        let mut ty = (*old).clone();
        if !stmt.old_val.is_empty() {
            return Err(feature("renaming enum values"));
        }
        if ty.labels.contains(&stmt.new_val) {
            if stmt.skip_if_new_val_exists {
                return Ok(());
            }
            return Err(Error::new(
                sqlstate::DUPLICATE_OBJECT,
                format!("enum label \"{}\" already exists", stmt.new_val),
            ));
        }
        if !stmt.new_val_neighbor.is_empty() {
            // Stored values carry their position, so labels can only be
            // appended.
            return Err(feature("ALTER TYPE ... ADD VALUE ... BEFORE/AFTER"));
        }
        ty.labels.push(stmt.new_val.clone());
        catalog.enums.insert((ty.schema.clone(), ty.name.clone()), Arc::new(ty));
        Ok(())
    })?;
    done("ALTER TYPE")
}

fn create_view(sess: &mut Session, stmt: &pb::ViewStmt) -> Result<QueryResult> {
    let rv = stmt.view.as_ref().ok_or_else(|| Error::internal("CREATE VIEW without name"))?;
    let query = stmt.query.as_ref().ok_or_else(|| Error::internal("CREATE VIEW without query"))?;
    let Some(N::SelectStmt(sel)) = query.node.as_ref() else {
        return Err(Error::syntax("a view must be defined by a SELECT"));
    };
    let sql = deparse_stmt(query)?;
    let columns = name_list(&stmt.aliases)?;
    let path = sess.state.search_path.clone();
    sess.alter_catalog(|catalog, _, _, _| {
        let (schema, name) = range_var_name(rv);
        let schema = creation_schema(catalog, schema, &path)?;
        let key = (schema.clone(), name.to_string());
        let existing = catalog.views.get(&key).cloned();
        if existing.is_none() && catalog.relation_name_taken(&schema, name) {
            return Err(duplicate_relation(name));
        }
        if existing.is_some() && !stmt.replace {
            return Err(duplicate_relation(name));
        }
        // The definition must bind against the current schema.
        let out = Binder::new(catalog, &path, Vec::new()).bind_query(sel)?.1;
        if columns.len() > out.len() {
            return Err(Error::syntax("CREATE VIEW specifies more column names than columns"));
        }
        let id = existing.map_or_else(|| catalog.alloc_id(), |v| v.id);
        catalog.views.insert(
            key,
            Arc::new(View { id, schema, name: name.to_string(), sql: sql.clone(), columns: columns.clone() }),
        );
        Ok(())
    })?;
    done("CREATE VIEW")
}

fn create_schema(sess: &mut Session, stmt: &pb::CreateSchemaStmt) -> Result<QueryResult> {
    if !stmt.schema_elts.is_empty() {
        return Err(feature("CREATE SCHEMA with embedded objects"));
    }
    let name = stmt.schemaname.clone();
    sess.alter_catalog(|catalog, _, _, _| {
        if catalog.has_schema(&name) || matches!(name.as_str(), "pg_catalog" | "information_schema") {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::new(sqlstate::DUPLICATE_SCHEMA, format!("schema \"{name}\" already exists")));
        }
        catalog.schemas.push(name.clone());
        Ok(())
    })?;
    done("CREATE SCHEMA")
}

fn truncate(sess: &mut Session, stmt: &pb::TruncateStmt) -> Result<QueryResult> {
    let path = sess.state.search_path.clone();
    let wait = WaitPolicy::Wait(sess.state.lock_timeout);
    let cascade = stmt.behavior == pb::DropBehavior::DropCascade as i32;
    let catalog = sess.catalog();
    let mut tables: Vec<Arc<Table>> = Vec::new();
    for r in &stmt.relations {
        let N::RangeVar(rv) = node(r)? else { continue };
        tables.push(find_table(&catalog, rv, &path)?);
    }
    // Tables referencing the truncated ones must be truncated as well.
    let mut i = 0;
    while i < tables.len() {
        for (child, fk) in catalog.referencing(tables[i].id) {
            if tables.iter().any(|t| t.id == child.id) {
                continue;
            }
            if !cascade {
                return Err(Error::new(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    "cannot truncate a table referenced in a foreign key constraint",
                )
                .with_detail(format!("Table \"{}\" references \"{}\".", child.name, tables[i].name))
                .with_hint(format!(
                    "Truncate table \"{}\" at the same time, or use TRUNCATE ... CASCADE.",
                    child.name
                )));
            }
            let _ = fk;
            tables.push(child);
        }
        i += 1;
    }
    sess.start_statement()?;
    let shared = sess.shared.clone();
    let t = sess.txn.as_mut().unwrap();
    for table in &tables {
        Session::lock_table_exclusive(&mut t.txn, table.id, wait)?;
        clear_table(table, &mut t.txn)?;
        crate::columnar::clear(table, &mut t.txn, &mut t.dropped_objects)?;
        if stmt.restart_seqs {
            for seq in catalog.sequences.values() {
                if matches!(&seq.owned_by, Some((id, _)) if *id == table.id) {
                    shared.setval(seq, seq.start, false)?;
                }
            }
        }
    }
    done("TRUNCATE TABLE")
}

fn create_table_as(sess: &mut Session, stmt: &pb::CreateTableAsStmt) -> Result<QueryResult> {
    let into = stmt.into.as_ref().ok_or_else(|| Error::internal("CREATE TABLE AS without target"))?;
    let rv = into.rel.as_ref().ok_or_else(|| Error::internal("CREATE TABLE AS without target"))?;
    let query = stmt.query.as_ref().ok_or_else(|| Error::internal("CREATE TABLE AS without query"))?;
    let Some(N::SelectStmt(sel)) = query.node.as_ref() else { return Err(feature("CREATE TABLE AS EXECUTE")) };
    let path = sess.state.search_path.clone();
    let names = name_list(&into.col_names)?;
    let created = sess.alter_catalog(|catalog, _, shared, _| {
        let (schema, name) = range_var_name(rv);
        let schema = creation_schema(catalog, schema, &path)?;
        if catalog.relation_name_taken(&schema, name) {
            if stmt.if_not_exists {
                return Ok(None);
            }
            return Err(duplicate_relation(name));
        }
        let out = Binder::new(catalog, &path, Vec::new()).bind_query(sel)?.1;
        let mut columns: Vec<Column> = out
            .iter()
            .enumerate()
            .map(|(i, c)| Column {
                name: names.get(i).cloned().unwrap_or_else(|| c.name.clone()),
                ty: c.ty.clone(),
                decl: c.ty.name(),
                decl_oid: c.ty.oid(),
                typmod: -1,
                not_null: false,
                default: None,
                identity_always: false,
                hidden: false,
            })
            .collect();
        columns.push(rowid_column());
        let id = catalog.alloc_id();
        let pk = vec![columns.len() - 1];
        shared.reset_counter(id)?;
        let table = Table {
            id,
            schema: schema.clone(),
            name: name.to_string(),
            columns,
            pk,
            rowid: true,
            indexes: Vec::new(),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            columnar: None,
        };
        let qualified = quote_qualified(&schema, name);
        put_table(catalog, table);
        Ok(Some(qualified))
    })?;
    let mut count = 0;
    if let (Some(target), false) = (created, into.skip_data) {
        let sql = format!("INSERT INTO {target} {}", deparse_stmt(query)?);
        let insert = sess.prepare(&sql, &[])?;
        // Run inside the surrounding statement's transaction.
        let explicit = sess.txn.as_ref().is_some_and(|t| t.explicit);
        if let Some(t) = &mut sess.txn {
            t.explicit = true;
        }
        let result = sess.execute_prepared(&insert, &[]);
        if let Some(t) = &mut sess.txn {
            t.explicit = explicit;
        }
        count = result?.rows_affected;
    }
    done(&format!("SELECT {count}"))
}

fn transaction(sess: &mut Session, stmt: &pb::TransactionStmt) -> Result<QueryResult> {
    use pb::TransactionStmtKind as K;
    match K::try_from(stmt.kind).unwrap_or(K::Undefined) {
        K::TransStmtBegin | K::TransStmtStart => {
            let mut isolation = None;
            let mut read_only = false;
            for (name, arg) in def_elems(&stmt.options) {
                let value = arg.and_then(const_text).unwrap_or_default();
                match name {
                    "transaction_isolation" => isolation = Some(parse_isolation(&value)?),
                    "transaction_read_only" => {
                        read_only = matches!(value.as_str(), "1" | "on" | "true");
                        if !read_only && sess.shared.in_recovery() {
                            return Err(Error::new(
                                sqlstate::READ_ONLY_SQL_TRANSACTION,
                                "cannot set transaction read-write mode on a read replica",
                            ));
                        }
                    }
                    _ => {}
                }
            }
            match &mut sess.txn {
                // Already inside a block: Postgres only warns.
                Some(t) if t.explicit && !t.batch => {}
                // BEGIN inside a multi-statement string turns the string's
                // implicit transaction into a real block.
                Some(t) => {
                    t.explicit = true;
                    t.batch = false;
                    if isolation.is_some_and(|i| i != t.txn.isolation()) && !t.txn.has_writes() {
                        sess.rollback();
                        sess.begin(isolation, true)?;
                    }
                }
                None => sess.begin(isolation, true)?,
            }
            if let Some(t) = &mut sess.txn {
                t.txn.set_read_only(read_only || sess.shared.in_recovery());
            }
            done("BEGIN")
        }
        K::TransStmtCommit => {
            let failed = sess.txn.as_ref().is_some_and(|t| t.failed);
            if failed {
                sess.rollback();
                return done("ROLLBACK");
            }
            // Make the surrounding statement machinery skip its own commit.
            let result = sess.commit();
            if result.is_err() {
                sess.rollback();
            }
            result?;
            done("COMMIT")
        }
        K::TransStmtRollback => {
            sess.rollback();
            done("ROLLBACK")
        }
        K::TransStmtSavepoint => {
            sess.savepoint(&stmt.savepoint_name)?;
            done("SAVEPOINT")
        }
        K::TransStmtRelease => {
            sess.release(&stmt.savepoint_name)?;
            done("RELEASE")
        }
        K::TransStmtRollbackTo => {
            sess.rollback_to(&stmt.savepoint_name)?;
            done("ROLLBACK")
        }
        _ => Err(feature("two-phase commit")),
    }
}

fn set_variable(sess: &mut Session, stmt: &pb::VariableSetStmt) -> Result<QueryResult> {
    use pb::VariableSetKind as K;
    let kind = K::try_from(stmt.kind).unwrap_or(K::Undefined);
    let values: Vec<String> = stmt.args.iter().filter_map(const_text).collect();
    match kind {
        K::VarSetValue | K::VarSetCurrent => {
            let value = if stmt.name == "search_path" {
                values.join(", ")
            } else {
                values.first().cloned().unwrap_or_default()
            };
            if stmt.name.eq_ignore_ascii_case("transaction_isolation") {
                // SET transaction_isolation acts on the current block.
                let iso = parse_isolation(&value)?;
                if sess.txn.as_ref().is_some_and(|t| t.txn.isolation() != iso && t.txn.has_writes()) {
                    return Err(Error::new(
                        sqlstate::ACTIVE_SQL_TRANSACTION,
                        "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                    ));
                }
                if sess.txn.as_ref().is_some_and(|t| t.explicit) {
                    let explicit = true;
                    sess.rollback();
                    sess.begin(Some(iso), explicit)?;
                }
                return done("SET");
            }
            apply_setting(&mut sess.state, &stmt.name, Some(&value))?;
            done("SET")
        }
        K::VarSetDefault | K::VarReset => {
            apply_setting(&mut sess.state, &stmt.name, None)?;
            done(if kind == K::VarReset { "RESET" } else { "SET" })
        }
        K::VarResetAll => {
            sess.state.settings.clear();
            sess.state.search_path = vec!["public".to_string()];
            sess.state.tz = chrono_tz::UTC;
            sess.state.lock_timeout = None;
            done("RESET")
        }
        K::VarSetMulti => {
            // SET TRANSACTION ... / SET SESSION CHARACTERISTICS ...
            let session_wide = stmt.name.eq_ignore_ascii_case("SESSION CHARACTERISTICS");
            for (name, arg) in def_elems(&stmt.args) {
                let value = arg.and_then(const_text).unwrap_or_default();
                match name {
                    "transaction_isolation" => {
                        let iso = parse_isolation(&value)?;
                        if session_wide {
                            sess.state.default_isolation = iso;
                        } else if let Some(t) = &sess.txn {
                            if t.txn.isolation() != iso {
                                if t.txn.has_writes() {
                                    return Err(Error::new(
                                        sqlstate::ACTIVE_SQL_TRANSACTION,
                                        "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                                    ));
                                }
                                let explicit = t.explicit;
                                sess.rollback();
                                sess.begin(Some(iso), explicit)?;
                            }
                        }
                    }
                    "transaction_read_only" => {
                        let read_only = matches!(value.as_str(), "1" | "on" | "true");
                        if !read_only && sess.shared.in_recovery() {
                            return Err(Error::new(
                                sqlstate::READ_ONLY_SQL_TRANSACTION,
                                "cannot set transaction read-write mode on a read replica",
                            ));
                        }
                        if let Some(t) = &mut sess.txn {
                            t.txn.set_read_only(read_only);
                        }
                    }
                    _ => {}
                }
            }
            done("SET")
        }
        K::Undefined => Err(Error::internal("undefined SET kind")),
    }
}

fn explain(sess: &mut Session, stmt: &pb::ExplainStmt) -> Result<QueryResult> {
    let query = stmt.query.as_ref().ok_or_else(|| Error::internal("EXPLAIN without statement"))?;
    let mut analyze = false;
    for (name, arg) in def_elems(&stmt.options) {
        let on = arg.and_then(const_text).is_none_or(|v| !matches!(v.as_str(), "false" | "off" | "0"));
        match name {
            "analyze" => analyze = on,
            "format" if arg.and_then(const_text).is_some_and(|v| !v.eq_ignore_ascii_case("text")) => {
                return Err(feature("EXPLAIN formats other than TEXT"));
            }
            _ => {}
        }
    }
    let sql = deparse_stmt(query)?;
    let prepared = sess.prepare(&sql, &[])?;
    let mut lines = crate::explain::render(&prepared);
    if analyze {
        let start = std::time::Instant::now();
        let explicit = sess.txn.as_ref().is_some_and(|t| t.explicit);
        if sess.txn.is_none() {
            sess.begin(None, false)?;
        }
        sess.txn.as_mut().unwrap().explicit = true;
        let result = sess.execute_prepared(&prepared, &[]);
        if let Some(t) = &mut sess.txn {
            t.explicit = explicit;
        }
        let result = result?;
        let rows = if prepared.returns_rows() { result.rows.len() as u64 } else { result.rows_affected };
        lines.push(format!("Actual Rows: {rows}"));
        lines.push(format!("Execution Time: {:.3} ms", start.elapsed().as_secs_f64() * 1000.0));
    }
    Ok(QueryResult {
        columns: text_column("QUERY PLAN"),
        rows: lines.into_iter().map(|l| vec![Value::Text(l)]).collect(),
        tag: "EXPLAIN".into(),
        rows_affected: 0,
    })
}

/// Runs a statement that is neither a query nor INSERT/UPDATE/DELETE.
pub(crate) fn run_utility(sess: &mut Session, stmt: &N, _params: &[Value]) -> Result<QueryResult> {
    match stmt {
        N::TransactionStmt(s) => transaction(sess, s),
        N::CreateStmt(s) => create_table(sess, s),
        N::CreateTableAsStmt(s) => create_table_as(sess, s),
        N::IndexStmt(s) => create_index(sess, s),
        N::DropStmt(s) => drop_objects(sess, s),
        N::AlterTableStmt(s) => alter_table(sess, s),
        N::RenameStmt(s) => rename(sess, s),
        N::CreateSeqStmt(s) => create_sequence(sess, s),
        N::AlterSeqStmt(s) => alter_sequence(sess, s),
        N::CreateEnumStmt(s) => create_enum(sess, s),
        N::AlterEnumStmt(s) => alter_enum(sess, s),
        N::ViewStmt(s) => create_view(sess, s),
        N::CreateSchemaStmt(s) => create_schema(sess, s),
        N::TruncateStmt(s) => truncate(sess, s),
        N::VariableSetStmt(s) => set_variable(sess, s),
        N::VariableShowStmt(s) => {
            if s.name.eq_ignore_ascii_case("all") {
                return Err(feature("SHOW ALL"));
            }
            let value = if s.name.eq_ignore_ascii_case("transaction_isolation") {
                match sess.txn.as_ref().map_or(sess.state.default_isolation, |t| t.txn.isolation()) {
                    Isolation::ReadCommitted => "read committed".to_string(),
                    Isolation::RepeatableRead => "repeatable read".to_string(),
                    Isolation::Serializable => "serializable".to_string(),
                }
            } else if s.name.eq_ignore_ascii_case("transaction_read_only") {
                // The open transaction's mode; a replica is always read-only.
                let read_only = match &sess.txn {
                    Some(t) if t.explicit => t.txn.is_read_only(),
                    _ => sess.shared.in_recovery(),
                };
                if read_only { "on" } else { "off" }.to_string()
            } else {
                show_setting(&sess.state, &s.name)?
            };
            Ok(QueryResult {
                columns: text_column(&show_column_name(&s.name)),
                rows: vec![vec![Value::Text(value)]],
                tag: "SHOW".into(),
                rows_affected: 0,
            })
        }
        N::ExplainStmt(s) => explain(sess, s),
        N::PrepareStmt(s) => {
            if sess.named.contains_key(&s.name) {
                return Err(Error::new(
                    sqlstate::DUPLICATE_PREPARED_STATEMENT,
                    format!("prepared statement \"{}\" already exists", s.name),
                ));
            }
            let query = s.query.as_ref().ok_or_else(|| Error::internal("PREPARE without statement"))?;
            let catalog = sess.catalog();
            let binder = Binder::new(&catalog, &sess.state.search_path, Vec::new());
            let mut types = Vec::with_capacity(s.argtypes.len());
            for t in &s.argtypes {
                let N::TypeName(tn) = node(t)? else { continue };
                types.push(Some(binder.resolve_type(tn)?.ty));
            }
            let prepared = sess.prepare(&deparse_stmt(query)?, &types)?;
            sess.named.insert(s.name.clone(), prepared);
            done("PREPARE")
        }
        N::ExecuteStmt(s) => {
            let target = sess.named.get(&s.name).cloned().ok_or_else(|| {
                Error::new(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{}\" does not exist", s.name),
                )
            })?;
            if s.params.len() != target.param_types.len() {
                return Err(Error::syntax(format!("wrong number of parameters for prepared statement \"{}\"", s.name))
                    .with_detail(format!(
                        "Expected {} parameters but got {}.",
                        target.param_types.len(),
                        s.params.len()
                    )));
            }
            // Parameters are constant expressions.
            let catalog = sess.catalog();
            let path = sess.state.search_path.clone();
            let mut binder = Binder::new(&catalog, &path, Vec::new());
            let mut exprs = Vec::with_capacity(s.params.len());
            for (p, ty) in s.params.iter().zip(&target.param_types) {
                exprs.push(binder.bind_as(p, &Scope::default(), ty)?);
            }
            let implicit = sess.txn.is_none();
            if implicit {
                sess.begin(None, false)?;
            }
            let values = sess.with_cx(|cx| exprs.iter().map(|e| e.eval(&[], cx)).collect::<Result<Vec<_>>>());
            if implicit {
                sess.rollback();
            }
            sess.execute_prepared(&target, &values?)
        }
        N::DeallocateStmt(s) => {
            if s.isall || s.name.is_empty() {
                sess.named.clear();
            } else if sess.named.remove(&s.name).is_none() {
                return Err(Error::new(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{}\" does not exist", s.name),
                ));
            }
            done(if s.isall { "DEALLOCATE ALL" } else { "DEALLOCATE" })
        }
        N::DiscardStmt(_) => {
            sess.named.clear();
            sess.state.settings.clear();
            sess.state.currval.clear();
            sess.state.lastval = None;
            done("DISCARD ALL")
        }
        N::VacuumStmt(s) => {
            if sess.txn.as_ref().is_some_and(|t| t.explicit) && s.is_vacuumcmd {
                return Err(Error::new(
                    sqlstate::ACTIVE_SQL_TRANSACTION,
                    "VACUUM cannot run inside a transaction block",
                ));
            }
            if s.is_vacuumcmd {
                sess.shared.check_writable("VACUUM")?;
                sess.shared.store.gc()?;
                // For columnar tables VACUUM moves the delta into segments
                // and merges small segments.
                let catalog = sess.catalog();
                let path = sess.state.search_path.clone();
                let mut wanted = Vec::new();
                for r in &s.rels {
                    if let Some(N::VacuumRelation(v)) = r.node.as_ref() {
                        if let Some(rv) = &v.relation {
                            wanted.push(find_table(&catalog, rv, &path)?.id);
                        }
                    }
                }
                let ids: Vec<u32> = catalog
                    .tables
                    .values()
                    .filter(|t| t.columnar.is_some() && (wanted.is_empty() || wanted.contains(&t.id)))
                    .map(|t| t.id)
                    .collect();
                let compacted = !ids.is_empty();
                for id in ids {
                    sess.compact_table(id)?;
                }
                // Compaction deletes the delta rows it moved; without this
                // their tombstones would slow every scan of the delta
                // until the next VACUUM.
                if compacted {
                    sess.shared.store.gc()?;
                }
            }
            done(if s.is_vacuumcmd { "VACUUM" } else { "ANALYZE" })
        }
        N::CheckPointStmt(_) => {
            sess.shared.store.engine().checkpoint()?;
            done("CHECKPOINT")
        }
        // Accepted for compatibility with dumps and migration tools;
        // squad has no roles, extensions or comments.
        N::CreateExtensionStmt(_) => done("CREATE EXTENSION"),
        N::CommentStmt(_) => done("COMMENT"),
        N::GrantStmt(s) => done(if s.is_grant { "GRANT" } else { "REVOKE" }),
        N::GrantRoleStmt(s) => done(if s.is_grant { "GRANT ROLE" } else { "REVOKE ROLE" }),
        N::AlterOwnerStmt(_) => done("ALTER TABLE"),
        N::AlterDefaultPrivilegesStmt(_) => done("ALTER DEFAULT PRIVILEGES"),
        N::CreateRoleStmt(_) => done("CREATE ROLE"),
        N::AlterRoleStmt(_) | N::AlterRoleSetStmt(_) => done("ALTER ROLE"),
        N::DropRoleStmt(_) => done("DROP ROLE"),
        N::LockStmt(_) => done("LOCK TABLE"),
        N::ConstraintsSetStmt(_) => done("SET CONSTRAINTS"),
        N::ReindexStmt(_) => done("REINDEX"),
        N::ClusterStmt(_) => done("CLUSTER"),
        N::CreateFunctionStmt(_) | N::DoStmt(_) | N::CallStmt(_) => {
            Err(feature("user-defined functions and procedural code"))
        }
        N::CreateTrigStmt(_) => Err(feature("triggers")),
        N::ListenStmt(_) | N::NotifyStmt(_) | N::UnlistenStmt(_) => Err(feature("LISTEN/NOTIFY")),
        N::DeclareCursorStmt(_) | N::FetchStmt(_) | N::ClosePortalStmt(_) => Err(feature("SQL cursors")),
        N::CopyStmt(_) => Err(feature("COPY through this interface")),
        N::MergeStmt(_) => Err(feature("MERGE")),
        N::CreatedbStmt(_) | N::DropdbStmt(_) => Err(feature("multiple databases")),
        N::CreateDomainStmt(_) | N::CompositeTypeStmt(_) | N::CreateRangeStmt(_) | N::DefineStmt(_) => {
            Err(feature("this kind of user-defined type"))
        }
        other => Err(feature(&format!("statement {}", crate::bind::node_kind(other)))),
    }
}

#[allow(dead_code)]
fn _relation(_: Relation) {}
