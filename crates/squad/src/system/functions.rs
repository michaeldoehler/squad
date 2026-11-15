//! Catalog helper functions: the `reg*` casts, `format_type`,
//! `pg_get_*def`, visibility and privilege checks and the like.

use std::sync::OnceLock;

use rust_decimal::prelude::ToPrimitive;

use super::{
    builtin_types, oid_of, relations, Env, ENUM_ARRAY_BASE, INFORMATION_SCHEMA, OWNER_OID, PG_CATALOG, SYSTEM_REL_BASE,
};
use crate::exec::ExecCx;
use crate::session::split_qualified;
use crate::types::{Type, Value, PG_EPOCH_UNIX_MICROS};
use crate::{sqlstate, Error, Result};

/// Type of argument `i` of a catalog helper function with `args`
/// arguments, where PostgreSQL's signature is not text. Untyped arguments
/// such as parameters get this type.
pub(crate) fn catalog_fn_arg_type(name: &str, args: usize, i: usize) -> Option<Type> {
    Some(match (name, args, i) {
        ("pg_cancel_backend" | "pg_terminate_backend", _, 0) => Type::Int4,
        ("pg_terminate_backend", 2, 1) => Type::Int8,
        (
            "pg_advisory_lock"
            | "pg_advisory_lock_shared"
            | "pg_advisory_xact_lock"
            | "pg_advisory_xact_lock_shared"
            | "pg_advisory_unlock"
            | "pg_advisory_unlock_shared"
            | "pg_try_advisory_lock"
            | "pg_try_advisory_lock_shared"
            | "pg_try_advisory_xact_lock"
            | "pg_try_advisory_xact_lock_shared",
            1 | 2,
            _,
        ) => {
            if args == 1 {
                Type::Int8
            } else {
                Type::Int4
            }
        }
        _ => return None,
    })
}

/// Result type of a catalog helper function, if `name` is one that takes
/// `args` arguments.
pub(crate) fn catalog_fn_type(name: &str, args: usize) -> Option<Type> {
    Some(match (name, args) {
        ("regclass" | "regtype" | "regproc" | "regnamespace" | "regrole" | "regprocedure" | "regoper", 1) => Type::Int8,
        ("regclass_out" | "regtype_out" | "regproc_out" | "regnamespace_out" | "regrole_out", 1) => Type::Text,
        ("to_regclass" | "to_regtype" | "to_regnamespace" | "to_regproc" | "to_regrole", 1) => Type::Int8,
        ("format_type", 2) => Type::Text,
        ("pg_get_expr", 2 | 3) => Type::Text,
        ("pg_get_indexdef", 1 | 3) => Type::Text,
        ("pg_get_constraintdef", 1 | 2) => Type::Text,
        ("pg_get_viewdef", 1 | 2) => Type::Text,
        ("pg_get_serial_sequence", 2) => Type::Text,
        ("pg_get_userbyid", 1) => Type::Text,
        (
            "pg_get_partkeydef"
            | "pg_get_statisticsobjdef_columns"
            | "pg_get_function_identity_arguments"
            | "pg_get_function_arguments"
            | "pg_get_function_result"
            | "pg_get_functiondef"
            | "pg_get_ruledef"
            | "pg_get_partition_constraintdef"
            | "pg_tablespace_location",
            1,
        ) => Type::Text,
        ("pg_get_triggerdef" | "pg_get_ruledef", 1 | 2) => Type::Text,
        (
            "pg_table_is_visible"
            | "pg_type_is_visible"
            | "pg_function_is_visible"
            | "pg_operator_is_visible"
            | "pg_opclass_is_visible"
            | "pg_collation_is_visible"
            | "pg_conversion_is_visible"
            | "pg_statistics_obj_is_visible"
            | "pg_ts_config_is_visible"
            | "pg_ts_dict_is_visible"
            | "pg_ts_parser_is_visible"
            | "pg_ts_template_is_visible"
            | "pg_is_other_temp_schema"
            | "pg_relation_is_publishable",
            1,
        ) => Type::Bool,
        ("obj_description", 1 | 2) | ("col_description" | "shobj_description", 2) => Type::Text,
        (
            "has_table_privilege"
            | "has_schema_privilege"
            | "has_database_privilege"
            | "has_sequence_privilege"
            | "has_function_privilege"
            | "has_type_privilege"
            | "has_language_privilege"
            | "has_tablespace_privilege"
            | "has_any_column_privilege"
            | "pg_has_role",
            2 | 3,
        ) => Type::Bool,
        ("has_column_privilege", 3 | 4) => Type::Bool,
        ("pg_encoding_to_char", 1) => Type::Text,
        ("pg_char_to_encoding", 1) => Type::Int4,
        ("pg_relation_size", 1 | 2) => Type::Int8,
        (
            "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" | "pg_database_size" | "pg_tablespace_size",
            1,
        ) => Type::Int8,
        ("pg_relation_filenode" | "pg_sequence_last_value", 1) => Type::Int8,
        ("pg_size_pretty", 1) => Type::Text,
        ("pg_is_in_recovery", 0) => Type::Bool,
        ("squad_promote", 0) | ("pg_promote", 0..=2) => Type::Bool,
        ("squad_replication_status", 0) => Type::Jsonb,
        ("pg_postmaster_start_time" | "pg_conf_load_time", 0) => Type::TimestampTz,
        ("pg_my_temp_schema", 0) => Type::Int8,
        ("pg_current_xact_id", 0) => Type::Int8,
        ("pg_indexam_has_property" | "pg_index_has_property", 2) | ("pg_index_column_has_property", 3) => Type::Bool,
        (
            "pg_advisory_lock" | "pg_advisory_lock_shared" | "pg_advisory_xact_lock" | "pg_advisory_xact_lock_shared",
            1 | 2,
        ) => Type::Void,
        ("pg_advisory_unlock_all", 0) => Type::Void,
        ("pg_cancel_backend", 1) | ("pg_terminate_backend", 1 | 2) => Type::Bool,
        (
            "pg_advisory_unlock"
            | "pg_advisory_unlock_shared"
            | "pg_try_advisory_lock"
            | "pg_try_advisory_lock_shared"
            | "pg_try_advisory_xact_lock"
            | "pg_try_advisory_xact_lock_shared",
            1 | 2,
        ) => Type::Bool,
        _ => return None,
    })
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Numeric(d) => d.to_i64(),
        Value::Text(s) => s.trim().parse().ok(),
        v => v.as_i64(),
    }
}

fn text(v: &Value) -> &str {
    v.as_str().unwrap_or_default()
}

fn flag(v: Option<&Value>) -> bool {
    v.and_then(Value::as_bool).unwrap_or(false)
}

/// OID of the relation called `name`, if it exists.
fn lookup_class(env: &Env, name: &str) -> Option<i64> {
    if let Ok(oid) = name.trim().parse::<i64>() {
        return Some(oid);
    }
    let (schema, rel) = split_qualified(name);
    let schema = schema.as_deref();
    let cat = &env.cat;
    if let Some(t) = cat.table(schema, &rel, &env.path) {
        return Some(oid_of(t.id));
    }
    if let Some(v) = cat.view(schema, &rel, &env.path) {
        return Some(oid_of(v.id));
    }
    if let Some(s) = cat.sequence(schema, &rel, &env.path) {
        return Some(oid_of(s.id));
    }
    if let Some((t, pos)) = cat.index(schema, &rel, &env.path) {
        return Some(oid_of(t.indexes[pos].id));
    }
    let system_schema = match schema {
        Some(s @ (PG_CATALOG | INFORMATION_SCHEMA)) => s,
        None if rel.starts_with("pg_") => PG_CATALOG,
        _ => return None,
    };
    let qualified = format!("{system_schema}.{rel}");
    relations().iter().position(|r| r.name == qualified).map(|i| SYSTEM_REL_BASE + i as i64)
}

/// OID of the type called `name` (SQL spelling or `pg_type.typname`).
fn lookup_type(env: &Env, name: &str) -> Option<i64> {
    let name = name.trim();
    if let Ok(oid) = name.parse::<i64>() {
        return Some(oid);
    }
    if let Some(elem) = name.strip_suffix("[]") {
        let elem = lookup_type(env, elem)?;
        if let Some(e) = env.enum_by_oid(elem) {
            return Some(ENUM_ARRAY_BASE + e.id as i64);
        }
        return builtin_types().iter().find(|t| t.oid == elem).map(|t| t.array).filter(|a| *a != 0);
    }
    // A type modifier does not change the type.
    let base = name.split('(').next().unwrap_or(name).trim();
    let after_parens = name.rsplit(')').next().unwrap_or("").trim();
    let spelled = if name.contains('(') && !after_parens.is_empty() {
        format!("{base} {after_parens}")
    } else {
        base.to_string()
    };
    let (schema, bare) = if spelled.contains(' ') && !spelled.contains('"') {
        (None, spelled.to_ascii_lowercase())
    } else {
        split_qualified(&spelled)
    };
    if let Some(e) = env.cat.enum_type(schema.as_deref(), &bare, &env.path) {
        return Some(oid_of(e.id));
    }
    if let Some(e) = bare.strip_prefix('_').and_then(|n| env.cat.enum_type(schema.as_deref(), n, &env.path)) {
        return Some(ENUM_ARRAY_BASE + e.id as i64);
    }
    if !matches!(schema.as_deref(), None | Some(PG_CATALOG)) {
        return None;
    }
    let alias = match bare.as_str() {
        "character varying" => Some(1043),
        "character" | "char" => Some(1042),
        "decimal" => Some(1700),
        "int" | "integer" => Some(23),
        "smallint" => Some(21),
        "bigint" => Some(20),
        "real" => Some(700),
        "double precision" | "float" => Some(701),
        "boolean" => Some(16),
        "time with time zone" => Some(1266),
        "bit varying" => Some(1562),
        _ => None,
    };
    if alias.is_some() {
        return alias;
    }
    if let Some(t) = builtin_types().iter().find(|t| t.name == bare) {
        return Some(t.oid);
    }
    Type::from_name(&bare).map(|t| t.oid() as i64)
}

fn undefined(code: &'static str, what: &str, name: &str) -> Error {
    Error::new(code, format!("{what} \"{name}\" does not exist"))
}

/// The relation an argument denotes: an OID, or a name as text.
fn class_arg(env: &Env, v: &Value) -> Result<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Text(s) => {
            lookup_class(env, s).map(Some).ok_or_else(|| undefined(sqlstate::UNDEFINED_TABLE, "relation", s))
        }
        v => Ok(int(v)),
    }
}

fn size_pretty(n: i64) -> String {
    const LIMIT: i64 = 10 * 1024;
    if n.abs() < LIMIT {
        return format!("{n} bytes");
    }
    let half_rounded = |x: i64| (x + x.signum()) / 2;
    // Keep one extra bit for rounding.
    let mut size = n >> 9;
    let units = ["kB", "MB", "GB", "TB", "PB"];
    for (i, unit) in units.iter().enumerate() {
        if size.abs() < LIMIT * 2 - 1 || i == units.len() - 1 {
            return format!("{} {unit}", half_rounded(size));
        }
        size >>= 10;
    }
    unreachable!()
}

fn start_time() -> i64 {
    static START: OnceLock<i64> = OnceLock::new();
    *START.get_or_init(|| chrono::Utc::now().timestamp_micros() - PG_EPOCH_UNIX_MICROS)
}

pub(crate) fn catalog_fn(name: &str, args: Vec<Value>, cx: &mut ExecCx) -> Result<Value> {
    if let Some(result) = crate::advisory::call(name, &args, cx) {
        return result;
    }
    if let Some(result) = crate::backends::call(name, &args, cx) {
        return result;
    }
    let env = Env::new(cx);
    let arg = |i: usize| args.get(i).unwrap_or(&Value::Null);
    let opt_text = |s: Option<String>| s.map_or(Value::Null, Value::Text);
    Ok(match name {
        // ---- name -> OID ------------------------------------------------
        "regclass" | "to_regclass" => match arg(0) {
            Value::Null => Value::Null,
            v => {
                match (int(v).filter(|_| !matches!(v, Value::Text(_))).or_else(|| lookup_class(&env, text(v))), name) {
                    (Some(oid), _) => Value::Int8(oid),
                    (None, "to_regclass") => Value::Null,
                    (None, _) => return Err(undefined(sqlstate::UNDEFINED_TABLE, "relation", text(v))),
                }
            }
        },
        "regtype" | "to_regtype" => match arg(0) {
            Value::Null => Value::Null,
            v => match (lookup_type(&env, text(v)), name) {
                (Some(oid), _) => Value::Int8(oid),
                (None, "to_regtype") => Value::Null,
                (None, _) => return Err(undefined(sqlstate::UNDEFINED_OBJECT, "type", text(v))),
            },
        },
        "regnamespace" | "to_regnamespace" => match arg(0) {
            Value::Null => Value::Null,
            v => {
                let (_, ns) = split_qualified(text(v));
                match (env.namespaces().into_iter().find(|n| n.1 == ns), name) {
                    (Some((oid, _)), _) => Value::Int8(oid),
                    (None, "to_regnamespace") => Value::Null,
                    (None, _) => return Err(undefined(sqlstate::INVALID_SCHEMA_NAME, "schema", &ns)),
                }
            }
        },
        "regrole" | "to_regrole" => match arg(0) {
            Value::Null => Value::Null,
            v if env.role_oid(text(v)).is_some() => Value::Int8(env.role_oid(text(v)).unwrap_or(OWNER_OID)),
            _ if name == "to_regrole" => Value::Null,
            v => return Err(undefined(sqlstate::UNDEFINED_OBJECT, "role", text(v))),
        },
        // There are no user-visible function or operator OIDs.
        "regproc" | "regprocedure" | "regoper" | "to_regproc" => match arg(0) {
            Value::Null => Value::Null,
            _ if name == "to_regproc" => Value::Null,
            v => Value::Int8(int(v).unwrap_or(0)),
        },

        // ---- OID -> name ------------------------------------------------
        "regclass_out" | "regtype_out" | "regnamespace_out" | "regproc_out" if int(arg(0)) == Some(0) => {
            Value::text("-")
        }
        "regclass_out" => match int(arg(0)) {
            None => Value::Null,
            Some(oid) => Value::Text(match env.relation_name(oid) {
                Some((schema, rel)) => env.display_name(&schema, &rel),
                None => oid.to_string(),
            }),
        },
        "regtype_out" => match int(arg(0)) {
            None => Value::Null,
            Some(oid) => Value::Text(match env.format_type(oid, -1) {
                t if t == "???" => oid.to_string(),
                t => t,
            }),
        },
        "regnamespace_out" => match int(arg(0)) {
            None => Value::Null,
            Some(oid) => Value::Text(env.ns_name(oid).map_or_else(|| oid.to_string(), super::quote_ident)),
        },
        "regrole_out" | "pg_get_userbyid" => match int(arg(0)) {
            None => Value::Null,
            Some(OWNER_OID) => Value::Text(env.user.clone()),
            Some(oid) if env.role_name(oid).is_some() => Value::Text(env.role_name(oid).unwrap_or_default()),
            Some(oid) if name == "regrole_out" => Value::Text(oid.to_string()),
            Some(oid) => Value::Text(format!("unknown (OID={oid})")),
        },
        "regproc_out" => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Text(int(v).map_or_else(|| text(v).to_string(), |o| o.to_string())),
        },

        // ---- definitions ------------------------------------------------
        "format_type" => match int(arg(0)) {
            None => Value::Null,
            Some(oid) => Value::Text(env.format_type(oid, int(arg(1)).map_or(-1, |m| m as i32))),
        },
        "pg_get_expr" => arg(0).clone(),
        "pg_get_indexdef" => {
            let Some((table, ix)) = class_arg(&env, arg(0))?.and_then(|oid| env.index_by_oid(oid)) else {
                return Ok(Value::Null);
            };
            match int(arg(1)).unwrap_or(0) {
                0 => Value::Text(env.index_def(table, ix, flag(args.get(2)))),
                n => match usize::try_from(n - 1).ok().and_then(|i| ix.columns.get(i)) {
                    Some(&c) => Value::Text(super::quote_ident(&table.columns[c].name)),
                    None => Value::text(""),
                },
            }
        }
        "pg_get_constraintdef" => match int(arg(0)).and_then(|oid| env.constraint_by_oid(oid)) {
            Some((table, con)) => Value::Text(env.constraint_def(table, &con, flag(args.get(1)))),
            None => Value::Null,
        },
        "pg_get_viewdef" => match class_arg(&env, arg(0))?.and_then(|oid| env.view_by_oid(oid)) {
            Some(v) => Value::Text(format!(" {};", v.sql)),
            None => Value::Null,
        },
        "pg_get_serial_sequence" => {
            if arg(0).is_null() || arg(1).is_null() {
                return Ok(Value::Null);
            }
            let oid = lookup_class(&env, text(arg(0)));
            let table = oid
                .and_then(|oid| env.table_by_oid(oid))
                .ok_or_else(|| undefined(sqlstate::UNDEFINED_TABLE, "relation", text(arg(0))))?;
            let (_, column) = split_qualified(text(arg(1)));
            if !table.columns.iter().any(|c| c.name == column && !c.hidden) {
                return Err(Error::new(
                    sqlstate::UNDEFINED_COLUMN,
                    format!("column \"{column}\" of relation \"{}\" does not exist", table.name),
                ));
            }
            let owned = env
                .cat
                .sequences
                .values()
                .find(|s| matches!(&s.owned_by, Some((t, c)) if *t == table.id && *c == column));
            opt_text(owned.map(|s| format!("{}.{}", super::quote_ident(&s.schema), super::quote_ident(&s.name))))
        }
        "pg_tablespace_location" => Value::text(""),
        "pg_get_function_identity_arguments" | "pg_get_function_arguments" => Value::text(""),
        "pg_get_partkeydef"
        | "pg_get_statisticsobjdef_columns"
        | "pg_get_function_result"
        | "pg_get_functiondef"
        | "pg_get_ruledef"
        | "pg_get_triggerdef"
        | "pg_get_partition_constraintdef"
        | "obj_description"
        | "col_description"
        | "shobj_description"
        | "pg_sequence_last_value" => Value::Null,

        // ---- visibility and privileges ------------------------------------
        "pg_table_is_visible" => match int(arg(0)).and_then(|oid| env.relation_name(oid)) {
            Some((schema, _)) => Value::Bool(env.visible(&schema)),
            None => Value::Null,
        },
        "pg_type_is_visible" => match int(arg(0)) {
            None => Value::Null,
            Some(oid) => Value::Bool(match env.enum_by_oid(oid) {
                Some(e) => env.visible(&e.schema),
                None => match env.cat.enums.values().find(|e| ENUM_ARRAY_BASE + e.id as i64 == oid) {
                    Some(e) => env.visible(&e.schema),
                    None => true,
                },
            }),
        },
        "pg_is_other_temp_schema" | "pg_relation_is_publishable" => Value::Bool(false),
        "pg_is_in_recovery" => Value::Bool(cx.shared.in_recovery()),
        "squad_promote" | "pg_promote" => {
            crate::replica::promote(cx.shared)?;
            Value::Bool(true)
        }
        "squad_replication_status" => match crate::replica::status(cx.shared) {
            Some(status) => Value::Jsonb(Box::new(status.to_json())),
            None => Value::Null,
        },
        n if n.ends_with("_is_visible") || n.starts_with("has_") || n == "pg_has_role" => Value::Bool(true),

        // ---- sizes and server facts ---------------------------------------
        "pg_encoding_to_char" => Value::text("UTF8"),
        "pg_char_to_encoding" => Value::Int4(6),
        "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" | "pg_tablespace_size" => {
            match arg(0) {
                Value::Null => Value::Null,
                _ => Value::Int8(0),
            }
        }
        "pg_database_size" => Value::Int8(cx.shared.store.engine().stats().page_count as i64 * 8192),
        "pg_relation_filenode" => class_arg(&env, arg(0))?.map_or(Value::Null, Value::Int8),
        "pg_size_pretty" => int(arg(0)).map_or(Value::Null, |n| Value::Text(size_pretty(n))),
        "pg_postmaster_start_time" | "pg_conf_load_time" => Value::TimestampTz(start_time()),
        "pg_my_temp_schema" => Value::Int8(0),
        "pg_current_xact_id" => Value::Int8(cx.txn.id() as i64),
        "pg_indexam_has_property" | "pg_index_has_property" | "pg_index_column_has_property" => {
            match text(args.last().unwrap_or(&Value::Null)).to_ascii_lowercase().as_str() {
                "asc" | "nulls_last" | "orderable" | "search_array" | "search_nulls" | "clusterable" | "index_scan"
                | "returnable" | "can_order" | "can_unique" | "can_multi_col" | "backward_scan" => Value::Bool(true),
                "desc" | "nulls_first" | "distance_orderable" | "can_exclude" | "can_include" | "bitmap_scan" => {
                    Value::Bool(false)
                }
                _ => Value::Null,
            }
        }

        other => return Err(Error::internal(format!("unknown catalog function {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_pretty_matches_postgres() {
        // Expected values from PostgreSQL 16.
        assert_eq!(size_pretty(0), "0 bytes");
        assert_eq!(size_pretty(10239), "10239 bytes");
        assert_eq!(size_pretty(10240), "10 kB");
        assert_eq!(size_pretty(8192 * 1024), "8192 kB");
        assert_eq!(size_pretty(20 * 1024 * 1024), "20 MB");
        assert_eq!(size_pretty(123456789012), "115 GB");
        assert_eq!(size_pretty(-10240), "-10 kB");
    }
}
