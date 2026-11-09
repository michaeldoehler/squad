//! Row generators for the `information_schema` views.

use super::{builtin_type, row, Att, Env, Rel};
use crate::catalog::FkAction;
use crate::expr::Row;
use crate::types::Type;

fn yes_no(b: bool) -> &'static str {
    if b {
        "YES"
    } else {
        "NO"
    }
}

fn fk_rule(a: FkAction) -> &'static str {
    match a {
        FkAction::NoAction => "NO ACTION",
        FkAction::Restrict => "RESTRICT",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
    }
}

fn constraint_type(kind: char) -> &'static str {
    match kind {
        'p' => "PRIMARY KEY",
        'u' => "UNIQUE",
        'c' => "CHECK",
        _ => "FOREIGN KEY",
    }
}

/// One row of `information_schema.columns`.
fn column_row(rel: &Rel, env: &Env, schema: &str, table: &str, att: &Att, updatable: bool) -> Row {
    let builtin = builtin_type(att.typid);
    let udt_name = match &att.ty {
        Type::Enum(name) => name.clone(),
        Type::Array(elem) => match &**elem {
            Type::Enum(name) => format!("_{name}"),
            _ => builtin.map_or_else(|| "_text".to_string(), |t| t.name.to_string()),
        },
        _ => builtin.map_or_else(|| "text".to_string(), |t| t.name.to_string()),
    };
    let udt_schema = match &att.ty {
        Type::Enum(name) => env.cat.enums.values().find(|e| e.name == *name).map_or("public", |e| e.schema.as_str()),
        _ => "pg_catalog",
    };
    let data_type = match &att.ty {
        Type::Array(_) => "ARRAY".to_string(),
        Type::Enum(_) => "USER-DEFINED".to_string(),
        _ => env.format_type(att.typid, -1).trim_matches('"').to_string(),
    };
    let is_char = matches!(att.typid, 1042 | 1043);
    let char_len = (is_char && att.typmod >= 4).then(|| att.typmod - 4);
    let octet_len = match (&att.ty, char_len) {
        (Type::Text, Some(n)) => Some(n * 4),
        (Type::Text, None) => Some(1 << 30),
        _ => None,
    };
    let (precision, radix, scale) = match att.ty {
        Type::Int2 => (Some(16), Some(2), Some(0)),
        Type::Int4 => (Some(32), Some(2), Some(0)),
        Type::Int8 => (Some(64), Some(2), Some(0)),
        Type::Float4 => (Some(24), Some(2), None),
        Type::Float8 => (Some(53), Some(2), None),
        Type::Numeric if att.typmod >= 4 => {
            (Some(((att.typmod - 4) >> 16) & 0xffff), Some(10), Some((att.typmod - 4) & 0xffff))
        }
        Type::Numeric => (None, Some(10), None),
        _ => (None, None, None),
    };
    let datetime_precision = match att.ty {
        Type::Date => Some(0),
        Type::Time | Type::Timestamp | Type::TimestampTz | Type::Interval => Some(6),
        _ => None,
    };
    row![rel;
        "table_catalog" => &env.database,
        "table_schema" => schema,
        "table_name" => table,
        "column_name" => &att.name,
        "ordinal_position" => att.num as i32,
        "column_default" => att.default.clone(),
        "is_nullable" => yes_no(!att.not_null),
        "data_type" => data_type,
        "character_maximum_length" => char_len,
        "character_octet_length" => octet_len,
        "numeric_precision" => precision,
        "numeric_precision_radix" => radix,
        "numeric_scale" => scale,
        "datetime_precision" => datetime_precision,
        "udt_catalog" => &env.database,
        "udt_schema" => udt_schema,
        "udt_name" => udt_name,
        "dtd_identifier" => att.num.to_string(),
        "is_self_referencing" => "NO",
        "is_identity" => yes_no(att.identity_always),
        "identity_generation" => att.identity_always.then_some("ALWAYS"),
        "is_generated" => "NEVER",
        "is_updatable" => yes_no(updatable),
    ]
}

pub(super) fn scan(name: &str, rel: &Rel, env: &Env) -> Vec<Row> {
    let cat = &env.cat;
    let db = &env.database;
    let mut out: Vec<Row> = Vec::new();
    match name {
        "information_schema_catalog_name" => out.push(row![rel; "catalog_name" => db]),
        "schemata" => {
            for (_, nsname) in env.namespaces() {
                out.push(row![rel; "catalog_name" => db, "schema_name" => nsname, "schema_owner" => &env.user]);
            }
        }
        "tables" => {
            for t in cat.tables.values() {
                out.push(row![rel;
                    "table_catalog" => db,
                    "table_schema" => &t.schema,
                    "table_name" => &t.name,
                    "table_type" => "BASE TABLE",
                    "is_insertable_into" => "YES",
                    "is_typed" => "NO",
                ]);
            }
            for v in cat.views.values() {
                out.push(row![rel;
                    "table_catalog" => db,
                    "table_schema" => &v.schema,
                    "table_name" => &v.name,
                    "table_type" => "VIEW",
                    "is_insertable_into" => "NO",
                    "is_typed" => "NO",
                ]);
            }
        }
        "columns" => {
            for t in cat.tables.values() {
                for a in env.table_atts(t) {
                    out.push(column_row(rel, env, &t.schema, &t.name, &a, true));
                }
            }
            for v in cat.views.values() {
                for a in env.view_atts(v) {
                    out.push(column_row(rel, env, &v.schema, &v.name, &a, false));
                }
            }
        }
        "table_constraints" => {
            for t in cat.tables.values() {
                for con in env.constraints(t) {
                    out.push(row![rel;
                        "constraint_catalog" => db,
                        "constraint_schema" => &t.schema,
                        "constraint_name" => con.name,
                        "table_catalog" => db,
                        "table_schema" => &t.schema,
                        "table_name" => &t.name,
                        "constraint_type" => constraint_type(con.kind),
                        "is_deferrable" => "NO",
                        "initially_deferred" => "NO",
                        "enforced" => "YES",
                        "nulls_distinct" => con.index.filter(|_| con.kind == 'u').map(|ix| yes_no(!ix.nulls_not_distinct)),
                    ]);
                }
            }
        }
        "key_column_usage" => {
            for t in cat.tables.values() {
                for con in env.constraints(t).into_iter().filter(|c| c.kind != 'c') {
                    for (i, &c) in con.cols.iter().enumerate() {
                        out.push(row![rel;
                            "constraint_catalog" => db,
                            "constraint_schema" => &t.schema,
                            "constraint_name" => con.name,
                            "table_catalog" => db,
                            "table_schema" => &t.schema,
                            "table_name" => &t.name,
                            "column_name" => &t.columns[c].name,
                            "ordinal_position" => i as i32 + 1,
                            "position_in_unique_constraint" => con.fk.map(|_| i as i32 + 1),
                        ]);
                    }
                }
            }
        }
        "constraint_column_usage" => {
            for t in cat.tables.values() {
                for con in env.constraints(t) {
                    // For a foreign key the used columns are the referenced ones.
                    let (owner, cols) = match con.fk.and_then(|fk| cat.table_by_id(fk.ref_table).map(|p| (p, fk))) {
                        Some((parent, fk)) => (&**parent, fk.ref_columns.clone()),
                        None if con.kind == 'f' => continue,
                        None => (&**t, con.cols.clone()),
                    };
                    for c in cols {
                        out.push(row![rel;
                            "table_catalog" => db,
                            "table_schema" => &owner.schema,
                            "table_name" => &owner.name,
                            "column_name" => &owner.columns[c].name,
                            "constraint_catalog" => db,
                            "constraint_schema" => &t.schema,
                            "constraint_name" => con.name,
                        ]);
                    }
                }
            }
        }
        "constraint_table_usage" => {
            for t in cat.tables.values() {
                for con in env.constraints(t).into_iter().filter(|c| c.kind != 'c') {
                    let owner = match con.fk {
                        Some(fk) => match cat.table_by_id(fk.ref_table) {
                            Some(p) => &**p,
                            None => continue,
                        },
                        None => &**t,
                    };
                    out.push(row![rel;
                        "table_catalog" => db,
                        "table_schema" => &owner.schema,
                        "table_name" => &owner.name,
                        "constraint_catalog" => db,
                        "constraint_schema" => &t.schema,
                        "constraint_name" => con.name,
                    ]);
                }
            }
        }
        "referential_constraints" => {
            for t in cat.tables.values() {
                for fk in &t.foreign_keys {
                    let parent = cat.table_by_id(fk.ref_table);
                    let unique = parent.and_then(|p| env.referenced_index(p, fk));
                    out.push(row![rel;
                        "constraint_catalog" => db,
                        "constraint_schema" => &t.schema,
                        "constraint_name" => &fk.name,
                        "unique_constraint_catalog" => db,
                        "unique_constraint_schema" => parent.map(|p| p.schema.clone()),
                        "unique_constraint_name" => unique.map(|ix| ix.name.clone()),
                        "match_option" => "NONE",
                        "update_rule" => fk_rule(fk.on_update),
                        "delete_rule" => fk_rule(fk.on_delete),
                    ]);
                }
            }
        }
        "check_constraints" => {
            for t in cat.tables.values() {
                for c in &t.checks {
                    out.push(row![rel;
                        "constraint_catalog" => db,
                        "constraint_schema" => &t.schema,
                        "constraint_name" => &c.name,
                        "check_clause" => format!("({})", c.expr),
                    ]);
                }
            }
        }
        "views" => {
            for v in cat.views.values() {
                out.push(row![rel;
                    "table_catalog" => db,
                    "table_schema" => &v.schema,
                    "table_name" => &v.name,
                    "view_definition" => &v.sql,
                    "check_option" => "NONE",
                    "is_updatable" => "NO",
                    "is_insertable_into" => "NO",
                    "is_trigger_updatable" => "NO",
                    "is_trigger_deletable" => "NO",
                    "is_trigger_insertable_into" => "NO",
                ]);
            }
        }
        "sequences" => {
            for s in cat.sequences.values() {
                // Sequences of identity columns are internal in Postgres.
                let identity = s.owned_by.as_ref().is_some_and(|(table, column)| {
                    cat.table_by_id(*table)
                        .is_some_and(|t| t.columns.iter().any(|c| c.name == *column && c.identity_always))
                });
                if identity {
                    continue;
                }
                let (_, type_name, bits) = super::sequence_type(s);
                out.push(row![rel;
                    "sequence_catalog" => db,
                    "sequence_schema" => &s.schema,
                    "sequence_name" => &s.name,
                    "data_type" => type_name,
                    "numeric_precision" => bits,
                    "numeric_precision_radix" => 2,
                    "numeric_scale" => 0,
                    "start_value" => s.start.to_string(),
                    "minimum_value" => s.min.to_string(),
                    "maximum_value" => s.max.to_string(),
                    "increment" => s.increment.to_string(),
                    "cycle_option" => yes_no(s.cycle),
                ]);
            }
        }
        "character_sets" => out.push(row![rel;
            "character_set_name" => "UTF8",
            "character_repertoire" => "UCS",
            "form_of_use" => "UTF8",
            "default_collate_catalog" => db,
            "default_collate_schema" => "pg_catalog",
            "default_collate_name" => "C",
        ]),
        "enabled_roles" => out.push(row![rel; "role_name" => &env.user]),
        // Routines, triggers, domains, privileges, ...: nothing to list.
        _ => {}
    }
    out
}
