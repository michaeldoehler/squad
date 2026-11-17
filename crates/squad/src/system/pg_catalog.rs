//! Row generators for the `pg_catalog` relations.

use super::{
    builtin_type, builtin_types, oid_of, row, Att, Env, Rel, Vector, ATTRDEF_BASE, DATABASE_OID, ENUM_ARRAY_BASE,
    ENUM_LABEL_BASE, NS_PG_CATALOG, OWNER_OID,
};
use crate::catalog::{FkAction, Table};
use crate::exec::ExecCx;
use crate::expr::Row;
use crate::types::{Type, Value};

const AM_HEAP: i64 = 2;
const AM_BTREE: i64 = 403;
const AM_HASH: i64 = 405;
/// Default collation, reported for collatable columns.
const COLLATION_DEFAULT: i64 = 100;
const COLLATION_C: i64 = 950;

/// Settings listed in `pg_settings` in addition to those a session set.
const SETTINGS: &[&str] = &[
    "application_name",
    "bytea_output",
    "client_encoding",
    "client_min_messages",
    "datestyle",
    "default_transaction_isolation",
    "default_transaction_read_only",
    "extra_float_digits",
    "integer_datetimes",
    "intervalstyle",
    "lc_collate",
    "lc_ctype",
    "lock_timeout",
    "max_connections",
    "max_identifier_length",
    "search_path",
    "server_encoding",
    "server_version",
    "server_version_num",
    "standard_conforming_strings",
    "statement_timeout",
    "timezone",
    "transaction_isolation",
    "transaction_read_only",
    "work_mem",
];

fn fk_code(a: FkAction) -> char {
    match a {
        FkAction::NoAction => 'a',
        FkAction::Restrict => 'r',
        FkAction::Cascade => 'c',
        FkAction::SetNull => 'n',
        FkAction::SetDefault => 'd',
    }
}

/// Postgres implements foreign keys with triggers on both tables.
fn has_fk_triggers(env: &Env, t: &Table) -> bool {
    !t.foreign_keys.is_empty() || env.cat.tables.values().any(|o| o.foreign_keys.iter().any(|fk| fk.ref_table == t.id))
}

fn is_collatable(ty: &Type) -> bool {
    matches!(ty, Type::Text) || matches!(ty, Type::Array(e) if **e == Type::Text)
}

fn collation(ty: &Type) -> i64 {
    if is_collatable(ty) {
        COLLATION_DEFAULT
    } else {
        0
    }
}

/// The default B-tree operator classes of PostgreSQL 16 for the types
/// squad has: `(oid, opcname, opcfamily, opcintype, opckeytype)`.
const OPCLASSES: &[(i64, &str, i64, i64, i64)] = &[
    (1978, "int4_ops", 1976, 23, 0),
    (1979, "int2_ops", 1976, 21, 0),
    (1981, "oid_ops", 1989, 26, 0),
    (3122, "date_ops", 434, 1082, 0),
    (3123, "float8_ops", 1970, 701, 0),
    (3124, "int8_ops", 1976, 20, 0),
    (3125, "numeric_ops", 1988, 1700, 0),
    (3126, "text_ops", 1994, 25, 0),
    (3127, "timestamptz_ops", 434, 1184, 0),
    (3128, "timestamp_ops", 434, 1114, 0),
    (10000, "array_ops", 397, 2277, 0),
    (10003, "bool_ops", 424, 16, 0),
    (10004, "bpchar_ops", 426, 1042, 0),
    (10006, "bytea_ops", 428, 17, 0),
    (10007, "char_ops", 429, 18, 0),
    (10012, "float4_ops", 1970, 700, 0),
    (10022, "interval_ops", 1982, 1186, 0),
    (10028, "name_ops", 1994, 19, 2275),
    (10032, "oidvector_ops", 1991, 30, 0),
    (10034, "record_ops", 2994, 2249, 0),
    (10038, "time_ops", 1996, 1083, 0),
    (10065, "uuid_ops", 2968, 2950, 0),
    (10069, "enum_ops", 3522, 3500, 0),
    (10088, "jsonb_ops", 4033, 3802, 0),
];

/// OID of the operator class an index uses for a column: the default
/// B-tree class of its type, as PostgreSQL chooses it.
fn default_opclass(typid: i64, ty: &Type) -> i64 {
    let intype = match ty {
        Type::Enum(_) => 3500,
        Type::Array(_) => 2277,
        // varchar has no class of its own and uses text's.
        _ if typid == 1043 => 25,
        _ => typid,
    };
    OPCLASSES.iter().find(|c| c.3 == intype).map_or(0, |c| c.0)
}

/// One `pg_attribute` row.
fn attribute(rel: &Rel, relid: i64, att: &Att) -> Row {
    let t = builtin_type(att.typid);
    row![rel;
        "attrelid" => relid,
        "attname" => &att.name,
        "atttypid" => att.typid,
        "attlen" => t.map_or(if matches!(att.ty, Type::Enum(_)) { 4 } else { -1 }, |t| t.len),
        "attnum" => att.num,
        "attcacheoff" => -1,
        "atttypmod" => att.typmod,
        "attndims" => if matches!(att.ty, Type::Array(_)) { 1i16 } else { 0i16 },
        "attbyval" => t.is_some_and(|t| t.byval),
        "attalign" => t.map_or('i', |t| t.align),
        "attstorage" => t.map_or('p', |t| t.storage),
        "attcompression" => "",
        "attnotnull" => att.not_null,
        "atthasdef" => att.default.is_some(),
        "atthasmissing" => false,
        "attidentity" => if att.identity_always { "a" } else { "" },
        "attgenerated" => "",
        "attisdropped" => false,
        "attislocal" => true,
        "attinhcount" => 0i16,
        "attstattarget" => -1i16,
        "attcollation" => collation(&att.ty),
    ]
}

#[allow(clippy::too_many_arguments)]
fn class_row(
    rel: &Rel,
    env: &Env,
    oid: i64,
    name: &str,
    schema: &str,
    kind: char,
    natts: usize,
    table: Option<&Table>,
) -> Row {
    let has_index = table.is_some_and(|t| t.indexes.iter().any(|_| true));
    row![rel;
        "oid" => oid,
        "relname" => name,
        "relnamespace" => env.ns_oid(schema),
        "reltype" => 0i64,
        "reloftype" => 0i64,
        "relowner" => OWNER_OID,
        "relam" => match kind { 'r' => AM_HEAP, 'i' => AM_BTREE, _ => 0 },
        "relfilenode" => if kind == 'v' { 0 } else { oid },
        "reltablespace" => 0i64,
        "relpages" => 0,
        "reltuples" => -1.0,
        "relallvisible" => 0,
        "reltoastrelid" => 0i64,
        "relhasindex" => has_index,
        "relisshared" => false,
        "relpersistence" => 'p',
        "relkind" => kind,
        "relnatts" => natts as i16,
        "relchecks" => table.map_or(0, |t| t.checks.len()) as i16,
        "relhasrules" => kind == 'v',
        "relhastriggers" => table.is_some_and(|t| has_fk_triggers(env, t)),
        "relhassubclass" => false,
        "relrowsecurity" => table.is_some_and(|t| t.row_security),
        "relforcerowsecurity" => table.is_some_and(|t| t.force_row_security),
        "relispopulated" => true,
        "relreplident" => if kind == 'r' { 'd' } else { 'n' },
        "relispartition" => false,
        "relrewrite" => 0i64,
        "relfrozenxid" => 0i64,
        "relminmxid" => 0i64,
    ]
}

/// One `pg_type` row for a type that is not built in.
#[allow(clippy::too_many_arguments)]
fn user_type_row(
    rel: &Rel,
    oid: i64,
    name: String,
    ns: i64,
    typtype: char,
    category: char,
    elem: i64,
    array: i64,
    io: &str,
) -> Row {
    row![rel;
        "oid" => oid,
        "typname" => name,
        "typnamespace" => ns,
        "typowner" => OWNER_OID,
        "typlen" => if elem == 0 { 4i16 } else { -1i16 },
        "typbyval" => elem == 0,
        "typtype" => typtype,
        "typcategory" => category,
        "typispreferred" => false,
        "typisdefined" => true,
        "typdelim" => ',',
        "typrelid" => 0i64,
        "typsubscript" => if elem == 0 { "-" } else { "array_subscript_handler" },
        "typelem" => elem,
        "typarray" => array,
        "typinput" => format!("{io}_in"),
        "typoutput" => format!("{io}_out"),
        "typreceive" => format!("{io}_recv"),
        "typsend" => format!("{io}_send"),
        "typmodin" => "-",
        "typmodout" => "-",
        "typanalyze" => "-",
        "typalign" => 'i',
        "typstorage" => if elem == 0 { 'p' } else { 'x' },
        "typnotnull" => false,
        "typbasetype" => 0i64,
        "typtypmod" => -1,
        "typndims" => 0,
        "typcollation" => 0i64,
    ]
}

fn role_row(rel: &Rel, oid: i64, name: &str, privileged: bool) -> Row {
    // The union of the columns of pg_roles, pg_authid, pg_user and
    // pg_shadow; each relation picks the ones it has.
    let has = |c: &str| rel.cols.iter().any(|x| x.0 == c);
    let mut r = vec![Value::Null; rel.cols.len()];
    let mut set = |c: &str, v: Value| {
        if has(c) {
            r[rel.pos(c)] = v;
        }
    };
    set("oid", Value::Int8(oid));
    set("usesysid", Value::Int8(oid));
    set("rolname", Value::Text(name.to_string()));
    set("usename", Value::Text(name.to_string()));
    set("rolinherit", Value::Bool(true));
    set("rolcanlogin", Value::Bool(true));
    for c in [
        "rolsuper",
        "rolinherit",
        "rolcreaterole",
        "rolcreatedb",
        "rolcanlogin",
        "rolreplication",
        "rolbypassrls",
        "usecreatedb",
        "usesuper",
        "userepl",
        "usebypassrls",
    ] {
        if c != "rolinherit" && c != "rolcanlogin" {
            set(c, Value::Bool(privileged));
        }
    }
    set("rolconnlimit", Value::Int4(-1));
    set("rolpassword", Value::Text("********".into()));
    set("passwd", Value::Text("********".into()));
    r
}

/// A policy expression as `pg_get_expr` shows it: PostgreSQL puts an
/// operator expression in parentheses; a bare column or constant stays as
/// it is.
fn policy_expr_text(sql: &str) -> String {
    let simple = sql.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '"' || c == '.');
    if simple {
        sql.to_string()
    } else {
        format!("({sql})")
    }
}

pub(super) fn scan(name: &str, rel: &Rel, env: &Env, cx: &mut ExecCx) -> Vec<Row> {
    let cat = &env.cat;
    let mut out: Vec<Row> = Vec::new();
    match name {
        "pg_namespace" => {
            for (oid, nsname) in env.namespaces() {
                out.push(row![rel; "oid" => oid, "nspname" => nsname, "nspowner" => OWNER_OID]);
            }
        }
        "pg_class" => {
            for t in cat.tables.values() {
                let natts = t.visible_columns().count();
                out.push(class_row(rel, env, oid_of(t.id), &t.name, &t.schema, 'r', natts, Some(t)));
                for ix in &t.indexes {
                    out.push(class_row(rel, env, oid_of(ix.id), &ix.name, &t.schema, 'i', ix.columns.len(), None));
                }
            }
            for s in cat.sequences.values() {
                out.push(class_row(rel, env, oid_of(s.id), &s.name, &s.schema, 'S', 3, None));
            }
            for v in cat.views.values() {
                let natts = env.view_atts(v).len();
                out.push(class_row(rel, env, oid_of(v.id), &v.name, &v.schema, 'v', natts, None));
            }
        }
        "pg_attribute" => {
            for t in cat.tables.values() {
                let atts = env.table_atts(t);
                for a in &atts {
                    out.push(attribute(rel, oid_of(t.id), a));
                }
                for ix in &t.indexes {
                    for (n, &c) in ix.columns.iter().enumerate() {
                        let col = &t.columns[c];
                        let att = Att {
                            name: col.name.clone(),
                            num: n as i16 + 1,
                            typid: col.decl_oid as i64,
                            typmod: col.typmod,
                            not_null: false,
                            default: None,
                            identity_always: false,
                            ty: col.ty.clone(),
                        };
                        out.push(attribute(rel, oid_of(ix.id), &att));
                    }
                }
            }
            for v in cat.views.values() {
                for a in env.view_atts(v) {
                    out.push(attribute(rel, oid_of(v.id), &a));
                }
            }
        }
        "pg_attrdef" => {
            for t in cat.tables.values() {
                for a in env.table_atts(t) {
                    if let Some(def) = &a.default {
                        out.push(row![rel;
                            "oid" => ATTRDEF_BASE + t.id as i64 * 2000 + a.num as i64,
                            "adrelid" => oid_of(t.id),
                            "adnum" => a.num,
                            "adbin" => def,
                        ]);
                    }
                }
            }
        }
        "pg_type" => {
            for t in builtin_types() {
                out.push(row![rel;
                    "oid" => t.oid,
                    "typname" => t.name,
                    "typnamespace" => NS_PG_CATALOG,
                    "typowner" => OWNER_OID,
                    "typlen" => t.len,
                    "typbyval" => t.byval,
                    "typtype" => t.typtype,
                    "typcategory" => t.category,
                    "typispreferred" => t.preferred,
                    "typisdefined" => true,
                    "typdelim" => t.delim,
                    "typrelid" => 0i64,
                    "typsubscript" => if t.category == 'A' { "array_subscript_handler" } else { "-" },
                    "typelem" => t.elem,
                    "typarray" => t.array,
                    "typinput" => t.input,
                    "typoutput" => t.output,
                    "typreceive" => t.receive,
                    "typsend" => t.send,
                    "typmodin" => "-",
                    "typmodout" => "-",
                    "typanalyze" => "-",
                    "typalign" => t.align,
                    "typstorage" => t.storage,
                    "typnotnull" => false,
                    "typbasetype" => 0i64,
                    "typtypmod" => -1,
                    "typndims" => 0,
                    "typcollation" => t.collation,
                ]);
            }
            for e in cat.enums.values() {
                let (oid, arr, ns) = (oid_of(e.id), ENUM_ARRAY_BASE + e.id as i64, env.ns_oid(&e.schema));
                out.push(user_type_row(rel, oid, e.name.clone(), ns, 'e', 'E', 0, arr, "enum"));
                out.push(user_type_row(rel, arr, format!("_{}", e.name), ns, 'b', 'A', oid, 0, "array"));
            }
        }
        "pg_enum" => {
            for e in cat.enums.values() {
                for (i, label) in e.labels.iter().enumerate() {
                    out.push(row![rel;
                        "oid" => ENUM_LABEL_BASE + e.id as i64 * 1000 + i as i64,
                        "enumtypid" => oid_of(e.id),
                        "enumsortorder" => (i + 1) as f64,
                        "enumlabel" => label,
                    ]);
                }
            }
        }
        "pg_index" => {
            for t in cat.tables.values() {
                for ix in &t.indexes {
                    let n = ix.columns.len();
                    out.push(row![rel;
                        "indexrelid" => oid_of(ix.id),
                        "indrelid" => oid_of(t.id),
                        "indnatts" => n as i16,
                        "indnkeyatts" => n as i16,
                        "indisunique" => ix.unique,
                        "indnullsnotdistinct" => ix.nulls_not_distinct,
                        "indisprimary" => ix.primary,
                        "indisexclusion" => false,
                        "indimmediate" => true,
                        "indisclustered" => false,
                        "indisvalid" => true,
                        "indcheckxmin" => false,
                        "indisready" => true,
                        "indislive" => true,
                        "indisreplident" => false,
                        "indkey" => Vector::int2(env.attnums(t, &ix.columns)),
                        "indcollation" => Vector::oid(
                            ix.columns
                                .iter()
                                .map(|&c| match t.columns[c].decl_oid {
                                    // name columns have the C collation.
                                    19 => COLLATION_C,
                                    _ => collation(&t.columns[c].ty),
                                })
                                .collect()
                        ),
                        "indclass" => Vector::oid(
                            ix.columns
                                .iter()
                                .map(|&c| default_opclass(t.columns[c].decl_oid as i64, &t.columns[c].ty))
                                .collect()
                        ),
                        "indoption" => Vector::int2(vec![0; n]),
                    ]);
                }
            }
        }
        "pg_constraint" => {
            for t in cat.tables.values() {
                for con in env.constraints(t) {
                    let parent = con.fk.and_then(|fk| cat.table_by_id(fk.ref_table));
                    let conindid = match (con.index, con.fk, parent) {
                        (Some(ix), _, _) => oid_of(ix.id),
                        (_, Some(fk), Some(p)) => env.referenced_index(p, fk).map_or(0, |ix| oid_of(ix.id)),
                        _ => 0,
                    };
                    out.push(row![rel;
                        "oid" => con.oid,
                        "conname" => con.name,
                        "connamespace" => env.ns_oid(&t.schema),
                        "contype" => con.kind,
                        "condeferrable" => false,
                        "condeferred" => false,
                        "convalidated" => true,
                        "conrelid" => oid_of(t.id),
                        "contypid" => 0i64,
                        "conindid" => conindid,
                        "conparentid" => 0i64,
                        "confrelid" => parent.map_or(0, |p| oid_of(p.id)),
                        "confupdtype" => con.fk.map_or(' ', |fk| fk_code(fk.on_update)),
                        "confdeltype" => con.fk.map_or(' ', |fk| fk_code(fk.on_delete)),
                        "confmatchtype" => if con.fk.is_some() { 's' } else { ' ' },
                        "conislocal" => true,
                        "coninhcount" => 0i16,
                        "connoinherit" => con.kind != 'c',
                        "conkey" => env.attnums(t, &con.cols),
                        "confkey" => match (con.fk, parent) {
                            (Some(fk), Some(p)) => Some(env.attnums(p, &fk.ref_columns)),
                            _ => None,
                        },
                        "conbin" => con.check.map(|c| c.expr.clone()),
                    ]);
                }
            }
        }
        "pg_database" => out.push(row![rel;
            "oid" => DATABASE_OID,
            "datname" => &env.database,
            "datdba" => OWNER_OID,
            "encoding" => 6,
            "datlocprovider" => 'c',
            "datistemplate" => false,
            "datallowconn" => true,
            "datconnlimit" => -1,
            "datfrozenxid" => 0i64,
            "datminmxid" => 0i64,
            "dattablespace" => 1663i64,
            "datcollate" => "C",
            "datctype" => "C",
        ]),
        "pg_roles" | "pg_authid" | "pg_user" | "pg_shadow" => {
            for (oid, name) in env.roles() {
                out.push(role_row(rel, oid, &name, oid == OWNER_OID && env.bypass_rls));
            }
        }
        "pg_policy" => {
            for t in cat.tables.values() {
                for p in &t.policies {
                    let roles: Vec<i64> = if p.roles.is_empty() {
                        vec![0]
                    } else {
                        p.roles.iter().filter_map(|r| env.role_oid(r)).collect()
                    };
                    out.push(row![rel;
                        "oid" => oid_of(p.id),
                        "polname" => &p.name,
                        "polrelid" => oid_of(t.id),
                        "polcmd" => p.command.code(),
                        "polpermissive" => p.permissive,
                        "polroles" => roles,
                        "polqual" => p.using.as_deref().map_or(Value::Null, |e| Value::Text(policy_expr_text(e))),
                        "polwithcheck" => p.check.as_deref().map_or(Value::Null, |e| Value::Text(policy_expr_text(e))),
                    ]);
                }
            }
        }
        "pg_policies" => {
            for t in cat.tables.values() {
                for p in &t.policies {
                    let roles = if p.roles.is_empty() { vec!["public".to_string()] } else { p.roles.clone() };
                    out.push(row![rel;
                        "schemaname" => &t.schema,
                        "tablename" => &t.name,
                        "policyname" => &p.name,
                        "permissive" => if p.permissive { "PERMISSIVE" } else { "RESTRICTIVE" },
                        "roles" => roles,
                        "cmd" => p.command.name(),
                        "qual" => p.using.as_deref().map_or(Value::Null, |e| Value::Text(policy_expr_text(e))),
                        "with_check" => p.check.as_deref().map_or(Value::Null, |e| Value::Text(policy_expr_text(e))),
                    ]);
                }
            }
        }
        "pg_settings" => {
            let mut names: Vec<String> = SETTINGS.iter().map(|s| s.to_string()).collect();
            names.extend(cx.session.settings.keys().filter(|k| !SETTINGS.contains(&k.as_str())).cloned());
            names.sort();
            for n in names {
                if let Ok(value) = crate::ddl::show_setting(cx.session, &n) {
                    out.push(row![rel;
                        "name" => &n,
                        "setting" => &value,
                        "category" => "Client Connection Defaults",
                        "short_desc" => "",
                        "context" => "user",
                        "vartype" => "string",
                        "source" => "default",
                        "boot_val" => &value,
                        "reset_val" => &value,
                        "pending_restart" => false,
                    ]);
                }
            }
        }
        "pg_sequence" => {
            for s in cat.sequences.values() {
                out.push(row![rel;
                    "seqrelid" => oid_of(s.id),
                    "seqtypid" => super::sequence_type(s).0,
                    "seqstart" => s.start,
                    "seqincrement" => s.increment,
                    "seqmax" => s.max,
                    "seqmin" => s.min,
                    "seqcache" => 1i64,
                    "seqcycle" => s.cycle,
                ]);
            }
        }
        "pg_sequences" => {
            for s in cat.sequences.values() {
                out.push(row![rel;
                    "schemaname" => &s.schema,
                    "sequencename" => &s.name,
                    "sequenceowner" => &env.user,
                    "data_type" => super::sequence_type(s).1,
                    "start_value" => s.start,
                    "min_value" => s.min,
                    "max_value" => s.max,
                    "increment_by" => s.increment,
                    "cycle" => s.cycle,
                    "cache_size" => 1i64,
                ]);
            }
        }
        "pg_tables" => {
            for t in cat.tables.values() {
                out.push(row![rel;
                    "schemaname" => &t.schema,
                    "tablename" => &t.name,
                    "tableowner" => &env.user,
                    "hasindexes" => !t.indexes.is_empty(),
                    "hasrules" => false,
                    "hastriggers" => has_fk_triggers(env, t),
                    "rowsecurity" => false,
                ]);
            }
        }
        "pg_views" => {
            for v in cat.views.values() {
                out.push(row![rel;
                    "schemaname" => &v.schema,
                    "viewname" => &v.name,
                    "viewowner" => &env.user,
                    "definition" => &v.sql,
                ]);
            }
        }
        "pg_indexes" => {
            for t in cat.tables.values() {
                for ix in &t.indexes {
                    out.push(row![rel;
                        "schemaname" => &t.schema,
                        "tablename" => &t.name,
                        "indexname" => &ix.name,
                        "indexdef" => env.index_def(t, ix, false),
                    ]);
                }
            }
        }
        "pg_am" => {
            for (oid, amname, handler, amtype) in [
                (AM_HEAP, "heap", "heap_tableam_handler", 't'),
                (AM_BTREE, "btree", "bthandler", 'i'),
                (AM_HASH, "hash", "hashhandler", 'i'),
            ] {
                out.push(row![rel; "oid" => oid, "amname" => amname, "amhandler" => handler, "amtype" => amtype]);
            }
        }
        "pg_opclass" => {
            for &(oid, name, family, intype, keytype) in OPCLASSES {
                out.push(row![rel;
                    "oid" => oid,
                    "opcmethod" => AM_BTREE,
                    "opcname" => name,
                    "opcnamespace" => NS_PG_CATALOG,
                    "opcowner" => OWNER_OID,
                    "opcfamily" => family,
                    "opcintype" => intype,
                    "opcdefault" => true,
                    "opckeytype" => keytype,
                ]);
            }
        }
        "pg_collation" => {
            for (oid, collname, provider, locale) in [
                (COLLATION_DEFAULT, "default", 'd', None),
                (COLLATION_C, "C", 'c', Some("C")),
                (951, "POSIX", 'c', Some("POSIX")),
            ] {
                out.push(row![rel;
                    "oid" => oid,
                    "collname" => collname,
                    "collnamespace" => NS_PG_CATALOG,
                    "collowner" => OWNER_OID,
                    "collprovider" => provider,
                    "collisdeterministic" => true,
                    "collencoding" => -1,
                    "collcollate" => locale,
                    "collctype" => locale,
                ]);
            }
        }
        "pg_extension" => out.push(row![rel;
            "oid" => 13608i64,
            "extname" => "plpgsql",
            "extowner" => OWNER_OID,
            "extnamespace" => NS_PG_CATALOG,
            "extrelocatable" => false,
            "extversion" => "1.0",
        ]),
        "pg_language" => {
            for (oid, lanname, pl) in
                [(12i64, "internal", false), (13, "c", false), (14, "sql", false), (13612, "plpgsql", true)]
            {
                out.push(row![rel;
                    "oid" => oid,
                    "lanname" => lanname,
                    "lanowner" => OWNER_OID,
                    "lanispl" => pl,
                    "lanpltrusted" => pl || lanname == "sql",
                    "lanplcallfoid" => 0i64,
                    "laninline" => 0i64,
                    "lanvalidator" => 0i64,
                ]);
            }
        }
        "pg_tablespace" => {
            for (oid, spcname) in [(1663i64, "pg_default"), (1664, "pg_global")] {
                out.push(row![rel; "oid" => oid, "spcname" => spcname, "spcowner" => OWNER_OID]);
            }
        }
        "pg_stat_activity" => {
            let backends = cx.shared.backends.list();
            if !backends.iter().any(|b| b.pid == cx.session.pid) {
                // A session of the database's own background work.
                out.push(row![rel;
                    "datid" => DATABASE_OID,
                    "datname" => &env.database,
                    "pid" => cx.session.pid,
                    "usesysid" => OWNER_OID,
                    "usename" => &env.user,
                    "application_name" => cx.session.settings.get("application_name").cloned().unwrap_or_default(),
                    "backend_start" => Value::TimestampTz(cx.fn_ctx.txn_time),
                    "xact_start" => Value::TimestampTz(cx.fn_ctx.txn_time),
                    "query_start" => Value::TimestampTz(cx.fn_ctx.stmt_time),
                    "state_change" => Value::TimestampTz(cx.fn_ctx.stmt_time),
                    "state" => "active",
                    "query" => "",
                    "backend_type" => "client backend",
                ]);
            }
            let ts = |t: Option<i64>| t.map_or(Value::Null, Value::TimestampTz);
            for b in backends {
                let a = b.activity.lock().clone();
                out.push(row![rel;
                    "datid" => DATABASE_OID,
                    "datname" => &env.database,
                    "pid" => b.pid,
                    "usesysid" => OWNER_OID,
                    "usename" => a.user,
                    "application_name" => a.application_name,
                    "backend_start" => Value::TimestampTz(b.backend_start),
                    "xact_start" => ts(a.xact_start),
                    "query_start" => ts(a.query_start),
                    "state_change" => Value::TimestampTz(a.state_change),
                    "state" => a.state,
                    "query" => a.query,
                    "backend_type" => "client backend",
                ]);
            }
        }
        "pg_stat_database" => {
            let n = cx.shared.backends.list().len().max(1) as i32;
            out.push(row![rel; "datid" => DATABASE_OID, "datname" => &env.database, "numbackends" => n])
        }
        "pg_stat_user_tables" | "pg_stat_all_tables" | "pg_statio_user_tables" => {
            for t in cat.tables.values() {
                let mut r = row![rel; "relid" => oid_of(t.id), "schemaname" => &t.schema, "relname" => &t.name];
                // Counters are not tracked; report zero rather than NULL.
                for (i, c) in rel.cols.iter().enumerate() {
                    if c.1 == Type::Int8 && r[i].is_null() {
                        r[i] = Value::Int8(0);
                    }
                }
                out.push(r);
            }
        }
        "pg_range" => {
            // The built-in range types and their element types.
            for (range, subtype, multirange) in [
                (3904i64, 23i64, 4451i64),
                (3906, 1700, 4532),
                (3908, 1114, 4533),
                (3910, 1184, 4534),
                (3912, 1082, 4535),
                (3926, 20, 4536),
            ] {
                out.push(row![rel;
                    "rngtypid" => range,
                    "rngsubtype" => subtype,
                    "rngmultitypid" => multirange,
                    "rngcollation" => 0i64,
                    "rngsubopc" => 0i64,
                    "rngcanonical" => "-",
                    "rngsubdiff" => "-",
                ]);
            }
        }
        // Everything else has no rows in squad: comments, triggers,
        // policies, inheritance, functions, statistics, ...
        _ => {}
    }
    out
}
