//! System catalogs (`pg_catalog`, `information_schema`) and the catalog
//! helper functions that tools expect.
//!
//! Nothing here is stored: rows are generated on demand from the
//! transaction's catalog snapshot and the session state. Column lists are
//! taken from PostgreSQL 16 (`relations.txt`), as are the rows of
//! `pg_type` for the built-in types (`pg_type.txt`).
//!
//! Because the engine has no `oid` or `name` types, OID columns are
//! `bigint` and name columns are `text`; `oidvector` / `int2vector`
//! columns are ordinary arrays.

mod functions;
mod info_schema;
mod pg_catalog;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use pg_query::protobuf::node::Node as N;

use crate::bind::{parse_sql, Binder};
use crate::catalog::{Catalog, Check, EnumType, FkAction, ForeignKey, Index, Table, View, FIRST_USER_OID};
use crate::exec::ExecCx;
use crate::expr::{Expr, Row};
use crate::types::{Type, Value};
use crate::{Error, Result};

pub(crate) use functions::{catalog_fn, catalog_fn_type};

const PG_CATALOG: &str = "pg_catalog";
const INFORMATION_SCHEMA: &str = "information_schema";

/// OID of the single role every session runs as.
const OWNER_OID: i64 = 10;
/// OID of the single database.
const DATABASE_OID: i64 = 5;
const NS_PG_CATALOG: i64 = 11;
const NS_PUBLIC: i64 = 2200;
const NS_INFORMATION_SCHEMA: i64 = 13256;
/// User schemas other than `public` get OIDs from here on.
const NS_USER_BASE: i64 = 15000;
/// Synthetic OID ranges for objects the catalog does not number itself.
const CONSTRAINT_BASE: i64 = 1_000_000_000;
const ATTRDEF_BASE: i64 = 1_500_000_000;
const ENUM_LABEL_BASE: i64 = 1_800_000_000;
const ENUM_ARRAY_BASE: i64 = 2_000_000_000;
/// System relations are numbered from here in the order of `relations.txt`.
const SYSTEM_REL_BASE: i64 = 12000;

/// A system relation: its qualified name and columns.
pub(crate) struct Rel {
    name: &'static str,
    cols: Vec<(&'static str, Type)>,
}

impl Rel {
    fn pos(&self, col: &str) -> usize {
        self.cols
            .iter()
            .position(|c| c.0 == col)
            .unwrap_or_else(|| panic!("system relation {} has no column {col}", self.name))
    }

    /// Converts a value to the column's declared integer width, so that
    /// generators can use whatever integer type is convenient.
    fn fit(&self, i: usize, v: Value) -> Value {
        match (&self.cols[i].1, v) {
            (Type::Int8, Value::Int4(x)) => Value::Int8(x as i64),
            (Type::Int8, Value::Int2(x)) => Value::Int8(x as i64),
            (Type::Int4, Value::Int8(x)) => Value::Int4(x as i32),
            (Type::Int4, Value::Int2(x)) => Value::Int4(x as i32),
            (Type::Int2, Value::Int8(x)) => Value::Int2(x as i16),
            (Type::Int2, Value::Int4(x)) => Value::Int2(x as i16),
            (Type::Float4, Value::Float8(x)) => Value::Float4(x as f32),
            (Type::Float8, Value::Float4(x)) => Value::Float8(x as f64),
            (_, v) => v,
        }
    }
}

/// Builds a row of a system relation from `column => value` pairs; columns
/// not mentioned are NULL.
macro_rules! row {
    ($rel:expr; $($k:literal => $v:expr),* $(,)?) => {{
        let rel: &$crate::system::Rel = $rel;
        let mut r = vec![$crate::types::Value::Null; rel.cols.len()];
        $(
            let i = rel.pos($k);
            r[i] = rel.fit(i, $crate::system::V::v($v));
        )*
        r
    }};
}
pub(crate) use row;

/// Conversion of Rust values into SQL values for [`row!`].
pub(crate) trait V {
    fn v(self) -> Value;
}

impl V for Value {
    fn v(self) -> Value {
        self
    }
}
impl V for i64 {
    fn v(self) -> Value {
        Value::Int8(self)
    }
}
impl V for i32 {
    fn v(self) -> Value {
        Value::Int4(self)
    }
}
impl V for i16 {
    fn v(self) -> Value {
        Value::Int2(self)
    }
}
impl V for bool {
    fn v(self) -> Value {
        Value::Bool(self)
    }
}
impl V for f64 {
    fn v(self) -> Value {
        Value::Float8(self)
    }
}
impl V for &str {
    fn v(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl V for &String {
    fn v(self) -> Value {
        Value::Text(self.clone())
    }
}
impl V for String {
    fn v(self) -> Value {
        Value::Text(self)
    }
}
impl V for char {
    fn v(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl<T: V> V for Option<T> {
    fn v(self) -> Value {
        self.map_or(Value::Null, V::v)
    }
}
impl V for Vec<i16> {
    fn v(self) -> Value {
        Value::Array(self.into_iter().map(Value::Int2).collect())
    }
}
impl V for Vec<i64> {
    fn v(self) -> Value {
        Value::Array(self.into_iter().map(Value::Int8).collect())
    }
}
impl V for Vec<String> {
    fn v(self) -> Value {
        Value::Array(self.into_iter().map(Value::Text).collect())
    }
}

fn spec_type(name: &str) -> Type {
    match name {
        "oid" | "xid" | "int8" | "regclass" => Type::Int8,
        "int4" => Type::Int4,
        "int2" => Type::Int2,
        "bool" => Type::Bool,
        "float4" => Type::Float4,
        "float8" => Type::Float8,
        "timestamptz" => Type::TimestampTz,
        "interval" => Type::Interval,
        "bytea" => Type::Bytea,
        "_oid" | "oidvector" => Type::Array(Box::new(Type::Int8)),
        "_int2" | "int2vector" => Type::Array(Box::new(Type::Int2)),
        "_float4" => Type::Array(Box::new(Type::Float4)),
        "_text" | "_aclitem" | "_name" | "_char" | "_regtype" | "anyarray" | "_pg_statistic" => {
            Type::Array(Box::new(Type::Text))
        }
        // name, text, varchar, "char", regproc, regtype, pg_node_tree, ...
        _ => Type::Text,
    }
}

fn relations() -> &'static Vec<Rel> {
    static RELS: OnceLock<Vec<Rel>> = OnceLock::new();
    RELS.get_or_init(|| {
        include_str!("relations.txt")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let (name, cols) = line.split_once('|').expect("malformed relations.txt");
                let cols = cols
                    .split(' ')
                    .map(|c| {
                        let (n, t) = c.split_once(':').expect("malformed column in relations.txt");
                        (n, spec_type(t))
                    })
                    .collect();
                Rel { name, cols }
            })
            .collect()
    })
}

fn rel_index() -> &'static HashMap<&'static str, usize> {
    static INDEX: OnceLock<HashMap<&'static str, usize>> = OnceLock::new();
    INDEX.get_or_init(|| relations().iter().enumerate().map(|(i, r)| (r.name, i)).collect())
}

fn find_rel(schema: Option<&str>, name: &str) -> Option<(usize, &'static Rel)> {
    let schema = match schema {
        Some(s @ (PG_CATALOG | INFORMATION_SCHEMA)) => s,
        Some(_) => return None,
        // pg_catalog is always searched first; only its `pg_` names are
        // claimed so that user tables keep every other name.
        None if name.starts_with("pg_") => PG_CATALOG,
        None => return None,
    };
    let i = *rel_index().get(format!("{schema}.{name}").as_str())?;
    Some((i, &relations()[i]))
}

/// Columns of a system relation, if `schema.name` is one.
pub(crate) fn table_columns(schema: Option<&str>, name: &str) -> Option<Vec<(&'static str, Type)>> {
    find_rel(schema, name).map(|(_, r)| r.cols.clone())
}

/// Rows of a system relation.
pub(crate) fn scan(name: &str, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let (schema, rel_name) = name.split_once('.').unwrap_or((PG_CATALOG, name));
    let (_, rel) =
        find_rel(Some(schema), rel_name).ok_or_else(|| Error::internal(format!("unknown system relation {name}")))?;
    let env = Env::new(cx);
    Ok(match schema {
        INFORMATION_SCHEMA => info_schema::scan(rel_name, rel, &env),
        _ => pg_catalog::scan(rel_name, rel, &env, cx),
    })
}

/// A built-in type as PostgreSQL's `pg_type` describes it.
pub(crate) struct PgType {
    oid: i64,
    name: &'static str,
    len: i16,
    byval: bool,
    typtype: char,
    category: char,
    preferred: bool,
    delim: char,
    elem: i64,
    array: i64,
    input: &'static str,
    output: &'static str,
    receive: &'static str,
    send: &'static str,
    align: char,
    storage: char,
    collation: i64,
}

fn builtin_types() -> &'static Vec<PgType> {
    static TYPES: OnceLock<Vec<PgType>> = OnceLock::new();
    TYPES.get_or_init(|| {
        include_str!("pg_type.txt")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let f: Vec<&'static str> = line.split('|').collect();
                let ch = |s: &str| s.chars().next().unwrap_or(' ');
                PgType {
                    oid: f[0].parse().expect("oid"),
                    name: f[1],
                    len: f[2].parse().expect("typlen"),
                    byval: f[3] == "t",
                    typtype: ch(f[4]),
                    category: ch(f[5]),
                    preferred: f[6] == "t",
                    delim: ch(f[7]),
                    elem: f[8].parse().expect("typelem"),
                    array: f[9].parse().expect("typarray"),
                    input: f[10],
                    output: f[11],
                    receive: f[12],
                    send: f[13],
                    align: ch(f[14]),
                    storage: ch(f[15]),
                    collation: f[16].parse().expect("typcollation"),
                }
            })
            .collect()
    })
}

fn builtin_type(oid: i64) -> Option<&'static PgType> {
    builtin_types().iter().find(|t| t.oid == oid)
}

/// One column of a table, view or index as `pg_attribute` shows it.
pub(crate) struct Att {
    name: String,
    num: i16,
    typid: i64,
    typmod: i32,
    not_null: bool,
    default: Option<String>,
    identity_always: bool,
    ty: Type,
}

/// A constraint of a table, in `pg_constraint` order.
pub(crate) struct Con<'a> {
    oid: i64,
    name: &'a str,
    /// `p`, `u`, `c` or `f`.
    kind: char,
    /// Positions of the constrained columns in the table.
    cols: Vec<usize>,
    index: Option<&'a Index>,
    check: Option<&'a Check>,
    fk: Option<&'a ForeignKey>,
}

/// What generators and functions need to know about the session.
pub(crate) struct Env {
    cat: Arc<Catalog>,
    path: Vec<String>,
    user: String,
    database: String,
}

impl Env {
    fn new(cx: &ExecCx) -> Env {
        Env {
            cat: cx.catalog.clone(),
            path: cx.session.search_path.clone(),
            user: cx.session.user.clone(),
            database: cx.shared.database_name.clone(),
        }
    }

    /// `(oid, name)` of every schema, system schemas first.
    fn namespaces(&self) -> Vec<(i64, &str)> {
        let mut out = vec![(NS_PG_CATALOG, PG_CATALOG), (NS_INFORMATION_SCHEMA, INFORMATION_SCHEMA)];
        out.extend(self.cat.schemas.iter().map(|s| (self.ns_oid(s), s.as_str())));
        out
    }

    fn ns_oid(&self, schema: &str) -> i64 {
        match schema {
            PG_CATALOG => NS_PG_CATALOG,
            INFORMATION_SCHEMA => NS_INFORMATION_SCHEMA,
            "public" => NS_PUBLIC,
            other => NS_USER_BASE + self.cat.schemas.iter().position(|s| s == other).unwrap_or(0) as i64,
        }
    }

    fn ns_name(&self, oid: i64) -> Option<&str> {
        self.namespaces().into_iter().find(|n| n.0 == oid).map(|n| n.1)
    }

    fn visible(&self, schema: &str) -> bool {
        schema == PG_CATALOG || self.path.iter().any(|s| s == schema)
    }

    /// `name`, qualified with its schema unless the schema is on the
    /// search path.
    fn display_name(&self, schema: &str, name: &str) -> String {
        if self.visible(schema) {
            quote_ident(name)
        } else {
            format!("{}.{}", quote_ident(schema), quote_ident(name))
        }
    }

    fn table_by_oid(&self, oid: i64) -> Option<&Arc<Table>> {
        self.cat.tables.values().find(|t| oid_of(t.id) == oid)
    }

    fn index_by_oid(&self, oid: i64) -> Option<(&Arc<Table>, &Index)> {
        self.cat.tables.values().find_map(|t| t.indexes.iter().find(|ix| oid_of(ix.id) == oid).map(|ix| (t, ix)))
    }

    fn view_by_oid(&self, oid: i64) -> Option<&Arc<View>> {
        self.cat.views.values().find(|v| oid_of(v.id) == oid)
    }

    fn enum_by_oid(&self, oid: i64) -> Option<&Arc<EnumType>> {
        self.cat.enums.values().find(|e| oid_of(e.id) == oid)
    }

    /// `schema`, `name` of the relation with this OID, of any kind.
    fn relation_name(&self, oid: i64) -> Option<(String, String)> {
        if let Some(t) = self.table_by_oid(oid) {
            return Some((t.schema.clone(), t.name.clone()));
        }
        if let Some((t, ix)) = self.index_by_oid(oid) {
            return Some((t.schema.clone(), ix.name.clone()));
        }
        if let Some(v) = self.view_by_oid(oid) {
            return Some((v.schema.clone(), v.name.clone()));
        }
        if let Some(s) = self.cat.sequences.values().find(|s| oid_of(s.id) == oid) {
            return Some((s.schema.clone(), s.name.clone()));
        }
        let i = usize::try_from(oid - SYSTEM_REL_BASE).ok()?;
        let (schema, name) = relations().get(i)?.name.split_once('.')?;
        Some((schema.to_string(), name.to_string()))
    }

    /// Columns of a table as attributes; hidden columns are left out and
    /// the remaining ones numbered from 1.
    fn table_atts(&self, table: &Table) -> Vec<Att> {
        table
            .visible_columns()
            .enumerate()
            .map(|(n, (_, c))| Att {
                name: c.name.clone(),
                num: n as i16 + 1,
                typid: c.decl_oid as i64,
                typmod: c.typmod,
                not_null: c.not_null,
                // An identity column has no default in Postgres; its
                // sequence is implied.
                default: c
                    .default
                    .as_ref()
                    .filter(|_| !c.identity_always)
                    .map(|d| self.display_default(d, c.decl_oid as i64)),
                identity_always: c.identity_always,
                ty: c.ty.clone(),
            })
            .collect()
    }

    /// A default as Postgres prints it: a bare string literal carries the
    /// column type, e.g. `'anon'::text`.
    fn display_default(&self, sql: &str, typid: i64) -> String {
        let inner = sql.strip_prefix('\'').and_then(|s| s.strip_suffix('\''));
        match inner {
            Some(body) if !body.replace("''", "").contains('\'') => format!("{sql}::{}", self.format_type(typid, -1)),
            _ => sql.to_string(),
        }
    }

    /// Output columns of a view, found by binding its definition.
    fn view_atts(&self, view: &View) -> Vec<Att> {
        let bound = (|| -> Result<Vec<crate::bind::OutputColumn>> {
            let stmts = parse_sql(&view.sql)?;
            let Some(N::SelectStmt(sel)) = stmts.first().and_then(|s| s.stmt.as_ref()).and_then(|s| s.node.as_ref())
            else {
                return Ok(Vec::new());
            };
            Ok(Binder::new(&self.cat, &self.path, Vec::new()).bind_query(sel)?.1)
        })();
        bound
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(i, c)| Att {
                name: view.columns.get(i).cloned().unwrap_or(c.name),
                num: i as i16 + 1,
                typid: self.type_oid(&c.ty),
                typmod: -1,
                not_null: false,
                default: None,
                identity_always: false,
                ty: c.ty,
            })
            .collect()
    }

    fn type_oid(&self, ty: &Type) -> i64 {
        match ty {
            Type::Enum(name) => self.cat.enums.values().find(|e| e.name == *name).map_or(25, |e| oid_of(e.id)),
            Type::Array(elem) => match &**elem {
                Type::Enum(name) => {
                    self.cat.enums.values().find(|e| e.name == *name).map_or(1009, |e| ENUM_ARRAY_BASE + e.id as i64)
                }
                _ => ty.oid() as i64,
            },
            other => other.oid() as i64,
        }
    }

    /// Attribute number (1-based, hidden columns skipped) of a table column.
    fn attnum(&self, table: &Table, col: usize) -> i16 {
        table.columns[..col].iter().filter(|c| !c.hidden).count() as i16 + 1
    }

    fn attnums(&self, table: &Table, cols: &[usize]) -> Vec<i16> {
        cols.iter().map(|&c| self.attnum(table, c)).collect()
    }

    fn constraints<'a>(&self, table: &'a Table) -> Vec<Con<'a>> {
        let base = CONSTRAINT_BASE + table.id as i64 * 1000;
        let mut out = Vec::new();
        for ix in table.indexes.iter().filter(|ix| ix.constraint) {
            out.push(Con {
                oid: 0,
                name: &ix.name,
                kind: if ix.primary { 'p' } else { 'u' },
                cols: ix.columns.clone(),
                index: Some(ix),
                check: None,
                fk: None,
            });
        }
        for c in &table.checks {
            out.push(Con {
                oid: 0,
                name: &c.name,
                kind: 'c',
                cols: self.check_columns(table, &c.expr),
                index: None,
                check: Some(c),
                fk: None,
            });
        }
        for fk in &table.foreign_keys {
            out.push(Con {
                oid: 0,
                name: &fk.name,
                kind: 'f',
                cols: fk.columns.clone(),
                index: None,
                check: None,
                fk: Some(fk),
            });
        }
        for (i, c) in out.iter_mut().enumerate() {
            c.oid = base + i as i64;
        }
        out
    }

    /// Table columns a CHECK expression refers to.
    fn check_columns(&self, table: &Table, sql: &str) -> Vec<usize> {
        let mut cols = Vec::new();
        let table = Arc::new(table.clone());
        if let Ok(expr) = Binder::new(&self.cat, &self.path, Vec::new()).bind_check(&table, sql) {
            expr.walk(&mut |e| {
                if let Expr::Column(i) = e {
                    if !cols.contains(i) {
                        cols.push(*i);
                    }
                }
            });
        }
        cols.sort_unstable();
        cols
    }

    fn constraint_by_oid(&self, oid: i64) -> Option<(&Arc<Table>, Con<'_>)> {
        let rel = oid.checked_sub(CONSTRAINT_BASE)?;
        let table = self.cat.tables.values().find(|t| t.id as i64 == rel / 1000)?;
        let con = self.constraints(table).into_iter().find(|c| c.oid == oid)?;
        Some((table, con))
    }

    /// Text of `pg_get_constraintdef`.
    fn constraint_def(&self, table: &Table, con: &Con, pretty: bool) -> String {
        let names = |t: &Table, cols: &[usize]| {
            cols.iter().map(|&c| quote_ident(&t.columns[c].name)).collect::<Vec<_>>().join(", ")
        };
        match con.kind {
            'p' => format!("PRIMARY KEY ({})", names(table, &con.cols)),
            'u' => {
                let nnd = if con.index.is_some_and(|ix| ix.nulls_not_distinct) { " NULLS NOT DISTINCT" } else { "" };
                format!("UNIQUE{nnd} ({})", names(table, &con.cols))
            }
            'c' => {
                let expr = con.check.map_or("", |c| c.expr.as_str());
                if pretty {
                    format!("CHECK ({expr})")
                } else {
                    format!("CHECK (({expr}))")
                }
            }
            _ => {
                let Some(fk) = con.fk else { return String::new() };
                let action = |a: FkAction| match a {
                    FkAction::NoAction => None,
                    FkAction::Restrict => Some("RESTRICT"),
                    FkAction::Cascade => Some("CASCADE"),
                    FkAction::SetNull => Some("SET NULL"),
                    FkAction::SetDefault => Some("SET DEFAULT"),
                };
                let mut def = format!("FOREIGN KEY ({}) REFERENCES ", names(table, &fk.columns));
                match self.cat.table_by_id(fk.ref_table) {
                    Some(parent) => {
                        def.push_str(&self.display_name(&parent.schema, &parent.name));
                        def.push_str(&format!("({})", names(parent, &fk.ref_columns)));
                    }
                    None => def.push('?'),
                }
                if let Some(a) = action(fk.on_update) {
                    def.push_str(&format!(" ON UPDATE {a}"));
                }
                if let Some(a) = action(fk.on_delete) {
                    def.push_str(&format!(" ON DELETE {a}"));
                }
                def
            }
        }
    }

    /// Text of `pg_get_indexdef`. The pretty form leaves out the schema
    /// of a table that is on the search path.
    fn index_def(&self, table: &Table, ix: &Index, pretty: bool) -> String {
        let cols = ix.columns.iter().map(|&c| quote_ident(&table.columns[c].name)).collect::<Vec<_>>().join(", ");
        let on = if pretty {
            self.display_name(&table.schema, &table.name)
        } else {
            format!("{}.{}", quote_ident(&table.schema), quote_ident(&table.name))
        };
        format!(
            "CREATE {}INDEX {} ON {on} USING btree ({cols}){}",
            if ix.unique { "UNIQUE " } else { "" },
            quote_ident(&ix.name),
            if ix.nulls_not_distinct { " NULLS NOT DISTINCT" } else { "" },
        )
    }

    /// The unique index a foreign key points at.
    fn referenced_index<'a>(&self, parent: &'a Table, fk: &ForeignKey) -> Option<&'a Index> {
        parent
            .indexes
            .iter()
            .find(|ix| ix.primary && ix.columns == fk.ref_columns)
            .or_else(|| parent.indexes.iter().find(|ix| ix.unique && ix.columns == fk.ref_columns))
    }

    /// Text of `format_type(oid, typmod)`.
    fn format_type(&self, oid: i64, typmod: i32) -> String {
        if let Some(e) = self.enum_by_oid(oid) {
            return self.display_name(&e.schema, &e.name);
        }
        if let Some(e) = self.cat.enums.values().find(|e| ENUM_ARRAY_BASE + e.id as i64 == oid) {
            return format!("{}[]", self.display_name(&e.schema, &e.name));
        }
        let Some(t) = builtin_type(oid) else { return "???".to_string() };
        if t.category == 'A' && t.elem != 0 && t.name.starts_with('_') {
            return format!("{}[]", self.format_type(t.elem, typmod));
        }
        let precision = |base: &str, suffix: &str| {
            if typmod >= 0 {
                format!("{base}({typmod}){suffix}")
            } else {
                format!("{base}{suffix}")
            }
        };
        match t.name {
            "bool" => "boolean".into(),
            "int2" => "smallint".into(),
            "int4" => "integer".into(),
            "int8" => "bigint".into(),
            "float4" => "real".into(),
            "float8" => "double precision".into(),
            "varchar" if typmod >= 4 => format!("character varying({})", typmod - 4),
            "varchar" => "character varying".into(),
            "bpchar" if typmod >= 4 => format!("character({})", typmod - 4),
            "numeric" if typmod >= 4 => format!("numeric({},{})", ((typmod - 4) >> 16) & 0xffff, (typmod - 4) & 0xffff),
            "char" => "\"char\"".into(),
            "timestamp" => precision("timestamp", " without time zone"),
            "timestamptz" => precision("timestamp", " with time zone"),
            "time" => precision("time", " without time zone"),
            "timetz" => precision("time", " with time zone"),
            "varbit" => "bit varying".into(),
            other => other.to_string(),
        }
    }
}

/// `(type OID, SQL name, bits)` of the integer type a sequence produces,
/// judged by its upper bound (serial columns bound their sequence).
pub(crate) fn sequence_type(seq: &crate::catalog::Sequence) -> (i64, &'static str, i32) {
    if seq.max == i16::MAX as i64 {
        (21, "smallint", 16)
    } else if seq.max == i32::MAX as i64 {
        (23, "integer", 32)
    } else {
        (20, "bigint", 64)
    }
}

pub(crate) fn oid_of(id: u32) -> i64 {
    (FIRST_USER_OID + id) as i64
}

/// Quotes an identifier where SQL requires it.
pub(crate) fn quote_ident(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !s.starts_with(|c: char| c.is_ascii_digit())
        && !is_reserved(s);
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn is_reserved(s: &str) -> bool {
    matches!(
        s,
        "all"
            | "analyse"
            | "analyze"
            | "and"
            | "any"
            | "array"
            | "as"
            | "asc"
            | "asymmetric"
            | "both"
            | "case"
            | "cast"
            | "check"
            | "collate"
            | "column"
            | "constraint"
            | "create"
            | "current_catalog"
            | "current_date"
            | "current_role"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "deferrable"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "from"
            | "grant"
            | "group"
            | "having"
            | "in"
            | "initially"
            | "intersect"
            | "into"
            | "lateral"
            | "leading"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "not"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "placing"
            | "primary"
            | "references"
            | "returning"
            | "select"
            | "session_user"
            | "some"
            | "symmetric"
            | "table"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "user"
            | "using"
            | "variadic"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_files_parse() {
        assert!(relations().len() > 100);
        assert!(builtin_types().len() > 300);
        assert_eq!(builtin_type(23).map(|t| t.name), Some("int4"));
        assert!(find_rel(None, "pg_class").is_some());
        assert!(find_rel(None, "tables").is_none());
        assert!(find_rel(Some("information_schema"), "tables").is_some());
        assert!(find_rel(Some("public"), "pg_class").is_none());
    }

    #[test]
    fn identifiers_are_quoted_when_needed() {
        assert_eq!(quote_ident("users"), "users");
        assert_eq!(quote_ident("User"), "\"User\"");
        assert_eq!(quote_ident("order"), "\"order\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }
}
