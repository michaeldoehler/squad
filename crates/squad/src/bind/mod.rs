//! Semantic analysis and planning: turns the Postgres parse tree into a
//! [`Plan`](crate::plan::Plan) with resolved names, types and access paths.

mod dml;
mod expr;
mod opt;
mod query;
mod rls;

use std::sync::Arc;

use pg_query::protobuf::{self as pb, node::Node as N};

use crate::catalog::{Catalog, Table};
use crate::expr::{Expr, FuncRef};
use crate::plan::{AggCall, WindowCall};
use crate::types::{Type, Value};
use crate::value;
use crate::{sqlstate, Error, Result};

pub use query::OutputColumn;
pub(crate) use rls::reads_target;
pub use rls::{RlsCheck, RowSecurityUser};

pub(crate) use expr::node_kind;

/// A column visible to expressions.
#[derive(Debug, Clone)]
pub struct ScopeCol {
    /// Table name or alias the column can be qualified with.
    pub table: Option<String>,
    pub name: String,
    pub ty: Type,
    /// Not expanded by `*` (hidden row id).
    pub hidden: bool,
    /// Duplicate of a `USING` / `NATURAL` join column: reachable only when
    /// qualified.
    pub merged: bool,
    /// Index into [`Scope::rels`].
    pub rel: usize,
}

/// A relation instance in FROM.
#[derive(Debug, Clone)]
pub struct ScopeRel {
    pub alias: String,
    pub start: usize,
    pub len: usize,
    pub table: Option<Arc<Table>>,
}

#[derive(Debug, Clone, Default)]
pub struct Scope {
    pub cols: Vec<ScopeCol>,
    pub rels: Vec<ScopeRel>,
}

impl Scope {
    pub fn types(&self) -> Vec<Type> {
        self.cols.iter().map(|c| c.ty.clone()).collect()
    }

    /// Scope of one relation with the given columns.
    pub fn relation(alias: &str, cols: Vec<(String, Type, bool)>, table: Option<Arc<Table>>) -> Scope {
        let len = cols.len();
        Scope {
            cols: cols
                .into_iter()
                .map(|(name, ty, hidden)| ScopeCol {
                    table: Some(alias.to_string()),
                    name,
                    ty,
                    hidden,
                    merged: false,
                    rel: 0,
                })
                .collect(),
            rels: vec![ScopeRel { alias: alias.to_string(), start: 0, len, table }],
        }
    }

    pub fn for_table(table: &Arc<Table>, alias: &str) -> Scope {
        Scope::relation(
            alias,
            table.columns.iter().map(|c| (c.name.clone(), c.ty.clone(), c.hidden)).collect(),
            Some(table.clone()),
        )
    }

    /// Concatenation: the columns of `other` follow those of `self`.
    pub fn join(mut self, other: Scope) -> Scope {
        let (col_off, rel_off) = (self.cols.len(), self.rels.len());
        self.cols.extend(other.cols.into_iter().map(|mut c| {
            c.rel += rel_off;
            c
        }));
        self.rels.extend(other.rels.into_iter().map(|mut r| {
            r.start += col_off;
            r
        }));
        self
    }

    /// Gives every column the same qualifier (`(a JOIN b) AS j`, subquery
    /// aliases).
    pub fn rename(mut self, alias: &str, col_names: &[String]) -> Scope {
        let mut names = col_names.iter();
        for c in &mut self.cols {
            c.table = Some(alias.to_string());
            if !c.hidden && !c.merged {
                if let Some(n) = names.next() {
                    c.name = n.clone();
                }
            }
        }
        let len = self.cols.len();
        let table = if self.rels.len() == 1 { self.rels[0].table.clone() } else { None };
        self.rels = vec![ScopeRel { alias: alias.to_string(), start: 0, len, table }];
        for c in &mut self.cols {
            c.rel = 0;
        }
        self
    }
}

/// A CTE visible by name while binding.
#[derive(Debug, Clone)]
struct CteBinding {
    name: String,
    id: usize,
    cols: Vec<(String, Type)>,
}

/// State of one query level that may contain aggregates / window calls.
#[derive(Default)]
struct AggLevel {
    aggs: Vec<AggCall>,
    windows: Vec<WindowCall>,
    /// Named windows from the WINDOW clause.
    window_defs: Vec<pb::WindowDef>,
    /// Set-returning functions in the select list.
    srfs: Vec<(crate::plan::Srf, Vec<Expr>)>,
    allow_aggs: bool,
    allow_windows: bool,
    allow_srfs: bool,
    /// Currently binding the arguments of an aggregate.
    in_aggregate: bool,
}

pub struct Binder<'a> {
    pub catalog: &'a Catalog,
    pub search_path: &'a [String],
    /// Types of `$n` parameters: given by the client or inferred from use.
    pub param_types: Vec<Option<Type>>,
    outer_scopes: Vec<Scope>,
    /// Parallel to `outer_scopes`: whether the query level bound inside it
    /// referenced that scope or anything further out.
    correlated: Vec<bool>,
    /// Levels of `outer_scopes` that are currently not visible: the FROM
    /// items to the left of a JOIN, which only LATERAL items inside the
    /// join may reference.
    hidden_levels: Vec<usize>,
    /// Nesting depth of the expression being bound within its query level.
    expr_depth: usize,
    /// Nesting depth of the query being bound (1 = the statement itself).
    select_depth: usize,
    /// Constant function calls whose evaluation failed while planning.
    const_errors: Vec<(Expr, Error)>,
    ctes: Vec<CteBinding>,
    agg_levels: Vec<AggLevel>,
    next_subquery_id: usize,
    next_cte_id: usize,
    /// CTE ids referenced so far (to tell recursive CTEs from plain ones).
    cte_refs: Vec<usize>,
    /// Clause being bound, for error messages.
    clause: &'static str,
    /// Views being expanded, to detect cycles.
    view_stack: Vec<u32>,
    /// Tables referenced by the statement, for plan cache invalidation and
    /// table locks.
    pub tables_used: Vec<u32>,
    /// The login the statement is bound for. `None` binds without
    /// row-level security (definitions, internal statements).
    pub rls: Option<RowSecurityUser>,
    /// Tables whose policy expressions are being bound, to detect cycles.
    rls_stack: Vec<u32>,
}

pub(crate) fn node(n: &pb::Node) -> Result<&N> {
    n.node.as_ref().ok_or_else(|| Error::internal("empty parse node"))
}

pub(crate) fn opt_node(n: &Option<Box<pb::Node>>) -> Option<&N> {
    n.as_ref().and_then(|n| n.node.as_ref())
}

/// The string inside a `String` node.
pub(crate) fn str_val(n: &pb::Node) -> Result<&str> {
    match node(n)? {
        N::String(s) => Ok(&s.sval),
        other => Err(Error::internal(format!("expected a name, found {other:?}"))),
    }
}

pub(crate) fn name_list(nodes: &[pb::Node]) -> Result<Vec<String>> {
    nodes.iter().map(|n| str_val(n).map(str::to_string)).collect()
}

/// Splits a possibly qualified name into `(schema, name)`, dropping a
/// leading catalog (database) name.
pub(crate) fn qualified(parts: &[String]) -> (Option<&str>, &str) {
    match parts {
        [name] => (None, name),
        [schema, name] => (Some(schema), name),
        [_, schema, name] => (Some(schema), name),
        _ => (None, ""),
    }
}

pub(crate) fn range_var_name(rv: &pb::RangeVar) -> (Option<&str>, &str) {
    (if rv.schemaname.is_empty() { None } else { Some(rv.schemaname.as_str()) }, rv.relname.as_str())
}

/// Renders an expression node back to SQL text.
pub(crate) fn deparse_expr(expr: &pb::Node) -> Result<String> {
    let target = pb::Node {
        node: Some(N::ResTarget(Box::new(pb::ResTarget { val: Some(Box::new(expr.clone())), ..Default::default() }))),
    };
    let select = pb::SelectStmt {
        target_list: vec![target],
        op: pb::SetOperation::SetopNone as i32,
        limit_option: pb::LimitOption::Default as i32,
        ..Default::default()
    };
    let sql = deparse_stmt(&pb::Node { node: Some(N::SelectStmt(Box::new(select))) })?;
    Ok(sql.strip_prefix("SELECT ").unwrap_or(&sql).to_string())
}

/// Renders a statement node back to SQL text.
pub(crate) fn deparse_stmt(stmt: &pb::Node) -> Result<String> {
    let tree = pb::ParseResult {
        version: 0,
        stmts: vec![pb::RawStmt { stmt: Some(Box::new(stmt.clone())), stmt_location: 0, stmt_len: 0 }],
    };
    pg_query::deparse(&tree, Default::default()).map_err(|e| Error::internal(format!("cannot deparse: {e}")))
}

/// Parses SQL text, mapping parser errors to SQLSTATE 42601.
pub fn parse_sql(sql: &str) -> Result<Vec<pb::RawStmt>> {
    match pg_query::parse(sql, 0) {
        Ok(r) => Ok(r.protobuf.stmts),
        Err(pg_query::Error::Parse(msg)) => Err(Error::syntax(msg)),
        Err(e) => Err(Error::syntax(e.to_string())),
    }
}

/// Parses a stored expression (`DEFAULT`, `CHECK`) back into a node.
pub(crate) fn parse_expr(sql: &str) -> Result<pb::Node> {
    let stmts = parse_sql(&format!("SELECT {sql}"))?;
    let stmt = stmts.into_iter().next().and_then(|s| s.stmt).ok_or_else(|| Error::internal("empty expression"))?;
    match stmt.node {
        Some(N::SelectStmt(mut s)) if s.target_list.len() == 1 => match s.target_list.remove(0).node {
            Some(N::ResTarget(t)) => t.val.map(|v| *v).ok_or_else(|| Error::internal("empty expression")),
            _ => Err(Error::internal("malformed stored expression")),
        },
        _ => Err(Error::internal("malformed stored expression")),
    }
}

/// A declared type: the logical type plus how it was spelled.
#[derive(Debug, Clone)]
pub struct DeclaredType {
    pub ty: Type,
    pub decl: String,
    pub oid: u32,
    pub typmod: i32,
    /// `serial` family: the column gets an owned sequence.
    pub serial: bool,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog, search_path: &'a [String], param_types: Vec<Option<Type>>) -> Binder<'a> {
        Binder {
            catalog,
            search_path,
            param_types,
            outer_scopes: Vec::new(),
            correlated: Vec::new(),
            hidden_levels: Vec::new(),
            expr_depth: 0,
            select_depth: 0,
            const_errors: Vec::new(),
            ctes: Vec::new(),
            agg_levels: Vec::new(),
            next_subquery_id: 0,
            next_cte_id: 0,
            cte_refs: Vec::new(),
            clause: "SELECT",
            view_stack: Vec::new(),
            tables_used: Vec::new(),
            rls: None,
            rls_stack: Vec::new(),
        }
    }

    /// Resolves a type name from the parse tree.
    pub fn resolve_type(&self, tn: &pb::TypeName) -> Result<DeclaredType> {
        let parts = name_list(&tn.names)?;
        let (schema, name) = qualified(&parts);
        let typmods: Vec<i32> = tn
            .typmods
            .iter()
            .filter_map(|n| match n.node.as_ref() {
                Some(N::AConst(c)) => match &c.val {
                    Some(pb::a_const::Val::Ival(i)) => Some(i.ival),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        let lower = name.to_ascii_lowercase();
        let builtin = match schema {
            // Unquoted CHAR arrives as pg_catalog.bpchar; the bare name
            // "char" is the single-byte catalog type.
            None | Some("pg_catalog") if lower == "char" => Some(Type::Char),
            None | Some("pg_catalog") => Type::from_name(&lower),
            _ => None,
        };
        let (ty, mut decl, mut oid, mut typmod) = match builtin {
            Some(ty) => {
                let (decl, oid) = match lower.as_str() {
                    "varchar" | "character varying" => ("character varying".to_string(), 1043),
                    "bpchar" | "character" => ("character".to_string(), 1042),
                    "name" => ("name".to_string(), 19),
                    "oid" | "regclass" | "regtype" | "regproc" => ("oid".to_string(), 26),
                    _ => (ty.name(), ty.oid()),
                };
                let mut typmod = -1;
                let mut decl = decl;
                match (&ty, typmods.as_slice()) {
                    (Type::Text, [n]) if oid == 1043 || oid == 1042 => {
                        if *n < 1 {
                            return Err(Error::new(
                                sqlstate::INVALID_PARAMETER_VALUE,
                                format!(
                                    "length for type {} must be at least 1",
                                    if oid == 1043 { "varchar" } else { "char" }
                                ),
                            ));
                        }
                        typmod = n + 4;
                        decl = format!("{decl}({n})");
                    }
                    (Type::Numeric, [p]) => {
                        typmod = (p << 16) + 4;
                        decl = format!("numeric({p},0)");
                    }
                    (Type::Numeric, [p, s]) => {
                        typmod = ((p << 16) | (s & 0xffff)) + 4;
                        decl = format!("numeric({p},{s})");
                    }
                    _ => {}
                }
                // `char` without a length means char(1).
                if oid == 1042 && typmod == -1 && lower != "bpchar" {
                    typmod = 5;
                    decl = "character(1)".into();
                }
                (ty, decl, oid, typmod)
            }
            None => match self.catalog.enum_type(schema, name, self.search_path) {
                Some(e) => (Type::Enum(e.name.clone()), e.name.clone(), crate::catalog::FIRST_USER_OID + e.id, -1),
                None => return Err(Error::new(sqlstate::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist"))),
            },
        };
        let serial =
            matches!(lower.as_str(), "serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2");
        let mut ty = ty;
        if !tn.array_bounds.is_empty() {
            ty = Type::Array(Box::new(ty));
            decl = format!("{decl}[]");
            oid = ty.oid();
            typmod = -1;
        }
        Ok(DeclaredType { ty, decl, oid, typmod, serial })
    }

    /// Wraps an expression so that its value conforms to a type modifier
    /// (`varchar(n)`, `numeric(p,s)`). `explicit` casts truncate strings
    /// silently; assignments raise 22001.
    pub(crate) fn apply_typmod(&self, expr: Expr, decl: &DeclaredType, explicit: bool) -> Result<Expr> {
        if decl.typmod < 0 {
            return Ok(expr);
        }
        let (eval, name): (value::FuncImpl, &str) = match (&decl.ty, decl.oid) {
            (Type::Text, 1043) if explicit => (typmod::varchar_truncate, "varchar"),
            (Type::Text, 1043) => (typmod::varchar_check, "varchar"),
            (Type::Text, 1042) if explicit => (typmod::bpchar_truncate, "bpchar"),
            (Type::Text, 1042) => (typmod::bpchar_check, "bpchar"),
            (Type::Numeric, _) => (typmod::numeric_typmod, "numeric"),
            _ => return Ok(expr),
        };
        // A constant is checked while planning, as Postgres does when it
        // folds constants: a too-long literal is an error even if the
        // statement ends up touching no rows.
        if let Expr::Const(v) = &expr {
            if v.is_null() {
                return Ok(expr);
            }
            let ctx = crate::types::FnCtx::default();
            return eval(&ctx, &[v.clone(), Value::Int4(decl.typmod)]).map(Expr::Const);
        }
        Ok(Expr::Call(
            FuncRef { name: name.into(), eval, strict: true, ret: decl.ty.clone() },
            vec![expr, Expr::Const(Value::Int4(decl.typmod))],
        ))
    }

    /// Converts `expr` of type `from` to `to` as an implicit coercion.
    /// String literals are parsed at plan time, untyped parameters adopt
    /// the target type.
    pub(crate) fn coerce(&mut self, expr: Expr, from: &Type, to: &Type) -> Result<Expr> {
        if from == to || matches!(to, Type::Unknown | Type::Record) {
            return Ok(expr);
        }
        match (&expr, from) {
            (Expr::Const(Value::Null), _) => return Ok(expr),
            (Expr::Const(Value::Text(s)), Type::Unknown) => return self.literal(s, to).map(Expr::Const),
            (Expr::Param(i), Type::Unknown) => {
                if self.param_types.len() <= *i {
                    self.param_types.resize(*i + 1, None);
                }
                if self.param_types[*i].is_none() {
                    self.param_types[*i] = Some(to.clone());
                    return Ok(expr);
                }
            }
            _ => {}
        }
        self.cast_expr(expr, from, to)
    }

    /// Builds the runtime conversion node, without checking permission.
    pub(crate) fn cast_expr(&self, expr: Expr, from: &Type, to: &Type) -> Result<Expr> {
        Ok(match to {
            Type::Enum(name) => match from {
                Type::Enum(other) if other == name => expr,
                _ => Expr::EnumCast(Box::new(expr), self.enum_by_name(name)?),
            },
            Type::Array(elem) if matches!(**elem, Type::Enum(_)) => {
                return Err(Error::unsupported("casting to an array of enums"))
            }
            _ => {
                // Casts among numbers, booleans and text do not depend on
                // session settings: fold them for constants.
                let plain = |t: &Type| t.is_numeric() || matches!(t, Type::Bool | Type::Text);
                if let (Expr::Const(v), true, true) = (&expr, plain(from), plain(to)) {
                    if let Ok(folded) = value::cast(v.clone(), to, &crate::types::FnCtx::default()) {
                        return Ok(Expr::Const(folded));
                    }
                }
                Expr::Cast(Box::new(expr), to.clone())
            }
        })
    }

    pub(crate) fn enum_by_name(&self, name: &str) -> Result<Arc<crate::catalog::EnumType>> {
        self.catalog
            .enums
            .values()
            .find(|e| e.name == name)
            .cloned()
            .ok_or_else(|| Error::new(sqlstate::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist")))
    }

    /// Parses a literal of the given type at plan time.
    pub(crate) fn literal(&self, text: &str, ty: &Type) -> Result<Value> {
        match ty {
            Type::Enum(name) => crate::expr::enum_value(&*self.enum_by_name(name)?, text),
            Type::Unknown => Ok(Value::Text(text.to_string())),
            _ => value::parse_value(text, ty, &crate::types::FnCtx::default()),
        }
    }

    pub(crate) fn alloc_subquery_id(&mut self) -> usize {
        self.next_subquery_id += 1;
        self.next_subquery_id - 1
    }

    /// Number of subquery cache slots the finished plan needs.
    pub fn subquery_count(&self) -> usize {
        self.next_subquery_id
    }
}

/// Runtime enforcement of type modifiers.
mod typmod {
    use rust_decimal::Decimal;

    use crate::types::{FnCtx, Value};
    use crate::{sqlstate, Error, Result};

    fn too_long(kind: &str, n: usize) -> Error {
        Error::new(sqlstate::STRING_DATA_RIGHT_TRUNCATION, format!("value too long for type {kind}({n})"))
    }

    fn parts(args: &[Value]) -> (&str, usize) {
        let n = match &args[1] {
            Value::Int4(t) => (*t - 4).max(0) as usize,
            _ => 0,
        };
        (args[0].as_str().unwrap_or_default(), n)
    }

    /// `char(n)` values are stored without their padding: trailing spaces
    /// are insignificant for the type, and every operation that converts
    /// to text drops them anyway.
    fn unpadded(s: &str) -> Value {
        Value::Text(s.trim_end_matches(' ').to_string())
    }

    fn check(args: &[Value], kind: &str, bpchar: bool) -> Result<Value> {
        let (s, n) = parts(args);
        let len = s.chars().count();
        if len <= n {
            return Ok(if bpchar { unpadded(s) } else { args[0].clone() });
        }
        // Excess characters may be dropped only if they are all spaces.
        let cut = s.char_indices().nth(n).map_or(s.len(), |(i, _)| i);
        if s[cut..].chars().all(|c| c == ' ') {
            Ok(if bpchar { unpadded(&s[..cut]) } else { Value::Text(s[..cut].to_string()) })
        } else {
            Err(too_long(kind, n))
        }
    }

    fn truncate(args: &[Value], bpchar: bool) -> Result<Value> {
        let (s, n) = parts(args);
        let cut = s.char_indices().nth(n).map_or(s.len(), |(i, _)| i);
        Ok(if bpchar { unpadded(&s[..cut]) } else { Value::Text(s[..cut].to_string()) })
    }

    pub(super) fn varchar_check(_: &FnCtx, args: &[Value]) -> Result<Value> {
        check(args, "character varying", false)
    }
    pub(super) fn bpchar_check(_: &FnCtx, args: &[Value]) -> Result<Value> {
        check(args, "character", true)
    }
    pub(super) fn varchar_truncate(_: &FnCtx, args: &[Value]) -> Result<Value> {
        truncate(args, false)
    }
    pub(super) fn bpchar_truncate(_: &FnCtx, args: &[Value]) -> Result<Value> {
        truncate(args, true)
    }

    pub(super) fn numeric_typmod(_: &FnCtx, args: &[Value]) -> Result<Value> {
        let (Value::Numeric(d), Value::Int4(typmod)) = (&args[0], &args[1]) else { return Ok(args[0].clone()) };
        let precision = ((typmod - 4) >> 16) & 0xffff;
        let scale = (typmod - 4) & 0xffff;
        let mut rounded = d.round_dp_with_strategy(scale as u32, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
        rounded.rescale(scale as u32);
        let limit = Decimal::from_i128_with_scale(10i128.pow((precision - scale).max(0) as u32), 0);
        if rounded.abs() >= limit {
            return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "numeric field overflow").with_detail(
                format!(
                "A field with precision {precision}, scale {scale} must round to an absolute value less than 10^{}.",
                precision - scale
            ),
            ));
        }
        Ok(Value::Numeric(rounded))
    }
}
