//! Helpers for working with the Postgres parse tree.

use pg_query::protobuf::{self as pb, a_const::Val, node::Node as N};
use squad::{sqlstate, Error, Result, Value};

pub(crate) fn unsupported(what: impl std::fmt::Display) -> Error {
    Error::new(sqlstate::FEATURE_NOT_SUPPORTED, format!("not supported on a cluster: {what}"))
}

/// One statement of a parsed string, with its source text.
pub(crate) struct Parsed {
    pub text: String,
    pub node: pb::Node,
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Parsed>> {
    let stmts = squad::bind::parse_sql(sql)?;
    let single = stmts.len() == 1;
    let mut out = Vec::with_capacity(stmts.len());
    for raw in stmts {
        let Some(node) = raw.stmt else { continue };
        let start = raw.stmt_location.max(0) as usize;
        let end = if raw.stmt_len > 0 { start + raw.stmt_len as usize } else { sql.len() };
        let text = if single { sql } else { sql.get(start..end.min(sql.len())).unwrap_or(sql) };
        out.push(Parsed { text: text.trim().trim_end_matches(';').to_string(), node: *node });
    }
    Ok(out)
}

pub(crate) fn deparse(stmt: &pb::Node) -> Result<String> {
    let tree = pb::ParseResult {
        version: 0,
        stmts: vec![pb::RawStmt { stmt: Some(Box::new(stmt.clone())), stmt_location: 0, stmt_len: 0 }],
    };
    pg_query::deparse(&tree, Default::default()).map_err(|e| Error::internal(format!("cannot render statement: {e}")))
}

pub(crate) fn select_node(sel: pb::SelectStmt) -> pb::Node {
    pb::Node { node: Some(N::SelectStmt(Box::new(sel))) }
}

/// SQL text of an expression.
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
    let sql = deparse(&select_node(select))?;
    Ok(sql.strip_prefix("SELECT ").unwrap_or(&sql).to_string())
}

/// Parses `SELECT <text>` and returns the single select statement.
pub(crate) fn parse_select(sql: &str) -> Result<pb::SelectStmt> {
    let mut stmts = parse(sql)?;
    match stmts.pop().and_then(|p| p.node.node) {
        Some(N::SelectStmt(s)) => Ok(*s),
        _ => Err(Error::internal("generated SQL is not a SELECT")),
    }
}

/// Parses an expression from SQL text.
pub(crate) fn parse_expr(sql: &str) -> Result<pb::Node> {
    let mut sel = parse_select(&format!("SELECT {sql}"))?;
    match sel.target_list.pop().and_then(|t| t.node) {
        Some(N::ResTarget(t)) => t.val.map(|v| *v).ok_or_else(|| Error::internal("empty generated expression")),
        _ => Err(Error::internal("malformed generated expression")),
    }
}

pub(crate) fn res_target(name: &str, val: pb::Node) -> pb::Node {
    pb::Node {
        node: Some(N::ResTarget(Box::new(pb::ResTarget {
            name: name.to_string(),
            val: Some(Box::new(val)),
            ..Default::default()
        }))),
    }
}

pub(crate) fn string_of(n: &pb::Node) -> Option<&str> {
    match n.node.as_ref()? {
        N::String(s) => Some(&s.sval),
        _ => None,
    }
}

pub(crate) fn func_name(f: &pb::FuncCall) -> String {
    f.funcname.last().and_then(string_of).unwrap_or_default().to_ascii_lowercase()
}

/// A constant or parameter in a routing position.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Operand {
    Const(Value),
    Param(usize),
}

impl Operand {
    pub(crate) fn resolve<'a>(&'a self, params: &'a [Value]) -> Result<&'a Value> {
        match self {
            Operand::Const(v) => Ok(v),
            Operand::Param(i) => params
                .get(*i)
                .ok_or_else(|| Error::new(sqlstate::UNDEFINED_PARAMETER, format!("there is no parameter ${}", i + 1))),
        }
    }
}

/// The node as a constant or parameter, looking through casts.
pub(crate) fn operand(n: &pb::Node) -> Option<Operand> {
    match n.node.as_ref()? {
        N::AConst(c) if !c.isnull => Some(Operand::Const(match c.val.as_ref()? {
            Val::Ival(i) => Value::Int8(i.ival as i64),
            Val::Fval(f) => match f.fval.parse::<i64>() {
                Ok(i) => Value::Int8(i),
                Err(_) => Value::Text(f.fval.clone()),
            },
            Val::Sval(s) => Value::Text(s.sval.clone()),
            Val::Boolval(b) => Value::Bool(b.boolval),
            Val::Bsval(s) => Value::Text(s.bsval.clone()),
        })),
        N::ParamRef(p) if p.number >= 1 => Some(Operand::Param(p.number as usize - 1)),
        N::TypeCast(c) => operand(c.arg.as_ref()?),
        N::AExpr(e) if e.lexpr.is_none() && e.name.len() == 1 && string_of(&e.name[0]) == Some("-") => {
            match operand(e.rexpr.as_ref()?)? {
                Operand::Const(Value::Int8(i)) => Some(Operand::Const(Value::Int8(-i))),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Top-level AND conjuncts of a condition.
pub(crate) fn conjuncts(n: &pb::Node, out: &mut Vec<pb::Node>) {
    match n.node.as_ref() {
        Some(N::BoolExpr(b)) if b.boolop == pb::BoolExprType::AndExpr as i32 => {
            b.args.iter().for_each(|a| conjuncts(a, out))
        }
        _ => out.push(n.clone()),
    }
}

/// Visits an expression tree. `f` is called on every node before its
/// children; returning `true` means the node was handled (or replaced)
/// and its children are skipped. Nested SELECTs must be handled by `f`.
pub(crate) fn walk(node: &mut pb::Node, f: &mut dyn FnMut(&mut pb::Node) -> Result<bool>) -> Result<()> {
    if f(node)? {
        return Ok(());
    }
    macro_rules! opt {
        ($e:expr) => {
            if let Some(c) = $e.as_mut() {
                walk(c, f)?;
            }
        };
    }
    macro_rules! list {
        ($e:expr) => {
            for c in $e.iter_mut() {
                walk(c, f)?;
            }
        };
    }
    let Some(n) = node.node.as_mut() else { return Ok(()) };
    match n {
        N::AExpr(e) => {
            opt!(e.lexpr);
            opt!(e.rexpr);
        }
        N::BoolExpr(e) => list!(e.args),
        N::FuncCall(e) => {
            list!(e.args);
            list!(e.agg_order);
            opt!(e.agg_filter);
            if let Some(w) = e.over.as_mut() {
                list!(w.partition_clause);
                list!(w.order_clause);
                opt!(w.start_offset);
                opt!(w.end_offset);
            }
        }
        N::TypeCast(e) => opt!(e.arg),
        N::CaseExpr(e) => {
            opt!(e.arg);
            list!(e.args);
            opt!(e.defresult);
        }
        N::CaseWhen(e) => {
            opt!(e.expr);
            opt!(e.result);
        }
        N::CoalesceExpr(e) => list!(e.args),
        N::MinMaxExpr(e) => list!(e.args),
        N::NullTest(e) => opt!(e.arg),
        N::BooleanTest(e) => opt!(e.arg),
        N::AArrayExpr(e) => list!(e.elements),
        N::RowExpr(e) => list!(e.args),
        N::SubLink(e) => {
            opt!(e.testexpr);
            opt!(e.subselect);
        }
        N::AIndirection(e) => {
            opt!(e.arg);
            list!(e.indirection);
        }
        N::AIndices(e) => {
            opt!(e.lidx);
            opt!(e.uidx);
        }
        N::CollateClause(e) => opt!(e.arg),
        N::NamedArgExpr(e) => opt!(e.arg),
        N::List(e) => list!(e.items),
        N::ResTarget(e) => {
            list!(e.indirection);
            opt!(e.val);
        }
        N::SortBy(e) => opt!(e.node),
        N::WindowDef(e) => {
            list!(e.partition_clause);
            list!(e.order_clause);
        }
        N::GroupingSet(e) => list!(e.content),
        N::ColumnRef(_)
        | N::AConst(_)
        | N::ParamRef(_)
        | N::SqlvalueFunction(_)
        | N::String(_)
        | N::Integer(_)
        | N::Float(_)
        | N::Boolean(_)
        | N::AStar(_)
        | N::SetToDefault(_)
        | N::TypeName(_) => {}
        N::SelectStmt(_) => return Err(unsupported("a subquery in this position")),
        other => {
            let dbg = format!("{other:?}");
            return Err(unsupported(format!("expression {}", dbg.split(['(', ' ', '{']).next().unwrap_or("node"))));
        }
    }
    Ok(())
}

/// Functions that aggregate.
pub(crate) fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "bool_or"
            | "every"
            | "bit_and"
            | "bit_or"
            | "string_agg"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "json_object_agg"
            | "jsonb_object_agg"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "approx_count_distinct"
            | "approx_distinct"
            | "approx_percentile"
    )
}
