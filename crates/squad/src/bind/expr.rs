//! Binding of scalar expressions.

use pg_query::protobuf::{self as pb, a_const::Val, node::Node as N};

use super::{name_list, node, opt_node, Binder, Scope};
use crate::expr::{BoolTest, Expr, FuncRef, SessionFn, SubKind, SubqueryExpr};
use crate::plan::{AggCall, AggFunc, Frame, FrameBound, FrameUnit, Plan, SortKey, Srf, WindowCall, WindowFunc};
use crate::types::{Type, Value};
use crate::value::{self, CastContext};
use crate::{sqlstate, Error, Result};

pub(crate) type Typed = (Expr, Type);

/// Names and types of the columns a set-returning function produces.
pub(crate) type Columns = Vec<(String, Type)>;

fn agg_func(name: &str) -> Option<AggFunc> {
    Some(match name {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "bool_and" | "every" => AggFunc::BoolAnd,
        "bool_or" => AggFunc::BoolOr,
        "bit_and" => AggFunc::BitAnd,
        "bit_or" => AggFunc::BitOr,
        "string_agg" => AggFunc::StringAgg,
        "array_agg" => AggFunc::ArrayAgg,
        "json_agg" => AggFunc::JsonAgg { jsonb: false },
        "jsonb_agg" => AggFunc::JsonAgg { jsonb: true },
        "json_object_agg" => AggFunc::JsonObjectAgg { jsonb: false },
        "jsonb_object_agg" => AggFunc::JsonObjectAgg { jsonb: true },
        "variance" | "var_samp" => AggFunc::Variance { sample: true, stddev: false },
        "var_pop" => AggFunc::Variance { sample: false, stddev: false },
        "stddev" | "stddev_samp" => AggFunc::Variance { sample: true, stddev: true },
        "stddev_pop" => AggFunc::Variance { sample: false, stddev: true },
        // `approx_distinct` is the name Trino, Presto and Apache Superset use.
        "approx_count_distinct" | "approx_distinct" => AggFunc::ApproxCountDistinct,
        // The percentile is filled in from the second argument.
        "approx_percentile" => AggFunc::ApproxPercentile(0.5),
        _ => return None,
    })
}

fn window_func(name: &str) -> Option<WindowFunc> {
    Some(match name {
        "row_number" => WindowFunc::RowNumber,
        "rank" => WindowFunc::Rank,
        "dense_rank" => WindowFunc::DenseRank,
        "percent_rank" => WindowFunc::PercentRank,
        "cume_dist" => WindowFunc::CumeDist,
        "ntile" => WindowFunc::Ntile,
        "lag" => WindowFunc::Lag,
        "lead" => WindowFunc::Lead,
        "first_value" => WindowFunc::FirstValue,
        "last_value" => WindowFunc::LastValue,
        "nth_value" => WindowFunc::NthValue,
        _ => return None,
    })
}

pub(crate) fn srf(name: &str) -> Option<Srf> {
    Some(match name {
        "generate_series" => Srf::GenerateSeries,
        "unnest" => Srf::Unnest,
        // Without table inheritance a relation's only ancestor is itself.
        "pg_partition_ancestors" | "pg_inherits_ancestors" => Srf::Unnest,
        "json_array_elements" | "jsonb_array_elements" => Srf::JsonArrayElements { text: false },
        "json_array_elements_text" | "jsonb_array_elements_text" => Srf::JsonArrayElements { text: true },
        "json_each" | "jsonb_each" => Srf::JsonEach { text: false },
        "json_each_text" | "jsonb_each_text" => Srf::JsonEach { text: true },
        "json_object_keys" | "jsonb_object_keys" => Srf::JsonObjectKeys,
        "regexp_split_to_table" => Srf::RegexpSplitToTable,
        "string_to_table" => Srf::StringToTable,
        "generate_subscripts" => Srf::GenerateSubscripts,
        _ => return None,
    })
}

fn undefined_function(name: &str, args: &[Type]) -> Error {
    let list = args.iter().map(Type::name).collect::<Vec<_>>().join(", ");
    Error::new(sqlstate::UNDEFINED_FUNCTION, format!("function {name}({list}) does not exist"))
        .with_hint("No function matches the given name and argument types. You might need to add explicit type casts.")
}

/// Result type of an aggregate for the given argument types.
fn agg_type(func: &AggFunc, name: &str, args: &[Type]) -> Result<Type> {
    let arg = args.first().cloned().unwrap_or(Type::Unknown);
    Ok(match func {
        AggFunc::CountStar | AggFunc::Count => Type::Int8,
        AggFunc::Sum => match arg {
            Type::Int2 | Type::Int4 => Type::Int8,
            Type::Int8 | Type::Numeric => Type::Numeric,
            Type::Float4 => Type::Float4,
            Type::Float8 => Type::Float8,
            Type::Interval => Type::Interval,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::Avg => match arg {
            Type::Int2 | Type::Int4 | Type::Int8 | Type::Numeric => Type::Numeric,
            Type::Float4 | Type::Float8 => Type::Float8,
            Type::Interval => Type::Interval,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::Variance { .. } => match arg {
            Type::Int2 | Type::Int4 | Type::Int8 | Type::Numeric => Type::Numeric,
            Type::Float4 | Type::Float8 => Type::Float8,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::Min | AggFunc::Max => match arg {
            Type::Json | Type::Jsonb | Type::Record | Type::Bool | Type::Uuid | Type::Bytea => {
                return Err(undefined_function(name, args))
            }
            t => t,
        },
        AggFunc::BoolAnd | AggFunc::BoolOr => match arg {
            Type::Bool => Type::Bool,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::BitAnd | AggFunc::BitOr => match arg {
            Type::Int2 | Type::Int4 | Type::Int8 => arg,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::ApproxCountDistinct => Type::Int8,
        AggFunc::ApproxPercentile(_) => match arg {
            Type::Int2 | Type::Int4 | Type::Int8 | Type::Numeric | Type::Float4 | Type::Float8 => Type::Float8,
            _ => return Err(undefined_function(name, args)),
        },
        AggFunc::StringAgg => Type::Text,
        AggFunc::ArrayAgg => Type::Array(Box::new(arg)),
        AggFunc::JsonAgg { jsonb } | AggFunc::JsonObjectAgg { jsonb } => {
            if *jsonb {
                Type::Jsonb
            } else {
                Type::Json
            }
        }
    })
}

const FRAME_NONDEFAULT: i32 = 0x1;
const FRAME_RANGE: i32 = 0x2;
const FRAME_ROWS: i32 = 0x4;
const FRAME_GROUPS: i32 = 0x8;
const FRAME_START_UNBOUNDED_PRECEDING: i32 = 0x20;
const FRAME_END_UNBOUNDED_FOLLOWING: i32 = 0x100;
const FRAME_START_CURRENT_ROW: i32 = 0x200;
const FRAME_END_CURRENT_ROW: i32 = 0x400;
const FRAME_START_OFFSET_PRECEDING: i32 = 0x800;
const FRAME_END_OFFSET_PRECEDING: i32 = 0x1000;
const FRAME_START_OFFSET_FOLLOWING: i32 = 0x2000;
const FRAME_END_OFFSET_FOLLOWING: i32 = 0x4000;
const FRAME_EXCLUSION: i32 = 0x8000 | 0x10000 | 0x20000;

impl<'a> Binder<'a> {
    /// Binds an expression and converts it to `expected` implicitly.
    pub(crate) fn bind_as(&mut self, n: &pb::Node, scope: &Scope, expected: &Type) -> Result<Expr> {
        let (e, t) = self.bind_expr(n, scope)?;
        self.coerce(e, &t, expected)
    }

    /// Binds a predicate; `what` names the clause for the error message.
    pub(crate) fn bind_bool(&mut self, n: &pb::Node, scope: &Scope, what: &str) -> Result<Expr> {
        let (e, t) = self.bind_expr(n, scope)?;
        match t {
            Type::Bool => Ok(e),
            Type::Unknown => self.coerce(e, &t, &Type::Bool),
            other => Err(Error::type_mismatch(format!("argument of {what} must be type boolean, not type {other}"))),
        }
    }

    fn boolean(&mut self, (e, t): Typed, what: &str) -> Result<Expr> {
        match t {
            Type::Bool => Ok(e),
            Type::Unknown => self.coerce(e, &t, &Type::Bool),
            other => Err(Error::type_mismatch(format!("argument of {what} must be type boolean, not type {other}"))),
        }
    }

    /// Builds a call to a resolved function, coercing the arguments.
    pub(crate) fn call(&mut self, name: &str, f: value::Func, args: Vec<Typed>) -> Result<Typed> {
        let mut out = Vec::with_capacity(args.len());
        for ((e, t), want) in args.into_iter().zip(&f.args) {
            out.push(self.coerce(e, &t, want)?);
        }
        // Constant folding: an immutable function of constants is computed
        // once, while planning.
        let foldable = f.volatility == value::Volatility::Immutable && out.iter().all(Expr::is_const);
        // A constant argument that already failed must still be reported.
        let failed_argument = !self.const_errors.is_empty()
            && out.iter().any(|a| {
                let mut hit = false;
                a.walk(&mut |n| hit |= self.const_errors.iter().any(|(c, _)| c == n));
                hit
            });
        let folded = if !failed_argument && f.strict && out.iter().any(|a| matches!(a, Expr::Const(Value::Null))) {
            // A strict function of a NULL constant is NULL whatever the
            // other arguments are; they are not evaluated (so `x / 0 +
            // NULL` is NULL, as in Postgres).
            Some(Ok(Value::Null))
        } else if !foldable {
            None
        } else {
            let vals: Vec<Value> = out
                .iter()
                .map(|a| match a {
                    Expr::Const(v) => v.clone(),
                    _ => Value::Null,
                })
                .collect();
            Some((f.eval)(&crate::types::FnCtx::default(), &vals))
        };
        let call = Expr::Call(FuncRef { name: name.into(), eval: f.eval, strict: f.strict, ret: f.ret.clone() }, out);
        match folded {
            Some(Ok(v)) => Ok((Expr::Const(v), f.ret)),
            Some(Err(e)) => {
                // Whether the failure is reported depends on where the
                // expression sits (see `check_constant_errors`); until then
                // it stays a call that fails when evaluated.
                self.const_errors.push((call.clone(), e));
                Ok((call, f.ret))
            }
            None => Ok((call, f.ret)),
        }
    }

    /// Reports the failure of a constant subexpression the way Postgres
    /// does when it simplifies expressions while planning: also for rows
    /// that are never produced, but not inside branches that constant
    /// conditions rule out.
    fn check_constant_errors(&self, e: &Expr) -> Result<()> {
        match e {
            Expr::And(args) | Expr::Or(args) => {
                let decisive = matches!(e, Expr::Or(_));
                for a in args {
                    self.check_constant_errors(a)?;
                    if constant_value(a) == Some(Value::Bool(decisive)) {
                        break;
                    }
                }
                Ok(())
            }
            Expr::Case(whens, els) => {
                for (cond, result) in whens {
                    self.check_constant_errors(cond)?;
                    match constant_value(cond) {
                        Some(Value::Bool(true)) => return self.check_constant_errors(result),
                        // Constant false or NULL: the branch is unreachable.
                        Some(_) => {}
                        None => self.check_constant_errors(result)?,
                    }
                }
                match els {
                    Some(e) => self.check_constant_errors(e),
                    None => Ok(()),
                }
            }
            Expr::Coalesce(args) => {
                for a in args {
                    self.check_constant_errors(a)?;
                    if constant_value(a).is_some_and(|v| !v.is_null()) {
                        break;
                    }
                }
                Ok(())
            }
            other => {
                if let Expr::Call(..) = other {
                    if let Some((_, err)) = self.const_errors.iter().find(|(call, _)| call == other) {
                        return Err(err.clone());
                    }
                }
                let mut result = Ok(());
                other.for_each_child(&mut |c| {
                    if result.is_ok() {
                        result = self.check_constant_errors(c);
                    }
                });
                result
            }
        }
    }

    pub(crate) fn bind_op(&mut self, op: &str, left: Option<Typed>, right: Typed) -> Result<Typed> {
        let f = value::resolve_operator(op, left.as_ref().map(|l| &l.1), &right.1)?;
        let args = left.into_iter().chain(std::iter::once(right)).collect();
        self.call(op, f, args)
    }

    /// Converts a set of expressions to their common type.
    pub(crate) fn unify(&mut self, items: Vec<Typed>, context: &str) -> Result<(Vec<Expr>, Type)> {
        let types: Vec<Type> = items.iter().map(|(_, t)| t.clone()).collect();
        let common = value::common_type(&types, context)?;
        let mut out = Vec::with_capacity(items.len());
        for (e, t) in items {
            out.push(self.coerce(e, &t, &common)?);
        }
        Ok((out, common))
    }

    pub(crate) fn bind_expr(&mut self, n: &pb::Node, scope: &Scope) -> Result<Typed> {
        self.expr_depth += 1;
        let result = self.bind_expr_node(n, scope);
        self.expr_depth -= 1;
        let mut typed = result?;
        // Logical connectives over constants are constants themselves.
        if !matches!(typed.0, Expr::Const(_) | Expr::Column(_) | Expr::Call(..)) {
            if let Some(v) = constant_value(&typed.0) {
                typed.0 = Expr::Const(v);
            }
        }
        // Output columns of nested queries are skipped: Postgres drops the
        // ones the outer query does not use before simplifying them.
        let nested_output = self.select_depth > 1 && self.clause == "SELECT";
        if self.expr_depth == 0 && !self.const_errors.is_empty() && !nested_output {
            self.check_constant_errors(&typed.0)?;
        }
        Ok(typed)
    }

    fn bind_expr_node(&mut self, n: &pb::Node, scope: &Scope) -> Result<Typed> {
        match node(n)? {
            N::ColumnRef(c) => self.bind_column(c, scope),
            N::AConst(c) => Ok(bind_const(c)),
            N::ParamRef(p) => {
                let i = (p.number - 1).max(0) as usize;
                if self.param_types.len() <= i {
                    self.param_types.resize(i + 1, None);
                }
                Ok((Expr::Param(i), self.param_types[i].as_ref().map_or(Type::Unknown, Type::base)))
            }
            N::TypeCast(c) => self.bind_cast(c, scope),
            N::AExpr(e) => self.bind_a_expr(e, scope),
            N::BoolExpr(b) => {
                let op = pb::BoolExprType::try_from(b.boolop).unwrap_or(pb::BoolExprType::Undefined);
                let what = match op {
                    pb::BoolExprType::AndExpr => "AND",
                    pb::BoolExprType::OrExpr => "OR",
                    _ => "NOT",
                };
                let mut args = Vec::with_capacity(b.args.len());
                for a in &b.args {
                    args.push(self.bind_bool(a, scope, what)?);
                }
                Ok((
                    match op {
                        pb::BoolExprType::AndExpr => Expr::And(args),
                        pb::BoolExprType::OrExpr => Expr::Or(args),
                        _ => Expr::Not(Box::new(args.remove(0))),
                    },
                    Type::Bool,
                ))
            }
            N::NullTest(t) => {
                let arg = t.arg.as_ref().ok_or_else(|| Error::internal("NullTest without argument"))?;
                let (e, _) = self.bind_expr(arg, scope)?;
                Ok((Expr::IsNull(Box::new(e), t.nulltesttype == pb::NullTestType::IsNotNull as i32), Type::Bool))
            }
            N::BooleanTest(t) => {
                let arg = t.arg.as_ref().ok_or_else(|| Error::internal("BooleanTest without argument"))?;
                let e = self.bind_bool(arg, scope, "IS TRUE")?;
                let test = match pb::BoolTestType::try_from(t.booltesttype).unwrap_or(pb::BoolTestType::Undefined) {
                    pb::BoolTestType::IsTrue => BoolTest::IsTrue,
                    pb::BoolTestType::IsNotTrue => BoolTest::IsNotTrue,
                    pb::BoolTestType::IsFalse => BoolTest::IsFalse,
                    pb::BoolTestType::IsNotFalse => BoolTest::IsNotFalse,
                    pb::BoolTestType::IsUnknown => BoolTest::IsUnknown,
                    _ => BoolTest::IsNotUnknown,
                };
                Ok((Expr::BoolTest(Box::new(e), test), Type::Bool))
            }
            N::CaseExpr(c) => self.bind_case(c, scope),
            N::CoalesceExpr(c) => {
                let items = c.args.iter().map(|a| self.bind_expr(a, scope)).collect::<Result<Vec<_>>>()?;
                let (args, ty) = self.unify(items, "COALESCE")?;
                Ok((Expr::Coalesce(args), ty))
            }
            N::MinMaxExpr(m) => {
                let greatest = m.op == pb::MinMaxOp::IsGreatest as i32;
                let items = m.args.iter().map(|a| self.bind_expr(a, scope)).collect::<Result<Vec<_>>>()?;
                let (args, ty) = self.unify(items, if greatest { "GREATEST" } else { "LEAST" })?;
                Ok((Expr::MinMax(args, greatest), ty))
            }
            N::FuncCall(f) => self.bind_func(f, scope),
            N::SubLink(s) => self.bind_sublink(s, scope),
            N::AArrayExpr(a) => {
                let items = a.elements.iter().map(|e| self.bind_expr(e, scope)).collect::<Result<Vec<_>>>()?;
                if items.is_empty() {
                    return Ok((Expr::Array(Vec::new()), Type::Array(Box::new(Type::Unknown))));
                }
                if items.iter().any(|(_, t)| matches!(t, Type::Array(_))) {
                    return Err(Error::unsupported("multidimensional arrays"));
                }
                let (args, ty) = self.unify(items, "ARRAY")?;
                Ok((Expr::Array(args), Type::Array(Box::new(ty))))
            }
            N::RowExpr(r) => {
                let mut fields = Vec::with_capacity(r.args.len());
                for a in &r.args {
                    let (e, t) = self.bind_expr(a, scope)?;
                    // Untyped literals inside rows become text, as in PG.
                    fields.push(if t == Type::Unknown { self.coerce(e, &t, &Type::Text)? } else { e });
                }
                Ok((Expr::Row(fields), Type::Record))
            }
            N::AIndirection(i) => self.bind_indirection(i, scope),
            N::SqlvalueFunction(f) => {
                use pb::SqlValueFunctionOp as Op;
                let op = Op::try_from(f.op).unwrap_or(Op::SqlvalueFunctionOpUndefined);
                let func = match op {
                    Op::SvfopCurrentDate => "current_date",
                    Op::SvfopCurrentTime | Op::SvfopCurrentTimeN => "current_time",
                    Op::SvfopCurrentTimestamp | Op::SvfopCurrentTimestampN => "now",
                    Op::SvfopLocaltime | Op::SvfopLocaltimeN => "localtime",
                    Op::SvfopLocaltimestamp | Op::SvfopLocaltimestampN => "localtimestamp",
                    Op::SvfopCurrentCatalog => {
                        return Ok((Expr::Session(SessionFn::CurrentDatabase, vec![]), Type::Text))
                    }
                    Op::SvfopCurrentSchema => return Ok((Expr::Session(SessionFn::CurrentSchema, vec![]), Type::Text)),
                    _ => return Ok((Expr::Session(SessionFn::CurrentUser, vec![]), Type::Text)),
                };
                let f = value::resolve_function(func, &[])?;
                self.call(func, f, vec![])
            }
            N::CollateClause(c) => {
                let arg = c.arg.as_ref().ok_or_else(|| Error::internal("COLLATE without argument"))?;
                self.bind_expr(arg, scope)
            }
            N::SetToDefault(_) => Err(Error::syntax("DEFAULT is not allowed in this context")),
            N::NamedArgExpr(_) => Err(Error::unsupported("named function arguments")),
            N::GroupingFunc(_) => Err(Error::unsupported("GROUPING")),
            N::XmlExpr(_) | N::XmlSerialize(_) => Err(Error::unsupported("XML")),
            N::CurrentOfExpr(_) => Err(Error::unsupported("WHERE CURRENT OF")),
            N::MultiAssignRef(_) => Err(Error::unsupported("multiple-column assignment from a subquery")),
            other => Err(Error::unsupported(format!("expression {}", node_kind(other)))),
        }
    }

    /// `oid_column = $1`: an untyped parameter compared with a column of
    /// an alias type (`oid`, `"char"`) is described to the client with
    /// that type, as PostgreSQL does. Drivers that look up types in
    /// `pg_type` send the OID in the binary format of `oid`.
    fn adopt_alias_type(&mut self, column: Option<&pb::Node>, param: Option<&pb::Node>, scope: &Scope) {
        let (Some(N::ColumnRef(c)), Some(N::ParamRef(p))) =
            (column.and_then(|n| n.node.as_ref()), param.and_then(|n| n.node.as_ref()))
        else {
            return;
        };
        if p.number < 1 {
            return;
        }
        let parts: Vec<&str> = c
            .fields
            .iter()
            .filter_map(|f| match f.node.as_ref() {
                Some(N::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .collect();
        let (table, name) = match parts.as_slice() {
            [name] if c.fields.len() == 1 => (None, *name),
            [.., table, name] if parts.len() == c.fields.len() => (Some(*table), *name),
            _ => return,
        };
        let Ok(Some(Found::Col(i))) = lookup(scope, table, name) else { return };
        let ty = &scope.cols[i].ty;
        if *ty == ty.base() {
            return;
        }
        let at = p.number as usize - 1;
        if self.param_types.len() <= at {
            self.param_types.resize(at + 1, None);
        }
        if self.param_types[at].is_none() {
            self.param_types[at] = Some(ty.clone());
        }
    }

    fn bind_column(&mut self, c: &pb::ColumnRef, scope: &Scope) -> Result<Typed> {
        let mut parts = Vec::with_capacity(c.fields.len());
        for f in &c.fields {
            match node(f)? {
                N::String(s) => parts.push(s.sval.as_str()),
                N::AStar(_) => return Err(Error::syntax("row expansion via \"*\" is not supported here")),
                _ => return Err(Error::internal("unexpected column reference field")),
            }
        }
        let (table, name) = match parts.as_slice() {
            [name] => (None, *name),
            [table, name] => (Some(*table), *name),
            [_, table, name] => (Some(*table), *name),
            [_, _, table, name] => (Some(*table), *name),
            _ => return Err(Error::syntax("improper qualified name (too many dotted names)")),
        };

        // Current scope first, then enclosing queries from the inside out.
        if let Some(found) = lookup(scope, table, name)? {
            return Ok(match found {
                Found::Col(i) => (Expr::Column(i), scope.cols[i].ty.base()),
                Found::Row(start, len) => (Expr::Row((start..start + len).map(Expr::Column).collect()), Type::Record),
            });
        }
        for depth in 1..=self.outer_scopes.len() {
            let level = self.outer_scopes.len() - depth;
            if self.hidden_levels.contains(&level) {
                continue;
            }
            if let Some(found) = lookup(&self.outer_scopes[level], table, name)? {
                for flag in &mut self.correlated[level..] {
                    *flag = true;
                }
                let outer = &self.outer_scopes[level];
                return Ok(match found {
                    Found::Col(i) => (Expr::Outer(depth, i), outer.cols[i].ty.base()),
                    Found::Row(start, len) => {
                        (Expr::Row((start..start + len).map(|i| Expr::Outer(depth, i)).collect()), Type::Record)
                    }
                });
            }
        }
        Err(match table {
            Some(t)
                if !has_rel(scope, t)
                    && !self
                        .outer_scopes
                        .iter()
                        .enumerate()
                        .any(|(i, s)| !self.hidden_levels.contains(&i) && has_rel(s, t)) =>
            {
                Error::new(sqlstate::UNDEFINED_TABLE, format!("missing FROM-clause entry for table \"{t}\""))
            }
            Some(t) => Error::new(sqlstate::UNDEFINED_COLUMN, format!("column {t}.{name} does not exist")),
            None => Error::new(sqlstate::UNDEFINED_COLUMN, format!("column \"{name}\" does not exist")),
        }
        .with_position(c.location.max(0) as usize + 1))
    }

    fn bind_cast(&mut self, c: &pb::TypeCast, scope: &Scope) -> Result<Typed> {
        let tn = c.type_name.as_ref().ok_or_else(|| Error::internal("cast without type"))?;
        let arg = c.arg.as_ref().ok_or_else(|| Error::internal("cast without argument"))?;

        // 'name'::regclass and friends resolve catalog objects.
        let type_name = name_list(&tn.names)?.pop().unwrap_or_default();
        if matches!(type_name.as_str(), "regclass" | "regtype" | "regproc" | "regnamespace") {
            let (e, t) = self.bind_expr(arg, scope)?;
            if t.is_integer() {
                // There is no regclass type: casting an OID to one yields
                // the object's name, which is what such casts are used
                // for (display); casting a name yields the OID.
                let e = self.coerce(e, &t, &Type::Int8)?;
                return Ok((Expr::Session(SessionFn::Catalog(format!("{type_name}_out")), vec![e]), Type::Text));
            }
            let e = self.coerce(e, &t, &Type::Text)?;
            return Ok((Expr::Session(SessionFn::Catalog(type_name), vec![e]), Type::Int8));
        }

        let decl = self.resolve_type(tn)?;
        let (e, from) = self.bind_expr(arg, scope)?;
        let to = decl.ty.base();
        let e = self.explicit_cast(e, &from, &to)?;
        Ok((self.apply_typmod(e, &decl, true)?, to))
    }

    pub(crate) fn explicit_cast(&mut self, e: Expr, from: &Type, to: &Type) -> Result<Expr> {
        if from == to {
            return Ok(e);
        }
        if *from == Type::Unknown {
            // A parameter of unknown type adopts the cast's type; a literal
            // is parsed as that type.
            return self.coerce(e, from, to);
        }
        if !value::can_cast(from, to, CastContext::Explicit) {
            return Err(Error::new(sqlstate::CANNOT_COERCE, format!("cannot cast type {from} to {to}")));
        }
        self.cast_expr(e, from, to)
    }

    fn bind_a_expr(&mut self, e: &pb::AExpr, scope: &Scope) -> Result<Typed> {
        use pb::AExprKind as K;
        let kind = K::try_from(e.kind).unwrap_or(K::Undefined);
        let op = name_list(&e.name)?.pop().unwrap_or_default();
        let left =
            |b: &mut Self| -> Result<Option<Typed>> { e.lexpr.as_ref().map(|l| b.bind_expr(l, scope)).transpose() };
        let rexpr = e.rexpr.as_ref().ok_or_else(|| Error::internal("operator without right operand"))?;
        match kind {
            K::AexprOp | K::AexprLike | K::AexprIlike | K::AexprSimilar => {
                if op == "=" {
                    self.adopt_alias_type(e.lexpr.as_deref(), Some(rexpr), scope);
                    self.adopt_alias_type(Some(rexpr), e.lexpr.as_deref(), scope);
                }
                let l = left(self)?;
                let r = self.bind_expr(rexpr, scope)?;
                self.bind_op(&op, l, r)
            }
            K::AexprOpAny | K::AexprOpAll => {
                let l = left(self)?.ok_or_else(|| Error::internal("ANY without left operand"))?;
                let (r, rt) = self.bind_expr(rexpr, scope)?;
                // An untyped right side is an array literal of the left type.
                let (r, rt) = match rt {
                    Type::Unknown => {
                        let lt = if l.1 == Type::Unknown { Type::Text } else { l.1.clone() };
                        let want = Type::Array(Box::new(lt));
                        (self.coerce(r, &Type::Unknown, &want)?, want)
                    }
                    other => (r, other),
                };
                let elem = match &rt {
                    Type::Array(elem) if **elem == Type::Unknown => Type::Text,
                    Type::Array(elem) => (**elem).clone(),
                    v if v.is_vector() => v.vector_elem().unwrap_or(Type::Int2),
                    _ => {
                        return Err(Error::new(
                            sqlstate::DATATYPE_MISMATCH,
                            "op ANY/ALL (array) requires array on right side",
                        ))
                    }
                };
                let f = value::resolve_operator(&op, Some(&l.1), &elem)?;
                if f.ret != Type::Bool {
                    return Err(Error::new(
                        sqlstate::DATATYPE_MISMATCH,
                        "op ANY/ALL (array) requires operator to yield boolean",
                    ));
                }
                let left = self.coerce(l.0, &l.1, &f.args[0])?;
                let want = Type::Array(Box::new(f.args[1].clone()));
                let right = self.coerce(r, &rt, &want)?;
                Ok((
                    Expr::AnyAll {
                        left: Box::new(left),
                        op: FuncRef { name: op.as_str().into(), eval: f.eval, strict: f.strict, ret: Type::Bool },
                        right: Box::new(right),
                        all: kind == K::AexprOpAll,
                    },
                    Type::Bool,
                ))
            }
            K::AexprDistinct | K::AexprNotDistinct => {
                let l = left(self)?.ok_or_else(|| Error::internal("IS DISTINCT FROM without left operand"))?;
                let r = self.bind_expr(rexpr, scope)?;
                let (mut args, _) = self.unify(vec![l, r], "IS DISTINCT FROM")?;
                let b = args.pop().unwrap();
                let a = args.pop().unwrap();
                Ok((Expr::Distinct(Box::new(a), Box::new(b), kind == K::AexprNotDistinct), Type::Bool))
            }
            K::AexprNullif => {
                let l = left(self)?.ok_or_else(|| Error::internal("NULLIF without left operand"))?;
                let r = self.bind_expr(rexpr, scope)?;
                // The result has the type the left operand is compared as.
                // (Postgres compares integers of different widths directly.)
                let ty = match value::resolve_operator("=", Some(&l.1), &r.1) {
                    Ok(_) if l.1.is_integer() && r.1.is_integer() => l.1.clone(),
                    Ok(f) if f.args[0] != Type::Unknown => f.args[0].clone(),
                    _ if l.1 == Type::Unknown => Type::Text,
                    _ => l.1.clone(),
                };
                let result = self.coerce(l.0.clone(), &l.1, &ty)?;
                let (cond, _) = self.bind_op("=", Some(l), r)?;
                Ok((Expr::Case(vec![(cond, Expr::null())], Some(Box::new(result))), ty))
            }
            K::AexprIn => {
                let l = left(self)?.ok_or_else(|| Error::internal("IN without left operand"))?;
                let N::List(list) = node(rexpr)? else { return Err(Error::internal("IN without list")) };
                let mut items = vec![l];
                for item in &list.items {
                    items.push(self.bind_expr(item, scope)?);
                }
                let (mut args, _) = self.unify(items, "IN")?;
                let left = args.remove(0);
                Ok((Expr::InList(Box::new(left), args, op == "<>"), Type::Bool))
            }
            K::AexprBetween | K::AexprNotBetween | K::AexprBetweenSym | K::AexprNotBetweenSym => {
                let l = left(self)?.ok_or_else(|| Error::internal("BETWEEN without left operand"))?;
                let N::List(list) = node(rexpr)? else { return Err(Error::internal("BETWEEN without bounds")) };
                let [lo, hi] = list.items.as_slice() else { return Err(Error::internal("BETWEEN needs two bounds")) };
                let lo = self.bind_expr(lo, scope)?;
                let hi = self.bind_expr(hi, scope)?;
                let range = |b: &mut Self, lo: Typed, hi: Typed| -> Result<Expr> {
                    Ok(Expr::And(vec![
                        b.bind_op(">=", Some(l.clone()), lo)?.0,
                        b.bind_op("<=", Some(l.clone()), hi)?.0,
                    ]))
                };
                let sym = matches!(kind, K::AexprBetweenSym | K::AexprNotBetweenSym);
                let mut cond = range(self, lo.clone(), hi.clone())?;
                if sym {
                    cond = Expr::Or(vec![cond, range(self, hi, lo)?]);
                }
                if matches!(kind, K::AexprNotBetween | K::AexprNotBetweenSym) {
                    cond = Expr::Not(Box::new(cond));
                }
                Ok((cond, Type::Bool))
            }
            K::Undefined => Err(Error::internal("undefined expression kind")),
        }
    }

    fn bind_case(&mut self, c: &pb::CaseExpr, scope: &Scope) -> Result<Typed> {
        let operand = c.arg.as_ref().map(|a| self.bind_expr(a, scope)).transpose()?;
        let mut conds = Vec::with_capacity(c.args.len());
        let mut results = Vec::with_capacity(c.args.len() + 1);
        for w in &c.args {
            let N::CaseWhen(w) = node(w)? else { return Err(Error::internal("malformed CASE")) };
            let when = w.expr.as_ref().ok_or_else(|| Error::internal("WHEN without condition"))?;
            let cond = match &operand {
                Some(op) => {
                    let rhs = self.bind_expr(when, scope)?;
                    let cmp = self.bind_op("=", Some(op.clone()), rhs)?;
                    self.boolean(cmp, "CASE/WHEN")?
                }
                None => self.bind_bool(when, scope, "CASE/WHEN")?,
            };
            conds.push(cond);
            let result = w.result.as_ref().ok_or_else(|| Error::internal("WHEN without result"))?;
            results.push(self.bind_expr(result, scope)?);
        }
        let has_else = c.defresult.is_some();
        if let Some(d) = &c.defresult {
            results.push(self.bind_expr(d, scope)?);
        }
        let (mut results, ty) = self.unify(results, "CASE")?;
        let els = if has_else { results.pop().map(Box::new) } else { None };
        Ok((Expr::Case(conds.into_iter().zip(results).collect(), els), ty))
    }

    fn bind_indirection(&mut self, i: &pb::AIndirection, scope: &Scope) -> Result<Typed> {
        let arg = i.arg.as_ref().ok_or_else(|| Error::internal("indirection without base"))?;
        let (mut e, mut t) = self.bind_expr(arg, scope)?;
        for ind in &i.indirection {
            match node(ind)? {
                N::AIndices(ix) => {
                    let bound = |b: &mut Self, n: &Option<Box<pb::Node>>| -> Result<Option<Box<Expr>>> {
                        n.as_ref().map(|n| b.bind_as(n, scope, &Type::Int4).map(Box::new)).transpose()
                    };
                    match &t {
                        Type::Array(elem) => {
                            if ix.is_slice {
                                e = Expr::Slice(Box::new(e), bound(self, &ix.lidx)?, bound(self, &ix.uidx)?);
                            } else {
                                let idx = bound(self, &ix.uidx)?.ok_or_else(|| Error::syntax("missing subscript"))?;
                                e = Expr::Subscript(Box::new(e), idx);
                                t = (**elem).clone();
                            }
                        }
                        // Subscripts of int2vector and oidvector start at
                        // 0; a slice of one is an ordinary array.
                        v if v.is_vector() => {
                            let elem = v.vector_elem().unwrap_or(Type::Int2);
                            if ix.is_slice {
                                e = Expr::Slice(Box::new(e), bound(self, &ix.lidx)?, bound(self, &ix.uidx)?);
                                t = elem.array_of();
                            } else {
                                let idx = bound(self, &ix.uidx)?.ok_or_else(|| Error::syntax("missing subscript"))?;
                                e = Expr::Subscript(Box::new(e), idx);
                                t = elem;
                            }
                        }
                        Type::Jsonb if !ix.is_slice => {
                            let idx = ix.uidx.as_ref().ok_or_else(|| Error::syntax("missing subscript"))?;
                            let (ie, it) = self.bind_expr(idx, scope)?;
                            let ie = if it.is_integer() { ie } else { self.coerce(ie, &it, &Type::Text)? };
                            e = Expr::Subscript(Box::new(e), Box::new(ie));
                        }
                        other => {
                            return Err(Error::new(
                                sqlstate::DATATYPE_MISMATCH,
                                format!("cannot subscript type {other} because it does not support subscripting"),
                            ))
                        }
                    }
                }
                N::String(_) | N::AStar(_) => return Err(Error::unsupported("field selection from composite values")),
                _ => return Err(Error::internal("unexpected indirection")),
            }
        }
        Ok((e, t))
    }

    fn bind_func(&mut self, f: &pb::FuncCall, scope: &Scope) -> Result<Typed> {
        let parts = name_list(&f.funcname)?;
        let name = parts.last().cloned().unwrap_or_default().to_ascii_lowercase();
        if f.func_variadic {
            return Err(Error::unsupported("VARIADIC function calls"));
        }

        if f.agg_within_group {
            if f.over.is_some() {
                return Err(Error::new(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    format!("OVER is not supported for ordered-set aggregate {name}"),
                ));
            }
            return self.bind_ordered_set(f, &name, scope);
        }
        if let Some(over) = &f.over {
            return self.bind_window(f, &name, over, scope);
        }
        if f.agg_star || agg_func(&name).is_some() {
            return self.bind_aggregate(f, &name, scope);
        }
        if window_func(&name).is_some() {
            return Err(Error::new(
                sqlstate::WRONG_OBJECT_TYPE,
                format!("window function {name} requires an OVER clause"),
            ));
        }
        if f.agg_distinct || !f.agg_order.is_empty() || f.agg_filter.is_some() {
            return Err(Error::new(
                sqlstate::WRONG_OBJECT_TYPE,
                format!("DISTINCT, ORDER BY or FILTER specified, but {name} is not an aggregate function"),
            ));
        }

        let mut args = Vec::with_capacity(f.args.len());
        for a in &f.args {
            args.push(self.bind_expr(a, scope)?);
        }

        if let Some(kind) = srf(&name) {
            return self.bind_select_list_srf(kind, &name, args);
        }
        if let Some(r) = self.bind_session_fn(&name, &mut args)? {
            return Ok(r);
        }
        if matches!(name.as_str(), "gen_random_uuid" | "uuid_generate_v4") && args.is_empty() {
            fn random_uuid(_: &crate::types::FnCtx, _: &[Value]) -> Result<Value> {
                let mut bytes: [u8; 16] = rand::random();
                // Version 4, RFC 4122 variant.
                bytes[6] = (bytes[6] & 0x0f) | 0x40;
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                Ok(Value::Uuid(bytes))
            }
            let func = FuncRef { name: name.as_str().into(), eval: random_uuid, strict: false, ret: Type::Uuid };
            return Ok((Expr::Call(func, Vec::new()), Type::Uuid));
        }
        if name == "pg_typeof" && args.len() == 1 {
            return Ok((Expr::Const(Value::Text(args[0].1.name())), Type::Text));
        }

        let types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
        match value::resolve_function(&name, &types) {
            Ok(func) => self.call(&name, func, args),
            Err(err) => {
                // `int4(x)`, `text(x)`, `date(x)`: a type name used as a
                // function is a cast.
                if args.len() == 1 && err.code() == sqlstate::UNDEFINED_FUNCTION && !value::function_exists(&name) {
                    if let Some(to) = Type::from_name(&name) {
                        let (e, from) = args.pop().unwrap();
                        return Ok((self.explicit_cast(e, &from, &to)?, to));
                    }
                }
                Err(err.with_position(f.location.max(0) as usize + 1))
            }
        }
    }

    /// Functions answered by the session or the catalog.
    fn bind_session_fn(&mut self, name: &str, args: &mut Vec<Typed>) -> Result<Option<Typed>> {
        // Untyped arguments (`$1`, `'42'`) get the type PostgreSQL's
        // signature gives them, text where squad does not know it.
        let text_args = |b: &mut Self, args: &mut Vec<Typed>| -> Result<Vec<Expr>> {
            let n = args.len();
            std::mem::take(args)
                .into_iter()
                .enumerate()
                .map(|(i, (e, t))| {
                    if t == Type::Unknown {
                        let to = crate::system::catalog_fn_arg_type(name, n, i).unwrap_or(Type::Text);
                        b.coerce(e, &t, &to)
                    } else {
                        Ok(e)
                    }
                })
                .collect()
        };
        let (func, ret) = match (name, args.len()) {
            ("nextval", 1) => (SessionFn::NextVal, Type::Int8),
            ("currval", 1) => (SessionFn::CurrVal, Type::Int8),
            ("setval", 2 | 3) => (SessionFn::SetVal, Type::Int8),
            ("lastval", 0) => (SessionFn::LastVal, Type::Int8),
            ("current_schema", 0) => (SessionFn::CurrentSchema, Type::Text),
            ("current_schemas", 1) => (SessionFn::CurrentSchemas, Type::Array(Box::new(Type::Text))),
            ("current_user" | "session_user" | "user" | "current_role", 0) => (SessionFn::CurrentUser, Type::Text),
            ("current_database" | "current_catalog", 0) => (SessionFn::CurrentDatabase, Type::Text),
            ("version", 0) => (SessionFn::Version, Type::Text),
            ("current_setting", 1 | 2) => (SessionFn::CurrentSetting, Type::Text),
            ("set_config", 3) => (SessionFn::SetConfig, Type::Text),
            ("txid_current", 0) => (SessionFn::TxidCurrent, Type::Int8),
            ("pg_backend_pid", 0) => (SessionFn::BackendPid, Type::Int4),
            _ => match crate::system::catalog_fn_type(name, args.len()) {
                Some(ret) => (SessionFn::Catalog(name.to_string()), ret),
                None => return Ok(None),
            },
        };
        // `nextval('seq'::regclass)`: sequence functions take the name.
        if matches!(func, SessionFn::NextVal | SessionFn::CurrVal | SessionFn::SetVal) {
            if let Some((Expr::Session(SessionFn::Catalog(c), inner), _)) = args.first_mut() {
                if c == "regclass" && inner.len() == 1 {
                    let name = inner.pop().unwrap();
                    args[0] = (name, Type::Text);
                }
            }
        }
        if matches!(func, SessionFn::SetVal) {
            let (seq, st) = args.remove(0);
            let seq = self.coerce(seq, &st, &Type::Text)?;
            let (val, vt) = args.remove(0);
            let mut out = vec![seq, self.coerce(val, &vt, &Type::Int8)?];
            if let Some((called, ct)) = args.pop() {
                out.push(self.coerce(called, &ct, &Type::Bool)?);
            }
            return Ok(Some((Expr::Session(func, out), ret)));
        }
        let exprs = text_args(self, args)?;
        Ok(Some((Expr::Session(func, exprs), ret)))
    }

    fn bind_select_list_srf(&mut self, kind: Srf, name: &str, args: Vec<Typed>) -> Result<Typed> {
        let (exprs, mut cols) = self.bind_srf_args(&kind, name, args)?;
        if cols.len() != 1 {
            return Err(Error::unsupported(format!("{name} returning multiple columns in the select list")));
        }
        let Some(level) = self.agg_levels.last_mut().filter(|l| l.allow_srfs) else {
            return Err(Error::new(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("set-returning function {name} is not allowed in this context"),
            ));
        };
        level.srfs.push((kind, exprs));
        Ok((Expr::SrfRef(level.srfs.len() - 1), cols.remove(0).1))
    }

    /// Checks and coerces the arguments of a set-returning function and
    /// returns its output columns.
    pub(crate) fn bind_srf_args(&mut self, kind: &Srf, name: &str, args: Vec<Typed>) -> Result<(Vec<Expr>, Columns)> {
        let types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
        let bad = || undefined_function(name, &types);
        let coerce_all = |b: &mut Self, want: &[Type]| -> Result<Vec<Expr>> {
            args.iter().cloned().zip(want).map(|((e, t), w)| b.coerce(e, &t, w)).collect()
        };
        Ok(match kind {
            Srf::GenerateSeries => {
                if !(2..=3).contains(&types.len()) {
                    return Err(bad());
                }
                let first = &types[0];
                let (elem, step) = if matches!(first, Type::Timestamp | Type::TimestampTz | Type::Date) {
                    if types.len() != 3 {
                        return Err(bad());
                    }
                    let t = if *first == Type::TimestampTz { Type::TimestampTz } else { Type::Timestamp };
                    (t, Type::Interval)
                } else {
                    let t = value::common_type(&types, "generate_series")?;
                    let t = match t {
                        Type::Int2 | Type::Int4 | Type::Unknown | Type::Text => Type::Int4,
                        Type::Int8 => Type::Int8,
                        Type::Numeric | Type::Float4 | Type::Float8 => Type::Numeric,
                        _ => return Err(bad()),
                    };
                    (t.clone(), t)
                };
                let want = [elem.clone(), elem.clone(), step];
                (coerce_all(self, &want[..types.len()])?, vec![(name.to_string(), elem)])
            }
            Srf::Unnest if name != "unnest" => {
                // pg_partition_ancestors(regclass): the relation itself.
                if types.len() != 1 {
                    return Err(bad());
                }
                let (e, t) = args[0].clone();
                let oid = self.coerce(e, &t, &Type::Int8)?;
                (vec![Expr::Array(vec![oid])], vec![("relid".to_string(), Type::Int8)])
            }
            Srf::Unnest => {
                if types.is_empty() {
                    return Err(bad());
                }
                let mut cols = Vec::new();
                let mut want = Vec::new();
                for t in &types {
                    let elem = match t {
                        Type::Array(e) if **e != Type::Unknown => (**e).clone(),
                        Type::Array(_) | Type::Unknown => Type::Text,
                        t if t.is_vector() => t.vector_elem().unwrap_or(Type::Int2),
                        _ => return Err(bad()),
                    };
                    want.push(Type::Array(Box::new(elem.clone())));
                    cols.push((name.to_string(), elem));
                }
                (coerce_all(self, &want)?, cols)
            }
            Srf::JsonArrayElements { text } => {
                let json = name.starts_with("json_");
                let src = if json { Type::Json } else { Type::Jsonb };
                if types.len() != 1 {
                    return Err(bad());
                }
                (
                    coerce_all(self, std::slice::from_ref(&src))?,
                    vec![("value".to_string(), if *text { Type::Text } else { src })],
                )
            }
            Srf::JsonEach { text } => {
                let src = if name.starts_with("json_") { Type::Json } else { Type::Jsonb };
                if types.len() != 1 {
                    return Err(bad());
                }
                (
                    coerce_all(self, std::slice::from_ref(&src))?,
                    vec![("key".to_string(), Type::Text), ("value".to_string(), if *text { Type::Text } else { src })],
                )
            }
            Srf::JsonObjectKeys => {
                let src = if name.starts_with("json_") { Type::Json } else { Type::Jsonb };
                if types.len() != 1 {
                    return Err(bad());
                }
                (coerce_all(self, &[src])?, vec![(name.to_string(), Type::Text)])
            }
            Srf::RegexpSplitToTable | Srf::StringToTable => {
                if !(2..=3).contains(&types.len()) {
                    return Err(bad());
                }
                (
                    coerce_all(self, &[Type::Text, Type::Text, Type::Text][..types.len()])?,
                    vec![(name.to_string(), Type::Text)],
                )
            }
            Srf::GenerateSubscripts => {
                if types.len() != 2 || !(matches!(types[0], Type::Array(_)) || types[0].is_vector()) {
                    return Err(bad());
                }
                let want = [types[0].clone(), Type::Int4];
                (coerce_all(self, &want)?, vec![(name.to_string(), Type::Int4)])
            }
        })
    }

    fn bind_agg_call(&mut self, f: &pb::FuncCall, name: &str, scope: &Scope, nested_aggs: bool) -> Result<AggCall> {
        let func = if f.agg_star {
            AggFunc::CountStar
        } else {
            agg_func(name).ok_or_else(|| Error::internal("not an aggregate"))?
        };
        // Aggregate arguments belong to this query level but must not
        // contain aggregates themselves.
        let saved = self.agg_levels.last_mut().map(|l| std::mem::replace(&mut l.allow_aggs, nested_aggs));
        let result = (|| -> Result<AggCall> {
            let mut args = Vec::with_capacity(f.args.len());
            for a in &f.args {
                args.push(self.bind_expr(a, scope)?);
            }
            let expected = match func {
                AggFunc::CountStar => 0,
                AggFunc::StringAgg | AggFunc::JsonObjectAgg { .. } | AggFunc::ApproxPercentile(_) => 2,
                _ => 1,
            };
            let types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
            if args.len() != expected {
                return Err(undefined_function(name, &types));
            }
            let mut exprs = Vec::with_capacity(args.len());
            let mut arg_types = Vec::with_capacity(args.len());
            for (i, (e, t)) in args.into_iter().enumerate() {
                let want = match (&func, i, &t) {
                    (AggFunc::StringAgg, _, _) => Type::Text,
                    (AggFunc::JsonObjectAgg { .. }, 0, _) => Type::Text,
                    (_, _, Type::Unknown) => Type::Text,
                    (_, _, t) => t.clone(),
                };
                let e = match (&func, i) {
                    (AggFunc::StringAgg, _) if t != Type::Text && t != Type::Unknown => {
                        return Err(undefined_function(name, &types))
                    }
                    // Object keys of any type are converted to text.
                    (AggFunc::JsonObjectAgg { .. }, 0) if t != Type::Text && t != Type::Unknown => {
                        self.explicit_cast(e, &t, &Type::Text)?
                    }
                    _ => self.coerce(e, &t, &want)?,
                };
                exprs.push(e);
                arg_types.push(want);
            }
            // approx_percentile(x, p): p is a constant between 0 and 1.
            let func = match func {
                AggFunc::ApproxPercentile(_) => {
                    let p = match exprs.pop() {
                        Some(Expr::Const(v)) => v.as_f64(),
                        _ => None,
                    };
                    arg_types.pop();
                    match p {
                        Some(p) if (0.0..=1.0).contains(&p) => AggFunc::ApproxPercentile(p),
                        _ => {
                            return Err(Error::new(
                                sqlstate::INVALID_PARAMETER_VALUE,
                                "the percentile of approx_percentile must be a constant between 0 and 1",
                            ))
                        }
                    }
                }
                other => other,
            };
            let ret = agg_type(&func, name, &arg_types)?;
            let filter = f.agg_filter.as_ref().map(|n| self.bind_bool(n, scope, "FILTER")).transpose()?;
            let order_by = self.bind_sort_keys(&f.agg_order, scope)?;
            // The arguments are replaced by a placeholder in the enclosing
            // expression, so their constant parts are checked here.
            if !self.const_errors.is_empty() {
                for e in exprs.iter().chain(filter.iter()).chain(order_by.iter().map(|k| &k.expr)) {
                    self.check_constant_errors(e)?;
                }
            }
            // (Two bindings of the same subquery never compare equal, so
            // sort keys containing one are given the benefit of the doubt.)
            let has_subquery = |e: &Expr| {
                let mut found = false;
                e.walk(&mut |n| found |= matches!(n, Expr::Subquery(_)));
                found
            };
            if f.agg_distinct && order_by.iter().any(|k| !exprs.contains(&k.expr) && !has_subquery(&k.expr)) {
                return Err(Error::new(
                    sqlstate::INVALID_COLUMN_REFERENCE,
                    "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
                ));
            }
            Ok(AggCall { func, args: exprs, distinct: f.agg_distinct, filter, order_by, ret })
        })();
        if let (Some(level), Some(allow)) = (self.agg_levels.last_mut(), saved) {
            level.allow_aggs = allow;
        }
        result
    }

    /// `percentile_cont`, `percentile_disc` and `mode` with WITHIN GROUP:
    /// an `array_agg` of the sorted non-null values, and a final function
    /// of it and the direct argument, which belongs to the enclosing
    /// expression (so it may use only grouped columns).
    fn bind_ordered_set(&mut self, f: &pb::FuncCall, name: &str, scope: &Scope) -> Result<Typed> {
        let (eval, direct): (crate::value::FuncImpl, usize) = match name {
            "percentile_cont" => (crate::agg::percentile_cont_final, 1),
            "percentile_disc" => (crate::agg::percentile_disc_final, 1),
            "mode" => (crate::agg::mode_final, 0),
            "rank" | "dense_rank" | "percent_rank" | "cume_dist" => {
                return Err(Error::unsupported(format!("hypothetical-set aggregate {name}(…) WITHIN GROUP")))
            }
            _ if f.agg_star => {
                return Err(Error::new(
                    sqlstate::WRONG_OBJECT_TYPE,
                    format!("{name} is not an ordered-set aggregate, so it cannot have WITHIN GROUP"),
                ))
            }
            _ => return Err(undefined_function(name, &[])),
        };
        match self.agg_levels.last() {
            Some(l) if l.allow_aggs => {}
            Some(l) if l.in_aggregate => {
                return Err(Error::new(sqlstate::GROUPING_ERROR, "aggregate function calls cannot be nested"))
            }
            _ => {
                return Err(Error::new(
                    sqlstate::GROUPING_ERROR,
                    format!("aggregate functions are not allowed in {}", self.clause),
                ))
            }
        }
        // Neither the direct argument nor the sorted one may contain
        // aggregates.
        let level = self.agg_levels.last_mut().unwrap();
        let saved = (std::mem::replace(&mut level.allow_aggs, false), std::mem::replace(&mut level.in_aggregate, true));
        let result = self.bind_ordered_set_parts(f, name, direct, scope);
        let level = self.agg_levels.last_mut().unwrap();
        (level.allow_aggs, level.in_aggregate) = saved;
        let (direct_args, call, ret) = result?;
        let level = self.agg_levels.last_mut().unwrap();
        let idx = match level.aggs.iter().position(|a| *a == call) {
            Some(i) => i,
            None => {
                level.aggs.push(call);
                level.aggs.len() - 1
            }
        };
        let mut args = direct_args;
        args.push(Expr::AggRef(idx));
        Ok((Expr::Call(FuncRef { name: name.into(), eval, strict: false, ret: ret.clone() }, args), ret))
    }

    fn bind_ordered_set_parts(
        &mut self,
        f: &pb::FuncCall,
        name: &str,
        direct: usize,
        scope: &Scope,
    ) -> Result<(Vec<Expr>, AggCall, Type)> {
        let mut args = Vec::with_capacity(f.args.len());
        for a in &f.args {
            args.push(self.bind_expr(a, scope)?);
        }
        let [order] = f.agg_order.as_slice() else {
            let types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
            return Err(undefined_function(name, &types));
        };
        let N::SortBy(s) = node(order)? else { return Err(Error::internal("malformed WITHIN GROUP")) };
        let target = s.node.as_ref().ok_or_else(|| Error::internal("WITHIN GROUP without expression"))?;
        let (x, xt) = self.bind_expr(target, scope)?;
        let mut types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
        types.push(xt.clone());
        if args.len() != direct {
            return Err(undefined_function(name, &types));
        }
        // percentile_cont interpolates float8 or interval values; the
        // others take any sortable type.
        let elem = match name {
            "percentile_cont" if xt == Type::Interval => Type::Interval,
            "percentile_cont" if value::can_cast(&xt, &Type::Float8, CastContext::Implicit) => Type::Float8,
            "percentile_cont" => return Err(undefined_function(name, &types)),
            _ if xt == Type::Unknown => {
                return Err(Error::new(
                    sqlstate::DATATYPE_MISMATCH,
                    "could not determine polymorphic type because input has type unknown",
                ))
            }
            _ => xt.clone(),
        };
        let x = self.coerce(x, &xt, &elem)?;
        // The fraction: float8, or an array of them for several results.
        let mut direct_args = Vec::new();
        let mut ret = elem.clone();
        if let Some((p, pt)) = args.pop() {
            let want = match &pt {
                Type::Array(_) => Type::Float8.array_of(),
                _ => Type::Float8,
            };
            if !value::can_cast(&pt, &want, CastContext::Implicit) {
                return Err(undefined_function(name, &types));
            }
            if matches!(want, Type::Array(_)) {
                ret = elem.clone().array_of();
            }
            direct_args.push(self.coerce(p, &pt, &want)?);
        }
        let key = sort_key(x.clone(), s)?;
        let not_null = Expr::IsNull(Box::new(x.clone()), true);
        let filter = match f.agg_filter.as_ref().map(|n| self.bind_bool(n, scope, "FILTER")).transpose()? {
            Some(cond) => Expr::And(vec![cond, not_null]),
            None => not_null,
        };
        if !self.const_errors.is_empty() {
            for e in [&x, &filter] {
                self.check_constant_errors(e)?;
            }
        }
        let call = AggCall {
            func: AggFunc::ArrayAgg,
            args: vec![x],
            distinct: false,
            filter: Some(filter),
            order_by: vec![key],
            ret: elem.array_of(),
        };
        Ok((direct_args, call, ret))
    }

    fn bind_aggregate(&mut self, f: &pb::FuncCall, name: &str, scope: &Scope) -> Result<Typed> {
        match self.agg_levels.last() {
            Some(l) if l.allow_aggs => {}
            Some(l) if l.in_aggregate => {
                return Err(Error::new(sqlstate::GROUPING_ERROR, "aggregate function calls cannot be nested"))
            }
            _ => {
                return Err(Error::new(
                    sqlstate::GROUPING_ERROR,
                    format!("aggregate functions are not allowed in {}", self.clause),
                ))
            }
        }
        self.agg_levels.last_mut().unwrap().in_aggregate = true;
        let call = self.bind_agg_call(f, name, scope, false);
        let level = self.agg_levels.last_mut().unwrap();
        level.in_aggregate = false;
        let call = call?;
        let ret = call.ret.clone();
        // Identical aggregates are computed once.
        let idx = match level.aggs.iter().position(|a| *a == call) {
            Some(i) => i,
            None => {
                level.aggs.push(call);
                level.aggs.len() - 1
            }
        };
        Ok((Expr::AggRef(idx), ret))
    }

    pub(crate) fn bind_sort_keys(&mut self, nodes: &[pb::Node], scope: &Scope) -> Result<Vec<SortKey>> {
        let mut keys = Vec::with_capacity(nodes.len());
        for n in nodes {
            let N::SortBy(s) = node(n)? else { return Err(Error::internal("malformed ORDER BY")) };
            let target = s.node.as_ref().ok_or_else(|| Error::internal("ORDER BY without expression"))?;
            let (expr, ty) = self.bind_expr(target, scope)?;
            let expr = if ty == Type::Unknown { self.coerce(expr, &ty, &Type::Text)? } else { expr };
            keys.push(sort_key(expr, s)?);
        }
        Ok(keys)
    }

    fn bind_window(&mut self, f: &pb::FuncCall, name: &str, over: &pb::WindowDef, scope: &Scope) -> Result<Typed> {
        match self.agg_levels.last() {
            Some(l) if l.allow_windows => {}
            _ => {
                return Err(Error::new(
                    sqlstate::WINDOWING_ERROR,
                    format!("window functions are not allowed in {}", self.clause),
                ))
            }
        }
        // Resolve `OVER w` / `OVER (w ...)` against the WINDOW clause.
        let mut def = over.clone();
        let base = if !over.refname.is_empty() { &over.refname } else { &over.name };
        if !base.is_empty() {
            let named = self
                .agg_levels
                .last()
                .and_then(|l| l.window_defs.iter().find(|w| w.name == *base))
                .cloned()
                .ok_or_else(|| Error::new(sqlstate::UNDEFINED_OBJECT, format!("window \"{base}\" does not exist")))?;
            if def.partition_clause.is_empty() {
                def.partition_clause = named.partition_clause.clone();
            }
            if def.order_clause.is_empty() {
                def.order_clause = named.order_clause.clone();
            }
            if def.frame_options & FRAME_NONDEFAULT == 0 {
                def.frame_options = named.frame_options;
                def.start_offset = named.start_offset.clone();
                def.end_offset = named.end_offset.clone();
            }
        }

        // Window function arguments may contain aggregates but not other
        // window functions.
        let level = self.agg_levels.last_mut().unwrap();
        level.allow_windows = false;
        let result = (|| -> Result<WindowCall> {
            let (func, args, ret) = if f.agg_star || agg_func(name).is_some() {
                let call = self.bind_agg_call(f, name, scope, true)?;
                if call.distinct {
                    return Err(Error::unsupported("DISTINCT in window aggregates"));
                }
                (WindowFunc::Agg(call.func), call.args, call.ret)
            } else {
                let func = window_func(name).ok_or_else(|| {
                    Error::new(
                        sqlstate::WRONG_OBJECT_TYPE,
                        format!("OVER specified, but {name} is not a window function nor an aggregate function"),
                    )
                })?;
                let mut args = Vec::with_capacity(f.args.len());
                for a in &f.args {
                    args.push(self.bind_expr(a, scope)?);
                }
                let types: Vec<Type> = args.iter().map(|(_, t)| t.clone()).collect();
                let bad = || undefined_function(name, &types);
                // These are polymorphic: an untyped literal has no type to
                // take the result type from.
                let known = |t: Type| -> Result<Type> {
                    if t == Type::Unknown {
                        return Err(Error::type_mismatch(
                            "could not determine polymorphic type because input has type unknown",
                        ));
                    }
                    Ok(t)
                };
                let mut it = args.into_iter();
                let (exprs, ret) = match func {
                    WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
                        if !types.is_empty() {
                            return Err(bad());
                        }
                        (vec![], Type::Int8)
                    }
                    WindowFunc::PercentRank | WindowFunc::CumeDist => {
                        if !types.is_empty() {
                            return Err(bad());
                        }
                        (vec![], Type::Float8)
                    }
                    WindowFunc::Ntile => {
                        let (e, t) = it.next().ok_or_else(bad)?;
                        (vec![self.coerce(e, &t, &Type::Int4)?], Type::Int4)
                    }
                    WindowFunc::Lag | WindowFunc::Lead => {
                        let (e, t) = it.next().ok_or_else(bad)?;
                        let ty = known(t.clone())?;
                        let mut exprs = vec![self.coerce(e, &t, &ty)?];
                        if let Some((o, ot)) = it.next() {
                            exprs.push(self.coerce(o, &ot, &Type::Int4)?);
                        }
                        if let Some((d, dt)) = it.next() {
                            exprs.push(self.coerce(d, &dt, &ty)?);
                        }
                        (exprs, ty)
                    }
                    WindowFunc::FirstValue | WindowFunc::LastValue => {
                        let (e, t) = it.next().ok_or_else(bad)?;
                        let ty = known(t.clone())?;
                        (vec![self.coerce(e, &t, &ty)?], ty)
                    }
                    WindowFunc::NthValue => {
                        let (e, t) = it.next().ok_or_else(bad)?;
                        let (n, nt) = it.next().ok_or_else(bad)?;
                        let ty = known(t.clone())?;
                        (vec![self.coerce(e, &t, &ty)?, self.coerce(n, &nt, &Type::Int4)?], ty)
                    }
                    WindowFunc::Agg(_) => unreachable!(),
                };
                (func, exprs, ret)
            };
            let filter = match &func {
                WindowFunc::Agg(_) => f.agg_filter.as_ref().map(|n| self.bind_bool(n, scope, "FILTER")).transpose()?,
                _ => None,
            };
            let mut partition_by = Vec::with_capacity(def.partition_clause.len());
            for p in &def.partition_clause {
                partition_by.push(self.bind_expr(p, scope)?.0);
            }
            let order_by = self.bind_sort_keys(&def.order_clause, scope)?;
            let frame = self.bind_frame(&def, scope)?;
            Ok(WindowCall { func, args, partition_by, order_by, frame, filter, ret })
        })();
        let level = self.agg_levels.last_mut().unwrap();
        level.allow_windows = true;
        let call = result?;
        let ret = call.ret.clone();
        level.windows.push(call);
        Ok((Expr::WindowRef(level.windows.len() - 1), ret))
    }

    fn bind_frame(&mut self, def: &pb::WindowDef, scope: &Scope) -> Result<Frame> {
        let o = def.frame_options;
        if o & FRAME_NONDEFAULT == 0 {
            return Ok(Frame {
                unit: FrameUnit::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            });
        }
        if o & FRAME_EXCLUSION != 0 {
            return Err(Error::unsupported("frame exclusion"));
        }
        let unit = if o & FRAME_ROWS != 0 {
            FrameUnit::Rows
        } else if o & FRAME_GROUPS != 0 {
            FrameUnit::Groups
        } else {
            debug_assert!(o & FRAME_RANGE != 0);
            FrameUnit::Range
        };
        let offset = |b: &mut Self, n: &Option<Box<pb::Node>>| -> Result<Expr> {
            let n = n.as_ref().ok_or_else(|| Error::internal("frame offset missing"))?;
            let (e, t) = b.bind_expr(n, scope)?;
            if unit == FrameUnit::Range {
                // RANGE offsets are in the unit of the ordering column.
                return if t == Type::Unknown { b.coerce(e, &t, &Type::Interval) } else { Ok(e) };
            }
            b.coerce(e, &t, &Type::Int8)
        };
        let start = if o & FRAME_START_UNBOUNDED_PRECEDING != 0 {
            FrameBound::UnboundedPreceding
        } else if o & FRAME_START_CURRENT_ROW != 0 {
            FrameBound::CurrentRow
        } else if o & FRAME_START_OFFSET_PRECEDING != 0 {
            FrameBound::Preceding(offset(self, &def.start_offset)?)
        } else if o & FRAME_START_OFFSET_FOLLOWING != 0 {
            FrameBound::Following(offset(self, &def.start_offset)?)
        } else {
            return Err(Error::syntax("frame start cannot be UNBOUNDED FOLLOWING"));
        };
        let end = if o & FRAME_END_UNBOUNDED_FOLLOWING != 0 {
            FrameBound::UnboundedFollowing
        } else if o & FRAME_END_CURRENT_ROW != 0 {
            FrameBound::CurrentRow
        } else if o & FRAME_END_OFFSET_PRECEDING != 0 {
            FrameBound::Preceding(offset(self, &def.end_offset)?)
        } else if o & FRAME_END_OFFSET_FOLLOWING != 0 {
            FrameBound::Following(offset(self, &def.end_offset)?)
        } else {
            return Err(Error::syntax("frame end cannot be UNBOUNDED PRECEDING"));
        };
        if unit == FrameUnit::Range
            && matches!(start, FrameBound::Preceding(_) | FrameBound::Following(_))
                | matches!(end, FrameBound::Preceding(_) | FrameBound::Following(_))
            && def.order_clause.len() != 1
        {
            return Err(Error::new(
                sqlstate::WINDOWING_ERROR,
                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
            ));
        }
        Ok(Frame { unit, start, end })
    }

    fn bind_sublink(&mut self, s: &pb::SubLink, scope: &Scope) -> Result<Typed> {
        use pb::SubLinkType as T;
        let kind = T::try_from(s.sub_link_type).unwrap_or(T::Undefined);
        let Some(N::SelectStmt(sel)) = opt_node(&s.subselect) else {
            return Err(Error::internal("sublink without subquery"));
        };
        let (plan, out, correlated) = self.bind_subquery(sel, scope)?;
        let one_column = |out: &[super::OutputColumn]| -> Result<Type> {
            match out {
                [c] => Ok(if c.ty == Type::Unknown { Type::Text } else { c.ty.base() }),
                _ => Err(Error::syntax("subquery must return only one column")),
            }
        };
        let id = self.alloc_subquery_id();
        let make = |plan: Plan, kind: SubKind| {
            Expr::Subquery(Box::new(SubqueryExpr { id, plan: plan.into(), kind, correlated }))
        };
        match kind {
            T::ExistsSublink => Ok((make(plan, SubKind::Exists), Type::Bool)),
            T::ExprSublink => {
                let ty = one_column(&out)?;
                Ok((make(plan, SubKind::Scalar), ty))
            }
            T::ArraySublink => {
                let ty = one_column(&out)?;
                Ok((make(plan, SubKind::Array), Type::Array(Box::new(ty))))
            }
            T::AnySublink | T::AllSublink => {
                let test =
                    s.testexpr.as_ref().ok_or_else(|| Error::internal("quantified subquery without left side"))?;
                let lefts: Vec<Typed> = match node(test)? {
                    N::RowExpr(r) => r.args.iter().map(|a| self.bind_expr(a, scope)).collect::<Result<_>>()?,
                    _ => vec![self.bind_expr(test, scope)?],
                };
                if lefts.len() != out.len() {
                    return Err(Error::syntax(if lefts.len() > out.len() {
                        "subquery has too few columns"
                    } else {
                        "subquery has too many columns"
                    }));
                }
                let op_name = name_list(&s.oper_name)?.pop();
                let mut left_exprs = Vec::with_capacity(lefts.len());
                let mut casts: Vec<Option<Type>> = Vec::with_capacity(lefts.len());
                let mut op = None;
                for ((e, lt), col) in lefts.into_iter().zip(&out) {
                    let rt = if col.ty == Type::Unknown { Type::Text } else { col.ty.base() };
                    match &op_name {
                        Some(name) if name != "=" || out.len() > 1 && false => {
                            let f = value::resolve_operator(name, Some(&lt), &rt)?;
                            left_exprs.push(self.coerce(e, &lt, &f.args[0])?);
                            casts.push(if f.args[1] != rt { Some(f.args[1].clone()) } else { None });
                            op = Some(FuncRef {
                                name: name.as_str().into(),
                                eval: f.eval,
                                strict: f.strict,
                                ret: Type::Bool,
                            });
                        }
                        _ => {
                            let common = value::common_type(&[lt.clone(), rt.clone()], "IN")?;
                            left_exprs.push(self.coerce(e, &lt, &common)?);
                            casts.push(if common != rt { Some(common) } else { None });
                        }
                    }
                }
                let plan = if casts.iter().any(Option::is_some) {
                    let exprs = casts
                        .iter()
                        .enumerate()
                        .map(|(i, c)| match c {
                            Some(t) => self.cast_expr(Expr::Column(i), &out[i].ty, t),
                            None => Ok(Expr::Column(i)),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Plan::Project(Box::new(plan), exprs)
                } else {
                    plan
                };
                Ok((make(plan, SubKind::Quantified { left: left_exprs, op, all: kind == T::AllSublink }), Type::Bool))
            }
            _ => Err(Error::unsupported("this kind of subquery expression")),
        }
    }
}

/// Value of an expression built only from constants and logical
/// connectives, if it is one.
fn constant_value(e: &Expr) -> Option<Value> {
    let tri = |b: Option<bool>| b.map_or(Value::Null, Value::Bool);
    Some(match e {
        Expr::Const(v) => v.clone(),
        Expr::Not(a) => tri(constant_value(a)?.as_bool().map(|b| !b)),
        Expr::And(args) | Expr::Or(args) => {
            // One decisive constant settles the result whatever the other
            // operands are, as in Postgres' expression simplification.
            let decisive = matches!(e, Expr::Or(_));
            let (mut unknown, mut variable) = (false, false);
            for a in args {
                match constant_value(a) {
                    None => variable = true,
                    Some(v) => match v.as_bool() {
                        Some(b) if b == decisive => return Some(Value::Bool(decisive)),
                        Some(_) => {}
                        None => unknown = true,
                    },
                }
            }
            if variable {
                return None;
            }
            tri(if unknown { None } else { Some(!decisive) })
        }
        Expr::IsNull(a, negated) => match constant_value(a)? {
            Value::Record(_) => return None,
            v => Value::Bool(v.is_null() != *negated),
        },
        Expr::BoolTest(a, test) => {
            let v = constant_value(a)?.as_bool();
            Value::Bool(match test {
                BoolTest::IsTrue => v == Some(true),
                BoolTest::IsNotTrue => v != Some(true),
                BoolTest::IsFalse => v == Some(false),
                BoolTest::IsNotFalse => v != Some(false),
                BoolTest::IsUnknown => v.is_none(),
                BoolTest::IsNotUnknown => v.is_some(),
            })
        }
        Expr::InList(a, list, negated) => {
            let v = constant_value(a)?;
            if v.is_null() {
                return Some(Value::Null);
            }
            let mut unknown = false;
            for item in list {
                match crate::expr::sql_eq(&v, &constant_value(item)?) {
                    Some(true) => return Some(Value::Bool(!negated)),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            tri(if unknown { None } else { Some(*negated) })
        }
        Expr::Distinct(a, b, negated) => {
            let (a, b) = (constant_value(a)?, constant_value(b)?);
            let distinct = match (a.is_null(), b.is_null()) {
                (true, true) => false,
                (true, false) | (false, true) => true,
                _ => crate::types::compare(&a, &b) != std::cmp::Ordering::Equal,
            };
            Value::Bool(distinct != *negated)
        }
        Expr::Coalesce(args) => {
            for a in args {
                let v = constant_value(a)?;
                if !v.is_null() {
                    return Some(v);
                }
            }
            Value::Null
        }
        Expr::Case(whens, els) => {
            for (cond, result) in whens {
                if constant_value(cond)? == Value::Bool(true) {
                    return constant_value(result);
                }
            }
            match els {
                Some(e) => constant_value(e)?,
                None => Value::Null,
            }
        }
        _ => return None,
    })
}

pub(crate) fn sort_key(expr: Expr, s: &pb::SortBy) -> Result<SortKey> {
    use pb::{SortByDir as D, SortByNulls as Nl};
    let desc = match D::try_from(s.sortby_dir).unwrap_or(D::Undefined) {
        D::SortbyDesc => true,
        D::SortbyUsing => return Err(Error::unsupported("ORDER BY ... USING")),
        _ => false,
    };
    let nulls_first = match Nl::try_from(s.sortby_nulls).unwrap_or(Nl::Undefined) {
        Nl::SortbyNullsFirst => true,
        Nl::SortbyNullsLast => false,
        _ => desc,
    };
    Ok(SortKey { expr, desc, nulls_first })
}

fn bind_const(c: &pb::AConst) -> Typed {
    if c.isnull {
        return (Expr::Const(Value::Null), Type::Unknown);
    }
    match &c.val {
        Some(Val::Ival(i)) => (Expr::Const(Value::Int4(i.ival)), Type::Int4),
        Some(Val::Fval(f)) => {
            // Integers too large for int4 arrive as floats: int8 if they
            // fit, numeric otherwise (as in Postgres).
            if let Ok(i) = f.fval.parse::<i64>() {
                return (Expr::Const(Value::Int8(i)), Type::Int8);
            }
            match f.fval.parse::<rust_decimal::Decimal>().or_else(|_| rust_decimal::Decimal::from_scientific(&f.fval)) {
                Ok(d) => (Expr::Const(Value::Numeric(d)), Type::Numeric),
                Err(_) => (Expr::Const(Value::Float8(f.fval.parse().unwrap_or(f64::NAN))), Type::Float8),
            }
        }
        Some(Val::Boolval(b)) => (Expr::Const(Value::Bool(b.boolval)), Type::Bool),
        Some(Val::Sval(s)) => (Expr::Const(Value::Text(s.sval.clone())), Type::Unknown),
        Some(Val::Bsval(s)) => (Expr::Const(Value::Text(s.bsval.clone())), Type::Unknown),
        None => (Expr::Const(Value::Null), Type::Unknown),
    }
}

enum Found {
    Col(usize),
    /// Whole-row reference: `(start, len)` of the relation's columns.
    Row(usize, usize),
}

fn has_rel(scope: &Scope, alias: &str) -> bool {
    scope.rels.iter().any(|r| r.alias == alias)
}

fn lookup(scope: &Scope, table: Option<&str>, name: &str) -> Result<Option<Found>> {
    let mut hit = None;
    for (i, c) in scope.cols.iter().enumerate() {
        let matches = match table {
            Some(t) => c.name == name && c.table.as_deref() == Some(t),
            None => c.name == name && !c.merged,
        };
        if matches {
            if hit.is_some() {
                return Err(Error::new(
                    sqlstate::AMBIGUOUS_COLUMN,
                    format!("column reference \"{name}\" is ambiguous"),
                ));
            }
            hit = Some(i);
        }
    }
    if let Some(i) = hit {
        return Ok(Some(Found::Col(i)));
    }
    // An unqualified name may also denote a whole row of a relation.
    if table.is_none() {
        if let Some(rel) = scope.rels.iter().find(|r| r.alias == name) {
            let len = scope.cols[rel.start..rel.start + rel.len].iter().take_while(|c| !c.hidden).count();
            return Ok(Some(Found::Row(rel.start, len)));
        }
    }
    Ok(None)
}

pub(crate) fn node_kind(n: &N) -> String {
    let dbg = format!("{n:?}");
    dbg.split(['(', ' ', '{']).next().unwrap_or("node").to_string()
}
