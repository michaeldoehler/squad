//! Rule-based optimization applied while binding: predicate pushdown,
//! access path selection, join method selection and sort elimination.

use super::{Binder, Scope};
use crate::expr::{Expr, SessionFn};
use crate::plan::{JoinKind, Plan, ScanPlan, SortKey};
use crate::types::Type;
use crate::Result;

/// Splits a predicate into its top-level AND conjuncts.
pub(crate) fn conjuncts(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(items) => items.into_iter().for_each(|i| conjuncts(i, out)),
        other => out.push(other),
    }
}

pub(crate) fn and_all(mut items: Vec<Expr>) -> Option<Expr> {
    match items.len() {
        0 => None,
        1 => items.pop(),
        _ => Some(Expr::And(items)),
    }
}

/// Lowest and highest input column referenced, if any.
fn column_range(e: &Expr) -> Option<(usize, usize)> {
    let mut range: Option<(usize, usize)> = None;
    e.walk(&mut |n| {
        if let Expr::Column(i) = n {
            range = Some(match range {
                Some((lo, hi)) => (lo.min(*i), hi.max(*i)),
                None => (*i, *i),
            });
        }
    });
    range
}

/// True if the expression can only be evaluated against the current row of
/// this plan level (it contains a correlated subquery or placeholder).
fn opaque(e: &Expr) -> bool {
    let mut found = false;
    e.walk(&mut |n| match n {
        Expr::Subquery(s) if s.correlated => found = true,
        Expr::AggRef(_) | Expr::WindowRef(_) | Expr::SrfRef(_) => found = true,
        _ => {}
    });
    found
}

fn shift_down(e: Expr, by: usize) -> Expr {
    e.rewrite(&mut |n| Ok(if let Expr::Column(i) = n { Some(Expr::Column(i - by)) } else { None })).expect("infallible")
}

/// Types that share one key encoding and comparison semantics.
#[derive(PartialEq, Eq, Clone, Copy)]
enum KeyClass {
    Int,
    Float,
    Numeric,
    Text,
    Bool,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Interval,
    Uuid,
    Bytea,
    Enum,
}

fn key_class(t: &Type) -> Option<KeyClass> {
    Some(match t {
        Type::Int2 | Type::Int4 | Type::Int8 | Type::Oid => KeyClass::Int,
        Type::Char => KeyClass::Text,
        Type::Float4 | Type::Float8 => KeyClass::Float,
        Type::Numeric => KeyClass::Numeric,
        Type::Text => KeyClass::Text,
        Type::Bool => KeyClass::Bool,
        Type::Date => KeyClass::Date,
        Type::Timestamp => KeyClass::Timestamp,
        Type::TimestampTz => KeyClass::TimestampTz,
        Type::Time => KeyClass::Time,
        Type::Interval => KeyClass::Interval,
        Type::Uuid => KeyClass::Uuid,
        Type::Bytea => KeyClass::Bytea,
        Type::Enum(_) => KeyClass::Enum,
        _ => return None,
    })
}

/// A column reference, possibly under a widening integer cast (which does
/// not change the key encoding).
fn as_column(e: &Expr) -> Option<usize> {
    match e {
        Expr::Column(i) => Some(*i),
        Expr::Cast(inner, Type::Int4 | Type::Int8) => match **inner {
            Expr::Column(i) => Some(i),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Cmp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn flip(self) -> Cmp {
        match self {
            Cmp::Lt => Cmp::Gt,
            Cmp::Le => Cmp::Ge,
            Cmp::Gt => Cmp::Lt,
            Cmp::Ge => Cmp::Le,
            Cmp::Eq => Cmp::Eq,
        }
    }
}

/// A conjunct of the form `column <cmp> value` usable as a key bound.
struct Sarg {
    conjunct: usize,
    column: usize,
    cmp: Cmp,
    value: Expr,
}

impl<'a> Binder<'a> {
    /// Static type of a bound expression, where it can be determined.
    fn type_of(&self, e: &Expr, input: &[Type]) -> Option<Type> {
        Some(match e {
            Expr::Const(v) if !v.is_null() => v.type_of(),
            Expr::Column(i) => input.get(*i)?.clone(),
            Expr::Outer(depth, i) => {
                let level = self.outer_scopes.len().checked_sub(*depth)?;
                self.outer_scopes[level].cols.get(*i)?.ty.clone()
            }
            Expr::Param(i) => self.param_types.get(*i)?.clone()?,
            Expr::Cast(_, t) => t.clone(),
            Expr::EnumCast(_, t) => Type::Enum(t.name.clone()),
            Expr::Call(f, _) => f.ret.clone(),
            Expr::Coalesce(items) | Expr::MinMax(items, _) => self.type_of(items.first()?, input)?,
            Expr::Session(
                SessionFn::CurrentSetting
                | SessionFn::CurrentUser
                | SessionFn::CurrentSchema
                | SessionFn::CurrentDatabase
                | SessionFn::Version,
                _,
            ) => Type::Text,
            Expr::Session(SessionFn::BackendPid, _) => Type::Int4,
            _ => return None,
        })
    }

    fn sarg(&self, conjunct: usize, e: &Expr, types: &[Type]) -> Option<Sarg> {
        let Expr::Call(f, args) = e else { return None };
        let cmp = match &*f.name {
            "=" => Cmp::Eq,
            "<" => Cmp::Lt,
            "<=" => Cmp::Le,
            ">" => Cmp::Gt,
            ">=" => Cmp::Ge,
            _ => return None,
        };
        let [a, b] = args.as_slice() else { return None };
        let try_side = |col: &Expr, val: &Expr, cmp: Cmp| -> Option<Sarg> {
            let column = as_column(col)?;
            if val.uses_row() || val.is_volatile() {
                return None;
            }
            let col_class = key_class(types.get(column)?)?;
            let val_class = key_class(&self.type_of(val, types)?)?;
            (col_class == val_class).then(|| Sarg { conjunct, column, cmp, value: val.clone() })
        };
        try_side(a, b, cmp).or_else(|| try_side(b, a, cmp.flip()))
    }

    /// Chooses the primary key or an index for a scan from its filter.
    fn choose_access_path(&self, mut scan: ScanPlan, types: &[Type]) -> ScanPlan {
        let Some(filter) = scan.filter.take() else { return scan };
        let mut conj = Vec::new();
        conjuncts(filter, &mut conj);
        let sargs: Vec<Sarg> = conj.iter().enumerate().filter_map(|(i, c)| self.sarg(i, c, types)).collect();

        // Candidate keys: the primary key and every secondary index.
        struct Choice {
            index: Option<usize>,
            eq: Vec<usize>,
            lower: Option<usize>,
            upper: Option<usize>,
            score: (bool, usize, bool, bool),
        }
        let mut best: Option<Choice> = None;
        let candidates = std::iter::once((None, scan.table.pk.as_slice(), true)).chain(
            scan.table
                .indexes
                .iter()
                .enumerate()
                .filter(|(_, ix)| !ix.primary)
                .map(|(i, ix)| (Some(i), ix.columns.as_slice(), ix.unique)),
        );
        for (index, key_cols, unique) in candidates {
            let mut eq = Vec::new();
            for col in key_cols {
                match sargs.iter().position(|s| s.column == *col && s.cmp == Cmp::Eq) {
                    Some(s) => eq.push(s),
                    None => break,
                }
            }
            let (mut lower, mut upper) = (None, None);
            if let Some(next) = key_cols.get(eq.len()) {
                lower = sargs.iter().position(|s| s.column == *next && matches!(s.cmp, Cmp::Gt | Cmp::Ge));
                upper = sargs.iter().position(|s| s.column == *next && matches!(s.cmp, Cmp::Lt | Cmp::Le));
            }
            if eq.is_empty() && lower.is_none() && upper.is_none() {
                continue;
            }
            let full_unique = unique && eq.len() == key_cols.len();
            let score = (full_unique, eq.len(), lower.is_some() || upper.is_some(), index.is_none());
            if best.as_ref().is_none_or(|b| score > b.score) {
                best = Some(Choice { index, eq, lower, upper, score });
            }
        }

        if let Some(choice) = best {
            let mut used = vec![false; conj.len()];
            scan.index = choice.index;
            for s in &choice.eq {
                used[sargs[*s].conjunct] = true;
                scan.eq.push(sargs[*s].value.clone());
            }
            if let Some(s) = choice.lower {
                used[sargs[s].conjunct] = true;
                scan.lower = Some((sargs[s].value.clone(), sargs[s].cmp == Cmp::Ge));
            }
            if let Some(s) = choice.upper {
                used[sargs[s].conjunct] = true;
                scan.upper = Some((sargs[s].value.clone(), sargs[s].cmp == Cmp::Le));
            }
            let mut i = 0;
            conj.retain(|_| {
                i += 1;
                !used[i - 1]
            });
        }
        scan.filter = and_all(conj);
        scan
    }

    /// Pushes a predicate into a plan tree built from FROM items, choosing
    /// access paths and join methods on the way. `types` are the types of
    /// the plan's output columns.
    pub(crate) fn push_filter(&mut self, plan: Plan, pred: Option<Expr>, types: &[Type]) -> Result<Plan> {
        let mut preds = Vec::new();
        if let Some(p) = pred {
            conjuncts(p, &mut preds);
        }
        // A condition that can never hold makes the result empty without
        // evaluating the other conditions, which might fail.
        if preds.iter().any(cannot_be_true) {
            preds = vec![Expr::Const(crate::types::Value::Bool(false))];
        }
        Ok(self.push(plan, preds, types))
    }

    fn push(&mut self, plan: Plan, mut preds: Vec<Expr>, types: &[Type]) -> Plan {
        match plan {
            // A condition already in place (a row-level security policy)
            // stays ahead of the pushed ones, so that they only see rows
            // it lets through.
            Plan::Scan(mut scan) => {
                let mut all = Vec::new();
                if let Some(f) = scan.filter.take() {
                    conjuncts(f, &mut all);
                }
                all.append(&mut preds);
                scan.filter = and_all(all);
                Plan::Scan(self.choose_access_path(scan, types))
            }
            Plan::Filter(input, f) => {
                let mut all = Vec::new();
                conjuncts(f, &mut all);
                all.append(&mut preds);
                self.push(*input, all, types)
            }
            plan @ Plan::NestedLoop { kind: JoinKind::Inner, lateral: false, index_join: false, .. }
                if inner_join_leaves(&plan) >= 3 =>
            {
                self.push_reordered(plan, preds, types)
            }
            Plan::NestedLoop { left, right, kind, on, lateral, index_join: false, right_width } => {
                let left_width = types.len() - right_width;
                let (ltypes, rtypes) = types.split_at(left_width);
                let mut on_conj = Vec::new();
                if let Some(on) = on {
                    conjuncts(on, &mut on_conj);
                }

                enum Side {
                    Left,
                    Right,
                    Both,
                }
                let side = |e: &Expr| {
                    if opaque(e) {
                        return Side::Both;
                    }
                    match column_range(e) {
                        Some((_, hi)) if hi < left_width => Side::Left,
                        Some((lo, _)) if lo >= left_width => Side::Right,
                        // Row-independent predicates filter the left input.
                        None => Side::Left,
                        _ => Side::Both,
                    }
                };

                let (mut lpreds, mut rpreds, mut join, mut above) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                // Predicates from above (WHERE).
                for p in preds {
                    match (kind, side(&p)) {
                        (JoinKind::Inner, Side::Left) | (JoinKind::Left, Side::Left) => lpreds.push(p),
                        (JoinKind::Inner, Side::Right) | (JoinKind::Right, Side::Right) => {
                            rpreds.push(shift_down(p, left_width))
                        }
                        (JoinKind::Inner, Side::Both) => join.push(p),
                        _ => above.push(p),
                    }
                }
                // Predicates of the ON clause.
                for p in on_conj {
                    match (kind, side(&p)) {
                        (JoinKind::Inner, Side::Left) | (JoinKind::Right, Side::Left) => lpreds.push(p),
                        (JoinKind::Inner, Side::Right) | (JoinKind::Left, Side::Right) => {
                            rpreds.push(shift_down(p, left_width))
                        }
                        _ => join.push(p),
                    }
                }
                // A lateral right side is re-evaluated per left row; its
                // own predicates were bound when it was built.
                if lateral {
                    join.extend(rpreds.drain(..).map(|p| p.shift_columns(left_width)));
                }

                let left = self.push(*left, lpreds, ltypes);
                let right = if lateral { *right } else { self.push(*right, rpreds, rtypes) };
                let joined =
                    self.choose_join(left, right, kind, join, lateral, left_width, right_width, ltypes, rtypes);
                match and_all(above) {
                    Some(f) => Plan::Filter(Box::new(joined), f),
                    None => joined,
                }
            }
            other => match and_all(preds) {
                Some(f) => Plan::Filter(Box::new(other), f),
                None => other,
            },
        }
    }

    /// Plans a chain of three or more inner joins. The relations are
    /// joined in an order in which every step is connected to what was
    /// joined before by a join predicate wherever possible, rather than in
    /// the order they are written: `FROM a, b, c WHERE a.x = c.x AND
    /// c.y = b.y` must not start with the cross product of `a` and `b`.
    /// A final projection restores the written column order.
    fn push_reordered(&mut self, plan: Plan, mut preds: Vec<Expr>, types: &[Type]) -> Plan {
        let mut leaves: Vec<(Plan, usize, usize)> = Vec::new();
        flatten_inner_joins(plan, 0, types.len(), &mut leaves, &mut preds);
        let n = leaves.len();
        let leaf_of = |col: usize| leaves.iter().position(|(_, start, width)| col >= *start && col < start + width);

        // Predicates over one relation are pushed into it, predicates over
        // several become join conditions, and predicates that can only be
        // evaluated on the complete row stay on top.
        let mut local: Vec<Vec<Expr>> = vec![Vec::new(); n];
        let mut joins: Vec<(Expr, Vec<usize>, bool)> = Vec::new();
        let mut top: Vec<Expr> = Vec::new();
        for p in preds {
            if opaque(&p) {
                top.push(p);
                continue;
            }
            let mut used: Vec<usize> = Vec::new();
            let mut unknown = false;
            p.walk(&mut |e| {
                if let Expr::Column(c) = e {
                    match leaf_of(*c) {
                        Some(l) if !used.contains(&l) => used.push(l),
                        Some(_) => {}
                        None => unknown = true,
                    }
                }
            });
            match used.as_slice() {
                _ if unknown => top.push(p),
                [] => local[0].push(p),
                [l] => {
                    let start = leaves[*l].1;
                    local[*l].push(shift_down(p, start));
                }
                _ => {
                    let equi = matches!(&p, Expr::Call(f, args) if &*f.name == "=" && args.len() == 2);
                    joins.push((p, used, equi));
                }
            }
        }

        let mut planned: Vec<Option<Plan>> = Vec::with_capacity(n);
        let mut rank: Vec<u8> = Vec::with_capacity(n);
        let spans: Vec<(usize, usize)> = leaves.iter().map(|(_, s, w)| (*s, *w)).collect();
        for ((leaf, start, width), preds) in leaves.into_iter().zip(local) {
            let filtered = !preds.is_empty();
            let plan = self.push(leaf, preds, &types[start..start + width]);
            // How selective the relation looks, for choosing where to start.
            rank.push(match &plan {
                Plan::Scan(s) if s.is_unique_lookup() => 3,
                Plan::Scan(s) if !s.eq.is_empty() || s.lower.is_some() || s.upper.is_some() => 2,
                _ if filtered => 1,
                _ => 0,
            });
            planned.push(Some(plan));
        }

        let first = (0..n).max_by_key(|&i| (rank[i], std::cmp::Reverse(i))).unwrap_or(0);
        let mut joined = vec![false; n];
        let mut new_pos = vec![usize::MAX; types.len()];
        let mut cur_types: Vec<Type> = Vec::with_capacity(types.len());
        let place = |leaf: usize, new_pos: &mut Vec<usize>, cur_types: &mut Vec<Type>| {
            let (start, width) = spans[leaf];
            for c in start..start + width {
                new_pos[c] = cur_types.len();
                cur_types.push(types[c].clone());
            }
        };
        joined[first] = true;
        place(first, &mut new_pos, &mut cur_types);
        let mut current = planned[first].take().expect("leaf planned once");
        let mut order = vec![first];
        while order.len() < n {
            // Prefer a relation connected by an equality, then by any
            // predicate, then the most selective one that is left.
            let connected = |k: usize, equi_only: bool| {
                joins.iter().any(|(_, used, equi)| {
                    (*equi || !equi_only) && used.contains(&k) && used.iter().all(|u| *u == k || joined[*u])
                })
            };
            let remaining = (0..n).filter(|k| !joined[*k]);
            let next = remaining
                .max_by_key(|&k| (connected(k, true), connected(k, false), rank[k], std::cmp::Reverse(k)))
                .expect("a relation is left");
            let left_width = cur_types.len();
            let left_types = cur_types.clone();
            joined[next] = true;
            place(next, &mut new_pos, &mut cur_types);
            let mut conds = Vec::new();
            let mut i = 0;
            while i < joins.len() {
                if joins[i].1.iter().all(|u| joined[*u]) {
                    let (p, _, _) = joins.remove(i);
                    conds.push(remap_columns(p, &new_pos));
                } else {
                    i += 1;
                }
            }
            let right = planned[next].take().expect("leaf planned once");
            let (start, width) = spans[next];
            current = self.choose_join(
                current,
                right,
                JoinKind::Inner,
                conds,
                false,
                left_width,
                width,
                &left_types,
                &types[start..start + width],
            );
            order.push(next);
        }
        if order.iter().enumerate().any(|(i, k)| i != *k) {
            current = Plan::Project(Box::new(current), new_pos.iter().map(|p| Expr::Column(*p)).collect());
        }
        match and_all(top) {
            Some(f) => Plan::Filter(Box::new(current), f),
            None => current,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn choose_join(
        &mut self,
        left: Plan,
        right: Plan,
        kind: JoinKind,
        conds: Vec<Expr>,
        lateral: bool,
        left_width: usize,
        right_width: usize,
        ltypes: &[Type],
        rtypes: &[Type],
    ) -> Plan {
        let nested = |left: Plan, right: Plan, on: Vec<Expr>| Plan::NestedLoop {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            on: and_all(on),
            lateral,
            index_join: false,
            right_width,
        };
        if lateral {
            return nested(left, right, conds);
        }

        // Equi-join conditions `left expr = right expr`.
        let mut equi: Vec<(usize, Expr, Expr)> = Vec::new();
        for (i, c) in conds.iter().enumerate() {
            let Expr::Call(f, args) = c else { continue };
            if &*f.name != "=" || args.len() != 2 || opaque(c) {
                continue;
            }
            let (a, b) = (&args[0], &args[1]);
            let classes_match = |l: &Expr, r: &Expr| {
                let lt = self.type_of(l, ltypes).as_ref().and_then(key_class);
                let rt = self.type_of(&shift_down(r.clone(), left_width), rtypes).as_ref().and_then(key_class);
                lt.is_some() && lt == rt
            };
            match (column_range(a), column_range(b)) {
                (Some((_, ahi)), Some((blo, _))) if ahi < left_width && blo >= left_width && classes_match(a, b) => {
                    equi.push((i, a.clone(), shift_down(b.clone(), left_width)))
                }
                (Some((alo, _)), Some((_, bhi))) if bhi < left_width && alo >= left_width && classes_match(b, a) => {
                    equi.push((i, b.clone(), shift_down(a.clone(), left_width)))
                }
                _ => {}
            }
        }
        if equi.is_empty() {
            // RIGHT and FULL joins must emit unmatched right rows padded to
            // the left width, which only the hash join node knows. Without
            // keys it degenerates into a nested loop over one bucket.
            if matches!(kind, JoinKind::Right | JoinKind::Full) {
                return Plan::HashJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                    left_keys: Vec::new(),
                    right_keys: Vec::new(),
                    filter: and_all(conds),
                    left_width,
                    right_width,
                };
            }
            return nested(left, right, conds);
        }

        // Index nested loop: the right side is a base table whose key
        // starts with the join columns.
        if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Semi | JoinKind::Anti) {
            if let Plan::Scan(scan) = &right {
                if scan.eq.is_empty() && scan.lower.is_none() && scan.upper.is_none() {
                    let candidates = std::iter::once((None, scan.table.pk.as_slice())).chain(
                        scan.table
                            .indexes
                            .iter()
                            .enumerate()
                            .filter(|(_, ix)| !ix.primary)
                            .map(|(i, ix)| (Some(i), ix.columns.as_slice())),
                    );
                    let mut best: Option<(Option<usize>, Vec<usize>)> = None;
                    for (index, key_cols) in candidates {
                        let mut matched = Vec::new();
                        for col in key_cols {
                            match equi.iter().position(|(_, _, r)| as_column(r) == Some(*col)) {
                                Some(e) => matched.push(e),
                                None => break,
                            }
                        }
                        if !matched.is_empty() && best.as_ref().is_none_or(|(_, b)| matched.len() > b.len()) {
                            best = Some((index, matched));
                        }
                    }
                    if let Some((index, matched)) = best {
                        let mut scan = scan.clone();
                        scan.index = index;
                        let mut used = vec![false; conds.len()];
                        for m in &matched {
                            let (ci, lexpr, _) = &equi[*m];
                            used[*ci] = true;
                            let key = lexpr
                                .clone()
                                .rewrite(&mut |n| {
                                    Ok(if let Expr::Column(i) = n { Some(Expr::NlLeft(*i)) } else { None })
                                })
                                .expect("infallible");
                            scan.eq.push(key);
                        }
                        let rest: Vec<Expr> =
                            conds.into_iter().enumerate().filter(|(i, _)| !used[*i]).map(|(_, c)| c).collect();
                        return Plan::NestedLoop {
                            left: Box::new(left),
                            right: Box::new(Plan::Scan(scan)),
                            kind,
                            on: and_all(rest),
                            lateral: false,
                            index_join: true,
                            right_width,
                        };
                    }
                }
            }
        }

        let mut used = vec![false; conds.len()];
        let (mut left_keys, mut right_keys) = (Vec::new(), Vec::new());
        for (ci, l, r) in equi {
            used[ci] = true;
            left_keys.push(l);
            right_keys.push(r);
        }
        let rest: Vec<Expr> = conds.into_iter().enumerate().filter(|(i, _)| !used[*i]).map(|(_, c)| c).collect();
        Plan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            left_keys,
            right_keys,
            filter: and_all(rest),
            left_width,
            right_width,
        }
    }

    /// Adds a sort, unless the plan is a scan that can deliver the order
    /// directly from its key.
    pub(crate) fn sort_or_use_order(&mut self, plan: Plan, keys: Vec<SortKey>, has_limit: bool, scope: &Scope) -> Plan {
        let _ = scope;
        let Plan::Scan(scan) = plan else { return Plan::Sort { input: Box::new(plan), keys } };
        match order_from_scan(&scan, &keys, has_limit) {
            Some(scan) => Plan::Scan(scan),
            None => Plan::Sort { input: Box::new(Plan::Scan(scan)), keys },
        }
    }
}

fn is_const_null_or(e: &Expr, b: bool) -> bool {
    matches!(e, Expr::Const(crate::types::Value::Null)) || *e == Expr::Const(crate::types::Value::Bool(b))
}

/// True if the predicate is known, from its constants alone, to be false
/// or NULL for every row.
pub(crate) fn cannot_be_true(e: &Expr) -> bool {
    match e {
        Expr::And(items) => items.iter().any(cannot_be_true),
        Expr::Or(items) => items.iter().all(cannot_be_true),
        Expr::Not(inner) => cannot_be_false(inner),
        // `x NOT IN (..., NULL)` is false or NULL.
        Expr::InList(_, items, true) => items.iter().any(|i| matches!(i, Expr::Const(crate::types::Value::Null))),
        other => is_const_null_or(other, false),
    }
}

/// True if the predicate is known to be true or NULL for every row.
fn cannot_be_false(e: &Expr) -> bool {
    match e {
        Expr::Or(items) => items.iter().any(cannot_be_false),
        Expr::And(items) => items.iter().all(cannot_be_false),
        Expr::Not(inner) => cannot_be_true(inner),
        Expr::InList(_, items, false) => items.iter().any(|i| matches!(i, Expr::Const(crate::types::Value::Null))),
        other => is_const_null_or(other, true),
    }
}

/// Number of relations joined by a tree of plain inner joins.
fn inner_join_leaves(plan: &Plan) -> usize {
    match plan {
        Plan::NestedLoop { left, right, kind: JoinKind::Inner, lateral: false, index_join: false, .. } => {
            inner_join_leaves(left) + inner_join_leaves(right)
        }
        _ => 1,
    }
}

/// Takes a tree of plain inner joins apart into its relations, each with
/// the position and number of its columns in the joined row, and collects
/// the ON conditions (rewritten to positions in the joined row).
fn flatten_inner_joins(
    plan: Plan,
    offset: usize,
    width: usize,
    leaves: &mut Vec<(Plan, usize, usize)>,
    conds: &mut Vec<Expr>,
) {
    match plan {
        // A correlated subquery in an ON clause addresses the columns of
        // its own join by position, so such a join can only be taken
        // apart where those positions equal the ones in the complete row.
        Plan::NestedLoop { left, right, kind: JoinKind::Inner, on, lateral: false, index_join: false, right_width }
            if offset == 0 || !on.as_ref().is_some_and(opaque) =>
        {
            let left_width = width - right_width;
            flatten_inner_joins(*left, offset, left_width, leaves, conds);
            flatten_inner_joins(*right, offset + left_width, right_width, leaves, conds);
            if let Some(on) = on {
                conjuncts(on.shift_columns(offset), conds);
            }
        }
        other => leaves.push((other, offset, width)),
    }
}

fn remap_columns(e: Expr, new_pos: &[usize]) -> Expr {
    e.rewrite(&mut |n| Ok(if let Expr::Column(i) = n { Some(Expr::Column(new_pos[*i])) } else { None }))
        .expect("infallible")
}

/// Returns the scan adjusted to produce rows in the order of `keys`, if its
/// key (or, for an unconstrained scan under LIMIT, some index) allows it.
fn order_from_scan(scan: &ScanPlan, keys: &[SortKey], has_limit: bool) -> Option<ScanPlan> {
    let table = &scan.table;
    let desc = keys.first()?.desc;
    // Key order puts NULLs first ascending; that only equals the SQL
    // default (NULLs last ascending) for columns that cannot be NULL.
    let mut cols = Vec::with_capacity(keys.len());
    for k in keys {
        let Expr::Column(c) = k.expr else { return None };
        if k.desc != desc || k.nulls_first != desc || !table.columns[c].not_null {
            return None;
        }
        cols.push(c);
    }
    let matches = |key_cols: &[usize], skip: usize, unique: bool| -> bool {
        let rest = &key_cols[skip.min(key_cols.len())..];
        if cols.len() <= rest.len() {
            return rest[..cols.len()] == cols[..];
        }
        // Sorting by a whole unique key and then more is still that order.
        unique && cols[..rest.len()] == *rest
    };
    let current_unique = scan.index_def().is_none_or(|ix| ix.unique);
    if matches(scan.key_columns(), scan.eq.len(), current_unique) {
        let mut scan = scan.clone();
        scan.reverse = desc;
        return Some(scan);
    }
    // An unconstrained scan may switch to an index that provides the
    // order, which pays off when only the first rows are needed.
    if has_limit && scan.index.is_none() && scan.eq.is_empty() && scan.lower.is_none() && scan.upper.is_none() {
        for (i, ix) in table.indexes.iter().enumerate() {
            if !ix.primary && matches(&ix.columns, 0, ix.unique) {
                let mut scan = scan.clone();
                scan.index = Some(i);
                scan.reverse = desc;
                return Some(scan);
            }
        }
    }
    None
}
