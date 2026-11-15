//! Function and operator tables and Postgres-style overload resolution.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::cast::{can_cast, category, common_type, is_preferred, CastContext, Category};
use crate::error::sqlstate;
use crate::{Error, FnCtx, Result, Type, Value};

/// How often a function may change its result for the same arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

pub type FuncImpl = fn(&FnCtx, &[Value]) -> Result<Value>;

/// A resolved function or operator, ready to be called.
#[derive(Clone)]
pub struct Func {
    pub name: String,
    /// Types the caller must coerce the actual arguments to.
    pub args: Vec<Type>,
    pub ret: Type,
    /// Return NULL without calling `eval` if any argument is NULL.
    pub strict: bool,
    pub volatility: Volatility,
    pub eval: FuncImpl,
}

impl std::fmt::Debug for Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?}) -> {:?}", self.name, self.args, self.ret)
    }
}

/// Declared parameter type.
#[derive(Debug, Clone, PartialEq)]
pub enum P {
    T(Type),
    /// `"any"`: accepts everything, no unification.
    Any,
    /// `anycompatible`
    Elem,
    /// `anycompatiblearray`
    Array,
    /// `anycompatiblenonarray`
    NonArray,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ret {
    T(Type),
    /// Array of the unified polymorphic element type.
    Array,
}

pub struct Sig {
    pub name: &'static str,
    pub params: Vec<P>,
    /// The last parameter repeats one or more times.
    pub variadic: bool,
    pub ret: Ret,
    pub strict: bool,
    pub vol: Volatility,
    pub eval: FuncImpl,
    /// Coerce the arguments to these types instead of the declared ones
    /// (cross-type operators implemented by a same-type function).
    pub coerce: Option<Vec<Type>>,
}

impl Sig {
    pub fn nonstrict(&mut self) -> &mut Sig {
        self.strict = false;
        self
    }
    pub fn stable(&mut self) -> &mut Sig {
        self.vol = Volatility::Stable;
        self
    }
    pub fn volatile(&mut self) -> &mut Sig {
        self.vol = Volatility::Volatile;
        self
    }
    pub fn variadic(&mut self) -> &mut Sig {
        self.variadic = true;
        self
    }
    pub fn coerce(&mut self, types: &[Type]) -> &mut Sig {
        self.coerce = Some(types.to_vec());
        self
    }
}

#[derive(Default)]
pub struct Registry {
    pub funcs: HashMap<&'static str, Vec<Sig>>,
    pub ops: HashMap<&'static str, Vec<Sig>>,
}

fn new_sig(name: &'static str, params: &[P], ret: Ret, eval: FuncImpl) -> Sig {
    Sig {
        name,
        params: params.to_vec(),
        variadic: false,
        ret,
        strict: true,
        vol: Volatility::Immutable,
        eval,
        coerce: None,
    }
}

impl Registry {
    /// Registers a strict, immutable function; adjust with the builder
    /// methods on the returned signature.
    pub fn f(&mut self, name: &'static str, params: &[P], ret: Ret, eval: FuncImpl) -> &mut Sig {
        let list = self.funcs.entry(name).or_default();
        list.push(new_sig(name, params, ret, eval));
        list.last_mut().unwrap()
    }

    pub fn op(&mut self, name: &'static str, params: &[P], ret: Ret, eval: FuncImpl) -> &mut Sig {
        let list = self.ops.entry(name).or_default();
        list.push(new_sig(name, params, ret, eval));
        list.last_mut().unwrap()
    }
}

pub fn registry() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| {
        let mut r = Registry::default();
        super::ops::register(&mut r);
        super::f_string::register(&mut r);
        super::f_math::register(&mut r);
        super::f_datetime::register(&mut r);
        super::f_json::register(&mut r);
        super::f_array::register(&mut r);
        r
    })
}

struct Match<'a> {
    sig: &'a Sig,
    args: Vec<Type>,
    ret: Type,
    exact: usize,
    preferred: usize,
}

fn try_match<'a>(sig: &'a Sig, actual: &[Type]) -> Option<Match<'a>> {
    let n = actual.len();
    if sig.variadic {
        if n < sig.params.len() {
            return None;
        }
    } else if n != sig.params.len() {
        return None;
    }
    let param = |i: usize| -> &P {
        if i < sig.params.len() {
            &sig.params[i]
        } else {
            sig.params.last().unwrap()
        }
    };
    // Unify the polymorphic parameters.
    let mut elems: Vec<Type> = Vec::new();
    let mut has_poly = false;
    for (i, a) in actual.iter().enumerate() {
        match param(i) {
            P::Elem => {
                has_poly = true;
                if *a != Type::Unknown {
                    elems.push(a.clone());
                }
            }
            P::NonArray => {
                has_poly = true;
                match a {
                    Type::Array(_) | Type::Int2Vector | Type::OidVector => return None,
                    Type::Unknown => {}
                    t => elems.push(t.clone()),
                }
            }
            P::Array => {
                has_poly = true;
                match a {
                    Type::Array(e) => {
                        if **e != Type::Unknown {
                            elems.push((**e).clone())
                        }
                    }
                    Type::Unknown => {}
                    v if v.is_vector() => elems.extend(v.vector_elem()),
                    _ => return None,
                }
            }
            _ => {}
        }
    }
    let elem = if !has_poly {
        Type::Unknown
    } else if elems.is_empty() {
        Type::Text
    } else {
        common_type(&elems, "").ok()?
    };
    let mut args = Vec::with_capacity(n);
    let mut exact = 0;
    let mut preferred = 0;
    for (i, a) in actual.iter().enumerate() {
        let declared = param(i);
        let target = match declared {
            P::T(t) => t.clone(),
            P::Any => {
                if *a != Type::Unknown {
                    exact += 1;
                    args.push(a.clone());
                } else {
                    args.push(Type::Text);
                }
                continue;
            }
            P::Elem | P::NonArray => elem.clone(),
            // A vector is passed as it is, so that functions see its lower
            // bound of 0. Only a function that returns an array of a wider
            // element type gets it converted to such an array.
            P::Array if a.is_vector() && (a.vector_elem().as_ref() == Some(&elem) || sig.ret != Ret::Array) => {
                a.clone()
            }
            P::Array => elem.clone().array_of(),
        };
        if *a == target {
            exact += 1;
        } else if can_cast(a, &target, CastContext::Implicit) {
            // Only concrete parameter types count as preferred matches.
            if matches!(declared, P::T(_)) && is_preferred(&target) {
                preferred += 1;
            }
        } else {
            return None;
        }
        args.push(target);
    }
    let ret = match &sig.ret {
        Ret::T(t) => t.clone(),
        Ret::Array => elem.array_of(),
    };
    if let Some(c) = &sig.coerce {
        args = c.clone();
    }
    Some(Match { sig, args, ret, exact, preferred })
}

fn finish(m: Match<'_>) -> Func {
    Func {
        name: m.sig.name.to_string(),
        args: m.args,
        ret: m.ret,
        strict: m.sig.strict,
        volatility: m.sig.vol,
        eval: m.sig.eval,
    }
}

enum Pick<'a> {
    One(Match<'a>),
    None,
    Ambiguous,
}

fn select<'a>(sigs: &'a [Sig], actual: &[Type]) -> Pick<'a> {
    let mut cands: Vec<Match<'a>> = sigs.iter().filter_map(|s| try_match(s, actual)).collect();
    if cands.is_empty() {
        return Pick::None;
    }
    let n = actual.len();
    if let Some(i) = cands.iter().position(|m| m.exact == n) {
        return Pick::One(cands.swap_remove(i));
    }
    let best = cands.iter().map(|m| m.exact).max().unwrap();
    cands.retain(|m| m.exact == best);
    if cands.len() > 1 {
        let best = cands.iter().map(|m| m.preferred).max().unwrap();
        cands.retain(|m| m.preferred == best);
    }
    // The declared parameter types decide from here on, not the coercion
    // targets of cross-type operators.
    fn declared(m: &Match<'_>, i: usize) -> Type {
        match m.sig.params.get(i).or(m.sig.params.last()) {
            Some(P::T(t)) => t.clone(),
            _ => m.args[i].clone(),
        }
    }
    if cands.len() > 1 {
        for (i, a) in actual.iter().enumerate() {
            if *a != Type::Unknown || cands.len() == 1 {
                continue;
            }
            let cats: Vec<Category> = cands.iter().map(|m| category(&declared(m, i))).collect();
            let chosen = if cats.contains(&Category::String) {
                Category::String
            } else if cats.iter().all(|c| *c == cats[0]) {
                cats[0]
            } else {
                return Pick::Ambiguous;
            };
            cands.retain(|m| category(&declared(m, i)) == chosen);
            if cands.iter().any(|m| is_preferred(&declared(m, i))) {
                cands.retain(|m| is_preferred(&declared(m, i)));
            }
        }
    }
    if cands.len() > 1 {
        let known: Vec<&Type> = actual.iter().filter(|t| **t != Type::Unknown).collect();
        if !known.is_empty() && known.len() < n && known.iter().all(|t| *t == known[0]) {
            let t = known[0];
            let matching: Vec<usize> = (0..cands.len())
                .filter(|&c| {
                    actual.iter().enumerate().all(|(i, a)| *a != Type::Unknown || declared(&cands[c], i) == *t)
                })
                .collect();
            if matching.len() == 1 {
                return Pick::One(cands.swap_remove(matching[0]));
            }
        }
    }
    if cands.len() == 1 {
        Pick::One(cands.pop().unwrap())
    } else {
        Pick::Ambiguous
    }
}

fn type_list(types: &[Type]) -> String {
    types.iter().map(Type::name).collect::<Vec<_>>().join(", ")
}

/// Resolves a function call by name and argument types.
pub fn resolve_function(name: &str, args: &[Type]) -> Result<Func> {
    let lower;
    let name = if name.bytes().any(|b| b.is_ascii_uppercase()) {
        lower = name.to_ascii_lowercase();
        &lower
    } else {
        name
    };
    let missing = || {
        Error::new(sqlstate::UNDEFINED_FUNCTION, format!("function {name}({}) does not exist", type_list(args)))
            .with_hint(
                "No function matches the given name and argument types. You might need to add explicit type casts.",
            )
    };
    let sigs = registry().funcs.get(name).ok_or_else(missing)?;
    match select(sigs, args) {
        Pick::One(m) => Ok(finish(m)),
        Pick::None => Err(missing()),
        Pick::Ambiguous => {
            Err(Error::new(sqlstate::AMBIGUOUS_FUNCTION, format!("function {name}({}) is not unique", type_list(args)))
                .with_hint("Could not choose a best candidate function. You might need to add explicit type casts."))
        }
    }
}

/// True if a function of this name exists with any signature.
pub fn function_exists(name: &str) -> bool {
    registry().funcs.contains_key(name.to_ascii_lowercase().as_str())
}

const COMPARISONS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// Resolves an operator; `left` is `None` for prefix operators.
pub fn resolve_operator(op: &str, left: Option<&Type>, right: &Type) -> Result<Func> {
    let op = if op == "!=" { "<>" } else { op };
    let actual: Vec<Type> = match left {
        Some(l) => vec![l.clone(), right.clone()],
        None => vec![right.clone()],
    };
    let describe = || match left {
        Some(l) => format!("{} {op} {}", l.name(), right.name()),
        None => format!("{op} {}", right.name()),
    };
    let missing = || {
        Error::new(sqlstate::UNDEFINED_FUNCTION, format!("operator does not exist: {}", describe())).with_hint(
            "No operator matches the given name and argument types. You might need to add explicit type casts.",
        )
    };
    // Enums and records compare through the generic ordering.
    if let (Some(l), true) = (left, COMPARISONS.contains(&op)) {
        let enum_ty = match (l, right) {
            (Type::Enum(a), Type::Enum(b)) if a == b => Some(l.clone()),
            (Type::Enum(_), Type::Unknown) => Some(l.clone()),
            (Type::Unknown, Type::Enum(_)) => Some(right.clone()),
            (Type::Record, Type::Record) => Some(Type::Record),
            _ => None,
        };
        if let Some(t) = enum_ty {
            let eval = super::ops::generic_comparison(op);
            return Ok(Func {
                name: op.to_string(),
                args: vec![t.clone(), t],
                ret: Type::Bool,
                strict: true,
                volatility: Volatility::Immutable,
                eval,
            });
        }
    }
    let sigs = registry().ops.get(op).ok_or_else(missing)?;
    // An unknown literal next to a typed operand first tries to take the
    // operand's type.
    if let Some(l) = left {
        let other = match (l, right) {
            (Type::Unknown, r) if *r != Type::Unknown => Some(r),
            (l, Type::Unknown) if *l != Type::Unknown => Some(l),
            _ => None,
        };
        if let Some(t) = other {
            let assumed = [t.clone(), t.clone()];
            if let Some(m) = sigs.iter().filter_map(|s| try_match(s, &assumed)).find(|m| m.exact == 2) {
                return Ok(finish(m));
            }
        }
    }
    match select(sigs, &actual) {
        Pick::One(m) => Ok(finish(m)),
        Pick::None => Err(missing()),
        Pick::Ambiguous => {
            Err(Error::new(sqlstate::AMBIGUOUS_FUNCTION, format!("operator is not unique: {}", describe()))
                .with_hint("Could not choose a best candidate operator. You might need to add explicit type casts."))
        }
    }
}
