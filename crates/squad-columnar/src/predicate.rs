//! Filter predicates: vectorized evaluation with SQL three-valued logic,
//! and pruning of row groups and segments by statistics.

use std::cmp::Ordering;

use crate::segment::ColumnStats;
use crate::types::{cmp_datum, cmp_f64, BytesColumn, Column, ColumnData, Datum};
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    #[inline]
    fn test(self, o: Ordering) -> bool {
        match self {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
        }
    }
}

/// A filter over the columns of a table (by position in its schema).
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `column op value`
    Cmp {
        column: usize,
        op: CmpOp,
        value: Datum,
    },
    /// `column IN (values)`
    In {
        column: usize,
        values: Vec<Datum>,
    },
    /// `column BETWEEN low AND high`
    Between {
        column: usize,
        low: Datum,
        high: Datum,
    },
    /// `column IS [NOT] NULL`
    IsNull {
        column: usize,
        negated: bool,
    },
    /// `column [I]LIKE pattern`, with `%`, `_` and `\` as escape.
    Like {
        column: usize,
        pattern: String,
        case_insensitive: bool,
    },
    /// A boolean column used as a condition.
    BoolColumn(usize),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    /// A constant: `Some(true)`, `Some(false)` or NULL.
    Const(Option<bool>),
}

/// Truth values, ordered so that AND is `min` and OR is `max`.
pub(crate) const FALSE: u8 = 0;
pub(crate) const UNKNOWN: u8 = 1;
pub(crate) const TRUE: u8 = 2;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Lit(Vec<u8>),
    AnySeq,
    AnyOne,
}

fn parse_like(pattern: &str) -> Vec<Tok> {
    let mut toks: Vec<Tok> = Vec::new();
    let mut lit = Vec::new();
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if i + 1 < bytes.len() => {
                lit.push(bytes[i + 1]);
                i += 1;
            }
            b'%' | b'_' => {
                if !lit.is_empty() {
                    toks.push(Tok::Lit(std::mem::take(&mut lit)));
                }
                let t = if bytes[i] == b'%' { Tok::AnySeq } else { Tok::AnyOne };
                if !(t == Tok::AnySeq && toks.last() == Some(&Tok::AnySeq)) {
                    toks.push(t);
                }
            }
            b => lit.push(b),
        }
        i += 1;
    }
    if !lit.is_empty() {
        toks.push(Tok::Lit(lit));
    }
    toks
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

fn like_match(toks: &[Tok], text: &[u8]) -> bool {
    match toks {
        [] => text.is_empty(),
        [Tok::Lit(l)] => text == l.as_slice(),
        [Tok::Lit(l), Tok::AnySeq] => text.starts_with(l),
        [Tok::AnySeq, Tok::Lit(l)] => text.ends_with(l),
        [Tok::AnySeq, Tok::Lit(l), Tok::AnySeq] => contains(text, l),
        [Tok::AnySeq] => true,
        [Tok::Lit(l), rest @ ..] => text.starts_with(l) && like_match(rest, &text[l.len()..]),
        [Tok::AnyOne, rest @ ..] => {
            // One character, which may be several UTF-8 bytes.
            if text.is_empty() {
                return false;
            }
            let mut n = 1;
            while n < text.len() && text[n] & 0xC0 == 0x80 {
                n += 1;
            }
            like_match(rest, &text[n..])
        }
        [Tok::AnySeq, rest @ ..] => (0..=text.len()).any(|i| like_match(rest, &text[i..])),
    }
}

fn mask(col: &Column, mut out: Vec<u8>) -> Vec<u8> {
    if let Some(v) = &col.validity {
        for (o, valid) in out.iter_mut().zip(v) {
            if !*valid {
                *o = UNKNOWN;
            }
        }
    }
    out
}

#[inline]
fn truth(b: bool) -> u8 {
    if b {
        TRUE
    } else {
        FALSE
    }
}

fn type_error(column: usize, value: &Datum) -> Error {
    Error::Type(format!("cannot compare column {column} with {value:?}"))
}

/// Per-row truth of `value-of-row op constant` for a bytes column, going
/// through the dictionary when there is one.
fn bytes_test(col: &BytesColumn, test: impl Fn(&[u8]) -> bool) -> Vec<u8> {
    match col {
        BytesColumn::Plain(d) => d.iter().map(|v| truth(test(v))).collect(),
        BytesColumn::Dict { codes, dict } => {
            let per_entry: Vec<u8> = dict.iter().map(|v| truth(test(v))).collect();
            codes.iter().map(|c| per_entry[*c as usize]).collect()
        }
    }
}

fn eval_cmp(col: &Column, column: usize, op: CmpOp, value: &Datum) -> Result<Vec<u8>> {
    let n = col.len();
    if value.is_null() {
        return Ok(vec![UNKNOWN; n]);
    }
    let out: Vec<u8> = match (&col.data, value) {
        (ColumnData::Int64(v), Datum::Int(c)) => {
            let c = *c;
            match op {
                CmpOp::Eq => v.iter().map(|x| truth(*x == c)).collect(),
                CmpOp::Ne => v.iter().map(|x| truth(*x != c)).collect(),
                CmpOp::Lt => v.iter().map(|x| truth(*x < c)).collect(),
                CmpOp::Le => v.iter().map(|x| truth(*x <= c)).collect(),
                CmpOp::Gt => v.iter().map(|x| truth(*x > c)).collect(),
                CmpOp::Ge => v.iter().map(|x| truth(*x >= c)).collect(),
            }
        }
        (ColumnData::Int64(v), Datum::Float(c)) => v.iter().map(|x| truth(op.test(cmp_f64(*x as f64, *c)))).collect(),
        (ColumnData::Float64(v), Datum::Float(_) | Datum::Int(_)) => {
            let c = value.as_f64().unwrap();
            v.iter().map(|x| truth(op.test(cmp_f64(*x, c)))).collect()
        }
        (ColumnData::Decimal(v), Datum::Decimal(c)) => v.iter().map(|x| truth(op.test(x.cmp(c)))).collect(),
        (ColumnData::Bool(v), Datum::Bool(c)) => v.iter().map(|x| truth(op.test(x.cmp(c)))).collect(),
        (ColumnData::Bytes(b), Datum::Bytes(c)) => bytes_test(b, |x| op.test(x.cmp(c.as_slice()))),
        _ => return Err(type_error(column, value)),
    };
    Ok(mask(col, out))
}

fn and_into(acc: &mut [u8], other: &[u8]) {
    for (a, b) in acc.iter_mut().zip(other) {
        *a = (*a).min(*b);
    }
}

fn or_into(acc: &mut [u8], other: &[u8]) {
    for (a, b) in acc.iter_mut().zip(other) {
        *a = (*a).max(*b);
    }
}

impl Predicate {
    /// Adds the columns this predicate reads to `out`.
    pub fn columns(&self, out: &mut Vec<usize>) {
        match self {
            Predicate::Cmp { column, .. }
            | Predicate::In { column, .. }
            | Predicate::Between { column, .. }
            | Predicate::IsNull { column, .. }
            | Predicate::Like { column, .. }
            | Predicate::BoolColumn(column) => {
                if !out.contains(column) {
                    out.push(*column);
                }
            }
            Predicate::And(ps) | Predicate::Or(ps) => ps.iter().for_each(|p| p.columns(out)),
            Predicate::Not(p) => p.columns(out),
            Predicate::Const(_) => {}
        }
    }

    /// Evaluates the predicate for `rows` rows. `columns` is indexed by
    /// schema position and must hold every column the predicate reads.
    /// Returns one truth value per row (see [`selection`]).
    pub(crate) fn evaluate(&self, columns: &[Option<Column>], rows: usize) -> Result<Vec<u8>> {
        let col = |i: usize| -> Result<&Column> {
            columns
                .get(i)
                .and_then(Option::as_ref)
                .ok_or_else(|| Error::Invalid(format!("predicate reads column {i}, which is not available")))
        };
        Ok(match self {
            Predicate::Const(c) => vec![c.map_or(UNKNOWN, truth); rows],
            Predicate::Cmp { column, op, value } => eval_cmp(col(*column)?, *column, *op, value)?,
            Predicate::Between { column, low, high } => {
                let c = col(*column)?;
                let mut out = eval_cmp(c, *column, CmpOp::Ge, low)?;
                and_into(&mut out, &eval_cmp(c, *column, CmpOp::Le, high)?);
                out
            }
            Predicate::In { column, values } => {
                let c = col(*column)?;
                let mut out = vec![FALSE; rows];
                for v in values {
                    or_into(&mut out, &eval_cmp(c, *column, CmpOp::Eq, v)?);
                }
                // An empty list is false even for NULL input.
                if values.is_empty() {
                    out
                } else {
                    mask(c, out)
                }
            }
            Predicate::IsNull { column, negated } => {
                let c = col(*column)?;
                match &c.validity {
                    None => vec![truth(*negated); rows],
                    Some(v) => v.iter().map(|valid| truth(*valid == *negated)).collect(),
                }
            }
            Predicate::Like { column, pattern, case_insensitive } => {
                let c = col(*column)?;
                let ColumnData::Bytes(b) = &c.data else {
                    return Err(Error::Type(format!("LIKE on column {column}, which is not text")));
                };
                let out = if *case_insensitive {
                    let toks = parse_like(&pattern.to_lowercase());
                    bytes_test(b, |x| like_match(&toks, String::from_utf8_lossy(x).to_lowercase().as_bytes()))
                } else {
                    let toks = parse_like(pattern);
                    bytes_test(b, |x| like_match(&toks, x))
                };
                mask(c, out)
            }
            Predicate::BoolColumn(column) => {
                let c = col(*column)?;
                let ColumnData::Bool(v) = &c.data else {
                    return Err(Error::Type(format!("column {column} is not boolean")));
                };
                mask(c, v.iter().map(|b| truth(*b)).collect())
            }
            Predicate::And(ps) => {
                let mut out = vec![TRUE; rows];
                for p in ps {
                    and_into(&mut out, &p.evaluate(columns, rows)?);
                }
                out
            }
            Predicate::Or(ps) => {
                let mut out = vec![FALSE; rows];
                for p in ps {
                    or_into(&mut out, &p.evaluate(columns, rows)?);
                }
                out
            }
            Predicate::Not(p) => {
                let mut out = p.evaluate(columns, rows)?;
                out.iter_mut().for_each(|x| *x = TRUE - *x);
                out
            }
        })
    }

    /// False if no row described by the statistics can satisfy the
    /// predicate. `stats(column)` returns `None` when nothing is known.
    pub fn may_match<'a>(&self, stats: &dyn Fn(usize) -> Option<&'a ColumnStats>) -> bool {
        let bounds = |column: usize| -> Option<(&'a ColumnStats, bool)> {
            let s = stats(column)?;
            Some((s, !s.min.is_null() && !s.max.is_null()))
        };
        let comparable = |a: &Datum, b: &Datum| {
            matches!(
                (a, b),
                (Datum::Int(_) | Datum::Float(_), Datum::Int(_) | Datum::Float(_))
                    | (Datum::Decimal(_), Datum::Decimal(_))
                    | (Datum::Bytes(_), Datum::Bytes(_))
                    | (Datum::Bool(_), Datum::Bool(_))
            )
        };
        let cmp_may = |column: usize, op: CmpOp, value: &Datum| -> bool {
            let Some((s, known)) = bounds(column) else { return true };
            if value.is_null() || s.all_null() {
                return false;
            }
            if !known || !comparable(&s.min, value) {
                return true;
            }
            let (lo, hi) = (cmp_datum(&s.min, value), cmp_datum(&s.max, value));
            match op {
                CmpOp::Eq => lo != Ordering::Greater && hi != Ordering::Less,
                CmpOp::Lt => lo == Ordering::Less,
                CmpOp::Le => lo != Ordering::Greater,
                CmpOp::Gt => hi == Ordering::Greater,
                CmpOp::Ge => hi != Ordering::Less,
                // Byte bounds may be shortened, so equality of both bounds
                // with the value proves nothing for them.
                CmpOp::Ne => matches!(value, Datum::Bytes(_)) || lo != Ordering::Equal || hi != Ordering::Equal,
            }
        };
        match self {
            Predicate::Const(c) => *c == Some(true),
            Predicate::Cmp { column, op, value } => cmp_may(*column, *op, value),
            Predicate::Between { column, low, high } => {
                cmp_may(*column, CmpOp::Ge, low) && cmp_may(*column, CmpOp::Le, high)
            }
            Predicate::In { column, values } => values.iter().any(|v| cmp_may(*column, CmpOp::Eq, v)),
            Predicate::IsNull { column, negated } => match stats(*column) {
                Some(s) if *negated => s.null_count < s.rows,
                Some(s) => s.null_count > 0,
                None => true,
            },
            Predicate::Like { column, pattern, case_insensitive } => {
                let Some((s, known)) = bounds(*column) else { return true };
                if s.all_null() {
                    return false;
                }
                let toks = parse_like(pattern);
                match (toks.first(), known, case_insensitive) {
                    (Some(Tok::Lit(prefix)), true, false) => {
                        let (Datum::Bytes(min), Datum::Bytes(max)) = (&s.min, &s.max) else { return true };
                        let min_head = &min[..min.len().min(prefix.len())];
                        min_head <= prefix.as_slice() && max.as_slice() >= prefix.as_slice()
                    }
                    _ => true,
                }
            }
            Predicate::BoolColumn(column) => cmp_may(*column, CmpOp::Eq, &Datum::Bool(true)),
            Predicate::And(ps) => ps.iter().all(|p| p.may_match(stats)),
            Predicate::Or(ps) => ps.iter().any(|p| p.may_match(stats)),
            // The negation of "may match" is not "cannot match".
            Predicate::Not(_) => true,
        }
    }

    /// Equality constraints that every matching row must satisfy:
    /// `(column, candidate values)`. Used to consult bloom filters.
    pub(crate) fn required_values(&self, out: &mut Vec<(usize, Vec<Datum>)>) {
        match self {
            Predicate::Cmp { column, op: CmpOp::Eq, value } => out.push((*column, vec![value.clone()])),
            Predicate::In { column, values } => out.push((*column, values.clone())),
            Predicate::And(ps) => ps.iter().for_each(|p| p.required_values(out)),
            _ => {}
        }
    }
}

/// Converts truth values into the indices of the rows that are true, or
/// `None` if every row is.
pub(crate) fn selection(truth_values: &[u8]) -> Option<Vec<u32>> {
    let selected = truth_values.iter().filter(|t| **t == TRUE).count();
    if selected == truth_values.len() {
        return None;
    }
    let mut out = Vec::with_capacity(selected);
    for (i, t) in truth_values.iter().enumerate() {
        if *t == TRUE {
            out.push(i as u32);
        }
    }
    Some(out)
}
