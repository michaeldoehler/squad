//! LIKE, SIMILAR TO and regular-expression matching.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use regex::Regex;

use crate::error::sqlstate;
use crate::{Error, Result};

fn char_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

/// SQL LIKE with `\` as escape character. Allocation-free.
pub fn like_match(text: &str, pattern: &str) -> Result<bool> {
    let (t, p) = (text.as_bytes(), pattern.as_bytes());
    let (mut ti, mut pi) = (0usize, 0usize);
    // Position to resume from after the most recent `%`.
    let mut star: Option<(usize, usize)> = None;
    loop {
        if pi < p.len() {
            match p[pi] {
                b'%' => {
                    pi += 1;
                    star = Some((pi, ti));
                    continue;
                }
                b'_' if ti < t.len() => {
                    ti += char_len(t[ti]);
                    pi += 1;
                    continue;
                }
                b'\\' => {
                    if pi + 1 >= p.len() {
                        return Err(Error::new(
                            sqlstate::INVALID_ESCAPE_SEQUENCE,
                            "LIKE pattern must not end with escape character",
                        ));
                    }
                    if ti < t.len() && t[ti] == p[pi + 1] {
                        ti += 1;
                        pi += 2;
                        continue;
                    }
                }
                c if c != b'_' && ti < t.len() && t[ti] == c => {
                    ti += 1;
                    pi += 1;
                    continue;
                }
                _ => {}
            }
        } else if ti == t.len() {
            return Ok(true);
        }
        // Mismatch: let the last `%` swallow one more character.
        match star {
            Some((sp, st)) if st < t.len() => {
                let next = st + char_len(t[st]);
                star = Some((sp, next));
                pi = sp;
                ti = next;
            }
            _ => return Ok(false),
        }
    }
}

/// Rewrites a LIKE pattern that uses `escape` into one using `\`.
pub fn like_escape(pattern: &str, escape: &str) -> Result<String> {
    let mut chars = escape.chars();
    let esc = chars.next();
    if chars.next().is_some() {
        return Err(Error::new(sqlstate::INVALID_ESCAPE_SEQUENCE, "invalid escape string")
            .with_hint("Escape string must be empty or one character."));
    }
    let mut out = String::with_capacity(pattern.len());
    let mut it = pattern.chars();
    while let Some(c) = it.next() {
        match esc {
            Some(e) if c == e => {
                out.push('\\');
                if let Some(next) = it.next() {
                    out.push(next);
                }
            }
            _ if c == '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    Ok(out)
}

/// Converts a SQL `SIMILAR TO` pattern into an anchored POSIX regex.
pub fn similar_to_escape(pattern: &str, escape: Option<&str>) -> Result<String> {
    let esc = match escape {
        None => Some('\\'),
        Some(e) => {
            let mut c = e.chars();
            let first = c.next();
            if c.next().is_some() {
                return Err(Error::new(sqlstate::INVALID_ESCAPE_SEQUENCE, "invalid escape string")
                    .with_hint("Escape string must be empty or one character."));
            }
            first
        }
    };
    let mut out = String::from("^(?:");
    let mut it = pattern.chars();
    let mut in_bracket = false;
    while let Some(c) = it.next() {
        if Some(c) == esc {
            if let Some(next) = it.next() {
                out.push('\\');
                out.push(next);
            }
            continue;
        }
        if in_bracket {
            if c == ']' {
                in_bracket = false;
            }
            if c == '\\' {
                out.push('\\');
            }
            out.push(c);
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                in_bracket = true;
                out.push('[');
            }
            '.' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out.push_str(")$");
    Ok(out)
}

thread_local! {
    static CACHE: RefCell<HashMap<(String, String), Rc<Regex>>> = RefCell::new(HashMap::new());
}

/// Translates the few Postgres ARE spellings the `regex` crate lacks.
fn translate(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut it = pattern.chars().peekable();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('m') => out.push_str(r"\b"),
                Some('M') => out.push_str(r"\b"),
                Some('y') => out.push_str(r"\b"),
                Some('Y') => out.push_str(r"\B"),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Compiles (with caching) a Postgres regular expression. Flags: `i`
/// case-insensitive, `n`/`m` newline-sensitive, `s` dot matches newline
/// (default), `x` extended. `g` is handled by the callers.
pub fn regex(pattern: &str, flags: &str) -> Result<Rc<Regex>> {
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(r) = cache.get(&(pattern.to_string(), flags.to_string())) {
            return Ok(r.clone());
        }
        let mut prefix = String::from("(?s");
        let mut multi_line = false;
        for f in flags.chars() {
            match f {
                'i' => prefix.push('i'),
                'x' => prefix.push('x'),
                'n' | 'm' => multi_line = true,
                'c' | 's' | 'g' | 't' => {}
                other => {
                    return Err(Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        format!("invalid regular expression option: \"{other}\""),
                    ))
                }
            }
        }
        if multi_line {
            prefix = prefix.replace('s', "m");
        }
        prefix.push(')');
        let compiled = Regex::new(&format!("{prefix}{}", translate(pattern))).map_err(|e| {
            Error::new(sqlstate::INVALID_REGULAR_EXPRESSION, format!("invalid regular expression: {e}"))
        })?;
        if cache.len() >= 256 {
            cache.clear();
        }
        let rc = Rc::new(compiled);
        cache.insert((pattern.to_string(), flags.to_string()), rc.clone());
        Ok(rc)
    })
}
