//! `to_char`, `to_timestamp`, `to_date` and `to_number` template handling.

use rust_decimal::Decimal;

use super::datetime::{self as dt, Tm};
use super::numeric;
use crate::error::sqlstate;
use crate::{Error, Result};

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const ROMAN: [&str; 12] = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"];

/// Template patterns, longest first so that matching is greedy.
const PATTERNS: &[&str] = &[
    "Y,YYY", "MONTH", "Month", "month", "SSSSS", "A.M.", "P.M.", "a.m.", "p.m.", "B.C.", "A.D.", "b.c.", "a.d.",
    "HH24", "HH12", "SSSS", "YYYY", "IYYY", "IDDD", "yyyy", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "MON", "Mon",
    "mon", "DAY", "Day", "day", "DDD", "YYY", "IYY", "TZH", "TZM", "HH", "MI", "SS", "MS", "US", "AM", "PM", "am",
    "pm", "BC", "AD", "bc", "ad", "MM", "DY", "Dy", "dy", "DD", "ID", "IW", "WW", "CC", "RM", "rm", "TZ", "tz", "OF",
    "YY", "IY", "hh24", "hh12", "hh", "mi", "ss", "ms", "us", "mm", "dd", "yy", "Y", "I", "D", "W", "J", "Q",
];

/// Values a datetime template can refer to.
pub struct Fields {
    pub tm: Tm,
    /// UTC offset in seconds and zone abbreviation for `timestamptz`.
    pub zone: Option<(i32, String)>,
    /// Total hours for intervals (may exceed 23); `tm.hour` otherwise.
    pub hours: i64,
}

fn ordinal(n: i64, upper: bool) -> &'static str {
    let s = match (n.abs() % 100, n.abs() % 10) {
        (11..=13, _) => "th",
        (_, 1) => "st",
        (_, 2) => "nd",
        (_, 3) => "rd",
        _ => "th",
    };
    if upper {
        match s {
            "st" => "ST",
            "nd" => "ND",
            "rd" => "RD",
            _ => "TH",
        }
    } else {
        s
    }
}

fn cased(name: &str, pat: &str) -> String {
    let b = pat.as_bytes();
    if b[0].is_ascii_lowercase() {
        name.to_lowercase()
    } else if b.len() > 1 && b[1].is_ascii_uppercase() {
        name.to_uppercase()
    } else {
        name.to_string()
    }
}

pub fn datetime_to_char(f: &Fields, fmt: &str) -> String {
    let tm = &f.tm;
    let mut out = String::new();
    let mut rest = fmt;
    let mut fill_mode = false;
    let display_year = if tm.year > 0 { tm.year } else { 1 - tm.year };
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix("FM").or_else(|| rest.strip_prefix("fm")) {
            fill_mode = true;
            rest = r;
            continue;
        }
        if let Some(r) = rest.strip_prefix('"') {
            match r.find('"') {
                Some(end) => {
                    out.push_str(&r[..end]);
                    rest = &r[end + 1..];
                }
                None => {
                    out.push_str(r);
                    rest = "";
                }
            }
            continue;
        }
        if let Some(r) = rest.strip_prefix('\\') {
            let mut ch = r.chars();
            if let Some(c) = ch.next() {
                out.push(c);
            }
            rest = ch.as_str();
            continue;
        }
        let Some(pat) = PATTERNS.iter().find(|p| rest.starts_with(**p)) else {
            let mut ch = rest.chars();
            out.push(ch.next().unwrap());
            rest = ch.as_str();
            continue;
        };
        rest = &rest[pat.len()..];
        let fm = std::mem::take(&mut fill_mode);
        let num = |v: i64, width: usize| if fm { v.to_string() } else { format!("{v:0width$}") };
        let name = |n: &str, width: usize| {
            let s = cased(n, pat);
            if fm {
                s
            } else {
                format!("{s:<width$}")
            }
        };
        let h12 = {
            let h = f.hours % 12;
            if h == 0 {
                12
            } else {
                h
            }
        };
        let (iso_year, iso_week) = tm.iso_week();
        let mut numeric_value: Option<i64> = None;
        let piece = match pat.to_ascii_uppercase().as_str() {
            "HH24" => {
                numeric_value = Some(f.hours);
                num(f.hours, 2)
            }
            "HH12" | "HH" => {
                numeric_value = Some(h12);
                num(h12, 2)
            }
            "MI" => num(tm.minute as i64, 2),
            "SS" => num(tm.second as i64, 2),
            "MS" => format!("{:03}", tm.micro / 1000),
            "US" => format!("{:06}", tm.micro),
            "FF1" => format!("{:01}", tm.micro / 100_000),
            "FF2" => format!("{:02}", tm.micro / 10_000),
            "FF3" => format!("{:03}", tm.micro / 1000),
            "FF4" => format!("{:04}", tm.micro / 100),
            "FF5" => format!("{:05}", tm.micro / 10),
            "FF6" => format!("{:06}", tm.micro),
            "SSSS" | "SSSSS" => (f.hours * 3600 + tm.minute as i64 * 60 + tm.second as i64).to_string(),
            "AM" | "PM" => cased(
                if tm.hour >= 12 { "PM" } else { "AM" },
                if pat.as_bytes()[0].is_ascii_lowercase() { "a" } else { "AA" },
            ),
            "A.M." | "P.M." => cased(
                if tm.hour >= 12 { "P.M." } else { "A.M." },
                if pat.as_bytes()[0].is_ascii_lowercase() { "a" } else { "AA" },
            ),
            "BC" | "AD" => cased(
                if tm.year <= 0 { "BC" } else { "AD" },
                if pat.as_bytes()[0].is_ascii_lowercase() { "a" } else { "AA" },
            ),
            "B.C." | "A.D." => cased(
                if tm.year <= 0 { "B.C." } else { "A.D." },
                if pat.as_bytes()[0].is_ascii_lowercase() { "a" } else { "AA" },
            ),
            "Y,YYY" => format!("{},{:03}", display_year / 1000, display_year % 1000),
            "YYYY" => {
                numeric_value = Some(display_year);
                num(display_year, 4)
            }
            "YYY" => num(display_year % 1000, 3),
            "YY" => num(display_year % 100, 2),
            "Y" => (display_year % 10).to_string(),
            "IYYY" => num(iso_year, 4),
            "IYY" => num(iso_year % 1000, 3),
            "IY" => num(iso_year % 100, 2),
            "I" => (iso_year % 10).to_string(),
            "MONTH" => name(MONTHS[tm.month as usize - 1], 9),
            "MON" => cased(&MONTHS[tm.month as usize - 1][..3], pat),
            "MM" => {
                numeric_value = Some(tm.month as i64);
                num(tm.month as i64, 2)
            }
            "DAY" => name(DAYS[tm.dow() as usize], 9),
            "DY" => cased(&DAYS[tm.dow() as usize][..3], pat),
            "DDD" => {
                numeric_value = Some(tm.doy() as i64);
                num(tm.doy() as i64, 3)
            }
            "IDDD" => {
                let d = (iso_week as i64 - 1) * 7 + if tm.dow() == 0 { 7 } else { tm.dow() as i64 };
                num(d, 3)
            }
            "DD" => {
                numeric_value = Some(tm.day as i64);
                num(tm.day as i64, 2)
            }
            "D" => (tm.dow() + 1).to_string(),
            "ID" => (if tm.dow() == 0 { 7 } else { tm.dow() }).to_string(),
            "W" => ((tm.day - 1) / 7 + 1).to_string(),
            "WW" => num(((tm.doy() - 1) / 7 + 1) as i64, 2),
            "IW" => num(iso_week as i64, 2),
            "CC" => num(if tm.year > 0 { (tm.year + 99) / 100 } else { -((1 - tm.year + 99) / 100) }, 2),
            "J" => (dt::days_from_civil(tm.year, tm.month, tm.day) + 2_451_545).to_string(),
            "Q" => ((tm.month - 1) / 3 + 1).to_string(),
            "RM" => {
                let s = cased(ROMAN[tm.month as usize - 1], if *pat == "rm" { "a" } else { "AA" });
                if fm {
                    s
                } else {
                    format!("{s:<4}")
                }
            }
            "TZ" => match &f.zone {
                Some((_, abbr)) => {
                    if *pat == "tz" {
                        abbr.to_lowercase()
                    } else {
                        abbr.to_uppercase()
                    }
                }
                None => String::new(),
            },
            "OF" => f.zone.as_ref().map_or(String::new(), |(off, _)| dt::fmt_offset(*off)),
            "TZH" => f.zone.as_ref().map_or(String::new(), |(off, _)| {
                format!("{}{:02}", if *off < 0 { '-' } else { '+' }, off.abs() / 3600)
            }),
            "TZM" => f.zone.as_ref().map_or(String::new(), |(off, _)| format!("{:02}", off.abs() / 60 % 60)),
            _ => pat.to_string(),
        };
        out.push_str(&piece);
        // Ordinal suffix.
        if let Some(r) = rest.strip_prefix("TH") {
            if let Some(v) = numeric_value.or_else(|| piece.trim().parse().ok()) {
                out.push_str(ordinal(v, true));
                rest = r;
            }
        } else if let Some(r) = rest.strip_prefix("th") {
            if let Some(v) = numeric_value.or_else(|| piece.trim().parse().ok()) {
                out.push_str(ordinal(v, false));
                rest = r;
            }
        }
    }
    out
}

/// `to_char(numeric, text)` for the common template characters
/// `9 0 . , D G S MI PR L FM`.
pub fn numeric_to_char(value: Decimal, fmt: &str) -> Result<String> {
    let upper = fmt.to_ascii_uppercase();
    if upper.contains("EEEE") || upper.contains("RN") || upper.contains('V') {
        return Err(Error::unsupported("this to_char() numeric format"));
    }
    #[derive(PartialEq, Clone, Copy)]
    enum Tok {
        Nine,
        Zero,
        Point,
        Group,
        Sign,
        Minus,
        Angle,
        Currency,
        Lit(char),
    }
    let mut toks = Vec::new();
    let mut fill_mode = false;
    let mut rest = upper.as_str();
    let orig: Vec<char> = fmt.chars().collect();
    let mut pos = 0;
    while !rest.is_empty() {
        let (tok, len) = if rest.starts_with("FM") {
            fill_mode = true;
            (None, 2)
        } else if rest.starts_with("MI") {
            (Some(Tok::Minus), 2)
        } else if rest.starts_with("PR") {
            (Some(Tok::Angle), 2)
        } else if rest.starts_with("SG") {
            (Some(Tok::Sign), 2)
        } else if rest.starts_with("TH") {
            (None, 2)
        } else {
            let c = rest.chars().next().unwrap();
            let t = match c {
                '9' => Tok::Nine,
                '0' => Tok::Zero,
                '.' | 'D' => Tok::Point,
                ',' | 'G' => Tok::Group,
                'S' => Tok::Sign,
                'L' => Tok::Currency,
                _ => Tok::Lit(orig[pos]),
            };
            (Some(t), c.len_utf8())
        };
        if let Some(t) = tok {
            toks.push(t);
        }
        pos += rest[..len].chars().count();
        rest = &rest[len..];
    }
    let point = toks.iter().position(|t| *t == Tok::Point);
    let is_slot = |t: &Tok| matches!(t, Tok::Nine | Tok::Zero);
    let int_toks = &toks[..point.unwrap_or(toks.len())];
    let int_slots = int_toks.iter().filter(|t| is_slot(t)).count();
    let frac_slots = point.map_or(0, |p| toks[p..].iter().filter(|t| is_slot(t)).count());
    let rounded = numeric::round(value.abs(), frac_slots as i32);
    // The sign of a value that rounds to zero is not shown.
    let negative = value.is_sign_negative() && !rounded.is_zero();
    let s = rounded.to_string();
    let (int_part, frac_part) = s.split_once('.').unwrap_or((&s, ""));
    let int_digits: Vec<char> = if int_part == "0" { Vec::new() } else { int_part.chars().collect() };
    let overflow = int_digits.len() > int_slots;
    // Index of the first integer slot that shows a digit.
    let first_zero = int_toks.iter().filter(|t| is_slot(t)).position(|t| *t == Tok::Zero);
    let mut first_digit_slot = int_slots - int_digits.len().min(int_slots);
    if let Some(z) = first_zero {
        first_digit_slot = first_digit_slot.min(z);
    }
    // A zero integer part is printed when nothing else would be: without
    // a fractional part, or in fill mode when the whole value is zero and
    // no fractional digit is forced.
    let forced_frac = point.is_some_and(|p| toks[p..].contains(&Tok::Zero));
    if int_slots > 0 && int_digits.is_empty() && (frac_slots == 0 || (fill_mode && rounded.is_zero() && !forced_frac)) {
        first_digit_slot = first_digit_slot.min(int_slots - 1);
    }
    // Where the sign goes: glued to the first digit unless a trailing
    // S or MI asks for it at the end.
    let first_slot_tok = toks.iter().position(is_slot).unwrap_or(toks.len());
    let leading_s = toks[..first_slot_tok].contains(&Tok::Sign);
    let trailing_s = toks[first_slot_tok..].contains(&Tok::Sign);
    let has_mi = toks.contains(&Tok::Minus);
    let has_pr = toks.contains(&Tok::Angle);
    let anchored: &str = if trailing_s || has_mi {
        ""
    } else if has_pr {
        if negative {
            "<"
        } else if fill_mode {
            ""
        } else {
            " "
        }
    } else if leading_s {
        if negative {
            "-"
        } else {
            "+"
        }
    } else if negative {
        "-"
    } else if fill_mode {
        ""
    } else {
        " "
    };
    let mut body = String::new();
    let mut slot = 0;
    let mut frac_iter = frac_part.chars();
    let mut started = false;
    let mut in_frac = false;
    let mut sign_done = false;
    for (idx, t) in toks.iter().enumerate() {
        match t {
            Tok::Nine | Tok::Zero if !in_frac => {
                if slot >= first_digit_slot {
                    if !sign_done {
                        body.push_str(anchored);
                        sign_done = true;
                    }
                    let from_right = int_slots - slot;
                    let d = if overflow {
                        '#'
                    } else if from_right <= int_digits.len() {
                        int_digits[int_digits.len() - from_right]
                    } else {
                        '0'
                    };
                    body.push(d);
                    started = true;
                } else if !fill_mode {
                    body.push(' ');
                }
                slot += 1;
            }
            Tok::Nine | Tok::Zero => body.push(if overflow { '#' } else { frac_iter.next().unwrap_or('0') }),
            Tok::Point => {
                if !sign_done {
                    body.push_str(anchored);
                    sign_done = true;
                }
                body.push('.');
                in_frac = true;
            }
            Tok::Group => {
                if started {
                    body.push(',');
                } else if !fill_mode {
                    body.push(' ');
                }
            }
            Tok::Sign => {
                if idx >= first_slot_tok {
                    body.push(if negative { '-' } else { '+' });
                }
            }
            Tok::Minus => {
                if negative {
                    body.push('-')
                } else if !fill_mode {
                    body.push(' ')
                }
            }
            Tok::Angle => {}
            Tok::Currency => {
                if !fill_mode {
                    body.push(' ')
                }
            }
            Tok::Lit(c) => body.push(*c),
        }
    }
    if !sign_done {
        body.push_str(anchored);
    }
    if fill_mode && in_frac {
        // Trailing fractional zeros coming from '9' slots are dropped.
        let keep = point.map_or(0, |p| {
            let slots: Vec<&Tok> = toks[p..].iter().filter(|t| is_slot(t)).collect();
            slots.iter().rposition(|t| **t == Tok::Zero).map_or(0, |i| i + 1)
        });
        let dot = body.rfind('.').unwrap();
        let mut end = body.len();
        while end > dot + 1 + keep && body.as_bytes()[end - 1] == b'0' {
            end -= 1;
        }
        body.truncate(end);
    }
    if has_pr {
        if negative {
            body.push('>');
        } else if !fill_mode {
            body.push(' ');
        }
    }
    Ok(body)
}

fn invalid_value(what: &str, input: &str) -> Error {
    Error::new(sqlstate::INVALID_DATETIME_FORMAT, format!("invalid value \"{input}\" for \"{what}\""))
}

/// Result of parsing a string against a datetime template.
#[derive(Default, Debug)]
pub struct ParsedFields {
    pub year: Option<i64>,
    pub month: Option<u32>,
    pub day: Option<u32>,
    pub doy: Option<u32>,
    pub hour: Option<u32>,
    pub minute: u32,
    pub second: u32,
    pub micro: u32,
    pub pm: Option<bool>,
    pub bc: bool,
    pub tz_offset: Option<i32>,
    pub twelve_hour: bool,
}

pub fn parse_with_format(input: &str, fmt: &str) -> Result<ParsedFields> {
    let mut out = ParsedFields::default();
    let mut s = input.trim_start();
    let mut rest = fmt;
    let take_digits = |s: &mut &str, max: usize, what: &str, next_is_digit_pattern: bool| -> Result<i64> {
        let t = s.trim_start();
        let neg = t.starts_with('-');
        let t2 = t.strip_prefix('-').or_else(|| t.strip_prefix('+')).unwrap_or(t);
        let avail = t2.bytes().take_while(u8::is_ascii_digit).count();
        let n = if next_is_digit_pattern { avail.min(max) } else { avail };
        if n == 0 {
            return Err(invalid_value(what, &t.chars().take(max.max(1)).collect::<String>()));
        }
        let v: i64 = t2[..n].parse().map_err(|_| invalid_value(what, &t2[..n]))?;
        *s = &t2[n..];
        Ok(if neg { -v } else { v })
    };
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix("FM").or_else(|| rest.strip_prefix("fm")).or_else(|| rest.strip_prefix("FX"))
        {
            rest = r;
            continue;
        }
        if let Some(r) = rest.strip_prefix('"') {
            let end = r.find('"').unwrap_or(r.len());
            let n = r[..end].chars().count();
            s = s.char_indices().nth(n).map_or("", |(i, _)| &s[i..]);
            rest = r.get(end + 1..).unwrap_or("");
            continue;
        }
        let Some(pat) = PATTERNS.iter().find(|p| rest.starts_with(**p)) else {
            // Literal separator: skip one template char and any run of
            // separators in the input.
            let mut ch = rest.chars();
            let c = ch.next().unwrap();
            rest = ch.as_str();
            if c.is_whitespace() || !c.is_alphanumeric() {
                s = s.trim_start_matches(|x: char| x.is_whitespace() || (!x.is_alphanumeric() && x != '-' && x != '+'));
                if !c.is_whitespace() && s.starts_with(c) {
                    s = &s[c.len_utf8()..];
                }
            } else if !s.is_empty() {
                let mut sc = s.chars();
                sc.next();
                s = sc.as_str();
            }
            continue;
        };
        rest = &rest[pat.len()..];
        let next_digit = PATTERNS.iter().find(|p| rest.starts_with(**p)).is_some_and(|p| {
            matches!(
                p.to_ascii_uppercase().as_str(),
                "HH24" | "HH12" | "HH" | "MI" | "SS" | "MS" | "US" | "YYYY" | "YYY" | "YY" | "Y" | "MM" | "DD" | "DDD"
            )
        });
        let up = pat.to_ascii_uppercase();
        match up.as_str() {
            "YYYY" | "Y,YYY" => out.year = Some(take_digits(&mut s, 4, "YYYY", next_digit)?),
            "YYY" => out.year = Some(2000 + take_digits(&mut s, 3, "YYY", next_digit)?),
            "YY" => {
                let y = take_digits(&mut s, 2, "YY", next_digit)?;
                out.year = Some(if y < 70 { 2000 + y } else { 1900 + y });
            }
            "Y" => out.year = Some(2020 + take_digits(&mut s, 1, "Y", next_digit)?),
            "MM" => out.month = Some(take_digits(&mut s, 2, "MM", next_digit)? as u32),
            "DD" => out.day = Some(take_digits(&mut s, 2, "DD", next_digit)? as u32),
            "DDD" => out.doy = Some(take_digits(&mut s, 3, "DDD", next_digit)? as u32),
            "HH24" => out.hour = Some(take_digits(&mut s, 2, "HH24", next_digit)? as u32),
            "HH12" | "HH" => {
                out.hour = Some(take_digits(&mut s, 2, "HH12", next_digit)? as u32);
                out.twelve_hour = true;
            }
            "MI" => out.minute = take_digits(&mut s, 2, "MI", next_digit)? as u32,
            "SS" => out.second = take_digits(&mut s, 2, "SS", next_digit)? as u32,
            "MS" => {
                let t = s.trim_start();
                let n = t.bytes().take_while(u8::is_ascii_digit).count().min(3);
                let v: u32 = t[..n].parse().map_err(|_| invalid_value("MS", t))?;
                out.micro += v * 10u32.pow(3 - n as u32) * 1000;
                s = &t[n..];
            }
            "US" | "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" => {
                let t = s.trim_start();
                let n = t.bytes().take_while(u8::is_ascii_digit).count().min(6);
                let v: u32 = t[..n].parse().map_err(|_| invalid_value("US", t))?;
                out.micro += v * 10u32.pow(6 - n as u32);
                s = &t[n..];
            }
            "AM" | "PM" | "A.M." | "P.M." => {
                let t = s.trim_start().to_ascii_lowercase();
                let (is_pm, len) = if t.starts_with("p.m.") {
                    (true, 4)
                } else if t.starts_with("a.m.") {
                    (false, 4)
                } else if t.starts_with("pm") {
                    (true, 2)
                } else if t.starts_with("am") {
                    (false, 2)
                } else {
                    return Err(invalid_value(pat, s));
                };
                out.pm = Some(is_pm);
                s = &s.trim_start()[len..];
            }
            "BC" | "AD" | "B.C." | "A.D." => {
                let t = s.trim_start().to_ascii_lowercase();
                out.bc = t.starts_with('b');
                let len = if t.starts_with("b.c.") || t.starts_with("a.d.") { 4 } else { 2 };
                s = s.trim_start().get(len..).unwrap_or("");
            }
            "MONTH" | "MON" => {
                let t = s.trim_start();
                let lower = t.to_ascii_lowercase();
                let idx = MONTHS
                    .iter()
                    .position(|m| {
                        lower.starts_with(&m.to_lowercase())
                            || (up == "MON" && lower.starts_with(&m[..3].to_lowercase()))
                    })
                    .or_else(|| MONTHS.iter().position(|m| lower.starts_with(&m[..3].to_lowercase())))
                    .ok_or_else(|| invalid_value(pat, t))?;
                let full = MONTHS[idx].len();
                let len = if lower.starts_with(&MONTHS[idx].to_lowercase()) { full } else { 3 };
                out.month = Some(idx as u32 + 1);
                s = &t[len..];
            }
            "DAY" | "DY" => {
                let t = s.trim_start();
                let n = t.bytes().take_while(u8::is_ascii_alphabetic).count();
                s = &t[n..];
            }
            "TZH" => {
                let v = take_digits(&mut s, 2, "TZH", next_digit)?;
                out.tz_offset = Some(out.tz_offset.unwrap_or(0) + v as i32 * 3600);
            }
            "TZM" => {
                let v = take_digits(&mut s, 2, "TZM", next_digit)? as i32;
                let base = out.tz_offset.unwrap_or(0);
                out.tz_offset = Some(if base < 0 { base - v * 60 } else { base + v * 60 });
            }
            _ => {
                // Other fields are consumed but ignored.
                let t = s.trim_start();
                let n = t.bytes().take_while(u8::is_ascii_alphanumeric).count();
                s = &t[n..];
            }
        }
    }
    Ok(out)
}

impl ParsedFields {
    /// Local date and time of day in microseconds.
    pub fn resolve(&self) -> Result<(i64, i64)> {
        let range = |what: &str| {
            Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, format!("date/time field value out of range: {what}"))
        };
        let mut year = self.year.unwrap_or(1);
        if self.bc {
            year = 1 - year;
        }
        let (month, day) = match self.doy {
            Some(doy) if self.month.is_none() => {
                let (_, m, d) = dt::civil_from_days(dt::days_from_civil(year, 1, 1) + doy as i64 - 1);
                (m, d)
            }
            _ => (self.month.unwrap_or(1), self.day.unwrap_or(1)),
        };
        if !(1..=12).contains(&month) || day < 1 || day > dt::days_in_month(year, month) {
            return Err(range("day or month"));
        }
        let mut hour = self.hour.unwrap_or(0);
        if self.twelve_hour || self.pm.is_some() {
            if !(1..=12).contains(&hour) && self.hour.is_some() {
                return Err(Error::new(
                    sqlstate::INVALID_DATETIME_FORMAT,
                    format!("hour \"{hour}\" is invalid for the 12-hour clock"),
                )
                .with_hint("Use the 24-hour clock, or give an hour between 1 and 12."));
            }
            hour = match (self.pm.unwrap_or(false), hour) {
                (true, 12) => 12,
                (true, h) => h + 12,
                (false, 12) => 0,
                (false, h) => h,
            };
        }
        if hour > 24 || self.minute > 59 || self.second > 59 {
            return Err(range("time"));
        }
        let time = hour as i64 * dt::USECS_PER_HOUR
            + self.minute as i64 * dt::USECS_PER_MIN
            + self.second as i64 * dt::USECS_PER_SEC
            + self.micro as i64;
        Ok((dt::days_from_civil(year, month, day), time))
    }
}

/// `to_number(text, text)`: reads the digits, sign and decimal point.
pub fn to_number(input: &str, fmt: &str) -> Result<Decimal> {
    let frac_slots = fmt
        .to_ascii_uppercase()
        .split_once(['.', 'D'])
        .map_or(0, |(_, f)| f.chars().filter(|c| matches!(c, '9' | '0')).count());
    let mut s = String::new();
    let mut negative = false;
    for c in input.chars() {
        match c {
            '0'..='9' => s.push(c),
            '.' if !s.contains('.') => s.push('.'),
            '-' | '<' => negative = true,
            _ => {}
        }
    }
    if s.is_empty() || s == "." {
        return Err(Error::invalid_text("numeric", input));
    }
    let d = numeric::parse_numeric(&s)?;
    let d = numeric::round(d, frac_slots.min(d.scale() as usize) as i32);
    Ok(if negative { -d } else { d })
}
