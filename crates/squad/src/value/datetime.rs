//! Calendar arithmetic, parsing and formatting of date/time values.
//!
//! Dates are days and timestamps microseconds relative to 2000-01-01, as in
//! Postgres. Years are astronomical internally (year 0 is 1 BC).

use chrono::{Offset, TimeZone};
use chrono_tz::Tz;
use rust_decimal::Decimal;

use crate::error::sqlstate;
use crate::types::{Interval, PG_EPOCH_UNIX_MICROS};
use crate::{Error, FnCtx, Result};

pub const USECS_PER_SEC: i64 = 1_000_000;
pub const USECS_PER_MIN: i64 = 60 * USECS_PER_SEC;
pub const USECS_PER_HOUR: i64 = 3600 * USECS_PER_SEC;
pub const USECS_PER_DAY: i64 = 86_400 * USECS_PER_SEC;
pub const TS_INF: i64 = i64::MAX;
pub const TS_NINF: i64 = i64::MIN;
pub const DATE_INF: i32 = i32::MAX;
pub const DATE_NINF: i32 = i32::MIN;

/// Days from 2000-01-01 to the given proleptic Gregorian date.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468 - 10_957
}

/// Inverse of [`days_from_civil`].
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 10_957 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

pub fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Broken-down timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tm {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micro: u32,
}

impl Tm {
    pub fn from_ts(ts: i64) -> Tm {
        let days = ts.div_euclid(USECS_PER_DAY);
        let t = ts.rem_euclid(USECS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        Tm {
            year,
            month,
            day,
            hour: (t / USECS_PER_HOUR) as u32,
            minute: (t / USECS_PER_MIN % 60) as u32,
            second: (t / USECS_PER_SEC % 60) as u32,
            micro: (t % USECS_PER_SEC) as u32,
        }
    }

    pub fn time_micros(&self) -> i64 {
        self.hour as i64 * USECS_PER_HOUR
            + self.minute as i64 * USECS_PER_MIN
            + self.second as i64 * USECS_PER_SEC
            + self.micro as i64
    }

    pub fn to_ts(&self) -> Result<i64> {
        days_from_civil(self.year, self.month, self.day)
            .checked_mul(USECS_PER_DAY)
            .and_then(|d| d.checked_add(self.time_micros()))
            .ok_or_else(ts_out_of_range)
    }

    /// Day of week, 0 = Sunday.
    pub fn dow(&self) -> u32 {
        (days_from_civil(self.year, self.month, self.day) + 6).rem_euclid(7) as u32
    }

    /// Day of year, 1-based.
    pub fn doy(&self) -> u32 {
        (days_from_civil(self.year, self.month, self.day) - days_from_civil(self.year, 1, 1) + 1) as u32
    }

    /// ISO 8601 week-numbering year and week.
    pub fn iso_week(&self) -> (i64, u32) {
        let days = days_from_civil(self.year, self.month, self.day);
        let isodow = (days + 5).rem_euclid(7); // 0 = Monday
        let thursday = days - isodow + 3;
        let (y, _, _) = civil_from_days(thursday);
        let week = (thursday - days_from_civil(y, 1, 1)) / 7 + 1;
        (y, week as u32)
    }
}

pub fn ts_out_of_range() -> Error {
    Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "timestamp out of range")
}

pub fn date_to_ts(d: i32) -> i64 {
    match d {
        DATE_INF => TS_INF,
        DATE_NINF => TS_NINF,
        d => d as i64 * USECS_PER_DAY,
    }
}

pub fn ts_to_date(ts: i64) -> i32 {
    match ts {
        TS_INF => DATE_INF,
        TS_NINF => DATE_NINF,
        ts => ts.div_euclid(USECS_PER_DAY) as i32,
    }
}

pub fn is_finite_ts(ts: i64) -> bool {
    ts != TS_INF && ts != TS_NINF
}

// ---------------------------------------------------------------- zones

/// A time zone specification.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Zone {
    Named(Tz),
    /// Seconds east of UTC.
    Fixed(i32),
}

const ABBREVS: &[(&str, i32)] = &[
    ("z", 0),
    ("utc", 0),
    ("ut", 0),
    ("gmt", 0),
    ("zulu", 0),
    ("est", -5 * 3600),
    ("edt", -4 * 3600),
    ("cst", -6 * 3600),
    ("cdt", -5 * 3600),
    ("mst", -7 * 3600),
    ("mdt", -6 * 3600),
    ("pst", -8 * 3600),
    ("pdt", -7 * 3600),
    ("akst", -9 * 3600),
    ("akdt", -8 * 3600),
    ("hst", -10 * 3600),
    ("cet", 3600),
    ("cest", 2 * 3600),
    ("met", 3600),
    ("mest", 2 * 3600),
    ("eet", 2 * 3600),
    ("eest", 3 * 3600),
    ("wet", 0),
    ("west", 3600),
    ("bst", 3600),
    ("ist", 2 * 3600),
    ("msk", 3 * 3600),
    ("jst", 9 * 3600),
    ("kst", 9 * 3600),
    ("aest", 10 * 3600),
    ("aedt", 11 * 3600),
    ("acst", 9 * 3600 + 1800),
    ("awst", 8 * 3600),
    ("nzst", 12 * 3600),
    ("nzdt", 13 * 3600),
];

/// Parses `[+-]HH[:MM[:SS]]` or `[+-]HHMM` into seconds (sign as written).
pub fn parse_offset(s: &str) -> Option<i32> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit() || b == b':') {
        return None;
    }
    let (h, m, sec): (i32, i32, i32) = if rest.contains(':') {
        let mut it = rest.split(':');
        let h = it.next()?.parse().ok()?;
        let m = it.next().map_or(Some(0), |x| x.parse().ok())?;
        let s = it.next().map_or(Some(0), |x| x.parse().ok())?;
        if it.next().is_some() {
            return None;
        }
        (h, m, s)
    } else if rest.len() <= 2 {
        (rest.parse().ok()?, 0, 0)
    } else if rest.len() == 4 {
        (rest[..2].parse().ok()?, rest[2..].parse().ok()?, 0)
    } else {
        return None;
    };
    if h > 15 || m > 59 || sec > 59 {
        return None;
    }
    Some(sign * (h * 3600 + m * 60 + sec))
}

/// Parses a zone name, abbreviation or numeric offset. With `posix` set a
/// bare numeric offset follows the POSIX convention (positive = west of
/// Greenwich), which is what `SET TIME ZONE` and `AT TIME ZONE` use.
pub fn parse_zone(s: &str, posix: bool) -> Option<Zone> {
    let s = s.trim();
    if let Some(off) = parse_offset(s) {
        return Some(Zone::Fixed(if posix { -off } else { off }));
    }
    let lower = s.to_ascii_lowercase();
    if let Some((_, off)) = ABBREVS.iter().find(|(n, _)| *n == lower) {
        return Some(Zone::Fixed(*off));
    }
    if let Ok(tz) = s.parse::<Tz>() {
        return Some(Zone::Named(tz));
    }
    // Case-insensitive lookup, e.g. 'europe/berlin'.
    chrono_tz::TZ_VARIANTS.iter().find(|tz| tz.name().eq_ignore_ascii_case(s)).map(|tz| Zone::Named(*tz))
}

pub fn zone_not_recognized(name: &str) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("time zone \"{name}\" not recognized"))
}

fn naive(ts: i64) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(ts.checked_add(PG_EPOCH_UNIX_MICROS)?).map(|d| d.naive_utc())
}

impl Zone {
    /// Offset in seconds east of UTC at the given UTC instant.
    pub fn offset_at_utc(&self, utc_ts: i64) -> i32 {
        match self {
            Zone::Fixed(o) => *o,
            Zone::Named(tz) => match naive(utc_ts) {
                Some(n) => tz.offset_from_utc_datetime(&n).fix().local_minus_utc(),
                None => 0,
            },
        }
    }

    /// Offset to apply to a local wall-clock time. Ambiguous times resolve
    /// to the later instant (standard time) and times inside a gap to the offset in force
    /// before the transition, as Postgres does.
    pub fn offset_at_local(&self, local_ts: i64) -> i32 {
        match self {
            Zone::Fixed(o) => *o,
            Zone::Named(tz) => {
                let Some(n) = naive(local_ts) else { return 0 };
                match tz.offset_from_local_datetime(&n) {
                    chrono::LocalResult::Single(o) => o.fix().local_minus_utc(),
                    chrono::LocalResult::Ambiguous(_, b) => b.fix().local_minus_utc(),
                    chrono::LocalResult::None => match naive(local_ts - USECS_PER_DAY) {
                        Some(before) => match tz.offset_from_local_datetime(&before) {
                            chrono::LocalResult::Single(o) | chrono::LocalResult::Ambiguous(o, _) => {
                                o.fix().local_minus_utc()
                            }
                            chrono::LocalResult::None => 0,
                        },
                        None => 0,
                    },
                }
            }
        }
    }

    pub fn utc_to_local(&self, utc_ts: i64) -> i64 {
        if !is_finite_ts(utc_ts) {
            return utc_ts;
        }
        utc_ts.saturating_add(self.offset_at_utc(utc_ts) as i64 * USECS_PER_SEC)
    }

    pub fn local_to_utc(&self, local_ts: i64) -> i64 {
        if !is_finite_ts(local_ts) {
            return local_ts;
        }
        local_ts.saturating_sub(self.offset_at_local(local_ts) as i64 * USECS_PER_SEC)
    }

    /// Abbreviation in force at the given UTC instant (`TZ` in `to_char`).
    pub fn abbrev_at_utc(&self, utc_ts: i64) -> String {
        match self {
            Zone::Fixed(o) => fmt_offset(*o),
            Zone::Named(tz) => match naive(utc_ts) {
                Some(n) => {
                    use chrono_tz::OffsetName;
                    tz.offset_from_utc_datetime(&n).abbreviation().map(str::to_string).unwrap_or_default()
                }
                None => String::new(),
            },
        }
    }
}

pub fn session_zone(ctx: &FnCtx) -> Zone {
    Zone::Named(ctx.tz)
}

// ------------------------------------------------------------ formatting

fn push_frac(out: &mut String, micro: u32) {
    if micro != 0 {
        let s = format!("{micro:06}");
        out.push('.');
        out.push_str(s.trim_end_matches('0'));
    }
}

fn push_date(out: &mut String, y: i64, m: u32, d: u32) {
    let year = if y <= 0 { 1 - y } else { y };
    out.push_str(&format!("{year:04}-{m:02}-{d:02}"));
}

pub fn fmt_date(d: i32) -> String {
    match d {
        DATE_INF => "infinity".into(),
        DATE_NINF => "-infinity".into(),
        d => {
            let (y, m, day) = civil_from_days(d as i64);
            let mut s = String::with_capacity(13);
            push_date(&mut s, y, m, day);
            if y <= 0 {
                s.push_str(" BC");
            }
            s
        }
    }
}

pub fn fmt_time(t: i64) -> String {
    let mut s = format!("{:02}:{:02}:{:02}", t / USECS_PER_HOUR, t / USECS_PER_MIN % 60, t / USECS_PER_SEC % 60);
    push_frac(&mut s, (t % USECS_PER_SEC) as u32);
    s
}

fn fmt_ts_inner(ts: i64, offset: Option<i32>) -> String {
    match ts {
        TS_INF => return "infinity".into(),
        TS_NINF => return "-infinity".into(),
        _ => {}
    }
    let tm = Tm::from_ts(ts);
    let mut s = String::with_capacity(32);
    push_date(&mut s, tm.year, tm.month, tm.day);
    s.push_str(&format!(" {:02}:{:02}:{:02}", tm.hour, tm.minute, tm.second));
    push_frac(&mut s, tm.micro);
    if let Some(off) = offset {
        s.push_str(&fmt_offset(off));
    }
    if tm.year <= 0 {
        s.push_str(" BC");
    }
    s
}

/// `+HH[:MM[:SS]]`
pub fn fmt_offset(off: i32) -> String {
    let a = off.abs();
    let mut s = format!("{}{:02}", if off < 0 { '-' } else { '+' }, a / 3600);
    if a % 3600 != 0 {
        s.push_str(&format!(":{:02}", a / 60 % 60));
        if a % 60 != 0 {
            s.push_str(&format!(":{:02}", a % 60));
        }
    }
    s
}

pub fn fmt_timestamp(ts: i64) -> String {
    fmt_ts_inner(ts, None)
}

pub fn fmt_timestamptz(ts: i64, zone: &Zone) -> String {
    if !is_finite_ts(ts) {
        return fmt_ts_inner(ts, None);
    }
    let off = zone.offset_at_utc(ts);
    fmt_ts_inner(ts.saturating_add(off as i64 * USECS_PER_SEC), Some(off))
}

pub fn fmt_interval(iv: &Interval) -> String {
    let mut out = String::new();
    let mut is_zero = true;
    let mut is_before = false;
    let mut part = |value: i64, unit: &str, out: &mut String| {
        if value == 0 {
            return;
        }
        if !is_zero {
            out.push(' ');
        }
        if is_before && value > 0 {
            out.push('+');
        }
        out.push_str(&format!("{value} {unit}{}", if value != 1 { "s" } else { "" }));
        is_before = value < 0;
        is_zero = false;
    };
    part((iv.months / 12) as i64, "year", &mut out);
    part((iv.months % 12) as i64, "mon", &mut out);
    part(iv.days as i64, "day", &mut out);
    if is_zero || iv.micros != 0 {
        let a = iv.micros.unsigned_abs();
        if !is_zero {
            out.push(' ');
        }
        if iv.micros < 0 {
            out.push('-');
        } else if is_before {
            out.push('+');
        }
        let us = USECS_PER_SEC as u64;
        out.push_str(&format!("{:02}:{:02}:{:02}", a / (3600 * us), a / (60 * us) % 60, a / us % 60));
        push_frac(&mut out, (a % us) as u32);
    }
    out
}

// --------------------------------------------------------------- parsing

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Special {
    Now,
    Today,
    Tomorrow,
    Yesterday,
    Epoch,
    Infinity,
    NegInfinity,
}

#[derive(Debug, Default)]
pub struct Parsed {
    pub date: Option<(i64, u32, u32)>,
    /// Microseconds since midnight.
    pub time: Option<i64>,
    pub zone: Option<Zone>,
    pub special: Option<Special>,
}

pub enum DtErr {
    Syntax,
    Range,
}

const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];
const DAYS: [&str; 7] = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];

fn month_from_name(w: &str) -> Option<u32> {
    let w = w.trim_end_matches('.');
    if w.len() < 3 {
        return None;
    }
    MONTHS
        .iter()
        .position(|m| m.starts_with(w) && (w.len() == 3 || w.len() == m.len() || (w == "sept")))
        .map(|i| i as u32 + 1)
}

fn is_day_name(w: &str) -> bool {
    let w = w.trim_end_matches('.');
    w.len() >= 3 && DAYS.iter().any(|d| d.starts_with(w))
}

fn parse_time_token(w: &str, out: &mut Parsed, pm: &mut Option<bool>) -> Result<(), DtErr> {
    // Split a trailing zone / meridiem off the clock part.
    let mut clock = w;
    let mut tail = "";
    if let Some(i) =
        w.char_indices().skip(1).find(|(_, c)| *c == '+' || *c == '-' || c.is_ascii_alphabetic()).map(|(i, _)| i)
    {
        clock = &w[..i];
        tail = &w[i..];
    }
    let mut parts = clock.split(':');
    let h: i64 = parts.next().unwrap_or("").parse().map_err(|_| DtErr::Syntax)?;
    let m: i64 = parts.next().ok_or(DtErr::Syntax)?.parse().map_err(|_| DtErr::Syntax)?;
    let mut micros = 0i64;
    let mut sec = 0i64;
    if let Some(s) = parts.next() {
        let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
        sec = whole.parse().map_err(|_| DtErr::Syntax)?;
        if !frac.is_empty() {
            if !frac.bytes().all(|b| b.is_ascii_digit()) {
                return Err(DtErr::Syntax);
            }
            let f: f64 = format!("0.{frac}").parse().map_err(|_| DtErr::Syntax)?;
            micros = (f * 1e6).round() as i64;
        }
    }
    if parts.next().is_some() {
        return Err(DtErr::Syntax);
    }
    if h > 24 || m > 59 || sec > 60 || (h == 24 && (m != 0 || sec != 0 || micros != 0)) {
        return Err(DtErr::Range);
    }
    out.time = Some(h * USECS_PER_HOUR + m * USECS_PER_MIN + sec * USECS_PER_SEC + micros);
    if !tail.is_empty() {
        match tail {
            "am" | "a.m." => *pm = Some(false),
            "pm" | "p.m." => *pm = Some(true),
            _ => out.zone = Some(parse_zone(tail, false).ok_or(DtErr::Syntax)?),
        }
    }
    Ok(())
}

fn fix_year(y: i64, digits: usize) -> i64 {
    if digits <= 2 {
        if y < 70 {
            y + 2000
        } else {
            y + 1900
        }
    } else {
        y
    }
}

fn num(s: &str) -> Result<i64, DtErr> {
    if s.is_empty() || s.len() > 9 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DtErr::Syntax);
    }
    s.parse().map_err(|_| DtErr::Syntax)
}

fn parse_date_token(w: &str, sep: char) -> Result<(i64, u32, u32), DtErr> {
    let parts: Vec<&str> = w.split(sep).collect();
    if parts.len() != 3 {
        return Err(DtErr::Syntax);
    }
    let (y, m, d);
    if let Some(mon) = month_from_name(parts[1]) {
        // 2020-Jan-05 or 05-Jan-2020
        m = mon as i64;
        if parts[0].len() >= 3 {
            y = fix_year(num(parts[0])?, parts[0].len());
            d = num(parts[2])?;
        } else {
            d = num(parts[0])?;
            y = fix_year(num(parts[2])?, parts[2].len());
        }
    } else if parts[0].len() >= 3 {
        y = num(parts[0])?;
        m = num(parts[1])?;
        d = num(parts[2])?;
    } else {
        // MDY, the default DateStyle.
        m = num(parts[0])?;
        d = num(parts[1])?;
        y = fix_year(num(parts[2])?, parts[2].len());
    }
    Ok((y, m as u32, d as u32))
}

/// Parses the free-form date/time syntax Postgres accepts (ISO 8601, SQL
/// and the common "month name" spellings).
pub fn parse_datetime(input: &str) -> Result<Parsed, DtErr> {
    let lower = input.trim().to_ascii_lowercase();
    let special = match lower.as_str() {
        "now" => Some(Special::Now),
        "today" => Some(Special::Today),
        "tomorrow" => Some(Special::Tomorrow),
        "yesterday" => Some(Special::Yesterday),
        "epoch" => Some(Special::Epoch),
        "infinity" | "+infinity" => Some(Special::Infinity),
        "-infinity" => Some(Special::NegInfinity),
        _ => None,
    };
    let mut out = Parsed { special, ..Parsed::default() };
    if out.special.is_some() {
        return Ok(out);
    }
    if lower.is_empty() {
        return Err(DtErr::Syntax);
    }
    let mut words: Vec<String> = Vec::new();
    for w in lower.split_whitespace() {
        let w = w.trim_end_matches(',');
        if w.is_empty() {
            continue;
        }
        // ISO 'T' separator between date and time.
        let b = w.as_bytes();
        match w.find('t') {
            Some(i)
                if i > 0
                    && b[0].is_ascii_digit()
                    && b[i - 1].is_ascii_digit()
                    && i + 1 < b.len()
                    && b[i + 1].is_ascii_digit() =>
            {
                words.push(w[..i].to_string());
                words.push(w[i + 1..].to_string());
            }
            _ => words.push(w.to_string()),
        }
    }
    let mut month: Option<u32> = None;
    let mut nums: Vec<(i64, usize)> = Vec::new();
    let mut pm: Option<bool> = None;
    let mut bc = false;
    for w in &words {
        let w = w.as_str();
        let first = w.as_bytes()[0];
        if first.is_ascii_digit() {
            if w.contains(':') {
                if out.time.is_some() {
                    return Err(DtErr::Syntax);
                }
                parse_time_token(w, &mut out, &mut pm)?;
            } else if w[1..].contains('-') || w.contains('/') {
                if out.date.is_some() {
                    return Err(DtErr::Syntax);
                }
                let sep = if w.contains('/') { '/' } else { '-' };
                // A date may carry a trailing zone only after a time, so a
                // fourth '-' part is an error here.
                out.date = Some(parse_date_token(w, sep)?);
            } else if w.len() == 8 && w.as_bytes()[4] == b'.' && w.bytes().filter(u8::is_ascii_digit).count() == 7 {
                // Year and day of year: 2024.075
                if out.date.is_some() {
                    return Err(DtErr::Syntax);
                }
                let (y, doy) = (num(&w[..4])?, num(&w[5..])?);
                if doy < 1 || doy > if is_leap(y) { 366 } else { 365 } {
                    return Err(DtErr::Range);
                }
                out.date = Some(civil_from_days(days_from_civil(y, 1, 1) + doy - 1));
            } else if w.bytes().all(|b| b.is_ascii_digit()) {
                if out.date.is_some() && out.time.is_none() && month.is_none() && (w.len() == 6 || w.len() == 4) {
                    // Compact time after a date: HHMMSS or HHMM.
                    let (h, m) = (num(&w[..2])?, num(&w[2..4])?);
                    let sec = if w.len() == 6 { num(&w[4..])? } else { 0 };
                    if h > 24 || m > 59 || sec > 60 {
                        return Err(DtErr::Range);
                    }
                    out.time = Some(h * USECS_PER_HOUR + m * USECS_PER_MIN + sec * USECS_PER_SEC);
                } else if month.is_none() && out.date.is_none() && w.len() == 8 {
                    out.date = Some((num(&w[..4])?, num(&w[4..6])? as u32, num(&w[6..])? as u32));
                } else if month.is_none() && out.date.is_none() && w.len() == 6 && nums.is_empty() {
                    out.date = Some((fix_year(num(&w[..2])?, 2), num(&w[2..4])? as u32, num(&w[4..])? as u32));
                } else {
                    nums.push((num(w)?, w.len()));
                }
            } else if let Some(h) = w.strip_suffix("am").or_else(|| w.strip_suffix("pm")) {
                let h = num(h)?;
                if h > 12 || out.time.is_some() {
                    return Err(DtErr::Range);
                }
                out.time = Some(h * USECS_PER_HOUR);
                pm = Some(w.ends_with("pm"));
            } else {
                return Err(DtErr::Syntax);
            }
        } else if first == b'+' || first == b'-' {
            if out.time.is_none() || out.zone.is_some() {
                return Err(DtErr::Syntax);
            }
            out.zone = Some(Zone::Fixed(parse_offset(w).ok_or(DtErr::Syntax)?));
        } else if first == b'j' && w.len() > 1 && w[1..].bytes().all(|b| b.is_ascii_digit()) {
            // Julian day number.
            if out.date.is_some() {
                return Err(DtErr::Syntax);
            }
            out.date = Some(civil_from_days(num(&w[1..])? - 2_451_545));
        } else if let Some(m) = month_from_name(w) {
            if month.is_some() {
                return Err(DtErr::Syntax);
            }
            month = Some(m);
        } else {
            match w {
                "am" | "a.m." => pm = Some(false),
                "pm" | "p.m." => pm = Some(true),
                "bc" => bc = true,
                "ad" => {}
                "allballs" => {
                    out.time = Some(0);
                    out.zone = Some(Zone::Fixed(0));
                }
                _ if is_day_name(w) => {}
                _ => {
                    if out.zone.is_some() {
                        return Err(DtErr::Syntax);
                    }
                    out.zone = Some(parse_zone(w, false).ok_or(DtErr::Syntax)?);
                }
            }
        }
    }
    if let Some(m) = month {
        if out.date.is_some() || nums.len() != 2 {
            return Err(DtErr::Syntax);
        }
        let (a, b) = (nums[0], nums[1]);
        let (day, year) = if a.1 >= 3 { (b.0, a) } else { (a.0, b) };
        out.date = Some((fix_year(year.0, year.1), m, day as u32));
    } else if !nums.is_empty() {
        return Err(DtErr::Syntax);
    }
    if let Some(is_pm) = pm {
        let t = out.time.ok_or(DtErr::Syntax)?;
        let h = t / USECS_PER_HOUR;
        if h > 12 {
            return Err(DtErr::Range);
        }
        let h24 = match (is_pm, h) {
            (true, 12) => 12,
            (true, h) => h + 12,
            (false, 12) => 0,
            (false, h) => h,
        };
        out.time = Some(t - h * USECS_PER_HOUR + h24 * USECS_PER_HOUR);
    }
    if let Some((y, m, d)) = &mut out.date {
        if bc {
            if *y <= 0 {
                return Err(DtErr::Range);
            }
            *y = 1 - *y;
        } else if *y == 0 {
            return Err(DtErr::Range);
        }
        if *m < 1 || *m > 12 || *d < 1 || *d > days_in_month(*y, *m) {
            return Err(DtErr::Range);
        }
    } else if bc {
        return Err(DtErr::Syntax);
    }
    if out.date.is_none() && out.time.is_none() {
        return Err(DtErr::Syntax);
    }
    Ok(out)
}

fn dt_error(e: DtErr, type_name: &str, input: &str) -> Error {
    match e {
        DtErr::Syntax => Error::new(
            sqlstate::INVALID_DATETIME_FORMAT,
            format!("invalid input syntax for type {type_name}: \"{input}\""),
        ),
        DtErr::Range => {
            Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, format!("date/time field value out of range: \"{input}\""))
        }
    }
}

fn today(ctx: &FnCtx) -> i32 {
    ts_to_date(session_zone(ctx).utc_to_local(ctx.txn_time))
}

pub fn parse_date(input: &str, ctx: &FnCtx) -> Result<i32> {
    let p = parse_datetime(input).map_err(|e| dt_error(e, "date", input))?;
    if let Some(sp) = p.special {
        return Ok(match sp {
            Special::Now | Special::Today => today(ctx),
            Special::Tomorrow => today(ctx) + 1,
            Special::Yesterday => today(ctx) - 1,
            Special::Epoch => -crate::types::PG_EPOCH_UNIX_DAYS,
            Special::Infinity => DATE_INF,
            Special::NegInfinity => DATE_NINF,
        });
    }
    let (y, m, d) = p.date.ok_or_else(|| dt_error(DtErr::Syntax, "date", input))?;
    let days = days_from_civil(y, m, d);
    if !(-4713..=5_874_897).contains(&y) {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, format!("date out of range: \"{input}\"")));
    }
    Ok(days as i32)
}

pub fn parse_time(input: &str, ctx: &FnCtx) -> Result<i64> {
    let p = parse_datetime(input).map_err(|e| dt_error(e, "time", input))?;
    if p.special == Some(Special::Now) {
        return Ok(session_zone(ctx).utc_to_local(ctx.txn_time).rem_euclid(USECS_PER_DAY));
    }
    p.time.ok_or_else(|| dt_error(DtErr::Syntax, "time", input))
}

fn parse_ts_common(input: &str, ctx: &FnCtx, type_name: &str, with_zone: bool) -> Result<i64> {
    let p = parse_datetime(input).map_err(|e| dt_error(e, type_name, input))?;
    let zone = session_zone(ctx);
    let midnight = |date: i32| {
        let local = date as i64 * USECS_PER_DAY;
        if with_zone {
            zone.local_to_utc(local)
        } else {
            local
        }
    };
    if let Some(sp) = p.special {
        return Ok(match sp {
            Special::Now => {
                if with_zone {
                    ctx.txn_time
                } else {
                    zone.utc_to_local(ctx.txn_time)
                }
            }
            Special::Today => midnight(today(ctx)),
            Special::Tomorrow => midnight(today(ctx) + 1),
            Special::Yesterday => midnight(today(ctx) - 1),
            Special::Epoch => -PG_EPOCH_UNIX_MICROS,
            Special::Infinity => TS_INF,
            Special::NegInfinity => TS_NINF,
        });
    }
    let (y, m, d) = p.date.ok_or_else(|| dt_error(DtErr::Syntax, type_name, input))?;
    if !(-4713..=294_276).contains(&y) {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, format!("timestamp out of range: \"{input}\"")));
    }
    let local = days_from_civil(y, m, d) * USECS_PER_DAY + p.time.unwrap_or(0);
    Ok(if with_zone { p.zone.unwrap_or(zone).local_to_utc(local) } else { local })
}

pub fn parse_timestamp(input: &str, ctx: &FnCtx) -> Result<i64> {
    parse_ts_common(input, ctx, "timestamp", false)
}

pub fn parse_timestamptz(input: &str, ctx: &FnCtx) -> Result<i64> {
    parse_ts_common(input, ctx, "timestamp with time zone", true)
}

fn interval_err(input: &str) -> Error {
    Error::new(sqlstate::INVALID_DATETIME_FORMAT, format!("invalid input syntax for type interval: \"{input}\""))
}

/// Accumulates a possibly fractional amount of a unit, spilling fractions
/// into the next smaller field the way Postgres does.
fn add_unit(iv: &mut (f64, f64, f64), value: f64, unit: &str) -> bool {
    let (months, days, micros) = iv;
    let u = unit.trim_end_matches('.');
    match u {
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" | "microsecon" => *micros += value,
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" | "millisecon" => *micros += value * 1e3,
        "s" | "sec" | "secs" | "second" | "seconds" => *micros += value * 1e6,
        "m" | "min" | "mins" | "minute" | "minutes" => *micros += value * 6e7,
        "h" | "hr" | "hrs" | "hour" | "hours" => *micros += value * 3.6e9,
        "d" | "day" | "days" => {
            *days += value.trunc();
            *micros += value.fract() * 8.64e10;
        }
        "w" | "week" | "weeks" => {
            let d = value * 7.0;
            *days += d.trunc();
            *micros += d.fract() * 8.64e10;
        }
        "mon" | "mons" | "month" | "months" => {
            *months += value.trunc();
            let d = value.fract() * 30.0;
            *days += d.trunc();
            *micros += d.fract() * 8.64e10;
        }
        "y" | "yr" | "yrs" | "year" | "years" => *months += (value * 12.0).round(),
        "decade" | "decades" | "dec" | "decs" => *months += (value * 120.0).round(),
        "century" | "centuries" | "c" | "cent" => *months += (value * 1200.0).round(),
        "millennium" | "millennia" | "mil" | "mils" => *months += (value * 12000.0).round(),
        _ => return false,
    }
    true
}

fn parse_iso_interval(s: &str) -> Option<(f64, f64, f64)> {
    let mut acc = (0.0, 0.0, 0.0);
    let mut in_time = false;
    let mut numbuf = String::new();
    let mut any = false;
    for c in s[1..].chars() {
        match c {
            't' => {
                if in_time || !numbuf.is_empty() {
                    return None;
                }
                in_time = true;
            }
            '0'..='9' | '.' | '-' | '+' => numbuf.push(c),
            _ => {
                let v: f64 = numbuf.parse().ok()?;
                numbuf.clear();
                let unit = match (c, in_time) {
                    ('y', false) => "year",
                    ('m', false) => "month",
                    ('w', false) => "week",
                    ('d', false) => "day",
                    ('h', true) => "hour",
                    ('m', true) => "minute",
                    ('s', true) => "second",
                    _ => return None,
                };
                add_unit(&mut acc, v, unit);
                any = true;
            }
        }
    }
    if !numbuf.is_empty() || !any {
        return None;
    }
    Some(acc)
}

pub fn parse_interval(input: &str) -> Result<Interval> {
    let err = || interval_err(input);
    let lower = input.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return Err(err());
    }
    let acc = if lower.starts_with('p') && lower.len() > 1 && !lower.starts_with("pm") {
        parse_iso_interval(&lower).ok_or_else(err)?
    } else {
        let mut acc = (0.0f64, 0.0f64, 0.0f64);
        let mut tokens: Vec<&str> = lower.split_whitespace().collect();
        if tokens.first() == Some(&"@") {
            tokens.remove(0);
        }
        let ago = tokens.last() == Some(&"ago");
        if ago {
            tokens.pop();
        }
        if tokens.is_empty() {
            return Err(err());
        }
        let mut i = 0;
        while i < tokens.len() {
            let t = tokens[i];
            i += 1;
            if t.contains(':') {
                let (neg, body) = match t.as_bytes()[0] {
                    b'-' => (true, &t[1..]),
                    b'+' => (false, &t[1..]),
                    _ => (false, t),
                };
                let parts: Vec<&str> = body.split(':').collect();
                if parts.len() > 3 {
                    return Err(err());
                }
                let h: f64 = parts[0].parse().map_err(|_| err())?;
                let m: f64 = parts[1].parse().map_err(|_| err())?;
                let s: f64 = if parts.len() == 3 { parts[2].parse().map_err(|_| err())? } else { 0.0 };
                if m >= 60.0 || s >= 60.0 || h.fract() != 0.0 || m.fract() != 0.0 {
                    return Err(err());
                }
                let us = h * 3.6e9 + m * 6e7 + (s * 1e6).round();
                acc.2 += if neg { -us } else { us };
                continue;
            }
            // Number with an optional attached unit ("5min").
            let split = t
                .char_indices()
                .find(|(idx, c)| {
                    c.is_ascii_alphabetic()
                        && !(*c == 'e'
                            && *idx > 0
                            && t[idx + 1..].starts_with(|x: char| x.is_ascii_digit() || x == '-' || x == '+'))
                })
                .map_or(t.len(), |(idx, _)| idx);
            let (n, attached) = t.split_at(split);
            let value: f64 = n.parse().map_err(|_| err())?;
            if !value.is_finite() {
                return Err(err());
            }
            let unit = if !attached.is_empty() {
                attached
            } else if i < tokens.len() && tokens[i].as_bytes()[0].is_ascii_alphabetic() {
                i += 1;
                tokens[i - 1]
            } else if i < tokens.len() && tokens[i].contains(':') {
                "day"
            } else if i == tokens.len() {
                "second"
            } else {
                return Err(err());
            };
            if !add_unit(&mut acc, value, unit) {
                return Err(err());
            }
        }
        if ago {
            acc = (-acc.0, -acc.1, -acc.2);
        }
        acc
    };
    let (months, days, micros) = (acc.0, acc.1, acc.2.round());
    if months.abs() > i32::MAX as f64 || days.abs() > i32::MAX as f64 || micros.abs() > i64::MAX as f64 {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "interval out of range"));
    }
    Ok(Interval { months: months as i32, days: days as i32, micros: micros as i64 })
}

// ------------------------------------------------------------ arithmetic

pub fn add_months(y: i64, m: u32, d: u32, months: i64) -> (i64, u32, u32) {
    let total = y * 12 + (m as i64 - 1) + months;
    let ny = total.div_euclid(12);
    let nm = total.rem_euclid(12) as u32 + 1;
    (ny, nm, d.min(days_in_month(ny, nm)))
}

/// `timestamp + interval` on a zone-less timestamp.
pub fn ts_add_interval(ts: i64, iv: &Interval) -> Result<i64> {
    if !is_finite_ts(ts) {
        return Ok(ts);
    }
    let mut out = ts;
    if iv.months != 0 || iv.days != 0 {
        let tm = Tm::from_ts(ts);
        let (y, m, d) = add_months(tm.year, tm.month, tm.day, iv.months as i64);
        out = (days_from_civil(y, m, d) + iv.days as i64)
            .checked_mul(USECS_PER_DAY)
            .and_then(|v| v.checked_add(tm.time_micros()))
            .ok_or_else(ts_out_of_range)?;
    }
    out.checked_add(iv.micros).ok_or_else(ts_out_of_range)
}

/// `timestamptz + interval`: month and day steps follow the wall clock of
/// the session time zone.
pub fn tstz_add_interval(ts: i64, iv: &Interval, zone: &Zone) -> Result<i64> {
    if !is_finite_ts(ts) {
        return Ok(ts);
    }
    if iv.months == 0 && iv.days == 0 {
        return ts.checked_add(iv.micros).ok_or_else(ts_out_of_range);
    }
    let local = zone.utc_to_local(ts);
    let stepped = ts_add_interval(local, &Interval { months: iv.months, days: iv.days, micros: 0 })?;
    zone.local_to_utc(stepped).checked_add(iv.micros).ok_or_else(ts_out_of_range)
}

pub fn neg_interval(iv: &Interval) -> Result<Interval> {
    let e = || Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "interval out of range");
    Ok(Interval {
        months: iv.months.checked_neg().ok_or_else(e)?,
        days: iv.days.checked_neg().ok_or_else(e)?,
        micros: iv.micros.checked_neg().ok_or_else(e)?,
    })
}

/// `timestamp - timestamp`: days plus time, never months.
pub fn ts_diff(a: i64, b: i64) -> Result<Interval> {
    if !is_finite_ts(a) || !is_finite_ts(b) {
        return Err(Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "cannot subtract infinite timestamps"));
    }
    let d = a.checked_sub(b).ok_or_else(ts_out_of_range)?;
    Ok(Interval { months: 0, days: (d / USECS_PER_DAY) as i32, micros: d % USECS_PER_DAY })
}

/// Symbolic difference in years, months, days and time (`age(a, b)`).
pub fn age(a: i64, b: i64) -> Interval {
    if !is_finite_ts(a) || !is_finite_ts(b) {
        return Interval::default();
    }
    let (t1, t2) = (Tm::from_ts(a), Tm::from_ts(b));
    let neg = a < b;
    let sgn = if neg { -1i64 } else { 1 };
    let mut usec = sgn * (t1.micro as i64 - t2.micro as i64);
    let mut sec = sgn * (t1.second as i64 - t2.second as i64);
    let mut min = sgn * (t1.minute as i64 - t2.minute as i64);
    let mut hour = sgn * (t1.hour as i64 - t2.hour as i64);
    let mut day = sgn * (t1.day as i64 - t2.day as i64);
    let mut mon = sgn * (t1.month as i64 - t2.month as i64);
    let mut year = sgn * (t1.year - t2.year);
    while usec < 0 {
        usec += USECS_PER_SEC;
        sec -= 1;
    }
    while sec < 0 {
        sec += 60;
        min -= 1;
    }
    while min < 0 {
        min += 60;
        hour -= 1;
    }
    while hour < 0 {
        hour += 24;
        day -= 1;
    }
    while day < 0 {
        let borrow = if neg { days_in_month(t1.year, t1.month) } else { days_in_month(t2.year, t2.month) };
        day += borrow as i64;
        mon -= 1;
    }
    while mon < 0 {
        mon += 12;
        year -= 1;
    }
    Interval {
        months: (sgn * (year * 12 + mon)) as i32,
        days: (sgn * day) as i32,
        micros: sgn * (hour * USECS_PER_HOUR + min * USECS_PER_MIN + sec * USECS_PER_SEC + usec),
    }
}

pub fn interval_mul(iv: &Interval, f: f64) -> Result<Interval> {
    let e = || Error::new(sqlstate::DATETIME_FIELD_OVERFLOW, "interval out of range");
    let months = iv.months as f64 * f;
    let days = iv.days as f64 * f;
    if !months.is_finite() || months.abs() > i32::MAX as f64 || days.abs() > i32::MAX as f64 {
        return Err(e());
    }
    // Fractional months become days (30 per month), fractional days time.
    let month_rem_days = months.fract() * 30.0;
    let month_rem_days = (month_rem_days * 1e6).round() / 1e6;
    let day_total = days.trunc() + month_rem_days.trunc();
    let mut day_total = day_total;
    let sec_rem = (days.fract() + month_rem_days.fract()) * 86_400.0;
    let mut sec_rem = (sec_rem * 1e6).round() / 1e6;
    if sec_rem.abs() >= 86_400.0 {
        let whole = (sec_rem / 86_400.0).trunc();
        day_total += whole;
        sec_rem -= whole * 86_400.0;
    }
    let micros = (iv.micros as f64 * f + sec_rem * 1e6).round();
    if micros.abs() >= i64::MAX as f64 {
        return Err(e());
    }
    Ok(Interval { months: months.trunc() as i32, days: day_total as i32, micros: micros as i64 })
}

pub fn justify_hours(iv: &Interval) -> Interval {
    let mut days = iv.days as i64 + iv.micros / USECS_PER_DAY;
    let mut micros = iv.micros % USECS_PER_DAY;
    if days > 0 && micros < 0 {
        micros += USECS_PER_DAY;
        days -= 1;
    } else if days < 0 && micros > 0 {
        micros -= USECS_PER_DAY;
        days += 1;
    }
    Interval { months: iv.months, days: days as i32, micros }
}

pub fn justify_days(iv: &Interval) -> Interval {
    let mut months = iv.months as i64 + iv.days as i64 / 30;
    let mut days = iv.days as i64 % 30;
    if months > 0 && days < 0 {
        days += 30;
        months -= 1;
    } else if months < 0 && days > 0 {
        days -= 30;
        months += 1;
    }
    Interval { months: months as i32, days: days as i32, micros: iv.micros }
}

// ------------------------------------------------- extract / date_trunc

/// What a date/time field is extracted from.
pub enum Source {
    /// Local wall-clock timestamp, with the UTC offset in seconds when the
    /// source is a `timestamptz`.
    Ts(i64, Option<i32>),
    Date(i32),
    Time(i64),
    Interval(Interval),
}

fn unit_error(field: &str, type_name: &str, supported: bool) -> Error {
    if supported {
        Error::new(sqlstate::FEATURE_NOT_SUPPORTED, format!("unit \"{field}\" not supported for type {type_name}"))
    } else {
        Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("unit \"{field}\" not recognized for type {type_name}"))
    }
}

fn canonical_field(field: &str) -> String {
    let f = field.trim().to_ascii_lowercase();
    match f.as_str() {
        "years" | "y" | "yr" | "yrs" => "year",
        "months" | "mon" | "mons" => "month",
        "days" | "d" => "day",
        "hours" | "h" | "hr" | "hrs" => "hour",
        "minutes" | "min" | "mins" | "m" => "minute",
        "seconds" | "sec" | "secs" | "s" => "second",
        "millisecond" | "msec" | "msecs" | "ms" | "millisecon" => "milliseconds",
        "microsecond" | "usec" | "usecs" | "us" | "microsecon" => "microseconds",
        "weeks" | "w" => "week",
        "decades" | "dec" | "decs" => "decade",
        "centuries" | "c" | "cent" => "century",
        "millennia" | "mil" | "mils" => "millennium",
        "quarters" | "qtr" => "quarter",
        other => return other.to_string(),
    }
    .to_string()
}

const KNOWN_FIELDS: &[&str] = &[
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "milliseconds",
    "microseconds",
    "week",
    "decade",
    "century",
    "millennium",
    "quarter",
    "dow",
    "isodow",
    "doy",
    "isoyear",
    "epoch",
    "julian",
    "timezone",
    "timezone_hour",
    "timezone_minute",
];

fn dec(v: i64) -> Decimal {
    Decimal::from(v)
}

/// `EXTRACT(field FROM source)` as an exact numeric.
pub fn extract(field: &str, src: &Source, type_name: &str) -> Result<Decimal> {
    let f = canonical_field(field);
    let unsupported = || unit_error(field.trim(), type_name, KNOWN_FIELDS.contains(&f.as_str()));
    let secs = |sec: i64, micro: i64| Decimal::new(sec * USECS_PER_SEC + micro, 6);
    match src {
        Source::Ts(ts, off) => {
            if !is_finite_ts(*ts) {
                return Err(Error::new(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    "extract from infinite timestamp is not supported",
                ));
            }
            let tm = Tm::from_ts(*ts);
            Ok(match f.as_str() {
                "year" => dec(if tm.year > 0 { tm.year } else { tm.year - 1 }),
                "month" => dec(tm.month as i64),
                "day" => dec(tm.day as i64),
                "hour" => dec(tm.hour as i64),
                "minute" => dec(tm.minute as i64),
                "second" => secs(tm.second as i64, tm.micro as i64),
                "milliseconds" => Decimal::new(tm.second as i64 * USECS_PER_SEC + tm.micro as i64, 3),
                "microseconds" => dec(tm.second as i64 * USECS_PER_SEC + tm.micro as i64),
                "dow" => dec(tm.dow() as i64),
                "isodow" => dec(if tm.dow() == 0 { 7 } else { tm.dow() as i64 }),
                "doy" => dec(tm.doy() as i64),
                "week" => dec(tm.iso_week().1 as i64),
                "isoyear" => {
                    let y = tm.iso_week().0;
                    dec(if y > 0 { y } else { y - 1 })
                }
                "quarter" => dec((tm.month as i64 - 1) / 3 + 1),
                "decade" => dec(tm.year.div_euclid(10)),
                "century" => dec(if tm.year > 0 { (tm.year + 99) / 100 } else { -((1 - tm.year + 99) / 100) }),
                "millennium" => dec(if tm.year > 0 { (tm.year + 999) / 1000 } else { -((1 - tm.year + 999) / 1000) }),
                "epoch" => {
                    let utc = *ts - off.unwrap_or(0) as i64 * USECS_PER_SEC;
                    Decimal::from_i128_with_scale(utc as i128 + PG_EPOCH_UNIX_MICROS as i128, 6)
                }
                "julian" => {
                    let day = ts.div_euclid(USECS_PER_DAY) + 2_451_545;
                    let frac = ts.rem_euclid(USECS_PER_DAY);
                    if frac == 0 {
                        dec(day)
                    } else {
                        dec(day) + (Decimal::from(frac) / Decimal::from(USECS_PER_DAY))
                    }
                }
                "timezone" => dec(off.ok_or_else(unsupported)? as i64),
                "timezone_hour" => dec(off.ok_or_else(unsupported)? as i64 / 3600),
                "timezone_minute" => dec(off.ok_or_else(unsupported)? as i64 / 60 % 60),
                _ => return Err(unsupported()),
            })
        }
        Source::Date(d) => {
            match f.as_str() {
                "hour" | "minute" | "second" | "milliseconds" | "microseconds" | "timezone" | "timezone_hour"
                | "timezone_minute" => return Err(unsupported()),
                "epoch" => return Ok(dec(*d as i64 * 86_400 + PG_EPOCH_UNIX_MICROS / USECS_PER_SEC)),
                "julian" => return Ok(dec(*d as i64 + 2_451_545)),
                _ => {}
            }
            extract(field, &Source::Ts(date_to_ts(*d), None), type_name)
        }
        Source::Time(t) => Ok(match f.as_str() {
            "hour" => dec(t / USECS_PER_HOUR),
            "minute" => dec(t / USECS_PER_MIN % 60),
            "second" => Decimal::new(t % USECS_PER_MIN, 6),
            "milliseconds" => Decimal::new(t % USECS_PER_MIN, 3),
            "microseconds" => dec(t % USECS_PER_MIN),
            "epoch" => Decimal::new(*t, 6),
            _ => return Err(unsupported()),
        }),
        Source::Interval(iv) => {
            let (years, mons) = ((iv.months / 12) as i64, (iv.months % 12) as i64);
            let t = iv.micros;
            Ok(match f.as_str() {
                "year" => dec(years),
                "month" => dec(mons),
                "day" => dec(iv.days as i64),
                "hour" => dec(t / USECS_PER_HOUR),
                "minute" => dec(t / USECS_PER_MIN % 60),
                "second" => Decimal::new(t % USECS_PER_MIN, 6),
                "milliseconds" => Decimal::new(t % USECS_PER_MIN, 3),
                "microseconds" => dec(t % USECS_PER_MIN),
                "quarter" => dec(mons / 3 + 1),
                "decade" => dec(years / 10),
                "century" => dec(years / 100),
                "millennium" => dec(years / 1000),
                "epoch" => {
                    // A year counts as 365.25 days, a month as 30 days.
                    let secs = years as i128 * 31_557_600 + mons as i128 * 2_592_000 + iv.days as i128 * 86_400;
                    Decimal::from_i128_with_scale(secs * USECS_PER_SEC as i128 + t as i128, 6)
                }
                _ => return Err(unsupported()),
            })
        }
    }
}

/// Truncates a zone-less (or already localized) timestamp.
pub fn trunc_ts(field: &str, ts: i64, type_name: &str) -> Result<i64> {
    if !is_finite_ts(ts) {
        return Ok(ts);
    }
    let f = canonical_field(field);
    let mut tm = Tm::from_ts(ts);
    let level = match f.as_str() {
        "microseconds" => return Ok(ts),
        "milliseconds" => return Ok(ts - ts.rem_euclid(1000)),
        "second" => 0,
        "minute" => 1,
        "hour" => 2,
        "day" => 3,
        "week" => {
            let days = ts.div_euclid(USECS_PER_DAY);
            let monday = days - (days + 5).rem_euclid(7);
            return Ok(monday * USECS_PER_DAY);
        }
        "month" => 4,
        "quarter" => {
            tm.month = (tm.month - 1) / 3 * 3 + 1;
            4
        }
        "year" => 5,
        "decade" => {
            tm.year = tm.year.div_euclid(10) * 10;
            5
        }
        "century" => {
            tm.year = if tm.year > 0 { (tm.year - 1) / 100 * 100 + 1 } else { -((99 - tm.year) / 100 * 100) + 1 };
            5
        }
        "millennium" => {
            tm.year = if tm.year > 0 { (tm.year - 1) / 1000 * 1000 + 1 } else { -((999 - tm.year) / 1000 * 1000) + 1 };
            5
        }
        _ => return Err(unit_error(field.trim(), type_name, KNOWN_FIELDS.contains(&f.as_str()))),
    };
    tm.micro = 0;
    if level >= 1 {
        tm.second = 0;
    }
    if level >= 2 {
        tm.minute = 0;
    }
    if level >= 3 {
        tm.hour = 0;
    }
    if level >= 4 {
        tm.day = 1;
    }
    if level >= 5 {
        tm.month = 1;
    }
    tm.to_ts()
}

pub fn trunc_interval(field: &str, iv: &Interval) -> Result<Interval> {
    let f = canonical_field(field);
    let mut out = *iv;
    let months_to = |n: i32, out: &mut Interval| {
        out.months = out.months / n * n;
        out.days = 0;
        out.micros = 0;
    };
    match f.as_str() {
        "microseconds" => {}
        "milliseconds" => out.micros = out.micros / 1000 * 1000,
        "second" => out.micros = out.micros / USECS_PER_SEC * USECS_PER_SEC,
        "minute" => out.micros = out.micros / USECS_PER_MIN * USECS_PER_MIN,
        "hour" => out.micros = out.micros / USECS_PER_HOUR * USECS_PER_HOUR,
        "day" => out.micros = 0,
        "month" => months_to(1, &mut out),
        "quarter" => months_to(3, &mut out),
        "year" => months_to(12, &mut out),
        "decade" => months_to(120, &mut out),
        "century" => months_to(1200, &mut out),
        "millennium" => months_to(12000, &mut out),
        _ => return Err(unit_error(field.trim(), "interval", KNOWN_FIELDS.contains(&f.as_str()))),
    }
    Ok(out)
}
