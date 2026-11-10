//! `COPY` support: parsing of the statement and the text / CSV row codecs.
//!
//! This module is transport-neutral. A caller (the wire-protocol server, or
//! an embedding application) parses the statement with [`parse_copy`], then
//! either feeds incoming bytes to a [`CopyDecoder`] and inserts the decoded
//! rows with the statement from [`CopyRequest::insert_sql`], or runs
//! [`CopyRequest::select_sql`] and renders each result row with
//! [`encode_row`].

use pg_query::protobuf::{self as pb, node::Node as N};

use crate::bind::{deparse_stmt, parse_sql};
use crate::{sqlstate, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

/// Formatting options of a COPY statement, with defaults applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    pub delimiter: u8,
    /// The string that represents NULL.
    pub null: String,
    pub header: bool,
    /// CSV quote character.
    pub quote: u8,
    /// CSV escape character (inside quoted fields).
    pub escape: u8,
}

impl CopyOptions {
    pub fn text() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Text,
            delimiter: b'\t',
            null: "\\N".into(),
            header: false,
            quote: b'"',
            escape: b'"',
        }
    }

    pub fn csv() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Csv,
            delimiter: b',',
            null: String::new(),
            header: false,
            quote: b'"',
            escape: b'"',
        }
    }
}

/// A parsed `COPY ... FROM STDIN` or `COPY ... TO STDOUT` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyRequest {
    /// Quoted, possibly schema-qualified table name; `None` for
    /// `COPY (query) TO`.
    pub table: Option<String>,
    /// Column list as written; empty means all columns.
    pub columns: Vec<String>,
    /// SQL of the source query of `COPY (query) TO`.
    pub query: Option<String>,
    /// `true` for `COPY FROM`, `false` for `COPY TO`.
    pub from: bool,
    pub options: CopyOptions,
}

/// Double-quotes an identifier.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn def_text(arg: Option<&pb::Node>) -> Option<String> {
    match arg?.node.as_ref()? {
        N::String(s) => Some(s.sval.clone()),
        N::Integer(i) => Some(i.ival.to_string()),
        N::Float(f) => Some(f.fval.clone()),
        N::Boolean(b) => Some(if b.boolval { "true" } else { "false" }.to_string()),
        N::AConst(c) => match c.val.as_ref()? {
            pb::a_const::Val::Sval(s) => Some(s.sval.clone()),
            pb::a_const::Val::Ival(i) => Some(i.ival.to_string()),
            pb::a_const::Val::Boolval(b) => Some(if b.boolval { "true" } else { "false" }.to_string()),
            pb::a_const::Val::Fval(f) => Some(f.fval.clone()),
            pb::a_const::Val::Bsval(s) => Some(s.bsval.clone()),
        },
        _ => None,
    }
}

fn single_byte(name: &str, value: Option<String>) -> Result<u8> {
    match value.as_deref().map(str::as_bytes) {
        Some([b]) => Ok(*b),
        _ => Err(Error::new(
            sqlstate::INVALID_PARAMETER_VALUE,
            format!("COPY {name} must be a single one-byte character"),
        )),
    }
}

fn parse_bool(value: Option<String>) -> Result<bool> {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        None | Some("true" | "on" | "1" | "match") => Ok(true),
        Some("false" | "off" | "0") => Ok(false),
        Some(other) => Err(Error::syntax(format!("header requires a Boolean value, not \"{other}\""))),
    }
}

/// Parses `sql` as a COPY statement. Returns `Ok(None)` if `sql` is not
/// exactly one COPY statement, and an error for COPY variants that are not
/// supported (files, programs, binary format).
pub fn parse_copy(sql: &str) -> Result<Option<CopyRequest>> {
    let stmts = parse_sql(sql)?;
    let [raw] = stmts.as_slice() else { return Ok(None) };
    match raw.stmt.as_ref().and_then(|s| s.node.as_ref()) {
        Some(N::CopyStmt(stmt)) => copy_request(stmt).map(Some),
        _ => Ok(None),
    }
}

/// Builds the request from a parsed `CopyStmt` node.
pub fn copy_request(stmt: &pb::CopyStmt) -> Result<CopyRequest> {
    if stmt.is_program {
        return Err(Error::unsupported("COPY to or from a program"));
    }
    if !stmt.filename.is_empty() {
        return Err(Error::unsupported("COPY to or from a server-side file"));
    }
    if stmt.where_clause.is_some() {
        return Err(Error::unsupported("COPY ... WHERE"));
    }
    let mut format = CopyFormat::Text;
    let mut opts: Vec<(&str, Option<String>)> = Vec::new();
    for o in &stmt.options {
        let Some(N::DefElem(d)) = o.node.as_ref() else { continue };
        let value = def_text(d.arg.as_deref());
        if d.defname == "format" {
            format = match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
                Some("text") => CopyFormat::Text,
                Some("csv") => CopyFormat::Csv,
                Some("binary") => return Err(Error::unsupported("COPY in binary format")),
                other => {
                    return Err(Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        format!("COPY format \"{}\" not recognized", other.unwrap_or_default()),
                    ))
                }
            };
        } else {
            opts.push((d.defname.as_str(), value));
        }
    }
    let mut options = if format == CopyFormat::Csv { CopyOptions::csv() } else { CopyOptions::text() };
    let mut escape = None;
    for (name, value) in opts {
        match name {
            "delimiter" => options.delimiter = single_byte("delimiter", value)?,
            "null" => options.null = value.unwrap_or_default(),
            "header" => options.header = parse_bool(value)?,
            "quote" => options.quote = single_byte("quote", value)?,
            "escape" => escape = Some(single_byte("escape", value)?),
            "encoding" => {
                let v = value.unwrap_or_default().to_ascii_uppercase();
                if !matches!(v.as_str(), "UTF8" | "UTF-8" | "UNICODE") {
                    return Err(Error::unsupported(format!("COPY encoding \"{v}\"")));
                }
            }
            // FREEZE only affects visibility bookkeeping in Postgres.
            "freeze" => {}
            "force_quote" | "force_not_null" | "force_null" | "on_error" | "log_verbosity" | "default" => {
                return Err(Error::unsupported(format!("COPY option {name}")))
            }
            other => return Err(Error::syntax(format!("option \"{other}\" not recognized"))),
        }
    }
    options.escape = escape.unwrap_or(options.quote);

    let table = stmt.relation.as_ref().map(|rv| {
        if rv.schemaname.is_empty() {
            quote_ident(&rv.relname)
        } else {
            format!("{}.{}", quote_ident(&rv.schemaname), quote_ident(&rv.relname))
        }
    });
    let mut columns = Vec::with_capacity(stmt.attlist.len());
    for a in &stmt.attlist {
        if let Some(N::String(s)) = a.node.as_ref() {
            columns.push(s.sval.clone());
        }
    }
    let query = match &stmt.query {
        Some(q) => Some(deparse_stmt(q)?),
        None => None,
    };
    if stmt.is_from && table.is_none() {
        return Err(Error::syntax("COPY FROM requires a table"));
    }
    Ok(CopyRequest { table, columns, query, from: stmt.is_from, options })
}

impl CopyRequest {
    fn column_list(&self) -> String {
        if self.columns.is_empty() {
            "*".to_string()
        } else {
            self.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
    }

    /// The query whose rows `COPY TO` writes (also usable, with its result
    /// columns, to learn the column names of the table).
    pub fn select_sql(&self) -> String {
        match (&self.query, &self.table) {
            (Some(q), _) => q.clone(),
            (None, Some(t)) => format!("SELECT {} FROM {t}", self.column_list()),
            (None, None) => String::new(),
        }
    }

    /// An `INSERT` with one `$n` parameter per column in `columns`, for
    /// `COPY FROM`. Its parameter types are the column types.
    pub fn insert_sql(&self, columns: &[String], rows: usize) -> String {
        let table = self.table.as_deref().unwrap_or_default();
        let list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let mut sql = format!("INSERT INTO {table} ({list}) VALUES ");
        let mut n = 0;
        for r in 0..rows {
            if r > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for c in 0..columns.len() {
                if c > 0 {
                    sql.push_str(", ");
                }
                n += 1;
                sql.push('$');
                sql.push_str(&n.to_string());
            }
            sql.push(')');
        }
        sql
    }
}

fn bad_copy(msg: impl Into<String>) -> Error {
    Error::new("22P04", msg)
}

/// Incremental decoder of COPY input in text or CSV format.
pub struct CopyDecoder {
    opts: CopyOptions,
    buf: Vec<u8>,
    pos: usize,
    done: bool,
    skip_header: bool,
}

impl CopyDecoder {
    pub fn new(opts: &CopyOptions) -> CopyDecoder {
        CopyDecoder { opts: opts.clone(), buf: Vec::new(), pos: 0, done: false, skip_header: opts.header }
    }

    /// Appends received bytes.
    pub fn feed(&mut self, data: &[u8]) {
        if self.done {
            return;
        }
        if self.pos > 0 && self.pos >= self.buf.len() / 2 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
    }

    /// End of the next complete record: `(end of content, start of next)`.
    fn record_end(&self) -> Option<(usize, usize)> {
        let data = &self.buf[self.pos..];
        let csv = self.opts.format == CopyFormat::Csv;
        let mut in_quotes = false;
        let mut i = 0;
        while i < data.len() {
            let b = data[i];
            if csv {
                if in_quotes {
                    if b == self.opts.escape && self.opts.escape != self.opts.quote {
                        i += 2;
                        continue;
                    }
                    if b == self.opts.quote {
                        in_quotes = false;
                    }
                } else if b == self.opts.quote {
                    in_quotes = true;
                } else if b == b'\n' {
                    let end = if i > 0 && data[i - 1] == b'\r' { i - 1 } else { i };
                    return Some((self.pos + end, self.pos + i + 1));
                }
            } else if b == b'\\' {
                // An escaped byte never ends the line.
                i += 2;
                continue;
            } else if b == b'\n' {
                let end = if i > 0 && data[i - 1] == b'\r' { i - 1 } else { i };
                return Some((self.pos + end, self.pos + i + 1));
            }
            i += 1;
        }
        None
    }

    /// Returns the next complete row, or `None` if more input is needed
    /// (or the end marker `\.` was seen). `finish` must be true once the
    /// sender signalled the end, so that a last line without a newline is
    /// returned too.
    pub fn next_row(&mut self, finish: bool) -> Result<Option<Vec<Option<String>>>> {
        loop {
            if self.done {
                return Ok(None);
            }
            let (end, next) = match self.record_end() {
                Some(r) => r,
                None if finish && self.pos < self.buf.len() => (self.buf.len(), self.buf.len()),
                None => return Ok(None),
            };
            let record = &self.buf[self.pos..end];
            self.pos = next;
            if record == b"\\." {
                self.done = true;
                return Ok(None);
            }
            if self.skip_header {
                self.skip_header = false;
                continue;
            }
            let row = match self.opts.format {
                CopyFormat::Text => decode_text(record, &self.opts)?,
                CopyFormat::Csv => decode_csv(record, &self.opts)?,
            };
            return Ok(Some(row));
        }
    }
}

fn utf8(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes)
        .map_err(|_| Error::new(sqlstate::CHARACTER_NOT_IN_REPERTOIRE, "invalid byte sequence for encoding \"UTF8\""))
}

fn decode_text(line: &[u8], opts: &CopyOptions) -> Result<Vec<Option<String>>> {
    let mut row = Vec::new();
    let mut field: Vec<u8> = Vec::new();
    let mut raw_start = 0;
    let mut i = 0;
    let mut push = |field: &mut Vec<u8>, raw: &[u8]| -> Result<()> {
        if raw == opts.null.as_bytes() {
            field.clear();
            row.push(None);
        } else {
            row.push(Some(utf8(std::mem::take(field))?));
        }
        Ok(())
    };
    while i < line.len() {
        let b = line[i];
        if b == opts.delimiter {
            push(&mut field, &line[raw_start..i])?;
            raw_start = i + 1;
        } else if b == b'\\' {
            i += 1;
            let Some(&c) = line.get(i) else {
                field.push(b'\\');
                break;
            };
            match c {
                b'b' => field.push(8),
                b'f' => field.push(12),
                b'n' => field.push(b'\n'),
                b'r' => field.push(b'\r'),
                b't' => field.push(b'\t'),
                b'v' => field.push(11),
                b'0'..=b'7' => {
                    let mut v = (c - b'0') as u32;
                    let mut n = 1;
                    while n < 3 && matches!(line.get(i + 1), Some(b'0'..=b'7')) {
                        i += 1;
                        v = v * 8 + (line[i] - b'0') as u32;
                        n += 1;
                    }
                    field.push(v as u8);
                }
                b'x' if line.get(i + 1).is_some_and(u8::is_ascii_hexdigit) => {
                    let mut v = 0u32;
                    let mut n = 0;
                    while n < 2 && line.get(i + 1).is_some_and(u8::is_ascii_hexdigit) {
                        i += 1;
                        v = v * 16 + (line[i] as char).to_digit(16).unwrap();
                        n += 1;
                    }
                    field.push(v as u8);
                }
                // `\N` is handled by the raw comparison with the null
                // string; any other escaped byte stands for itself.
                other => field.push(other),
            }
        } else {
            field.push(b);
        }
        i += 1;
    }
    push(&mut field, &line[raw_start.min(line.len())..])?;
    Ok(row)
}

fn decode_csv(record: &[u8], opts: &CopyOptions) -> Result<Vec<Option<String>>> {
    let mut row = Vec::new();
    let mut i = 0;
    loop {
        let mut field: Vec<u8> = Vec::new();
        let mut quoted = false;
        loop {
            match record.get(i) {
                None => break,
                Some(&b) if b == opts.delimiter => break,
                Some(&b) if b == opts.quote => {
                    quoted = true;
                    i += 1;
                    loop {
                        let Some(&c) = record.get(i) else { return Err(bad_copy("unterminated CSV quoted field")) };
                        if c == opts.escape && record.get(i + 1).is_some_and(|n| *n == opts.quote || *n == opts.escape)
                        {
                            field.push(record[i + 1]);
                            i += 2;
                        } else if c == opts.quote {
                            i += 1;
                            break;
                        } else {
                            field.push(c);
                            i += 1;
                        }
                    }
                }
                Some(&b) => {
                    field.push(b);
                    i += 1;
                }
            }
        }
        if !quoted && field == opts.null.as_bytes() {
            row.push(None);
        } else {
            row.push(Some(utf8(field)?));
        }
        match record.get(i) {
            Some(_) => i += 1,
            None => break,
        }
    }
    Ok(row)
}

/// Appends one output row, including the line terminator.
pub fn encode_row(opts: &CopyOptions, fields: &[Option<&str>], out: &mut Vec<u8>) {
    for (n, f) in fields.iter().enumerate() {
        if n > 0 {
            out.push(opts.delimiter);
        }
        match (f, opts.format) {
            (None, _) => out.extend_from_slice(opts.null.as_bytes()),
            (Some(s), CopyFormat::Text) => {
                for &b in s.as_bytes() {
                    match b {
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        8 => out.extend_from_slice(b"\\b"),
                        12 => out.extend_from_slice(b"\\f"),
                        11 => out.extend_from_slice(b"\\v"),
                        b if b == opts.delimiter => {
                            out.push(b'\\');
                            out.push(b);
                        }
                        b => out.push(b),
                    }
                }
            }
            (Some(s), CopyFormat::Csv) => {
                let bytes = s.as_bytes();
                let needs_quote = bytes.is_empty()
                    || *s == opts.null
                    || *s == "\\."
                    || bytes.iter().any(|&b| b == opts.delimiter || b == opts.quote || b == b'\n' || b == b'\r')
                    || bytes.first() == Some(&b' ')
                    || bytes.last() == Some(&b' ');
                if !needs_quote {
                    out.extend_from_slice(bytes);
                } else {
                    out.push(opts.quote);
                    for &b in bytes {
                        if b == opts.quote || b == opts.escape {
                            out.push(opts.escape);
                        }
                        out.push(b);
                    }
                    out.push(opts.quote);
                }
            }
        }
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(opts: &CopyOptions, input: &[u8]) -> Vec<Vec<Option<String>>> {
        let mut d = CopyDecoder::new(opts);
        let mut rows = Vec::new();
        // Feed byte by byte to exercise incremental decoding.
        for b in input {
            d.feed(&[*b]);
            while let Some(r) = d.next_row(false).unwrap() {
                rows.push(r);
            }
        }
        while let Some(r) = d.next_row(true).unwrap() {
            rows.push(r);
        }
        rows
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    #[test]
    fn parses_statements() {
        let r = parse_copy("COPY public.t (a, \"B\") FROM STDIN").unwrap().unwrap();
        assert!(r.from);
        assert_eq!(r.table.as_deref(), Some("\"public\".\"t\""));
        assert_eq!(r.columns, vec!["a", "B"]);
        assert_eq!(r.options, CopyOptions::text());
        assert_eq!(
            r.insert_sql(&r.columns, 2),
            "INSERT INTO \"public\".\"t\" (\"a\", \"B\") VALUES ($1, $2), ($3, $4)"
        );

        let r = parse_copy("COPY t TO STDOUT WITH (FORMAT csv, HEADER, DELIMITER ';', NULL 'nil')").unwrap().unwrap();
        assert!(!r.from);
        assert_eq!(r.options.format, CopyFormat::Csv);
        assert!(r.options.header);
        assert_eq!(r.options.delimiter, b';');
        assert_eq!(r.options.null, "nil");
        assert_eq!(r.select_sql(), "SELECT * FROM \"t\"");

        let r = parse_copy("COPY (SELECT 1 AS x) TO STDOUT CSV HEADER").unwrap().unwrap();
        assert_eq!(r.options.format, CopyFormat::Csv);
        assert_eq!(r.select_sql(), "SELECT 1 AS x");

        assert!(parse_copy("SELECT 1").unwrap().is_none());
        assert!(parse_copy("COPY t FROM '/tmp/x'").is_err());
        assert!(parse_copy("COPY t TO STDOUT (FORMAT binary)").is_err());
    }

    #[test]
    fn text_roundtrip() {
        let opts = CopyOptions::text();
        let rows = decode_all(&opts, b"1\tab\\tc\t\\N\n2\t\\\\x\\n\t\n\\.\nignored\n");
        assert_eq!(rows, vec![vec![s("1"), s("ab\tc"), None], vec![s("2"), s("\\x\n"), s("")]]);
        let mut out = Vec::new();
        encode_row(&opts, &[Some("a\tb\\\n"), None, Some("")], &mut out);
        assert_eq!(out, b"a\\tb\\\\\\n\t\\N\t\n");
        assert_eq!(decode_all(&opts, &out), vec![vec![s("a\tb\\\n"), None, s("")]]);
        // Last line without newline, CRLF line ends, octal and hex escapes.
        assert_eq!(decode_all(&opts, b"a\r\n\\101\\x42"), vec![vec![s("a")], vec![s("AB")]]);
    }

    #[test]
    fn csv_roundtrip() {
        let mut opts = CopyOptions::csv();
        opts.header = true;
        let rows = decode_all(&opts, b"id,name\n1,\"a,\"\"b\"\"\nc\",\n2,\"\",x\n");
        assert_eq!(rows, vec![vec![s("1"), s("a,\"b\"\nc"), None], vec![s("2"), s(""), s("x")]]);
        let mut out = Vec::new();
        encode_row(&opts, &[Some("a,\"b\"\nc"), None, Some(""), Some("plain")], &mut out);
        assert_eq!(out, b"\"a,\"\"b\"\"\nc\",,\"\",plain\n");
        opts.header = false;
        assert_eq!(decode_all(&opts, &out), vec![vec![s("a,\"b\"\nc"), None, s(""), s("plain")]]);
    }
}
