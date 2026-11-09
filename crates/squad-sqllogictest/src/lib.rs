//! sqllogictest driver for squad.
//!
//! [`Squad`] implements the [`sqllogictest::DB`] trait on top of the
//! embedded API, and [`run_file`] runs one test script against a fresh
//! in-memory database, counting passed and failed records.
//!
//! Values are rendered the way the original SQLite sqllogictest harness
//! does, so the public corpus can be used unchanged:
//!
//! * the type letter of the expected column decides the rendering: `I`
//!   prints an integer, `R` a real with three decimals, `T` text;
//! * NULL prints as `NULL`, the empty string as `(empty)`, and characters
//!   outside printable ASCII as `@`;
//! * results may be listed one row per line or one value per line;
//! * conditions name the engine `postgresql`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use sqllogictest::{DBOutput, DefaultColumnType, QueryExpect, Record, Runner, DB};
use squad::{Connection, Database, FnCtx, Value};

/// Type letters of the query that is about to run, shared between the
/// driver loop (which knows the record) and the connection (which renders
/// the values).
type Expected = Arc<Mutex<Vec<DefaultColumnType>>>;

/// A squad connection speaking the sqllogictest protocol.
pub struct Squad {
    conn: Connection,
    ctx: FnCtx,
    expected: Expected,
}

impl Squad {
    /// Opens a fresh in-memory database.
    pub fn new() -> squad::Result<Squad> {
        Ok(Squad::on(&Database::open_in_memory()?, Expected::default()))
    }

    fn on(db: &Database, expected: Expected) -> Squad {
        Squad { conn: db.connect(), ctx: FnCtx::default(), expected }
    }
}

fn natural_type(v: &Value) -> DefaultColumnType {
    match v {
        Value::Int2(_) | Value::Int4(_) | Value::Int8(_) => DefaultColumnType::Integer,
        Value::Float4(_) | Value::Float8(_) | Value::Numeric(_) => DefaultColumnType::FloatingPoint,
        Value::Null => DefaultColumnType::Any,
        _ => DefaultColumnType::Text,
    }
}

/// Leading integer of a string, like C's `atoi`.
fn atoi(s: &str) -> i64 {
    let t = s.trim_start();
    let end = t.char_indices().take_while(|(i, c)| c.is_ascii_digit() || (*i == 0 && (*c == '-' || *c == '+'))).count();
    t[..end].parse().unwrap_or(0)
}

fn render_text(s: &str) -> String {
    if s.is_empty() {
        return "(empty)".to_string();
    }
    s.chars().map(|c| if (' '..='~').contains(&c) { c } else { '@' }).collect()
}

fn render(v: &Value, ty: &DefaultColumnType, ctx: &FnCtx) -> String {
    if v.is_null() {
        return "NULL".to_string();
    }
    match ty {
        DefaultColumnType::Integer => match v {
            Value::Bool(b) => (*b as i64).to_string(),
            Value::Float4(_) | Value::Float8(_) | Value::Numeric(_) => {
                // The reference harness reads the column as a C int.
                let f = v.as_f64().unwrap_or(0.0);
                (f.trunc() as i64).to_string()
            }
            Value::Text(s) => atoi(s).to_string(),
            other => match other.as_i64() {
                Some(i) => i.to_string(),
                None => render_text(&squad::value::format_value(other, ctx)),
            },
        },
        DefaultColumnType::FloatingPoint => match v {
            Value::Text(s) => format!("{:.3}", s.trim().parse::<f64>().unwrap_or(0.0)),
            Value::Bool(b) => format!("{:.3}", *b as i64 as f64),
            other => match other.as_f64() {
                Some(f) => format!("{f:.3}"),
                None => render_text(&squad::value::format_value(other, ctx)),
            },
        },
        _ => render_text(&squad::value::format_value(v, ctx)),
    }
}

impl DB for Squad {
    type Error = squad::Error;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, squad::Error> {
        let expected = std::mem::take(&mut *self.expected.lock().unwrap());
        let mut results = self.conn.simple_query(sql)?;
        let Some(result) = results.pop() else { return Ok(DBOutput::StatementComplete(0)) };
        if result.columns.is_empty() {
            return Ok(DBOutput::StatementComplete(result.rows_affected));
        }
        let width = result.columns.len();
        // Without usable type letters, render every value by its own type.
        let declared = expected.len() == width;
        let mut types: Vec<DefaultColumnType> = if declared { expected } else { vec![DefaultColumnType::Any; width] };
        let mut rows = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let mut out = Vec::with_capacity(width);
            for (i, v) in row.iter().enumerate() {
                if !declared && types[i] == DefaultColumnType::Any {
                    types[i] = natural_type(v);
                }
                let ty = if declared { types[i].clone() } else { natural_type(v) };
                out.push(render(v, &ty, &self.ctx));
            }
            rows.push(out);
        }
        Ok(DBOutput::Rows { types, rows })
    }

    fn engine_name(&self) -> &str {
        "postgresql"
    }

    fn error_sql_state(err: &squad::Error) -> Option<String> {
        Some(err.code().to_string())
    }
}

/// Accepts results listed one row per line (values separated by
/// whitespace) or one value per line, as the SQLite corpus does.
fn validator(normalizer: sqllogictest::Normalizer, actual: &[Vec<String>], expected: &[String]) -> bool {
    let expected: Vec<String> = expected.iter().map(normalizer).collect();
    let by_row = actual.len() == expected.len()
        && actual.iter().zip(&expected).all(|(row, e)| row.iter().map(normalizer).collect::<Vec<_>>().join(" ") == *e);
    by_row || actual.iter().flatten().map(normalizer).eq(expected.iter().cloned())
}

/// The original format allows a trailing comment on condition lines
/// (`skipif mysql # reason`), which the parser does not accept.
fn strip_condition_comments(script: &str) -> String {
    let mut out = String::with_capacity(script.len());
    for line in script.lines() {
        let conditional = line.starts_with("skipif ") || line.starts_with("onlyif ");
        match line.find(" #") {
            Some(pos) if conditional => out.push_str(line[..pos].trim_end()),
            _ => out.push_str(line),
        }
        out.push('\n');
    }
    out
}

/// One failed record.
#[derive(Debug, Clone)]
pub struct Failure {
    pub line: u32,
    pub sql: String,
    pub message: String,
}

/// Outcome of running one script.
#[derive(Debug, Clone, Default)]
pub struct FileReport {
    pub passed: usize,
    pub failed: usize,
    /// Records skipped by `skipif` / `onlyif` conditions.
    pub skipped: usize,
    /// The first failures, up to the requested limit.
    pub failures: Vec<Failure>,
    /// Set if the script could not be parsed at all.
    pub parse_error: Option<String>,
    /// Line and outcome (`P`assed, `F`ailed, `S`kipped) of every record,
    /// if requested.
    pub verdicts: Vec<(u32, char)>,
}

/// Runs a script against a fresh in-memory database. Every `statement`
/// and `query` record counts as passed or failed; a failure does not stop
/// the run.
pub fn run_file(path: &Path, keep_failures: usize, record_verdicts: bool) -> FileReport {
    let mut report = FileReport::default();
    let script = match std::fs::read(path) {
        // The corpus contains a few bytes that are not UTF-8.
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(e) => {
            report.parse_error = Some(e.to_string());
            return report;
        }
    };
    let name = path.to_string_lossy().into_owned();
    let records: Vec<Record<DefaultColumnType>> = match sqllogictest::parse_with_name(&strip_condition_comments(&script), name) {
        Ok(r) => r,
        Err(e) => {
            report.parse_error = Some(e.to_string());
            return report;
        }
    };
    let db = match Database::open_in_memory() {
        Ok(db) => db,
        Err(e) => {
            report.parse_error = Some(e.to_string());
            return report;
        }
    };
    let expected = Expected::default();
    let (factory_db, factory_expected) = (db.clone(), expected.clone());
    let mut runner = Runner::new(move || {
        let conn = Squad::on(&factory_db, factory_expected.clone());
        async move { Ok(conn) }
    });
    runner.with_validator(validator);
    // Scripts in the original format (`.test`) rely on the reference
    // harness hashing results of more than eight values by default.
    if path.extension().is_some_and(|e| e == "test") {
        runner.with_hash_threshold(8);
    }

    for record in records {
        let (line, sql, counted) = match &record {
            Record::Query { loc, sql, expected: expect, .. } => {
                *expected.lock().unwrap() = match expect {
                    QueryExpect::Results { types, .. } => types.clone(),
                    QueryExpect::Error(_) => Vec::new(),
                };
                (loc.line(), sql.clone(), true)
            }
            Record::Statement { loc, sql, .. } => {
                expected.lock().unwrap().clear();
                (loc.line(), sql.clone(), true)
            }
            Record::Halt { .. } => break,
            _ => (0, String::new(), false),
        };
        let outcome = runner.run(record);
        if record_verdicts && counted {
            report.verdicts.push((
                line,
                match &outcome {
                    Ok(sqllogictest::RecordOutput::Nothing) => 'S',
                    Ok(_) => 'P',
                    Err(_) => 'F',
                },
            ));
        }
        match outcome {
            Ok(sqllogictest::RecordOutput::Nothing) if counted => report.skipped += 1,
            Ok(_) if counted => report.passed += 1,
            Ok(_) => {}
            Err(e) => {
                report.failed += 1;
                if report.failures.len() < keep_failures {
                    report.failures.push(Failure { line, sql, message: e.kind().to_string() });
                }
            }
        }
    }
    report
}

/// All `.slt` and `.test` files below `path`, sorted.
pub fn collect_files(path: &Path, out: &mut Vec<std::path::PathBuf>) {
    if path.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path).into_iter().flatten().flatten().map(|e| e.path()).collect();
        entries.sort();
        for e in entries {
            collect_files(&e, out);
        }
    } else if path.extension().is_some_and(|e| e == "slt" || e == "test") {
        out.push(path.to_path_buf());
    }
}
