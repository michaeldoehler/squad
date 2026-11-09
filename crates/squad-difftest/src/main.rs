//! Differential fuzzer for squad.
//!
//! Generates a random schema, data and statements from a seed, runs every
//! statement against an in-memory squad database and against a PostgreSQL
//! server, and reports every difference in outcome: success versus error,
//! SQLSTATE, column names, and the text of every value.

mod gen;

use std::fmt::Write as _;
use std::process::ExitCode;

use gen::{Gen, Only, Stmt};
use postgres::{Client, NoTls, SimpleQueryMessage};

/// What a statement produced on one side.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Rows { columns: Vec<String>, rows: Vec<Vec<Option<String>>> },
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Diff {
    None,
    /// Only one side raised a data-dependent error (overflow, division by
    /// zero, date out of range, invalid argument). Which rows and which
    /// unused columns an expression is evaluated for depends on the plan,
    /// so this is expected now and then.
    EvalOrder,
    /// Both sides failed, with different SQLSTATEs.
    ErrorCode,
    /// One side failed, or the results differ.
    Hard,
}

fn compare(a: &Outcome, b: &Outcome, ordered: bool) -> Diff {
    match (a, b) {
        // An internal error of the reference server says nothing about squad.
        (_, Outcome::Error(y)) if y == "XX000" => Diff::None,
        (Outcome::Error(x), Outcome::Error(y)) if x == y => Diff::None,
        (Outcome::Error(_), Outcome::Error(_)) => Diff::ErrorCode,
        (Outcome::Rows { columns: ca, rows: ra }, Outcome::Rows { columns: cb, rows: rb }) => {
            if ca != cb || ra.len() != rb.len() {
                return Diff::Hard;
            }
            let same = if ordered {
                // Rows that tie under the sort order may come in any
                // order; ties that are not identical rows only arise from
                // numerics that are equal but differ in scale.
                ra == rb || {
                    let (mut x, mut y) = (ra.clone(), rb.clone());
                    x.sort();
                    y.sort();
                    x == y && ra.iter().zip(rb).all(|(p, q)| p.iter().zip(q).all(|(c, d)| same_value(c, d)))
                }
            } else {
                let (mut x, mut y) = (ra.clone(), rb.clone());
                x.sort();
                y.sort();
                x == y
            };
            if same {
                Diff::None
            } else {
                Diff::Hard
            }
        }
        (Outcome::Error(code), Outcome::Rows { .. }) | (Outcome::Rows { .. }, Outcome::Error(code))
            if matches!(code.as_str(), "22003" | "22012" | "22008" | "2201F" | "2201E" | "22023") =>
        {
            Diff::EvalOrder
        }
        _ => Diff::Hard,
    }
}

/// Equal texts, or numbers that are equal apart from trailing zeros.
fn same_value(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) if x != y => {
            let trim = |s: &str| {
                if s.contains('.') {
                    s.trim_end_matches('0').trim_end_matches('.').to_string()
                } else {
                    s.to_string()
                }
            };
            x.parse::<f64>().is_ok() && y.parse::<f64>().is_ok() && trim(x) == trim(y)
        }
        _ => a == b,
    }
}

struct Squad {
    conn: squad::Connection,
    ctx: squad::FnCtx,
    /// Message of the most recent error, for reports.
    last_error: std::cell::RefCell<String>,
}

impl Squad {
    fn new() -> Squad {
        let db = squad::Database::open_in_memory().expect("open squad");
        Squad { conn: db.connect(), ctx: squad::FnCtx::default(), last_error: Default::default() }
    }

    fn run(&self, sql: &str) -> Outcome {
        // A panic in the engine is a finding, not a reason to stop.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.conn.simple_query(sql)));
        let result = match result {
            Ok(r) => r,
            Err(_) => return Outcome::Error("PANIC".to_string()),
        };
        match result {
            Err(e) => {
                *self.last_error.borrow_mut() = e.message().to_string();
                Outcome::Error(e.code().to_string())
            }
            Ok(results) => match results.into_iter().next_back() {
                Some(r) => Outcome::Rows {
                    columns: r.columns.iter().map(|c| c.name.clone()).collect(),
                    rows: r
                        .rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(
                                    |v| if v.is_null() { None } else { Some(squad::value::format_value(v, &self.ctx)) },
                                )
                                .collect()
                        })
                        .collect(),
                },
                None => Outcome::Rows { columns: Vec::new(), rows: Vec::new() },
            },
        }
    }
}

struct Postgres {
    client: Client,
}

impl Postgres {
    fn connect(conninfo: &str) -> Postgres {
        let mut admin = Client::connect(&format!("{conninfo} dbname=postgres"), NoTls).expect("connect to PostgreSQL");
        let db = format!("fuzz_difftest_{}", std::process::id());
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db}")).expect("drop scratch database");
        admin.batch_execute(&format!("CREATE DATABASE {db} TEMPLATE template0")).expect("create scratch database");
        let client = Client::connect(&format!("{conninfo} dbname={db}"), NoTls).expect("connect to scratch database");
        Postgres { client }
    }

    fn drop_database(conninfo: &str) {
        if let Ok(mut admin) = Client::connect(&format!("{conninfo} dbname=postgres"), NoTls) {
            let _ = admin.batch_execute(&format!("DROP DATABASE IF EXISTS fuzz_difftest_{}", std::process::id()));
        }
    }

    /// Back to an empty database.
    fn reset(&mut self) {
        let _ = self.client.batch_execute("ROLLBACK");
        self.client
            .batch_execute(
                "DROP SCHEMA public CASCADE; CREATE SCHEMA public; RESET ALL; SET TIME ZONE 'UTC'; \
                 SET statement_timeout = '20s'; SET client_min_messages = error",
            )
            .expect("reset scratch database");
    }

    fn run(&mut self, sql: &str) -> Outcome {
        match self.client.simple_query(sql) {
            Err(e) => Outcome::Error(e.code().map_or_else(|| format!("client: {e}"), |c| c.code().to_string())),
            Ok(messages) => {
                let mut columns = Vec::new();
                let mut rows = Vec::new();
                for m in messages {
                    match m {
                        SimpleQueryMessage::RowDescription(desc) => {
                            columns = desc.iter().map(|c| c.name().to_string()).collect();
                        }
                        SimpleQueryMessage::Row(row) => {
                            if columns.is_empty() {
                                columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                            }
                            rows.push((0..row.len()).map(|i| row.get(i).map(str::to_string)).collect());
                        }
                        _ => {}
                    }
                }
                Outcome::Rows { columns, rows }
            }
        }
    }
}

/// Runs a script on fresh databases and returns the outcomes of its last
/// statement.
fn replay(pg: &mut Postgres, script: &[(String, bool)]) -> (Outcome, Outcome, bool, String) {
    pg.reset();
    let squad = Squad::new();
    let mut last = (Outcome::Error("empty".into()), Outcome::Error("empty".into()), false);
    for (sql, ordered) in script {
        last = (squad.run(sql), pg.run(sql), *ordered);
    }
    let message = squad.last_error.borrow().clone();
    (last.0, last.1, last.2, message)
}

/// Removes statements from a failing script while its last statement keeps
/// showing a difference of the same severity (delta debugging).
fn minimize(pg: &mut Postgres, script: Vec<(String, bool)>, severity: Diff) -> Vec<(String, bool)> {
    let fails = |pg: &mut Postgres, s: &[(String, bool)]| {
        let (a, b, ordered, _) = replay(pg, s);
        compare(&a, &b, ordered) == severity
    };
    if !fails(pg, &script) {
        // Not reproducible from scratch (depends on aborted state); keep all.
        return script;
    }
    let mut current = script;
    let mut chunk = (current.len() / 2).max(1);
    let mut budget = 400;
    while chunk >= 1 && budget > 0 {
        let mut i = 0;
        let mut removed = false;
        while i + chunk < current.len() && budget > 0 {
            let mut candidate = current.clone();
            candidate.drain(i..i + chunk);
            budget -= 1;
            if fails(pg, &candidate) {
                current = candidate;
                removed = true;
            } else {
                i += chunk;
            }
        }
        if chunk == 1 && !removed {
            break;
        }
        chunk = if chunk == 1 { 1 } else { chunk / 2 };
    }
    current
}

fn render(o: &Outcome) -> String {
    match o {
        Outcome::Error(code) => format!("ERROR {code}"),
        Outcome::Rows { columns, rows } => {
            let mut s = format!("{}\n", columns.join("|"));
            for r in rows.iter().take(30) {
                let cells: Vec<&str> = r.iter().map(|c| c.as_deref().unwrap_or("NULL")).collect();
                writeln!(s, "{}", cells.join("|")).unwrap();
            }
            if rows.len() > 30 {
                writeln!(s, "... ({} rows)", rows.len()).unwrap();
            }
            s
        }
    }
}

struct Options {
    seed: u64,
    iterations: u64,
    statements: usize,
    only: Only,
    conninfo: String,
    strict: bool,
    minimize: bool,
    max_reports: usize,
    echo: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: squad-difftest [--seed N] [--iterations N] [--statements N] [--only select|dml|ddl|txn]\n\
         \x20                     [--pg CONNINFO] [--strict] [--no-minimize] [--max-reports N] [--echo]\n\n\
         CONNINFO defaults to host/port/user from PGHOST, PGPORT and PGUSER."
    );
    std::process::exit(2);
}

fn options() -> Options {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let mut o = Options {
        seed: 1,
        iterations: 10,
        statements: 200,
        only: Only::All,
        conninfo: format!(
            "host={} port={} user={}",
            env("PGHOST", "127.0.0.1"),
            env("PGPORT", "5432"),
            env("PGUSER", "postgres")
        ),
        strict: false,
        minimize: true,
        max_reports: 20,
        echo: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--seed" => o.seed = value().parse().unwrap_or_else(|_| usage()),
            "--iterations" => o.iterations = value().parse().unwrap_or_else(|_| usage()),
            "--statements" => o.statements = value().parse().unwrap_or_else(|_| usage()),
            "--only" => {
                o.only = match value().as_str() {
                    "select" => Only::Select,
                    "dml" => Only::Dml,
                    "ddl" => Only::Ddl,
                    "txn" => Only::Txn,
                    "all" => Only::All,
                    _ => usage(),
                }
            }
            "--pg" => o.conninfo = value(),
            "--strict" => o.strict = true,
            "--no-minimize" => o.minimize = false,
            "--echo" => o.echo = true,
            "--max-reports" => o.max_reports = value().parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
    }
    o
}

fn main() -> ExitCode {
    let opts = options();
    // Engine panics are caught and reported as outcome `PANIC`.
    std::panic::set_hook(Box::new(|_| {}));
    let mut pg = Postgres::connect(&opts.conninfo);
    let (mut total, mut hard, mut soft, mut both_error, mut eval_order) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut reports = 0;
    for it in 0..opts.iterations {
        let seed = opts.seed + it;
        let mut gen = Gen::new(seed, opts.only);
        pg.reset();
        let squad = Squad::new();
        let mut script: Vec<(String, bool)> = Vec::new();
        let mut pending: Vec<Stmt> = gen.setup().into_iter().map(|sql| Stmt::Other { sql }).collect();
        pending.reverse();
        let mut executed = 0;
        loop {
            let stmt = match pending.pop() {
                Some(s) => s,
                None if executed >= opts.statements => break,
                None => {
                    executed += 1;
                    gen.next()
                }
            };
            let ordered = matches!(&stmt, Stmt::Query { ordered: true, .. });
            let sql = stmt.sql().to_string();
            let (a, b) = (squad.run(&sql), pg.run(&sql));
            if opts.echo {
                println!("{sql};\n  -> {}", render(&a).lines().next().unwrap_or_default());
            }
            script.push((sql.clone(), ordered));
            total += 1;
            let diff = compare(&a, &b, ordered);
            if matches!((&a, &b), (Outcome::Error(_), Outcome::Error(_))) {
                both_error += 1;
                gen.note_failure_in_txn();
            }
            if diff == Diff::None {
                // Check the table a data-modifying statement touched.
                if let Stmt::Dml { table, .. } = &stmt {
                    if matches!(a, Outcome::Rows { .. }) {
                        pending.push(Stmt::Query { sql: format!("SELECT * FROM {table}"), ordered: false });
                    }
                }
                continue;
            }
            match diff {
                Diff::Hard => hard += 1,
                Diff::EvalOrder => eval_order += 1,
                _ => soft += 1,
            }
            if reports < opts.max_reports && (diff == Diff::Hard || (opts.strict && diff == Diff::ErrorCode)) {
                reports += 1;
                let minimal = if opts.minimize { minimize(&mut pg, script.clone(), diff) } else { script.clone() };
                let (ma, mb, _, message) = if opts.minimize {
                    replay(&mut pg, &minimal)
                } else {
                    (a.clone(), b.clone(), ordered, squad.last_error.borrow().clone())
                };
                println!(
                    "==== seed {seed}: {} ====",
                    if diff == Diff::Hard { "DIFFERENCE" } else { "SQLSTATE differs" }
                );
                for (s, _) in &minimal {
                    println!("{s};");
                }
                let detail = if matches!(ma, Outcome::Error(_)) { format!(" ({message})") } else { String::new() };
                println!("--- squad:\n{}{detail}\n--- postgres:\n{}", render(&ma).trim_end(), render(&mb).trim_end());
                println!();
            }
            // The two databases may have diverged: start the next iteration.
            break;
        }
    }
    Postgres::drop_database(&opts.conninfo);
    println!(
        "{total} statements, {hard} differences, {soft} SQLSTATE mismatches, {eval_order} one-sided arithmetic errors, \
         {both_error} statements failed identically on both sides"
    );
    if hard > 0 || (opts.strict && soft > 0) {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
