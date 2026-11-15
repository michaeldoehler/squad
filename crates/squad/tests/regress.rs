//! SQL regression suite.
//!
//! Each `tests/regress/NAME.sql` script is run statement by statement
//! against a fresh in-memory database. The output, formatted like
//! `psql -a -A`, must equal `NAME.out`, which was produced by running the
//! same script against PostgreSQL 16 (see `scripts/regress-expected.sh`).
//! Errors are compared by SQLSTATE.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use squad::{Database, FnCtx};

fn run_script(script: &str) -> String {
    let db = Database::open_in_memory().expect("open in-memory database");
    let conn = db.connect();
    let ctx = FnCtx::default();
    let mut out = String::new();
    let mut statement = String::new();
    for line in script.lines() {
        out.push_str(line);
        out.push('\n');
        if statement.is_empty() && (line.trim().is_empty() || line.trim_start().starts_with("--")) {
            continue;
        }
        statement.push_str(line);
        statement.push('\n');
        if !line.trim_end().ends_with(';') {
            continue;
        }
        match conn.simple_query(&statement) {
            Ok(results) => {
                for r in results.iter().filter(|r| !r.columns.is_empty()) {
                    let header: Vec<&str> = r.columns.iter().map(|c| c.name.as_str()).collect();
                    writeln!(out, "{}", header.join("|")).unwrap();
                    for row in &r.rows {
                        let cells: Vec<String> = row
                            .iter()
                            .map(|v| if v.is_null() { "NULL".to_string() } else { squad::value::format_value(v, &ctx) })
                            .collect();
                        writeln!(out, "{}", cells.join("|")).unwrap();
                    }
                    let n = r.rows.len();
                    writeln!(out, "({n} row{})", if n == 1 { "" } else { "s" }).unwrap();
                }
            }
            Err(e) => writeln!(out, "ERROR:  {}", e.code()).unwrap(),
        }
        statement.clear();
    }
    out
}

/// Splits psql-style output into one block per statement so that a
/// mismatch can be reported together with the statement that caused it.
fn first_differences(expected: &str, actual: &str, limit: usize) -> String {
    let (exp, act): (Vec<&str>, Vec<&str>) = (expected.lines().collect(), actual.lines().collect());
    let mut report = String::new();
    let mut shown = 0;
    let (mut i, mut j) = (0, 0);
    let mut last_statement = 0;
    while i < exp.len() && j < act.len() && shown < limit {
        if exp[i] == act[j] {
            if exp[i].trim_end().ends_with(';') {
                last_statement = i;
            }
            i += 1;
            j += 1;
            continue;
        }
        // Report from the statement up to the next echoed statement.
        let end = |lines: &[&str], from: usize| {
            lines[from..].iter().position(|l| l.trim_end().ends_with(';')).map_or(lines.len(), |p| from + p)
        };
        let (ei, aj) = (end(&exp, i), end(&act, j));
        writeln!(report, "--- statement:\n{}", exp[last_statement..i.min(last_statement + 12)].join("\n")).unwrap();
        writeln!(report, "--- expected (PostgreSQL):\n{}", exp[i..ei].join("\n")).unwrap();
        writeln!(report, "--- actual (squad):\n{}\n", act[j..aj].join("\n")).unwrap();
        shown += 1;
        i = ei;
        j = aj;
    }
    report
}

fn check(name: &str) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/regress");
    let script = std::fs::read_to_string(dir.join(format!("{name}.sql"))).expect("read script");
    let expected = std::fs::read_to_string(dir.join(format!("{name}.out"))).expect("read expected output");
    let actual = run_script(&script);
    if actual != expected {
        let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/regress");
        std::fs::create_dir_all(&out_dir).ok();
        let path = out_dir.join(format!("{name}.out"));
        std::fs::write(&path, &actual).ok();
        panic!(
            "regression script {name}.sql differs from PostgreSQL (full output in {}):\n\n{}",
            path.display(),
            first_differences(&expected, &actual, 12)
        );
    }
}

macro_rules! regress {
    ($($name:ident),* $(,)?) => {$(
        #[test]
        fn $name() {
            check(stringify!($name));
        }
    )*};
}

regress!(basics, tables, joins, aggregates, subqueries, window, cte, types, ddl, transactions, misc, fuzz_misc, noops);
regress!(settings, rls);

/// Every script in the directory must be listed above.
#[test]
fn all_scripts_are_registered() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/regress");
    let source = include_str!("regress.rs");
    for entry in std::fs::read_dir(dir).expect("list scripts") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "sql") {
            let name = path.file_stem().unwrap().to_string_lossy().into_owned();
            assert!(
                source.contains(&format!("{name},")) || source.contains(&format!("{name})")),
                "script {name}.sql is not registered in tests/regress.rs"
            );
        }
    }
}
