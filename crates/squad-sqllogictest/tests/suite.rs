//! Runs the sqllogictest scripts checked into `slt/`. Their expected
//! results were produced by PostgreSQL 16 (see `generate.py`).

use std::path::{Path, PathBuf};

use squad_sqllogictest::{collect_files, run_file};

#[test]
fn checked_in_suite_passes() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("slt");
    let mut files: Vec<PathBuf> = Vec::new();
    collect_files(&dir, &mut files);
    assert!(files.len() >= 10, "expected the suite in {}", dir.display());
    let mut failures = Vec::new();
    let mut passed = 0;
    for file in &files {
        let report = run_file(file, 20, false);
        if let Some(e) = &report.parse_error {
            failures.push(format!("{}: {e}", file.display()));
        }
        passed += report.passed;
        for f in report.failures {
            failures.push(format!("{}:{}\n{}\n{}", file.display(), f.line, f.sql, f.message));
        }
    }
    assert!(failures.is_empty(), "{} records failed:\n\n{}", failures.len(), failures.join("\n\n"));
    assert!(passed >= 400, "only {passed} records ran");
}

/// The conventions of the original (SQLite) corpus format: results listed
/// one value per line, rendering by type letter, result hashing, sort
/// modes and conditions.
#[test]
fn original_corpus_format() {
    let script = "\
statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c VARCHAR(10))

statement ok
INSERT INTO t1 VALUES(1, 20, 'x'), (2, 10, ''), (3, NULL, 'h\u{e9}llo')

query IIT nosort
SELECT a, b, c FROM t1 WHERE a > 1 ORDER BY a
----
2
10
(empty)
3
NULL
h@llo

query IR rowsort
SELECT a, b / 4.0 FROM t1 WHERE b IS NOT NULL
----
1
5.000
2
2.500

query I valuesort
SELECT a + b FROM t1 UNION ALL SELECT a FROM t1
----
1
12
2
21
3
NULL

query I
SELECT a * 1.9 FROM t1 ORDER BY a
----
1
3
5

query III rowsort label-x
SELECT a, a, a FROM t1
----
9 values hashing to f8d2c4b0e7a3f4c2c8d3d0c3ad3c7c0e

skipif postgresql
query I
SELECT this is not sql
----
1

onlyif sqlite # with a trailing comment
statement ok
PRAGMA whatever

onlyif postgresql
query T
SELECT 'ours'
----
ours

statement error
SELECT * FROM nosuch

halt

statement ok
THIS IS NEVER REACHED
";
    let dir = std::env::temp_dir().join(format!("squad-slt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("format.test");
    std::fs::write(&path, script).unwrap();
    let report = run_file(&path, 20, true);
    std::fs::remove_dir_all(&dir).ok();
    assert!(report.parse_error.is_none(), "{:?}", report.parse_error);
    // Everything passes except the hashed record, whose digest above is
    // deliberately wrong: hashing must be applied and compared.
    let failed: Vec<_> = report.failures.iter().map(|f| f.sql.as_str()).collect();
    assert_eq!(failed, ["SELECT a, a, a FROM t1"], "{:#?}", report.failures);
    assert!(report.failures[0].message.contains("9 values hashing to"), "{}", report.failures[0].message);
    assert_eq!((report.passed, report.failed, report.skipped), (8, 1, 2));
    assert_eq!(report.verdicts.len(), 11);
}
