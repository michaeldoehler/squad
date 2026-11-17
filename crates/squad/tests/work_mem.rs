//! `work_mem`: operators that exceed it spill to temporary files and give
//! the same results as in memory; what cannot spill fails with SQLSTATE
//! 53200; temporary files do not outlive their statement.

use std::path::Path;
use std::time::{Duration, Instant};

use squad::{Config, Connection, Database, FnCtx};

const SMALL: &str = "SET work_mem = '64kB'";
const LARGE: &str = "SET work_mem = '1GB'";

/// A table of 20000 rows with NULLs, duplicates and many types.
fn fill(conn: &Connection) {
    conn.batch_execute(
        "CREATE TABLE t (id int PRIMARY KEY, k int, txt text, num numeric, j jsonb, arr int[], ts timestamp, f float8);
         INSERT INTO t SELECT g,
             CASE WHEN g % 17 = 0 THEN NULL ELSE g % 500 END,
             CASE WHEN g % 13 = 0 THEN NULL ELSE 'v' || (g * 7919 % 1000) END,
             CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 97) / 7.0 END,
             CASE WHEN g % 19 = 0 THEN NULL ELSE jsonb_build_object('a', g % 50, 'b', 'x' || (g % 3)) END,
             CASE WHEN g % 23 = 0 THEN NULL ELSE ARRAY[g % 5, g % 7] END,
             timestamp '2020-01-01' + (g % 1000) * interval '1 hour',
             (g % 31) * 0.5
           FROM generate_series(1, 20000) g",
    )
    .unwrap();
}

fn render(conn: &Connection, sql: &str) -> Vec<String> {
    let ctx = FnCtx::default();
    let results = conn.simple_query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let result = results.last().unwrap();
    result
        .rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| if v.is_null() { "NULL".to_string() } else { squad::value::format_value(v, &ctx) })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn explain(conn: &Connection, sql: &str) -> String {
    render(conn, &format!("EXPLAIN ANALYZE {sql}")).join("\n")
}

/// Queries whose output order is fully determined (ties keep their
/// input order).
const ORDERED: &[&str] = &[
    "SELECT * FROM t ORDER BY txt NULLS FIRST, num DESC",
    "SELECT id, j, arr FROM t ORDER BY j DESC, arr NULLS FIRST",
    "SELECT ts, f, txt FROM t ORDER BY f, ts DESC",
    "SELECT * FROM t ORDER BY num NULLS FIRST LIMIT 50 OFFSET 15000",
    "SELECT DISTINCT ON (k) k, id, txt FROM t ORDER BY k, txt DESC",
    // Window partitions of at most 40 rows each fit into 64kB.
    "SELECT id, row_number() OVER (PARTITION BY id % 500 ORDER BY txt), sum(num) OVER (PARTITION BY id / 40 ORDER BY id),
            rank() OVER (PARTITION BY id % 500 ORDER BY ts), lag(txt) OVER (PARTITION BY id % 1000 ORDER BY id) FROM t",
    "SELECT k, count(*) FROM t GROUP BY k ORDER BY count(*) DESC, k",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 20000) SELECT n FROM r ORDER BY n DESC",
];

/// Queries whose row order is unspecified; their rows are compared sorted.
const UNORDERED: &[&str] = &[
    "SELECT id % 400, count(*), sum(num), avg(f), min(txt), max(ts), string_agg(txt, ','), array_agg(id),
            jsonb_agg(j), count(DISTINCT txt), stddev(num), bool_and(f > 3), sum(f) FROM t GROUP BY id % 400",
    "SELECT k, count(*), sum(num), avg(f), min(txt), max(ts), stddev(num), bool_or(f > 3), sum(f), sum(id)
       FROM t GROUP BY k",
    "SELECT txt, ts, count(*), json_object_agg(id, f), array_agg(arr) FROM t GROUP BY txt, ts",
    "SELECT j, arr, count(*) FROM t GROUP BY j, arr",
    "SELECT DISTINCT txt, arr FROM t",
    "SELECT DISTINCT j, num FROM t",
    "SELECT a.id, b.id FROM t a JOIN t b ON a.txt = b.txt AND a.id < b.id",
    "SELECT a.id, b.id FROM t a LEFT JOIN (SELECT * FROM t WHERE id % 10 = 0) b ON a.k = b.k",
    "SELECT a.id, b.id FROM (SELECT * FROM t WHERE id % 3 = 0) a RIGHT JOIN (SELECT * FROM t WHERE id % 5 = 0) b ON a.num = b.num",
    "SELECT a.id, b.id FROM (SELECT * FROM t WHERE id % 3 = 0) a FULL JOIN (SELECT * FROM t WHERE id % 7 = 0 AND id < 8000) b ON a.j = b.j AND a.id < 9000",
    "SELECT id FROM t a WHERE a.id % 10 = 0 AND EXISTS (SELECT 1 FROM t b WHERE b.k = a.k AND b.id > a.id + 10000)",
    "SELECT id FROM t a WHERE a.id % 50 = 0 AND NOT EXISTS (SELECT 1 FROM t b WHERE b.txt = a.txt AND b.id <> a.id)",
    "SELECT id FROM t WHERE txt IN (SELECT txt FROM t WHERE id % 7 = 0) AND num NOT IN (SELECT num FROM t WHERE id < 50 AND num IS NOT NULL)",
    "SELECT txt, arr FROM t INTERSECT SELECT txt, arr FROM t WHERE id % 2 = 0",
    "SELECT txt, k FROM t INTERSECT ALL SELECT txt, k FROM t WHERE id % 2 = 0",
    "SELECT txt, j FROM t EXCEPT SELECT txt, j FROM t WHERE id % 3 = 0",
    "SELECT k, num FROM t EXCEPT ALL SELECT k, num FROM t WHERE id % 3 = 0",
    "SELECT txt, num FROM t UNION SELECT txt, f::numeric FROM t",
    "SELECT a.id, b.id FROM (SELECT * FROM t WHERE id <= 1500) a JOIN (SELECT * FROM t WHERE id <= 1500) b ON a.id < b.id AND b.id < a.id + 3",
    "SELECT a.id, b.id FROM (SELECT * FROM t WHERE id <= 1500) a FULL JOIN (SELECT * FROM t WHERE id > 18000) b ON a.id + 18000 > b.id AND a.id % 7 = 0",
    "WITH c AS (SELECT * FROM t) SELECT x.id, y.txt FROM c x JOIN c y ON x.id = y.id + 1",
];

#[test]
fn spilled_results_equal_in_memory_results() {
    let conn = Database::open_in_memory().unwrap().connect();
    fill(&conn);
    for (queries, sorted) in [(ORDERED, false), (UNORDERED, true)] {
        for sql in queries {
            conn.batch_execute(LARGE).unwrap();
            let mut expected = render(&conn, sql);
            conn.batch_execute(SMALL).unwrap();
            let mut actual = render(&conn, sql);
            assert!(!expected.is_empty(), "{sql}");
            if sorted {
                expected.sort();
                actual.sort();
            }
            assert_eq!(actual.len(), expected.len(), "{sql}");
            if let Some(i) = (0..expected.len()).find(|&i| actual[i] != expected[i]) {
                panic!("{sql}\nrow {i}: {} (spilled) vs {} (in memory)", actual[i], expected[i]);
            }
        }
    }
}

#[test]
fn spilling_is_visible_in_explain_analyze() {
    let conn = Database::open_in_memory().unwrap().connect();
    fill(&conn);
    conn.batch_execute(SMALL).unwrap();
    let plan = explain(&conn, "SELECT * FROM t ORDER BY txt");
    assert!(plan.contains("Sort Method: external merge  Disk: "), "{plan}");
    let plan = explain(&conn, "SELECT txt, ts, count(*) FROM t GROUP BY txt, ts");
    assert!(plan.contains("Disk Usage: "), "{plan}");
    assert!(!plan.contains("Batches: 1 "), "{plan}");
    let plan = explain(&conn, "SELECT count(*) FROM t a JOIN t b ON a.txt = b.txt");
    assert!(plan.contains("Disk Usage: "), "{plan}");
    let plan = explain(&conn, "SELECT id, row_number() OVER (PARTITION BY id % 500 ORDER BY id) FROM t");
    assert!(plan.contains("Storage: Disk"), "{plan}");
    conn.batch_execute(LARGE).unwrap();
    let plan = explain(&conn, "SELECT * FROM t ORDER BY txt");
    assert!(plan.contains("Sort Method: quicksort  Memory: "), "{plan}");
    let plan = explain(&conn, "SELECT txt, ts, count(*) FROM t GROUP BY txt, ts");
    assert!(plan.contains("Batches: 1  Memory Usage: "), "{plan}");
    assert!(!plan.contains("Disk"), "{plan}");
}

#[test]
fn what_cannot_spill_fails_with_53200() {
    let conn = Database::open_in_memory().unwrap().connect();
    fill(&conn);
    conn.batch_execute(SMALL).unwrap();
    for sql in [
        // One window partition holds every row.
        "SELECT id, row_number() OVER (ORDER BY id) FROM t",
        // The state of a single group keeps growing.
        "SELECT array_agg(txt) FROM t",
        "SELECT count(DISTINCT id) FROM t",
    ] {
        let err = conn.query(sql, &[]).unwrap_err();
        assert_eq!(err.code(), "53200", "{sql}: {err}");
        assert!(err.message().contains("work_mem (64kB)"), "{}", err.message());
        // The connection keeps working, and a larger work_mem helps.
        conn.batch_execute("SET work_mem = '16MB'").unwrap();
        conn.query(sql, &[]).unwrap();
        conn.batch_execute(SMALL).unwrap();
    }
}

#[test]
fn work_mem_setting() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path(), Config { work_mem: 16 << 20, ..Config::default() }).unwrap();
    let conn = db.connect();
    let show = |conn: &Connection| conn.query_one("SHOW work_mem", &[]).unwrap().get::<String>(0);
    assert_eq!(show(&conn), "16MB");
    conn.batch_execute("SET work_mem = '64kB'").unwrap();
    assert_eq!(show(&conn), "64kB");
    conn.batch_execute("SET work_mem = 2048").unwrap();
    assert_eq!(show(&conn), "2MB");
    conn.batch_execute("RESET work_mem").unwrap();
    assert_eq!(show(&conn), "16MB");
    for bad in ["'63kB'", "'4 mb'", "'lots'", "-1"] {
        let err = conn.batch_execute(&format!("SET work_mem = {bad}")).unwrap_err();
        assert_eq!(err.code(), "22023", "{bad}");
    }
    assert!(db.set_work_mem(1000).is_err());
    db.set_work_mem(8 << 20).unwrap();
    assert_eq!(show(&db.connect()), "8MB");
    let current: String = conn.query_one("SELECT current_setting('work_mem')", &[]).unwrap().get(0);
    assert_eq!(current, "16MB");
    let n: i64 = conn.query_one("SELECT count(*) FROM pg_settings WHERE name = 'work_mem'", &[]).unwrap().get(0);
    assert_eq!(n, 1);
    assert_eq!(db.temp_dir(), dir.path().join("tmp"));
}

fn temp_files(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("squad_tmp_"))
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[test]
fn temporary_files_are_removed_after_success_error_and_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path(), Config::default()).unwrap();
    let tmp = db.temp_dir();
    let conn = db.connect();
    fill(&conn);
    conn.batch_execute(SMALL).unwrap();

    // Success.
    assert_eq!(render(&conn, "SELECT id FROM t ORDER BY txt, id LIMIT 1"), ["1000"]);
    assert_eq!(temp_files(&tmp), Vec::<String>::new());

    // An error while the sorted runs are being merged.
    let err = conn.query("SELECT 1 / (id - 15000) FROM (SELECT id FROM t ORDER BY txt, id) s", &[]).unwrap_err();
    assert_eq!(err.code(), "22012");
    assert_eq!(temp_files(&tmp), Vec::<String>::new());

    // Cancel while the files are in use: the nested loop reads the
    // merged runs slowly.
    let sql = "SELECT count(*) FROM (SELECT txt FROM t ORDER BY txt) s, generate_series(1, 100000) g";
    let worker = {
        let conn = conn.clone();
        std::thread::spawn(move || conn.query(sql, &[]))
    };
    let started = Instant::now();
    let mut seen = 0;
    while seen == 0 && !worker.is_finished() {
        seen = temp_files(&tmp).len();
        assert!(started.elapsed() < Duration::from_secs(60), "no temporary files appeared");
    }
    assert!(seen > 0, "the query ended before it wrote temporary files");
    while !worker.is_finished() {
        conn.cancel_handle().cancel();
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(worker.join().unwrap().unwrap_err().code(), "57014");
    assert_eq!(temp_files(&tmp), Vec::<String>::new());

    // statement_timeout ends the statement the same way.
    conn.batch_execute("SET statement_timeout = '300ms'").unwrap();
    assert_eq!(conn.query(sql, &[]).unwrap_err().code(), "57014");
    assert_eq!(temp_files(&tmp), Vec::<String>::new());
}

#[test]
fn leftover_temporary_files_are_removed_at_open() {
    let dir = tempfile::tempdir().unwrap();
    drop(Database::open(dir.path(), Config::default()).unwrap());
    let tmp = dir.path().join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(tmp.join("squad_tmp_4242_7"), b"left by a crash").unwrap();
    std::fs::write(tmp.join("unrelated"), b"kept").unwrap();
    let db = Database::open(dir.path(), Config::default()).unwrap();
    assert_eq!(temp_files(&tmp), Vec::<String>::new());
    assert!(tmp.join("unrelated").exists());

    // A configured directory is cleaned the same way.
    let other = tempfile::tempdir().unwrap();
    std::fs::write(other.path().join("squad_tmp_1_1"), b"old").unwrap();
    drop(db);
    let db =
        Database::open(dir.path(), Config { temp_dir: Some(other.path().to_path_buf()), ..Config::default() }).unwrap();
    assert_eq!(db.temp_dir(), other.path());
    assert_eq!(temp_files(other.path()), Vec::<String>::new());
    let conn = db.connect();
    fill(&conn);
    let sql = "SELECT count(*) FROM (SELECT DISTINCT txt, num FROM t) s";
    let expected = render(&conn, sql);
    conn.batch_execute(SMALL).unwrap();
    assert_eq!(render(&conn, sql), expected);
    assert!(explain(&conn, sql).contains("Disk Usage"));
}

#[test]
fn in_memory_databases_use_a_private_directory() {
    let db = Database::open_in_memory().unwrap();
    let tmp = db.temp_dir();
    let conn = db.connect();
    fill(&conn);
    conn.batch_execute(SMALL).unwrap();
    render(&conn, "SELECT * FROM t ORDER BY txt");
    assert!(tmp.exists());
    assert_eq!(temp_files(&tmp), Vec::<String>::new());
    drop(conn);
    drop(db);
    assert!(!tmp.exists());
}
