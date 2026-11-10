//! Columnar tables must answer every query exactly like an ordinary table
//! holding the same rows, whatever the split between delta and segments.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use squad::{sqlstate, Config, Connection, Database, FnCtx};

const COLUMNS: &str = "ts timestamp NOT NULL, project int NOT NULL, name text NOT NULL, user_id bigint, \
                       duration int, score double precision, revenue numeric(10,2), ok boolean, \
                       day date, id uuid, props jsonb, tags text[]";

fn setup(conn: &Connection) {
    conn.batch_execute(&format!(
        "CREATE TABLE plain ({COLUMNS});
         CREATE TABLE col ({COLUMNS}) USING columnar WITH (order_by = 'project, ts');"
    ))
    .unwrap();
}

/// Inserts the same pseudo-random rows into both tables.
fn load(conn: &Connection, from: i64, to: i64) {
    for table in ["plain", "col"] {
        conn.execute(
            &format!(
                "INSERT INTO {table}
                 SELECT timestamp '2024-01-01' + (g * interval '37 seconds'),
                        (g * 7) % 5,
                        (ARRAY['view', 'click', 'buy', 'signup', 'Ünïcode'])[1 + (g * 13) % 5],
                        CASE WHEN g % 11 = 0 THEN NULL ELSE (g * 31) % 300 END,
                        CASE WHEN g % 17 = 0 THEN NULL ELSE (g * 97) % 1000 - 100 END,
                        CASE WHEN g % 23 = 0 THEN NULL ELSE ((g * 53) % 2000) / 8.0 - 20 END,
                        CASE WHEN g % 19 = 0 THEN NULL ELSE ((g * 71) % 100000) / 100.0 END,
                        CASE g % 3 WHEN 0 THEN true WHEN 1 THEN false END,
                        date '2024-01-01' + (g % 40)::int,
                        ('00000000-0000-0000-0000-' || lpad((g % 50)::text, 12, '0'))::uuid,
                        jsonb_build_object('k', g % 4, 's', 'v' || (g % 3)),
                        ARRAY['t' || (g % 2), 'x']
                 FROM generate_series($1::bigint, $2::bigint) g"
            ),
            &[&from, &to],
        )
        .unwrap();
    }
}

const QUERIES: &[&str] = &[
    "SELECT count(*), count(user_id), count(duration), count(DISTINCT user_id), count(DISTINCT name) FROM {t}",
    "SELECT sum(duration), min(duration), max(duration), avg(duration) FROM {t}",
    "SELECT sum(score), min(score), max(score) FROM {t}",
    "SELECT sum(revenue), min(revenue), max(revenue), avg(revenue) FROM {t}",
    "SELECT min(ts), max(ts), min(day), max(day), min(name), max(name) FROM {t}",
    "SELECT name, count(*), sum(duration), avg(revenue) FROM {t} GROUP BY name ORDER BY name",
    "SELECT project, name, count(*), min(ts), max(user_id) FROM {t} GROUP BY project, name ORDER BY 1, 2",
    "SELECT date_trunc('day', ts) AS d, count(*), count(DISTINCT user_id) FROM {t} GROUP BY 1 ORDER BY 1",
    "SELECT date_trunc('hour', ts) AS h, sum(duration) FROM {t} WHERE project = 3 GROUP BY 1 ORDER BY 1 LIMIT 20",
    "SELECT date_trunc('week', ts), date_trunc('month', ts), count(*) FROM {t} GROUP BY 1, 2 ORDER BY 1, 2",
    "SELECT ok, count(*) FROM {t} GROUP BY ok ORDER BY ok NULLS FIRST",
    "SELECT day, count(*) FROM {t} WHERE day >= '2024-01-10' AND day < '2024-01-20' GROUP BY day ORDER BY day",
    "SELECT count(*) FROM {t} WHERE project = 2 AND ts >= '2024-01-03' AND ts < '2024-01-05 12:00:00'",
    "SELECT count(*) FROM {t} WHERE duration > 500 OR user_id IS NULL",
    "SELECT count(*) FROM {t} WHERE duration BETWEEN 0 AND 10 AND NOT (name = 'buy')",
    "SELECT count(*) FROM {t} WHERE name IN ('view', 'buy') AND user_id NOT IN (1, 2, 3)",
    "SELECT count(*) FROM {t} WHERE name LIKE 'c%' OR name LIKE '%up' OR name ILIKE 'ü%'",
    "SELECT count(*) FROM {t} WHERE ok AND revenue > 500.25",
    "SELECT count(*) FROM {t} WHERE ok IS NULL AND score <= 12.5",
    "SELECT count(*) FROM {t} WHERE id = '00000000-0000-0000-0000-000000000007'",
    "SELECT count(*), sum(duration) FROM {t} WHERE user_id = 77",
    "SELECT count(*) FROM {t} WHERE duration IS NOT NULL AND duration <> 5 AND duration >= -100",
    "SELECT count(*) FROM {t} WHERE false",
    "SELECT sum(duration) FROM {t} WHERE project = 99",
    "SELECT name, count(*) FROM {t} WHERE project = 99 GROUP BY name",
    // Shapes the column engine cannot run: they take the row path.
    "SELECT name, sum(duration + 1), count(*) FILTER (WHERE ok) FROM {t} GROUP BY name ORDER BY name",
    "SELECT props ->> 's' AS s, count(*) FROM {t} GROUP BY 1 ORDER BY 1",
    "SELECT count(*) FROM {t} WHERE props @> '{\"k\": 2}' AND tags[1] = 't1'",
    "SELECT upper(name), string_agg(DISTINCT project::text, ',' ORDER BY project::text) FROM {t} GROUP BY 1 ORDER BY 1",
    "SELECT ts, project, name, user_id, duration, score, revenue, ok, day, id, props, tags FROM {t} WHERE user_id = 5 AND project = 1 ORDER BY ts",
    "SELECT name, user_id FROM {t} WHERE duration = 777 ORDER BY ts, name LIMIT 5",
    "SELECT a.name, count(*) FROM {t} a JOIN {t} b ON a.user_id = b.user_id AND a.ts = b.ts WHERE a.project = 0 GROUP BY a.name ORDER BY 1",
    "SELECT name, rank() OVER (ORDER BY count(*) DESC, name) FROM {t} GROUP BY name ORDER BY 2",
    "SELECT count(*) FROM (SELECT DISTINCT user_id, name FROM {t}) s",
    "SELECT name, count(*) FROM {t} GROUP BY name HAVING count(*) > 10 ORDER BY count(*) DESC, name LIMIT 2",
];

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let ctx = FnCtx::default();
    conn.query(sql, &[])
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| {
            r.values().iter().map(|v| if v.is_null() { "NULL".into() } else { squad::value::format_value(v, &ctx) }).collect()
        })
        .collect()
}

fn compare_all(conn: &Connection, stage: &str) {
    for q in QUERIES {
        let expected = rows(conn, &q.replace("{t}", "plain"));
        let actual = rows(conn, &q.replace("{t}", "col"));
        assert_eq!(actual, expected, "{stage}: {q}");
    }
}

#[test]
fn columnar_tables_answer_like_ordinary_tables() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connect();
    setup(&conn);

    compare_all(&conn, "empty");
    load(&conn, 1, 3000);
    compare_all(&conn, "delta only");
    conn.batch_execute("VACUUM col").unwrap();
    compare_all(&conn, "one segment");
    load(&conn, 3001, 5000);
    compare_all(&conn, "segment and delta");
    conn.batch_execute("VACUUM col").unwrap();
    load(&conn, 5001, 5200);
    conn.batch_execute("VACUUM col").unwrap();
    load(&conn, 5201, 5300);
    compare_all(&conn, "several segments and delta");

    // Deletes rewrite the segments that hold matching rows.
    for predicate in ["ts < '2024-01-01 06:00:00'", "name = 'signup' AND duration > 300", "user_id IS NULL", "props ->> 's' = 'v1' AND project = 4"] {
        let expected = conn.execute(&format!("DELETE FROM plain WHERE {predicate}"), &[]).unwrap();
        let actual = conn.execute(&format!("DELETE FROM col WHERE {predicate}"), &[]).unwrap();
        assert_eq!(actual, expected, "DELETE WHERE {predicate}");
        compare_all(&conn, predicate);
    }

    // A rolled-back transaction leaves both parts untouched.
    conn.batch_execute("BEGIN; DELETE FROM col; INSERT INTO col (ts, project, name) VALUES ('2030-01-01', 1, 'x'); ROLLBACK").unwrap();
    compare_all(&conn, "after rollback");

    // The fast path is taken where it applies.
    let plan = rows(&conn, "EXPLAIN SELECT name, count(*) FROM col WHERE project = 1 GROUP BY name");
    assert!(plan[0][0].starts_with("Columnar"), "{plan:?}");

    conn.batch_execute("TRUNCATE col; TRUNCATE plain").unwrap();
    compare_all(&conn, "truncated");
}

#[test]
fn approximate_aggregates_are_close() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connect();
    conn.batch_execute("CREATE TABLE e (u bigint, d int) USING columnar").unwrap();
    conn.execute("INSERT INTO e SELECT g % 20000, g % 1000 FROM generate_series(1, 200000) g", &[]).unwrap();
    for stage in ["delta", "segments"] {
        let row = conn
            .query_one("SELECT approx_count_distinct(u), approx_percentile(d, 0.5), approx_percentile(d, 0.99) FROM e", &[])
            .unwrap();
        let (distinct, p50, p99): (i64, f64, f64) = (row.get(0), row.get(1), row.get(2));
        assert!((distinct - 20000).abs() < 1000, "{stage}: distinct estimate {distinct}");
        assert!((p50 - 500.0).abs() < 15.0, "{stage}: median estimate {p50}");
        assert!((p99 - 990.0).abs() < 15.0, "{stage}: p99 estimate {p99}");
        conn.batch_execute("VACUUM e").unwrap();
    }
    let err = conn.query("SELECT approx_percentile(d, 2) FROM e", &[]).unwrap_err();
    assert_eq!(err.code(), sqlstate::INVALID_PARAMETER_VALUE);
}

#[test]
fn restrictions_are_reported() {
    let conn = Database::open_in_memory().unwrap().connect();
    let unsupported = |sql: &str| assert_eq!(conn.batch_execute(sql).unwrap_err().code(), sqlstate::FEATURE_NOT_SUPPORTED, "{sql}");
    unsupported("CREATE TABLE c (id int PRIMARY KEY) USING columnar");
    unsupported("CREATE TABLE c (id int UNIQUE) USING columnar");
    unsupported("CREATE TABLE c (id int) USING nosuch");
    conn.batch_execute("CREATE TABLE c (id int NOT NULL CHECK (id > 0), v text DEFAULT 'd') USING columnar").unwrap();
    unsupported("UPDATE c SET v = 'x'");
    unsupported("CREATE INDEX ON c (id)");
    unsupported("ALTER TABLE c ADD COLUMN z int");
    unsupported("DELETE FROM c RETURNING id");
    // Row constraints still apply on insert.
    assert_eq!(conn.batch_execute("INSERT INTO c (id) VALUES (0)").unwrap_err().code(), sqlstate::CHECK_VIOLATION);
    assert_eq!(conn.batch_execute("INSERT INTO c (id) VALUES (NULL)").unwrap_err().code(), sqlstate::NOT_NULL_VIOLATION);
    conn.batch_execute("INSERT INTO c (id) VALUES (1)").unwrap();
    assert_eq!(rows(&conn, "SELECT id, v FROM c"), vec![vec!["1".to_string(), "d".to_string()]]);
}

#[test]
fn segments_survive_reopen_and_live_in_the_object_directory() {
    let dir = tempfile::tempdir().unwrap();
    let expected;
    {
        let db = Database::open(dir.path(), Config::default()).unwrap();
        let conn = db.connect();
        setup(&conn);
        load(&conn, 1, 4000);
        conn.batch_execute("VACUUM col").unwrap();
        load(&conn, 4001, 4500);
        expected = rows(&conn, "SELECT name, count(*), sum(duration), min(ts) FROM col GROUP BY name ORDER BY name");
    }
    let segments = std::fs::read_dir(dir.path().join("objects/columnar")).unwrap().count();
    assert_eq!(segments, 1, "one table directory in the object store");
    let db = Database::open(dir.path(), Config::default()).unwrap();
    let conn = db.connect();
    assert_eq!(rows(&conn, "SELECT name, count(*), sum(duration), min(ts) FROM col GROUP BY name ORDER BY name"), expected);
    compare_all(&conn, "after reopen");
    conn.batch_execute("DROP TABLE col").unwrap();
    let left: usize = walk(&dir.path().join("objects"));
    assert_eq!(left, 0, "dropping the table removes its segment objects");
}

fn walk(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries.flatten().map(|e| if e.path().is_dir() { walk(&e.path()) } else { 1 }).sum()
    })
}

#[test]
fn readers_see_consistent_totals_during_inserts_and_compaction() {
    let db = Database::open_in_memory().unwrap();
    db.connect().batch_execute("CREATE TABLE e (batch int NOT NULL, n int NOT NULL) USING columnar").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    // Every batch inserts 100 rows whose `n` sum to 4950, atomically.
    let writers: Vec<_> = (0..3)
        .map(|w| {
            let conn = db.connect();
            thread::spawn(move || {
                for b in 0..40 {
                    conn.execute("INSERT INTO e SELECT $1, g FROM generate_series(0, 99) g", &[&(w * 1000 + b)]).unwrap();
                }
            })
        })
        .collect();
    let compactor = {
        let (conn, stop) = (db.connect(), stop.clone());
        thread::spawn(move || {
            let mut runs = 0;
            while !stop.load(Ordering::Relaxed) {
                conn.batch_execute("VACUUM e").unwrap();
                runs += 1;
            }
            runs
        })
    };
    let reader = {
        let (conn, stop) = (db.connect(), stop.clone());
        thread::spawn(move || {
            let mut checks = 0;
            while !stop.load(Ordering::Relaxed) {
                // Both the column engine and the row path must see whole
                // batches only, each row exactly once.
                let fast = conn.query_one("SELECT count(*), sum(n), count(DISTINCT batch) FROM e", &[]).unwrap();
                let (count, sum, batches): (i64, Option<i64>, i64) = (fast.get(0), fast.get(1), fast.get(2));
                assert_eq!(count, batches * 100, "a row was lost or duplicated");
                assert_eq!(sum.unwrap_or(0), batches * 4950);
                let slow = conn.query_one("SELECT count(*), sum(n + 0) FROM e", &[]).unwrap();
                assert_eq!(slow.get::<i64>(0) % 100, 0);
                checks += 1;
            }
            checks
        })
    };
    writers.into_iter().for_each(|w| w.join().unwrap());
    stop.store(true, Ordering::Relaxed);
    assert!(compactor.join().unwrap() > 0);
    assert!(reader.join().unwrap() > 0);
    let conn = db.connect();
    conn.batch_execute("VACUUM e").unwrap();
    let row = conn.query_one("SELECT count(*), sum(n), count(DISTINCT batch) FROM e", &[]).unwrap();
    assert_eq!((row.get::<i64>(0), row.get::<i64>(1), row.get::<i64>(2)), (12000, 594000, 120));
}

#[test]
fn large_inserts_compact_automatically() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connect();
    conn.batch_execute("CREATE TABLE e (n int) USING columnar").unwrap();
    conn.execute("INSERT INTO e SELECT g FROM generate_series(1, 150000) g", &[]).unwrap();
    // The delta was moved into a segment by the inserting session.
    let plan = rows(&conn, "EXPLAIN ANALYZE SELECT count(*) FROM e");
    assert!(plan[0][0].starts_with("Columnar"), "{plan:?}");
    assert_eq!(conn.query_one("SELECT count(*), sum(n) FROM e", &[]).unwrap().get::<i64>(1), 150000 * 150001 / 2);
}
