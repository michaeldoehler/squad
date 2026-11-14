//! Columnar tables must answer every query exactly like an ordinary table
//! holding the same rows, whatever the split between delta and segments.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use squad::object::ObjectStore as _;
use squad::{sqlstate, ColumnarConfig, Config, Connection, Database, FnCtx, Value};

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
    "SELECT props ->> 'k' AS k, count(*), sum(duration) FROM {t} WHERE props ->> 's' IN ('v0', 'v2') GROUP BY 1 ORDER BY 1",
    "SELECT count(*) FROM {t} WHERE props ->> 'k' = '2' OR props ->> 'nope' IS NOT NULL",
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
            r.values()
                .iter()
                .map(|v| if v.is_null() { "NULL".into() } else { squad::value::format_value(v, &ctx) })
                .collect()
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
    for predicate in [
        "ts < '2024-01-01 06:00:00'",
        "name = 'signup' AND duration > 300",
        "user_id IS NULL",
        "props ->> 's' = 'v1' AND project = 4",
    ] {
        let expected = conn.execute(&format!("DELETE FROM plain WHERE {predicate}"), &[]).unwrap();
        let actual = conn.execute(&format!("DELETE FROM col WHERE {predicate}"), &[]).unwrap();
        assert_eq!(actual, expected, "DELETE WHERE {predicate}");
        compare_all(&conn, predicate);
    }

    // A rolled-back transaction leaves both parts untouched.
    conn.batch_execute(
        "BEGIN; DELETE FROM col; INSERT INTO col (ts, project, name) VALUES ('2030-01-01', 1, 'x'); ROLLBACK",
    )
    .unwrap();
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
            .query_one(
                "SELECT approx_count_distinct(u), approx_percentile(d, 0.5), approx_percentile(d, 0.99) FROM e",
                &[],
            )
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
    let unsupported =
        |sql: &str| assert_eq!(conn.batch_execute(sql).unwrap_err().code(), sqlstate::FEATURE_NOT_SUPPORTED, "{sql}");
    unsupported("CREATE TABLE c (id int PRIMARY KEY) USING columnar");
    unsupported("CREATE TABLE c (id int UNIQUE) USING columnar");
    unsupported("CREATE TABLE c (id int) USING nosuch");
    conn.batch_execute("CREATE TABLE c (id int NOT NULL CHECK (id > 0), v text DEFAULT 'd') USING columnar").unwrap();
    unsupported("UPDATE c SET v = 'x'");
    unsupported("CREATE INDEX ON c (id)");
    unsupported("ALTER TABLE c ALTER COLUMN v TYPE int");
    unsupported("ALTER TABLE c ADD COLUMN z serial");
    unsupported("ALTER TABLE c ADD COLUMN z float8 DEFAULT random()");
    unsupported("ALTER TABLE c ADD CONSTRAINT k CHECK (id < 100)");
    unsupported("SELECT * FROM c FOR UPDATE");
    unsupported("DELETE FROM c RETURNING id");
    // Row constraints still apply on insert.
    assert_eq!(conn.batch_execute("INSERT INTO c (id) VALUES (0)").unwrap_err().code(), sqlstate::CHECK_VIOLATION);
    assert_eq!(
        conn.batch_execute("INSERT INTO c (id) VALUES (NULL)").unwrap_err().code(),
        sqlstate::NOT_NULL_VIOLATION
    );
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
    assert_eq!(
        rows(&conn, "SELECT name, count(*), sum(duration), min(ts) FROM col GROUP BY name ORDER BY name"),
        expected
    );
    compare_all(&conn, "after reopen");
    conn.batch_execute("DROP TABLE col").unwrap();
    let left: usize = walk(&dir.path().join("objects"));
    assert_eq!(left, 0, "dropping the table removes its segment objects");
}

fn walk(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map_or(0, |entries| entries.flatten().map(|e| if e.path().is_dir() { walk(&e.path()) } else { 1 }).sum())
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
                    conn.execute("INSERT INTO e SELECT $1, g FROM generate_series(0, 99) g", &[&(w * 1000 + b)])
                        .unwrap();
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
    // Batches below the bulk-insert size go to the delta; the background
    // compactor moves them into segments.
    for b in 0..5 {
        conn.execute("INSERT INTO e SELECT g FROM generate_series($1 * 30000 + 1, ($1 + 1) * 30000) g", &[&(b as i64)])
            .unwrap();
    }
    assert!(db.wait_for_compaction(std::time::Duration::from_secs(60)));
    let plan = rows(&conn, "EXPLAIN ANALYZE SELECT count(*) FROM e");
    assert!(plan[0][0].starts_with("Columnar"), "{plan:?}");
    assert_eq!(conn.query_one("SELECT count(*), sum(n) FROM e", &[]).unwrap().get::<i64>(1), 150000 * 150001 / 2);
}

/// Settings for tests that decide themselves when compaction runs.
fn manual() -> ColumnarConfig {
    ColumnarConfig { background_compaction: false, ..ColumnarConfig::default() }
}

fn segment_objects(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    fn go(d: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            if e.path().is_dir() {
                go(&e.path(), out);
            } else {
                out.push(e.path().to_string_lossy().into_owned());
            }
        }
    }
    go(&dir.join("objects/columnar"), &mut out);
    out.sort();
    out
}

const ALTERED_QUERIES: &[&str] = &[
    "SELECT count(*), count(extra), sum(extra), min(extra), max(extra), count(note), count(DISTINCT note) FROM {t}",
    "SELECT event, extra, count(*), sum(user_id) FROM {t} GROUP BY event, extra ORDER BY 1, 2",
    "SELECT count(*) FROM {t} WHERE extra = 5",
    "SELECT count(*) FROM {t} WHERE extra IS NULL OR note IS NULL",
    "SELECT count(*) FROM {t} WHERE note = 'n3' AND extra > 5",
    "SELECT note, count(*) FROM {t} GROUP BY note ORDER BY note NULLS FIRST",
    "SELECT event, project, extra, note, ts FROM {t} WHERE user_id = 17 ORDER BY ts, event",
    "SELECT date_trunc('day', ts), sum(extra) FROM {t} GROUP BY 1 ORDER BY 1",
    "SELECT count(*) FROM {t} WHERE event LIKE 'c%' AND extra + 0 = 5",
];

fn compare_altered(conn: &Connection, stage: &str) {
    for q in ALTERED_QUERIES {
        let expected = rows(conn, &q.replace("{t}", "plain"));
        let actual = rows(conn, &q.replace("{t}", "col"));
        assert_eq!(actual, expected, "{stage}: {q}");
    }
}

#[test]
fn alter_table_adds_drops_and_renames_columns_without_rewriting_segments() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path(), Config { columnar: manual(), ..Config::default() }).unwrap();
    let conn = db.connect();
    setup(&conn);
    load(&conn, 1, 3000);
    conn.batch_execute("VACUUM col").unwrap();
    load(&conn, 3001, 3500);
    let before = segment_objects(dir.path());
    assert_eq!(before.len(), 1);

    for sql in [
        "ALTER TABLE {t} ADD COLUMN extra int DEFAULT 5",
        "ALTER TABLE {t} ADD COLUMN note text",
        "ALTER TABLE {t} DROP COLUMN duration",
        "ALTER TABLE {t} RENAME COLUMN name TO event",
        "ALTER TABLE {t} DROP COLUMN tags",
        "ALTER TABLE {t} ALTER COLUMN extra SET DEFAULT 6",
    ] {
        for t in ["plain", "col"] {
            conn.batch_execute(&sql.replace("{t}", t)).unwrap();
        }
    }
    // No segment was rewritten.
    assert_eq!(segment_objects(dir.path()), before);
    compare_altered(&conn, "after ALTER");
    // Adding a NOT NULL column without default fails on a non-empty table.
    for t in ["plain", "col"] {
        let err = conn.batch_execute(&format!("ALTER TABLE {t} ADD COLUMN must int NOT NULL")).unwrap_err();
        assert_eq!(err.code(), sqlstate::NOT_NULL_VIOLATION, "{t}");
    }
    // Rows written after the change have the new columns.
    for t in ["plain", "col"] {
        conn.execute(
            &format!(
                "INSERT INTO {t} (ts, project, event, user_id, note)
                 SELECT timestamp '2024-03-01' + g * interval '1 minute', g % 5, 'click', g % 40, 'n' || (g % 7)
                 FROM generate_series(1, 400) g"
            ),
            &[],
        )
        .unwrap();
        conn.execute(&format!("INSERT INTO {t} (ts, project, event, extra) VALUES ('2024-03-02', 1, 'buy', 9)"), &[])
            .unwrap();
    }
    compare_altered(&conn, "delta after ALTER");
    // Segments of different column layouts are merged into one.
    conn.batch_execute("VACUUM col").unwrap();
    compare_altered(&conn, "compacted after ALTER");
    for _ in 0..8 {
        for t in ["plain", "col"] {
            conn.execute(
                &format!("INSERT INTO {t} (ts, project, event, extra, note) VALUES (now(), 2, 'view', 1, 'n1')"),
                &[],
            )
            .unwrap();
        }
        conn.batch_execute("VACUUM col").unwrap();
    }
    compare_altered(&conn, "merged");
    // Dropping and re-adding a column of the same name does not bring old
    // values back.
    for t in ["plain", "col"] {
        conn.batch_execute(&format!("ALTER TABLE {t} DROP COLUMN note; ALTER TABLE {t} ADD COLUMN note text")).unwrap();
    }
    compare_altered(&conn, "re-added");
    drop(conn);
    drop(db);
    let db = Database::open(dir.path(), Config { columnar: manual(), ..Config::default() }).unwrap();
    compare_altered(&db.connect(), "after reopen");
}

/// Queries on a `jsonb` column whose filters and groupings the column
/// engine runs.
const JSON_QUERIES: &[&str] = &[
    "SELECT count(*) FROM {t} WHERE props ->> 's' = 'v1'",
    "SELECT count(*) FROM {t} WHERE 'v2' = props ->> 's' AND project = 3",
    "SELECT count(*) FROM {t} WHERE props ->> 'k' IN ('1', '3')",
    "SELECT count(*) FROM {t} WHERE props ->> 'k' NOT IN ('1', '3')",
    "SELECT count(*) FROM {t} WHERE props ->> 'k' <> '2'",
    "SELECT count(*) FROM {t} WHERE props ->> 'missing' IS NULL",
    "SELECT count(*) FROM {t} WHERE props ->> 'n' IS NOT NULL",
    "SELECT count(*) FROM {t} WHERE props ->> 'n' IS NULL AND props ->> 's' = 'v0'",
    "SELECT count(*) FROM {t} WHERE props ->> 'o' = '{\"x\": [1, \"a\"]}'",
    "SELECT count(*) FROM {t} WHERE props ->> 'q' = 'say \"hi\"'",
    "SELECT props ->> 's', count(*), sum(duration) FROM {t} GROUP BY 1 ORDER BY 1 NULLS FIRST",
    "SELECT props ->> 'k', props ->> 's', count(*) FROM {t} WHERE props ->> 'n' IS NULL GROUP BY 1, 2 ORDER BY 1, 2",
    "SELECT project, props ->> 'missing', count(*) FROM {t} GROUP BY 1, 2 ORDER BY 1, 2",
    "SELECT ts, props FROM {t} WHERE props ->> 's' = 'v1' AND user_id = 7 ORDER BY ts",
    "SELECT count(*) FROM {t} WHERE props ->> 's' = $1",
];

#[test]
fn jsonb_filters_and_groupings_match_the_row_engine() {
    let db = Database::open_in_memory_with(manual()).unwrap();
    let conn = db.connect();
    setup(&conn);
    for t in ["plain", "col"] {
        conn.execute(
            &format!(
                "INSERT INTO {t} (ts, project, name, user_id, duration, props)
                 SELECT timestamp '2024-01-01' + g * interval '1 minute', g % 5, 'e', g % 50, g % 100,
                        CASE WHEN g % 13 = 0 THEN NULL
                             WHEN g % 17 = 0 THEN '[1, 2]'::jsonb
                             WHEN g % 19 = 0 THEN '\"scalar\"'::jsonb
                             ELSE jsonb_build_object('k', g % 4, 's', 'v' || (g % 3),
                                                     'n', CASE WHEN g % 5 = 0 THEN NULL ELSE g END,
                                                     'o', CASE WHEN g % 7 = 0 THEN '{{\"x\": [1, \"a\"]}}'::jsonb END,
                                                     'q', CASE WHEN g % 11 = 0 THEN 'say \"hi\"' END)
                        END
                 FROM generate_series(1, 6000) g"
            ),
            &[],
        )
        .unwrap();
    }
    let check = |stage: &str| {
        for q in JSON_QUERIES {
            let run = |t: &str| -> Vec<Vec<String>> {
                let sql = q.replace("{t}", t);
                let result = if sql.contains("$1") { conn.query(&sql, &[&"v2"]) } else { conn.query(&sql, &[]) };
                let ctx = FnCtx::default();
                result
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .iter()
                    .map(|r| r.values().iter().map(|v| squad::value::format_value(v, &ctx)).collect())
                    .collect()
            };
            assert_eq!(run("col"), run("plain"), "{stage}: {q}");
        }
    };
    check("delta");
    conn.batch_execute("VACUUM col").unwrap();
    check("segments");
    let plan = rows(&conn, "EXPLAIN SELECT props ->> 's', count(*) FROM col WHERE props ->> 'k' = '1' GROUP BY 1");
    assert!(plan[0][0].starts_with("Columnar"), "{plan:?}");
}

/// An object store that counts what is read from segment objects.
struct Counting {
    inner: squad::object::MemoryStore,
    gets: AtomicU64,
    ranges: AtomicU64,
    bytes: AtomicU64,
    keys: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl Counting {
    fn new() -> Arc<Counting> {
        Arc::new(Counting {
            inner: squad::object::MemoryStore::new(),
            gets: AtomicU64::new(0),
            ranges: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            keys: Default::default(),
        })
    }
    fn reset(&self) -> (u64, u64, u64, usize) {
        let keys = std::mem::take(&mut *self.keys.lock().unwrap()).len();
        (
            self.gets.swap(0, Ordering::SeqCst),
            self.ranges.swap(0, Ordering::SeqCst),
            self.bytes.swap(0, Ordering::SeqCst),
            keys,
        )
    }
    fn segment_bytes(&self) -> u64 {
        self.inner.list("columnar/").unwrap().iter().map(|m| m.size).sum()
    }
}

impl squad::object::ObjectStore for Counting {
    fn put(&self, key: &str, data: &[u8]) -> squad::object::Result<()> {
        self.inner.put(key, data)
    }
    fn get(&self, key: &str) -> squad::object::Result<Option<Vec<u8>>> {
        if key.starts_with("columnar/") {
            self.gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> squad::object::Result<Vec<u8>> {
        if key.starts_with("columnar/") {
            self.ranges.fetch_add(1, Ordering::SeqCst);
            self.bytes.fetch_add(len, Ordering::SeqCst);
            self.keys.lock().unwrap().insert(key.to_string());
        }
        self.inner.get_range(key, offset, len)
    }
    fn head(&self, key: &str) -> squad::object::Result<Option<squad::object::ObjectMeta>> {
        self.inner.head(key)
    }
    fn list(&self, prefix: &str) -> squad::object::Result<Vec<squad::object::ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> squad::object::Result<()> {
        self.inner.delete(key)
    }
}

#[test]
fn segments_are_read_in_ranges_and_pruned_by_manifest_statistics() {
    let store = Counting::new();
    let engine: Arc<dyn squad_storage::Engine> = Arc::new(squad_storage::MemEngine::new());
    let open = || Database::with_engine_and_objects(engine.clone(), store.clone(), manual()).unwrap();
    {
        let db = open();
        let conn = db.connect();
        conn.batch_execute(&format!("CREATE TABLE col ({COLUMNS}) USING columnar WITH (order_by = 'project, ts')"))
            .unwrap();
        // Four segments, one per month.
        for month in 1..=4 {
            conn.execute(
                "INSERT INTO col (ts, project, name, user_id, duration, props)
                 SELECT make_timestamp(2024, $1, 1, 0, 0, 0) + g * interval '1 second', g % 5, 'e' || (g % 9), g, g % 1000,
                        jsonb_build_object('k', g % 4)
                 FROM generate_series(1, 20000) g",
                &[&month],
            )
            .unwrap();
            conn.batch_execute("VACUUM col").unwrap();
        }
    }
    let total = store.segment_bytes();
    store.reset();

    // A fresh database handle has an empty cache.
    let db = open();
    let conn = db.connect();
    let n: i64 =
        conn.query_one("SELECT count(*) FROM col WHERE ts >= '2024-03-01' AND ts < '2024-03-02'", &[]).unwrap().get(0);
    assert_eq!(n, 20000);
    let (gets, ranges, bytes, keys) = store.reset();
    assert_eq!(gets, 0, "segments are never fetched whole");
    assert_eq!(keys, 1, "the manifest rules out three of four segments before any read");
    assert!(ranges > 0 && bytes * 4 < total, "{bytes} of {total} bytes read");

    // One narrow column of all segments: much less than everything.
    let sum: i64 = conn.query_one("SELECT sum(duration) FROM col", &[]).unwrap().get(0);
    assert!(sum > 0);
    let (_, _, bytes, keys) = store.reset();
    assert_eq!(keys, 4);
    assert!(bytes * 3 < total, "a one-column aggregate read {bytes} of {total} bytes");
    // Cached now.
    conn.query_one("SELECT sum(duration) FROM col", &[]).unwrap();
    assert_eq!(store.reset().1, 0, "a repeated query reads from the cache");

    // Concurrent cold readers fetch every range once.
    drop(conn);
    drop(db);
    let db = open();
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let conn = db.connect();
            thread::spawn(move || {
                conn.query_one("SELECT sum(duration) FROM col WHERE project = 2", &[]).unwrap().get::<i64>(0)
            })
        })
        .collect();
    let sums: Vec<i64> = readers.into_iter().map(|r| r.join().unwrap()).collect();
    assert!(sums.windows(2).all(|w| w[0] == w[1]));
    let (_, concurrent, _, _) = store.reset();
    drop(db);
    let db = open();
    db.connect().query_one("SELECT sum(duration) FROM col WHERE project = 2", &[]).unwrap();
    let (_, single, _, _) = store.reset();
    assert!(concurrent <= single, "four concurrent cold readers made {concurrent} requests, one reader {single}");
    let (hits, misses) = db.columnar_cache_stats();
    assert!(misses > 0 && hits + misses > 0);
}

#[test]
fn background_compaction_runs_alongside_inserts_deletes_and_queries() {
    let config = ColumnarConfig {
        compact_rows: 700,
        compact_blocks: 6,
        compact_age: std::time::Duration::from_millis(20),
        ..ColumnarConfig::default()
    };
    let db = Database::open_in_memory_with(config).unwrap();
    db.connect()
        .batch_execute(
            "CREATE TABLE e (batch int NOT NULL, n int NOT NULL, p jsonb) USING columnar WITH (order_by = 'batch')",
        )
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    // Every batch inserts 100 rows whose `n` sum to 4950, atomically, by
    // SQL or by the append API.
    let writers: Vec<_> = (0..3)
        .map(|w| {
            let conn = db.connect();
            thread::spawn(move || {
                for b in 0..60 {
                    let batch = w * 1000 + b;
                    if b % 2 == 0 {
                        conn.execute(
                            "INSERT INTO e SELECT $1, g, jsonb_build_object('w', $1 % 7) FROM generate_series(0, 99) g",
                            &[&batch],
                        )
                        .unwrap();
                    } else {
                        let rows: Vec<Vec<Value>> = (0..100)
                            .map(|g| {
                                vec![
                                    Value::Int4(batch),
                                    Value::Int4(g),
                                    Value::Text(format!("{{\"w\": {}}}", batch % 7)),
                                ]
                            })
                            .collect();
                        assert_eq!(conn.append("e", &rows).unwrap(), 100);
                    }
                }
            })
        })
        .collect();
    // Deletes remove whole batches.
    let deleter = {
        let conn = db.connect();
        thread::spawn(move || {
            for b in (0..60).step_by(9) {
                // Wait until the batch is there, then delete it.
                while conn.execute("DELETE FROM e WHERE batch = $1", &[&b]).unwrap() == 0 {
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        })
    };
    let reader = {
        let (conn, stop) = (db.connect(), stop.clone());
        thread::spawn(move || {
            let mut checks = 0;
            while !stop.load(Ordering::Relaxed) {
                // Both the column engine and the row path see whole batches
                // only, each row exactly once.
                let fast = conn.query_one("SELECT count(*), sum(n), count(DISTINCT batch) FROM e", &[]).unwrap();
                let (count, sum, batches): (i64, Option<i64>, i64) = (fast.get(0), fast.get(1), fast.get(2));
                assert_eq!(count, batches * 100, "a row was lost or duplicated");
                assert_eq!(sum.unwrap_or(0), batches * 4950);
                let slow = conn.query_one("SELECT count(*), sum(n + 0) FROM e WHERE p ->> 'w' <> 'x'", &[]).unwrap();
                assert_eq!(slow.get::<i64>(0) % 100, 0);
                checks += 1;
            }
            checks
        })
    };
    writers.into_iter().for_each(|w| w.join().unwrap());
    deleter.join().unwrap();
    assert!(db.wait_for_compaction(std::time::Duration::from_secs(60)));
    stop.store(true, Ordering::Relaxed);
    assert!(reader.join().unwrap() > 0);
    let conn = db.connect();
    let deleted: i64 = conn.query_one("SELECT count(*) FROM generate_series(0, 59, 9) g", &[]).unwrap().get(0);
    let row = conn.query_one("SELECT count(*), sum(n), count(DISTINCT batch) FROM e", &[]).unwrap();
    let batches = 180 - deleted;
    assert_eq!((row.get::<i64>(0), row.get::<i64>(1), row.get::<i64>(2)), (batches * 100, batches * 4950, batches));
}

#[test]
fn closing_the_database_during_compaction_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let config = || Config {
        columnar: ColumnarConfig { compact_rows: 1000, compact_blocks: 2, ..ColumnarConfig::default() },
        ..Config::default()
    };
    let mut total = 0i64;
    for round in 0..4i64 {
        let db = Database::open(dir.path(), config()).unwrap();
        let conn = db.connect();
        if round == 0 {
            conn.batch_execute("CREATE TABLE e (n bigint) USING columnar").unwrap();
        }
        for b in 0..20i64 {
            let rows: Vec<Vec<Value>> = (0..500).map(|i| vec![Value::Int8(round * 100_000 + b * 1000 + i)]).collect();
            conn.append("e", &rows).unwrap();
            total += 500;
        }
        // Dropped while compactions are due or running; reopening right
        // away must work and show every row once.
    }
    let db = Database::open(dir.path(), config()).unwrap();
    let conn = db.connect();
    let row = conn.query_one("SELECT count(*), count(DISTINCT n) FROM e", &[]).unwrap();
    assert_eq!((row.get::<i64>(0), row.get::<i64>(1)), (total, total));
    assert!(db.wait_for_compaction(std::time::Duration::from_secs(60)));
    assert_eq!(conn.query_one("SELECT count(*) FROM e", &[]).unwrap().get::<i64>(0), total);
    assert!(!segment_objects(dir.path()).is_empty(), "the delta was compacted into segments");
}

#[test]
fn append_inserts_like_insert() {
    let db = Database::open_in_memory_with(manual()).unwrap();
    let conn = db.connect();
    conn.batch_execute(
        "CREATE TABLE r (ts timestamptz NOT NULL, n int CHECK (n >= 0), s varchar(4), p jsonb, d numeric(6,2));
         CREATE TABLE c (ts timestamptz NOT NULL, n int CHECK (n >= 0), s varchar(4), p jsonb, d numeric(6,2)) USING columnar;
         CREATE SCHEMA other; CREATE TABLE other.c (x int) USING columnar;",
    )
    .unwrap();
    let rows = vec![
        vec![
            Value::Text("2024-01-01 10:00:00+00".into()),
            Value::Int8(5),
            Value::Text("abc".into()),
            Value::Text("{\"a\": 1}".into()),
            Value::Text("1.255".into()),
        ],
        vec![Value::TimestampTz(0), Value::Null, Value::Null, Value::Null, Value::Int4(3)],
    ];
    for t in ["r", "c"] {
        assert_eq!(conn.append(t, &rows).unwrap(), 2);
    }
    assert_eq!(rows_of(&conn, "SELECT * FROM c ORDER BY ts"), rows_of(&conn, "SELECT * FROM r ORDER BY ts"));
    for t in ["r", "c"] {
        let err = |values: Vec<Value>| conn.append(t, &[values]).unwrap_err().code().to_string();
        assert_eq!(
            err(vec![Value::Null, Value::Int4(1), Value::Null, Value::Null, Value::Null]),
            sqlstate::NOT_NULL_VIOLATION
        );
        assert_eq!(
            err(vec![Value::TimestampTz(0), Value::Int4(-1), Value::Null, Value::Null, Value::Null]),
            sqlstate::CHECK_VIOLATION
        );
        assert_eq!(
            err(vec![Value::TimestampTz(0), Value::Int4(1), Value::text("toolong"), Value::Null, Value::Null]),
            "22001"
        );
        assert_eq!(err(vec![Value::TimestampTz(0)]), "08P01");
        assert_eq!(err(vec![Value::TimestampTz(0), Value::text("x"), Value::Null, Value::Null, Value::Null]), "22P02");
    }
    assert_eq!(conn.append("nosuch", &[]).unwrap_err().code(), sqlstate::UNDEFINED_TABLE);
    conn.append("other.c", &[vec![Value::Int4(1)]]).unwrap();
    // Inside a transaction it is part of it.
    conn.batch_execute("BEGIN").unwrap();
    conn.append("c", &rows).unwrap();
    conn.batch_execute("ROLLBACK").unwrap();
    assert_eq!(conn.query_one("SELECT count(*) FROM c", &[]).unwrap().get::<i64>(0), 2);
    assert_eq!(conn.query_one("SELECT count(*) FROM other.c", &[]).unwrap().get::<i64>(0), 1);
}

fn rows_of(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    rows(conn, sql)
}

#[test]
fn vacuum_removes_orphaned_segment_objects_after_the_grace_period() {
    let dir = tempfile::tempdir().unwrap();
    let open = |grace: u64| {
        let columnar = ColumnarConfig { orphan_grace: std::time::Duration::from_secs(grace), ..manual() };
        Database::open(dir.path(), Config { columnar, ..Config::default() }).unwrap()
    };
    let db = open(3600);
    let conn = db.connect();
    conn.batch_execute("CREATE TABLE e (n int) USING columnar; CREATE TABLE gone (n int) USING columnar").unwrap();
    conn.execute("INSERT INTO e SELECT g FROM generate_series(1, 1000) g", &[]).unwrap();
    conn.batch_execute("VACUUM e").unwrap();
    let live = segment_objects(dir.path());
    assert_eq!(live.len(), 1);
    // A bulk insert that rolls back deletes the segment it wrote.
    conn.batch_execute("BEGIN").unwrap();
    conn.execute("INSERT INTO e SELECT g FROM generate_series(1, 60000) g", &[]).unwrap();
    assert_eq!(segment_objects(dir.path()).len(), 2);
    conn.batch_execute("ROLLBACK").unwrap();
    assert_eq!(segment_objects(dir.path()), live);
    // Leftovers of crashes: an id of an older version (no time), a recent
    // one, and one of a table that no longer exists.
    let e_dir = std::path::Path::new(&live[0]).parent().unwrap().to_path_buf();
    let old = e_dir.join("0123456789abcdef0123456789abcdef.seg");
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    let recent = e_dir.join(format!("{millis:x}-00000000000000ff.seg"));
    std::fs::write(&old, b"x").unwrap();
    std::fs::write(&recent, b"x").unwrap();
    conn.execute("INSERT INTO gone SELECT g FROM generate_series(1, 10) g", &[]).unwrap();
    conn.batch_execute("VACUUM gone").unwrap();
    let with_gone = segment_objects(dir.path());
    let gone_obj = with_gone.iter().find(|p| !p.starts_with(e_dir.to_str().unwrap())).unwrap().clone();
    // Simulate a crash between DROP TABLE and the deletion of its objects.
    let saved = std::fs::read(&gone_obj).unwrap();
    conn.batch_execute("DROP TABLE gone").unwrap();
    std::fs::create_dir_all(std::path::Path::new(&gone_obj).parent().unwrap()).unwrap();
    std::fs::write(&gone_obj, &saved).unwrap();

    conn.batch_execute("VACUUM").unwrap();
    let left = segment_objects(dir.path());
    assert!(left.contains(&live[0]), "a referenced segment is kept");
    assert!(left.contains(&recent.to_string_lossy().into_owned()), "a recent object is kept during the grace period");
    assert!(!left.contains(&old.to_string_lossy().into_owned()), "an old orphan is removed");
    assert!(left.contains(&gone_obj), "a recent object of a dropped table is kept during the grace period");
    drop(conn);
    drop(db);
    let db = open(0);
    let conn = db.connect();
    conn.batch_execute("VACUUM e").unwrap();
    assert!(segment_objects(dir.path()).contains(&gone_obj), "VACUUM of one table looks at that table only");
    conn.batch_execute("VACUUM").unwrap();
    assert_eq!(segment_objects(dir.path()), live);
    assert_eq!(conn.query_one("SELECT count(*), sum(n) FROM e", &[]).unwrap().get::<i64>(1), 500500);
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(e.file_name());
        if e.path().is_dir() {
            copy_dir(&e.path(), &target);
        } else {
            std::fs::copy(e.path(), target).unwrap();
        }
    }
}

/// A database written by squad 0.1.0 before delta blocks, column ids and
/// JSON columns (`tests/data/columnar-v0.1`, made by inserting rows 1–370
/// with two compactions in between: two segments with a `SQMANI01`
/// manifest and 20 delta rows stored one per key) is read, changed and
/// compacted by this version.
#[test]
fn databases_of_the_previous_format_are_read_and_upgraded() {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/columnar-v0.1"), dir.path());
    let db = Database::open(dir.path(), Config { columnar: manual(), ..Config::default() }).unwrap();
    let conn = db.connect();
    fn expect(conn: &Connection, stage: &str, n: i64) {
        let fast = conn
            .query_one(
                "SELECT count(*), sum(n)::int8, count(DISTINCT props ->> 's') FROM ev WHERE props ->> 'k' IS NOT NULL",
                &[],
            )
            .unwrap();
        let slow =
            conn.query_one("SELECT count(*), sum(n + 0)::int8 FROM ev WHERE props::text LIKE '%k%'", &[]).unwrap();
        let by_field: i64 = conn.query_one("SELECT count(*) FROM ev WHERE props ->> 's' = 'v1'", &[]).unwrap().get(0);
        assert_eq!(fast.get::<i64>(0), n, "{stage}");
        assert_eq!(fast.get::<i64>(1), n * (n + 1) / 2, "{stage}");
        assert_eq!(fast.get::<i64>(2), 2, "{stage}");
        assert_eq!((slow.get::<i64>(0), slow.get::<i64>(1)), (n, n * (n + 1) / 2), "{stage}");
        assert_eq!(by_field, n / 2, "{stage}");
    }
    expect(&conn, "as written by 0.1.0", 370);
    // New rows go to blocks next to the old delta rows.
    conn.execute(
        "INSERT INTO ev SELECT timestamp '2024-01-01' + g * interval '1 minute', g % 3, 'e' || (g % 4), g,
                jsonb_build_object('k', g % 5, 's', 'v' || (g % 2)) FROM generate_series(371, 400) g",
        &[],
    )
    .unwrap();
    expect(&conn, "with new rows", 400);
    // Deletes reach old delta rows, segments and blocks.
    assert_eq!(conn.execute("DELETE FROM ev WHERE n > 395 OR n = 360 OR n = 10", &[]).unwrap(), 7);
    conn.execute(
        "INSERT INTO ev SELECT timestamp '2024-01-01' + g * interval '1 minute', g % 3, 'e' || (g % 4), g,
                jsonb_build_object('k', g % 5, 's', 'v' || (g % 2)) FROM unnest(ARRAY[10, 360]) g",
        &[],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO ev SELECT timestamp '2024-01-01' + g * interval '1 minute', g % 3, 'e' || (g % 4), g,
                jsonb_build_object('k', g % 5, 's', 'v' || (g % 2)) FROM generate_series(396, 400) g",
        &[],
    )
    .unwrap();
    // A schema change converts the old delta rows first.
    conn.batch_execute("ALTER TABLE ev ADD COLUMN extra int DEFAULT 1").unwrap();
    expect(&conn, "after ADD COLUMN", 400);
    assert_eq!(conn.query_one("SELECT sum(extra)::int8 FROM ev", &[]).unwrap().get::<i64>(0), 400);
    conn.batch_execute("VACUUM ev").unwrap();
    expect(&conn, "compacted", 400);
    drop(conn);
    drop(db);
    let db = Database::open(dir.path(), Config { columnar: manual(), ..Config::default() }).unwrap();
    let conn = db.connect();
    expect(&conn, "reopened", 400);
}
