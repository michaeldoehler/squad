//! Read replicas: a second database that follows a writer through the
//! object store, serves read-only queries, and can be promoted.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use squad::object::{LocalStore, MemoryStore, ObjectStore};
use squad::{Config, Connection, Database, RemoteConfig, RemoteSync, ReplicaConfig, ReplicationRole};

const READ_ONLY: &str = "25006";

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(MemoryStore::new())
}

fn writer(dir: &tempfile::TempDir, store: &Arc<dyn ObjectStore>) -> Database {
    writer_with(dir, store, RemoteSync::Sync)
}

fn writer_with(dir: &tempfile::TempDir, store: &Arc<dyn ObjectStore>, mode: RemoteSync) -> Database {
    Database::open(dir.path(), Config { remote: Some(RemoteConfig::new(store.clone(), mode)), ..Config::default() })
        .unwrap()
}

/// A replica that only moves when the test tells it to.
fn manual(dir: &tempfile::TempDir, store: &Arc<dyn ObjectStore>) -> Database {
    let config = ReplicaConfig { poll_interval: Duration::from_secs(3600), ..ReplicaConfig::new(store.clone()) };
    Database::open_replica(dir.path(), config).unwrap()
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_one(sql, &[]).unwrap().get(0)
}

fn code(conn: &Connection, sql: &str) -> String {
    match conn.batch_execute(sql) {
        Ok(()) => "ok".to_string(),
        Err(e) => e.code().to_string(),
    }
}

/// Every row of every user table, as text.
fn dump(conn: &Connection) -> Vec<String> {
    let tables = conn
        .query(
            "SELECT table_name::text FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY 1",
            &[],
        )
        .unwrap();
    let mut out = Vec::new();
    for t in tables {
        let name: String = t.get(0);
        out.push(format!("== {name}"));
        let rows = conn.query(&format!("SELECT t::text FROM \"{name}\" t ORDER BY 1"), &[]).unwrap();
        out.extend(rows.iter().map(|r| r.get::<String>(0)));
    }
    out
}

#[test]
fn a_replica_follows_data_and_schema() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute(
        "CREATE TABLE users (id serial PRIMARY KEY, email text UNIQUE NOT NULL, meta jsonb);
         INSERT INTO users (email, meta) SELECT 'u' || g || '@example.org', jsonb_build_object('n', g)
         FROM generate_series(1, 500) g;",
    )
    .unwrap();
    db.checkpoint().unwrap();
    w.execute("DELETE FROM users WHERE id % 5 = 0", &[]).unwrap();

    let replica = manual(&rdir, &store);
    assert!(replica.is_replica());
    let r = replica.connect();
    assert_eq!(count(&r, "SELECT count(*) FROM users"), 400);
    assert!(r.query_one("SELECT pg_is_in_recovery()", &[]).unwrap().get::<bool>(0));
    assert!(!w.query_one("SELECT pg_is_in_recovery()", &[]).unwrap().get::<bool>(0));
    let status = replica.replication_status().unwrap();
    assert_eq!(status.role, ReplicationRole::Replica);
    assert_eq!(status.applied_lsn, status.remote_lsn);
    assert!(db.replication_status().is_none());

    // New commits appear after the next poll, not before.
    w.execute("UPDATE users SET meta = meta || '{\"seen\": true}' WHERE id <= 10", &[]).unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM users WHERE meta ? 'seen'"), 0);
    replica.replication_catch_up().unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM users WHERE meta ? 'seen'"), 8);
    assert_eq!(count(&r, "SELECT count(*) FROM users WHERE email = 'u7@example.org'"), 1);

    // Schema changes: new tables, columns, indexes, types, dropped tables.
    w.batch_execute(
        "CREATE TYPE plan AS ENUM ('free', 'pro');
         ALTER TABLE users ADD COLUMN plan plan NOT NULL DEFAULT 'free';
         CREATE INDEX users_plan ON users (plan);
         UPDATE users SET plan = 'pro' WHERE id < 4;
         CREATE TABLE notes (id int PRIMARY KEY, user_id int REFERENCES users (id), body text);
         INSERT INTO notes VALUES (1, 1, 'hello'), (2, 2, 'world');
         CREATE VIEW pro_users AS SELECT id, email FROM users WHERE plan = 'pro';",
    )
    .unwrap();
    // A statement prepared before the schema change keeps working.
    let prepared = r.prepare("SELECT count(*) FROM users WHERE id < $1").unwrap();
    assert_eq!(prepared.query_one(&[&100i32]).unwrap().get::<i64>(0), 80);
    replica.replication_catch_up().unwrap();
    assert_eq!(prepared.query_one(&[&100i32]).unwrap().get::<i64>(0), 80);
    assert_eq!(count(&r, "SELECT count(*) FROM pro_users"), 3);
    assert_eq!(count(&r, "SELECT count(*) FROM notes n JOIN users u ON u.id = n.user_id WHERE u.plan = 'pro'"), 2);
    assert_eq!(dump(&r), dump(&w));

    w.batch_execute("DROP VIEW pro_users; DROP TABLE notes; ALTER TABLE users DROP COLUMN meta").unwrap();
    replica.replication_catch_up().unwrap();
    assert_eq!(code(&r, "SELECT * FROM notes"), "42P01");
    assert_eq!(code(&r, "SELECT meta FROM users"), "42703");
    assert_eq!(dump(&r), dump(&w));
    assert!(replica.verify().unwrap().is_ok());
}

#[test]
fn a_replica_is_read_only() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute(
        "CREATE TABLE t (id int PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a'), (2, 'b');
         CREATE SEQUENCE s;
         SELECT nextval('s');
         CREATE TABLE ev (ts timestamp NOT NULL, n int) USING columnar;
         INSERT INTO ev VALUES ('2026-01-01', 1);",
    )
    .unwrap();
    let replica = manual(&rdir, &store);
    let r = replica.connect();
    let before = dump(&r);
    for sql in [
        "INSERT INTO t VALUES (3, 'c')",
        "UPDATE t SET v = 'x'",
        "DELETE FROM t",
        "INSERT INTO ev VALUES ('2026-01-02', 2)",
        "DELETE FROM ev",
        "CREATE TABLE u (id int)",
        "ALTER TABLE t ADD COLUMN w int",
        "DROP TABLE t",
        "CREATE INDEX t_v ON t (v)",
        "TRUNCATE t",
        "VACUUM",
        "VACUUM ev",
        "SELECT nextval('s')",
        "SELECT setval('s', 100)",
        "SELECT * FROM t FOR UPDATE",
        "BEGIN READ WRITE",
        "WITH d AS (DELETE FROM t RETURNING id) SELECT * FROM d",
    ] {
        assert_eq!(code(&r, sql), READ_ONLY, "{sql}");
        // The failed statement leaves the session usable.
        assert_eq!(count(&r, "SELECT count(*) FROM t"), 2, "after {sql}");
    }

    // Reading in every form works.
    r.batch_execute("BEGIN; SELECT * FROM t; COMMIT").unwrap();
    r.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; SELECT count(*) FROM ev; ROLLBACK").unwrap();
    r.batch_execute("SET search_path = public; SHOW transaction_read_only").unwrap();
    assert_eq!(r.query_one("SHOW transaction_read_only", &[]).unwrap().get::<String>(0), "on");
    let tx = r.transaction().unwrap();
    assert_eq!(tx.query_one("SELECT v FROM t WHERE id = $1", &[&2i32]).unwrap().get::<String>(0), "b");
    assert_eq!(tx.execute("INSERT INTO t VALUES (9, 'z')", &[]).unwrap_err().code(), READ_ONLY);
    tx.rollback().unwrap();
    assert_eq!(dump(&r), before);
    assert_eq!(dump(&r), dump(&w));
    let status = r.query_one("SELECT squad_replication_status()", &[]).unwrap().get::<serde_json::Value>(0);
    assert_eq!(status["role"], "replica");
    assert_eq!(w.query_one("SELECT squad_replication_status() IS NULL", &[]).unwrap().get::<bool>(0), true);
    // A writer cannot be "promoted".
    assert!(db.promote().is_err());
}

#[test]
fn snapshots_on_a_replica_are_stable_while_it_applies() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute("CREATE TABLE acct (id int PRIMARY KEY, balance int NOT NULL)").unwrap();
    w.execute("INSERT INTO acct SELECT g, 100 FROM generate_series(1, 50) g", &[]).unwrap();
    let replica = manual(&rdir, &store);
    let r = replica.connect();
    let other = replica.connect();

    r.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
    assert_eq!(count(&r, "SELECT sum(balance) FROM acct"), 5000);
    let first = dump(&r);
    // The writer changes, deletes and vacuums what the snapshot reads,
    // and changes the schema.
    w.batch_execute(
        "UPDATE acct SET balance = balance + 1;
         DELETE FROM acct WHERE id > 40;
         ALTER TABLE acct ADD COLUMN note text DEFAULT 'n';",
    )
    .unwrap();
    w.batch_execute("VACUUM").unwrap();
    replica.replication_catch_up().unwrap();
    // A new statement outside sees the new state...
    assert_eq!(count(&other, "SELECT sum(balance) FROM acct"), 40 * 101);
    assert_eq!(count(&other, "SELECT count(note) FROM acct"), 40);
    // ...the open transaction still its own.
    assert_eq!(count(&r, "SELECT sum(balance) FROM acct"), 5000);
    assert_eq!(count(&r, "SELECT count(*) FROM acct WHERE id > 40"), 10);
    assert_eq!(dump(&r), first);
    r.batch_execute("COMMIT").unwrap();
    assert_eq!(count(&r, "SELECT sum(balance) FROM acct"), 40 * 101);

    // Read committed moves with each statement.
    r.batch_execute("BEGIN").unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM acct"), 40);
    w.execute("DELETE FROM acct WHERE id > 30", &[]).unwrap();
    replica.replication_catch_up().unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM acct"), 30);
    r.batch_execute("COMMIT").unwrap();
}

/// Money moves between accounts on the writer while replicas read: every
/// snapshot a replica hands out must show a state in which the total is
/// unchanged, i.e. transactions appear as a whole or not at all.
#[test]
fn replicas_only_see_whole_transactions() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute("CREATE TABLE acct (id int PRIMARY KEY, balance int NOT NULL)").unwrap();
    w.execute("INSERT INTO acct SELECT g, 1000 FROM generate_series(1, 20) g", &[]).unwrap();
    let config = ReplicaConfig { poll_interval: Duration::from_millis(2), ..ReplicaConfig::new(store.clone()) };
    let replica = Database::open_replica(rdir.path(), config).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|i| {
            let (replica, stop) = (replica.clone(), stop.clone());
            std::thread::spawn(move || {
                let conn = replica.connect();
                let mut checks = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if i == 0 {
                        // Two statements in one snapshot.
                        conn.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
                        let a = count(&conn, "SELECT sum(balance) FROM acct WHERE id <= 10");
                        let b = count(&conn, "SELECT sum(balance) FROM acct WHERE id > 10");
                        conn.batch_execute("COMMIT").unwrap();
                        assert_eq!(a + b, 20_000);
                    } else {
                        assert_eq!(count(&conn, "SELECT sum(balance) FROM acct"), 20_000);
                    }
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    let mut x = 12345u64;
    for i in 0..400 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let (from, to, amount) = ((x >> 33) % 20 + 1, (x >> 40) % 20 + 1, ((x >> 50) % 50) as i32);
        let tx = w.transaction().unwrap();
        tx.execute("UPDATE acct SET balance = balance - $2 WHERE id = $1", &[&(from as i32), &amount]).unwrap();
        tx.execute("UPDATE acct SET balance = balance + $2 WHERE id = $1", &[&(to as i32), &amount]).unwrap();
        tx.commit().unwrap();
        if i % 100 == 99 {
            w.batch_execute("VACUUM").unwrap();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let checks: u64 = readers.into_iter().map(|t| t.join().unwrap()).sum();
    assert!(checks > 0);
    replica.replication_catch_up().unwrap();
    assert_eq!(dump(&replica.connect()), dump(&w));
}

#[test]
fn the_background_thread_follows_without_being_asked() {
    let remote = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(remote.path()).unwrap());
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    let config = ReplicaConfig { poll_interval: Duration::from_millis(5), ..ReplicaConfig::new(store.clone()) };
    let replica = Database::open_replica(rdir.path(), config).unwrap();
    let r = replica.connect();
    for i in 0..20 {
        w.execute("INSERT INTO t VALUES ($1)", &[&i]).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while count(&r, "SELECT count(*) FROM t") < 20 {
        assert!(Instant::now() < deadline, "the replica did not catch up: {:?}", replica.replication_status());
        std::thread::sleep(Duration::from_millis(5));
    }
    let status = replica.replication_status().unwrap();
    assert!(status.since_last_apply.is_some() && status.since_last_poll.is_some());
    assert!(status.last_error.is_none(), "{status:?}");
    // Dropping the database stops the thread; the directory can be
    // opened again right away.
    drop(r);
    drop(replica);
    let config = ReplicaConfig { poll_interval: Duration::from_millis(5), ..ReplicaConfig::new(store.clone()) };
    let replica = Database::open_replica(rdir.path(), config).unwrap();
    assert_eq!(count(&replica.connect(), "SELECT count(*) FROM t"), 20);
}

#[test]
fn columnar_tables_on_a_replica() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute(
        "CREATE TABLE events (ts timestamp NOT NULL, project int NOT NULL, name text, n int)
         USING columnar WITH (order_by = 'project, ts');
         INSERT INTO events
         SELECT timestamp '2026-01-01' + g * interval '1 minute', g % 7, 'e' || g % 13, g
         FROM generate_series(1, 60000) g;",
    )
    .unwrap();
    // Rows still in the delta as well as rows in segments.
    w.execute("INSERT INTO events VALUES ('2026-06-01', 3, 'late', -1)", &[]).unwrap();
    let replica = manual(&rdir, &store);
    let r = replica.connect();
    let queries = [
        "SELECT count(*), sum(n), min(ts)::text, max(ts)::text FROM events",
        "SELECT project, count(*), sum(n) FROM events GROUP BY project ORDER BY project",
        "SELECT name, count(*) FROM events WHERE project = 3 AND ts >= '2026-01-10' GROUP BY name ORDER BY 2 DESC, 1 LIMIT 5",
        "SELECT * FROM events WHERE n IN (-1, 17, 59999) ORDER BY n",
        "SELECT date_trunc('day', ts) AS d, count(*) FROM events GROUP BY 1 ORDER BY 1 LIMIT 10",
    ];
    let same = |r: &Connection, w: &Connection| {
        for q in queries {
            let text = |c: &Connection| {
                c.query(&format!("SELECT x::text FROM ({q}) x"), &[]).unwrap().iter().map(|r| r.get::<String>(0)).collect::<Vec<_>>()
            };
            assert_eq!(text(r), text(w), "{q}");
        }
    };
    same(&r, &w);

    // Compaction and deletes on the writer rewrite segments.
    w.batch_execute("VACUUM events").unwrap();
    w.execute("DELETE FROM events WHERE project = 5", &[]).unwrap();
    w.execute("INSERT INTO events SELECT '2026-07-01', 1, 'x', g FROM generate_series(1, 300) g", &[]).unwrap();
    replica.replication_catch_up().unwrap();
    same(&r, &w);
    assert_eq!(count(&r, "SELECT count(*) FROM events WHERE project = 5"), 0);

    // Promote: the new writer owns the segments from here.
    replica.promote().unwrap();
    r.execute("INSERT INTO events SELECT '2026-08-01', 2, 'p', g FROM generate_series(1, 70000) g", &[]).unwrap();
    r.batch_execute("VACUUM events").unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM events WHERE name = 'p'"), 70000);
}

#[test]
fn a_replica_far_behind_switches_to_a_newer_snapshot() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute("CREATE TABLE t (id int PRIMARY KEY, v text)").unwrap();
    w.execute("INSERT INTO t SELECT g, 'first' FROM generate_series(1, 100) g", &[]).unwrap();
    let replica = manual(&rdir, &store);
    let r = replica.connect();
    r.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM t"), 100);

    // Three snapshots later the log the replica needs is gone.
    for round in 0..3 {
        w.execute("UPDATE t SET v = $1", &[&format!("round {round}")]).unwrap();
        w.execute("INSERT INTO t SELECT g + $1, 'more' FROM generate_series(1, 100) g", &[&(1000 * (round + 1))])
            .unwrap();
        db.checkpoint().unwrap();
    }
    w.batch_execute("CREATE TABLE later (id int); INSERT INTO later VALUES (1)").unwrap();
    let status = replica.replication_catch_up().unwrap();
    assert_eq!(status.rebases, 1, "{status:?}");
    // The transaction that was reading the replaced copy is cancelled
    // the way PostgreSQL cancels queries that conflict with recovery...
    let err = r.query("SELECT count(*) FROM t", &[]).unwrap_err();
    assert_eq!(err.code(), "40001", "{err}");
    r.batch_execute("ROLLBACK").unwrap();
    // ...and a new one reads the new copy, schema included.
    assert_eq!(count(&r, "SELECT count(*) FROM t"), 400);
    assert_eq!(count(&r, "SELECT count(*) FROM later"), 1);
    assert_eq!(dump(&r), dump(&w));
    assert!(replica.verify().unwrap().is_ok());
}

#[test]
fn a_restarted_replica_continues() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer(&wdir, &store);
    let w = db.connect();
    w.batch_execute("CREATE TABLE t (id serial PRIMARY KEY, v text)").unwrap();
    w.execute("INSERT INTO t (v) SELECT 'a' FROM generate_series(1, 200)", &[]).unwrap();
    let replica = manual(&rdir, &store);
    let at = replica.replication_status().unwrap().applied_lsn;
    drop(replica);
    w.batch_execute("INSERT INTO t (v) VALUES ('b'); ALTER TABLE t ADD COLUMN extra int").unwrap();
    let replica = manual(&rdir, &store);
    assert!(replica.replication_status().unwrap().applied_lsn > at);
    assert_eq!(dump(&replica.connect()), dump(&w));
    // A replica directory is not opened as an ordinary database by
    // accident... it can be, and is then a plain local copy.
    drop(replica);
    // A directory that holds some other database is refused.
    let other = tempfile::tempdir().unwrap();
    Database::open(other.path(), Config::default()).unwrap().connect().batch_execute("CREATE TABLE x (i int)").unwrap();
    let config = ReplicaConfig::new(store.clone());
    assert!(Database::open_replica(other.path(), config).is_err());
    // And a store without a database cannot be followed.
    let empty = tempfile::tempdir().unwrap();
    assert!(Database::open_replica(empty.path(), ReplicaConfig::new(self::store())).is_err());
}

#[test]
fn promotion_makes_the_replica_the_writer() {
    let store = store();
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let db = writer(&dirs[0], &store);
    let w = db.connect();
    w.batch_execute(
        "CREATE TABLE orders (id serial PRIMARY KEY, item text NOT NULL, qty int NOT NULL CHECK (qty > 0));
         INSERT INTO orders (item, qty) SELECT 'item' || g, g FROM generate_series(1, 100) g;",
    )
    .unwrap();
    let replica = manual(&dirs[1], &store);
    let second = manual(&dirs[2], &store);
    // Acknowledged by the writer, not yet polled by anyone.
    for i in 0..25 {
        w.execute("INSERT INTO orders (item, qty) VALUES ($1, 1)", &[&format!("late{i}")]).unwrap();
    }
    let r = replica.connect();
    // A session that was open before the promotion.
    let old_session = replica.connect();
    assert_eq!(code(&old_session, "INSERT INTO orders (item, qty) VALUES ('x', 1)"), READ_ONLY);

    assert!(r.query_one("SELECT squad_promote()", &[]).unwrap().get::<bool>(0));
    assert!(!replica.is_replica());
    assert_eq!(replica.replication_status().unwrap().role, ReplicationRole::Writer);
    assert!(!r.query_one("SELECT pg_is_in_recovery()", &[]).unwrap().get::<bool>(0));
    // Nothing acknowledged was lost.
    assert_eq!(count(&r, "SELECT count(*) FROM orders"), 125);
    replica.promote().unwrap();

    // The old writer is fenced: its commits fail from now on.
    let err = w.execute("INSERT INTO orders (item, qty) VALUES ('ghost', 1)", &[]).unwrap_err();
    assert!(!err.code().is_empty());
    assert!(w.execute("INSERT INTO orders (item, qty) VALUES ('ghost', 1)", &[]).is_err());

    // The promoted node takes writes on old and new sessions; sequences
    // continue after the values the old writer handed out, constraints hold.
    let id: i32 =
        old_session.query_one("INSERT INTO orders (item, qty) VALUES ('new', 2) RETURNING id", &[]).unwrap().get(0);
    assert!(id > 125, "sequence restarted at {id}");
    assert_eq!(code(&r, "INSERT INTO orders (item, qty) VALUES ('bad', 0)"), "23514");
    r.batch_execute("CREATE TABLE audit (at timestamptz DEFAULT now(), what text); INSERT INTO audit (what) VALUES ('promoted')")
        .unwrap();
    r.batch_execute("VACUUM").unwrap();
    assert_eq!(count(&r, "SELECT count(*) FROM orders WHERE item = 'ghost'"), 0);

    // The other replica follows the new writer.
    second.replication_catch_up().unwrap();
    let s = second.connect();
    assert_eq!(dump(&s), dump(&r));
    assert!(second.is_replica());
    assert_eq!(second.replication_status().unwrap().epoch, replica.replication_status().unwrap().epoch);

    // The promoted node restarts as an ordinary writer on its directory,
    // and a machine without any local state restores the same data.
    let expected = dump(&r);
    drop((r, old_session, replica));
    let again = writer(&dirs[1], &store);
    assert_eq!(dump(&again.connect()), expected);
    again.connect().execute("INSERT INTO audit (what) VALUES ('restarted')", &[]).unwrap();
    second.replication_catch_up().unwrap();
    assert_eq!(count(&s, "SELECT count(*) FROM audit"), 2);
    drop(again);
    Database::restore_from(&*store, dirs[3].path().join("copy")).unwrap();
    let copy = Database::open(dirs[3].path().join("copy"), Config::default()).unwrap();
    assert_eq!(count(&copy.connect(), "SELECT count(*) FROM audit"), 2);
}

/// Several writer threads run random transactions while a replica
/// follows with a fast poll and readers query it. At the end both hold
/// the same rows; then the replica is promoted and the same is done
/// again with the roles moved on.
#[test]
fn random_workload_with_concurrent_replica() {
    let seeds: u64 = std::env::var("SQUAD_REPLICA_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
    for seed in 0..seeds {
        let store = store();
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let mut current = writer(&dirs[0], &store);
        current
            .connect()
            .batch_execute(
                "CREATE TABLE kv (k int PRIMARY KEY, v text NOT NULL, n int NOT NULL DEFAULT 0);
                 CREATE INDEX kv_n ON kv (n);
                 CREATE TABLE log (id bigserial PRIMARY KEY, k int, note text);",
            )
            .unwrap();
        for generation in 0..2u64 {
            let config = ReplicaConfig { poll_interval: Duration::from_millis(1), ..ReplicaConfig::new(store.clone()) };
            let replica = Database::open_replica(dirs[generation as usize + 1].path(), config).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let reader = {
                let (replica, stop) = (replica.clone(), stop.clone());
                std::thread::spawn(move || {
                    let conn = replica.connect();
                    while !stop.load(Ordering::Relaxed) {
                        // The index and the table agree in every snapshot.
                        let row = conn
                            .query_one(
                                "SELECT (SELECT count(*) FROM kv WHERE n >= 0), (SELECT count(*) FROM kv), \
                                 (SELECT count(*) FROM log l WHERE NOT EXISTS (SELECT 1 FROM kv WHERE kv.k = l.k) AND l.note = 'put')",
                                &[],
                            )
                            .unwrap();
                        assert_eq!(row.get::<i64>(0), row.get::<i64>(1));
                    }
                })
            };
            let writers: Vec<_> = (0..3u64)
                .map(|t| {
                    let db = current.clone();
                    std::thread::spawn(move || {
                        let conn = db.connect();
                        let mut x = seed * 1000 + generation * 100 + t + 1;
                        let mut next = || {
                            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                            (x >> 33) as i64
                        };
                        for _ in 0..150 {
                            let k = (next() % 200) as i32;
                            let result = match next() % 10 {
                                0..=4 => conn.batch_execute(&format!(
                                    "BEGIN; INSERT INTO kv VALUES ({k}, 'v{t}', {t}) ON CONFLICT (k) DO UPDATE SET v = excluded.v, n = kv.n + 1; \
                                     INSERT INTO log (k, note) VALUES ({k}, 'put'); COMMIT"
                                )),
                                5..=6 => conn.batch_execute(&format!("DELETE FROM kv WHERE k = {k}")),
                                7 => conn.batch_execute(&format!(
                                    "BEGIN; UPDATE kv SET n = n + 1 WHERE k BETWEEN {k} AND {k} + 5; ROLLBACK"
                                )),
                                8 => conn.batch_execute(&format!("UPDATE kv SET v = v || '!' WHERE k % 17 = {}", k % 17)),
                                _ => conn.batch_execute("VACUUM"),
                            };
                            if let Err(e) = result {
                                // Lock conflicts between the writers are fine.
                                assert!(matches!(e.code(), "40001" | "40P01" | "55P03"), "{e}");
                                let _ = conn.batch_execute("ROLLBACK");
                            }
                        }
                    })
                })
                .collect();
            for t in writers {
                t.join().unwrap();
            }
            stop.store(true, Ordering::Relaxed);
            reader.join().unwrap();
            replica.replication_catch_up().unwrap();
            assert_eq!(dump(&replica.connect()), dump(&current.connect()), "seed {seed} generation {generation}");
            assert!(replica.verify().unwrap().is_ok());
            replica.promote().unwrap();
            assert!(current.connect().execute("INSERT INTO log (note) VALUES ('fenced')", &[]).is_err());
            current = replica;
            current.connect().execute("INSERT INTO log (note) VALUES ('promoted')", &[]).unwrap();
        }
    }
}

#[test]
fn asynchronous_shipping_bounds_the_lag() {
    let store = store();
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = writer_with(&wdir, &store, RemoteSync::Async { interval: Duration::from_millis(10) });
    let w = db.connect();
    w.batch_execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    // The first upload happens within the interval.
    std::thread::sleep(Duration::from_millis(200));
    let config = ReplicaConfig { poll_interval: Duration::from_millis(5), ..ReplicaConfig::new(store.clone()) };
    let replica = Database::open_replica(rdir.path(), config).unwrap();
    let r = replica.connect();
    let mut worst = Duration::ZERO;
    for i in 0..30 {
        w.execute("INSERT INTO t VALUES ($1)", &[&i]).unwrap();
        let start = Instant::now();
        while count(&r, "SELECT count(*) FROM t") <= i as i64 {
            assert!(start.elapsed() < Duration::from_secs(20), "commit {i} never arrived");
            std::thread::sleep(Duration::from_millis(1));
        }
        worst = worst.max(start.elapsed());
    }
    // Upload interval plus poll interval, with a wide margin for a busy
    // test machine.
    assert!(worst < Duration::from_secs(5), "worst lag {worst:?}");
}
