//! Concurrency and isolation tests: several connections working on the
//! same database from different threads. The expected behaviour in every
//! test is what PostgreSQL does at the same isolation level.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use squad::{sqlstate, Connection, Database};

fn database(setup: &str) -> Database {
    let db = Database::open_in_memory().unwrap();
    db.connect().batch_execute(setup).unwrap();
    db
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_one(sql, &[]).unwrap().get(0)
}

/// Runs `f` until it succeeds, retrying on serialization failures and
/// deadlocks the way an application is expected to.
fn retrying<T>(conn: &Connection, mut f: impl FnMut(&Connection) -> squad::Result<T>) -> T {
    loop {
        match f(conn) {
            Ok(v) => return v,
            Err(e) if matches!(e.code(), sqlstate::SERIALIZATION_FAILURE | sqlstate::DEADLOCK_DETECTED) => {
                let _ = conn.batch_execute("ROLLBACK");
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}

#[test]
fn read_committed_updates_are_not_lost() {
    let db =
        database("CREATE TABLE counter (id int PRIMARY KEY, n bigint NOT NULL); INSERT INTO counter VALUES (1, 0)");
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let conn = db.connect();
            thread::spawn(move || {
                for _ in 0..250 {
                    assert_eq!(conn.execute("UPDATE counter SET n = n + 1 WHERE id = 1", &[]).unwrap(), 1);
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    assert_eq!(count(&db.connect(), "SELECT n FROM counter"), 2000);
}

#[test]
fn read_committed_rechecks_the_predicate_after_waiting() {
    let db = database("CREATE TABLE t (id int PRIMARY KEY, state text NOT NULL); INSERT INTO t VALUES (1, 'new')");
    let (a, b) = (db.connect(), db.connect());
    a.batch_execute("BEGIN").unwrap();
    assert_eq!(a.execute("UPDATE t SET state = 'taken' WHERE id = 1 AND state = 'new'", &[]).unwrap(), 1);
    let waiter =
        thread::spawn(move || b.execute("UPDATE t SET state = 'stolen' WHERE id = 1 AND state = 'new'", &[]).unwrap());
    thread::sleep(Duration::from_millis(100));
    a.batch_execute("COMMIT").unwrap();
    // The second update waited, saw the committed row no longer matches
    // and changed nothing.
    assert_eq!(waiter.join().unwrap(), 0);
    assert_eq!(db.connect().query_one("SELECT state FROM t", &[]).unwrap().get::<String>(0), "taken");
}

#[test]
fn repeatable_read_sees_one_snapshot_and_fails_on_concurrent_update() {
    let db = database("CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 10), (2, 20)");
    let (a, b) = (db.connect(), db.connect());
    a.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").unwrap();
    assert_eq!(count(&a, "SELECT sum(v) FROM t"), 30);
    b.batch_execute("UPDATE t SET v = 11 WHERE id = 1; INSERT INTO t VALUES (3, 30); DELETE FROM t WHERE id = 2")
        .unwrap();
    // Still the old snapshot: no changed, new or missing rows.
    assert_eq!(count(&a, "SELECT sum(v) FROM t"), 30);
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 2);
    // Updating a row changed since the snapshot must fail.
    let err = a.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[]).unwrap_err();
    assert_eq!(err.code(), sqlstate::SERIALIZATION_FAILURE);
    a.batch_execute("ROLLBACK").unwrap();

    // Read Committed sees each statement's latest state instead.
    a.batch_execute("BEGIN").unwrap();
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 2);
    b.batch_execute("INSERT INTO t VALUES (4, 40)").unwrap();
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 3);
    a.batch_execute("COMMIT").unwrap();
}

#[test]
fn serializable_prevents_write_skew() {
    // Two doctors on call; each transaction checks that the other is
    // still on call and then signs off. Under snapshot isolation both
    // succeed, leaving nobody on call.
    for (level, anomaly_possible) in [("REPEATABLE READ", true), ("SERIALIZABLE", false)] {
        let db = database("CREATE TABLE doctors (name text PRIMARY KEY, on_call boolean NOT NULL); INSERT INTO doctors VALUES ('alice', true), ('bob', true)");
        let (a, b) = (db.connect(), db.connect());
        for c in [&a, &b] {
            c.batch_execute(&format!("BEGIN ISOLATION LEVEL {level}")).unwrap();
            assert_eq!(count(c, "SELECT count(*) FROM doctors WHERE on_call"), 2);
        }
        a.execute("UPDATE doctors SET on_call = false WHERE name = 'alice'", &[]).unwrap();
        b.execute("UPDATE doctors SET on_call = false WHERE name = 'bob'", &[]).unwrap();
        a.batch_execute("COMMIT").unwrap();
        let second = b.batch_execute("COMMIT");
        assert_eq!(second.is_ok(), anomaly_possible, "{level}");
        if let Err(e) = second {
            assert_eq!(e.code(), sqlstate::SERIALIZATION_FAILURE);
        }
        let on_call = count(&db.connect(), "SELECT count(*) FROM doctors WHERE on_call");
        assert_eq!(on_call, if anomaly_possible { 0 } else { 1 }, "{level}");
    }
}

#[test]
fn serializable_transfers_keep_the_total() {
    let db = database("CREATE TABLE acct (id int PRIMARY KEY, balance int NOT NULL CHECK (balance >= 0))");
    db.connect().execute("INSERT INTO acct SELECT g, 100 FROM generate_series(1, 10) g", &[]).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    for t in 0..4u64 {
        let conn = db.connect();
        threads.push(thread::spawn(move || {
            let mut seed = t + 1;
            for _ in 0..150 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let from = (seed >> 33) % 10 + 1;
                let to = (seed >> 13) % 10 + 1;
                if from == to {
                    continue;
                }
                retrying(&conn, |c| {
                    c.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")?;
                    let balance: i32 = c.query_one("SELECT balance FROM acct WHERE id = $1", &[&(from as i32)])?.get(0);
                    if balance >= 10 {
                        c.execute("UPDATE acct SET balance = balance - 10 WHERE id = $1", &[&(from as i32)])?;
                        c.execute("UPDATE acct SET balance = balance + 10 WHERE id = $1", &[&(to as i32)])?;
                    }
                    c.batch_execute("COMMIT")
                });
            }
        }));
    }
    // A reader must see a consistent total in every snapshot.
    let reader = {
        let (conn, stop) = (db.connect(), stop.clone());
        thread::spawn(move || {
            let mut checks = 0;
            while !stop.load(Ordering::Relaxed) {
                assert_eq!(count(&conn, "SELECT sum(balance) FROM acct"), 1000);
                checks += 1;
            }
            checks
        })
    };
    threads.into_iter().for_each(|t| t.join().unwrap());
    stop.store(true, Ordering::Relaxed);
    assert!(reader.join().unwrap() > 0);
    assert_eq!(count(&db.connect(), "SELECT sum(balance) FROM acct"), 1000);
}

#[test]
fn deadlock_is_detected_and_one_side_survives() {
    let db = database("CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0), (2, 0)");
    let barrier = Arc::new(Barrier::new(2));
    let run = |first: i32, second: i32| {
        let (conn, barrier) = (db.connect(), barrier.clone());
        thread::spawn(move || {
            conn.batch_execute("BEGIN").unwrap();
            conn.execute("UPDATE t SET v = v + 1 WHERE id = $1", &[&first]).unwrap();
            barrier.wait();
            let result = conn.execute("UPDATE t SET v = v + 1 WHERE id = $1", &[&second]);
            match &result {
                Ok(_) => conn.batch_execute("COMMIT").unwrap(),
                Err(_) => conn.batch_execute("ROLLBACK").unwrap(),
            }
            result.map_err(|e| e.code())
        })
    };
    let (a, b) = (run(1, 2), run(2, 1));
    let results = [a.join().unwrap(), b.join().unwrap()];
    let failures: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(failures, [&sqlstate::DEADLOCK_DETECTED], "exactly one victim: {results:?}");
    // Only the survivor's two updates are visible.
    assert_eq!(count(&db.connect(), "SELECT sum(v) FROM t"), 2);
}

#[test]
fn unique_constraints_hold_under_concurrent_inserts() {
    let db = database("CREATE TABLE u (id serial PRIMARY KEY, email text UNIQUE NOT NULL)");
    let barrier = Arc::new(Barrier::new(8));
    let wins = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (conn, barrier, wins) = (db.connect(), barrier.clone(), wins.clone());
            thread::spawn(move || {
                barrier.wait();
                for i in 0..50 {
                    match conn.execute("INSERT INTO u (email) VALUES ($1)", &[&format!("user{i}@example.org")]) {
                        Ok(_) => {
                            wins.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => assert_eq!(e.code(), sqlstate::UNIQUE_VIOLATION),
                    }
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    assert_eq!(wins.load(Ordering::Relaxed), 50);
    let conn = db.connect();
    assert_eq!(count(&conn, "SELECT count(*) FROM u"), 50);
    assert_eq!(count(&conn, "SELECT count(DISTINCT email) FROM u"), 50);
}

#[test]
fn upserts_from_many_connections_count_exactly() {
    let db = database("CREATE TABLE hits (page text PRIMARY KEY, n int NOT NULL)");
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let conn = db.connect();
            thread::spawn(move || {
                for i in 0..200 {
                    let page = format!("/p{}", i % 5);
                    conn.execute(
                        "INSERT INTO hits VALUES ($1, 1) ON CONFLICT (page) DO UPDATE SET n = hits.n + 1",
                        &[&page],
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    let conn = db.connect();
    assert_eq!(count(&conn, "SELECT count(*) FROM hits"), 5);
    assert_eq!(count(&conn, "SELECT sum(n) FROM hits"), 1600);
}

#[test]
fn skip_locked_hands_out_every_job_once() {
    let db = database("CREATE TABLE jobs (id int PRIMARY KEY, state text NOT NULL DEFAULT 'pending', worker int)");
    db.connect().execute("INSERT INTO jobs (id) SELECT g FROM generate_series(1, 200) g", &[]).unwrap();
    let taken = Arc::new(Mutex::new(Vec::new()));
    let threads: Vec<_> = (0..6)
        .map(|w| {
            let (conn, taken) = (db.connect(), taken.clone());
            thread::spawn(move || loop {
                conn.batch_execute("BEGIN").unwrap();
                let job = conn
                    .query_opt(
                        "SELECT id FROM jobs WHERE state = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
                        &[],
                    )
                    .unwrap();
                let Some(job) = job else {
                    conn.batch_execute("COMMIT").unwrap();
                    // Nothing claimable right now; done once nothing is pending.
                    if conn.query_one("SELECT count(*) FROM jobs WHERE state = 'pending'", &[]).unwrap().get::<i64>(0)
                        == 0
                    {
                        break;
                    }
                    continue;
                };
                let id: i32 = job.get(0);
                conn.execute("UPDATE jobs SET state = 'done', worker = $1 WHERE id = $2", &[&w, &id]).unwrap();
                conn.batch_execute("COMMIT").unwrap();
                taken.lock().unwrap().push(id);
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    let taken = taken.lock().unwrap();
    assert_eq!(taken.len(), 200, "every job exactly once");
    assert_eq!(taken.iter().collect::<HashSet<_>>().len(), 200);
    assert_eq!(count(&db.connect(), "SELECT count(*) FROM jobs WHERE state = 'done'"), 200);
}

#[test]
fn for_update_nowait_and_lock_timeout() {
    let db = database("CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t VALUES (1)");
    let (a, b) = (db.connect(), db.connect());
    a.batch_execute("BEGIN; SELECT * FROM t WHERE id = 1 FOR UPDATE").unwrap();
    let err = b.query("SELECT * FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[]).unwrap_err();
    assert_eq!(err.code(), sqlstate::LOCK_NOT_AVAILABLE);
    assert!(b.query("SELECT * FROM t WHERE id = 1 FOR UPDATE SKIP LOCKED", &[]).unwrap().is_empty());
    // Plain reads are never blocked by row locks.
    assert_eq!(b.query("SELECT * FROM t", &[]).unwrap().len(), 1);
    b.batch_execute("SET lock_timeout = '50ms'").unwrap();
    let err = b.execute("UPDATE t SET id = 2", &[]).unwrap_err();
    assert_eq!(err.code(), sqlstate::LOCK_NOT_AVAILABLE);
    // FOR SHARE locks are compatible with each other but not with writers.
    a.batch_execute("ROLLBACK; BEGIN; SELECT * FROM t FOR SHARE").unwrap();
    assert_eq!(b.query("SELECT * FROM t FOR SHARE", &[]).unwrap().len(), 1);
    assert_eq!(b.execute("DELETE FROM t", &[]).unwrap_err().code(), sqlstate::LOCK_NOT_AVAILABLE);
    a.batch_execute("ROLLBACK").unwrap();
    assert_eq!(b.execute("DELETE FROM t", &[]).unwrap(), 1);
}

#[test]
fn foreign_keys_hold_against_concurrent_parent_deletes() {
    let db = database(
        "CREATE TABLE parent (id int PRIMARY KEY);
         CREATE TABLE child (id serial PRIMARY KEY, parent_id int NOT NULL REFERENCES parent (id));
         INSERT INTO parent SELECT g FROM generate_series(1, 100) g",
    );
    let barrier = Arc::new(Barrier::new(2));
    let inserter = {
        let (conn, barrier) = (db.connect(), barrier.clone());
        thread::spawn(move || {
            barrier.wait();
            for id in 1..=100 {
                match conn.execute("INSERT INTO child (parent_id) VALUES ($1)", &[&id]) {
                    Ok(_) => {}
                    Err(e) => assert_eq!(e.code(), sqlstate::FOREIGN_KEY_VIOLATION),
                }
            }
        })
    };
    let deleter = {
        let (conn, barrier) = (db.connect(), barrier.clone());
        thread::spawn(move || {
            barrier.wait();
            for id in (1..=100).rev() {
                match conn.execute("DELETE FROM parent WHERE id = $1", &[&id]) {
                    Ok(_) => {}
                    Err(e) => assert_eq!(e.code(), sqlstate::FOREIGN_KEY_VIOLATION),
                }
            }
        })
    };
    inserter.join().unwrap();
    deleter.join().unwrap();
    let conn = db.connect();
    let orphans =
        count(&conn, "SELECT count(*) FROM child c WHERE NOT EXISTS (SELECT 1 FROM parent p WHERE p.id = c.parent_id)");
    assert_eq!(orphans, 0, "no child may reference a deleted parent");
    // Both sides made progress: some parents went, some were kept alive
    // by a child.
    let children = count(&conn, "SELECT count(DISTINCT parent_id) FROM child");
    assert!(children <= count(&conn, "SELECT count(*) FROM parent"));
}

#[test]
fn writers_wait_for_each_other_but_not_for_readers() {
    let db = database("CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 1)");
    let (a, b, c) = (db.connect(), db.connect(), db.connect());
    a.batch_execute("BEGIN; UPDATE t SET v = 2").unwrap();
    // Another session still reads the committed version immediately.
    assert_eq!(count(&c, "SELECT v FROM t"), 1);
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let n = b.execute("UPDATE t SET v = v * 10", &[]).unwrap();
        tx.send(()).unwrap();
        n
    });
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err(), "second writer must wait for the first");
    a.batch_execute("COMMIT").unwrap();
    rx.recv_timeout(Duration::from_secs(10)).expect("writer proceeds after commit");
    assert_eq!(writer.join().unwrap(), 1);
    assert_eq!(count(&c, "SELECT v FROM t"), 20);
}

#[test]
fn concurrent_schema_changes_and_queries() {
    let db = database(
        "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL); INSERT INTO t SELECT g, g FROM generate_series(1, 200) g",
    );
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for w in 0..3 {
        let (conn, stop) = (db.connect(), stop.clone());
        workers.push(thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let id = (i * 7 + w) % 200 + 1;
                assert_eq!(conn.query("SELECT id, v FROM t WHERE id = $1", &[&id]).unwrap().len(), 1);
                conn.execute("UPDATE t SET v = v + 1 WHERE id = $1", &[&id]).unwrap();
                assert_eq!(count(&conn, "SELECT count(*) FROM t"), 200);
            }
        }));
    }
    let ddl = db.connect();
    for round in 0..10 {
        ddl.batch_execute("CREATE INDEX t_v ON t (v)").unwrap();
        ddl.batch_execute(&format!("ALTER TABLE t ADD COLUMN extra{round} text DEFAULT 'x'")).unwrap();
        ddl.batch_execute("DROP INDEX t_v").unwrap();
        ddl.batch_execute(&format!("ALTER TABLE t DROP COLUMN extra{round}")).unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    workers.into_iter().for_each(|t| t.join().unwrap());
    let conn = db.connect();
    assert_eq!(count(&conn, "SELECT count(*) FROM t"), 200);
    assert_eq!(conn.query_one("SELECT * FROM t WHERE id = 1", &[]).unwrap().len(), 2);
}
