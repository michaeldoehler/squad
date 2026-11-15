//! Cancelling statements, `statement_timeout`, notices about statements
//! without effect, and `LOCK TABLE`.

use std::time::{Duration, Instant};

use squad::{Database, Severity};

/// A query that takes far longer than any test: 10^10 joined rows.
const ENDLESS: &str = "SELECT count(*) FROM generate_series(1, 100000) a, generate_series(1, 100000) b";

fn memory() -> squad::Connection {
    Database::open_in_memory().unwrap().connect()
}

/// Runs `sql` on another thread and cancels it until it stops.
fn run_and_cancel(conn: &squad::Connection, sql: &'static str) -> squad::Error {
    let cancel = conn.cancel_handle();
    let worker = {
        let conn = conn.clone();
        std::thread::spawn(move || conn.query(sql, &[]))
    };
    let started = Instant::now();
    // A request made before the query started is discarded, so repeat it
    // until the query has stopped.
    while !worker.is_finished() {
        std::thread::sleep(Duration::from_millis(20));
        cancel.cancel();
        assert!(started.elapsed() < Duration::from_secs(60), "the query was not cancelled");
    }
    worker.join().unwrap().expect_err("the query should have been cancelled")
}

#[test]
fn cancel_handle_stops_a_running_query() {
    let conn = memory();
    conn.batch_execute("CREATE TABLE t (a int)").unwrap();
    let err = run_and_cancel(&conn, ENDLESS);
    assert_eq!(err.code(), "57014");
    assert_eq!(err.message(), "canceling statement due to user request");
    // The connection is usable afterwards, and a request made while idle
    // does not cancel the next statement.
    conn.cancel_handle().cancel();
    conn.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    let n: i64 = conn.query_one("SELECT count(*) FROM t", &[]).unwrap().get(0);
    assert_eq!(n, 1);
    // Sorting, hashing and materializing loops are interruptible too.
    let err = run_and_cancel(&conn, "SELECT count(*) FROM generate_series(1, 2000000000)");
    assert_eq!(err.code(), "57014");
    let err = run_and_cancel(
        &conn,
        "SELECT count(*) FROM (SELECT a FROM generate_series(1, 100000) a, generate_series(1, 100000) b ORDER BY 1) s",
    );
    assert_eq!(err.code(), "57014");
}

#[test]
fn cancel_inside_a_transaction_fails_the_transaction() {
    let conn = memory();
    conn.batch_execute("CREATE TABLE t (a int)").unwrap();
    conn.batch_execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    assert_eq!(run_and_cancel(&conn, ENDLESS).code(), "57014");
    assert_eq!(conn.execute("SELECT 1", &[]).unwrap_err().code(), "25P02");
    conn.batch_execute("ROLLBACK").unwrap();
    let n: i64 = conn.query_one("SELECT count(*) FROM t", &[]).unwrap().get(0);
    assert_eq!(n, 0);
}

#[test]
fn statement_timeout() {
    let conn = memory();
    let show = |conn: &squad::Connection| conn.query_one("SHOW statement_timeout", &[]).unwrap().get::<String>(0);
    assert_eq!(show(&conn), "0");
    conn.batch_execute("SET statement_timeout = 100").unwrap();
    assert_eq!(show(&conn), "100ms");
    let started = Instant::now();
    let err = conn.query(ENDLESS, &[]).unwrap_err();
    assert_eq!(err.code(), "57014");
    assert_eq!(err.message(), "canceling statement due to statement timeout");
    assert!(started.elapsed() < Duration::from_secs(10));
    // Short statements are unaffected; each statement has its own budget.
    for _ in 0..3 {
        conn.query("SELECT count(*) FROM generate_series(1, 1000)", &[]).unwrap();
    }
    let err = conn.query("SELECT count(*) FROM generate_series(1, 2000000000)", &[]).unwrap_err();
    assert_eq!(err.code(), "57014");
    conn.batch_execute("RESET statement_timeout").unwrap();
    assert_eq!(show(&conn), "0");
    conn.batch_execute("SET statement_timeout = '2s'").unwrap();
    assert_eq!(show(&conn), "2000ms");
    conn.batch_execute("RESET ALL").unwrap();
    assert_eq!(show(&conn), "0");
}

#[test]
fn statements_without_effect_report_a_notice() {
    let conn = memory();
    conn.batch_execute("CREATE TABLE t (a int)").unwrap();
    let notices = |sql: &str| {
        let results = conn.simple_query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        results.into_iter().flat_map(|r| r.notices).collect::<Vec<_>>()
    };
    let warning = Severity::Warning;
    let notice = Severity::Notice;
    for (sql, severity, text) in [
        ("GRANT SELECT ON t TO PUBLIC", warning, "squad has no privileges; GRANT has no effect"),
        ("REVOKE ALL ON t FROM PUBLIC", warning, "squad has no privileges; REVOKE has no effect"),
        ("CREATE ROLE app LOGIN", warning, "squad has no roles; CREATE ROLE has no effect"),
        ("CREATE USER app", warning, "squad has no roles; CREATE USER has no effect"),
        ("ALTER ROLE app PASSWORD 'x'", warning, "squad has no roles; ALTER ROLE has no effect"),
        ("DROP ROLE app", warning, "squad has no roles; DROP ROLE has no effect"),
        ("GRANT app TO other", warning, "squad has no roles; GRANT ROLE has no effect"),
        (
            "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC",
            warning,
            "squad has no privileges; ALTER DEFAULT PRIVILEGES has no effect",
        ),
        ("ALTER TABLE t OWNER TO app", notice, "squad has no object owners; ALTER ... OWNER TO has no effect"),
        ("COMMENT ON TABLE t IS 'x'", notice, "squad does not store comments; COMMENT has no effect"),
        (
            "CREATE EXTENSION IF NOT EXISTS pgcrypto",
            notice,
            "squad has no extensions; CREATE EXTENSION \"pgcrypto\" has no effect",
        ),
        (
            "SET CONSTRAINTS ALL DEFERRED",
            warning,
            "squad checks every constraint immediately; SET CONSTRAINTS ... DEFERRED has no effect",
        ),
    ] {
        let got = notices(sql);
        assert_eq!(got.len(), 1, "{sql}: {got:?}");
        assert_eq!((got[0].severity, got[0].message.as_str()), (severity, text), "{sql}");
        assert_eq!(got[0].code, if severity == warning { "01000" } else { "00000" }, "{sql}");
    }
    // Statements that do what PostgreSQL does raise nothing.
    for sql in [
        "SET CONSTRAINTS ALL IMMEDIATE",
        "REINDEX TABLE t",
        "SELECT 1",
        "INSERT INTO t VALUES (1)",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
    ] {
        assert!(notices(sql).is_empty(), "{sql}");
    }
    // Notices are reported once, with their own statement.
    let results = conn.simple_query("SELECT 1; GRANT SELECT ON t TO PUBLIC; SELECT 2").unwrap();
    assert_eq!(results.iter().map(|r| r.notices.len()).collect::<Vec<_>>(), [0, 1, 0]);
    // Through PREPARE / EXECUTE as well.
    conn.batch_execute("PREPARE g AS SELECT 1").unwrap();
    assert!(notices("EXECUTE g").is_empty());
    // GRANT on a missing table still fails.
    assert_eq!(conn.batch_execute("GRANT SELECT ON missing TO PUBLIC").unwrap_err().code(), "42P01");
}

#[test]
fn lock_table_modes() {
    let db = Database::open_in_memory().unwrap();
    let a = db.connect();
    let b = db.connect();
    a.batch_execute("CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0)").unwrap();
    // Outside a transaction block, as in PostgreSQL.
    assert_eq!(a.batch_execute("LOCK TABLE t").unwrap_err().code(), "25P01");
    assert_eq!(a.batch_execute("LOCK TABLE t IN ROW EXCLUSIVE MODE").unwrap_err().code(), "25P01");
    // Modes squad cannot provide fail instead of doing nothing.
    for mode in ["SHARE UPDATE EXCLUSIVE", "SHARE", "SHARE ROW EXCLUSIVE", "ACCESS EXCLUSIVE"] {
        a.batch_execute("BEGIN").unwrap();
        let err = a.batch_execute(&format!("LOCK TABLE t IN {mode} MODE")).unwrap_err();
        assert_eq!(err.code(), "0A000", "{mode}");
        a.batch_execute("ROLLBACK").unwrap();
    }
    a.batch_execute("BEGIN").unwrap();
    assert_eq!(a.batch_execute("LOCK TABLE t").unwrap_err().code(), "0A000");
    a.batch_execute("ROLLBACK").unwrap();
    a.batch_execute("BEGIN").unwrap();
    assert_eq!(a.batch_execute("LOCK TABLE missing IN ROW SHARE MODE").unwrap_err().code(), "42P01");
    a.batch_execute("ROLLBACK").unwrap();

    // EXCLUSIVE lets readers through but blocks writers and other locks.
    a.batch_execute("BEGIN").unwrap();
    a.batch_execute("LOCK TABLE t IN EXCLUSIVE MODE").unwrap();
    let n: i64 = b.query_one("SELECT count(*) FROM t", &[]).unwrap().get(0);
    assert_eq!(n, 1);
    b.batch_execute("SET lock_timeout = 100").unwrap();
    assert_eq!(b.execute("UPDATE t SET v = 1", &[]).unwrap_err().code(), "55P03");
    b.batch_execute("BEGIN").unwrap();
    let err = b.batch_execute("LOCK TABLE t IN ROW SHARE MODE NOWAIT").unwrap_err();
    assert_eq!((err.code(), err.message()), ("55P03", "could not obtain lock on relation \"t\""));
    b.batch_execute("ROLLBACK").unwrap();
    // ACCESS SHARE conflicts only with ACCESS EXCLUSIVE.
    b.batch_execute("BEGIN; LOCK TABLE t IN ACCESS SHARE MODE NOWAIT; COMMIT").unwrap();
    a.batch_execute("COMMIT").unwrap();

    // ROW EXCLUSIVE conflicts with EXCLUSIVE and with DDL, not with writers.
    a.batch_execute("BEGIN").unwrap();
    a.batch_execute("LOCK TABLE t IN ROW EXCLUSIVE MODE").unwrap();
    b.execute("UPDATE t SET v = 2", &[]).unwrap();
    assert_eq!(b.batch_execute("ALTER TABLE t ADD COLUMN w int").unwrap_err().code(), "55P03");
    b.batch_execute("BEGIN").unwrap();
    assert_eq!(b.batch_execute("LOCK TABLE t IN EXCLUSIVE MODE NOWAIT").unwrap_err().code(), "55P03");
    b.batch_execute("ROLLBACK").unwrap();
    a.batch_execute("COMMIT").unwrap();
    b.batch_execute("ALTER TABLE t ADD COLUMN w int").unwrap();

    // As in PostgreSQL, a read-only transaction may take every mode.
    a.batch_execute("BEGIN READ ONLY").unwrap();
    a.batch_execute("LOCK TABLE t IN ACCESS SHARE MODE").unwrap();
    a.batch_execute("LOCK TABLE t IN ROW EXCLUSIVE MODE").unwrap();
    a.batch_execute("LOCK TABLE t IN EXCLUSIVE MODE").unwrap();
    a.batch_execute("ROLLBACK").unwrap();
}
