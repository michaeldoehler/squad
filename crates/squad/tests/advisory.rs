//! Advisory locks (`pg_advisory_lock` and relatives) between sessions.
//! Expected behaviour is PostgreSQL 16's.

use std::time::{Duration, Instant};

use squad::Database;

fn try_lock(conn: &squad::Connection, call: &str) -> bool {
    conn.query_one(&format!("SELECT {call}"), &[]).unwrap().get(0)
}

#[test]
fn an_exclusive_lock_blocks_other_sessions_until_it_is_released() {
    let db = Database::open_in_memory().unwrap();
    let (a, b) = (db.connect(), db.connect());
    a.query("SELECT pg_advisory_lock(42)", &[]).unwrap();
    assert!(!try_lock(&b, "pg_try_advisory_lock(42)"));
    assert!(!try_lock(&b, "pg_try_advisory_lock_shared(42)"));
    // Other keys, and the same number in the two-key space, are free.
    assert!(try_lock(&b, "pg_try_advisory_lock(43)"));
    assert!(try_lock(&b, "pg_try_advisory_lock(0, 42)"));
    // A session never conflicts with itself.
    assert!(try_lock(&a, "pg_try_advisory_lock(42)"));

    let waiter = {
        let db = db.clone();
        std::thread::spawn(move || {
            let c = db.connect();
            let started = Instant::now();
            c.query("SELECT pg_advisory_lock(42)", &[]).unwrap();
            started.elapsed()
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(!waiter.is_finished(), "the lock was granted while another session held it");
    // Taken twice, so released twice.
    assert!(try_lock(&a, "pg_advisory_unlock(42)"));
    std::thread::sleep(Duration::from_millis(100));
    assert!(!waiter.is_finished());
    assert!(try_lock(&a, "pg_advisory_unlock(42)"));
    let waited = waiter.join().unwrap();
    assert!(waited >= Duration::from_millis(250), "waited only {waited:?}");
}

#[test]
fn shared_locks_coexist_and_exclude_exclusive_ones() {
    let db = Database::open_in_memory().unwrap();
    let (a, b) = (db.connect(), db.connect());
    assert!(try_lock(&a, "pg_try_advisory_lock_shared(7)"));
    assert!(try_lock(&b, "pg_try_advisory_lock_shared(7)"));
    assert!(!try_lock(&b, "pg_try_advisory_lock(7)"));
    assert!(try_lock(&a, "pg_advisory_unlock_shared(7)"));
    assert!(try_lock(&b, "pg_try_advisory_lock(7)"));
}

#[test]
fn unlocking_a_lock_not_held_returns_false_with_a_warning() {
    let db = Database::open_in_memory().unwrap();
    let a = db.connect();
    let results = a.simple_query("SELECT pg_advisory_unlock(1)").unwrap();
    let notice = &results[0].notices[0];
    assert_eq!(notice.message, "you don't own a lock of type ExclusiveLock");
    assert!(!try_lock(&a, "pg_advisory_unlock(1)"));
    // NULL arguments: NULL, no lock.
    let row = a.query_one("SELECT pg_try_advisory_lock(NULL::bigint) IS NULL", &[]).unwrap();
    assert!(row.get::<bool>(0));
}

#[test]
fn transaction_locks_end_with_the_transaction_and_session_locks_survive_rollback() {
    let db = Database::open_in_memory().unwrap();
    let (a, b) = (db.connect(), db.connect());
    a.batch_execute("BEGIN").unwrap();
    a.query("SELECT pg_advisory_xact_lock(5)", &[]).unwrap();
    a.query("SELECT pg_advisory_lock(6)", &[]).unwrap();
    assert!(!try_lock(&b, "pg_try_advisory_lock(5)"));
    // pg_advisory_unlock does not release a transaction-level lock.
    assert!(!try_lock(&a, "pg_advisory_unlock(5)"));
    a.batch_execute("ROLLBACK").unwrap();
    assert!(try_lock(&b, "pg_try_advisory_lock(5)"));
    assert!(!try_lock(&b, "pg_try_advisory_lock(6)"), "a session-level lock survives a rollback");

    // Outside a transaction block, a transaction-level lock lasts for
    // the statement.
    assert!(try_lock(&a, "pg_try_advisory_xact_lock(8)"));
    assert!(try_lock(&b, "pg_try_advisory_xact_lock(8)"));

    a.query("SELECT pg_advisory_unlock_all()", &[]).unwrap();
    assert!(try_lock(&b, "pg_try_advisory_lock(6)"));
}

#[test]
fn locks_are_released_when_the_session_ends() {
    let db = Database::open_in_memory().unwrap();
    let b = db.connect();
    {
        let a = db.connect();
        a.query("SELECT pg_advisory_lock(9)", &[]).unwrap();
        assert!(!try_lock(&b, "pg_try_advisory_lock(9)"));
    }
    assert!(try_lock(&b, "pg_try_advisory_lock(9)"));
}

#[test]
fn a_wait_ends_with_lock_timeout_or_cancel() {
    let db = Database::open_in_memory().unwrap();
    let (a, b) = (db.connect(), db.connect());
    a.query("SELECT pg_advisory_lock(1)", &[]).unwrap();

    b.batch_execute("SET lock_timeout = '100ms'").unwrap();
    let err = b.query("SELECT pg_advisory_lock(1)", &[]).unwrap_err();
    assert_eq!(err.code(), "55P03");
    b.batch_execute("RESET lock_timeout").unwrap();

    let cancel = b.cancel_handle();
    let worker = std::thread::spawn(move || b.query("SELECT pg_advisory_lock(1)", &[]));
    let started = Instant::now();
    while !worker.is_finished() {
        std::thread::sleep(Duration::from_millis(20));
        cancel.cancel();
        assert!(started.elapsed() < Duration::from_secs(10), "the wait was not cancelled");
    }
    assert_eq!(worker.join().unwrap().unwrap_err().code(), "57014");
}
