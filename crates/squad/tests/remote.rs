//! A database whose durable home is an object store.

use std::sync::Arc;
use std::time::Duration;

use squad::object::{LocalStore, MemoryStore, ObjectStore};
use squad::{Config, Database, RemoteConfig, RemoteSync};

fn config(store: &Arc<dyn ObjectStore>, mode: RemoteSync) -> Config {
    Config { remote: Some(RemoteConfig::new(store.clone(), mode)), ..Config::default() }
}

#[test]
fn a_lost_machine_is_rebuilt_from_the_object_store() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let first = tempfile::tempdir().unwrap();
    {
        let db = Database::open(first.path(), config(&store, RemoteSync::Sync)).unwrap();
        let conn = db.connect();
        conn.batch_execute(
            "CREATE TYPE state AS ENUM ('new', 'done');
             CREATE TABLE jobs (id serial PRIMARY KEY, payload jsonb NOT NULL, s state NOT NULL DEFAULT 'new');
             CREATE INDEX jobs_s ON jobs (s);",
        )
        .unwrap();
        for i in 0..300 {
            conn.execute("INSERT INTO jobs (payload) VALUES ($1)", &[&serde_json::json!({ "n": i })]).unwrap();
        }
        conn.execute("UPDATE jobs SET s = 'done' WHERE id % 3 = 0", &[]).unwrap();
        // No clean shutdown: with Sync every acknowledged commit is
        // already in the object store.
        std::mem::forget(conn);
        std::mem::forget(db);
    }
    drop(first);

    // Explicit restore into a plain local database.
    let copy = tempfile::tempdir().unwrap();
    Database::restore_from(&*store, copy.path()).unwrap();
    let db = Database::open(copy.path(), Config::default()).unwrap();
    let conn = db.connect();
    assert_eq!(conn.query_one("SELECT count(*) FROM jobs", &[]).unwrap().get::<i64>(0), 300);
    assert_eq!(conn.query_one("SELECT count(*) FROM jobs WHERE s = 'done'", &[]).unwrap().get::<i64>(0), 100);
    assert!(db.verify().unwrap().is_ok());

    // Or: open an empty directory with the same remote and carry on.
    let second = tempfile::tempdir().unwrap();
    let db = Database::open(second.path(), config(&store, RemoteSync::Sync)).unwrap();
    let conn = db.connect();
    let id: i32 = conn.query_one("INSERT INTO jobs (payload) VALUES ('{}') RETURNING id", &[]).unwrap().get(0);
    assert!(id > 300, "sequence restarted at {id}");
    assert_eq!(conn.query_one("SELECT count(*) FROM jobs", &[]).unwrap().get::<i64>(0), 301);
}

#[test]
fn a_directory_as_object_store() {
    let remote = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(remote.path()).unwrap());
    let local = tempfile::tempdir().unwrap();
    let mode = RemoteSync::Async { interval: Duration::from_millis(20) };
    {
        let db = Database::open(local.path(), config(&store, mode)).unwrap();
        let conn = db.connect();
        conn.batch_execute("CREATE TABLE t (id int PRIMARY KEY, v text)").unwrap();
        conn.execute("INSERT INTO t SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g", &[]).unwrap();
        db.checkpoint().unwrap();
        conn.execute("DELETE FROM t WHERE id > 1500", &[]).unwrap();
        // A clean shutdown ships what the background thread has not yet.
    }
    let copy = tempfile::tempdir().unwrap();
    Database::restore_from(&*store, copy.path()).unwrap();
    let db = Database::open(copy.path(), Config::default()).unwrap();
    assert_eq!(db.connect().query_one("SELECT count(*) FROM t", &[]).unwrap().get::<i64>(0), 1500);
    // Restoring into a directory that already holds a database is refused.
    assert!(Database::restore_from(&*store, copy.path()).is_err());
}
