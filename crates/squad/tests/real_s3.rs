//! Remote storage, replicas and promotion against a real S3 service.
//!
//! These tests run only when `SQUAD_TEST_S3_URL` is set to an
//! `s3://bucket?…` URL (`eval "$(scripts/garage-dev.sh start)"` starts a
//! local Garage and sets it). Each test works under a key prefix of its
//! own and removes it at the end. Without the variable each test prints
//! that it was skipped and passes.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use squad::object::{open_url, ObjectStore};
use squad::{Config, Database, RemoteConfig, RemoteSync, ReplicaConfig};

/// The store under a fresh prefix, or `None` (with a visible note).
fn real_s3(what: &str) -> Option<Arc<dyn ObjectStore>> {
    let Ok(url) = std::env::var("SQUAD_TEST_S3_URL") else {
        // Written to the handle directly: the test harness captures
        // `eprintln!`, and a skipped test should be noticed.
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "skipped {what}: SQUAD_TEST_S3_URL not set");
        return None;
    };
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let prefix = format!("squad-tests/{what}-{}-{nanos:x}", std::process::id());
    let (location, query) = url.split_once('?').unwrap_or((&url, ""));
    let url = format!("{}/{prefix}?{query}", location.trim_end_matches('/'));
    Some(open_url(&url).unwrap())
}

fn clean(store: &dyn ObjectStore) {
    for meta in store.list("").unwrap() {
        let _ = store.delete(&meta.key);
    }
}

fn writer_config(store: &Arc<dyn ObjectStore>, mode: RemoteSync) -> Config {
    Config { remote: Some(RemoteConfig::new(store.clone(), mode)), ..Config::default() }
}

fn count(db: &Database, sql: &str) -> i64 {
    db.connect().query_one(sql, &[]).unwrap().get(0)
}

#[test]
fn real_s3_restore_after_losing_the_machine() {
    let Some(store) = real_s3("restore") else { return };
    let first = tempfile::tempdir().unwrap();
    {
        let db = Database::open(first.path(), writer_config(&store, RemoteSync::Sync)).unwrap();
        let conn = db.connect();
        conn.batch_execute("CREATE TABLE t (id int PRIMARY KEY, v text)").unwrap();
        for i in 0..50 {
            conn.execute("INSERT INTO t VALUES ($1, repeat('x', 100))", &[&i]).unwrap();
        }
        db.checkpoint().unwrap();
        for i in 50..80 {
            conn.execute("INSERT INTO t VALUES ($1, 'after the snapshot')", &[&i]).unwrap();
        }
        // No clean shutdown: every acknowledged commit is in the store.
        std::mem::forget(conn);
        std::mem::forget(db);
    }
    let copy = tempfile::tempdir().unwrap();
    Database::restore_from(&*store, copy.path()).unwrap();
    let db = Database::open(copy.path(), Config::default()).unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 80);
    assert!(db.verify().unwrap().is_ok());
    drop(db);

    // A new machine opens the same remote and carries on.
    let second = tempfile::tempdir().unwrap();
    let db = Database::open(second.path(), writer_config(&store, RemoteSync::Sync)).unwrap();
    db.connect().execute("INSERT INTO t VALUES (1000, 'new machine')", &[]).unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 81);
    drop(db);
    clean(&*store);
}

#[test]
fn real_s3_a_replica_follows_the_writer() {
    let Some(store) = real_s3("replica") else { return };
    let (wdir, rdir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let db = Database::open(wdir.path(), writer_config(&store, RemoteSync::Sync)).unwrap();
    let w = db.connect();
    w.batch_execute("CREATE TABLE events (id serial PRIMARY KEY, payload jsonb)").unwrap();
    w.execute("INSERT INTO events (payload) SELECT jsonb_build_object('n', g) FROM generate_series(1, 200) g", &[])
        .unwrap();
    let config = ReplicaConfig { poll_interval: Duration::from_millis(50), ..ReplicaConfig::new(store.clone()) };
    let replica = Database::open_replica(rdir.path(), config).unwrap();
    for round in 0..5 {
        for _ in 0..10 {
            w.execute("INSERT INTO events (payload) VALUES ('{}')", &[]).unwrap();
        }
        let want = 200 + (round + 1) * 10;
        let deadline = Instant::now() + Duration::from_secs(30);
        while count(&replica, "SELECT count(*) FROM events") != want {
            assert!(Instant::now() < deadline, "round {round}: the replica did not catch up");
            thread::sleep(Duration::from_millis(20));
        }
    }
    let status = replica.replication_status().unwrap();
    assert!(status.last_error.is_none(), "{status:?}");
    drop(replica);
    drop(db);
    clean(&*store);
}

/// A writer commits continuously while two replicas are promoted at the
/// same instant and then commit continuously too. Exactly one node can
/// commit at the end, every other one is refused, and every
/// acknowledged commit of any of them is in the store.
#[test]
fn real_s3_two_concurrent_promotions() {
    let Some(store) = real_s3("promotion") else { return };
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let writer = Database::open(dirs[0].path(), writer_config(&store, RemoteSync::Sync)).unwrap();
    writer.connect().batch_execute("CREATE TABLE t (id text PRIMARY KEY)").unwrap();
    let manual = ReplicaConfig { poll_interval: Duration::from_secs(3600), ..ReplicaConfig::new(store.clone()) };
    let replicas: Vec<Database> =
        (1..=2).map(|i| Database::open_replica(dirs[i].path(), manual.clone()).unwrap()).collect();

    let stop = Arc::new(AtomicBool::new(false));
    // Commits until refused or told to stop; returns the acknowledged ids
    // and whether it was refused.
    let commit_loop = |db: Database, name: &'static str, stop: Arc<AtomicBool>| {
        thread::spawn(move || {
            let conn = db.connect();
            let mut acked = Vec::new();
            for i in 0.. {
                if stop.load(Ordering::SeqCst) {
                    return (acked, false);
                }
                let id = format!("{name}-{i:05}");
                match conn.execute("INSERT INTO t VALUES ($1)", &[&id]) {
                    Ok(_) => acked.push(id),
                    Err(_) => return (acked, true),
                }
            }
            unreachable!()
        })
    };
    let first = commit_loop(writer.clone(), "w0", stop.clone());
    thread::sleep(Duration::from_millis(200));

    let barrier = Arc::new(Barrier::new(2));
    let promotions: Vec<_> = replicas
        .iter()
        .cloned()
        .zip(["r1", "r2"])
        .map(|(db, name)| {
            let (barrier, stop) = (barrier.clone(), stop.clone());
            thread::spawn(move || {
                barrier.wait();
                match db.promote() {
                    Ok(()) => Some(commit_loop(db, name, stop)),
                    Err(e) => {
                        eprintln!("{name}: promotion refused: {e}");
                        None
                    }
                }
            })
        })
        .collect();
    let loops: Vec<_> = promotions.into_iter().map(|p| p.join().unwrap()).collect();
    assert!(loops.iter().any(Option::is_some), "neither promotion succeeded");
    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::SeqCst);

    let (mut acked, refused) = first.join().unwrap();
    assert!(refused, "the old writer was not fenced");
    let mut survivors = 0;
    for handle in loops.into_iter().flatten() {
        let (ids, refused) = handle.join().unwrap();
        acked.extend(ids);
        if !refused {
            survivors += 1;
        }
    }
    // A promoted node that stopped before it noticed its successor must
    // not commit now.
    let mut can_commit = 0;
    for (n, db) in replicas.iter().enumerate() {
        if !db.is_replica() && db.connect().execute("INSERT INTO t VALUES ($1)", &[&format!("probe-{n}")]).is_ok() {
            acked.push(format!("probe-{n}"));
            can_commit += 1;
        }
    }
    assert_eq!(can_commit, 1, "exactly one node must be the writer");
    assert!(survivors <= 1);
    drop(writer);
    drop(replicas);

    let copy = &dirs[3];
    Database::restore_from(&*store, copy.path()).unwrap();
    let db = Database::open(copy.path(), Config::default()).unwrap();
    let rows = db.connect().query("SELECT id FROM t", &[]).unwrap();
    let stored: BTreeSet<String> = rows.iter().map(|r| r.get::<String>(0)).collect();
    let missing: Vec<&String> = acked.iter().filter(|id| !stored.contains(*id)).collect();
    assert!(missing.is_empty(), "acknowledged commits lost: {missing:?}");
    eprintln!("{} acknowledged commits, all restored", acked.len());
    drop(db);
    clean(&*store);
}
