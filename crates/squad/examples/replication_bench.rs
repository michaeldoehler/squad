//! Measures read replicas: how fast a replica applies a backlog, and how
//! long a commit takes to become visible on a replica that follows.
//!
//! ```sh
//! cargo run --release -p squad --example replication_bench
//! ```
//!
//! The object store is a local directory (`file://`), so the numbers
//! show squad's own cost; a real bucket adds its request latency to
//! every upload and every poll.

use std::sync::Arc;
use std::time::{Duration, Instant};

use squad::object::{LocalStore, ObjectStore};
use squad::{Config, Database, RemoteConfig, RemoteSync, ReplicaConfig};

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn main() -> squad::Result<()> {
    let rows: i64 = std::env::var("ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let root = tempfile::tempdir().expect("temporary directory");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(root.path().join("store")).expect("object store"));
    let db = Database::open(
        root.path().join("writer"),
        Config { remote: Some(RemoteConfig::new(store.clone(), RemoteSync::Sync)), ..Config::default() },
    )?;
    let w = db.connect();
    w.batch_execute(
        "CREATE TABLE events (id bigint PRIMARY KEY, project int NOT NULL, name text NOT NULL, payload jsonb);
         CREATE INDEX events_project ON events (project);",
    )?;

    // 1. A backlog: single-row commits and bulk commits the replica has
    //    to apply when it starts or after it was disconnected.
    let insert = w.prepare("INSERT INTO events VALUES ($1, $2, $3, $4)")?;
    let payload = serde_json::json!({ "path": "/pricing", "ms": 123, "ua": "Mozilla/5.0" });
    let start = Instant::now();
    for i in 0..rows {
        insert.execute(&[&i, &((i % 50) as i32), &"pageview", &payload])?;
    }
    let commit_time = start.elapsed();
    println!(
        "writer: {rows} single-row commits shipped synchronously in {:.2?} ({:.0} commits/s)",
        commit_time,
        rows as f64 / commit_time.as_secs_f64()
    );
    w.execute(
        "INSERT INTO events SELECT g, g % 50, 'bulk', '{}' FROM generate_series($1::bigint, $2::bigint) g",
        &[&rows, &(rows * 11 - 1)],
    )?;
    let manual = || ReplicaConfig { poll_interval: Duration::from_secs(3600), ..ReplicaConfig::new(store.clone()) };
    let start = Instant::now();
    let replica = Database::open_replica(root.path().join("replica-a"), manual())?;
    let elapsed = start.elapsed();
    let status = replica.replication_status().unwrap();
    let count: i64 = replica.connect().query_one("SELECT count(*) FROM events", &[])?.get(0);
    println!(
        "replica: opened from an empty directory and applied {} log records ({count} rows) in {:.2?} ({:.0} records/s, {:.0} rows/s)",
        status.applied_lsn,
        elapsed,
        status.applied_lsn as f64 / elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );
    drop(replica);

    // 2. The same after a snapshot: the replica downloads the snapshot
    //    instead of replaying the log.
    db.checkpoint()?;
    let start = Instant::now();
    let replica = Database::open_replica(root.path().join("replica-b"), manual())?;
    println!("replica: opened from a snapshot of {count} rows in {:.2?}", start.elapsed());
    drop(replica);

    // 3. Lag: time from the writer's commit returning to the row being
    //    visible on a replica that polls.
    for poll_ms in [1u64, 10, 100] {
        let config =
            ReplicaConfig { poll_interval: Duration::from_millis(poll_ms), ..ReplicaConfig::new(store.clone()) };
        let dir = root.path().join(format!("replica-lag-{poll_ms}"));
        let replica = Database::open_replica(&dir, config)?;
        let r = replica.connect();
        let seen = r.prepare("SELECT 1 FROM events WHERE id = $1")?;
        let mut lags = Vec::new();
        for i in 0..300i64 {
            let id = -1 - i - 1000 * poll_ms as i64;
            insert.execute(&[&id, &1i32, &"lag", &payload])?;
            let start = Instant::now();
            while seen.query_opt(&[&id])?.is_none() {
                std::thread::yield_now();
            }
            lags.push(start.elapsed());
            // Commits arrive at arbitrary points of the poll cycle.
            std::thread::sleep(Duration::from_micros(1700));
        }
        lags.sort();
        println!(
            "lag with a {poll_ms} ms poll interval: p50 {:.2?}, p99 {:.2?}, max {:.2?}",
            percentile(&lags, 0.50),
            percentile(&lags, 0.99),
            lags[lags.len() - 1]
        );
    }

    // 4. Promotion.
    let replica = Database::open_replica(root.path().join("replica-c"), manual())?;
    let start = Instant::now();
    replica.promote()?;
    replica.connect().execute("INSERT INTO events VALUES (-999999, 0, 'promoted', NULL)", &[])?;
    println!("promotion until the first commit of the new writer: {:.2?}", start.elapsed());
    Ok(())
}
