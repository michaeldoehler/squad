//! Measures what the remote copy costs: commit latency with synchronous
//! shipping to an S3 endpoint, the upload volume of snapshots, and the
//! time a restore takes with one and with several requests at a time.
//!
//! ```text
//! cargo run --release -p squad-storage --example remote_bench --features squad-object/test-util
//! ```
//!
//! Without arguments it starts the in-process S3 emulator, so the numbers
//! show the overhead of the mechanism (HTTP, signing, two requests per
//! commit group) without network latency. Pass an `s3://…` or `file://…`
//! URL to measure a real store.

use std::sync::Arc;
use std::time::{Duration, Instant};

use squad_object::{open_url, ObjectStore};
use squad_storage::{
    restore_from_with, DiskEngine, DiskOptions, Engine, RemoteConfig, RemoteSync, TieredEngine, WriteBatch,
    DEFAULT_RESTORE_CONCURRENCY,
};

fn batch(i: u64) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.put(format!("key{:08}", i.wrapping_mul(2654435761) % 200_000).into_bytes(), vec![(i % 251) as u8; 120]);
    b
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)].as_secs_f64() * 1e6
}

fn commits(engine: &dyn Engine, n: u64) -> (f64, f64, f64) {
    let mut times = Vec::with_capacity(n as usize);
    let start = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        let lsn = engine.write(batch(i)).unwrap();
        engine.sync(lsn).unwrap();
        times.push(t.elapsed());
    }
    let total = start.elapsed().as_secs_f64();
    times.sort();
    (percentile(&times, 0.5), percentile(&times, 0.99), n as f64 / total)
}

fn main() {
    let url = std::env::args().nth(1);
    let emulator;
    let store: Arc<dyn ObjectStore> = match &url {
        Some(u) => open_url(u).expect("open object store"),
        None => {
            emulator = squad_object::emulator::S3Emulator::start();
            open_url(&emulator.url("bench")).expect("open emulator")
        }
    };
    let dir = tempfile::tempdir().unwrap();
    println!("{:<44} {:>10} {:>10} {:>10}", "commit of one 120-byte row", "p50 (µs)", "p99 (µs)", "commits/s");

    let local = DiskEngine::open(&dir.path().join("local"), DiskOptions::default()).unwrap();
    let (p50, p99, rate) = commits(&local, 2000);
    println!("{:<44} {p50:>10.0} {p99:>10.0} {rate:>10.0}", "local only (fsync)");
    drop(local);

    let mode = RemoteSync::Async { interval: Duration::from_millis(200) };
    let engine =
        TieredEngine::open(&dir.path().join("async"), DiskOptions::default(), RemoteConfig::new(store.clone(), mode))
            .unwrap();
    let (p50, p99, rate) = commits(&engine, 2000);
    println!("{:<44} {p50:>10.0} {p99:>10.0} {rate:>10.0}", "remote, async shipping");
    drop(engine);

    let engine = TieredEngine::open(
        &dir.path().join("sync"),
        DiskOptions::default(),
        RemoteConfig { restore: true, ..RemoteConfig::new(store.clone(), RemoteSync::Sync) },
    )
    .unwrap();
    let (p50, p99, rate) = commits(&engine, 2000);
    println!("{:<44} {p50:>10.0} {p99:>10.0} {rate:>10.0}", "remote, sync shipping (1 thread)");

    // Group commit over uploads: many committers share one object.
    let engine = Arc::new(engine);
    let start = Instant::now();
    let threads: Vec<_> = (0..16u64)
        .map(|t| {
            let engine = engine.clone();
            std::thread::spawn(move || {
                for i in 0..500 {
                    let lsn = engine.write(batch(t * 1_000_000 + i)).unwrap();
                    engine.sync(lsn).unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    println!(
        "{:<44} {:>10} {:>10} {:>10.0}",
        "remote, sync shipping (16 threads)",
        "",
        "",
        8000.0 / start.elapsed().as_secs_f64()
    );
    let before = engine.remote_stats();
    println!("\nlog objects so far: {}, {} bytes", before.wal_objects, before.wal_bytes);

    // Snapshot volume: load, snapshot, change 1% of the rows, snapshot.
    for i in 0..200_000u64 {
        let mut b = WriteBatch::new();
        b.put(format!("row{i:08}").into_bytes(), vec![(i % 251) as u8; 100]);
        engine.write(b).unwrap();
    }
    engine.checkpoint().unwrap();
    let full = engine.remote_stats();
    for i in 0..2000u64 {
        let mut b = WriteBatch::new();
        b.put(format!("row{:08}", i * 100).into_bytes(), vec![7u8; 100]);
        engine.write(b).unwrap();
    }
    let t = Instant::now();
    engine.checkpoint().unwrap();
    let after = engine.remote_stats();
    let size = dir.path().join("sync").join("data.db").metadata().unwrap().len();
    println!("data file: {:.1} MiB", size as f64 / 1048576.0);
    println!(
        "first snapshot uploaded {} chunks, {:.1} MiB",
        full.chunks_uploaded - before.chunks_uploaded,
        (full.chunk_bytes_uploaded - before.chunk_bytes_uploaded) as f64 / 1048576.0
    );
    println!(
        "snapshot after changing 1% of the rows (spread evenly): {} chunks uploaded, {:.1} MiB, {} chunks reused, {:.0} ms",
        after.chunks_uploaded - full.chunks_uploaded,
        (after.chunk_bytes_uploaded - full.chunk_bytes_uploaded) as f64 / 1048576.0,
        after.chunks_reused - full.chunks_reused,
        t.elapsed().as_secs_f64() * 1e3
    );

    // Restore: that snapshot plus 1500 log objects (one per commit),
    // with one request at a time and with the default of 16.
    for i in 0..1500 {
        let lsn = engine.write(batch(i)).unwrap();
        engine.sync(lsn).unwrap();
    }
    println!();
    for concurrency in [1, DEFAULT_RESTORE_CONCURRENCY] {
        let target = dir.path().join(format!("restore-{concurrency}"));
        let stats = restore_from_with(&*store, &target, DiskOptions::default(), concurrency).unwrap();
        println!(
            "restore with {concurrency:>2} requests at a time: {} chunks ({:.1} MiB), {} log objects, {:.2} s",
            stats.chunks,
            stats.chunk_bytes as f64 / 1048576.0,
            stats.wal_objects,
            stats.duration.as_secs_f64()
        );
    }
}
