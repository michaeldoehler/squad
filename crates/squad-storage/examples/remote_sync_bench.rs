//! Commit throughput and latency with a remote copy, by number of
//! concurrent writers and shipping mode.
//!
//! ```text
//! eval "$(scripts/garage-dev.sh start)"
//! cargo run --release -p squad-storage --example remote_sync_bench -- \
//!     [--seconds 5] [--writers 1,8,32] [--modes local,async,sync] [--uploads 1] [URL]
//! ```
//!
//! The store is the `s3://…` or `file://…` URL given, or else the one in
//! `SQUAD_TEST_S3_URL`. The benchmark works under a fresh key prefix and
//! removes it afterwards. Each writer thread commits one 120-byte row per
//! transaction for the given time; every commit waits for `Engine::sync`
//! (local fsync, and in `Sync` mode the upload of its log record). For
//! each configuration it prints commits/s, the 50th and 99th percentile
//! of commit latency, and how many commits one log object carried on
//! average.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use squad_object::{open_url, ObjectStore};
use squad_storage::{DiskEngine, DiskOptions, Engine, RemoteConfig, RemoteSync, TieredEngine, WriteBatch};

fn batch(writer: u64, i: u64) -> WriteBatch {
    let mut b = WriteBatch::new();
    let key = (writer << 40 | i).wrapping_mul(2_654_435_761) % 1_000_000;
    b.put(format!("key{key:08}").into_bytes(), vec![(i % 251) as u8; 120]);
    b
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

/// Runs `writers` threads committing for `seconds`; returns all commit
/// latencies and the elapsed time.
fn run(engine: &Arc<dyn Engine>, writers: u64, seconds: f64) -> (Vec<Duration>, f64) {
    let barrier = Arc::new(Barrier::new(writers as usize + 1));
    let threads: Vec<_> = (0..writers)
        .map(|w| {
            let (engine, barrier) = (engine.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                let end = Instant::now() + Duration::from_secs_f64(seconds);
                let mut times = Vec::new();
                let mut i = 0;
                while Instant::now() < end {
                    let t = Instant::now();
                    let lsn = engine.write(batch(w, i)).unwrap();
                    engine.sync(lsn).unwrap();
                    times.push(t.elapsed());
                    i += 1;
                }
                times
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    let mut all: Vec<Duration> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
    let elapsed = start.elapsed().as_secs_f64();
    all.sort();
    (all, elapsed)
}

fn main() {
    let mut seconds = 5.0;
    let mut writer_counts = vec![1u64, 8, 32];
    let mut modes = vec!["local".to_string(), "async".to_string(), "sync".to_string()];
    let mut uploads = 1usize;
    let mut url = std::env::var("SQUAD_TEST_S3_URL").ok();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seconds" => seconds = args.next().and_then(|v| v.parse().ok()).expect("--seconds N"),
            "--writers" => {
                writer_counts = args
                    .next()
                    .expect("--writers 1,8,32")
                    .split(',')
                    .map(|v| v.parse().expect("writer count"))
                    .collect()
            }
            "--modes" => {
                modes = args.next().expect("--modes local,async,sync").split(',').map(str::to_string).collect()
            }
            "--uploads" => uploads = args.next().and_then(|v| v.parse().ok()).expect("--uploads N"),
            other => url = Some(other.to_string()),
        }
    }
    let Some(url) = url else {
        eprintln!("give an s3:// or file:// URL, or set SQUAD_TEST_S3_URL");
        std::process::exit(2);
    };
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let prefix = format!("squad-bench/remote-sync-{nanos:x}");
    let (location, query) = url.split_once('?').unwrap_or((&url, ""));
    let base = location.trim_end_matches('/').to_string();
    // The store for one configuration, under its own part of the prefix.
    let store_url = |sub: &str| {
        let path = format!("{base}/{prefix}/{sub}");
        if query.is_empty() {
            path
        } else {
            format!("{path}?{query}")
        }
    };
    let display = base.clone();

    println!("remote_sync_bench: store {display} (prefix {prefix}), {seconds} s per configuration");
    println!("commit = one 120-byte row, local fsync (SyncMode::Sync) on every commit; {uploads} upload(s) in flight");
    println!(
        "{:<26} {:>8} {:>10} {:>10} {:>10} {:>9} {:>14}",
        "mode", "writers", "commits", "commits/s", "p50 (ms)", "p99 (ms)", "commits/upload"
    );
    let dir = tempfile::tempdir().unwrap();
    for &writers in &writer_counts {
        for mode in modes.iter().map(String::as_str) {
            let store: Arc<dyn ObjectStore> =
                open_url(&store_url(&format!("{mode}-{writers}"))).expect("open object store");
            let path = dir.path().join(format!("{mode}-{writers}"));
            let (engine, label): (Arc<dyn Engine>, &str) = match mode {
                "local" => (Arc::new(DiskEngine::open(&path, DiskOptions::default()).unwrap()), "local only"),
                "async" => {
                    let config =
                        RemoteConfig::new(store.clone(), RemoteSync::Async { interval: Duration::from_millis(100) });
                    (Arc::new(TieredEngine::open(&path, DiskOptions::default(), config).unwrap()), "remote Async 100ms")
                }
                _ => {
                    let config = RemoteConfig {
                        upload_concurrency: uploads,
                        ..RemoteConfig::new(store.clone(), RemoteSync::Sync)
                    };
                    (Arc::new(TieredEngine::open(&path, DiskOptions::default(), config).unwrap()), "remote Sync")
                }
            };
            let (times, elapsed) = run(&engine, writers, seconds);
            let n = times.len();
            let ms = |d: Duration| d.as_secs_f64() * 1e3;
            // Log objects written during the run, to show the grouping.
            let objects = store.list("wal/").map(|l| l.len()).unwrap_or(0);
            let per_upload = if mode == "local" || objects == 0 {
                "-".to_string()
            } else {
                format!("{:.1}", n as f64 / objects as f64)
            };
            println!(
                "{label:<26} {writers:>8} {n:>10} {:>10.0} {:>10.2} {:>9.2} {per_upload:>14}",
                n as f64 / elapsed,
                ms(percentile(&times, 0.5)),
                ms(percentile(&times, 0.99)),
            );
            drop(engine);
            for meta in store.list("").unwrap_or_default() {
                let _ = store.delete(&meta.key);
            }
        }
    }
}
