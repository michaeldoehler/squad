//! Rough timing of the disk engine. Not a rigorous benchmark.
//!
//! ```text
//! cargo run --release -p squad-storage --example storage_bench [dir]
//! ```

use std::ops::Bound;
use std::time::Instant;

use squad_storage::{DiskEngine, DiskOptions, Engine, SyncMode, WriteBatch};

const KEYS: u64 = 1_000_000;

fn key(i: u64) -> Vec<u8> {
    // Scramble so inserts and lookups hit the tree in random order.
    format!("user:{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes()
}

fn main() {
    let dir = match std::env::args().nth(1) {
        Some(d) => std::path::PathBuf::from(d),
        None => std::env::temp_dir().join(format!("squad-storage-bench-{}", std::process::id())),
    };
    let _ = std::fs::remove_dir_all(&dir);
    let open = |mode| {
        DiskEngine::open(&dir, DiskOptions { sync_mode: mode, cache_pages: 64 * 1024, ..Default::default() }).unwrap()
    };

    let engine = open(SyncMode::Async);
    let t = Instant::now();
    for chunk in 0..KEYS / 1000 {
        let mut b = WriteBatch::new();
        for i in chunk * 1000..(chunk + 1) * 1000 {
            b.put(key(i), vec![b'v'; 100]);
        }
        engine.write(b).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    println!("bulk load:        {KEYS} keys in {secs:.2} s ({:.0} keys/s, batches of 1000)", KEYS as f64 / secs);

    let t = Instant::now();
    engine.checkpoint().unwrap();
    println!("checkpoint:       {:.0} ms, {} pages", t.elapsed().as_secs_f64() * 1e3, engine.stats().page_count);

    let n = 2_000_000usize;
    for (label, distinct) in [("all keys, uniformly random", KEYS), ("hot set of 1000 keys", 1000)] {
        let keys: Vec<Vec<u8>> = (0..distinct).map(key).collect();
        let t = Instant::now();
        let mut found = 0usize;
        for i in 0..n {
            found += engine.get(&keys[i % keys.len()]).unwrap().is_some() as usize;
        }
        assert_eq!(found, n);
        println!("cached get:       {:.0} ns/op ({label})", t.elapsed().as_nanos() as f64 / n as f64);
    }

    let t = Instant::now();
    let mut rows = 0;
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let lo = cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let page = engine.scan(lo, Bound::Unbounded, false, 1000).unwrap();
        if page.is_empty() {
            break;
        }
        rows += page.len();
        cursor = page.last().map(|(k, _)| k.clone());
    }
    println!(
        "full scan:        {:.0} ns/row ({rows} rows, chunks of 1000)",
        t.elapsed().as_nanos() as f64 / rows as f64
    );

    let n = 100_000u64;
    let t = Instant::now();
    for i in 0..n {
        let mut b = WriteBatch::new();
        b.put(key(KEYS + i), vec![b'v'; 100]);
        engine.write(b).unwrap();
    }
    println!("single-key write: {:.0} ops/s (async, no fsync per write)", n as f64 / t.elapsed().as_secs_f64());
    drop(engine);

    let t = Instant::now();
    let engine = open(SyncMode::Sync);
    println!("reopen:           {:.1} ms (clean shutdown)", t.elapsed().as_secs_f64() * 1e3);

    let n = 2000u64;
    let t = Instant::now();
    for i in 0..n {
        let mut b = WriteBatch::new();
        b.put(key(2 * KEYS + i), vec![b'v'; 100]);
        let lsn = engine.write(b).unwrap();
        engine.sync(lsn).unwrap();
    }
    println!("durable write:    {:.0} ops/s (one thread, fsync per write)", n as f64 / t.elapsed().as_secs_f64());

    let threads = 16u64;
    let engine = std::sync::Arc::new(engine);
    let t = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|th| {
            let engine = engine.clone();
            std::thread::spawn(move || {
                for i in 0..n {
                    let mut b = WriteBatch::new();
                    b.put(key(3 * KEYS + th * n + i), vec![b'v'; 100]);
                    let lsn = engine.write(b).unwrap();
                    engine.sync(lsn).unwrap();
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let stats = engine.stats();
    println!(
        "durable write:    {:.0} ops/s ({threads} threads, group commit, {} fsyncs total)",
        (threads * n) as f64 / t.elapsed().as_secs_f64(),
        stats.fsyncs
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
