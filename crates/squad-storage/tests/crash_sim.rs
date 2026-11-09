//! Deterministic crash simulation.
//!
//! For every seed the test runs random write batches against a
//! [`SimVfs`], kills the "process" at a random I/O operation (or injects
//! I/O errors), simulates power loss with random survival of unsynced
//! writes, reopens, and checks:
//!
//! * every batch acknowledged by `sync` or covered by a completed
//!   checkpoint is present,
//! * batches are all-or-nothing and the recovered state is exactly the
//!   state after some prefix of the batch history,
//! * `verify` finds nothing wrong.
//!
//! `SQUAD_SIM_SEEDS` sets the number of seeds, `SQUAD_SIM_START` the first
//! seed. A failure prints the seed; rerun with `SQUAD_SIM_START=<seed>
//! SQUAD_SIM_SEEDS=1` to reproduce it.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::*;
use squad_storage::{DiskEngine, DiskOptions, Engine, SimVfs, SyncMode, WriteBatch};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn run_seed(seed: u64) {
    let vfs = SimVfs::new(seed);
    let mut rng = Rng(seed.wrapping_mul(0xA24B_AED4_963E_E407) ^ 0x5EED);
    let dir = Path::new("/db");
    let space = [40, 400, 5000][rng.below(3) as usize];
    let opts = DiskOptions {
        sync_mode: if rng.chance(10) { SyncMode::Group } else { SyncMode::Sync },
        cache_pages: [16, 64, 1024][rng.below(3) as usize],
        checkpoint_wal_bytes: [2 << 10, 32 << 10, 1 << 20][rng.below(3) as usize],
        vfs: Arc::new(vfs.clone()),
    };

    // history[i] is the batch with LSN i + 1.
    let mut history: Vec<WriteBatch> = Vec::new();
    let mut durable_lsn = 0u64;
    let rounds = rng.range(3, 8);
    for round in 0..=rounds {
        let last_round = round == rounds;
        let errors_before = vfs.injected_errors();
        if !last_round {
            match rng.below(10) {
                0..=6 => vfs.crash_after(rng.range(1, 600)),
                7 => vfs.crash_after(rng.range(1, 40)),
                _ => vfs.set_error_rate(rng.range(500, 30_000)),
            }
        }
        let ctx = format!("seed {seed} round {round}");
        let engine = match DiskEngine::open(dir, opts.clone()) {
            Ok(e) => e,
            Err(e) => {
                assert!(
                    vfs.is_crashed() || vfs.injected_errors() > errors_before,
                    "{ctx}: open failed without an injected fault: {e}"
                );
                vfs.kill();
                vfs.crash();
                continue;
            }
        };

        // What recovery produced must be a prefix of the history that
        // includes everything we were promised is durable.
        let lsn = engine.last_lsn();
        assert!(lsn >= durable_lsn, "{ctx}: recovered LSN {lsn} but LSN {durable_lsn} was acknowledged as durable");
        assert!(lsn as usize <= history.len(), "{ctx}: recovered LSN {lsn} beyond history {}", history.len());
        history.truncate(lsn as usize);
        durable_lsn = lsn;
        let mut model = Model::new();
        history.iter().for_each(|b| apply(&mut model, b));
        assert_same(&engine, &model, &ctx);
        assert_verify_clean(&engine, &ctx);

        let steps = if last_round { 30 } else { rng.range(1, 300) };
        for _ in 0..steps {
            let batch = gen_batch(&mut rng, space, 10);
            // Even a failed write may have reached the log, so it takes
            // its place in the history either way.
            history.push(batch.clone());
            let lsn = match engine.write(batch.clone()) {
                Ok(lsn) => lsn,
                Err(_) => break,
            };
            assert_eq!(lsn as usize, history.len(), "{ctx}: unexpected LSN");
            apply(&mut model, &batch);
            if rng.chance(40) {
                match engine.sync(lsn) {
                    Ok(()) => durable_lsn = lsn,
                    Err(_) => break,
                }
            }
            if rng.chance(3) {
                match engine.checkpoint() {
                    Ok(()) => durable_lsn = lsn,
                    Err(_) => break,
                }
            }
            if rng.chance(2) {
                // A backup taken mid-flight must open and hold a prefix.
                let dest = format!("/backup-{round}-{lsn}");
                if engine.backup(Path::new(&dest)).is_ok() {
                    durable_lsn = lsn;
                    let copy = DiskEngine::open(Path::new(&dest), opts.clone());
                    if let Ok(copy) = copy {
                        assert_same(&copy, &model, &format!("{ctx} backup"));
                        assert_verify_clean(&copy, &format!("{ctx} backup"));
                    } else {
                        assert!(vfs.is_crashed() || vfs.injected_errors() > errors_before, "{ctx}: backup unreadable");
                    }
                } else {
                    break;
                }
            }
            if rng.chance(30) && !vfs.is_crashed() {
                // Reads keep working while the engine is healthy.
                if vfs.injected_errors() == errors_before {
                    check_reads(&mut rng, &engine, &model, space);
                }
            }
        }
        if last_round {
            assert_same(&engine, &model, &format!("{ctx} final"));
            assert_verify_clean(&engine, &format!("{ctx} final"));
            drop(engine);
            // Clean shutdown: everything must survive a power cut.
            vfs.crash();
            let engine = DiskEngine::open(dir, opts.clone()).unwrap();
            assert_same(&engine, &model, &format!("{ctx} after clean shutdown"));
        } else {
            vfs.kill();
            drop(engine);
            vfs.crash();
        }
    }
}

#[test]
fn crash_recovery_simulation() {
    let start = env("SQUAD_SIM_START", 0);
    let seeds = env("SQUAD_SIM_SEEDS", 150);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()) as u64;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            std::thread::spawn(move || {
                for seed in (start..start + seeds).filter(|s| s % threads == t) {
                    if let Err(panic) = std::panic::catch_unwind(|| run_seed(seed)) {
                        eprintln!("crash simulation failed for seed {seed}");
                        std::panic::resume_unwind(panic);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

/// Concurrent variant: several writers and a checkpointer race while the
/// process is killed at a random I/O operation. Thread interleaving makes
/// this test non-deterministic, but it covers what the single-threaded
/// simulation cannot: commits acknowledged while a checkpoint is rotating
/// the log.
fn run_concurrent_seed(seed: u64) {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    const OPS_PER_BATCH: usize = 3;
    let vfs = SimVfs::new(seed);
    let mut rng = Rng(seed ^ 0xC0FFEE);
    let dir = Path::new("/db");
    let opts = DiskOptions {
        sync_mode: SyncMode::Sync,
        cache_pages: 64,
        checkpoint_wal_bytes: [1 << 10, 16 << 10][rng.below(2) as usize],
        vfs: Arc::new(vfs.clone()),
    };
    // batch id -> LSN, for every write that returned successfully.
    let written: Arc<Mutex<HashMap<String, u64>>> = Arc::default();
    let acked: Arc<Mutex<HashSet<String>>> = Arc::default();
    for round in 0..4 {
        let ctx = format!("concurrent seed {seed} round {round}");
        let engine = Arc::new(DiskEngine::open(dir, opts.clone()).unwrap());
        let recovered = engine.last_lsn();
        let mut present: HashMap<String, usize> = HashMap::new();
        for (k, v) in dump(&*engine) {
            let k = String::from_utf8(k).unwrap();
            let id = k.rsplit_once('/').unwrap().0.to_string();
            assert_eq!(v, id.as_bytes(), "{ctx}: value does not belong to its key");
            *present.entry(id).or_default() += 1;
        }
        for (id, n) in &present {
            assert_eq!(*n, OPS_PER_BATCH, "{ctx}: batch {id} was applied partially");
        }
        for id in acked.lock().unwrap().iter() {
            assert!(present.contains_key(id), "{ctx}: acknowledged batch {id} is missing");
        }
        for (id, lsn) in written.lock().unwrap().iter() {
            assert_eq!(
                present.contains_key(id),
                *lsn <= recovered,
                "{ctx}: batch {id} (LSN {lsn}) vs recovered {recovered}"
            );
        }
        assert_verify_clean(&*engine, &ctx);
        // Batches lost in the crash can never come back; forget their LSNs,
        // which are about to be reused.
        written.lock().unwrap().retain(|_, lsn| *lsn <= recovered);

        vfs.crash_after(rng.range(20, 1500));
        let mut handles = Vec::new();
        for t in 0..3 {
            let (engine, written, acked) = (engine.clone(), written.clone(), acked.clone());
            handles.push(std::thread::spawn(move || {
                for n in 0.. {
                    let id = format!("r{round}t{t}n{n}");
                    let mut b = WriteBatch::new();
                    for i in 0..OPS_PER_BATCH {
                        b.put(format!("{id}/{i}").into_bytes(), id.clone().into_bytes());
                    }
                    let Ok(lsn) = engine.write(b) else { return };
                    written.lock().unwrap().insert(id.clone(), lsn);
                    if engine.sync(lsn).is_err() {
                        return;
                    }
                    acked.lock().unwrap().insert(id);
                }
            }));
        }
        let ck = engine.clone();
        handles.push(std::thread::spawn(move || {
            while ck.checkpoint().is_ok() {
                std::thread::yield_now();
            }
        }));
        handles.into_iter().for_each(|h| h.join().unwrap());
        vfs.kill();
        drop(engine);
        vfs.crash();
    }
}

#[test]
fn concurrent_crash_simulation() {
    let start = env("SQUAD_SIM_START", 0);
    for seed in start..start + env("SQUAD_SIM_CONCURRENT_SEEDS", 40) {
        if let Err(panic) = std::panic::catch_unwind(|| run_concurrent_seed(seed)) {
            eprintln!("concurrent crash simulation failed for seed {seed}");
            std::panic::resume_unwind(panic);
        }
    }
}
