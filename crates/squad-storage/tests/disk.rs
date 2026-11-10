//! Tests against the real file system.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common::*;
use squad_storage::{DiskEngine, DiskOptions, Engine, SimVfs, StorageError, SyncMode, WriteBatch};

fn opts(sync_mode: SyncMode) -> DiskOptions {
    DiskOptions { sync_mode, cache_pages: 256, ..Default::default() }
}

fn put(engine: &dyn Engine, k: &str, v: &str) -> u64 {
    let mut b = WriteBatch::new();
    b.put(k.as_bytes().to_vec(), v.as_bytes().to_vec());
    engine.write(b).unwrap()
}

#[test]
fn reopen_keeps_data() {
    let tmp = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut rng = Rng(5);
    for round in 0..4 {
        let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Sync)).unwrap();
        assert_same(&engine, &model, &format!("round {round} after open"));
        for _ in 0..300 {
            let batch = gen_batch(&mut rng, 2000, 10);
            apply(&mut model, &batch);
            engine.write(batch).unwrap();
        }
        if round % 2 == 0 {
            engine.checkpoint().unwrap();
        }
        assert_verify_clean(&engine, &format!("round {round}"));
    }
    let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Sync)).unwrap();
    assert_same(&engine, &model, "final");
    let stats = engine.stats();
    assert!(stats.page_count > 2);
    assert_eq!(stats.wal_bytes, 0, "a clean shutdown leaves an empty log");
}

#[test]
fn recovers_from_wal_without_clean_shutdown() {
    let tmp = tempfile::tempdir().unwrap();
    let copy = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut rng = Rng(6);
    {
        let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Sync)).unwrap();
        let mut lsn = 0;
        for i in 0..400 {
            let batch = gen_batch(&mut rng, 500, 10);
            apply(&mut model, &batch);
            lsn = engine.write(batch).unwrap();
            if i == 150 {
                engine.checkpoint().unwrap();
            }
        }
        engine.sync(lsn).unwrap();
        // Simulate a process crash: copy the directory as it is on disk
        // right now, while the engine is still open and un-checkpointed.
        for sub in ["", "wal"] {
            let from = tmp.path().join(sub);
            let to = copy.path().join(sub);
            std::fs::create_dir_all(&to).unwrap();
            for entry in std::fs::read_dir(&from).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() && entry.file_name() != "LOCK" {
                    std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
                }
            }
        }
        assert!(engine.checkpoint_lsn() < engine.last_lsn());
    }
    let engine = DiskEngine::open(copy.path(), opts(SyncMode::Sync)).unwrap();
    assert_same(&engine, &model, "after WAL replay");
    assert_verify_clean(&engine, "after WAL replay");
    assert_eq!(engine.last_lsn(), 400);
}

#[test]
fn double_open_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let first = DiskEngine::open(tmp.path(), opts(SyncMode::Sync)).unwrap();
    match DiskEngine::open(tmp.path(), opts(SyncMode::Sync)) {
        Err(StorageError::Locked(_)) => {}
        other => panic!("expected Locked, got {other:?}"),
    }
    drop(first);
    DiskEngine::open(tmp.path(), opts(SyncMode::Sync)).unwrap();

    // Same guarantee on the simulated file system.
    let vfs = SimVfs::new(1);
    let o = DiskOptions { vfs: Arc::new(vfs), ..Default::default() };
    let first = DiskEngine::open(std::path::Path::new("/x"), o.clone()).unwrap();
    assert!(matches!(DiskEngine::open(std::path::Path::new("/x"), o.clone()), Err(StorageError::Locked(_))));
    drop(first);
    DiskEngine::open(std::path::Path::new("/x"), o).unwrap();
}

#[test]
fn backup_is_a_consistent_openable_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let target = dest.path().join("copy");
    let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Async)).unwrap();
    let mut model = Model::new();
    let mut rng = Rng(8);
    for _ in 0..500 {
        let batch = gen_batch(&mut rng, 800, 10);
        apply(&mut model, &batch);
        engine.write(batch).unwrap();
    }
    engine.backup(&target).unwrap();
    // Later writes must not leak into the backup.
    put(&engine, "after-backup", "x");
    assert!(matches!(engine.backup(&target), Err(StorageError::Invalid(_))), "non-empty target must be refused");

    let copy = DiskEngine::open(&target, opts(SyncMode::Sync)).unwrap();
    assert_same(&copy, &model, "backup");
    assert_verify_clean(&copy, "backup");
    assert_eq!(engine.get(b"after-backup").unwrap().unwrap(), b"x");
}

#[test]
fn async_mode_flushes_in_the_background() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Async)).unwrap();
    let lsn = put(&engine, "a", "1");
    engine.sync(lsn).unwrap();
    let before = engine.stats().fsyncs;
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(engine.stats().fsyncs >= before, "stats must not go backwards");
    assert!(engine.stats().fsyncs > 0, "background flusher never synced the log");
    drop(engine);
    let engine = DiskEngine::open(tmp.path(), opts(SyncMode::Async)).unwrap();
    assert_eq!(engine.get(b"a").unwrap().unwrap(), b"1");
}

#[test]
fn group_commit_shares_fsyncs() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = Arc::new(DiskEngine::open(tmp.path(), opts(SyncMode::Group)).unwrap());
    let threads = 8;
    let per_thread = 50;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let engine = engine.clone();
            std::thread::spawn(move || {
                for i in 0..per_thread {
                    let lsn = put(&*engine, &format!("t{t}-{i}"), "v");
                    engine.sync(lsn).unwrap();
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let stats = engine.stats();
    assert_eq!(dump(&*engine).len(), threads * per_thread);
    assert!(
        stats.fsyncs < (threads * per_thread) as u64,
        "expected commits to share fsyncs, got {} fsyncs for {} commits",
        stats.fsyncs,
        threads * per_thread
    );
}

#[test]
fn concurrent_readers_writer_and_checkpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let o = DiskOptions {
        sync_mode: SyncMode::Async,
        cache_pages: 64,
        checkpoint_wal_bytes: 64 << 10,
        ..Default::default()
    };
    let engine = Arc::new(DiskEngine::open(tmp.path(), o).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    // Invariant maintained by the writer: every batch rewrites all N
    // counters to the same generation, so any snapshot a reader takes in a
    // single scan must show one generation only.
    const N: usize = 200;
    let generation = Arc::new(AtomicU64::new(0));
    let write_gen = |engine: &DiskEngine, g: u64| {
        let mut b = WriteBatch::new();
        for i in 0..N {
            b.put(format!("c{i:04}").into_bytes(), format!("{g:020}").into_bytes().repeat(1 + (g as usize + i) % 40));
        }
        engine.write(b).unwrap();
    };
    write_gen(&engine, 0);

    let mut handles = Vec::new();
    for r in 0..4 {
        let (engine, stop, generation) = (engine.clone(), stop.clone(), generation.clone());
        handles.push(std::thread::spawn(move || {
            let mut rng = Rng(r);
            let mut scans = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let floor = generation.load(Ordering::Acquire);
                let rows = dump(&*engine);
                assert_eq!(rows.len(), N);
                let g = &rows[0].1[..20];
                assert!(rows.iter().all(|(_, v)| &v[..20] == g), "scan saw a half-applied batch");
                let seen: u64 = std::str::from_utf8(g).unwrap().parse().unwrap();
                assert!(seen >= floor, "scan went back in time");
                let k = format!("c{:04}", rng.below(N as u64));
                assert!(engine.get(k.as_bytes()).unwrap().is_some());
                scans += 1;
            }
            scans
        }));
    }
    let ck = {
        let (engine, stop) = (engine.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                engine.checkpoint().unwrap();
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            0
        })
    };
    for g in 1..=300 {
        write_gen(&engine, g);
        generation.store(g, Ordering::Release);
    }
    stop.store(true, Ordering::Relaxed);
    let scans: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    ck.join().unwrap();
    assert!(scans > 0);
    assert_verify_clean(&*engine, "concurrent");
    assert!(engine.stats().checkpoints > 0);
}

#[test]
fn detects_corrupted_pages() {
    let vfs = SimVfs::new(3);
    let o = DiskOptions { cache_pages: 16, vfs: Arc::new(vfs.clone()), ..Default::default() };
    let dir = std::path::Path::new("/c");
    {
        let engine = DiskEngine::open(dir, o.clone()).unwrap();
        let mut rng = Rng(4);
        for _ in 0..300 {
            engine.write(gen_batch(&mut rng, 3000, 10)).unwrap();
        }
    }
    // Flip a bit in the middle of some page of the checkpointed data file.
    assert!(vfs.corrupt_byte(&dir.join("data.db"), 5 * 8192 + 1234));
    let engine = DiskEngine::open(dir, o).unwrap();
    let report = engine.verify().unwrap();
    assert!(!report.is_ok(), "verify missed a flipped bit");
    assert!(report.errors.iter().any(|e| e.contains("checksum mismatch on page 5")), "{:?}", report.errors);
    // Reading through the damaged page fails loudly instead of returning
    // wrong data.
    let err = engine.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, false, usize::MAX);
    assert!(matches!(err, Err(StorageError::Corruption(_))));
}

#[test]
fn engine_is_poisoned_after_io_error() {
    let vfs = SimVfs::new(11);
    let o = DiskOptions { vfs: Arc::new(vfs.clone()), ..Default::default() };
    let dir = std::path::Path::new("/p");
    let engine = DiskEngine::open(dir, o.clone()).unwrap();
    let lsn = put(&engine, "a", "1");
    engine.sync(lsn).unwrap();
    vfs.set_error_rate(1_000_000);
    let lsn = put(&engine, "b", "2");
    assert!(engine.sync(lsn).is_err());
    vfs.set_error_rate(0);
    // The failed fsync must not be retried silently.
    assert!(matches!(engine.sync(lsn), Err(StorageError::Poisoned(_))));
    let mut b = WriteBatch::new();
    b.put(b"c".to_vec(), b"3".to_vec());
    assert!(matches!(engine.write(b), Err(StorageError::Poisoned(_))));
    assert!(matches!(engine.checkpoint(), Err(StorageError::Poisoned(_))));
    // Reads still work, and a reopen recovers the durable prefix.
    assert_eq!(engine.get(b"a").unwrap().unwrap(), b"1");
    drop(engine);
    vfs.crash();
    let engine = DiskEngine::open(dir, o).unwrap();
    assert_eq!(engine.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(engine.get(b"c").unwrap(), None);
}
