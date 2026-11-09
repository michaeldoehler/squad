//! Model-based tests: the disk engine must behave exactly like a BTreeMap.

mod common;

use std::sync::Arc;

use common::*;
use squad_storage::{
    DiskEngine, DiskOptions, Engine, MemEngine, SimVfs, StorageError, SyncMode, WriteBatch, MAX_KEY_LEN,
};

fn run_model(seed: u64, steps: u64, space: u64, cache_pages: usize) {
    let vfs = SimVfs::new(seed);
    let opts =
        DiskOptions { sync_mode: SyncMode::Sync, cache_pages, checkpoint_wal_bytes: 256 << 10, vfs: Arc::new(vfs) };
    let dir = std::path::Path::new("/db");
    let mut engine = DiskEngine::open(dir, opts.clone()).unwrap();
    let mut model = Model::new();
    let mut rng = Rng(seed);
    for step in 0..steps {
        let batch = gen_batch(&mut rng, space, 12);
        apply(&mut model, &batch);
        engine.write(batch).unwrap();
        for _ in 0..3 {
            check_reads(&mut rng, &engine, &model, space);
        }
        match rng.below(400) {
            0 => engine.checkpoint().unwrap(),
            1 => {
                // Clean close and reopen.
                drop(engine);
                engine = DiskEngine::open(dir, opts.clone()).unwrap();
                assert_same(&engine, &model, &format!("seed {seed} step {step} after reopen"));
            }
            2 => assert_verify_clean(&engine, &format!("seed {seed} step {step}")),
            _ => {}
        }
    }
    assert_same(&engine, &model, &format!("seed {seed} final"));
    assert_verify_clean(&engine, &format!("seed {seed} final"));
    let report = engine.verify().unwrap();
    assert_eq!(report.entries, model.len() as u64);

    // Delete everything: the tree must shrink back and stay consistent.
    let keys: Vec<_> = model.keys().cloned().collect();
    for chunk in keys.chunks(50) {
        let mut b = WriteBatch::new();
        chunk.iter().for_each(|k| b.delete(k.clone()));
        engine.write(b).unwrap();
    }
    assert!(dump(&engine).is_empty());
    assert_verify_clean(&engine, &format!("seed {seed} after deleting all"));
    let pages_before = engine.stats().page_count;
    // Freed pages are reused: refilling must not grow the file much.
    let mut model = Model::new();
    for _ in 0..steps / 2 {
        let batch = gen_batch(&mut rng, space, 12);
        apply(&mut model, &batch);
        engine.write(batch).unwrap();
    }
    assert_same(&engine, &model, &format!("seed {seed} refill"));
    assert_verify_clean(&engine, &format!("seed {seed} refill"));
    assert!(engine.stats().page_count <= pages_before + pages_before / 4 + 16, "free pages were not reused");
}

#[test]
fn random_ops_small_keyspace() {
    for seed in 0..3 {
        run_model(seed, 1000, 300, 64);
    }
}

#[test]
fn random_ops_large_keyspace_tiny_cache() {
    for seed in 100..102 {
        run_model(seed, 1500, 20_000, 32);
    }
}

#[test]
fn sequential_and_reverse_inserts() {
    let vfs = SimVfs::new(1);
    let opts = DiskOptions { cache_pages: 128, vfs: Arc::new(vfs), ..Default::default() };
    let engine = DiskEngine::open(std::path::Path::new("/seq"), opts).unwrap();
    let mut model = Model::new();
    let mut b = WriteBatch::new();
    for i in 0..30_000u32 {
        let k = if i % 2 == 0 { format!("a{i:08}") } else { format!("z{:08}", u32::MAX - i) };
        b.put(k.clone().into_bytes(), i.to_le_bytes().to_vec());
        model.insert(k.into_bytes(), i.to_le_bytes().to_vec());
        if b.len() == 500 {
            engine.write(std::mem::take(&mut b)).unwrap();
        }
    }
    engine.write(b).unwrap();
    assert_same(&engine, &model, "sequential");
    assert_verify_clean(&engine, "sequential");
    let mut rng = Rng(9);
    for _ in 0..500 {
        check_reads(&mut rng, &engine, &model, 1000);
    }
}

#[test]
fn rejects_oversized_keys_without_side_effects() {
    let vfs = SimVfs::new(2);
    let opts = DiskOptions { vfs: Arc::new(vfs), ..Default::default() };
    let engine = DiskEngine::open(std::path::Path::new("/big"), opts).unwrap();
    let mut b = WriteBatch::new();
    b.put(b"ok".to_vec(), b"1".to_vec());
    b.put(vec![1u8; MAX_KEY_LEN + 1], b"2".to_vec());
    assert!(matches!(engine.write(b), Err(StorageError::KeyTooLarge(_))));
    assert_eq!(engine.get(b"ok").unwrap(), None);
    // The engine stays usable.
    let mut b = WriteBatch::new();
    b.put(vec![1u8; MAX_KEY_LEN], vec![3u8; 100_000]);
    b.put(Vec::new(), b"empty key".to_vec());
    let lsn = engine.write(b).unwrap();
    engine.sync(lsn).unwrap();
    assert_eq!(engine.get(&vec![1u8; MAX_KEY_LEN]).unwrap().unwrap().len(), 100_000);
    assert_eq!(engine.get(b"").unwrap().unwrap(), b"empty key");
    assert_verify_clean(&engine, "oversized");
}

#[test]
fn mem_engine_matches_model() {
    let engine = MemEngine::new();
    let mut model = Model::new();
    let mut rng = Rng(77);
    for _ in 0..2000 {
        let batch = gen_batch(&mut rng, 200, 8);
        apply(&mut model, &batch);
        engine.write(batch).unwrap();
        check_reads(&mut rng, &engine, &model, 200);
    }
    assert_same(&engine, &model, "mem");
}
