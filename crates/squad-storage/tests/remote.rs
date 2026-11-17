//! The object-store copy of a database: shipping, snapshots, restore,
//! garbage collection, fencing and fault tolerance.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use squad_object::emulator::S3Emulator;
use squad_object::{Fault, MemoryStore, ObjectStore, S3Store};
use squad_storage::{
    restore_from, DiskEngine, DiskOptions, Engine, RemoteConfig, RemoteStats, RemoteSync, SimVfs, SnapshotTrigger,
    StorageError, SyncMode, TieredEngine, WriteBatch,
};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) ^ (self.0 >> 12)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn options(vfs: &SimVfs) -> DiskOptions {
    DiskOptions {
        sync_mode: SyncMode::Sync,
        cache_pages: 64,
        checkpoint_wal_bytes: 64 << 10,
        vfs: Arc::new(vfs.clone()),
    }
}

fn config(store: &MemoryStore, mode: RemoteSync) -> RemoteConfig {
    RemoteConfig { snapshot_wal_bytes: 16 << 10, ..RemoteConfig::new(Arc::new(store.clone()), mode) }
}

fn random_batch(rng: &mut Rng, model: &mut Model) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for _ in 0..1 + rng.below(4) {
        let key = format!("key{:04}", rng.below(300)).into_bytes();
        if rng.below(5) == 0 {
            model.remove(&key);
            batch.delete(key);
        } else {
            let value = vec![rng.below(256) as u8; 1 + rng.below(600) as usize];
            model.insert(key.clone(), value.clone());
            batch.put(key, value);
        }
    }
    batch
}

fn contents(engine: &dyn Engine) -> Model {
    engine.scan(Bound::Unbounded, Bound::Unbounded, false, usize::MAX).unwrap().into_iter().collect()
}

fn restored(store: &MemoryStore) -> Model {
    let vfs = SimVfs::new(99);
    restore_from(store, Path::new("/restored"), options(&vfs)).unwrap();
    let engine = DiskEngine::open(Path::new("/restored"), options(&vfs)).unwrap();
    assert!(engine.verify().unwrap().is_ok());
    contents(&engine)
}

#[test]
fn sync_mode_restores_every_commit() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(1);
    let mut rng = Rng(1);
    let mut model = Model::new();
    {
        let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, RemoteSync::Sync)).unwrap();
        for _ in 0..400 {
            let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
            engine.sync(lsn).unwrap();
        }
        assert_eq!(engine.remote_stats().shipped_lsn, 400);
        // More log than `snapshot_wal_bytes` was shipped, so the
        // background thread uploads a snapshot.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while engine.remote_stats().snapshots == 0 {
            assert!(std::time::Instant::now() < deadline, "no background snapshot: {:?}", engine.remote_stats());
            std::thread::sleep(Duration::from_millis(20));
        }
        // The machine dies: no clean shutdown, local disk gone.
        vfs.kill();
        std::mem::forget(engine);
    }
    assert_eq!(restored(&store), model);

    // A new machine opens the database from the object store and goes on.
    let vfs2 = SimVfs::new(2);
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs2), config(&store, RemoteSync::Sync)).unwrap();
    assert_eq!(contents(&engine), model);
    assert_eq!(engine.remote_stats().epoch, 2);
    for _ in 0..50 {
        let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
        engine.sync(lsn).unwrap();
    }
    drop(engine);
    assert_eq!(restored(&store), model);
}

#[test]
fn async_mode_ships_in_the_background_and_on_flush() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(3);
    let mut rng = Rng(3);
    let mut model = Model::new();
    let mode = RemoteSync::Async { interval: Duration::from_millis(20) };
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, mode)).unwrap();
    for _ in 0..100 {
        let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
        engine.sync(lsn).unwrap();
    }
    // Within a few intervals everything has been shipped.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while engine.remote_stats().shipped_lsn < 100 {
        assert!(std::time::Instant::now() < deadline, "background shipping stalled");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(restored(&store), model);
    // An explicit flush makes the latest commits safe at once.
    let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
    engine.sync(lsn).unwrap();
    engine.flush_remote().unwrap();
    assert_eq!(restored(&store), model);
    // A clean shutdown ships what is left.
    let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
    engine.sync(lsn).unwrap();
    drop(engine);
    assert_eq!(restored(&store), model);
}

#[test]
fn reopening_the_same_directory_continues() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(4);
    let mut rng = Rng(4);
    let mut model = Model::new();
    for round in 0..4 {
        let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, RemoteSync::Sync)).unwrap();
        assert_eq!(contents(&engine), model, "round {round}");
        for _ in 0..60 {
            let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
            engine.sync(lsn).unwrap();
        }
        if round % 2 == 0 {
            engine.checkpoint().unwrap();
        }
        drop(engine);
        assert_eq!(restored(&store), model, "round {round}");
    }
}

#[test]
fn unshipped_local_commits_reach_the_store_on_reopen() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(5);
    let mut rng = Rng(5);
    let mut model = Model::new();
    let mode = RemoteSync::Async { interval: Duration::from_secs(3600) };
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, mode)).unwrap();
    for _ in 0..40 {
        let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
        engine.sync(lsn).unwrap();
    }
    // The process dies before anything was shipped; the disk survives.
    std::mem::forget(engine);
    vfs.crash();
    assert!(restored_or_empty(&store).len() < model.len().max(1));
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, mode)).unwrap();
    assert_eq!(contents(&engine), model);
    drop(engine);
    assert_eq!(restored(&store), model);
}

fn restored_or_empty(store: &MemoryStore) -> Model {
    let vfs = SimVfs::new(98);
    match restore_from(store, Path::new("/r"), options(&vfs)) {
        Ok(_) => contents(&DiskEngine::open(Path::new("/r"), options(&vfs)).unwrap()),
        Err(_) => Model::new(),
    }
}

#[test]
fn a_stale_directory_is_refused_unless_restore_is_set() {
    let store = MemoryStore::new();
    let (vfs_a, vfs_b) = (SimVfs::new(6), SimVfs::new(7));
    let mut rng = Rng(6);
    let mut model = Model::new();
    let write = |engine: &TieredEngine, rng: &mut Rng, model: &mut Model, n: usize| {
        for _ in 0..n {
            let lsn = engine.write(random_batch(rng, model)).unwrap();
            engine.sync(lsn).unwrap();
        }
    };
    let a = TieredEngine::open(Path::new("/db"), options(&vfs_a), config(&store, RemoteSync::Sync)).unwrap();
    write(&a, &mut rng, &mut model, 30);
    drop(a);
    // Another machine takes over and moves on.
    let b = TieredEngine::open(Path::new("/db"), options(&vfs_b), config(&store, RemoteSync::Sync)).unwrap();
    write(&b, &mut rng, &mut model, 30);
    drop(b);
    // The first machine's directory is now behind and of an old lineage.
    let err = TieredEngine::open(Path::new("/db"), options(&vfs_a), config(&store, RemoteSync::Sync)).err().unwrap();
    assert!(matches!(err, StorageError::Invalid(_)), "{err}");
    let again = RemoteConfig { restore: true, ..config(&store, RemoteSync::Sync) };
    let a = TieredEngine::open(Path::new("/db"), options(&vfs_a), again).unwrap();
    assert_eq!(contents(&a), model);
    assert!(a.verify().unwrap().is_ok());
}

/// A store that enforces conditional writes, and one that ignores them
/// as Garage does. Fencing must not depend on the difference.
fn both_kinds() -> [MemoryStore; 2] {
    let ignoring = MemoryStore::new();
    ignoring.set_ignore_conditions(true);
    [MemoryStore::new(), ignoring]
}

#[test]
fn a_superseded_writer_is_fenced() {
    for store in both_kinds() {
        superseded_writer_is_fenced(store);
    }
}

fn superseded_writer_is_fenced(store: MemoryStore) {
    let (vfs_a, vfs_b) = (SimVfs::new(8), SimVfs::new(9));
    let mut rng = Rng(8);
    let mut model = Model::new();
    let a = TieredEngine::open(Path::new("/db"), options(&vfs_a), config(&store, RemoteSync::Sync)).unwrap();
    for _ in 0..20 {
        let lsn = a.write(random_batch(&mut rng, &mut model)).unwrap();
        a.sync(lsn).unwrap();
    }
    // A second writer starts while the first is still alive.
    let b = TieredEngine::open(Path::new("/db"), options(&vfs_b), config(&store, RemoteSync::Sync)).unwrap();
    assert_eq!(contents(&b), model);

    // The first writer can no longer get a commit acknowledged...
    let mut ignored = model.clone();
    let lsn = a.write(random_batch(&mut rng, &mut ignored)).unwrap();
    let err = a.sync(lsn).unwrap_err();
    assert!(matches!(err, StorageError::Remote(_)), "{err}");
    // ...and refuses further writes.
    assert!(a.write(random_batch(&mut rng, &mut ignored)).is_err());
    assert!(a.checkpoint().is_err());

    for _ in 0..20 {
        let lsn = b.write(random_batch(&mut rng, &mut model)).unwrap();
        b.sync(lsn).unwrap();
    }
    b.checkpoint().unwrap();
    drop(a);
    drop(b);
    // Only the second writer's history is restored.
    assert_eq!(restored(&store), model);
}

#[test]
fn garbage_collection_keeps_what_restore_and_replicas_need() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(10);
    let mut rng = Rng(10);
    let mut model = Model::new();
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, RemoteSync::Sync)).unwrap();
    // LSN of the snapshot taken at the end of the previous round.
    let mut previous = 0;
    for round in 0..8 {
        for _ in 0..80 {
            let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
            engine.sync(lsn).unwrap();
        }
        engine.checkpoint().unwrap();
        let keys: Vec<String> = store.list("").unwrap().into_iter().map(|m| m.key).collect();
        assert_eq!(keys.iter().filter(|k| k.starts_with("snapshot/")).count(), 1, "round {round}: {keys:?}");
        // The log is kept back to the snapshot before the newest one, for
        // replicas that were reading it, and no further.
        let logs: Vec<u64> = keys
            .iter()
            .filter(|k| k.starts_with("wal/"))
            .map(|k| k.rsplit('-').next().unwrap().parse::<u64>().unwrap())
            .collect();
        assert!(logs.iter().all(|last| *last > previous), "round {round}: {keys:?}");
        assert!(logs.len() <= 80, "round {round}: {keys:?}");
        previous = engine.remote_stats().snapshot_lsn;
        // No chunk is stored that the manifest does not reference.
        let manifest = store.get(keys.iter().find(|k| k.starts_with("snapshot/")).unwrap()).unwrap().unwrap();
        let referenced = String::from_utf8(manifest).unwrap().lines().filter(|l| l.starts_with("chunk ")).count();
        let stored = keys.iter().filter(|k| k.starts_with("chunk/")).count();
        assert!(stored <= referenced, "round {round}: {stored} chunks stored, {referenced} referenced");
        assert_eq!(restored(&store), model, "round {round}");
    }
    let stats = engine.remote_stats();
    assert!(stats.chunks_reused > 0 || stats.chunks_uploaded < 8 * 4, "{stats:?}");
}

/// Random workloads against an object store that fails requests and
/// loses confirmations, with the machine dying at a random point. After
/// each death a new machine restores from the store alone.
#[test]
fn restore_after_random_faults() {
    let start = env("SQUAD_REMOTE_START", 0);
    let seeds = env("SQUAD_REMOTE_SEEDS", 60);
    for seed in start..start + seeds {
        let store = MemoryStore::new();
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) + 17);
        let sync = seed % 3 != 0;
        let mode = if sync { RemoteSync::Sync } else { RemoteSync::Async { interval: Duration::from_millis(1) } };
        // `model` follows every write; `acked` is the state as of the
        // last commit whose sync succeeded.
        let mut model;
        let mut history: Vec<Model> = vec![Model::new()];
        let mut acked = 0usize;
        for generation in 0..3u64 {
            let ctx = format!("seed {seed} generation {generation}");
            let vfs = SimVfs::new(seed * 10 + generation);
            let fault_rate = [0, 15, 40][rng.below(3) as usize];
            let counter = Arc::new(AtomicU64::new(seed * 7919 + generation));
            let hook_counter = counter.clone();
            store.set_faults(None);
            let engine = match TieredEngine::open(Path::new("/db"), options(&vfs), config(&store, mode)) {
                Ok(e) => e,
                Err(e) => panic!("{ctx}: open without faults failed: {e}"),
            };
            // What the new writer starts from becomes the truth.
            let base = contents(&engine);
            let position = history
                .iter()
                .rposition(|m| *m == base)
                .unwrap_or_else(|| panic!("{ctx}: restored state is not a state the database ever had"));
            assert!(position >= acked, "{ctx}: restore lost acknowledged commits ({position} < {acked})");
            history.truncate(position + 1);
            acked = position;
            model = base;
            store.set_faults(Some(Box::new(move |op, _| {
                let n =
                    hook_counter.fetch_add(0x9E37_79B9, Ordering::Relaxed).wrapping_mul(0x2545_F491_4F6C_DD1D) >> 40;
                if op != "put" && op != "delete" && op != "list" && op != "head" {
                    return Fault::None;
                }
                match n % 100 {
                    x if x < fault_rate / 2 => Fault::Fail,
                    x if x < fault_rate => Fault::FailAfterEffect,
                    _ => Fault::None,
                }
            })));
            let steps = 30 + rng.below(120);
            for _ in 0..steps {
                let batch = random_batch(&mut rng, &mut model);
                match engine.write(batch) {
                    Ok(lsn) => {
                        history.push(model.clone());
                        if engine.sync(lsn).is_ok() && sync {
                            acked = history.len() - 1;
                        }
                    }
                    Err(e) => panic!("{ctx}: local write failed: {e}"),
                }
                if rng.below(40) == 0 {
                    let _ = engine.checkpoint();
                }
            }
            if rng.below(2) == 0 {
                // Clean shutdown, possibly with a failing store.
                drop(engine);
            } else {
                vfs.kill();
                std::mem::forget(engine);
            }
            store.set_faults(None);
            let check_vfs = SimVfs::new(97);
            let got = match restore_from(&store, Path::new("/r"), options(&check_vfs)) {
                Ok(_) => contents(&DiskEngine::open(Path::new("/r"), options(&check_vfs)).unwrap()),
                Err(e) if acked == 0 => {
                    let _ = e;
                    Model::new()
                }
                Err(e) => panic!(
                    "{ctx}: restore failed: {e}\nobjects: {:?}",
                    store
                        .list("")
                        .unwrap()
                        .iter()
                        .map(|m| m.key.clone())
                        .filter(|k| !k.starts_with("chunk/"))
                        .collect::<Vec<_>>()
                ),
            };
            let position = history
                .iter()
                .rposition(|m| *m == got)
                .unwrap_or_else(|| panic!("{ctx}: restored state is not a state the database ever had"));
            assert!(position >= acked, "{ctx}: restore lost acknowledged commits ({position} < {acked})");
            let _ = counter;
        }
    }
}

#[test]
fn works_against_s3() {
    let emulator = S3Emulator::start();
    let store: Arc<dyn ObjectStore> = Arc::new(S3Store::new(emulator.config("prod/db1")).unwrap());
    let vfs = SimVfs::new(11);
    let mut rng = Rng(11);
    let mut model = Model::new();
    let cfg = RemoteConfig { snapshot_wal_bytes: 32 << 10, ..RemoteConfig::new(store.clone(), RemoteSync::Sync) };
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), cfg.clone()).unwrap();
    for i in 0..150 {
        if i % 40 == 7 {
            emulator.fail_next(2);
        }
        let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
        engine.sync(lsn).unwrap();
    }
    engine.checkpoint().unwrap();
    for _ in 0..30 {
        let lsn = engine.write(random_batch(&mut rng, &mut model)).unwrap();
        engine.sync(lsn).unwrap();
    }
    vfs.kill();
    std::mem::forget(engine);
    assert_eq!(emulator.rejected(), 0);

    let fresh = SimVfs::new(12);
    let engine = TieredEngine::open(Path::new("/db"), options(&fresh), cfg).unwrap();
    assert_eq!(contents(&engine), model);
    assert!(engine.verify().unwrap().is_ok());
    assert!(emulator.keys().iter().all(|k| k.starts_with("prod/db1/")));
}

/// Over S3, against the emulator in both modes: honouring and ignoring
/// `If-None-Match: *`.
#[test]
fn fencing_against_s3_with_and_without_conditional_writes() {
    for ignore in [false, true] {
        let emulator = S3Emulator::start();
        emulator.set_ignore_if_none_match(ignore);
        let store: Arc<dyn ObjectStore> = Arc::new(S3Store::new(emulator.config("fence")).unwrap());
        let cfg = RemoteConfig::new(store.clone(), RemoteSync::Sync);
        let (vfs_a, vfs_b) = (SimVfs::new(21), SimVfs::new(22));
        let mut rng = Rng(21);
        let mut model = Model::new();
        let a = TieredEngine::open(Path::new("/db"), options(&vfs_a), cfg.clone()).unwrap();
        assert_eq!(a.remote_stats().conditional_writes, Some(!ignore));
        for _ in 0..10 {
            let lsn = a.write(random_batch(&mut rng, &mut model)).unwrap();
            a.sync(lsn).unwrap();
        }
        let b = TieredEngine::open(Path::new("/db"), options(&vfs_b), cfg.clone()).unwrap();
        assert_eq!(contents(&b), model);
        let mut ignored = model.clone();
        let lsn = a.write(random_batch(&mut rng, &mut ignored)).unwrap();
        assert!(a.sync(lsn).is_err());
        for _ in 0..10 {
            let lsn = b.write(random_batch(&mut rng, &mut model)).unwrap();
            b.sync(lsn).unwrap();
        }
        drop(a);
        drop(b);
        let vfs = SimVfs::new(23);
        restore_from(&*store, Path::new("/r"), options(&vfs)).unwrap();
        assert_eq!(contents(&DiskEngine::open(Path::new("/r"), options(&vfs)).unwrap()), model);
        assert_eq!(emulator.rejected(), 0);
    }
}

/// One writer commits continuously while two more start at the same
/// instant and then commit continuously too. Each commit writes a key of
/// its own. Exactly one writer - the one with the highest epoch - can
/// still commit at the end; every other one was fenced; and every commit
/// any of them got acknowledged is in the store.
#[test]
fn concurrent_writers_one_wins_and_no_acknowledged_commit_is_lost() {
    for seed in 0..env("SQUAD_REMOTE_CONCURRENT_SEEDS", 6) {
        let store = &both_kinds()[seed as usize % 2];
        store.set_faults(Some(Box::new(|_, _| {
            // Requests take a little while, so that uploads, claims and
            // listings of different writers interleave.
            std::thread::sleep(Duration::from_micros(50));
            Fault::None
        })));
        let cfg = config(store, RemoteSync::Sync);
        let first = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(seed * 10)), cfg.clone()).unwrap();
        let first: Arc<dyn Engine> = Arc::new(first);
        let start = Arc::new(std::sync::Barrier::new(3));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Commits on `engine` until it is fenced or told to stop; returns
        // the acknowledged keys and whether it was fenced.
        let commit_loop = |engine: Arc<dyn Engine>, name: String, stop: Arc<std::sync::atomic::AtomicBool>| {
            std::thread::spawn(move || {
                let mut acked = Vec::new();
                let mut i = 0u64;
                loop {
                    if stop.load(Ordering::SeqCst) {
                        return (acked, false);
                    }
                    let key = format!("{name}-{i:05}").into_bytes();
                    let mut b = WriteBatch::new();
                    b.put(key.clone(), key.clone());
                    let result = engine.write(b).and_then(|lsn| engine.sync(lsn));
                    match result {
                        Ok(()) => acked.push(key),
                        Err(_) => return (acked, true),
                    }
                    i += 1;
                }
            })
        };
        let first_loop = commit_loop(first.clone(), "w0".into(), stop.clone());
        let openers: Vec<_> = (1..=2u64)
            .map(|n| {
                let (cfg, start, stop) = (cfg.clone(), start.clone(), stop.clone());
                std::thread::spawn(move || {
                    start.wait();
                    let vfs = SimVfs::new(seed * 10 + n);
                    let engine = match TieredEngine::open(Path::new("/db"), options(&vfs), cfg) {
                        Ok(e) => e,
                        // Superseded while opening: that is a fenced writer too.
                        Err(StorageError::Remote(_)) => return None,
                        Err(e) => panic!("open failed: {e}"),
                    };
                    let epoch = engine.remote_stats().epoch;
                    let engine: Arc<dyn Engine> = Arc::new(engine);
                    let handle = commit_loop(engine.clone(), format!("w{n}"), stop);
                    Some((epoch, engine, handle))
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(20));
        start.wait();
        let opened: Vec<_> = openers.into_iter().filter_map(|h| h.join().unwrap()).collect();
        assert!(!opened.is_empty(), "seed {seed}: no new writer could open");
        std::thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::SeqCst);

        let mut acked = Vec::new();
        let (keys, fenced) = first_loop.join().unwrap();
        assert!(fenced, "seed {seed}: the first writer was not fenced");
        acked.extend(keys);
        let top = opened.iter().map(|o| o.0).max().unwrap();
        let mut winner = None;
        for (epoch, engine, handle) in opened {
            let (keys, fenced) = handle.join().unwrap();
            acked.extend(keys);
            if epoch == top {
                assert!(!fenced, "seed {seed}: the newest writer (epoch {epoch}) was fenced");
                winner = Some(engine);
            } else {
                // It may have stopped before it noticed; it must not
                // commit any more now.
                let mut b = WriteBatch::new();
                b.put(b"late".to_vec(), b"late".to_vec());
                assert!(engine.write(b).and_then(|lsn| engine.sync(lsn)).is_err(), "seed {seed}: two writers");
            }
        }
        let winner = winner.unwrap();
        let mut b = WriteBatch::new();
        b.put(b"last".to_vec(), b"last".to_vec());
        winner.sync(winner.write(b).unwrap()).unwrap();
        store.set_faults(None);
        drop(first);
        drop(winner);

        let got = restored(store);
        for key in &acked {
            assert_eq!(
                got.get(key),
                Some(key),
                "seed {seed}: acknowledged commit {} lost",
                String::from_utf8_lossy(key)
            );
        }
        assert!(got.contains_key(&b"last"[..]));
        assert!(!got.contains_key(&b"late"[..]));
    }
}

/// Many committers in `Sync` mode on a slow store that fails requests:
/// a second upload starts while one is in flight, commits share uploads,
/// and every acknowledged commit is restored.
#[test]
fn concurrent_sync_commits_share_and_overlap_uploads() {
    for seed in 0..env("SQUAD_REMOTE_CONCURRENT_SEEDS", 6) {
        let store = MemoryStore::new();
        let in_flight = Arc::new(AtomicU64::new(0));
        let most = Arc::new(AtomicU64::new(0));
        let counter = Arc::new(AtomicU64::new(seed * 7919 + 1));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (f, m, c, x) = (in_flight.clone(), most.clone(), counter.clone(), fail.clone());
        store.set_faults(Some(Box::new(move |op, key| {
            if op == "put" && key.starts_with("wal/") {
                let now = f.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(3));
                f.fetch_sub(1, Ordering::SeqCst);
            }
            let n = c.fetch_add(0x9E37_79B9, Ordering::Relaxed).wrapping_mul(0x2545_F491_4F6C_DD1D) >> 40;
            match (x.load(Ordering::SeqCst), op, n % 20) {
                (true, "put" | "list", 0) => Fault::Fail,
                (true, "put" | "list", 1) => Fault::FailAfterEffect,
                _ => Fault::None,
            }
        })));
        let cfg = RemoteConfig { upload_concurrency: 2, ..config(&store, RemoteSync::Sync) };
        let engine = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(seed)), cfg).unwrap();
        fail.store(seed % 2 == 1, Ordering::SeqCst);
        let engine = Arc::new(engine);
        let threads: Vec<_> = (0..12u64)
            .map(|t| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let mut acked = Vec::new();
                    for i in 0..40u64 {
                        let key = format!("t{t:02}-{i:03}").into_bytes();
                        let mut b = WriteBatch::new();
                        b.put(key.clone(), key.clone());
                        let lsn = engine.write(b).unwrap();
                        if engine.sync(lsn).is_ok() {
                            acked.push(key);
                        }
                    }
                    acked
                })
            })
            .collect();
        let acked: Vec<Vec<u8>> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        let stats = engine.remote_stats();
        assert!(most.load(Ordering::SeqCst) >= 2, "seed {seed}: uploads never overlapped");
        assert!(stats.grouped_uploads > 0, "seed {seed}: no upload carried more than one commit: {stats:?}");
        assert!(stats.wal_objects < 480, "seed {seed}: {stats:?}");
        assert!(!acked.is_empty());
        // The machine dies; only the store is left.
        std::mem::forget(Arc::try_unwrap(engine).ok().unwrap());
        store.set_faults(None);
        let got = restored(&store);
        for key in &acked {
            assert_eq!(
                got.get(key),
                Some(key),
                "seed {seed}: acknowledged commit {} lost",
                String::from_utf8_lossy(key)
            );
        }
    }
}

/// A configuration whose snapshots happen only when asked for or when a
/// test turns one trigger on.
fn quiet(store: &MemoryStore) -> RemoteConfig {
    RemoteConfig {
        snapshot_wal_bytes: u64::MAX,
        snapshot_wal_objects: 0,
        snapshot_interval: None,
        ..RemoteConfig::new(Arc::new(store.clone()), RemoteSync::Sync)
    }
}

fn commit(engine: &TieredEngine, key: &str) {
    let mut b = WriteBatch::new();
    b.put(key.as_bytes().to_vec(), vec![7u8; 200]);
    let lsn = engine.write(b).unwrap();
    engine.sync(lsn).unwrap();
}

/// Waits until the engine has taken `n` snapshots.
fn wait_for_snapshots(engine: &TieredEngine, n: u64) -> RemoteStats {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let stats = engine.remote_stats();
        if stats.snapshots >= n {
            return stats;
        }
        assert!(std::time::Instant::now() < deadline, "no snapshot {n}: {stats:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The log after the newest snapshot is counted from the store when a
/// writer opens: a restart does not reset the measure, and a threshold
/// that the log in the store already exceeds starts a snapshot without
/// any new commit.
#[test]
fn the_log_since_the_newest_snapshot_counts_across_restarts() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(40);
    let (objects, bytes) = {
        let engine = TieredEngine::open(Path::new("/db"), options(&vfs), quiet(&store)).unwrap();
        for i in 0..5 {
            commit(&engine, &format!("a{i}"));
        }
        engine.snapshot_remote().unwrap();
        for i in 0..30 {
            commit(&engine, &format!("b{i}"));
        }
        let stats = engine.remote_stats();
        assert_eq!((stats.snapshots, stats.wal_objects_since_snapshot), (1, 30), "{stats:?}");
        (stats.wal_objects_since_snapshot, stats.wal_bytes_since_snapshot)
    };
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), quiet(&store)).unwrap();
    let stats = engine.remote_stats();
    assert_eq!((stats.wal_objects_since_snapshot, stats.wal_bytes_since_snapshot), (objects, bytes), "{stats:?}");
    assert_eq!((stats.snapshots, stats.snapshot_lsn), (0, 5));
    commit(&engine, "c");
    assert_eq!(engine.remote_stats().wal_objects_since_snapshot, 31);
    drop(engine);

    let config = RemoteConfig { snapshot_wal_objects: 31, ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), config).unwrap();
    let stats = wait_for_snapshots(&engine, 1);
    assert_eq!(stats.last_snapshot_trigger, Some(SnapshotTrigger::WalObjects), "{stats:?}");
    assert_eq!((stats.snapshot_lsn, stats.wal_objects_since_snapshot, stats.wal_bytes_since_snapshot), (36, 0, 0));
    assert!(stats.last_restore.is_none());
}

#[test]
fn a_snapshot_follows_enough_log_objects() {
    let store = MemoryStore::new();
    let config = RemoteConfig { snapshot_wal_objects: 20, ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(41)), config).unwrap();
    for i in 0..19 {
        commit(&engine, &format!("k{i}"));
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(engine.remote_stats().snapshots, 0);
    commit(&engine, "k19");
    let stats = wait_for_snapshots(&engine, 1);
    assert_eq!(stats.last_snapshot_trigger, Some(SnapshotTrigger::WalObjects), "{stats:?}");
    assert_eq!(stats.snapshot_lsn, 20);
}

#[test]
fn a_snapshot_follows_enough_log_bytes() {
    let store = MemoryStore::new();
    let config = RemoteConfig { snapshot_wal_bytes: 4 << 10, ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(42)), config).unwrap();
    for i in 0..30 {
        commit(&engine, &format!("k{i}"));
    }
    let stats = wait_for_snapshots(&engine, 1);
    assert_eq!(stats.last_snapshot_trigger, Some(SnapshotTrigger::WalBytes), "{stats:?}");
}

/// With `snapshot_interval`, a snapshot follows once that much time has
/// passed since the newest one, but only if there is new log.
#[test]
fn a_snapshot_follows_the_interval_if_there_is_new_log() {
    let store = MemoryStore::new();
    let config = RemoteConfig { snapshot_interval: Some(Duration::from_millis(300)), ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(43)), config).unwrap();
    commit(&engine, "a");
    engine.snapshot_remote().unwrap();
    let taken = engine.remote_stats().snapshots;
    assert_eq!(engine.remote_stats().last_snapshot_trigger, Some(SnapshotTrigger::Requested));
    // Nothing new: no snapshot however long the wait.
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(engine.remote_stats().snapshots, taken);
    commit(&engine, "b");
    let stats = wait_for_snapshots(&engine, taken + 1);
    assert_eq!(stats.last_snapshot_trigger, Some(SnapshotTrigger::Interval), "{stats:?}");
    assert_eq!(stats.snapshot_lsn, 2);
}

/// A writer that restored at open and had to replay more log than the
/// thresholds allow takes a snapshot in the background soon after, so
/// that the next restore is short.
#[test]
fn a_long_restore_is_followed_by_a_snapshot() {
    let store = MemoryStore::new();
    let vfs = SimVfs::new(44);
    let engine = TieredEngine::open(Path::new("/db"), options(&vfs), quiet(&store)).unwrap();
    for i in 0..5 {
        commit(&engine, &format!("a{i}"));
    }
    engine.snapshot_remote().unwrap();
    for i in 0..60 {
        commit(&engine, &format!("b{i}"));
    }
    let want = contents(&engine);
    // The machine dies.
    vfs.kill();
    std::mem::forget(engine);

    let config = RemoteConfig { snapshot_wal_objects: 50, restore_concurrency: 4, ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db"), options(&SimVfs::new(45)), config).unwrap();
    assert_eq!(contents(&engine), want);
    let restore = engine.remote_stats().last_restore.expect("restored at open");
    assert_eq!((restore.snapshot_lsn, restore.lsn, restore.wal_objects, restore.records), (5, 65, 60, 60));
    assert_eq!(restore.concurrency, 4);
    let stats = wait_for_snapshots(&engine, 1);
    assert_eq!(stats.last_snapshot_trigger, Some(SnapshotTrigger::Restore), "{stats:?}");
    assert_eq!(stats.snapshot_lsn, 65);
    drop(engine);
    assert_eq!(restored(&store), want);

    // A short restore is not followed by one.
    let config = RemoteConfig { snapshot_wal_objects: 50, ..quiet(&store) };
    let engine = TieredEngine::open(Path::new("/db2"), options(&SimVfs::new(46)), config).unwrap();
    assert_eq!(engine.remote_stats().last_restore.map(|r| r.wal_objects), Some(0));
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(engine.remote_stats().snapshots, 0);
}
