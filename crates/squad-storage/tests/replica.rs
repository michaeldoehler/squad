//! Read replicas at the storage level: following a writer through the
//! object store, falling behind, restarts, promotion and fencing.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use squad_object::{Fault, MemoryStore, ObjectStore};
use squad_storage::{
    DiskOptions, Engine, RemoteConfig, RemoteSync, ReplayHooks, ReplicaEngine, SimVfs, StorageError, SyncMode,
    TieredEngine, WriteBatch,
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

fn write(engine: &dyn Engine, rng: &mut Rng, model: &mut Model, n: usize) {
    for _ in 0..n {
        let lsn = engine.write(random_batch(rng, model)).unwrap();
        engine.sync(lsn).unwrap();
    }
}

fn contents(engine: &dyn Engine) -> Model {
    engine.scan(Bound::Unbounded, Bound::Unbounded, false, usize::MAX).unwrap().into_iter().collect()
}

fn writer(store: &MemoryStore, vfs: &SimVfs) -> TieredEngine {
    TieredEngine::open(Path::new("/db"), options(vfs), config(store, RemoteSync::Sync)).unwrap()
}

fn replica(store: &MemoryStore, vfs: &SimVfs) -> ReplicaEngine {
    ReplicaEngine::open(Path::new("/replica"), options(vfs), config(store, RemoteSync::Sync)).unwrap()
}

/// Polls until nothing more is applied.
fn catch_up(replica: &ReplicaEngine) {
    while replica.poll(&mut ()).unwrap() > 0 {}
}

#[test]
fn an_empty_store_cannot_be_followed() {
    let store = MemoryStore::new();
    let err = ReplicaEngine::open(Path::new("/replica"), options(&SimVfs::new(1)), config(&store, RemoteSync::Sync))
        .err()
        .unwrap();
    assert!(matches!(err, StorageError::Invalid(_)), "{err}");
}

#[test]
fn a_replica_starts_from_snapshot_and_log_and_follows() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r) = (SimVfs::new(1), SimVfs::new(2));
    let mut rng = Rng(1);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 60);
    w.checkpoint().unwrap();
    write(&w, &mut rng, &mut model, 25);

    let r = replica(&store, &vfs_r);
    catch_up(&r);
    assert_eq!(contents(&r), model);
    let status = r.status();
    assert!(!status.promoted);
    assert_eq!(status.applied_lsn, w.disk().last_lsn());
    assert_eq!(status.remote_lsn, status.applied_lsn);
    assert_eq!(status.rebases, 0);

    // Writes are refused, reads and local maintenance work.
    let mut batch = WriteBatch::new();
    batch.put(b"x".to_vec(), b"y".to_vec());
    assert!(matches!(r.write(batch), Err(StorageError::Invalid(_))));
    r.checkpoint().unwrap();
    assert!(r.verify().unwrap().is_ok());

    // It never writes to the object store.
    let puts = store.put_stats().0;
    for round in 0..10 {
        write(&w, &mut rng, &mut model, 7);
        assert_eq!(r.poll(&mut ()).unwrap(), 7, "round {round}");
        assert_eq!(contents(&r), model, "round {round}");
        assert_eq!(r.status().applied_lsn, w.disk().last_lsn());
    }
    // A poll that finds nothing new is a handful of requests, however
    // much log the store holds.
    let requests = store.requests();
    assert_eq!(r.poll(&mut ()).unwrap(), 0);
    let idle = store.requests() - requests;
    assert!(idle <= 6, "{idle} requests for an idle poll");
    let by_writer = w.remote_stats();
    assert!(by_writer.shipped_lsn >= r.status().applied_lsn);
    drop(r);
    let after = store.put_stats().0;
    // Everything put since came from the writer's 70 commits.
    assert!(after - puts <= 70 + 10, "{} puts for 70 commits", after - puts);
}

#[test]
fn a_replica_that_fell_behind_takes_a_newer_snapshot() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r) = (SimVfs::new(3), SimVfs::new(4));
    let mut rng = Rng(3);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 20);
    let r = replica(&store, &vfs_r);
    catch_up(&r);
    assert_eq!(contents(&r), model);

    // The writer moves on by several snapshots; the log the replica
    // would need next is deleted.
    for _ in 0..4 {
        write(&w, &mut rng, &mut model, 50);
        w.checkpoint().unwrap();
    }
    write(&w, &mut rng, &mut model, 5);

    #[derive(Default)]
    struct Seen {
        before: u32,
        after: u32,
    }
    impl ReplayHooks for Seen {
        fn before_rebase(&mut self) {
            self.before += 1;
        }
        fn after_rebase(&mut self) -> squad_storage::Result<()> {
            self.after += 1;
            Ok(())
        }
    }
    let mut seen = Seen::default();
    while r.poll(&mut seen).unwrap() > 0 {}
    assert_eq!((seen.before, seen.after), (1, 1));
    assert_eq!(r.status().rebases, 1);
    assert_eq!(contents(&r), model);
    assert_eq!(r.status().applied_lsn, w.disk().last_lsn());
    assert!(r.verify().unwrap().is_ok());

    // And follows normally from there.
    write(&w, &mut rng, &mut model, 9);
    catch_up(&r);
    assert_eq!(contents(&r), model);
    assert_eq!(r.status().rebases, 1);
}

#[test]
fn a_restarted_replica_resumes_from_its_own_copy() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r) = (SimVfs::new(5), SimVfs::new(6));
    let mut rng = Rng(5);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 40);
    let r = replica(&store, &vfs_r);
    catch_up(&r);
    let stood_at = r.status().applied_lsn;
    drop(r);

    write(&w, &mut rng, &mut model, 15);
    let gets = Arc::new(AtomicU64::new(0));
    let counter = gets.clone();
    store.set_faults(Some(Box::new(move |op, key| {
        if op == "get" && key.starts_with("chunk/") {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        Fault::None
    })));
    let r = replica(&store, &vfs_r);
    assert_eq!(r.status().applied_lsn, stood_at);
    catch_up(&r);
    assert_eq!(contents(&r), model);
    // The local copy was reused, not downloaded again.
    assert_eq!(gets.load(Ordering::Relaxed), 0);
    store.set_faults(None);

    // A crash of the replica (unsynced data torn) is survived as well.
    write(&w, &mut rng, &mut model, 15);
    assert!(r.poll(&mut ()).unwrap() > 0);
    drop(r);
    vfs_r.crash();
    write(&w, &mut rng, &mut model, 5);
    let r = replica(&store, &vfs_r);
    catch_up(&r);
    assert_eq!(contents(&r), model);
    assert!(r.verify().unwrap().is_ok());
}

#[test]
fn a_writer_directory_is_not_overwritten_by_a_replica() {
    let store = MemoryStore::new();
    let other = MemoryStore::new();
    let (vfs_a, vfs_b) = (SimVfs::new(7), SimVfs::new(8));
    let mut rng = Rng(7);
    let mut model = Model::new();
    let a = writer(&store, &vfs_a);
    write(&a, &mut rng, &mut model, 10);
    drop(a);
    // A directory with a different database.
    let b = TieredEngine::open(Path::new("/replica"), options(&vfs_b), config(&other, RemoteSync::Sync)).unwrap();
    let mut unrelated = Model::new();
    write(&b, &mut rng, &mut unrelated, 30);
    drop(b);
    let err =
        ReplicaEngine::open(Path::new("/replica"), options(&vfs_b), config(&store, RemoteSync::Sync)).err().unwrap();
    assert!(matches!(err, StorageError::Invalid(_)), "{err}");
}

#[test]
fn promotion_fences_the_writer_and_keeps_every_acknowledged_commit() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r, vfs_s) = (SimVfs::new(9), SimVfs::new(10), SimVfs::new(11));
    let mut rng = Rng(9);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 40);
    let r = replica(&store, &vfs_r);
    let second = ReplicaEngine::open(Path::new("/second"), options(&vfs_s), config(&store, RemoteSync::Sync)).unwrap();
    catch_up(&r);
    catch_up(&second);
    // Commits the replica has not polled yet are acknowledged too.
    write(&w, &mut rng, &mut model, 12);

    r.promote(&mut ()).unwrap();
    assert!(r.is_promoted());
    assert!(r.status().promoted);
    assert_eq!(contents(&r), model);
    // Promoting twice is harmless.
    r.promote(&mut ()).unwrap();

    // The old writer cannot get anything acknowledged any more.
    let mut ignored = model.clone();
    let lsn = w.write(random_batch(&mut rng, &mut ignored)).unwrap();
    let err = w.sync(lsn).unwrap_err();
    assert!(matches!(err, StorageError::Remote(_)), "{err}");
    assert!(w.write(random_batch(&mut rng, &mut ignored)).is_err());
    drop(w);

    // The promoted node writes and ships.
    write(&r, &mut rng, &mut model, 30);
    assert_eq!(contents(&r), model);
    assert!(r.remote_stats().unwrap().shipped_lsn >= r.status().applied_lsn);
    assert_eq!(r.poll(&mut ()).unwrap(), 0);

    // The other replica follows the new writer across the takeover.
    catch_up(&second);
    assert_eq!(contents(&second), model);
    assert!(second.status().epoch > 1);
    r.checkpoint().unwrap();
    write(&r, &mut rng, &mut model, 10);
    catch_up(&second);
    assert_eq!(contents(&second), model);

    // A fresh replica and a restarted promoted node agree.
    let fresh =
        ReplicaEngine::open(Path::new("/fresh"), options(&SimVfs::new(12)), config(&store, RemoteSync::Sync)).unwrap();
    catch_up(&fresh);
    assert_eq!(contents(&fresh), model);
    drop(r);
    let again = TieredEngine::open(Path::new("/replica"), options(&vfs_r), config(&store, RemoteSync::Sync)).unwrap();
    assert_eq!(contents(&again), model);
    assert!(again.verify().unwrap().is_ok());
}

#[test]
fn a_replica_does_not_apply_what_a_fenced_writer_ships_late() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r, vfs_s) = (SimVfs::new(13), SimVfs::new(14), SimVfs::new(15));
    let mut rng = Rng(13);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 20);
    let r = replica(&store, &vfs_r);
    let second = ReplicaEngine::open(Path::new("/second"), options(&vfs_s), config(&store, RemoteSync::Sync)).unwrap();
    catch_up(&second);
    r.promote(&mut ()).unwrap();
    // The fenced writer keeps trying; nothing of it may count.
    let mut ignored = model.clone();
    for _ in 0..5 {
        if let Ok(lsn) = w.write(random_batch(&mut rng, &mut ignored)) {
            let _ = w.sync(lsn);
        }
    }
    write(&r, &mut rng, &mut model, 20);
    catch_up(&second);
    assert_eq!(contents(&second), model);
}

/// A writer and a replica run side by side on an object store that
/// fails requests; the replica dies and restarts at random points. Polls
/// may fail, but whatever the replica holds is a state the writer went
/// through, and once the faults stop it reaches the writer's state.
#[test]
fn replicas_under_object_store_faults() {
    for seed in 0..env("SQUAD_REPLICA_SEEDS", 25) {
        let store = MemoryStore::new();
        let (vfs_w, vfs_r) = (SimVfs::new(seed), SimVfs::new(seed + 1000));
        let mut rng = Rng(seed + 77);
        let mut model = Model::new();
        // States by LSN, to check that the replica holds one of them.
        let mut history: BTreeMap<u64, Model> = BTreeMap::new();
        let w = writer(&store, &vfs_w);
        write(&w, &mut rng, &mut model, 5);
        history.insert(w.disk().last_lsn(), model.clone());
        let mut r = replica(&store, &vfs_r);

        let fault_seed = Arc::new(AtomicU64::new(seed * 31 + 1));
        for step in 0..120 {
            // Faults only hit the replica's reads; the writer has its own
            // fault test.
            store.set_faults(None);
            let lsn = w.write(random_batch(&mut rng, &mut model)).unwrap();
            w.sync(lsn).unwrap();
            history.insert(lsn, model.clone());
            if rng.below(40) == 0 {
                w.checkpoint().unwrap();
            }
            let state = fault_seed.clone();
            store.set_faults(Some(Box::new(move |op, _| {
                let x = state.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
                if matches!(op, "get" | "list" | "head") && (x >> 20) % 6 == 0 {
                    Fault::Fail
                } else {
                    Fault::None
                }
            })));
            match rng.below(10) {
                0 => {
                    drop(r);
                    if rng.below(2) == 0 {
                        vfs_r.crash();
                    }
                    store.set_faults(None);
                    r = replica(&store, &vfs_r);
                }
                1..=5 => {
                    let _ = r.poll(&mut ());
                }
                _ => {}
            }
            store.set_faults(None);
            let at = r.status().applied_lsn;
            match r.scan(Bound::Unbounded, Bound::Unbounded, false, usize::MAX) {
                Ok(rows) => {
                    let got: Model = rows.into_iter().collect();
                    // The position may have moved between the two calls
                    // only if a poll ran, and none does here.
                    assert_eq!(Some(&got), history.get(&at), "seed {seed} step {step}: state at LSN {at}");
                }
                // A swap that failed half way: the next poll repairs it.
                Err(StorageError::Remote(_)) => {}
                Err(e) => panic!("seed {seed} step {step}: {e}"),
            }
        }
        store.set_faults(None);
        catch_up(&r);
        assert_eq!(contents(&r), model, "seed {seed}");
        assert!(r.verify().unwrap().is_ok(), "seed {seed}");
    }
}

/// Promotion at a random point of a random workload, with a second
/// replica following through it.
#[test]
fn random_promotions() {
    for seed in 0..env("SQUAD_REPLICA_SEEDS", 25) {
        let store: MemoryStore = MemoryStore::new();
        let mut rng = Rng(seed + 500);
        let mut model = Model::new();
        let mut node = 0u64;
        let mut current: Box<dyn Engine> = Box::new(writer(&store, &SimVfs::new(seed * 100)));
        write(&*current, &mut rng, &mut model, 10);
        let observer = ReplicaEngine::open(Path::new("/observer"), options(&SimVfs::new(seed * 100 + 99)), {
            config(&store, RemoteSync::Sync)
        })
        .unwrap();
        for _ in 0..6 {
            node += 1;
            let vfs = SimVfs::new(seed * 100 + node);
            let next =
                ReplicaEngine::open(Path::new("/node"), options(&vfs), config(&store, RemoteSync::Sync)).unwrap();
            for _ in 0..rng.below(30) {
                let lsn = current.write(random_batch(&mut rng, &mut model)).unwrap();
                current.sync(lsn).unwrap();
                if rng.below(4) == 0 {
                    let _ = next.poll(&mut ());
                }
                if rng.below(6) == 0 {
                    let _ = observer.poll(&mut ());
                }
                if rng.below(25) == 0 {
                    current.checkpoint().unwrap();
                }
            }
            next.promote(&mut ()).unwrap();
            assert_eq!(contents(&next), model, "seed {seed} node {node}");
            // The replaced writer is fenced.
            let mut ignored = model.clone();
            let refused = match current.write(random_batch(&mut rng, &mut ignored)) {
                Ok(lsn) => current.sync(lsn).is_err(),
                Err(_) => true,
            };
            assert!(refused, "seed {seed} node {node}: the old writer still commits");
            current = Box::new(next);
            write(&*current, &mut rng, &mut model, 3);
        }
        catch_up(&observer);
        assert_eq!(contents(&observer), model, "seed {seed}");
        assert!(store.list("writer/").unwrap().len() >= 7);
    }
}

/// Starting a stale copy as a writer by mistake is refused without
/// claiming the database: the running writer and its replicas go on.
#[test]
fn a_refused_stale_writer_disturbs_nobody() {
    let store = MemoryStore::new();
    let (vfs_w, vfs_r, vfs_s) = (SimVfs::new(20), SimVfs::new(21), SimVfs::new(22));
    let mut rng = Rng(20);
    let mut model = Model::new();
    let w = writer(&store, &vfs_w);
    write(&w, &mut rng, &mut model, 20);
    let r = replica(&store, &vfs_r);
    let second = ReplicaEngine::open(Path::new("/second"), options(&vfs_s), config(&store, RemoteSync::Sync)).unwrap();
    r.promote(&mut ()).unwrap();
    drop(w);
    write(&r, &mut rng, &mut model, 10);

    let claims = store.list("writer/claim-").unwrap().len();
    for _ in 0..3 {
        let err =
            TieredEngine::open(Path::new("/db"), options(&vfs_w), config(&store, RemoteSync::Sync)).err().unwrap();
        assert!(matches!(err, StorageError::Invalid(_)), "{err}");
    }
    assert_eq!(store.list("writer/claim-").unwrap().len(), claims);
    // The writer is not fenced and the replica is not held back.
    write(&r, &mut rng, &mut model, 10);
    catch_up(&second);
    assert!(!second.status().waiting_for_takeover);
    assert_eq!(contents(&second), model);

    // The stale directory can rejoin as a replica: it had shipped
    // everything it wrote, so it is an earlier state of the same history.
    let rejoined = ReplicaEngine::open(Path::new("/db"), options(&vfs_w), config(&store, RemoteSync::Sync)).unwrap();
    catch_up(&rejoined);
    assert_eq!(contents(&rejoined), model);
}
