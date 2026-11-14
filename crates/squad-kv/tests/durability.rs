//! Commits become visible to other transactions only once the engine has
//! made them durable, and concurrent commits still share one wait.

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use squad_kv::{Isolation, Store};
use squad_storage::{Engine, EngineStats, MemEngine, Result, StorageError, VerifyReport, WriteBatch};

/// A memory engine whose `sync` waits until the test opens a gate, and
/// can be made to fail.
#[derive(Default)]
struct Gated {
    inner: MemEngine,
    /// Highest LSN the test has declared durable.
    durable: Mutex<u64>,
    moved: Condvar,
    fail: AtomicBool,
    /// Callers currently waiting in `sync`.
    waiting: Mutex<usize>,
}

impl Gated {
    fn open_through(&self, lsn: u64) {
        *self.durable.lock() = lsn;
        self.moved.notify_all();
    }

    fn wait_for_waiters(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while *self.waiting.lock() < n {
            assert!(Instant::now() < deadline, "no commit reached the durability wait");
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Engine for Gated {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan(lo, hi, reverse, limit)
    }
    fn write(&self, batch: WriteBatch) -> Result<u64> {
        self.inner.write(batch)
    }
    fn sync(&self, lsn: u64) -> Result<()> {
        *self.waiting.lock() += 1;
        let mut durable = self.durable.lock();
        while *durable < lsn && !self.fail.load(Ordering::SeqCst) {
            self.moved.wait(&mut durable);
        }
        *self.waiting.lock() -= 1;
        if *durable < lsn {
            return Err(StorageError::Remote("injected: the commit could not be made durable".into()));
        }
        Ok(())
    }
    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
    fn backup(&self, dest: &Path) -> Result<()> {
        self.inner.backup(dest)
    }
    fn verify(&self) -> Result<VerifyReport> {
        self.inner.verify()
    }
    fn stats(&self) -> EngineStats {
        self.inner.stats()
    }
}

fn read(store: &Store, isolation: Isolation, key: &[u8]) -> Option<Vec<u8>> {
    store.begin(isolation).get(key).unwrap()
}

#[test]
fn a_commit_is_invisible_until_durable() {
    let engine = Arc::new(Gated::default());
    let store = Store::new(engine.clone()).unwrap();
    // A Read Committed transaction that is already running.
    let mut running = store.begin(Isolation::ReadCommitted);

    let committer = {
        let store = store.clone();
        thread::spawn(move || {
            let mut t = store.begin(Isolation::ReadCommitted);
            t.put(b"k", b"v1").unwrap();
            t.commit().unwrap();
            // Once acknowledged, the committing session sees its commit.
            assert_eq!(read(&store, Isolation::ReadCommitted, b"k").as_deref(), Some(&b"v1"[..]));
        })
    };
    engine.wait_for_waiters(1);
    // The batch is in the engine, but not durable: nobody sees it.
    for isolation in [Isolation::ReadCommitted, Isolation::RepeatableRead, Isolation::Serializable] {
        assert_eq!(read(&store, isolation, b"k"), None, "{isolation:?}");
    }
    running.refresh_snapshot();
    assert_eq!(running.get(b"k").unwrap(), None);
    assert_eq!(store.last_committed(), 0);

    engine.open_through(u64::MAX);
    committer.join().unwrap();
    assert_eq!(read(&store, Isolation::RepeatableRead, b"k").as_deref(), Some(&b"v1"[..]));
    running.refresh_snapshot();
    assert_eq!(running.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
}

#[test]
fn concurrent_commits_do_not_wait_for_each_other_to_become_durable() {
    let engine = Arc::new(Gated::default());
    let store = Store::new(engine.clone()).unwrap();
    let committers: Vec<_> = (0..4u8)
        .map(|i| {
            let store = store.clone();
            thread::spawn(move || {
                let mut t = store.begin(Isolation::ReadCommitted);
                t.put(&[b'k', i], b"v").unwrap();
                t.commit().unwrap();
            })
        })
        .collect();
    // All four reach the durability wait at the same time: none holds
    // the commit lock while it waits.
    engine.wait_for_waiters(4);
    assert!(store
        .begin(Isolation::ReadCommitted)
        .scan(Bound::Unbounded, Bound::Unbounded, false, 10)
        .unwrap()
        .is_empty());
    // One durability point for all of them, as a group commit gives.
    engine.open_through(u64::MAX);
    for c in committers {
        c.join().unwrap();
    }
    let rows = store.begin(Isolation::ReadCommitted).scan(Bound::Unbounded, Bound::Unbounded, false, 10).unwrap();
    assert_eq!(rows.len(), 4);
}

#[test]
fn a_commit_whose_durability_failed_stays_invisible() {
    let engine = Arc::new(Gated::default());
    let store = Store::new(engine.clone()).unwrap();
    engine.fail.store(true, Ordering::SeqCst);
    let mut t = store.begin(Isolation::ReadCommitted);
    t.put(b"k", b"lost?").unwrap();
    assert!(t.commit().is_err());
    assert_eq!(read(&store, Isolation::ReadCommitted, b"k"), None);
    // A concurrent writer of the same key sees a conflict, not a lost
    // update: the outcome of the failed commit is unknown.
    engine.fail.store(false, Ordering::SeqCst);
    engine.open_through(u64::MAX);
    let mut u = store.begin(Isolation::RepeatableRead);
    u.put(b"k", b"other").unwrap();
    assert!(u.commit().is_err());
    // A later durable commit covers the earlier log record as well.
    let mut v = store.begin(Isolation::ReadCommitted);
    v.put(b"j", b"x").unwrap();
    v.commit().unwrap();
    assert_eq!(read(&store, Isolation::ReadCommitted, b"k").as_deref(), Some(&b"lost?"[..]));
}
