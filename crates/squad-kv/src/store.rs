use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use squad_storage::{Engine, MemEngine, WriteBatch};

use crate::codec;
use crate::lock::LockManager;
use crate::txn::{Isolation, Txn};
use crate::Result;

/// Write set of one commit: `(commit timestamp, keys written)`.
pub(crate) type CommitRecord = (u64, Arc<Vec<Vec<u8>>>);

pub(crate) struct Inner {
    pub(crate) engine: Arc<dyn Engine>,
    /// Serializes validation, timestamp assignment and batch application.
    pub(crate) commit_lock: Mutex<()>,
    /// Newest commit timestamp visible to new snapshots.
    pub(crate) last_committed: AtomicU64,
    /// Snapshot timestamps in use, with reference counts.
    pub(crate) active: Mutex<BTreeMap<u64, usize>>,
    pub(crate) locks: LockManager,
    pub(crate) next_txn_id: AtomicU64,
    /// Number of live serializable transactions; commits only record their
    /// write sets in `recent` while this is non-zero.
    pub(crate) serializable_live: AtomicUsize,
    /// Write sets of recent commits, for serializable validation.
    pub(crate) recent: Mutex<VecDeque<CommitRecord>>,
}

impl Inner {
    pub(crate) fn register_snapshot(&self) -> u64 {
        let mut active = self.active.lock();
        let ts = self.last_committed.load(Ordering::SeqCst);
        *active.entry(ts).or_insert(0) += 1;
        ts
    }

    pub(crate) fn unregister_snapshot(&self, ts: u64) {
        let mut active = self.active.lock();
        if let Some(n) = active.get_mut(&ts) {
            *n -= 1;
            if *n == 0 {
                active.remove(&ts);
            }
        }
    }

    /// Oldest timestamp any current or future snapshot may read at.
    pub(crate) fn oldest_snapshot(&self) -> u64 {
        let active = self.active.lock();
        let last = self.last_committed.load(Ordering::SeqCst);
        active.keys().next().copied().unwrap_or(last).min(last)
    }
}

/// A multi-version transactional key-value store.
///
/// Cloning is cheap and yields a handle to the same store.
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Inner>,
}

impl Store {
    /// Wraps an engine. Reads the last commit timestamp persisted by an
    /// earlier run, if any.
    pub fn new(engine: Arc<dyn Engine>) -> Result<Store> {
        let last = match engine.get(codec::META_LAST_TS)? {
            Some(v) if v.len() == 8 => u64::from_be_bytes(v.try_into().unwrap()),
            _ => 0,
        };
        Ok(Store {
            inner: Arc::new(Inner {
                engine,
                commit_lock: Mutex::new(()),
                last_committed: AtomicU64::new(last),
                active: Mutex::new(BTreeMap::new()),
                locks: LockManager::default(),
                next_txn_id: AtomicU64::new(1),
                serializable_live: AtomicUsize::new(0),
                recent: Mutex::new(VecDeque::new()),
            }),
        })
    }

    /// A store on a fresh volatile [`MemEngine`].
    pub fn in_memory() -> Store {
        Store::new(Arc::new(MemEngine::new())).expect("memory engine cannot fail")
    }

    /// The underlying engine.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.inner.engine
    }

    /// Starts a transaction reading at the current state.
    pub fn begin(&self, isolation: Isolation) -> Txn {
        Txn::new(self.inner.clone(), isolation)
    }

    /// Removes every version and tombstone that no snapshot can see any
    /// more. Returns the number of engine entries removed.
    pub fn gc(&self) -> Result<u64> {
        let inner = &self.inner;
        let mut removed = 0u64;
        let mut lo: Bound<Vec<u8>> = Bound::Included(codec::DATA_LO.to_vec());
        // State carried across chunks for the user key being processed.
        let mut cur_start: Vec<u8> = Vec::new();
        let mut covered = false; // a version <= horizon was already seen
        loop {
            // Re-read the horizon per chunk under the commit lock so that the
            // deletes cannot race with a commit that makes them wrong.
            let _guard = inner.commit_lock.lock();
            let horizon = inner.oldest_snapshot();
            let lo_ref = match &lo {
                Bound::Included(k) => Bound::Included(k.as_slice()),
                Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let chunk = inner.engine.scan(lo_ref, Bound::Excluded(codec::DATA_HI), false, 1024)?;
            let done = chunk.len() < 1024;
            let mut batch = WriteBatch::new();
            let mut last = None;
            for (ek, ev) in chunk {
                if !codec::same_key(&ek, &cur_start) {
                    cur_start = ek[..ek.len() - 8].to_vec();
                    covered = false;
                }
                let ts = !u64::from_be_bytes(ek[ek.len() - 8..].try_into().unwrap());
                if covered {
                    batch.delete(ek.clone());
                } else if ts <= horizon {
                    covered = true;
                    // The newest version at or below the horizon is the one
                    // old snapshots read. A tombstone there hides nothing
                    // once everything older is gone, so drop it as well.
                    if ev.first() != Some(&1) {
                        batch.delete(ek.clone());
                    }
                }
                last = Some(ek);
            }
            removed += batch.len() as u64;
            if !batch.is_empty() {
                inner.engine.write(batch)?;
            }
            match (done, last) {
                (false, Some(k)) => lo = Bound::Excluded(k),
                _ => break,
            }
        }
        Ok(removed)
    }
}
