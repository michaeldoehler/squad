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

/// Number of snapshots that can be registered without taking a lock.
const SNAPSHOT_SLOTS: usize = 128;

/// One registered snapshot: `0` when free, otherwise its timestamp plus
/// one. Padded so that transactions on different cores do not share a
/// cache line.
#[repr(align(64))]
pub(crate) struct Slot(AtomicU64);

/// Registry of the snapshots in use, which bound what garbage collection
/// may remove. Registration is lock-free in the common case, because
/// every statement of every session passes through it.
pub(crate) struct Snapshots {
    slots: Box<[Slot]>,
    /// Timestamps with reference counts, used when all slots are taken.
    overflow: Mutex<BTreeMap<u64, usize>>,
}

impl Snapshots {
    fn new() -> Snapshots {
        Snapshots {
            slots: (0..SNAPSHOT_SLOTS).map(|_| Slot(AtomicU64::new(0))).collect(),
            overflow: Mutex::new(BTreeMap::new()),
        }
    }
}

/// Handle of a registered snapshot.
pub(crate) enum SnapshotSlot {
    Fast(usize),
    Overflow(u64),
}

fn release_overflow(overflow: &mut BTreeMap<u64, usize>, ts: u64) {
    if let Some(n) = overflow.get_mut(&ts) {
        *n -= 1;
        if *n == 0 {
            overflow.remove(&ts);
        }
    }
}

pub(crate) struct Inner {
    pub(crate) engine: Arc<dyn Engine>,
    /// Serializes validation, timestamp assignment and batch application.
    pub(crate) commit_lock: Mutex<()>,
    /// Newest commit timestamp visible to new snapshots.
    pub(crate) last_committed: AtomicU64,
    /// Snapshot timestamps in use, with reference counts.
        pub(crate) snapshots: Snapshots,
    pub(crate) locks: LockManager,
    pub(crate) next_txn_id: AtomicU64,
    /// Number of live serializable transactions; commits only record their
    /// write sets in `recent` while this is non-zero.
    pub(crate) serializable_live: AtomicUsize,
    /// Write sets of recent commits, for serializable validation.
    pub(crate) recent: Mutex<VecDeque<CommitRecord>>,
}

impl Inner {
        /// Takes a snapshot at the latest commit and registers it so that the
    /// versions it can see are kept. `hint` spreads transactions over the
    /// registry. Returns the timestamp and the registration to release.
    pub(crate) fn register_snapshot(&self, hint: u64) -> (u64, SnapshotSlot) {
        let n = self.snapshots.slots.len();
        for probe in 0..n {
            let i = (hint as usize).wrapping_add(probe) % n;
            let ts = self.last_committed.load(Ordering::SeqCst);
            let slot = &self.snapshots.slots[i].0;
            if slot.compare_exchange(0, ts + 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return (self.settle(slot, ts), SnapshotSlot::Fast(i));
            }
        }
        // More concurrent transactions than slots.
        let mut overflow = self.snapshots.overflow.lock();
        let ts = self.last_committed.load(Ordering::SeqCst);
        *overflow.entry(ts).or_insert(0) += 1;
        (ts, SnapshotSlot::Overflow(ts))
    }

    /// Makes sure the timestamp published in `slot` is the latest commit.
    ///
    /// A garbage collector computes its horizon either before the slot was
    /// published — then from a `last_committed` that is not newer than the
    /// value confirmed here — or after, in which case it sees the slot.
    /// Either way it keeps everything this snapshot reads.
    fn settle(&self, slot: &AtomicU64, mut ts: u64) -> u64 {
        loop {
            let now = self.last_committed.load(Ordering::SeqCst);
            if now == ts {
                return ts;
            }
            ts = now;
            slot.store(ts + 1, Ordering::SeqCst);
        }
    }

    /// Moves a registered snapshot forward to the latest commit.
    pub(crate) fn advance_snapshot(&self, slot: &mut SnapshotSlot) -> u64 {
        match slot {
            SnapshotSlot::Fast(i) => {
                let slot = &self.snapshots.slots[*i].0;
                let ts = self.last_committed.load(Ordering::SeqCst);
                slot.store(ts + 1, Ordering::SeqCst);
                self.settle(slot, ts)
            }
            SnapshotSlot::Overflow(old) => {
                let mut overflow = self.snapshots.overflow.lock();
                release_overflow(&mut overflow, *old);
                let ts = self.last_committed.load(Ordering::SeqCst);
                *overflow.entry(ts).or_insert(0) += 1;
                *old = ts;
                ts
            }
        }
    }

    pub(crate) fn unregister_snapshot(&self, slot: &SnapshotSlot) {
        match slot {
            SnapshotSlot::Fast(i) => self.snapshots.slots[*i].0.store(0, Ordering::SeqCst),
            SnapshotSlot::Overflow(ts) => release_overflow(&mut self.snapshots.overflow.lock(), *ts),
        }
    }

    /// Oldest timestamp any current or future snapshot may read at.
    pub(crate) fn oldest_snapshot(&self) -> u64 {
        let overflow = self.snapshots.overflow.lock();
        let mut oldest = self.last_committed.load(Ordering::SeqCst);
        for slot in self.snapshots.slots.iter() {
            match slot.0.load(Ordering::SeqCst) {
                0 => {}
                published => oldest = oldest.min(published - 1),
            }
        }
        match overflow.keys().next() {
            Some(ts) => oldest.min(*ts),
            None => oldest,
        }
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
                                snapshots: Snapshots::new(),
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
            let mut chunk = inner.engine.scan(lo_ref, Bound::Excluded(codec::DATA_HI), false, 1024)?;
            let done = chunk.len() < 1024;
            // All versions of a key must be handled in one batch: removing
            // a tombstone in one write and the versions it hides in the
            // next would let a reader in between see the deleted value
            // again. So the last key of a full chunk, whose versions may
            // continue, is left for the next round.
            let mut resume = None;
            let mut split_key = false;
            if !done {
                let last_start = chunk.last().map(|(k, _)| k[..k.len() - 8].to_vec()).unwrap_or_default();
                let cut = chunk.iter().position(|(k, _)| codec::same_key(k, &last_start)).unwrap_or(0);
                if cut > 0 {
                    chunk.truncate(cut);
                    resume = Some(Bound::Included(last_start));
                } else {
                    // One key fills the whole chunk: trim its old versions
                    // but keep any tombstone.
                    split_key = true;
                }
            }
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
                    if ev.first() != Some(&1) && !split_key {
                        batch.delete(ek.clone());
                    }
                }
                last = Some(ek);
            }
            removed += batch.len() as u64;
            if !batch.is_empty() {
                inner.engine.write(batch)?;
            }
            match (done, resume, last) {
                (true, ..) => break,
                (false, Some(bound), _) => {
                    lo = bound;
                    cur_start.clear();
                    covered = false;
                }
                (false, None, Some(k)) => lo = Bound::Excluded(k),
                (false, None, None) => break,
            }
        }
        Ok(removed)
    }
}
