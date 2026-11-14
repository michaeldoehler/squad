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
    /// Newest commit timestamp visible to new snapshots. Only durable
    /// commits are visible: a commit's timestamp is published here after
    /// the engine confirmed that its log record is durable. Since the
    /// engine's durability is a prefix of its log, and timestamps are
    /// assigned in log order, publishing with `fetch_max` keeps the
    /// visible state a prefix of the commit history.
    pub(crate) last_committed: AtomicU64,
    /// Newest commit timestamp handed out, under `commit_lock`. Commits
    /// between `last_committed` and this are in the engine but wait for
    /// durability; they are invisible to snapshots, but they do count as
    /// conflicts for concurrent writers.
    pub(crate) last_assigned: AtomicU64,
    /// Snapshot timestamps in use, with reference counts.
    pub(crate) snapshots: Snapshots,
    pub(crate) locks: LockManager,
    pub(crate) next_txn_id: AtomicU64,
    /// Number of live serializable transactions; commits only record their
    /// write sets in `recent` while this is non-zero.
    pub(crate) serializable_live: AtomicUsize,
    /// Write sets of recent commits, for serializable validation.
    pub(crate) recent: Mutex<VecDeque<CommitRecord>>,
    /// Snapshots below this timestamp can no longer be read. Stays zero
    /// except on a read replica.
    pub(crate) min_snapshot: AtomicU64,
    /// Read replica only: version removals shipped by the writer that
    /// wait until no local snapshot needs the versions.
    pub(crate) deferred: Mutex<Deferred>,
}

/// Removals a replica has not applied yet, oldest first: the timestamp
/// from which on no snapshot reads the versions, and their engine keys.
#[derive(Default)]
pub(crate) struct Deferred {
    queue: VecDeque<(u64, Vec<Vec<u8>>)>,
    keys: usize,
}

/// A replica keeps at most this many removals waiting for its readers.
/// Beyond it the oldest snapshots are invalidated instead.
const MAX_DEFERRED_KEYS: usize = 1 << 20;

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
                last_assigned: AtomicU64::new(last),
                snapshots: Snapshots::new(),
                locks: LockManager::default(),
                next_txn_id: AtomicU64::new(1),
                serializable_live: AtomicUsize::new(0),
                recent: Mutex::new(VecDeque::new()),
                min_snapshot: AtomicU64::new(0),
                deferred: Mutex::new(Deferred::default()),
            }),
        })
    }

    // -----------------------------------------------------------------
    // Read replicas
    //
    // A replica does not commit. It receives the engine batches of the
    // writer's commits, in order, writes each to its own engine and then
    // moves the timestamp new snapshots read at. The batch of a commit
    // carries that commit's timestamp, so the versions it adds stay
    // invisible until then: a batch becomes visible as a whole.
    // -----------------------------------------------------------------

    /// Prepares a batch shipped by the writer for the local engine and
    /// returns it with the commit timestamp it carries, if it is a commit.
    ///
    /// The writer removes versions that none of *its* snapshots read. A
    /// snapshot here may be older, so removals are taken out of the batch
    /// and added to a later one, once every local snapshot is at least as
    /// new as the state the removals were shipped in. If more than about
    /// a million removals pile up behind a long-running reader, snapshots
    /// older than the oldest removals are invalidated instead and their
    /// reads fail with [`KvError::SnapshotTooOld`](crate::KvError).
    pub fn replay_prepare(&self, mut batch: WriteBatch) -> (WriteBatch, Option<u64>) {
        let inner = &self.inner;
        let mut commit_ts = None;
        let mut removed = Vec::new();
        batch.ops.retain_mut(|(key, value)| match value {
            None if key.first() == Some(&codec::DATA_LO[0]) => {
                removed.push(std::mem::take(key));
                false
            }
            Some(v) if key.as_slice() == codec::META_LAST_TS && v.len() == 8 => {
                commit_ts = Some(u64::from_be_bytes(v.as_slice().try_into().unwrap()));
                true
            }
            _ => true,
        });
        let mut deferred = inner.deferred.lock();
        if !removed.is_empty() {
            // Everything the writer removed was shadowed for snapshots at
            // or after the state before this batch.
            deferred.keys += removed.len();
            deferred.queue.push_back((inner.last_committed.load(Ordering::SeqCst), removed));
        }
        let mut horizon = inner.oldest_snapshot();
        loop {
            match deferred.queue.front() {
                Some((needed, _)) if *needed <= horizon => {}
                Some((needed, _)) if deferred.keys > MAX_DEFERRED_KEYS => {
                    inner.min_snapshot.fetch_max(*needed, Ordering::AcqRel);
                    horizon = horizon.max(*needed);
                }
                _ => break,
            }
            let (_, keys) = deferred.queue.pop_front().unwrap();
            deferred.keys -= keys.len();
            for key in keys {
                batch.delete(key);
            }
        }
        if batch.is_empty() {
            // The local engine must give this batch the log position the
            // writer gave it, which an empty batch would not take up.
            batch.delete(codec::META_REPLAY_PAD.to_vec());
        }
        (batch, commit_ts)
    }

    /// Makes the commit with timestamp `ts`, whose batch is in the
    /// engine, visible to new snapshots.
    pub fn replay_publish(&self, ts: u64) {
        self.inner.last_committed.fetch_max(ts, Ordering::SeqCst);
    }

    /// Invalidates every snapshot: the engine's contents are about to be
    /// replaced by a newer state. Reads fail with
    /// [`KvError::SnapshotTooOld`](crate::KvError) until
    /// [`replay_reset`](Store::replay_reset).
    pub fn replay_invalidate(&self) {
        self.inner.min_snapshot.store(u64::MAX, Ordering::Release);
    }

    /// Adopts the state now in the engine: new snapshots read at its last
    /// commit, older ones stay invalid.
    pub fn replay_reset(&self) -> Result<()> {
        let inner = &self.inner;
        let last = match inner.engine.get(codec::META_LAST_TS)? {
            Some(v) if v.len() == 8 => u64::from_be_bytes(v.try_into().unwrap()),
            _ => 0,
        };
        *inner.deferred.lock() = Deferred::default();
        inner.last_committed.fetch_max(last, Ordering::SeqCst);
        inner.min_snapshot.store(inner.last_committed.load(Ordering::SeqCst), Ordering::Release);
        Ok(())
    }

    /// Forgets the removals a replica was holding back. After promotion
    /// the versions are found again by [`gc`](Store::gc).
    pub fn replay_finish(&self) {
        *self.inner.deferred.lock() = Deferred::default();
    }

    /// The value `batch` stores for user key `key`: `Some(None)` if the
    /// batch deletes the key, `None` if it does not touch it.
    pub fn replayed_value(batch: &WriteBatch, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let start = codec::key_start(key);
        batch.ops.iter().find_map(|(k, v)| match v {
            Some(v) if codec::same_key(k, &start) => Some(codec::decode_value(v.clone())),
            _ => None,
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
        self.gc_encoded(codec::DATA_LO.to_vec(), codec::DATA_HI.to_vec())
    }

    /// Timestamp of the newest commit visible to new snapshots. Commits
    /// become visible once they are durable.
    pub fn last_committed(&self) -> u64 {
        self.inner.last_committed.load(Ordering::SeqCst)
    }

    /// The oldest timestamp any open snapshot reads at; the newest commit
    /// if none is open. State replaced at or before it is invisible to
    /// every present and future snapshot.
    pub fn oldest_snapshot(&self) -> u64 {
        self.inner.oldest_snapshot()
    }

    /// Like [`gc`](Store::gc), limited to keys in `lo..hi`.
    pub fn gc_range(&self, lo: &[u8], hi: &[u8]) -> Result<u64> {
        self.gc_encoded(codec::key_start(lo), codec::key_start(hi))
    }

    fn gc_encoded(&self, lo: Vec<u8>, hi: Vec<u8>) -> Result<u64> {
        let inner = &self.inner;
        let mut removed = 0u64;
        let mut lo: Bound<Vec<u8>> = Bound::Included(lo);
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
            let mut chunk = inner.engine.scan(lo_ref, Bound::Excluded(hi.as_slice()), false, 1024)?;
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
