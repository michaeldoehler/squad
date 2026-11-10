use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use squad_storage::WriteBatch;

use crate::codec;
use crate::lock::{LockMode, WaitPolicy};
use crate::store::Inner;
use crate::{KvError, Result};

/// Transaction isolation level, with Postgres semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Isolation {
    /// Each statement sees everything committed before it started. The
    /// caller marks statement boundaries with [`Txn::refresh_snapshot`].
    #[default]
    ReadCommitted,
    /// One snapshot for the whole transaction (snapshot isolation).
    RepeatableRead,
    /// Snapshot isolation plus commit-time validation of everything the
    /// transaction read; a transaction that would not fit into a serial
    /// order fails with [`KvError::SerializationFailure`].
    Serializable,
}

#[derive(Clone)]
struct Write {
    value: Option<Vec<u8>>,
    /// `Some(ts)`: fail at commit if a version newer than `ts` exists and
    /// the key is not exclusively locked by this transaction.
    check_ts: Option<u64>,
}

type Range = (Bound<Vec<u8>>, Bound<Vec<u8>>);

#[derive(Default)]
struct ReadSet {
    keys: HashSet<Vec<u8>>,
    ranges: Vec<Range>,
}

/// A transaction on a [`Store`](crate::Store).
///
/// Writes are buffered and become visible to others atomically at
/// [`commit`](Txn::commit). Dropping a transaction rolls it back.
pub struct Txn {
    inner: Arc<Inner>,
    id: u64,
    isolation: Isolation,
        snapshot: u64,
    /// Registration of `snapshot` with the store.
    slot: crate::store::SnapshotSlot,
    read_only: bool,
    writes: BTreeMap<Vec<u8>, Write>,
    /// Undo journal for savepoints: previous state of each overwritten key.
    journal: Vec<(Vec<u8>, Option<Write>)>,
    journaling: bool,
    locks: HashMap<Vec<u8>, LockMode>,
    reads: Mutex<ReadSet>,
    finished: bool,
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k.as_slice()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn in_range(key: &[u8], (lo, hi): &Range) -> bool {
    (match lo {
        Bound::Included(l) => key >= l.as_slice(),
        Bound::Excluded(l) => key > l.as_slice(),
        Bound::Unbounded => true,
    }) && (match hi {
        Bound::Included(h) => key <= h.as_slice(),
        Bound::Excluded(h) => key < h.as_slice(),
        Bound::Unbounded => true,
    })
}

/// Hands out transaction ids from a per-thread block, so that starting a
/// transaction does not write to a counter shared by all threads.
fn next_id(inner: &Arc<Inner>) -> u64 {
    use std::cell::Cell;
    const BLOCK: u64 = 1024;
    thread_local! {
        /// (store the block belongs to, next id, end of block)
        static IDS: Cell<(usize, u64, u64)> = const { Cell::new((0, 0, 0)) };
    }
    let store = Arc::as_ptr(inner) as usize;
    IDS.with(|ids| {
        let (owner, next, end) = ids.get();
        if owner == store && next < end {
            ids.set((owner, next + 1, end));
            return next;
        }
        let start = inner.next_txn_id.fetch_add(BLOCK, Ordering::Relaxed);
        ids.set((store, start + 1, start + BLOCK));
        start
    })
}

fn ts_of(enc: &[u8]) -> u64 {
    !u64::from_be_bytes(enc[enc.len() - 8..].try_into().unwrap())
}

impl Txn {
    pub(crate) fn new(inner: Arc<Inner>, isolation: Isolation) -> Txn {
        if isolation == Isolation::Serializable {
            inner.serializable_live.fetch_add(1, Ordering::SeqCst);
        }
                        let id = next_id(&inner);
        let (snapshot, slot) = inner.register_snapshot(id);
        Txn {
            inner,
            id,
            isolation,
                        snapshot,
            slot,
            read_only: false,
            writes: BTreeMap::new(),
            journal: Vec::new(),
            journaling: false,
            locks: HashMap::new(),
            reads: Mutex::new(ReadSet::default()),
            finished: false,
        }
    }

    /// Unique id of this transaction within the process.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    /// Commit timestamp this transaction currently reads at.
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// True if the transaction has buffered writes.
    pub fn has_writes(&self) -> bool {
        !self.writes.is_empty()
    }

    /// Moves the snapshot to the latest committed state. Under
    /// [`Isolation::ReadCommitted`] the caller invokes this at the start of
    /// every statement; under the other levels it is a no-op.
    pub fn refresh_snapshot(&mut self) {
        if self.isolation == Isolation::ReadCommitted {
                        self.snapshot = self.inner.advance_snapshot(&mut self.slot);
        }
    }

    /// Passes on the result of an engine read made for this snapshot,
    /// unless the snapshot was invalidated meanwhile. That only happens
    /// on a read replica; the check comes after the read because the
    /// versions disappear after the snapshot is declared too old.
    fn checked<T>(&self, read: squad_storage::Result<T>) -> Result<T> {
        if self.snapshot < self.inner.min_snapshot.load(Ordering::Acquire) {
            return Err(KvError::SnapshotTooOld);
        }
        Ok(read?)
    }

    fn track_key(&self, key: &[u8]) {
        if self.isolation == Isolation::Serializable {
            self.reads.lock().keys.insert(key.to_vec());
        }
    }

    fn track_range(&self, lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) {
        if self.isolation == Isolation::Serializable {
            self.reads.lock().ranges.push((lo.clone(), hi.clone()));
        }
    }

    /// Reads `key` as of this transaction's snapshot, seeing its own writes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(w) = self.writes.get(key) {
            return Ok(w.value.clone());
        }
        self.track_key(key);
        let lo = codec::encode(key, self.snapshot);
        let hi = codec::key_end(key);
        let hit = self.inner.engine.scan(Bound::Included(&lo), Bound::Excluded(&hi), false, 1);
        let mut hit = self.checked(hit)?;
        Ok(hit.pop().and_then(|(_, v)| codec::decode_value(v)))
    }

    /// Newest *committed* version of `key`, ignoring the snapshot and this
    /// transaction's own writes: `(value, commit timestamp)`, where the value
    /// is `None` for a deleted key. Returns `None` if the key never existed.
    ///
    /// After taking a row lock, comparing the timestamp with
    /// [`snapshot_ts`](Txn::snapshot_ts) tells whether a concurrent
    /// transaction changed the row.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        self.track_key(key);
        let lo = codec::key_start(key);
        let hi = codec::key_end(key);
        let mut hit = self.inner.engine.scan(Bound::Included(&lo), Bound::Excluded(&hi), false, 1)?;
        Ok(hit.pop().map(|(k, v)| (codec::decode_value(v), ts_of(&k))))
    }

    /// The value a new transaction would see for `key` if this transaction
    /// committed now: its own write if it has one, otherwise the newest
    /// committed version regardless of the snapshot. Used for uniqueness
    /// checks after taking a row lock.
    pub fn get_current(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(w) = self.writes.get(key) {
            return Ok(w.value.clone());
        }
        Ok(self.get_latest(key)?.and_then(|(v, _)| v))
    }

    fn write(&mut self, key: &[u8], value: Option<Vec<u8>>, check_ts: Option<u64>) -> Result<()> {
        if self.read_only {
            return Err(KvError::ReadOnly);
        }
        if key.len() > squad_storage::MAX_KEY_LEN / 2 - 16 {
            return Err(squad_storage::StorageError::KeyTooLarge(key.len()).into());
        }
        // Keep the earliest check timestamp: the first write decides which
        // concurrent commits count as conflicts.
        let check_ts = match self.writes.get(key) {
            Some(prev) => prev.check_ts.or(check_ts),
            None => check_ts,
        };
        let prev = self.writes.insert(key.to_vec(), Write { value, check_ts });
        if self.journaling {
            self.journal.push((key.to_vec(), prev));
        }
        Ok(())
    }

    /// Buffers a write. At commit the transaction fails with
    /// [`KvError::SerializationFailure`] if another transaction committed a
    /// newer version of the key in the meantime (first committer wins),
    /// unless this transaction holds an exclusive lock on the key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(key, Some(value.to_vec()), Some(self.snapshot))
    }

    /// Buffers a delete, with the same conflict rule as [`put`](Txn::put).
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None, Some(self.snapshot))
    }

    /// Buffers a write without commit-time conflict detection. For callers
    /// that serialize access themselves through [`lock`](Txn::lock).
    pub fn put_blind(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(key, Some(value.to_vec()), None)
    }

    /// Buffers a delete without commit-time conflict detection.
    pub fn delete_blind(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None, None)
    }

    /// Takes a row lock that is held until the transaction ends. Returns
    /// `false` only when `wait` is [`WaitPolicy::Skip`] and the lock is busy.
    pub fn lock(&mut self, key: &[u8], mode: LockMode, wait: WaitPolicy) -> Result<bool> {
        match (self.locks.get(key), mode) {
            (Some(LockMode::Exclusive), _) | (Some(LockMode::Shared), LockMode::Shared) => return Ok(true),
            _ => {}
        }
        if !self.inner.locks.acquire(self.id, key, mode, wait)? {
            return Ok(false);
        }
        self.locks.insert(key.to_vec(), mode);
        Ok(true)
    }

    /// Marks a point to which [`rollback_to`](Txn::rollback_to) can return.
    pub fn savepoint(&mut self) -> usize {
        self.journaling = true;
        self.journal.len()
    }

    /// Undoes every write made after `savepoint` was taken. Locks acquired
    /// since then stay held.
    pub fn rollback_to(&mut self, savepoint: usize) {
        while self.journal.len() > savepoint {
            let (key, prev) = self.journal.pop().unwrap();
            match prev {
                Some(w) => {
                    self.writes.insert(key, w);
                }
                None => {
                    self.writes.remove(&key);
                }
            }
        }
    }

    fn enc_lo(lo: &Bound<Vec<u8>>) -> Vec<u8> {
        match lo {
            Bound::Included(k) => codec::key_start(k),
            Bound::Excluded(k) => codec::key_end(k),
            Bound::Unbounded => codec::DATA_LO.to_vec(),
        }
    }

    fn enc_hi(hi: &Bound<Vec<u8>>) -> Vec<u8> {
        match hi {
            Bound::Included(k) => codec::key_end(k),
            Bound::Excluded(k) => codec::key_start(k),
            Bound::Unbounded => codec::DATA_HI.to_vec(),
        }
    }

    /// Committed entries visible at the snapshot, in scan order. Returns the
    /// entries, the last user key examined and whether the range is
    /// exhausted.
    #[allow(clippy::type_complexity)]
    fn scan_committed(
        &self,
        lo: &Bound<Vec<u8>>,
        hi: &Bound<Vec<u8>>,
        reverse: bool,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>, bool)> {
        let engine = &self.inner.engine;
        let mut elo = Bound::Included(Self::enc_lo(lo));
        let mut ehi = Bound::Excluded(Self::enc_hi(hi));
        let mut out = Vec::new();
        let mut examined: Option<Vec<u8>> = None;
        let mut cur_start: Vec<u8> = Vec::new();
        // Reverse scans see the versions of a key oldest first, so the
        // visible one is only known when the key changes.
        let mut cand: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut emit = |ek: Vec<u8>, ev: Vec<u8>, out: &mut Vec<(Vec<u8>, Vec<u8>)>| {
            let (uk, _) = codec::decode(&ek).expect("malformed versioned key");
            if let Some(v) = codec::decode_value(ev) {
                out.push((uk.clone(), v));
            }
            examined = Some(uk);
        };
        loop {
            let n = (limit.saturating_sub(out.len())).saturating_mul(2).clamp(2, 4096);
            let chunk = self.checked(engine.scan(as_ref(&elo), as_ref(&ehi), reverse, n))?;
            let short = chunk.len() < n;
            let mut last = None;
            for (ek, ev) in chunk {
                let same = codec::same_key(&ek, &cur_start);
                if reverse {
                    if !same {
                        if let Some((k, v)) = cand.take() {
                            emit(k, v, &mut out);
                            if out.len() >= limit {
                                return Ok((out, examined, false));
                            }
                        }
                        cur_start = ek[..ek.len() - 8].to_vec();
                    }
                    if ts_of(&ek) <= self.snapshot {
                        cand = Some((ek.clone(), ev));
                    }
                } else if !same && ts_of(&ek) <= self.snapshot {
                    cur_start = ek[..ek.len() - 8].to_vec();
                    emit(ek.clone(), ev, &mut out);
                    if out.len() >= limit {
                        return Ok((out, examined, false));
                    }
                }
                last = Some(ek);
            }
            match last {
                Some(k) if !short => {
                    if reverse {
                        ehi = Bound::Excluded(k);
                    } else {
                        elo = Bound::Excluded(k);
                    }
                }
                _ => break,
            }
        }
        if let Some((k, v)) = cand.take() {
            emit(k, v, &mut out);
        }
        Ok((out, examined, true))
    }

    /// One page of a range scan merged with this transaction's own writes.
    /// Returns the entries and, unless the range is exhausted, the key after
    /// which (exclusively) the scan continues.
    #[allow(clippy::type_complexity)]
    fn scan_chunk(
        &self,
        lo: &Bound<Vec<u8>>,
        hi: &Bound<Vec<u8>>,
        reverse: bool,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let (committed, examined, exhausted) = self.scan_committed(lo, hi, reverse, limit)?;
        if self.writes.is_empty() {
            return Ok((committed, if exhausted { None } else { examined }));
        }
        // Own writes may only be merged up to the last committed key
        // examined; beyond it unseen committed keys could interleave.
        let (olo, ohi) = match (&examined, exhausted, reverse) {
            (_, true, _) | (None, _, _) => (lo.clone(), hi.clone()),
            (Some(k), false, false) => (lo.clone(), Bound::Included(k.clone())),
            (Some(k), false, true) => (Bound::Included(k.clone()), hi.clone()),
        };
        let empty = match (&olo, &ohi) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
            _ => false,
        };
        let mut own: Box<dyn Iterator<Item = (&Vec<u8>, &Write)>> = if empty {
            Box::new(std::iter::empty())
        } else if reverse {
            Box::new(self.writes.range::<[u8], _>((as_ref(&olo), as_ref(&ohi))).rev())
        } else {
            Box::new(self.writes.range::<[u8], _>((as_ref(&olo), as_ref(&ohi))))
        };
        let mut out = Vec::with_capacity(committed.len());
        let mut committed = committed.into_iter().peekable();
        let mut next_own = own.next();
        let before = |a: &[u8], b: &[u8]| if reverse { a > b } else { a < b };
        loop {
            if out.len() >= limit {
                let more = committed.peek().is_some() || next_own.is_some() || !exhausted;
                let bound = out.last().map(|(k, _): &(Vec<u8>, Vec<u8>)| k.clone());
                return Ok((out, if more { bound } else { None }));
            }
            match (committed.peek(), next_own) {
                (None, None) => break,
                (Some(_), None) => out.push(committed.next().unwrap()),
                (c, Some((k, w))) => {
                    if let Some((ck, _)) = c {
                        if before(ck, k) {
                            out.push(committed.next().unwrap());
                            continue;
                        }
                        if ck == k {
                            committed.next();
                        }
                    }
                    if let Some(v) = &w.value {
                        out.push((k.clone(), v.clone()));
                    }
                    next_own = own.next();
                }
            }
        }
        Ok((out, if exhausted { None } else { examined }))
    }

    /// Collects up to `limit` entries of a range.
    pub fn scan(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cur = Cursor::new(self, lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec), reverse);
        cur.chunk = limit.clamp(1, 1024);
        let mut out = Vec::new();
        while out.len() < limit {
            match cur.next(self)? {
                Some(e) => out.push(e),
                None => break,
            }
        }
        Ok(out)
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
                self.finished = true;
        // Most transactions hold no row locks; do not touch the shared
        // lock table for them.
        if !self.locks.is_empty() {
            self.inner.locks.release_all(self.id, self.locks.keys());
        }
                self.inner.unregister_snapshot(&self.slot);
        if self.isolation == Isolation::Serializable {
            self.inner.serializable_live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn commit_inner(&mut self) -> Result<()> {
        if self.writes.is_empty() {
            return Ok(());
        }
        let inner = self.inner.clone();
        let guard = inner.commit_lock.lock();

        if self.isolation == Isolation::Serializable {
            let reads = self.reads.lock();
            for (ts, keys) in inner.recent.lock().iter() {
                if *ts <= self.snapshot {
                    continue;
                }
                let hit = keys.iter().any(|k| reads.keys.contains(k) || reads.ranges.iter().any(|r| in_range(k, r)));
                if hit {
                    return Err(KvError::SerializationFailure);
                }
            }
        }

        let horizon = inner.oldest_snapshot();
        let ts = inner.last_committed.load(Ordering::SeqCst) + 1;
        let mut batch = WriteBatch { ops: Vec::with_capacity(self.writes.len() + 1) };
        for (key, w) in &self.writes {
            let start = codec::key_start(key);
            let end = codec::key_end(key);
            let versions = inner.engine.scan(Bound::Included(&start), Bound::Excluded(&end), false, 64)?;
            if let (Some(check), Some((newest, _))) = (w.check_ts, versions.first()) {
                if ts_of(newest) > check && self.locks.get(key) != Some(&LockMode::Exclusive) {
                    return Err(KvError::SerializationFailure);
                }
            }
            // Drop versions that are shadowed for every live snapshot.
            let mut covered = false;
            for (ek, _) in versions {
                if covered {
                    batch.delete(ek);
                } else if ts_of(&ek) <= horizon {
                    covered = true;
                }
            }
            let mut ek = start;
            ek.extend_from_slice(&(!ts).to_be_bytes());
            batch.put(ek, codec::encode_value(w.value.as_deref()));
        }
        batch.put(codec::META_LAST_TS.to_vec(), ts.to_be_bytes().to_vec());
        let lsn = inner.engine.write(batch)?;
        inner.last_committed.store(ts, Ordering::SeqCst);

        {
            let mut recent = inner.recent.lock();
            if inner.serializable_live.load(Ordering::SeqCst) == 0 {
                recent.clear();
            } else {
                while recent.front().is_some_and(|(t, _)| *t <= horizon) {
                    recent.pop_front();
                }
                recent.push_back((ts, Arc::new(self.writes.keys().cloned().collect())));
            }
        }
        drop(guard);
        inner.engine.sync(lsn)?;
        Ok(())
    }

        /// Makes all buffered writes durable and visible. On error nothing was
    /// applied and the transaction is rolled back.
    pub fn commit(mut self) -> Result<()> {
        self.end(true)
    }

    /// Ends the transaction in place, by commit or rollback, so that the
    /// object can be reused with [`restart`](Txn::restart). Sessions that
    /// run one short transaction after another avoid setting up a new
    /// transaction object for each.
    pub fn end(&mut self, commit: bool) -> Result<()> {
        let result = if commit && !self.finished { self.commit_inner() } else { Ok(()) };
        self.finish();
        result
    }

    /// Starts a new transaction in an object whose previous transaction
    /// has ended.
    pub fn restart(&mut self, isolation: Isolation) {
        assert!(self.finished, "restart of a transaction that is still running");
        if isolation == Isolation::Serializable {
            self.inner.serializable_live.fetch_add(1, Ordering::SeqCst);
        }
        self.id = next_id(&self.inner);
        let (snapshot, slot) = self.inner.register_snapshot(self.id);
        self.isolation = isolation;
        self.snapshot = snapshot;
        self.slot = slot;
        self.read_only = false;
        self.writes.clear();
        self.journal.clear();
        self.journaling = false;
        self.locks.clear();
        let reads = self.reads.get_mut();
        reads.keys.clear();
        reads.ranges.clear();
        self.finished = false;
    }

    /// Discards all buffered writes and releases all locks.
    pub fn rollback(mut self) {
        self.finish();
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        self.finish();
    }
}

/// A resumable range scan that does not borrow the transaction between
/// calls, so the caller may write while iterating.
///
/// Entries written by the transaction after the cursor passed their
/// position are not returned; entries written ahead of it are.
pub struct Cursor {
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    reverse: bool,
    buf: VecDeque<(Vec<u8>, Vec<u8>)>,
    done: bool,
    chunk: usize,
}

impl Cursor {
    pub fn new(txn: &Txn, lo: Bound<Vec<u8>>, hi: Bound<Vec<u8>>, reverse: bool) -> Cursor {
        txn.track_range(&lo, &hi);
        Cursor { lo, hi, reverse, buf: VecDeque::new(), done: false, chunk: 4 }
    }

    /// Hint for how many entries the first fetch should load.
    pub fn with_chunk(mut self, chunk: usize) -> Cursor {
        self.chunk = chunk.clamp(1, 1024);
        self
    }

    pub fn next(&mut self, txn: &Txn) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if let Some(e) = self.buf.pop_front() {
                return Ok(Some(e));
            }
            if self.done {
                return Ok(None);
            }
            let (entries, next) = txn.scan_chunk(&self.lo, &self.hi, self.reverse, self.chunk)?;
            self.chunk = (self.chunk * 4).min(1024);
            self.buf = entries.into();
            match next {
                Some(k) if self.reverse => self.hi = Bound::Excluded(k),
                Some(k) => self.lo = Bound::Excluded(k),
                None => self.done = true,
            }
        }
    }
}
