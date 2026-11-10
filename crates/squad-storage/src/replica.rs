//! Read replicas.
//!
//! A [`ReplicaEngine`] keeps a local copy of a database whose writer
//! ships its log to an object store (see [`TieredEngine`]). It restores
//! the newest snapshot, then applies the log objects the writer uploads,
//! in order, each record as one atomic local write with the LSN the
//! writer gave it. It never writes to the object store while following.
//!
//! # What a replica applies
//!
//! Only records that are part of the history every later writer builds
//! on. A writer's records count up to the LSN at which the next writer
//! took over, so while a takeover is under way (an epoch is claimed but
//! its starting point is not fixed yet) the records of the writer being
//! replaced are held back until that point is known.
//!
//! # Falling behind
//!
//! The writer deletes log objects once a snapshot covers them (it keeps
//! them one snapshot longer than a restore needs). A replica that finds
//! a gap between its own position and the log downloads the newest
//! snapshot into a staging directory while it keeps serving the old
//! copy, then swaps the two. The layer above is told before and after
//! the swap so that no reader sees a mix of both.
//!
//! # Promotion
//!
//! [`ReplicaEngine::promote`] claims the next writer epoch (which fences
//! the old writer), applies everything the old writer managed to upload,
//! fixes its starting point and from then on behaves like a
//! [`TieredEngine`] on the same directory.

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use squad_object::ObjectStore;

use crate::disk::{DiskEngine, DiskOptions};
use crate::remote::{
    base_key, claim_epoch, claim_key, dir_is_empty, materialize_latest, max_claim, read_marker, read_remote,
    read_remote_from, remote_err, wipe, write_marker, RemoteConfig, RemoteStats, SnapshotState, TieredEngine,
    DATA_FILE, REPLICA_FILE,
};
use crate::wal;
use crate::{Engine, EngineStats, Result, StorageError, VerifyReport, WriteBatch};

/// Directory, inside the replica's own, into which a newer snapshot is
/// downloaded before it replaces the local copy.
const STAGING_DIR: &str = "rebase";

/// Callbacks through which the layers above a [`ReplicaEngine`] follow
/// what it applies.
pub trait ReplayHooks {
    /// A record shipped by the writer is about to be written to the local
    /// engine. The returned batch is written in its place, as one atomic
    /// write.
    fn prepare(&mut self, batch: WriteBatch) -> WriteBatch {
        batch
    }
    /// The batch returned by the last `prepare` is in the local engine.
    fn applied(&mut self, _lsn: u64) {}
    /// The local copy is about to be replaced by a newer snapshot. Reads
    /// that started on the old copy must not be answered from the new one.
    fn before_rebase(&mut self) {}
    /// The local copy was replaced and is readable again.
    fn after_rebase(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Hooks that do nothing: for a replica used as a plain key-value copy.
impl ReplayHooks for () {}

/// Position of a replica.
#[derive(Debug, Clone, Default)]
pub struct ReplicaStatus {
    /// True once the replica was promoted and is the writer.
    pub promoted: bool,
    /// Epoch of the writer whose records were applied last, or this
    /// node's own epoch after promotion.
    pub epoch: u64,
    /// LSN of the last record applied locally.
    pub applied_lsn: u64,
    /// Highest LSN seen in the object store at the last poll.
    pub remote_lsn: u64,
    /// Time since a record was last applied.
    pub since_last_apply: Option<Duration>,
    /// Time since the object store was last read successfully.
    pub since_last_poll: Option<Duration>,
    /// How often the local copy was replaced by a newer snapshot.
    pub rebases: u64,
    /// A new writer is taking over and has not fixed its starting point;
    /// records of the previous writer are held back until it has.
    pub waiting_for_takeover: bool,
    /// Why the last poll failed, if it did.
    pub last_error: Option<String>,
}

enum Mode {
    Follower(DiskEngine),
    Writer(TieredEngine),
    /// Between two local copies, or after a failed swap.
    Closed,
}

/// Mutable state of the apply loop; its lock serializes polls and
/// promotion.
struct Apply {
    /// Epoch recorded in the directory's marker file.
    epoch: u64,
}

/// A local engine that follows a remote writer. See the module
/// documentation.
pub struct ReplicaEngine {
    store: Arc<dyn ObjectStore>,
    dir: PathBuf,
    opts: DiskOptions,
    /// How this node ships once it is promoted.
    config: RemoteConfig,
    mode: RwLock<Mode>,
    apply: Mutex<Apply>,
    promoted: AtomicBool,
    epoch: AtomicU64,
    applied_lsn: AtomicU64,
    remote_lsn: AtomicU64,
    rebases: AtomicU64,
    waiting: AtomicBool,
    last_apply: Mutex<Option<Instant>>,
    last_poll: Mutex<Option<Instant>>,
    last_error: Mutex<Option<String>>,
}

fn closed() -> StorageError {
    StorageError::Remote("the replica is replacing its local copy with a newer snapshot".into())
}

fn read_only() -> StorageError {
    StorageError::Invalid("a read replica does not accept writes".into())
}

fn mark_replica(opts: &DiskOptions, dir: &Path) -> Result<()> {
    let vfs = &opts.vfs;
    let f = vfs.create(&dir.join(REPLICA_FILE))?;
    f.write_at(b"squad replica\n", 0)?;
    f.sync()?;
    vfs.sync_dir(dir)?;
    Ok(())
}

impl ReplicaEngine {
    /// Opens a replica of the database kept in `config.store`.
    ///
    /// An empty or missing `dir` is filled from the newest snapshot. A
    /// directory that already holds an earlier state of the same history
    /// (a replica's own copy, or the copy of a former writer that had
    /// shipped everything) is reused and brought up to date from the log.
    /// A replica's copy that cannot be continued is replaced; any other
    /// directory that does not match is refused.
    ///
    /// `config.mode` and `config.snapshot_wal_bytes` take effect when the
    /// replica is promoted.
    pub fn open(dir: &Path, opts: DiskOptions, config: RemoteConfig) -> Result<ReplicaEngine> {
        let store = config.store.clone();
        let vfs = opts.vfs.clone();
        vfs.create_dir_all(dir)?;
        let state = read_remote(&*store)?;
        if state.epochs.is_empty() {
            return Err(StorageError::Invalid("the object store holds no database to follow".into()));
        }
        let reused = if dir_is_empty(&opts, dir)? {
            None
        } else {
            let disk = DiskEngine::open(dir, opts.clone())?;
            let lsn = disk.last_lsn();
            let prefix = read_marker(&opts, dir)?.filter(|e| state.contains(*e, lsn) && lsn <= state.lsn);
            match prefix {
                Some(epoch) => Some((disk, epoch)),
                None if vfs.exists(&dir.join(REPLICA_FILE))? => None,
                None => {
                    return Err(StorageError::Invalid(format!(
                        "{} holds a database that is not an earlier state of the one in the object store; \
                         a replica needs an empty directory or its own earlier copy",
                        dir.display()
                    )))
                }
            }
        };
        let (disk, epoch) = match reused {
            Some(found) => found,
            None => {
                wipe(&opts, dir)?;
                let (disk, latest) = materialize_latest(&*store, dir, &opts)?;
                let epoch = latest
                    .epoch_at(disk.last_lsn())
                    .ok_or_else(|| StorageError::Invalid("the object store holds no database to follow".into()))?;
                write_marker(&opts, dir, epoch)?;
                (disk, epoch)
            }
        };
        mark_replica(&opts, dir)?;
        let lsn = disk.last_lsn();
        Ok(ReplicaEngine {
            store,
            dir: dir.to_path_buf(),
            opts,
            config,
            mode: RwLock::new(Mode::Follower(disk)),
            apply: Mutex::new(Apply { epoch }),
            promoted: AtomicBool::new(false),
            epoch: AtomicU64::new(epoch),
            applied_lsn: AtomicU64::new(lsn),
            remote_lsn: AtomicU64::new(state.lsn.max(lsn)),
            rebases: AtomicU64::new(0),
            waiting: AtomicBool::new(false),
            last_apply: Mutex::new(None),
            last_poll: Mutex::new(Some(Instant::now())),
            last_error: Mutex::new(None),
        })
    }

    /// Applies everything the writer has uploaded since the last call.
    /// Returns the number of records applied. Does nothing once the
    /// replica is promoted.
    pub fn poll(&self, hooks: &mut dyn ReplayHooks) -> Result<u64> {
        let mut apply = self.apply.lock();
        if self.promoted.load(Ordering::Acquire) {
            return Ok(0);
        }
        let result = self.advance(&mut apply, hooks, false);
        self.note(&result);
        result
    }

    fn note(&self, result: &Result<u64>) {
        match result {
            Ok(_) => {
                *self.last_poll.lock() = Some(Instant::now());
                *self.last_error.lock() = None;
            }
            Err(e) => *self.last_error.lock() = Some(e.to_string()),
        }
    }

    /// One round: reads the remote state and applies what continues the
    /// local copy. `claimant` is set by a node that has claimed the next
    /// epoch itself: everything visible to it counts.
    fn advance(&self, apply: &mut Apply, hooks: &mut dyn ReplayHooks, claimant: bool) -> Result<u64> {
        let local = match &*self.mode.read() {
            Mode::Follower(disk) => Some(disk.last_lsn()),
            Mode::Writer(_) => return Ok(0),
            Mode::Closed => None,
        };
        let Some(local) = local else {
            // An earlier swap failed half way; start over from a snapshot.
            self.rebase(apply, hooks)?;
            return Ok(1);
        };
        let store = &*self.store;
        let state = read_remote_from(store, Some(local))?;
        if state.epochs.is_empty() {
            return Err(StorageError::Invalid("the object store holds no database to follow".into()));
        }
        let snapshot_lsn = state.snapshot.map_or(0, |s| s.1);
        self.remote_lsn.store(state.lsn.max(snapshot_lsn), Ordering::Release);
        // The log no longer reaches back to this copy, or this copy is
        // not part of the history in the store: take the newest snapshot.
        if snapshot_lsn > state.lsn || !state.contains(apply.epoch, local) {
            self.rebase(apply, hooks)?;
            return Ok(1);
        }
        // The claims are read after the log listing. If no later epoch is
        // claimed now, every log object listed above was complete before
        // any future writer reads the store, so that writer starts after
        // it. Otherwise the newest established writer is being replaced
        // and its records wait until the new starting point is known.
        let frozen = match (claimant, state.epochs.last()) {
            (false, Some((newest, _))) if max_claim(store)? > *newest => Some(*newest),
            _ => None,
        };
        let mut waiting = false;
        let mut applied = 0u64;
        {
            let mode = self.mode.read();
            let Mode::Follower(disk) = &*mode else { return Ok(0) };
            for object in &state.wal {
                if frozen == Some(object.epoch) {
                    waiting = true;
                    break;
                }
                // Gone: the writer cleaned up meanwhile. The next round
                // sees the new state.
                let Some(bytes) = store.get(&object.key).map_err(remote_err)? else { break };
                let mut off = 0;
                while let Some((lsn, ops, next)) = wal::read_record(&bytes, off) {
                    off = next;
                    let have = disk.last_lsn();
                    if lsn <= have || lsn > object.limit {
                        continue;
                    }
                    if lsn != have + 1 {
                        return Err(StorageError::Corruption(format!(
                            "log object {} does not continue at LSN {}",
                            object.key,
                            have + 1
                        )));
                    }
                    if object.epoch != apply.epoch {
                        write_marker(&self.opts, &self.dir, object.epoch)?;
                        apply.epoch = object.epoch;
                        self.epoch.store(object.epoch, Ordering::Release);
                    }
                    let batch =
                        WriteBatch { ops: ops.iter().map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec))).collect() };
                    let batch = hooks.prepare(batch);
                    let written = disk.write(batch)?;
                    if written != lsn {
                        return Err(StorageError::Corruption(format!(
                            "replay of {} produced LSN {written}, expected {lsn}",
                            object.key
                        )));
                    }
                    self.applied_lsn.store(lsn, Ordering::Release);
                    hooks.applied(lsn);
                    applied += 1;
                }
                if off != bytes.len() {
                    return Err(StorageError::Corruption(format!("log object {} is damaged", object.key)));
                }
            }
            if applied > 0 {
                disk.sync(disk.last_lsn())?;
                *self.last_apply.lock() = Some(Instant::now());
            }
        }
        self.waiting.store(waiting, Ordering::Release);
        Ok(applied)
    }

    /// Replaces the local copy by the newest snapshot plus the log after
    /// it. The old copy serves reads until the new one is complete.
    fn rebase(&self, apply: &mut Apply, hooks: &mut dyn ReplayHooks) -> Result<()> {
        let vfs = &self.opts.vfs;
        let staging = self.dir.join(STAGING_DIR);
        vfs.create_dir_all(&staging)?;
        wipe(&self.opts, &staging)?;
        let (staged, state) = materialize_latest(&*self.store, &staging, &self.opts)?;
        let lsn = staged.last_lsn();
        let epoch = state
            .epoch_at(lsn)
            .ok_or_else(|| StorageError::Invalid("the object store holds no database to follow".into()))?;
        // After a checkpoint the data file alone is the whole database.
        staged.checkpoint()?;
        drop(staged);

        hooks.before_rebase();
        {
            let mut mode = self.mode.write();
            // Closes the old copy. A crash from here on leaves either the
            // old data file or none, or the complete new one.
            *mode = Mode::Closed;
            wipe(&self.opts, &self.dir)?;
            write_marker(&self.opts, &self.dir, epoch)?;
            mark_replica(&self.opts, &self.dir)?;
            vfs.rename(&staging.join(DATA_FILE), &self.dir.join(DATA_FILE))?;
            vfs.sync_dir(&self.dir)?;
            let disk = DiskEngine::open(&self.dir, self.opts.clone())?;
            if disk.last_lsn() != lsn {
                return Err(StorageError::Corruption(format!(
                    "the staged snapshot stands at LSN {} instead of {lsn}",
                    disk.last_lsn()
                )));
            }
            *mode = Mode::Follower(disk);
        }
        let _ = wipe(&self.opts, &staging);
        apply.epoch = epoch;
        self.epoch.store(epoch, Ordering::Release);
        self.applied_lsn.store(lsn, Ordering::Release);
        self.remote_lsn.fetch_max(lsn, Ordering::AcqRel);
        self.rebases.fetch_add(1, Ordering::Relaxed);
        *self.last_apply.lock() = Some(Instant::now());
        hooks.after_rebase()
    }

    /// Turns the replica into the writer.
    ///
    /// Claims the next epoch, which stops the current writer at its next
    /// upload, applies every record that writer uploaded, and starts
    /// shipping. On error the node stays a replica and keeps following;
    /// promotion can be tried again.
    pub fn promote(&self, hooks: &mut dyn ReplayHooks) -> Result<()> {
        let mut apply = self.apply.lock();
        if self.promoted.load(Ordering::Acquire) {
            return Ok(());
        }
        let store = &*self.store;
        let epoch = claim_epoch(store)?;
        // Everything the old writer uploads from now on either is visible
        // to the rounds below or fails that writer's fence check.
        while self.advance(&mut apply, hooks, true)? > 0 {}
        let state = read_remote(store)?;
        let snapshot = SnapshotState::load(store, &state)?;
        let lsn = match &*self.mode.read() {
            Mode::Follower(disk) => {
                let lsn = disk.last_lsn();
                disk.sync(lsn)?;
                lsn
            }
            _ => return Err(closed()),
        };
        if store.head(&claim_key(epoch + 1)).map_err(remote_err)?.is_some() {
            return Err(StorageError::Remote(format!(
                "another node claimed the database while this one was being promoted (epoch {epoch})"
            )));
        }
        // From here on this node's uploads count.
        store.put(&base_key(epoch), lsn.to_string().as_bytes()).map_err(remote_err)?;
        write_marker(&self.opts, &self.dir, epoch)?;
        apply.epoch = epoch;
        let vfs = &self.opts.vfs;
        if vfs.exists(&self.dir.join(REPLICA_FILE))? {
            vfs.remove(&self.dir.join(REPLICA_FILE))?;
            vfs.sync_dir(&self.dir)?;
        }
        let mut mode = self.mode.write();
        let Mode::Follower(disk) = std::mem::replace(&mut *mode, Mode::Closed) else { return Err(closed()) };
        let config = RemoteConfig { restore: false, ..self.config.clone() };
        *mode = Mode::Writer(TieredEngine::start(disk, &self.dir, self.opts.clone(), config, epoch, snapshot, lsn)?);
        self.epoch.store(epoch, Ordering::Release);
        self.waiting.store(false, Ordering::Release);
        self.promoted.store(true, Ordering::Release);
        Ok(())
    }

    /// True once the replica was promoted.
    pub fn is_promoted(&self) -> bool {
        self.promoted.load(Ordering::Acquire)
    }

    pub fn status(&self) -> ReplicaStatus {
        let promoted = self.is_promoted();
        let applied_lsn = match &*self.mode.read() {
            Mode::Follower(disk) => disk.last_lsn(),
            Mode::Writer(tiered) => tiered.disk().last_lsn(),
            Mode::Closed => self.applied_lsn.load(Ordering::Acquire),
        };
        let remote_lsn = match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.remote_stats().shipped_lsn,
            _ => self.remote_lsn.load(Ordering::Acquire).max(applied_lsn),
        };
        ReplicaStatus {
            promoted,
            epoch: self.epoch.load(Ordering::Acquire),
            applied_lsn,
            remote_lsn,
            since_last_apply: self.last_apply.lock().map(|t| t.elapsed()),
            since_last_poll: self.last_poll.lock().map(|t| t.elapsed()),
            rebases: self.rebases.load(Ordering::Relaxed),
            waiting_for_takeover: self.waiting.load(Ordering::Acquire),
            last_error: self.last_error.lock().clone(),
        }
    }

    /// Counters of the remote copy, once this node is the writer.
    pub fn remote_stats(&self) -> Option<RemoteStats> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => Some(tiered.remote_stats()),
            _ => None,
        }
    }

    /// Uploads all log records written so far, once this node is the
    /// writer. A replica has nothing to upload.
    pub fn flush_remote(&self) -> Result<()> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.flush_remote(),
            _ => Ok(()),
        }
    }
}

impl Engine for ReplicaEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match &*self.mode.read() {
            Mode::Follower(disk) => disk.get(key),
            Mode::Writer(tiered) => tiered.get(key),
            Mode::Closed => Err(closed()),
        }
    }

    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match &*self.mode.read() {
            Mode::Follower(disk) => disk.scan(lo, hi, reverse, limit),
            Mode::Writer(tiered) => tiered.scan(lo, hi, reverse, limit),
            Mode::Closed => Err(closed()),
        }
    }

    fn write(&self, batch: WriteBatch) -> Result<u64> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.write(batch),
            Mode::Follower(_) => Err(read_only()),
            Mode::Closed => Err(closed()),
        }
    }

    fn sync(&self, lsn: u64) -> Result<()> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.sync(lsn),
            Mode::Follower(disk) => disk.sync(lsn),
            Mode::Closed => Err(closed()),
        }
    }

    fn checkpoint(&self) -> Result<()> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.checkpoint(),
            // Local only: a replica uploads nothing.
            Mode::Follower(disk) => disk.checkpoint(),
            Mode::Closed => Err(closed()),
        }
    }

    fn backup(&self, dest: &Path) -> Result<()> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.backup(dest),
            Mode::Follower(disk) => disk.backup(dest),
            Mode::Closed => Err(closed()),
        }
    }

    fn verify(&self) -> Result<VerifyReport> {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.verify(),
            Mode::Follower(disk) => disk.verify(),
            Mode::Closed => Err(closed()),
        }
    }

    fn stats(&self) -> EngineStats {
        match &*self.mode.read() {
            Mode::Writer(tiered) => tiered.stats(),
            Mode::Follower(disk) => disk.stats(),
            Mode::Closed => EngineStats::default(),
        }
    }
}
