//! Tiering to an object store.
//!
//! [`TieredEngine`] wraps a [`DiskEngine`] and keeps a complete,
//! restorable copy of the database in an [`ObjectStore`]. The local
//! directory becomes a working copy; the object store is the durable
//! home from which a lost machine is rebuilt.
//!
//! # Layout in the object store
//!
//! ```text
//! writer/claim-<epoch>            one per writer that ever opened the database
//! writer/base-<epoch>             LSN at which that writer took over
//! wal/<epoch>/<first>-<last>      log records first..=last, in WAL record format
//! snapshot/<epoch>-<lsn>          manifest of a full image of the data file at <lsn>
//! chunk/<sha256>                  content-addressed pieces of data file images
//! ```
//!
//! Numbers are zero-padded so that keys sort numerically. Every object is
//! written once and never modified, which makes uploads idempotent: a
//! retried or duplicated upload writes the same bytes under the same key.
//!
//! # Restore
//!
//! The newest valid snapshot is downloaded and the log records after it
//! are replayed in LSN order. A writer's records count only up to the LSN
//! at which the next writer took over (`base` of the next epoch), so
//! anything a superseded writer still uploads is ignored.
//!
//! # Fencing
//!
//! A writer claims the next epoch with a conditional put before it reads
//! the remote state, and checks before and after each upload that no
//! later claim exists. Once it sees one it stops and refuses writes.

use std::collections::HashSet;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use squad_object::ObjectStore;

use crate::disk::{DiskEngine, DiskOptions};
use crate::page::{self, HDR, PAGE_SIZE};
use crate::wal;
use crate::{Engine, EngineStats, Result, StorageError, VerifyReport, WriteBatch};

/// Size of the pieces a data file image is cut into. A piece is uploaded
/// only if no piece with the same content exists yet.
const CHUNK: usize = 1 << 20;
/// Records waiting for upload are dropped beyond this size; the next
/// snapshot then covers them.
const MAX_PENDING: usize = 256 << 20;
pub(crate) const MARKER_FILE: &str = "REMOTE";
/// Present in a directory that is the working copy of a read replica.
/// Such a directory holds nothing the object store does not, so it may be
/// replaced by a fresh download at any time.
pub(crate) const REPLICA_FILE: &str = "REPLICA";
const SNAPSHOT_TMP: &str = "remote-snapshot.tmp";
pub(crate) const DATA_FILE: &str = "data.db";

/// When a commit counts as safe in the object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteSync {
    /// Commits return after the local `fsync`. Log records are uploaded
    /// in the background at least every `interval`. If the machine and
    /// its disk are lost, commits of roughly the last `interval` are lost.
    Async { interval: Duration },
    /// A commit returns only after its log record is in the object
    /// store. Concurrent commits share one upload. Nothing acknowledged
    /// is lost even if the machine is.
    Sync,
}

/// Configuration of the remote copy.
#[derive(Clone)]
pub struct RemoteConfig {
    pub store: Arc<dyn ObjectStore>,
    pub mode: RemoteSync,
    /// Replace the local directory with the remote state when the two
    /// disagree (the remote is ahead, or the directory belongs to a
    /// superseded writer). Without it such an open fails.
    pub restore: bool,
    /// A new snapshot is uploaded once this many bytes of log have been
    /// shipped since the last one.
    pub snapshot_wal_bytes: u64,
}

impl RemoteConfig {
    pub fn new(store: Arc<dyn ObjectStore>, mode: RemoteSync) -> RemoteConfig {
        RemoteConfig { store, mode, restore: false, snapshot_wal_bytes: 64 << 20 }
    }
}

impl std::fmt::Debug for RemoteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("mode", &self.mode)
            .field("restore", &self.restore)
            .field("snapshot_wal_bytes", &self.snapshot_wal_bytes)
            .finish_non_exhaustive()
    }
}

/// Counters of the remote copy.
#[derive(Debug, Clone, Default)]
pub struct RemoteStats {
    /// Epoch of this writer.
    pub epoch: u64,
    /// Highest LSN known to be in the object store.
    pub shipped_lsn: u64,
    /// LSN of the newest uploaded snapshot.
    pub snapshot_lsn: u64,
    pub wal_objects: u64,
    pub wal_bytes: u64,
    pub snapshots: u64,
    pub chunks_uploaded: u64,
    pub chunk_bytes_uploaded: u64,
    /// Chunks that were already present and not uploaded again.
    pub chunks_reused: u64,
}

pub(crate) fn remote_err(e: squad_object::Error) -> StorageError {
    StorageError::Remote(e.to_string())
}

pub(crate) fn claim_key(epoch: u64) -> String {
    format!("writer/claim-{epoch:020}")
}
pub(crate) fn base_key(epoch: u64) -> String {
    format!("writer/base-{epoch:020}")
}
fn wal_prefix(epoch: u64) -> String {
    format!("wal/{epoch:020}/")
}
fn wal_key(epoch: u64, first: u64, last: u64) -> String {
    format!("wal/{epoch:020}/{first:020}-{last:020}")
}
fn snapshot_key(epoch: u64, lsn: u64) -> String {
    format!("snapshot/{epoch:020}-{lsn:020}")
}
fn chunk_key(hash: &str) -> String {
    format!("chunk/{hash}")
}

fn pair(name: &str) -> Option<(u64, u64)> {
    let (a, b) = name.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
}

/// A snapshot manifest: the LSN and the ordered chunks of a data file.
struct Manifest {
    lsn: u64,
    size: u64,
    chunks: Vec<(String, u64)>,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut s = format!("squad-snapshot 1\nlsn {}\nsize {}\n", self.lsn, self.size);
        for (hash, len) in &self.chunks {
            s.push_str(&format!("chunk {hash} {len}\n"));
        }
        s.push_str("end\n");
        s.into_bytes()
    }

    fn decode(bytes: &[u8]) -> Option<Manifest> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        if lines.next()? != "squad-snapshot 1" {
            return None;
        }
        let lsn = lines.next()?.strip_prefix("lsn ")?.parse().ok()?;
        let size = lines.next()?.strip_prefix("size ")?.parse().ok()?;
        let mut chunks = Vec::new();
        let mut complete = false;
        for line in lines {
            if line == "end" {
                complete = true;
                break;
            }
            let rest = line.strip_prefix("chunk ")?;
            let (hash, len) = rest.split_once(' ')?;
            chunks.push((hash.to_string(), len.parse().ok()?));
        }
        (complete && chunks.iter().map(|c| c.1).sum::<u64>() == size).then_some(Manifest { lsn, size, chunks })
    }
}

/// A log object that continues the sequence.
pub(crate) struct WalObject {
    pub(crate) key: String,
    /// Epoch of the writer that uploaded it.
    pub(crate) epoch: u64,
    /// Records above this LSN are ignored: the next writer took over there.
    pub(crate) limit: u64,
}

/// What the object store holds, as far as it can be restored.
pub(crate) struct RemoteState {
    /// Epochs with an established base, ascending: `(epoch, base lsn)`.
    pub(crate) epochs: Vec<(u64, u64)>,
    /// Newest valid snapshot: `(epoch, lsn)`.
    pub(crate) snapshot: Option<(u64, u64)>,
    /// Log objects to replay, in order, starting after the snapshot (or
    /// after the LSN the listing was asked to continue from).
    pub(crate) wal: Vec<WalObject>,
    /// LSN the database reaches after replay.
    pub(crate) lsn: u64,
}

impl RemoteState {
    /// LSN up to which the records of `epoch` count, if it is established.
    pub(crate) fn limit(&self, epoch: u64) -> Option<u64> {
        let i = self.epochs.iter().position(|(e, _)| *e == epoch)?;
        Some(self.epochs.get(i + 1).map_or(u64::MAX, |(_, base)| *base))
    }

    /// The epoch whose writer produced the record at `lsn`.
    pub(crate) fn epoch_at(&self, lsn: u64) -> Option<u64> {
        self.epochs.iter().rev().find(|(_, base)| *base <= lsn).or(self.epochs.first()).map(|e| e.0)
    }

    /// True if a directory at `lsn` whose history ends in `epoch` holds a
    /// prefix of the history in the object store.
    pub(crate) fn contains(&self, epoch: u64, lsn: u64) -> bool {
        let base = self.epochs.iter().find(|(e, _)| *e == epoch).map(|e| e.1);
        matches!((base, self.limit(epoch)), (Some(base), Some(limit)) if base <= lsn && lsn <= limit)
    }
}

/// Highest claimed epoch.
pub(crate) fn max_claim(store: &dyn ObjectStore) -> Result<u64> {
    let claims = store.list("writer/claim-").map_err(remote_err)?;
    Ok(claims.iter().filter_map(|m| m.key.strip_prefix("writer/claim-")?.parse::<u64>().ok()).max().unwrap_or(0))
}

pub(crate) fn read_remote(store: &dyn ObjectStore) -> Result<RemoteState> {
    read_remote_from(store, None)
}

/// Reads the remote state. With `from`, the log objects returned are the
/// ones that continue a copy standing at that LSN instead of the ones
/// that continue the newest snapshot.
pub(crate) fn read_remote_from(store: &dyn ObjectStore, from: Option<u64>) -> Result<RemoteState> {
    let mut epochs = Vec::new();
    for meta in store.list("writer/base-").map_err(remote_err)? {
        if let Some(e) = meta.key.strip_prefix("writer/base-").and_then(|s| s.parse::<u64>().ok()) {
            let base = store.get(&meta.key).map_err(remote_err)?;
            let base = base.and_then(|b| String::from_utf8(b).ok()).and_then(|s| s.trim().parse::<u64>().ok());
            if let Some(base) = base {
                epochs.push((e, base));
            }
        }
    }
    epochs.sort_unstable();
    // Records and snapshots of an epoch are valid up to the next base.
    let limit = |epoch: u64| -> Option<u64> {
        let i = epochs.iter().position(|(e, _)| *e == epoch)?;
        Some(epochs.get(i + 1).map_or(u64::MAX, |(_, base)| *base))
    };

    let mut snapshot: Option<(u64, u64)> = None;
    for meta in store.list("snapshot/").map_err(remote_err)? {
        let Some((epoch, lsn)) = meta.key.strip_prefix("snapshot/").and_then(pair) else { continue };
        if limit(epoch).is_some_and(|lim| lsn <= lim) && snapshot.is_none_or(|(e, l)| (lsn, epoch) > (l, e)) {
            snapshot = Some((epoch, lsn));
        }
    }

    let mut lsn = from.unwrap_or(snapshot.map_or(0, |s| s.1));
    let mut wal = Vec::new();
    for (epoch, _) in &epochs {
        let lim = limit(*epoch).unwrap_or(u64::MAX);
        let prefix = wal_prefix(*epoch);
        let mut objects: Vec<(String, u64, u64)> = store
            .list(&prefix)
            .map_err(remote_err)?
            .into_iter()
            .filter_map(|m| pair(&m.key[prefix.len()..]).map(|(a, b)| (m.key, a, b)))
            .collect();
        objects.sort_by_key(|o| (o.1, o.2));
        for (key, first, last) in objects {
            // Usable if it continues the sequence without a gap.
            if first <= lsn + 1 && last > lsn && lsn < lim {
                lsn = last.min(lim);
                wal.push(WalObject { key, epoch: *epoch, limit: lim });
            }
        }
    }
    Ok(RemoteState { epochs, snapshot, wal, lsn })
}

fn data_file_lsn(first_page: &[u8]) -> Result<u64> {
    if first_page.len() < PAGE_SIZE {
        return Err(StorageError::Corruption("snapshot image is shorter than one page".into()));
    }
    Ok(page::rd64(first_page[..PAGE_SIZE].try_into().unwrap(), HDR + 24))
}

/// Downloads the remote state into the empty directory `dir` and returns
/// the opened engine, positioned at the last restorable commit.
pub(crate) fn materialize(
    store: &dyn ObjectStore,
    state: &RemoteState,
    dir: &Path,
    opts: &DiskOptions,
) -> Result<DiskEngine> {
    let vfs = &opts.vfs;
    vfs.create_dir_all(dir)?;
    if let Some((epoch, lsn)) = state.snapshot {
        let key = snapshot_key(epoch, lsn);
        let bytes = store.get(&key).map_err(remote_err)?.ok_or_else(|| vanished(&key))?;
        let manifest = Manifest::decode(&bytes)
            .ok_or_else(|| StorageError::Corruption(format!("snapshot manifest {key} is damaged")))?;
        let tmp = dir.join("data.db.restore");
        let file = vfs.create(&tmp)?;
        let mut off = 0u64;
        for (i, (hash, len)) in manifest.chunks.iter().enumerate() {
            let chunk = store.get(&chunk_key(hash)).map_err(remote_err)?.ok_or_else(|| vanished(&chunk_key(hash)))?;
            if chunk.len() as u64 != *len || sha256_hex(&chunk) != *hash {
                return Err(StorageError::Corruption(format!("chunk {hash} of snapshot {key} is damaged")));
            }
            if i == 0 && data_file_lsn(&chunk)? != manifest.lsn {
                return Err(StorageError::Corruption(format!("snapshot {key} does not match its manifest")));
            }
            file.write_at(&chunk, off)?;
            off += len;
        }
        file.sync()?;
        vfs.rename(&tmp, &dir.join(DATA_FILE))?;
        vfs.sync_dir(dir)?;
    }
    let engine = DiskEngine::open(dir, opts.clone())?;
    for WalObject { key, limit, .. } in &state.wal {
        let bytes = store.get(key).map_err(remote_err)?.ok_or_else(|| vanished(key))?;
        let mut off = 0;
        while let Some((lsn, ops, next)) = wal::read_record(&bytes, off) {
            off = next;
            let have = engine.last_lsn();
            if lsn <= have || lsn > *limit {
                continue;
            }
            if lsn != have + 1 {
                return Err(StorageError::Corruption(format!(
                    "log object {key} does not continue at LSN {}",
                    have + 1
                )));
            }
            let batch = WriteBatch { ops: ops.iter().map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec))).collect() };
            let applied = engine.write(batch)?;
            if applied != lsn {
                return Err(StorageError::Corruption(format!(
                    "replay of {key} produced LSN {applied}, expected {lsn}"
                )));
            }
        }
        if off != bytes.len() {
            return Err(StorageError::Corruption(format!("log object {key} is damaged")));
        }
    }
    if engine.last_lsn() != state.lsn {
        return Err(StorageError::Corruption(format!(
            "restore reached LSN {} but the object store promises {}",
            engine.last_lsn(),
            state.lsn
        )));
    }
    engine.sync(engine.last_lsn())?;
    Ok(engine)
}

pub(crate) const VANISHED: &str = "was removed while it was being restored";

/// An object listed a moment ago is gone: a live writer uploaded a newer
/// snapshot and cleaned up. The restore starts over from the new state.
fn vanished(key: &str) -> StorageError {
    StorageError::Remote(format!("{key} {VANISHED}"))
}

/// Restores into `dir`, starting over when a concurrent writer replaces
/// the snapshot being read.
pub(crate) fn materialize_latest(
    store: &dyn ObjectStore,
    dir: &Path,
    opts: &DiskOptions,
) -> Result<(DiskEngine, RemoteState)> {
    let mut attempt = 0;
    loop {
        let state = read_remote(store)?;
        match materialize(store, &state, dir, opts) {
            Ok(engine) => return Ok((engine, state)),
            Err(StorageError::Remote(m)) if m.ends_with(VANISHED) && attempt < 8 => {
                attempt += 1;
                wipe(opts, dir)?;
            }
            Err(e) => return Err(e),
        }
    }
}

pub(crate) fn dir_is_empty(opts: &DiskOptions, dir: &Path) -> Result<bool> {
    Ok(!opts.vfs.exists(&dir.join(DATA_FILE))?)
}

/// Removes a database from a local directory.
pub(crate) fn wipe(opts: &DiskOptions, dir: &Path) -> Result<()> {
    let vfs = &opts.vfs;
    for name in vfs.list(&dir.join("wal"))? {
        vfs.remove(&dir.join("wal").join(name))?;
    }
    for name in [DATA_FILE, "data.db.tmp", "data.db.restore", "checkpoint.journal", MARKER_FILE, REPLICA_FILE] {
        if vfs.exists(&dir.join(name))? {
            vfs.remove(&dir.join(name))?;
        }
    }
    vfs.sync_dir(dir)?;
    Ok(())
}

/// Restores the database stored in `store` into the empty or missing
/// directory `dir`, without becoming its writer. The directory can then
/// be opened as an ordinary local database.
pub fn restore_from(store: &dyn ObjectStore, dir: &Path, opts: DiskOptions) -> Result<u64> {
    opts.vfs.create_dir_all(dir)?;
    if !dir_is_empty(&opts, dir)? {
        return Err(StorageError::Invalid(format!("restore target {} already holds a database", dir.display())));
    }
    if read_remote(store)?.epochs.is_empty() {
        return Err(StorageError::Invalid("the object store holds no database".into()));
    }
    let (engine, _) = materialize_latest(store, dir, &opts)?;
    Ok(engine.last_lsn())
}

#[derive(Default)]
struct Pending {
    /// Encoded records `first..=last`, not yet uploaded.
    buf: Vec<u8>,
    first: u64,
    last: u64,
    /// Records were dropped; a snapshot must cover them.
    overflowed: bool,
}

#[derive(Default)]
pub(crate) struct SnapshotState {
    lsn: u64,
    /// Chunks referenced by the newest manifest.
    chunks: HashSet<String>,
}

impl SnapshotState {
    /// What the newest snapshot named by `state` consists of.
    pub(crate) fn load(store: &dyn ObjectStore, state: &RemoteState) -> Result<SnapshotState> {
        let mut snapshot = SnapshotState::default();
        if let Some((e, lsn)) = state.snapshot {
            if let Some(m) = store.get(&snapshot_key(e, lsn)).map_err(remote_err)?.and_then(|b| Manifest::decode(&b)) {
                snapshot.lsn = m.lsn;
                snapshot.chunks = m.chunks.into_iter().map(|c| c.0).collect();
            }
        }
        Ok(snapshot)
    }
}

/// Claims the next free writer epoch.
pub(crate) fn claim_epoch(store: &dyn ObjectStore) -> Result<u64> {
    let mut epoch = max_claim(store)? + 1;
    while !store.put_if_absent(&claim_key(epoch), b"squad writer\n").map_err(remote_err)? {
        epoch += 1;
    }
    Ok(epoch)
}

/// Epoch in which the history of the database in `dir` ends, if the
/// directory was ever tied to an object store.
pub(crate) fn read_marker(opts: &DiskOptions, dir: &Path) -> Result<Option<u64>> {
    let vfs = &opts.vfs;
    let marker = dir.join(MARKER_FILE);
    if !vfs.exists(&marker)? {
        return Ok(None);
    }
    let f = vfs.open(&marker)?;
    let mut buf = vec![0u8; f.size()? as usize];
    let n = f.read_at(&mut buf, 0)?;
    Ok(String::from_utf8_lossy(&buf[..n]).trim().parse::<u64>().ok())
}

pub(crate) fn write_marker(opts: &DiskOptions, dir: &Path, epoch: u64) -> Result<()> {
    let vfs = &opts.vfs;
    let f = vfs.create(&dir.join(MARKER_FILE))?;
    f.write_at(epoch.to_string().as_bytes(), 0)?;
    f.sync()?;
    vfs.sync_dir(dir)?;
    Ok(())
}

struct Shared {
    disk: DiskEngine,
    store: Arc<dyn ObjectStore>,
    config: RemoteConfig,
    dir: PathBuf,
    opts: DiskOptions,
    epoch: u64,
    /// Orders local writes and their entry into `pending`.
    write_lock: Mutex<()>,
    pending: Mutex<Pending>,
    /// One uploader at a time; waiting here is what batches commits.
    ship_lock: Mutex<()>,
    shipped_lsn: AtomicU64,
    wal_bytes_since_snapshot: AtomicU64,
    snapshot: Mutex<SnapshotState>,
    fenced: AtomicBool,
    stop: AtomicBool,
    stats: Mutex<RemoteStats>,
}

impl Shared {
    fn fenced_error(&self) -> StorageError {
        StorageError::Remote(format!(
            "another writer took over this database in the object store (this writer has epoch {})",
            self.epoch
        ))
    }

    /// Fails once a later writer has claimed the database.
    fn check_fence(&self) -> Result<()> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(self.fenced_error());
        }
        if self.store.head(&claim_key(self.epoch + 1)).map_err(remote_err)?.is_some() {
            self.fenced.store(true, Ordering::Release);
            return Err(self.fenced_error());
        }
        Ok(())
    }

    /// Uploads all pending log records as one object.
    fn ship(&self) -> Result<()> {
        let _uploader = self.ship_lock.lock();
        let (buf, first, last) = {
            let mut p = self.pending.lock();
            if p.buf.is_empty() {
                return Ok(());
            }
            (std::mem::take(&mut p.buf), p.first, p.last)
        };
        let result = (|| {
            self.check_fence()?;
            self.store.put(&wal_key(self.epoch, first, last), &buf).map_err(remote_err)?;
            // A commit must not be acknowledged if a new writer appeared
            // while the upload was under way: that writer may have fixed
            // its starting point before this object existed.
            self.check_fence()
        })();
        if let Err(e) = result {
            // Keep the records for the next attempt, ahead of newer ones.
            let mut p = self.pending.lock();
            if !p.overflowed {
                let newer = std::mem::replace(&mut p.buf, buf);
                p.buf.extend_from_slice(&newer);
                p.first = first;
                if newer.is_empty() {
                    p.last = last;
                }
            }
            return Err(e);
        }
        self.shipped_lsn.fetch_max(last, Ordering::AcqRel);
        self.wal_bytes_since_snapshot.fetch_add(buf.len() as u64, Ordering::Relaxed);
        let mut stats = self.stats.lock();
        stats.wal_objects += 1;
        stats.wal_bytes += buf.len() as u64;
        Ok(())
    }

    /// Uploads a full image of the data file and removes what it
    /// supersedes.
    fn snapshot(&self) -> Result<()> {
        let mut snap = self.snapshot.lock();
        self.check_fence()?;
        let vfs = &self.opts.vfs;
        let tmp = self.dir.join(SNAPSHOT_TMP);
        let cleanup = || -> Result<()> {
            for name in vfs.list(&tmp)? {
                vfs.remove(&tmp.join(name))?;
            }
            Ok(())
        };
        cleanup()?;
        // A consistent copy of the data file; writers carry on meanwhile.
        self.disk.backup(&tmp)?;
        let result = (|| -> Result<Manifest> {
            let file = vfs.open(&tmp.join(DATA_FILE))?;
            let size = file.size()?;
            let mut manifest = Manifest { lsn: 0, size, chunks: Vec::new() };
            let mut buf = vec![0u8; CHUNK];
            let mut off = 0u64;
            let (mut uploaded, mut uploaded_bytes, mut reused) = (0u64, 0u64, 0u64);
            while off < size {
                let want = CHUNK.min((size - off) as usize);
                let mut got = 0;
                while got < want {
                    let n = file.read_at(&mut buf[got..want], off + got as u64)?;
                    if n == 0 {
                        return Err(StorageError::Corruption("data file image ended early".into()));
                    }
                    got += n;
                }
                let piece = &buf[..want];
                if off == 0 {
                    manifest.lsn = data_file_lsn(piece)?;
                }
                let hash = sha256_hex(piece);
                if snap.chunks.contains(&hash) {
                    reused += 1;
                } else {
                    self.store.put(&chunk_key(&hash), piece).map_err(remote_err)?;
                    uploaded += 1;
                    uploaded_bytes += want as u64;
                }
                manifest.chunks.push((hash, want as u64));
                off += want as u64;
            }
            // The manifest goes last: it makes the snapshot exist.
            self.check_fence()?;
            self.store.put(&snapshot_key(self.epoch, manifest.lsn), &manifest.encode()).map_err(remote_err)?;
            let mut stats = self.stats.lock();
            stats.snapshots += 1;
            stats.snapshot_lsn = manifest.lsn;
            stats.chunks_uploaded += uploaded;
            stats.chunk_bytes_uploaded += uploaded_bytes;
            stats.chunks_reused += reused;
            Ok(manifest)
        })();
        let _ = cleanup();
        let manifest = result?;
        let previous = snap.lsn;
        snap.lsn = manifest.lsn;
        snap.chunks = manifest.chunks.iter().map(|c| c.0.clone()).collect();
        self.wal_bytes_since_snapshot.store(0, Ordering::Relaxed);
        self.pending.lock().overflowed = false;
        self.shipped_lsn.fetch_max(manifest.lsn, Ordering::AcqRel);
        // Failing to clean up only leaves garbage behind.
        let _ = self.collect_garbage(&manifest, previous);
        Ok(())
    }

    /// Deletes what the newest snapshot makes unnecessary: older manifests
    /// first, then unreferenced chunks and old log objects.
    ///
    /// Log objects are kept one snapshot longer than a restore needs them
    /// (up to `retain_from`, the LSN of the snapshot before the newest):
    /// a read replica that was following the log when the snapshot was
    /// taken can then finish reading it instead of downloading the image.
    fn collect_garbage(&self, newest: &Manifest, retain_from: u64) -> Result<()> {
        self.check_fence()?;
        let keep = snapshot_key(self.epoch, newest.lsn);
        for meta in self.store.list("snapshot/").map_err(remote_err)? {
            if meta.key != keep {
                self.store.delete(&meta.key).map_err(remote_err)?;
            }
        }
        let live: HashSet<&str> = newest.chunks.iter().map(|c| c.0.as_str()).collect();
        for meta in self.store.list("chunk/").map_err(remote_err)? {
            if !live.contains(&meta.key["chunk/".len()..]) {
                self.store.delete(&meta.key).map_err(remote_err)?;
            }
        }
        for meta in self.store.list("wal/").map_err(remote_err)? {
            let horizon = retain_from.min(newest.lsn);
            let covered = meta.key.rsplit('/').next().and_then(pair).is_some_and(|(_, last)| last <= horizon);
            if covered {
                self.store.delete(&meta.key).map_err(remote_err)?;
            }
        }
        Ok(())
    }

    fn background(&self) {
        if self.fenced.load(Ordering::Acquire) {
            return;
        }
        let _ = self.ship();
        let due = self.wal_bytes_since_snapshot.load(Ordering::Relaxed) >= self.config.snapshot_wal_bytes
            || self.pending.lock().overflowed;
        if due {
            let _ = self.snapshot();
        }
    }
}

fn worker(weak: Weak<Shared>, interval: Duration) {
    let mut last = Instant::now();
    loop {
        thread::park_timeout(interval.min(Duration::from_millis(50)));
        let Some(shared) = weak.upgrade() else { return };
        if shared.stop.load(Ordering::Acquire) {
            return;
        }
        if last.elapsed() >= interval {
            last = Instant::now();
            shared.background();
        }
    }
}

/// A [`DiskEngine`] whose log and checkpoints are also kept in an object
/// store. See the module documentation.
pub struct TieredEngine {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

impl TieredEngine {
    /// Opens the database in `dir`, restoring it from the object store if
    /// the directory is empty, and becomes the writer for the remote copy.
    pub fn open(dir: &Path, opts: DiskOptions, config: RemoteConfig) -> Result<TieredEngine> {
        let store = config.store.clone();
        let vfs = opts.vfs.clone();
        vfs.create_dir_all(dir)?;

        // Claim the next epoch first: everything a previous writer uploads
        // from now on either is visible to the listing below or fails that
        // writer's fence check.
        let epoch = claim_epoch(&*store)?;
        let mut state = read_remote(&*store)?;
        let remote_epoch = state.epochs.last().map(|e| e.0);
        let local_epoch = read_marker(&opts, dir)?;

        let mut needs_snapshot = false;
        let disk = if dir_is_empty(&opts, dir)? {
            wipe(&opts, dir)?;
            let (disk, latest) = materialize_latest(&*store, dir, &opts)?;
            state = latest;
            disk
        } else {
            let disk = DiskEngine::open(dir, opts.clone())?;
            let local_lsn = disk.last_lsn();
            let same_lineage = remote_epoch.is_none() || local_epoch == remote_epoch;
            if same_lineage && local_lsn >= state.lsn {
                // The local copy has everything the remote has, and
                // possibly commits that were never shipped.
                needs_snapshot = local_lsn > state.lsn || state.snapshot.is_none();
                disk
            } else if config.restore {
                drop(disk);
                wipe(&opts, dir)?;
                let (disk, latest) = materialize_latest(&*store, dir, &opts)?;
                state = latest;
                disk
            } else {
                let why = if same_lineage {
                    format!("the object store is ahead (LSN {} against local {local_lsn})", state.lsn)
                } else {
                    "the local directory does not descend from the latest writer of the object store".to_string()
                };
                return Err(StorageError::Invalid(format!(
                    "{why}; open with `restore` to replace the local directory with the remote state"
                )));
            }
        };
        if state.snapshot.is_none() && disk.last_lsn() > 0 {
            needs_snapshot = true;
        }

        // From here on this writer's uploads count.
        store.put(&base_key(epoch), disk.last_lsn().to_string().as_bytes()).map_err(remote_err)?;
        write_marker(&opts, dir, epoch)?;
        if vfs.exists(&dir.join(REPLICA_FILE))? {
            // The directory was a replica's working copy until now.
            vfs.remove(&dir.join(REPLICA_FILE))?;
            vfs.sync_dir(dir)?;
        }
        let snapshot = SnapshotState::load(&*store, &state)?;
        let shipped = if needs_snapshot { state.lsn.min(disk.last_lsn()) } else { disk.last_lsn() };
        let engine = TieredEngine::start(disk, dir, opts, config, epoch, snapshot, shipped)?;
        if needs_snapshot {
            engine.shared.snapshot()?;
        }
        Ok(engine)
    }

    /// Starts shipping for a local engine whose epoch is established in
    /// the object store.
    pub(crate) fn start(
        disk: DiskEngine,
        dir: &Path,
        opts: DiskOptions,
        config: RemoteConfig,
        epoch: u64,
        snapshot: SnapshotState,
        shipped: u64,
    ) -> Result<TieredEngine> {
        let shared = Arc::new(Shared {
            disk,
            store: config.store.clone(),
            dir: dir.to_path_buf(),
            opts,
            epoch,
            write_lock: Mutex::new(()),
            pending: Mutex::new(Pending::default()),
            ship_lock: Mutex::new(()),
            shipped_lsn: AtomicU64::new(shipped),
            wal_bytes_since_snapshot: AtomicU64::new(0),
            snapshot: Mutex::new(snapshot),
            fenced: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            stats: Mutex::new(RemoteStats { epoch, ..RemoteStats::default() }),
            config,
        });
        let interval = match shared.config.mode {
            RemoteSync::Async { interval } => interval.max(Duration::from_millis(1)),
            // Only snapshots happen in the background.
            RemoteSync::Sync => Duration::from_millis(200),
        };
        let weak = Arc::downgrade(&shared);
        let worker = thread::Builder::new()
            .name("squad-remote".into())
            .spawn(move || worker(weak, interval))
            .map_err(StorageError::Io)?;
        Ok(TieredEngine { shared, worker: Some(worker) })
    }

    /// Uploads all log records written so far. In `Async` mode this is
    /// what the background thread does periodically.
    pub fn flush_remote(&self) -> Result<()> {
        self.shared.disk.sync(self.shared.disk.last_lsn())?;
        self.shared.ship()
    }

    /// Uploads a snapshot now.
    pub fn snapshot_remote(&self) -> Result<()> {
        self.flush_remote()?;
        self.shared.snapshot()
    }

    pub fn remote_stats(&self) -> RemoteStats {
        let mut stats = self.shared.stats.lock().clone();
        stats.shipped_lsn = self.shared.shipped_lsn.load(Ordering::Acquire);
        stats
    }

    /// The wrapped local engine.
    pub fn disk(&self) -> &DiskEngine {
        &self.shared.disk
    }
}

impl Drop for TieredEngine {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(h) = self.worker.take() {
            h.thread().unpark();
            let _ = h.join();
        }
        // Best effort: leave nothing unshipped behind on a clean shutdown.
        if !self.shared.fenced.load(Ordering::Acquire) {
            let _ = self.flush_remote();
        }
    }
}

impl Engine for TieredEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.shared.disk.get(key)
    }

    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.shared.disk.scan(lo, hi, reverse, limit)
    }

    fn write(&self, batch: WriteBatch) -> Result<u64> {
        let s = &*self.shared;
        if s.fenced.load(Ordering::Acquire) {
            return Err(s.fenced_error());
        }
        if batch.is_empty() {
            return s.disk.write(batch);
        }
        let _order = s.write_lock.lock();
        // All writes pass through here, so the next LSN is known.
        let lsn = s.disk.last_lsn() + 1;
        let mut record = Vec::new();
        wal::encode_record(&mut record, lsn, &batch);
        let applied = s.disk.write(batch)?;
        debug_assert_eq!(applied, lsn);
        let mut p = s.pending.lock();
        if p.buf.is_empty() {
            p.first = applied;
        }
        p.last = applied;
        p.buf.extend_from_slice(&record);
        if p.buf.len() > MAX_PENDING && matches!(s.config.mode, RemoteSync::Async { .. }) {
            p.buf = Vec::new();
            p.overflowed = true;
        }
        Ok(applied)
    }

    fn sync(&self, lsn: u64) -> Result<()> {
        let s = &*self.shared;
        s.disk.sync(lsn)?;
        if s.config.mode == RemoteSync::Sync && s.shipped_lsn.load(Ordering::Acquire) < lsn {
            s.ship()?;
            if s.shipped_lsn.load(Ordering::Acquire) < lsn {
                return Err(StorageError::Remote(format!("commit at LSN {lsn} could not be stored remotely")));
            }
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        self.shared.disk.checkpoint()?;
        self.snapshot_remote()
    }

    fn backup(&self, dest: &Path) -> Result<()> {
        self.shared.disk.backup(dest)
    }

    fn verify(&self) -> Result<VerifyReport> {
        self.shared.disk.verify()
    }

    fn stats(&self) -> EngineStats {
        self.shared.disk.stats()
    }
}
