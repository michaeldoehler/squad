//! Tiering to an object store.
//!
//! [`TieredEngine`] wraps a [`DiskEngine`] and keeps a complete,
//! restorable copy of the database in an [`ObjectStore`]. The local
//! directory becomes a working copy; the object store is the durable
//! home from which a lost machine is rebuilt.
//!
//! # Layout in the object store (remote format 2)
//!
//! ```text
//! writer/claim-<epoch>-<id>          one per attempt to become the writer
//! writer/base-<epoch>-<id>           where the writer of that claim took over, and after whom
//! wal/<epoch>-<id>/<first>-<last>    log records first..=last, in WAL record format
//! snapshot/<epoch>-<id>-<lsn>        manifest of a full image of the data file at <lsn>
//! chunk/<sha256>                     content-addressed pieces of data file images
//! probe/<id>                         short-lived, written once at open (see below)
//! ```
//!
//! Numbers are zero-padded to 20 digits so that keys sort numerically;
//! `<id>` is 16 random hex digits chosen by each claimant. Every object is
//! written once and never modified, which makes uploads idempotent: a
//! retried or duplicated upload writes the same bytes under the same key.
//!
//! A base object holds `lsn <n>` (the LSN the writer started from) and
//! `prev <epoch>-<id>` (the writer whose history it continues), or
//! `prev -` for the first writer.
//!
//! # Restore
//!
//! The established writers (those with a base object) form a chain: it
//! starts at the highest epoch and follows the `prev` links. The newest
//! snapshot of a writer in the chain is downloaded and the log records
//! after it are replayed in LSN order. A writer's records count only up
//! to the LSN at which its successor in the chain took over, so anything
//! a superseded writer still uploads is ignored, and so is everything of
//! a writer that is not in the chain.
//!
//! # Fencing
//!
//! Safety needs only two properties of the store: a `PUT` is atomic and
//! durable when it returns, and a `LIST` that starts after a `PUT` has
//! returned shows the object (and a `GET` returns it). In other words,
//! `PUT`, `GET` and `LIST` are linearizable, which AWS S3, Garage, MinIO
//! and local directories provide. Conditional writes (`If-None-Match`)
//! are *not* needed: some S3-compatible stores, Garage among them, accept
//! them and silently overwrite. See [`claim_epoch`] for the claim
//! protocol and the argument why it never yields two writers for one
//! epoch, and `Shared::ship` for why no acknowledged commit is lost
//! when a new writer takes over.
//!
//! When a writer opens it checks whether the store enforces
//! `If-None-Match: *` and reports the answer in
//! [`RemoteStats::conditional_writes`]. The answer is informational.

use std::collections::{BTreeMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex};
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
/// How often a claimant tries again after losing against a concurrent
/// one before giving up.
const CLAIM_ATTEMPTS: u32 = 20;

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
    /// Log uploads that may be under way at the same time (default 1).
    /// With more than one, commits that arrive while an upload is in
    /// flight go out in a second upload instead of waiting for the first
    /// to finish. That helps on stores whose latency is high and which
    /// handle concurrent requests well; on a single-node Garage it was
    /// slower, because the groups get smaller and the store does not
    /// speed up (see `docs/cloud-storage.md`).
    pub upload_concurrency: usize,
}

impl RemoteConfig {
    pub fn new(store: Arc<dyn ObjectStore>, mode: RemoteSync) -> RemoteConfig {
        RemoteConfig { store, mode, restore: false, snapshot_wal_bytes: 64 << 20, upload_concurrency: 1 }
    }
}

impl std::fmt::Debug for RemoteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("mode", &self.mode)
            .field("restore", &self.restore)
            .field("snapshot_wal_bytes", &self.snapshot_wal_bytes)
            .field("upload_concurrency", &self.upload_concurrency)
            .finish_non_exhaustive()
    }
}

/// Counters of the remote copy.
#[derive(Debug, Clone, Default)]
pub struct RemoteStats {
    /// Epoch of this writer.
    pub epoch: u64,
    /// Random id of this writer's claim; together with the epoch it names
    /// the writer's objects in the store.
    pub claim_id: String,
    /// Whether the store refused a second `PUT` with `If-None-Match: *`
    /// to the same key when this writer opened; `None` if the probe
    /// failed. Fencing does not depend on it.
    pub conditional_writes: Option<bool>,
    /// Highest LSN known to be in the object store.
    pub shipped_lsn: u64,
    /// LSN of the newest commit in the local log. `local_lsn -
    /// shipped_lsn` is how far the object store is behind.
    pub local_lsn: u64,
    /// True once this writer saw a newer claim: another writer took over,
    /// and every further write fails.
    pub fenced: bool,
    /// LSN of the newest uploaded snapshot.
    pub snapshot_lsn: u64,
    pub wal_objects: u64,
    pub wal_bytes: u64,
    /// Uploads of log objects that carried more than one commit.
    pub grouped_uploads: u64,
    pub snapshots: u64,
    pub chunks_uploaded: u64,
    pub chunk_bytes_uploaded: u64,
    /// Chunks that were already present and not uploaded again.
    pub chunks_reused: u64,
}

pub(crate) fn remote_err(e: squad_object::Error) -> StorageError {
    StorageError::Remote(e.to_string())
}

/// One attempt to become the writer: an epoch and the claimant's random
/// id. Only claims that win (see [`claim_epoch`]) ever write anything
/// but their claim object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Claim {
    pub(crate) epoch: u64,
    pub(crate) id: String,
}

impl Claim {
    fn name(&self) -> String {
        format!("{:020}-{}", self.epoch, self.id)
    }

    /// Parses `<epoch>-<id>` at the start of `s`, returning the rest.
    fn parse_prefix(s: &str) -> Option<(Claim, &str)> {
        let epoch = s.get(..20)?;
        let rest = s.get(20..)?.strip_prefix('-')?;
        let id = rest.get(..16)?;
        if !epoch.bytes().all(|b| b.is_ascii_digit()) || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        Some((Claim { epoch: epoch.parse().ok()?, id: id.to_string() }, &rest[16..]))
    }

    fn parse(s: &str) -> Option<Claim> {
        match Claim::parse_prefix(s)? {
            (claim, "") => Some(claim),
            _ => None,
        }
    }
}

const CLAIM_PREFIX: &str = "writer/claim-";
const BASE_PREFIX: &str = "writer/base-";

pub(crate) fn claim_key(claim: &Claim) -> String {
    format!("{CLAIM_PREFIX}{}", claim.name())
}
/// A key that sorts before every claim of `epoch` and after every claim
/// of a lower epoch.
fn claims_from(epoch: u64) -> String {
    format!("{CLAIM_PREFIX}{epoch:020}")
}
pub(crate) fn base_key(claim: &Claim) -> String {
    format!("{BASE_PREFIX}{}", claim.name())
}
fn wal_prefix(claim: &Claim) -> String {
    format!("wal/{}/", claim.name())
}
fn wal_key(claim: &Claim, first: u64, last: u64) -> String {
    format!("wal/{}/{first:020}-{last:020}", claim.name())
}
fn snapshot_key(claim: &Claim, lsn: u64) -> String {
    format!("snapshot/{}-{lsn:020}", claim.name())
}
fn chunk_key(hash: &str) -> String {
    format!("chunk/{hash}")
}

fn pair(name: &str) -> Option<(u64, u64)> {
    let (a, b) = name.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn parse_snapshot_key(key: &str) -> Option<(Claim, u64)> {
    let (claim, rest) = Claim::parse_prefix(key.strip_prefix("snapshot/")?)?;
    Some((claim, rest.strip_prefix('-')?.parse().ok()?))
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
}

/// 64 bits that differ between calls, processes and machines. Not
/// cryptographic; used for claim ids and backoff jitter.
fn random_u64() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    // `RandomState` is seeded from the operating system's random source.
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u128(SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
    h.write_u32(std::process::id());
    h.finish()
}

fn new_claim_id() -> String {
    format!("{:016x}", random_u64())
}

/// Sleeps a random time below `10ms * 2^attempt` (at most one second):
/// exponential backoff with full jitter.
fn backoff(attempt: u32) {
    let cap = (10u64 << attempt.min(7)).min(1000);
    thread::sleep(Duration::from_micros(random_u64() % (cap * 1000) + 1));
}

/// The error for an object store written by squad before remote format 2.
fn old_format() -> StorageError {
    StorageError::Invalid(
        "the object store holds a database in remote format 1, written by an earlier squad version; \
         restore it with that version (`squad restore`) and open the restored directory with a fresh remote"
            .into(),
    )
}

/// True for the claim and base keys of remote format 1 (`writer/claim-<epoch>`).
fn is_format_1(key: &str) -> bool {
    let rest = key.strip_prefix(CLAIM_PREFIX).or_else(|| key.strip_prefix(BASE_PREFIX));
    rest.is_some_and(|r| !r.is_empty() && r.bytes().all(|b| b.is_ascii_digit()))
}

/// All claims in the store, or those from epoch `from` on.
fn list_claims(store: &dyn ObjectStore, from: Option<u64>) -> Result<Vec<Claim>> {
    let listed = match from {
        Some(epoch) => store.list_after(CLAIM_PREFIX, &claims_from(epoch)),
        None => store.list(CLAIM_PREFIX),
    }
    .map_err(remote_err)?;
    let mut claims = Vec::with_capacity(listed.len());
    for meta in listed {
        if is_format_1(&meta.key) {
            return Err(old_format());
        }
        if let Some(claim) = meta.key.strip_prefix(CLAIM_PREFIX).and_then(Claim::parse) {
            claims.push(claim);
        }
    }
    Ok(claims)
}

/// Highest claimed epoch from `from` on (0 if there is none).
pub(crate) fn max_claim(store: &dyn ObjectStore, from: u64) -> Result<u64> {
    Ok(list_claims(store, Some(from))?.iter().map(|c| c.epoch).max().unwrap_or(0))
}

/// True if a claim with an epoch above `claim`'s exists.
pub(crate) fn superseded(store: &dyn ObjectStore, claim: &Claim) -> Result<bool> {
    let later = store.list_after(CLAIM_PREFIX, &claim_key(claim)).map_err(remote_err)?;
    Ok(later
        .iter()
        .filter_map(|m| m.key.strip_prefix(CLAIM_PREFIX).and_then(Claim::parse))
        .any(|c| c.epoch > claim.epoch))
}

/// Claims the next writer epoch.
///
/// # Protocol
///
/// 1. List the claims and pick `N`, one above the highest epoch seen
///    (and above every epoch seen in earlier attempts).
/// 2. `PUT writer/claim-<N>-<id>` with a fresh random id. Claim keys are
///    unique, so no two claimants ever write the same key.
/// 3. List the claims again. If any claim other than our own has an
///    epoch `>= N`, we lost: delete our claim (best effort), back off with
///    jitter and start over at step 1. Otherwise we won epoch `N`.
///
/// A winner never deletes its claim. Only a winner writes a base object
/// and log or snapshot objects, all named with its claim.
///
/// # Why there are never two winners for one epoch
///
/// Write `P(c)` for the `PUT` of claim `c` and `L(c)` for the `LIST` in
/// step 3 that follows it; `P(c)` returns before `L(c)` starts. Let `x`
/// and `y` be two claims for the same epoch `N` and assume both won.
/// Their `PUT`s are linearizable, so one of them took effect first, say
/// `P(x)` before `P(y)`. Then `L(y)`, which starts after `P(y)`, sees
/// `x` (a winner's claim is never deleted, and `x` won). `x` is a claim
/// other than `y` with epoch `N >= N`, so `y` lost. Contradiction.
///
/// # Why winners are ordered by epoch
///
/// If `x` won epoch `N` and `y` won epoch `M > N`, then `L(x)` started
/// before `P(y)` took effect: otherwise `L(x)` would have seen `y`, an
/// epoch `>= N`, and `x` would have lost. So a higher epoch is always
/// claimed after every lower winner finished claiming, and a claimant
/// that read the state *after* its own claim (as [`TieredEngine::open`]
/// and promotion do) reads everything a lower winner did before it
/// claimed. The old writer's side of the argument is at `Shared::ship`.
///
/// Two claimants for the same epoch may both lose; each then retries
/// above everything it saw, after a random delay. A losing claim also
/// fences the current writer (any claim above its epoch does), which is
/// what a takeover attempt should do anyway.
pub(crate) fn claim_epoch(store: &dyn ObjectStore) -> Result<Claim> {
    let mut floor = 0;
    for attempt in 0..CLAIM_ATTEMPTS {
        let seen = list_claims(store, None)?;
        let epoch = seen.iter().map(|c| c.epoch).max().unwrap_or(0).max(floor) + 1;
        let mine = Claim { epoch, id: new_claim_id() };
        store.put(&claim_key(&mine), b"squad writer\n").map_err(remote_err)?;
        let rivals: Vec<Claim> = list_claims(store, Some(epoch))?.into_iter().filter(|c| *c != mine).collect();
        if rivals.is_empty() {
            return Ok(mine);
        }
        let _ = store.delete(&claim_key(&mine));
        floor = rivals.iter().map(|c| c.epoch).max().unwrap_or(epoch).max(epoch);
        backoff(attempt);
    }
    Err(StorageError::Remote(format!(
        "could not claim the database: lost {CLAIM_ATTEMPTS} times against concurrent claimants"
    )))
}

/// Asks whether the store enforces `If-None-Match: *`: puts a probe
/// object twice with the condition and removes it. `None` if a request
/// failed or the answer is not clear.
pub(crate) fn probe_conditional_writes(store: &dyn ObjectStore) -> Option<bool> {
    let key = format!("probe/{}", new_claim_id());
    let first = store.put_if_absent(&key, b"first");
    let second = store.put_if_absent(&key, b"second");
    let _ = store.delete(&key);
    match (first, second) {
        (Ok(true), Ok(second)) => Some(!second),
        _ => None,
    }
}

/// Contents of a base object.
pub(crate) fn encode_base(lsn: u64, prev: Option<&Claim>) -> Vec<u8> {
    let prev = prev.map_or_else(|| "-".to_string(), Claim::name);
    format!("squad-base 2\nlsn {lsn}\nprev {prev}\n").into_bytes()
}

fn decode_base(bytes: &[u8]) -> Option<(u64, Option<Claim>)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    if lines.next()? != "squad-base 2" {
        return None;
    }
    let lsn = lines.next()?.strip_prefix("lsn ")?.parse().ok()?;
    let prev = match lines.next()?.strip_prefix("prev ")? {
        "-" => None,
        name => Some(Claim::parse(name)?),
    };
    Some((lsn, prev))
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

/// A writer in the chain of established writers.
#[derive(Debug, Clone)]
pub(crate) struct Established {
    pub(crate) claim: Claim,
    /// LSN at which it took over.
    pub(crate) base: u64,
}

/// What the object store holds, as far as it can be restored.
pub(crate) struct RemoteState {
    /// The chain of established writers, oldest first. Epochs ascend.
    pub(crate) epochs: Vec<Established>,
    /// Newest valid snapshot: its writer and LSN.
    pub(crate) snapshot: Option<(Claim, u64)>,
    /// Log objects to replay, in order, starting after the snapshot (or
    /// after the LSN the listing was asked to continue from).
    pub(crate) wal: Vec<WalObject>,
    /// LSN the database reaches after replay.
    pub(crate) lsn: u64,
}

impl RemoteState {
    /// LSN up to which the records of `epoch` count, if it is in the chain.
    pub(crate) fn limit(&self, epoch: u64) -> Option<u64> {
        let i = self.epochs.iter().position(|e| e.claim.epoch == epoch)?;
        Some(self.epochs.get(i + 1).map_or(u64::MAX, |e| e.base))
    }

    /// The epoch whose writer produced the record at `lsn`.
    pub(crate) fn epoch_at(&self, lsn: u64) -> Option<u64> {
        self.epochs.iter().rev().find(|e| e.base <= lsn).or(self.epochs.first()).map(|e| e.claim.epoch)
    }

    /// The writer at the head of the chain.
    pub(crate) fn head(&self) -> Option<&Established> {
        self.epochs.last()
    }

    /// The writer of the chain with epoch `epoch`.
    pub(crate) fn writer(&self, epoch: u64) -> Option<&Claim> {
        self.epochs.iter().find(|e| e.claim.epoch == epoch).map(|e| &e.claim)
    }

    /// True if a directory at `lsn` whose history ends in `epoch` holds a
    /// prefix of the history in the object store.
    pub(crate) fn contains(&self, epoch: u64, lsn: u64) -> bool {
        let base = self.epochs.iter().find(|e| e.claim.epoch == epoch).map(|e| e.base);
        matches!((base, self.limit(epoch)), (Some(base), Some(limit)) if base <= lsn && lsn <= limit)
    }
}

/// Reads the base objects and returns the chain of established writers,
/// oldest first.
fn read_chain(store: &dyn ObjectStore) -> Result<Vec<Established>> {
    let mut bases: BTreeMap<u64, (Claim, u64, Option<Claim>)> = BTreeMap::new();
    for meta in store.list(BASE_PREFIX).map_err(remote_err)? {
        if is_format_1(&meta.key) {
            return Err(old_format());
        }
        let Some(claim) = meta.key.strip_prefix(BASE_PREFIX).and_then(Claim::parse) else { continue };
        let Some(bytes) = store.get(&meta.key).map_err(remote_err)? else { continue };
        let (lsn, prev) = decode_base(&bytes)
            .ok_or_else(|| StorageError::Corruption(format!("base object {} is damaged", meta.key)))?;
        if let Some((other, ..)) = bases.get(&claim.epoch) {
            // Impossible on a store whose PUT and LIST are linearizable.
            return Err(StorageError::Corruption(format!(
                "two writers established epoch {}: {} and {}",
                claim.epoch, other.id, claim.id
            )));
        }
        bases.insert(claim.epoch, (claim, lsn, prev));
    }
    let mut chain = Vec::new();
    let mut next = bases.keys().next_back().copied();
    while let Some(epoch) = next {
        let (claim, base, prev) = bases.remove(&epoch).ok_or_else(|| {
            StorageError::Corruption(format!("the chain of writers refers to epoch {epoch}, which has no base"))
        })?;
        next = match prev {
            Some(p) if p.epoch < epoch => {
                if bases.get(&p.epoch).is_some_and(|(c, ..)| *c != p) {
                    return Err(StorageError::Corruption(format!(
                        "the writer of epoch {epoch} continues an unknown writer {}",
                        p.name()
                    )));
                }
                Some(p.epoch)
            }
            Some(p) => {
                return Err(StorageError::Corruption(format!(
                    "the writer of epoch {epoch} claims to continue epoch {}",
                    p.epoch
                )))
            }
            None => None,
        };
        chain.push(Established { claim, base });
    }
    chain.reverse();
    Ok(chain)
}

pub(crate) fn read_remote(store: &dyn ObjectStore) -> Result<RemoteState> {
    read_remote_from(store, None, None)
}

/// Reads the remote state. With `from`, the log objects returned are the
/// ones that continue a copy standing at that LSN instead of the ones
/// that continue the newest snapshot.
///
/// `applied` names a log object (epoch and key) whose records, and those
/// of every object before it, are all at or below `from`: the listing
/// then starts after it, so that following a writer costs in proportion
/// to what is new and not to how much log the store retains.
pub(crate) fn read_remote_from(
    store: &dyn ObjectStore,
    from: Option<u64>,
    applied: Option<(u64, &str)>,
) -> Result<RemoteState> {
    let epochs = read_chain(store)?;
    // Records and snapshots of a writer are valid up to the next base.
    let limit = |claim: &Claim| -> Option<u64> {
        let i = epochs.iter().position(|e| e.claim == *claim)?;
        Some(epochs.get(i + 1).map_or(u64::MAX, |e| e.base))
    };

    let mut snapshot: Option<(Claim, u64)> = None;
    for meta in store.list("snapshot/").map_err(remote_err)? {
        let Some((claim, lsn)) = parse_snapshot_key(&meta.key) else { continue };
        let newer = snapshot.as_ref().is_none_or(|(c, l)| (lsn, claim.epoch) > (*l, c.epoch));
        if limit(&claim).is_some_and(|lim| lsn <= lim) && newer {
            snapshot = Some((claim, lsn));
        }
    }

    let mut lsn = from.unwrap_or(snapshot.as_ref().map_or(0, |s| s.1));
    let mut wal = Vec::new();
    for writer in &epochs {
        let epoch = writer.claim.epoch;
        let lim = limit(&writer.claim).unwrap_or(u64::MAX);
        let prefix = wal_prefix(&writer.claim);
        let listed = match applied {
            // Records of earlier writers count up to where a later one
            // took over, which lies at or below what was applied.
            Some((done, _)) if epoch < done => continue,
            Some((done, key)) if epoch == done && key.starts_with(&prefix) => store.list_after(&prefix, key),
            _ => store.list(&prefix),
        };
        let mut objects: Vec<(String, u64, u64)> = listed
            .map_err(remote_err)?
            .into_iter()
            .filter_map(|m| pair(&m.key[prefix.len()..]).map(|(a, b)| (m.key, a, b)))
            .collect();
        objects.sort_by_key(|o| (o.1, o.2));
        for (key, first, last) in objects {
            // Usable if it continues the sequence without a gap.
            if first <= lsn + 1 && last > lsn && lsn < lim {
                lsn = last.min(lim);
                wal.push(WalObject { key, epoch, limit: lim });
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
    if let Some((claim, lsn)) = &state.snapshot {
        let key = snapshot_key(claim, *lsn);
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
    /// Encoded records `first..=last`, not yet handed to an upload.
    buf: Vec<u8>,
    first: u64,
    last: u64,
    /// Records were dropped; a snapshot must cover them.
    overflowed: bool,
}

/// Uploads under way and what they have achieved.
#[derive(Default)]
struct Shipping {
    in_flight: usize,
    /// Every record up to this LSN is in an object that was put (or in a
    /// snapshot), though not necessarily certified by a fence check yet.
    put_through: u64,
    /// Ranges `first -> last` that were put but do not continue
    /// `put_through` yet, because an earlier range is still in flight.
    put_done: BTreeMap<u64, u64>,
    /// Ranges whose upload failed, to be uploaded again first:
    /// `first -> (last, records)`.
    retry: BTreeMap<u64, (u64, Vec<u8>)>,
}

impl Shipping {
    fn advance(&mut self) {
        while let Some((&first, &last)) = self.put_done.first_key_value() {
            if first > self.put_through + 1 {
                break;
            }
            self.put_through = self.put_through.max(last);
            self.put_done.remove(&first);
        }
    }
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
        if let Some((claim, lsn)) = &state.snapshot {
            let key = snapshot_key(claim, *lsn);
            if let Some(m) = store.get(&key).map_err(remote_err)?.and_then(|b| Manifest::decode(&b)) {
                snapshot.lsn = m.lsn;
                snapshot.chunks = m.chunks.into_iter().map(|c| c.0).collect();
            }
        }
        Ok(snapshot)
    }
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
    claim: Claim,
    /// Orders local writes and their entry into `pending`.
    write_lock: Mutex<()>,
    pending: Mutex<Pending>,
    /// Uploads under way; `shipped` is signalled when one ends.
    shipping: Mutex<Shipping>,
    shipped: Condvar,
    /// Every record up to here is in the store, and a fence check that
    /// started after it got there found no newer writer.
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
            self.claim.epoch
        ))
    }

    /// Fails once a later writer has claimed the database.
    fn check_fence(&self) -> Result<()> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(self.fenced_error());
        }
        if superseded(&*self.store, &self.claim)? {
            self.fenced.store(true, Ordering::Release);
            return Err(self.fenced_error());
        }
        Ok(())
    }

    /// Uploads the next unshipped range of log records as one object:
    /// a range whose earlier upload failed, or else everything pending.
    /// Returns `false` if there was nothing to upload.
    ///
    /// Up to [`RemoteConfig::upload_concurrency`] uploads run at once. Each puts its
    /// object and then lists the claims (the fence check); a check
    /// certifies every range whose `PUT` had completed, without gaps, when
    /// the check started, and only certified records count as shipped.
    ///
    /// # Why no acknowledged commit is lost to a takeover
    ///
    /// A commit is acknowledged (in `Sync` mode) only once `shipped_lsn`
    /// covers it: the objects holding its record and every earlier one
    /// were put (the last of these `PUT`s completing at `W`) and a
    /// following `LIST` of the claims (`F`) showed no claim above this
    /// writer's epoch. Let `n` be the claim of the writer that takes over,
    /// `P(n)` its `PUT` and `R(n)` the reading of the remote state that it
    /// does after winning. If `P(n)` took effect before `F` started, `F`
    /// sees `n` and nothing is acknowledged. Otherwise `W` < `F` < `P(n)`
    /// < `R(n)`, so `R(n)` sees all those objects: the new writer's history
    /// contains the commit, and its base (the LSN where this writer's
    /// records stop counting) lies at or after it. Objects this writer
    /// puts after `R(n)` lie above that base and are ignored by every
    /// reader.
    fn ship_one(&self) -> Result<bool> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(self.fenced_error());
        }
        let (first, last, buf) = {
            let mut st = self.shipping.lock();
            let range = match st.retry.pop_first() {
                Some((first, (last, buf))) => Some((first, last, buf)),
                None => {
                    let mut p = self.pending.lock();
                    (!p.buf.is_empty()).then(|| (p.first, p.last, std::mem::take(&mut p.buf)))
                }
            };
            let Some(range) = range else { return Ok(false) };
            st.in_flight += 1;
            range
        };
        let put = (|| {
            // Only records that are durable locally go out: a directory
            // must never be behind what the store holds of its own history.
            self.disk.sync(last)?;
            self.store.put(&wal_key(&self.claim, first, last), &buf).map_err(remote_err)
        })();
        let certify = {
            let mut st = self.shipping.lock();
            match &put {
                Ok(()) => {
                    st.put_done.insert(first, last);
                    st.advance();
                }
                // Upload it again later; overflowed records are covered
                // by the next snapshot instead.
                Err(_) if self.pending.lock().overflowed => {}
                Err(_) => {
                    st.retry.insert(first, (last, buf.clone()));
                }
            }
            st.put_through
        };
        let result = put.and_then(|()| self.certify(certify));
        self.shipping.lock().in_flight -= 1;
        self.shipped.notify_all();
        result?;
        self.wal_bytes_since_snapshot.fetch_add(buf.len() as u64, Ordering::Relaxed);
        let mut stats = self.stats.lock();
        stats.wal_objects += 1;
        stats.wal_bytes += buf.len() as u64;
        if last > first {
            stats.grouped_uploads += 1;
        }
        Ok(true)
    }

    /// Checks the fence and, if no newer writer exists, marks everything
    /// up to `put_through` (read before the check started) as shipped.
    fn certify(&self, put_through: u64) -> Result<()> {
        self.check_fence()?;
        self.shipped_lsn.fetch_max(put_through, Ordering::AcqRel);
        Ok(())
    }

    /// Returns once every record up to `lsn` is shipped. The caller
    /// uploads itself when there is something to upload and room for
    /// another upload, and otherwise waits for the uploads under way.
    /// Fails if an upload this caller made fails.
    fn ship_through(&self, lsn: u64) -> Result<()> {
        loop {
            if self.shipped_lsn.load(Ordering::Acquire) >= lsn {
                return Ok(());
            }
            if self.fenced.load(Ordering::Acquire) {
                return Err(self.fenced_error());
            }
            let mut st = self.shipping.lock();
            if self.shipped_lsn.load(Ordering::Acquire) >= lsn {
                return Ok(());
            }
            let unassigned = !st.retry.is_empty() || !self.pending.lock().buf.is_empty();
            if unassigned && st.in_flight < self.config.upload_concurrency.max(1) {
                drop(st);
                self.ship_one()?;
            } else if st.in_flight == 0 {
                // Everything is put, but no fence check covered it yet (the
                // last one failed): check now.
                let put_through = st.put_through;
                drop(st);
                if put_through < lsn {
                    return Err(StorageError::Remote(format!("log record {lsn} is not on its way to the store")));
                }
                self.certify(put_through)?;
            } else {
                self.shipped.wait_for(&mut st, Duration::from_millis(50));
            }
        }
    }

    /// Uploads what is pending now, and ranges to retry, for the
    /// background thread. Records that arrive meanwhile wait for the next
    /// round, so that `Async` mode uploads once per interval.
    fn ship_all(&self) -> Result<()> {
        let rounds = 1 + self.shipping.lock().retry.len();
        for _ in 0..rounds {
            if !self.ship_one()? {
                break;
            }
        }
        let put_through = self.shipping.lock().put_through;
        if put_through > self.shipped_lsn.load(Ordering::Acquire) {
            self.certify(put_through)?;
        }
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
            self.store.put(&snapshot_key(&self.claim, manifest.lsn), &manifest.encode()).map_err(remote_err)?;
            // Records the snapshot covers count as shipped only after a
            // fence check that started after the manifest was stored, for
            // the same reason as log objects (see `ship_one`).
            self.check_fence()?;
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
        {
            let mut st = self.shipping.lock();
            st.put_through = st.put_through.max(manifest.lsn);
            st.retry.retain(|_, (last, _)| *last > manifest.lsn);
            st.advance();
        }
        self.shipped_lsn.fetch_max(manifest.lsn, Ordering::AcqRel);
        self.shipped.notify_all();
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
    ///
    /// Everything is listed first and the fence is checked after the
    /// listings: an object of a newer writer can only be listed if that
    /// writer claimed before the listing, and then the check sees the
    /// claim and nothing is deleted.
    fn collect_garbage(&self, newest: &Manifest, retain_from: u64) -> Result<()> {
        let keep = snapshot_key(&self.claim, newest.lsn);
        let snapshots = self.store.list("snapshot/").map_err(remote_err)?;
        let chunks = self.store.list("chunk/").map_err(remote_err)?;
        let logs = self.store.list("wal/").map_err(remote_err)?;
        self.check_fence()?;
        for meta in snapshots {
            if meta.key != keep {
                self.store.delete(&meta.key).map_err(remote_err)?;
            }
        }
        let live: HashSet<&str> = newest.chunks.iter().map(|c| c.0.as_str()).collect();
        for meta in chunks {
            if !live.contains(&meta.key["chunk/".len()..]) {
                self.store.delete(&meta.key).map_err(remote_err)?;
            }
        }
        let horizon = retain_from.min(newest.lsn);
        for meta in logs {
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
        let _ = self.disk.sync(self.disk.last_lsn());
        let _ = self.ship_all();
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

/// Why a local directory at `local_lsn`, last written under
/// `local_epoch`, cannot continue the history in the object store, if it
/// cannot.
fn stale(state: &RemoteState, local_epoch: Option<u64>, local_lsn: u64) -> Option<String> {
    let remote_epoch = state.head().map(|e| e.claim.epoch);
    let same_lineage = remote_epoch.is_none() || local_epoch == remote_epoch;
    if same_lineage && local_lsn >= state.lsn {
        return None;
    }
    let why = if same_lineage {
        format!("the object store is ahead (LSN {} against local {local_lsn})", state.lsn)
    } else {
        "the local directory does not descend from the latest writer of the object store".to_string()
    };
    Some(format!("{why}; open with `restore` to replace the local directory with the remote state"))
}

impl TieredEngine {
    /// Opens the database in `dir`, restoring it from the object store if
    /// the directory is empty, and becomes the writer for the remote copy.
    pub fn open(dir: &Path, opts: DiskOptions, config: RemoteConfig) -> Result<TieredEngine> {
        let store = config.store.clone();
        let vfs = opts.vfs.clone();
        vfs.create_dir_all(dir)?;

        // A directory that cannot continue the remote history is refused
        // before anything is claimed, so that starting a stale copy by
        // mistake does not stop the writer that is running. The answer
        // cannot change later: a directory that is behind, or was left
        // by a replaced writer, stays that way.
        let local = if dir_is_empty(&opts, dir)? { None } else { Some(DiskEngine::open(dir, opts.clone())?) };
        let local_epoch = read_marker(&opts, dir)?;
        if let (Some(disk), false) = (&local, config.restore) {
            if let Some(why) = stale(&read_remote(&*store)?, local_epoch, disk.last_lsn()) {
                return Err(StorageError::Invalid(why));
            }
        }

        // Claim the next epoch before the state is read for good:
        // everything a previous writer uploads from now on either is
        // visible to the listing below or fails that writer's fence check.
        let conditional_writes = probe_conditional_writes(&*store);
        let claim = claim_epoch(&*store)?;
        let mut state = read_remote(&*store)?;

        let mut needs_snapshot = false;
        let disk = match local {
            None => {
                wipe(&opts, dir)?;
                let (disk, latest) = materialize_latest(&*store, dir, &opts)?;
                state = latest;
                disk
            }
            Some(disk) => {
                let local_lsn = disk.last_lsn();
                match stale(&state, local_epoch, local_lsn) {
                    // The local copy has everything the remote has, and
                    // possibly commits that were never shipped.
                    None => {
                        needs_snapshot = local_lsn > state.lsn || state.snapshot.is_none();
                        disk
                    }
                    Some(_) if config.restore => {
                        drop(disk);
                        wipe(&opts, dir)?;
                        let (disk, latest) = materialize_latest(&*store, dir, &opts)?;
                        state = latest;
                        disk
                    }
                    // Another writer got in between the two looks.
                    Some(why) => return Err(StorageError::Invalid(why)),
                }
            }
        };
        if state.snapshot.is_none() && disk.last_lsn() > 0 {
            needs_snapshot = true;
        }
        // The local copy now holds the history that ends with the head of
        // the chain in `state`; this writer continues it.
        let prev = state.head().map(|e| e.claim.clone());
        if prev.as_ref().is_some_and(|p| p.epoch > claim.epoch) {
            return Err(StorageError::Remote(format!(
                "another writer took over this database in the object store while this one was opening (epoch {})",
                claim.epoch
            )));
        }

        // From here on this writer's uploads count.
        store.put(&base_key(&claim), &encode_base(disk.last_lsn(), prev.as_ref())).map_err(remote_err)?;
        write_marker(&opts, dir, claim.epoch)?;
        if vfs.exists(&dir.join(REPLICA_FILE))? {
            // The directory was a replica's working copy until now.
            vfs.remove(&dir.join(REPLICA_FILE))?;
            vfs.sync_dir(dir)?;
        }
        let snapshot = SnapshotState::load(&*store, &state)?;
        let shipped = if needs_snapshot { state.lsn.min(disk.last_lsn()) } else { disk.last_lsn() };
        let engine = TieredEngine::start(disk, dir, opts, config, claim, snapshot, shipped)?;
        engine.shared.stats.lock().conditional_writes = conditional_writes;
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
        claim: Claim,
        snapshot: SnapshotState,
        shipped: u64,
    ) -> Result<TieredEngine> {
        let stats = RemoteStats { epoch: claim.epoch, claim_id: claim.id.clone(), ..RemoteStats::default() };
        let shared = Arc::new(Shared {
            disk,
            store: config.store.clone(),
            dir: dir.to_path_buf(),
            opts,
            claim,
            write_lock: Mutex::new(()),
            pending: Mutex::new(Pending::default()),
            shipping: Mutex::new(Shipping { put_through: shipped, ..Shipping::default() }),
            shipped: Condvar::new(),
            shipped_lsn: AtomicU64::new(shipped),
            wal_bytes_since_snapshot: AtomicU64::new(0),
            snapshot: Mutex::new(snapshot),
            fenced: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            stats: Mutex::new(stats),
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
        let last = self.shared.disk.last_lsn();
        self.shared.disk.sync(last)?;
        if self.shared.pending.lock().overflowed {
            // Some records were dropped from the upload queue; only the
            // next snapshot can cover them.
            return self.shared.ship_all();
        }
        self.shared.ship_through(last)
    }

    /// Records the answer of the conditional-write probe.
    pub(crate) fn set_conditional_writes(&self, answer: Option<bool>) {
        self.shared.stats.lock().conditional_writes = answer;
    }

    /// Uploads a snapshot now.
    pub fn snapshot_remote(&self) -> Result<()> {
        self.flush_remote()?;
        self.shared.snapshot()
    }

    pub fn remote_stats(&self) -> RemoteStats {
        let mut stats = self.shared.stats.lock().clone();
        stats.shipped_lsn = self.shared.shipped_lsn.load(Ordering::Acquire);
        stats.local_lsn = self.shared.disk.last_lsn();
        stats.fenced = self.shared.fenced.load(Ordering::Acquire);
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
        wal::encode_record(&mut record, lsn, &batch)?;
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
        if s.config.mode == RemoteSync::Sync {
            s.ship_through(lsn)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SimVfs, SyncMode};
    use squad_object::{Fault, MemoryStore};

    fn options(vfs: &SimVfs) -> DiskOptions {
        DiskOptions {
            sync_mode: SyncMode::Sync,
            cache_pages: 64,
            checkpoint_wal_bytes: 64 << 10,
            vfs: Arc::new(vfs.clone()),
        }
    }

    fn one(key: &str, value: &str) -> WriteBatch {
        let mut b = WriteBatch::new();
        b.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        b
    }

    fn dump(engine: &dyn Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
        engine.scan(Bound::Unbounded, Bound::Unbounded, false, usize::MAX).unwrap()
    }

    /// Many claimants at once, on a store that ignores conditional
    /// writes and whose requests take random time: no two win the same
    /// epoch. Claiming with `put_if_absent`, as remote format 1 did, gives
    /// the same epoch to several claimants on such a store.
    #[test]
    fn concurrent_claims_never_share_an_epoch() {
        for ignore in [false, true] {
            let store = MemoryStore::new();
            store.set_ignore_conditions(ignore);
            store.set_faults(Some(Box::new(|_, _| {
                thread::sleep(Duration::from_micros(random_u64() % 300));
                Fault::None
            })));
            let store: Arc<dyn ObjectStore> = Arc::new(store);
            let barrier = Arc::new(std::sync::Barrier::new(8));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let (store, barrier) = (store.clone(), barrier.clone());
                    thread::spawn(move || {
                        let mut won = Vec::new();
                        for _ in 0..6 {
                            barrier.wait();
                            won.push(claim_epoch(&*store).unwrap());
                        }
                        won
                    })
                })
                .collect();
            let mut winners: Vec<Claim> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
            assert_eq!(winners.len(), 48);
            winners.sort();
            for w in winners.windows(2) {
                assert_ne!(w[0].epoch, w[1].epoch, "two winners for epoch {} (ignore={ignore})", w[0].epoch);
            }
            // Every winner's claim is still there.
            let claims = list_claims(&*store, None).unwrap();
            assert!(winners.iter().all(|w| claims.contains(w)));
        }
    }

    #[test]
    fn keys_round_trip() {
        let claim = Claim { epoch: 42, id: "00ff00ff00ff00ff".into() };
        assert_eq!(Claim::parse(&claim_key(&claim)[CLAIM_PREFIX.len()..]), Some(claim.clone()));
        assert_eq!(parse_snapshot_key(&snapshot_key(&claim, 7)), Some((claim.clone(), 7)));
        assert!(wal_key(&claim, 1, 2).starts_with(&wal_prefix(&claim)));
        assert_eq!(decode_base(&encode_base(9, Some(&claim))), Some((9, Some(claim.clone()))));
        assert_eq!(decode_base(&encode_base(0, None)), Some((0, None)));
        assert!(claims_from(42) < claim_key(&claim) && claim_key(&claim) < claims_from(43));
        assert!(is_format_1("writer/claim-00000000000000000001"));
        assert!(!is_format_1(&claim_key(&claim)));
        assert_eq!(Claim::parse("00000000000000000001-xyz"), None);
    }

    /// A writer that won an epoch but fixed its starting point only after
    /// a later writer had read the state is not part of the history,
    /// even though its base and log objects exist and its epoch lies
    /// between the two others.
    #[test]
    fn a_writer_established_too_late_is_not_in_the_chain() {
        let store = MemoryStore::new();
        let config = RemoteConfig::new(Arc::new(store.clone()), RemoteSync::Sync);
        let a = TieredEngine::open(Path::new("/a"), options(&SimVfs::new(1)), config.clone()).unwrap();
        for i in 0..10 {
            let lsn = a.write(one(&format!("k{i}"), "a")).unwrap();
            a.sync(lsn).unwrap();
        }
        // Epoch 2 is won, and its claimant reads the state (LSN 10)...
        let late = claim_epoch(&store).unwrap();
        assert_eq!(late.epoch, 2);
        // ...while the old writer still uploads two records it cannot get
        // acknowledged any more.
        for i in 10..12 {
            let lsn = a.write(one(&format!("k{i}"), "a")).unwrap();
            assert!(a.sync(lsn).is_err());
            a.shared.fenced.store(false, Ordering::Release);
        }
        // Epoch 3 takes over from the state it reads, which includes them.
        let b = TieredEngine::open(Path::new("/b"), options(&SimVfs::new(2)), config.clone()).unwrap();
        assert_eq!(b.remote_stats().epoch, 3);
        assert_eq!(b.disk().last_lsn(), 12);
        let lsn = b.write(one("k12", "b")).unwrap();
        b.sync(lsn).unwrap();
        let want = dump(&b);
        // Only now does the writer of epoch 2 fix its starting point and
        // upload records that conflict with that history.
        store.put(&base_key(&late), &encode_base(10, Some(&a.shared.claim))).unwrap();
        let mut record = Vec::new();
        wal::encode_record(&mut record, 11, &one("k10", "late")).unwrap();
        wal::encode_record(&mut record, 12, &one("k11", "late")).unwrap();
        store.put(&wal_key(&late, 11, 12), &record).unwrap();
        a.shared.fenced.store(true, Ordering::Release);
        drop(a);

        let state = read_remote(&store).unwrap();
        assert_eq!(state.epochs.iter().map(|e| e.claim.epoch).collect::<Vec<_>>(), [1, 3]);
        let vfs = SimVfs::new(3);
        restore_from(&store, Path::new("/r"), options(&vfs)).unwrap();
        assert_eq!(dump(&DiskEngine::open(Path::new("/r"), options(&vfs)).unwrap()), want);
    }

    #[test]
    fn a_bucket_in_the_old_format_is_refused() {
        let store = MemoryStore::new();
        store.put("writer/claim-00000000000000000001", b"squad writer\n").unwrap();
        store.put("writer/base-00000000000000000001", b"0").unwrap();
        let config = RemoteConfig::new(Arc::new(store.clone()), RemoteSync::Sync);
        let err = TieredEngine::open(Path::new("/a"), options(&SimVfs::new(1)), config).err().unwrap();
        assert!(err.to_string().contains("remote format 1"), "{err}");
        // Nothing was claimed or written.
        let keys: Vec<String> = store.list("").unwrap().into_iter().map(|m| m.key).collect();
        assert_eq!(keys.len(), 2, "{keys:?}");
        let err = restore_from(&store, Path::new("/r"), options(&SimVfs::new(2))).unwrap_err();
        assert!(err.to_string().contains("remote format 1"), "{err}");
    }

    #[test]
    fn the_conditional_write_probe_is_reported() {
        for ignore in [false, true] {
            let store = MemoryStore::new();
            store.set_ignore_conditions(ignore);
            let config = RemoteConfig::new(Arc::new(store.clone()), RemoteSync::Sync);
            let a = TieredEngine::open(Path::new("/a"), options(&SimVfs::new(1)), config).unwrap();
            assert_eq!(a.remote_stats().conditional_writes, Some(!ignore));
            assert_eq!(a.remote_stats().claim_id.len(), 16);
            assert!(store.list("probe/").unwrap().is_empty());
        }
    }
}
