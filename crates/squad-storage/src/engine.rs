use std::ops::Bound;
use std::path::Path;

use crate::Result;

/// Maximum length of a key accepted by any engine, in bytes.
pub const MAX_KEY_LEN: usize = 2048;

/// How eagerly committed writes are forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Every [`Engine::sync`] call returns only after an `fsync` covering the
    /// requested LSN. Concurrent callers share one `fsync` (group commit).
    /// Survives power loss.
    #[default]
    Sync,
    /// Like `Sync`, but the engine may wait a short moment to batch more
    /// commits into one `fsync`. Survives power loss.
    Group,
    /// `sync` returns immediately; the WAL is flushed in the background.
    /// Survives process crashes, may lose the last moments on power loss.
    Async,
}

/// An atomic set of writes. `None` as value deletes the key.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    pub ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push((key, Some(value)));
    }
    pub fn delete(&mut self, key: Vec<u8>) {
        self.ops.push((key, None));
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
}

/// Counters exposed for diagnostics and benchmarks.
#[derive(Debug, Default, Clone)]
pub struct EngineStats {
    pub page_count: u64,
    pub cached_pages: u64,
    pub dirty_pages: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub wal_bytes: u64,
    pub checkpoints: u64,
    pub fsyncs: u64,
}

/// Outcome of an integrity check.
#[derive(Debug, Default, Clone)]
pub struct VerifyReport {
    pub pages_checked: u64,
    pub entries: u64,
    /// Human-readable descriptions of every problem found. Empty means OK.
    pub errors: Vec<String>,
}

impl VerifyReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

/// A durable, ordered map from byte-string keys to byte-string values.
///
/// Keys are ordered lexicographically by their bytes. All methods are safe to
/// call from many threads at once. Readers never block behind writers for
/// longer than a single batch application.
pub trait Engine: Send + Sync + 'static {
    /// Returns the value stored under `key`, if any.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Returns up to `limit` entries with keys inside the given bounds, in
    /// ascending key order, or descending when `reverse` is set.
    ///
    /// A caller pages through a range by calling again with the last
    /// returned key as an excluded bound.
    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Atomically applies a batch: after a crash either all of its
    /// operations are present or none. Returns the log sequence number (LSN)
    /// of the batch. The batch is visible to readers when this returns, but
    /// is only guaranteed durable after [`Engine::sync`] for that LSN.
    fn write(&self, batch: WriteBatch) -> Result<u64>;

    /// Blocks until everything up to and including `lsn` is durable
    /// according to the engine's [`SyncMode`].
    fn sync(&self, lsn: u64) -> Result<()>;

    /// Writes all dirty state to the data file and truncates the log.
    fn checkpoint(&self) -> Result<()>;

    /// Writes a consistent copy of the database into the empty or
    /// non-existing directory `dest` without blocking writers for the
    /// duration of the copy.
    fn backup(&self, dest: &Path) -> Result<()>;

    /// Checks structural integrity and checksums of all data.
    fn verify(&self) -> Result<VerifyReport>;

    /// Diagnostic counters.
    fn stats(&self) -> EngineStats;
}
