//! Read replicas and promotion.
//!
//! A replica is a [`Database`](crate::Database) on a
//! [`ReplicaEngine`]: a local copy that applies the log a writer ships
//! to an object store. This module connects that engine to the layers
//! above it: it moves the timestamp new snapshots read at after each
//! applied commit, swaps in the catalog when a commit changed the
//! schema, and runs the thread that polls the object store.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use squad_kv::Store;
use squad_object::ObjectStore;
use squad_storage::{
    DiskOptions, RemoteConfig, RemoteSync, ReplayHooks, ReplicaEngine, ReplicaStatus, StorageError, SyncMode,
    WriteBatch,
};

use crate::catalog::{Catalog, CATALOG_KEY};
use crate::session::{ReplayTarget, Shared};
use crate::{sqlstate, Error, Result};

/// Options for opening a read replica.
#[derive(Clone)]
pub struct ReplicaConfig {
    /// The object store the writer ships to.
    pub store: Arc<dyn ObjectStore>,
    /// How often the object store is asked for new log objects.
    pub poll_interval: Duration,
    /// How this node ships its own log once it is promoted to writer.
    pub promoted_sync: RemoteSync,
    /// Snapshot interval once promoted. See
    /// [`RemoteConfig::snapshot_wal_bytes`].
    pub snapshot_wal_bytes: u64,
    /// Requests run at a time to download a snapshot and the log. See
    /// [`RemoteConfig::restore_concurrency`].
    pub restore_concurrency: usize,
    /// How the local copy is made durable. A replica can always fetch
    /// again what it lost, so this only matters after promotion.
    pub sync_mode: SyncMode,
    /// Size of the page cache in bytes.
    pub cache_size: usize,
    /// Local log size that triggers a checkpoint of the local copy.
    pub checkpoint_wal_bytes: u64,
    /// Cache and compaction settings of columnar tables (compaction runs
    /// only after promotion).
    pub columnar: crate::ColumnarConfig,
}

impl ReplicaConfig {
    /// A replica of the database in `store` that polls once a second and
    /// ships synchronously after promotion.
    pub fn new(store: Arc<dyn ObjectStore>) -> ReplicaConfig {
        ReplicaConfig {
            store,
            poll_interval: Duration::from_secs(1),
            promoted_sync: RemoteSync::Sync,
            snapshot_wal_bytes: 64 << 20,
            restore_concurrency: squad_storage::DEFAULT_RESTORE_CONCURRENCY,
            sync_mode: SyncMode::Sync,
            cache_size: 128 << 20,
            checkpoint_wal_bytes: 64 << 20,
            columnar: crate::ColumnarConfig::default(),
        }
    }
}

impl std::fmt::Debug for ReplicaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaConfig")
            .field("poll_interval", &self.poll_interval)
            .field("promoted_sync", &self.promoted_sync)
            .field("snapshot_wal_bytes", &self.snapshot_wal_bytes)
            .field("restore_concurrency", &self.restore_concurrency)
            .field("sync_mode", &self.sync_mode)
            .field("cache_size", &self.cache_size)
            .field("checkpoint_wal_bytes", &self.checkpoint_wal_bytes)
            .finish_non_exhaustive()
    }
}

/// What a database opened with [`Database::open_replica`](crate::Database::open_replica) is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// Follows a writer; sessions are read-only.
    Replica,
    /// Was promoted and is the writer.
    Writer,
}

/// Position of a replica relative to its writer.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub role: ReplicationRole,
    /// Epoch of the writer whose commits were applied last (after
    /// promotion: this node's own epoch).
    pub epoch: u64,
    /// LSN of the last commit applied locally.
    pub applied_lsn: u64,
    /// Highest LSN found in the object store at the last poll (after
    /// promotion: the highest LSN this node has shipped).
    pub remote_lsn: u64,
    /// Time since a commit was last applied; `None` if none was applied
    /// since the replica was opened.
    pub since_last_apply: Option<Duration>,
    /// Time since the object store was last read successfully.
    pub since_last_poll: Option<Duration>,
    /// How often the local copy had to be replaced by a newer snapshot
    /// because the log no longer reached back to it.
    pub rebases: u64,
    /// A new writer is taking over; commits of the previous writer are
    /// held back until its starting point is known.
    pub waiting_for_takeover: bool,
    /// Why the last poll failed, if it did.
    pub last_error: Option<String>,
}

impl ReplicationStatus {
    fn new(s: ReplicaStatus) -> ReplicationStatus {
        ReplicationStatus {
            role: if s.promoted { ReplicationRole::Writer } else { ReplicationRole::Replica },
            epoch: s.epoch,
            applied_lsn: s.applied_lsn,
            remote_lsn: s.remote_lsn,
            since_last_apply: s.since_last_apply,
            since_last_poll: s.since_last_poll,
            rebases: s.rebases,
            waiting_for_takeover: s.waiting_for_takeover,
            last_error: s.last_error,
        }
    }

    /// The form `squad_replication_status()` returns.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let seconds = |d: Option<Duration>| d.map(|d| (d.as_secs_f64() * 1000.0).round() / 1000.0);
        serde_json::json!({
            "role": match self.role { ReplicationRole::Replica => "replica", ReplicationRole::Writer => "writer" },
            "epoch": self.epoch,
            "applied_lsn": self.applied_lsn,
            "remote_lsn": self.remote_lsn,
            "seconds_since_last_apply": seconds(self.since_last_apply),
            "seconds_since_last_poll": seconds(self.since_last_poll),
            "rebases": self.rebases,
            "waiting_for_takeover": self.waiting_for_takeover,
            "last_error": self.last_error,
        })
    }
}

/// The replication machinery of one database.
pub(crate) struct Replica {
    engine: Arc<ReplicaEngine>,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Replica {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.get_mut().take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}

/// Follows what the engine applies on behalf of the layers above it.
struct Hooks {
    target: ReplayTarget,
    /// Commit timestamp of the batch being applied.
    commit: Option<u64>,
    /// The catalog that batch stores, if it changes the schema.
    catalog: Option<Arc<Catalog>>,
}

impl Hooks {
    fn new(target: ReplayTarget) -> Hooks {
        Hooks { target, commit: None, catalog: None }
    }
}

impl ReplayHooks for Hooks {
    fn prepare(&mut self, batch: WriteBatch) -> WriteBatch {
        self.catalog = Store::replayed_value(&batch, CATALOG_KEY)
            .flatten()
            .and_then(|bytes| Catalog::from_bytes(&bytes).ok())
            .map(Arc::new);
        let (batch, commit) = self.target.store.replay_prepare(batch);
        self.commit = commit;
        batch
    }

    fn applied(&mut self, _lsn: u64) {
        let Some(ts) = self.commit.take() else { return };
        let store = &self.target.store;
        match self.catalog.take() {
            // The commit and the schema it brings appear together.
            Some(catalog) => self.target.replay_catalog(catalog, || store.replay_publish(ts)),
            None => store.replay_publish(ts),
        }
    }

    fn before_rebase(&mut self) {
        self.target.store.replay_invalidate();
    }

    fn after_rebase(&mut self) -> squad_storage::Result<()> {
        self.target.replay_reload().map_err(|e| StorageError::Corruption(e.to_string()))
    }
}

/// The apply loop. It holds the engine and the parts of the database it
/// changes, not the database: when the last handle is dropped, the
/// database's `Replica` stops and joins this thread, and the directory
/// is closed by the time that drop returns.
fn run(mut hooks: Hooks, engine: Arc<ReplicaEngine>, stop: Arc<AtomicBool>, interval: Duration) {
    let mut last = Instant::now();
    loop {
        // Unparked early only to stop; a spurious wakeup waits again.
        let wait = interval.saturating_sub(last.elapsed());
        if !wait.is_zero() {
            thread::park_timeout(wait);
        }
        if stop.load(Ordering::Acquire) || engine.is_promoted() {
            return;
        }
        if last.elapsed() < interval {
            continue;
        }
        // A failed poll is recorded in the status and tried again.
        let _ = engine.poll(&mut hooks);
        last = Instant::now();
    }
}

/// Opens the engine and the state above it. The caller starts following.
pub(crate) fn open(dir: &std::path::Path, config: ReplicaConfig) -> Result<Arc<Shared>> {
    let options = DiskOptions {
        sync_mode: config.sync_mode,
        cache_pages: (config.cache_size / 8192).max(64),
        checkpoint_wal_bytes: config.checkpoint_wal_bytes,
        ..DiskOptions::default()
    };
    let remote = RemoteConfig {
        snapshot_wal_bytes: config.snapshot_wal_bytes,
        restore_concurrency: config.restore_concurrency,
        ..RemoteConfig::new(config.store.clone(), config.promoted_sync)
    };
    let engine = Arc::new(ReplicaEngine::open(dir, options, remote)?);
    let shared = Shared::open(Store::new(engine.clone())?, "squad")?;
    shared.set_temp_dir(dir.join("tmp"))?;
    shared.set_recovery(true);
    // Segments of columnar tables are read from where the writer puts them.
    shared.set_objects(config.store.clone());
    // Start from everything that is in the object store right now.
    engine.poll(&mut Hooks::new(shared.replay_target()))?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let (hooks, engine, stop) = (Hooks::new(shared.replay_target()), engine.clone(), stop.clone());
        let interval = config.poll_interval.max(Duration::from_millis(1));
        thread::Builder::new()
            .name("squad-replica".into())
            .spawn(move || run(hooks, engine, stop, interval))
            .map_err(|e| Error::new(sqlstate::IO_ERROR, e.to_string()))?
    };
    let _ = shared.replica.set(Replica { engine, stop, thread: Mutex::new(Some(thread)) });
    Ok(shared)
}

fn replica(shared: &Shared) -> Result<&Replica> {
    shared.replica.get().ok_or_else(|| {
        Error::new(sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE, "this database was not opened as a read replica")
    })
}

pub(crate) fn status(shared: &Shared) -> Option<ReplicationStatus> {
    shared.replica.get().map(|r| ReplicationStatus::new(r.engine.status()))
}

/// Applies what is in the object store now, without waiting for the
/// next poll.
pub(crate) fn catch_up(shared: &Shared) -> Result<ReplicationStatus> {
    let r = replica(shared)?;
    r.engine.poll(&mut Hooks::new(shared.replay_target()))?;
    Ok(ReplicationStatus::new(r.engine.status()))
}

/// Makes this replica the writer.
pub(crate) fn promote(shared: &Shared) -> Result<()> {
    let r = replica(shared)?;
    r.engine.promote(&mut Hooks::new(shared.replay_target()))?;
    shared.replay_finished();
    Ok(())
}
