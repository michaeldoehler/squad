//! Sessions: statement dispatch, transactions, prepared statements and the
//! state shared by all sessions of one database.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use pg_query::protobuf::{self as pb, node::Node as N};
use squad_kv::{Isolation, LockMode, Store, Txn, WaitPolicy};

use crate::bind::{parse_sql, Binder, OutputColumn};
use crate::catalog::{sequence_key, Catalog, Sequence, CATALOG_KEY};
use crate::exec::{collect, ExecCx};
use crate::expr::{Row, SessionFn};
use crate::plan::Plan;
use crate::types::{FnCtx, Type, Value, PG_EPOCH_UNIX_MICROS};
use crate::{sqlstate, Error, Result};

/// Values reserved per write of a sequence's persistent state. As in
/// Postgres, a crash may skip up to this many values.
const SEQUENCE_CACHE: i64 = 32;
const ROWID_CACHE: i64 = 1024;

struct SeqState {
    /// Next value to hand out.
    next: i64,
    /// Last value covered by the persisted high-water mark.
    reserved: i64,
}

/// State shared by all sessions of a database.
pub struct Shared {
    pub store: Store,
    /// Latest committed catalog.
    pub catalog: Arc<RwLock<Arc<Catalog>>>,
    sequences: Mutex<HashMap<u32, SeqState>>,
    /// Plans of ad-hoc statements, keyed by search path and SQL text.
    plan_cache: Arc<Mutex<HashMap<String, Arc<Prepared>>>>,
    /// Where segments of columnar tables are kept: a directory next to
    /// the database, memory, or a remote bucket.
    objects: RwLock<Arc<dyn squad_object::ObjectStore>>,
    /// Caches, pending and retired segment objects and the background
    /// compaction of columnar tables.
    pub(crate) columnar: crate::columnar::ColumnarState,
    /// Held by every handle and session of the database; the last one to
    /// go stops background work before the database closes.
    guard: Mutex<std::sync::Weak<CloseGuard>>,
    next_pid: AtomicI32,
    pub database_name: String,
    /// Version of `catalog`, readable without taking its lock. Sessions
    /// keep their own reference to the catalog and only go back to the
    /// shared one when this number changes.
    catalog_version: Arc<std::sync::atomic::AtomicU64>,
    /// True while this database follows a writer as a read replica.
    recovery: AtomicBool,
    /// On a replica, a session holds this shared while it takes a catalog
    /// and a snapshot, and the apply loop holds it exclusively while it
    /// makes a schema change visible, so the two always belong together.
    replay_gate: Arc<RwLock<()>>,
    /// The replication machinery, if the database was opened as a
    /// replica. It stays after promotion.
    pub(crate) replica: std::sync::OnceLock<crate::replica::Replica>,
    /// The engine, if this database keeps its durable copy in an object
    /// store as a writer.
    pub(crate) tiered: std::sync::OnceLock<Arc<squad_storage::TieredEngine>>,
    /// Advisory locks held by the sessions of this database.
    pub(crate) advisory: Arc<crate::advisory::AdvisoryLocks>,
    /// The sessions of this database, for `pg_stat_activity` and
    /// `pg_cancel_backend` / `pg_terminate_backend`.
    pub(crate) backends: crate::backends::Backends,
    /// `work_mem` of sessions that do not set it.
    pub(crate) work_mem: std::sync::atomic::AtomicUsize,
    /// Where operators that exceed `work_mem` write temporary files.
    pub(crate) temp: RwLock<Arc<crate::spill::TempSpace>>,
}

/// What the apply loop of a read replica changes in a database. It
/// shares these parts with [`Shared`] without keeping the database
/// itself open, so that dropping the last handle closes the database on
/// the thread that drops it.
#[derive(Clone)]
pub(crate) struct ReplayTarget {
    pub(crate) store: Store,
    gate: Arc<RwLock<()>>,
    catalog: Arc<RwLock<Arc<Catalog>>>,
    catalog_version: Arc<std::sync::atomic::AtomicU64>,
    plan_cache: Arc<Mutex<HashMap<String, Arc<Prepared>>>>,
}

impl ReplayTarget {
    /// Runs `publish`, which makes a commit visible, together with the
    /// change of catalog that commit made.
    pub(crate) fn replay_catalog(&self, catalog: Arc<Catalog>, publish: impl FnOnce()) {
        let _gate = self.gate.write();
        publish();
        let mut current = self.catalog.write();
        self.catalog_version.store(catalog.version, Ordering::Release);
        *current = catalog;
        self.plan_cache.lock().clear();
    }

    /// Adopts the state of a replaced local copy.
    pub(crate) fn replay_reload(&self) -> Result<()> {
        let _gate = self.gate.write();
        self.store.replay_reset()?;
        let txn = self.store.begin(Isolation::ReadCommitted);
        let catalog = match txn.get(CATALOG_KEY)? {
            Some(bytes) => Catalog::from_bytes(&bytes)?,
            None => Catalog::bootstrap(),
        };
        drop(txn);
        let mut current = self.catalog.write();
        self.catalog_version.store(catalog.version, Ordering::Release);
        *current = Arc::new(catalog);
        self.plan_cache.lock().clear();
        Ok(())
    }
}

fn table_lock_key(id: u32) -> Vec<u8> {
    let mut k = b"\x00\x00\x00\x00lock/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

const DDL_LOCK_KEY: &[u8] = b"\x00\x00\x00\x00lock/ddl";
const PLAN_CACHE_LIMIT: usize = 4096;

impl Shared {
    pub fn open(store: Store, database_name: &str) -> Result<Arc<Shared>> {
        let txn = store.begin(Isolation::ReadCommitted);
        let catalog = match txn.get(CATALOG_KEY)? {
            Some(bytes) => Catalog::from_bytes(&bytes)?,
            None => Catalog::bootstrap(),
        };
        drop(txn);
        let version = catalog.version;
        Ok(Arc::new(Shared {
            store,
            catalog: Arc::new(RwLock::new(Arc::new(catalog))),
            sequences: Mutex::new(HashMap::new()),
            plan_cache: Arc::new(Mutex::new(HashMap::new())),
            objects: RwLock::new(Arc::new(squad_object::MemoryStore::new())),
            columnar: Default::default(),
            guard: Mutex::new(std::sync::Weak::new()),
            next_pid: AtomicI32::new(1),
            advisory: Arc::default(),
            backends: Default::default(),
            work_mem: std::sync::atomic::AtomicUsize::new(crate::spill::DEFAULT_WORK_MEM),
            temp: RwLock::new(Arc::new(crate::spill::TempSpace::private())),
            database_name: database_name.to_string(),
            catalog_version: Arc::new(std::sync::atomic::AtomicU64::new(version)),
            recovery: AtomicBool::new(false),
            replay_gate: Arc::new(RwLock::new(())),
            replica: std::sync::OnceLock::new(),
            tiered: std::sync::OnceLock::new(),
        }))
    }

    /// True while this database is a read replica that follows a writer.
    pub fn in_recovery(&self) -> bool {
        self.recovery.load(Ordering::Acquire)
    }

    pub(crate) fn set_recovery(&self, on: bool) -> bool {
        self.recovery.swap(on, Ordering::AcqRel)
    }

    /// Makes operators write their temporary files to `dir`, removing the
    /// files an earlier process left there.
    pub(crate) fn set_temp_dir(&self, dir: std::path::PathBuf) -> Result<()> {
        *self.temp.write() = Arc::new(crate::spill::TempSpace::in_dir(dir)?);
        Ok(())
    }

    /// The directory of temporary files.
    pub(crate) fn temp_space(&self) -> Arc<crate::spill::TempSpace> {
        self.temp.read().clone()
    }

    /// Fails on a read replica, where nothing may be written.
    pub(crate) fn check_writable(&self, what: &str) -> Result<()> {
        if self.in_recovery() {
            return Err(Error::new(
                sqlstate::READ_ONLY_SQL_TRANSACTION,
                format!("cannot execute {what} on a read replica"),
            ));
        }
        Ok(())
    }

    /// Replica: held by a session while it picks a catalog and a snapshot.
    fn replay_shared(&self) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
        self.in_recovery().then(|| self.replay_gate.read())
    }

    /// The parts of this database the apply loop of a replica changes.
    pub(crate) fn replay_target(&self) -> ReplayTarget {
        ReplayTarget {
            store: self.store.clone(),
            gate: self.replay_gate.clone(),
            catalog: self.catalog.clone(),
            catalog_version: self.catalog_version.clone(),
            plan_cache: self.plan_cache.clone(),
        }
    }

    /// The node stopped being a replica: forget what was derived from a
    /// state that only the writer changed.
    pub(crate) fn replay_finished(&self) {
        if self.set_recovery(false) {
            self.store.replay_finish();
            self.sequences.lock().clear();
            self.columnar.compactor.reset();
            let catalog = self.catalog.read().clone();
            for t in catalog.tables.values().filter(|t| t.columnar.is_some()) {
                self.columnar.compactor.mark_due(t.id);
            }
            self.plan_cache.lock().clear();
        }
    }

    /// Starts the background work of a database whose handles hold the
    /// returned guard.
    pub(crate) fn start(self: &Arc<Self>, config: crate::columnar::ColumnarConfig) -> Arc<CloseGuard> {
        self.columnar.configure(config);
        let guard = Arc::new(CloseGuard { shared: self.clone() });
        *self.guard.lock() = Arc::downgrade(&guard);
        self.columnar.compactor.start(self);
        guard
    }

    /// The object store holding the segments of columnar tables.
    pub fn objects(&self) -> Arc<dyn squad_object::ObjectStore> {
        self.objects.read().clone()
    }

    /// Replaces the object store. Must happen before any columnar table
    /// is used.
    pub fn set_objects(&self, store: Arc<dyn squad_object::ObjectStore>) {
        *self.objects.write() = store;
    }

    fn publish_catalog(&self, catalog: Arc<Catalog>) {
        let mut current = self.catalog.write();
        if catalog.version > current.version {
            self.catalog_version.store(catalog.version, Ordering::Release);
            *current = catalog;
            self.plan_cache.lock().clear();
        }
    }

    /// Hands out the next value of a counter stored under `sequence_key(id)`.
    #[allow(clippy::too_many_arguments)]
    fn next_counter(
        &self,
        id: u32,
        start: i64,
        increment: i64,
        min: i64,
        max: i64,
        cycle: bool,
        cache: i64,
        name: &str,
    ) -> Result<i64> {
        self.check_writable("nextval()")?;
        let mut seqs = self.sequences.lock();
        let state = match seqs.entry(id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                // First use in this process: continue after the persisted
                // high-water mark, or start fresh.
                let txn = self.store.begin(Isolation::ReadCommitted);
                e.insert(match txn.get(&sequence_key(id))? {
                    Some(v) if v.len() == 8 => {
                        let hwm = i64::from_be_bytes(v.try_into().unwrap());
                        SeqState { next: hwm.saturating_add(increment), reserved: hwm }
                    }
                    _ => SeqState { next: start, reserved: start.saturating_sub(increment) },
                })
            }
        };
        let exhausted = |v: i64| if increment > 0 { v > max } else { v < min };
        if exhausted(state.next) {
            if !cycle {
                let (bound, value) = if increment > 0 { ("maximum", max) } else { ("minimum", min) };
                return Err(Error::new(
                    "2200H",
                    format!("nextval: reached {bound} value of sequence \"{name}\" ({value})"),
                ));
            }
            state.next = if increment > 0 { min } else { max };
            state.reserved = state.next.saturating_sub(increment);
        }
        let value = state.next;
        let beyond = if increment > 0 { value > state.reserved } else { value < state.reserved };
        if beyond {
            let hwm = value.saturating_add(increment.saturating_mul(cache - 1)).clamp(min, max);
            let mut txn = self.store.begin(Isolation::ReadCommitted);
            txn.put_blind(&sequence_key(id), &hwm.to_be_bytes())?;
            txn.commit()?;
            state.reserved = hwm;
        }
        state.next = value.saturating_add(increment);
        Ok(value)
    }

    pub fn nextval(&self, seq: &Sequence) -> Result<i64> {
        self.next_counter(seq.id, seq.start, seq.increment, seq.min, seq.max, seq.cycle, SEQUENCE_CACHE, &seq.name)
    }

    pub fn next_rowid(&self, table_id: u32) -> Result<i64> {
        self.next_counter(table_id, 1, 1, 1, i64::MAX, false, ROWID_CACHE, "rowid")
    }

    /// Makes the next `nextval` return `value` (or the one after it if
    /// `is_called`).
    pub fn setval(&self, seq: &Sequence, value: i64, is_called: bool) -> Result<()> {
        if value < seq.min || value > seq.max {
            return Err(Error::new(
                sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
                format!(
                    "setval: value {value} is out of bounds for sequence \"{}\" ({}..{})",
                    seq.name, seq.min, seq.max
                ),
            ));
        }
        self.check_writable("setval()")?;
        let mut seqs = self.sequences.lock();
        let hwm = if is_called { value } else { value.saturating_sub(seq.increment) };
        let mut txn = self.store.begin(Isolation::ReadCommitted);
        txn.put_blind(&sequence_key(seq.id), &hwm.to_be_bytes())?;
        txn.commit()?;
        seqs.insert(seq.id, SeqState { next: hwm.saturating_add(seq.increment), reserved: hwm });
        Ok(())
    }

    /// Forgets a counter's state (new or dropped sequence, truncated table).
    pub fn reset_counter(&self, id: u32) -> Result<()> {
        self.check_writable("a sequence change")?;
        let mut seqs = self.sequences.lock();
        seqs.remove(&id);
        let mut txn = self.store.begin(Isolation::ReadCommitted);
        txn.delete_blind(&sequence_key(id))?;
        txn.commit()?;
        Ok(())
    }
}

/// Stops a database's background work when the last handle or session
/// goes away, before the database itself is closed.
pub(crate) struct CloseGuard {
    shared: Arc<Shared>,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        self.shared.columnar.compactor.shutdown();
    }
}

/// Who a session runs as, and what that login may do.
///
/// squad has no SQL roles. The login name is what `current_user` and
/// `session_user` return and what `CREATE POLICY ... TO name` matches.
/// [`Login::default`] is the embedded API's connection: user `squad`,
/// treated as the owner of every table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login {
    pub user: String,
    /// Not subject to row-level security, like the owner of a table in
    /// PostgreSQL: policies apply only to tables with `FORCE ROW LEVEL
    /// SECURITY`. Without it, the login is subject to the policies of
    /// every table with row-level security enabled and may not change
    /// those tables or their policies.
    pub bypass_rls: bool,
    /// Every transaction is read-only (SQLSTATE `25006` for writes and
    /// DDL), and cannot be made read-write.
    pub read_only: bool,
}

impl Default for Login {
    fn default() -> Login {
        Login { user: "squad".to_string(), bypass_rls: true, read_only: false }
    }
}

impl Login {
    /// A login named `user` with the default attributes.
    pub fn new(user: impl Into<String>) -> Login {
        Login { user: user.into(), ..Login::default() }
    }
}

/// Per-session settings and state.
pub struct SessionState {
    pub search_path: Vec<String>,
    /// The login name (`current_user`, `session_user`).
    pub user: String,
    /// See [`Login::bypass_rls`].
    pub bypass_rls: bool,
    /// See [`Login::read_only`].
    pub read_only_login: bool,
    pub settings: HashMap<String, String>,
    pub tz: chrono_tz::Tz,
    pub lock_timeout: Option<Duration>,
    /// `statement_timeout`: a statement running longer fails with 57014.
    pub statement_timeout: Option<Duration>,
    /// `work_mem` in bytes: the memory one operator may use for its
    /// buffers before it writes to temporary files.
    pub work_mem: usize,
    /// What `RESET work_mem` returns to: the database's default when the
    /// session was opened.
    pub work_mem_default: usize,
    /// Filled while `EXPLAIN ANALYZE` runs a statement: details about plan
    /// nodes (keyed by their address), such as how a sort was done.
    pub(crate) plan_notes: Option<HashMap<usize, Vec<String>>>,
    pub currval: HashMap<u32, i64>,
    pub lastval: Option<i64>,
    pub default_isolation: Isolation,
    /// `default_transaction_read_only`.
    pub default_read_only: bool,
    /// `row_security`: when off, a query that row-level security would
    /// filter fails instead (SQLSTATE 42501).
    pub row_security: bool,
    pub pid: i32,
    pub(crate) cancel: CancelHandle,
    /// Notices raised by the running statement.
    pub(crate) notices: Vec<crate::Notice>,
    /// Setting changes made by the open transaction.
    pub(crate) guc_txn: GucTxn,
}

/// The settings that `SET`, `SET LOCAL`, `RESET` and `set_config` change,
/// as one value that a transaction can save and restore.
#[derive(Clone)]
pub(crate) struct Gucs {
    search_path: Vec<String>,
    settings: HashMap<String, String>,
    tz: chrono_tz::Tz,
    lock_timeout: Option<Duration>,
    statement_timeout: Option<Duration>,
    work_mem: usize,
    default_isolation: Isolation,
    default_read_only: bool,
    row_security: bool,
}

/// What a transaction changed in the settings. As in PostgreSQL, `SET`
/// inside a transaction is undone if it rolls back, and `SET LOCAL` lasts
/// until the transaction ends either way. Both stay `None` until the
/// transaction changes a setting.
#[derive(Default)]
pub(crate) struct GucTxn {
    /// The settings when the transaction started; restored by rollback.
    start: Option<Gucs>,
    /// The settings as they will be after commit: the `SET LOCAL` changes
    /// left out.
    session: Option<Gucs>,
}

impl SessionState {
    pub(crate) fn gucs(&self) -> Gucs {
        Gucs {
            search_path: self.search_path.clone(),
            settings: self.settings.clone(),
            tz: self.tz,
            lock_timeout: self.lock_timeout,
            statement_timeout: self.statement_timeout,
            work_mem: self.work_mem,
            default_isolation: self.default_isolation,
            default_read_only: self.default_read_only,
            row_security: self.row_security,
        }
    }

    fn set_gucs(&mut self, g: Gucs) {
        self.search_path = g.search_path;
        self.settings = g.settings;
        self.tz = g.tz;
        self.lock_timeout = g.lock_timeout;
        self.statement_timeout = g.statement_timeout;
        self.work_mem = g.work_mem;
        self.default_isolation = g.default_isolation;
        self.default_read_only = g.default_read_only;
        self.row_security = g.row_security;
    }

    /// Changes settings with `f`. Inside a transaction (`in_txn`), the
    /// change is recorded so that rollback undoes it, and a `local` one
    /// is also undone by commit. Outside a transaction, a `local` change
    /// has no effect.
    pub(crate) fn change_settings(
        &mut self,
        in_txn: bool,
        local: bool,
        f: &mut dyn FnMut(&mut SessionState) -> Result<()>,
    ) -> Result<()> {
        if !in_txn {
            return if local { Ok(()) } else { f(self) };
        }
        if self.guc_txn.start.is_none() {
            let g = self.gucs();
            self.guc_txn.start = Some(g.clone());
            self.guc_txn.session = Some(g);
        }
        if !local {
            // The same change, applied to the values commit will keep.
            let current = self.gucs();
            let session = self.guc_txn.session.take().expect("set with start");
            self.set_gucs(session.clone());
            let applied = f(self);
            let changed = self.gucs();
            self.set_gucs(current);
            match applied {
                Ok(()) => self.guc_txn.session = Some(changed),
                Err(e) => {
                    self.guc_txn.session = Some(session);
                    return Err(e);
                }
            }
        }
        f(self)
    }

    /// The transaction ended: keep its session-level changes on commit,
    /// undo all of them on rollback.
    fn end_settings(&mut self, commit: bool) {
        let GucTxn { start, session } = std::mem::take(&mut self.guc_txn);
        let keep = if commit { session } else { start };
        if let Some(g) = keep {
            let before = std::mem::take(&mut self.settings);
            self.set_gucs(g);
            self.keep_placeholders(&before);
        }
    }

    /// A custom setting, once set, exists for the rest of the session,
    /// even if the transaction that set it rolls back or set it locally:
    /// it is then empty (PostgreSQL's placeholder variables).
    fn keep_placeholders(&mut self, before: &HashMap<String, String>) {
        for k in before.keys().filter(|k| k.contains('.')) {
            self.settings.entry(k.clone()).or_default();
        }
    }

    /// State to restore on `ROLLBACK TO` a savepoint taken now.
    fn settings_mark(&self) -> Option<(Gucs, Gucs)> {
        let session = self.guc_txn.session.clone()?;
        Some((self.gucs(), session))
    }

    fn rollback_settings_to(&mut self, mark: &Option<(Gucs, Gucs)>) {
        let before = self.settings.clone();
        self.rollback_settings_to_mark(mark);
        self.keep_placeholders(&before);
        if let Some(session) = &mut self.guc_txn.session {
            for k in before.keys().filter(|k| k.contains('.')) {
                session.settings.entry(k.clone()).or_default();
            }
        }
    }

    fn rollback_settings_to_mark(&mut self, mark: &Option<(Gucs, Gucs)>) {
        match mark {
            Some((effective, session)) => {
                self.set_gucs(effective.clone());
                self.guc_txn.session = Some(session.clone());
            }
            // Nothing had changed when the savepoint was taken.
            None => {
                if let Some(start) = self.guc_txn.start.clone() {
                    self.set_gucs(start.clone());
                    self.guc_txn.session = Some(start);
                }
            }
        }
    }

    /// The login this session runs as.
    pub fn login(&self) -> Login {
        Login { user: self.user.clone(), bypass_rls: self.bypass_rls, read_only: self.read_only_login }
    }

    /// Transactions that are started now are read-only whatever they ask
    /// for: on a replica and for a read-only login.
    pub(crate) fn forced_read_only(&self, shared: &Shared) -> bool {
        self.read_only_login || shared.in_recovery()
    }
}

/// Asks the statement running on a session to stop, from another thread.
///
/// The executor checks the request between rows; the statement then fails
/// with SQLSTATE `57014` ("canceling statement due to user request") and
/// its transaction is rolled back like after any other error. A request
/// made while no statement runs is discarded when the next statement
/// starts through [`Connection`](crate::Connection) or the wire protocol.
#[derive(Debug, Clone, Default)]
pub struct CancelHandle(Arc<CancelFlags>);

#[derive(Debug, Default)]
struct CancelFlags {
    requested: AtomicBool,
    /// Set by `pg_terminate_backend`; never cleared.
    terminated: AtomicBool,
}

impl CancelHandle {
    /// Requests cancellation of the running statement.
    pub fn cancel(&self) {
        self.0.requested.store(true, Ordering::SeqCst);
    }

    /// Ends the session: the running statement and every later one fail
    /// with `57P01`.
    pub(crate) fn terminate(&self) {
        self.0.terminated.store(true, Ordering::SeqCst);
        self.cancel();
    }

    /// True once the session was terminated with `pg_terminate_backend`.
    pub(crate) fn is_terminated(&self) -> bool {
        self.0.terminated.load(Ordering::SeqCst)
    }

    /// True if a request is pending; clears it.
    pub(crate) fn take(&self) -> bool {
        self.0.requested.load(Ordering::Relaxed) && self.0.requested.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn clear(&self) {
        self.0.requested.store(false, Ordering::SeqCst);
    }

    /// The error a statement stopped by this handle fails with.
    pub(crate) fn error(&self) -> Error {
        if self.is_terminated() {
            terminated_error()
        } else {
            Error::new(sqlstate::QUERY_CANCELED, "canceling statement due to user request")
        }
    }
}

pub(crate) fn terminated_error() -> Error {
    Error::new(sqlstate::ADMIN_SHUTDOWN, "terminating connection due to administrator command")
}

/// Command tag and kind of a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Tag {
    Select,
    Insert,
    Update,
    Delete,
    Other(&'static str),
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Kind {
    /// A planned query or DML statement.
    Plan {
        plan: Plan,
        subqueries: usize,
        tables: Vec<u32>,
        read_only: bool,
    },
    /// Everything else is interpreted from the parse tree at run time.
    Utility(Box<pb::Node>),
    Empty,
}

/// A parsed and, where applicable, planned statement.
pub struct Prepared {
    pub sql: String,
    pub columns: Vec<OutputColumn>,
    pub param_types: Vec<Type>,
    pub tag: Tag,
    pub(crate) kind: Kind,
    /// Catalog version the plan was built against.
    pub(crate) catalog_version: u64,
    /// What the plan was made for besides the catalog.
    context: PlanContext,
}

impl Prepared {
    /// True for statements that return rows.
    pub fn returns_rows(&self) -> bool {
        !self.columns.is_empty()
    }
}

/// What a plan depends on besides SQL text and catalog: names resolve
/// through the search path, and row-level security depends on the login
/// and on `row_security`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PlanContext {
    search_path: Vec<String>,
    user: String,
    bypass_rls: bool,
    row_security: bool,
}

impl PlanContext {
    fn of(state: &SessionState) -> PlanContext {
        PlanContext {
            search_path: state.search_path.clone(),
            user: state.user.clone(),
            bypass_rls: state.bypass_rls,
            row_security: state.row_security,
        }
    }

    fn matches(&self, state: &SessionState) -> bool {
        self.search_path == state.search_path
            && self.user == state.user
            && self.bypass_rls == state.bypass_rls
            && self.row_security == state.row_security
    }
}

/// Result of one statement.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub columns: Vec<OutputColumn>,
    pub rows: Vec<Row>,
    /// Postgres command tag, e.g. `INSERT 0 1`.
    pub tag: String,
    pub rows_affected: u64,
    /// Warnings and notices the statement raised, e.g. that it was
    /// accepted without effect.
    pub notices: Vec<crate::Notice>,
}

pub(crate) struct TxnState {
    pub(crate) txn: Txn,
    /// The catalog as this transaction sees it.
    pub(crate) catalog: Arc<Catalog>,
    /// The transaction changed the catalog.
    pub(crate) dirty: bool,
    /// A statement failed; only ROLLBACK is accepted.
    pub(crate) failed: bool,
    /// Started by BEGIN rather than implicitly for one statement.
    pub(crate) explicit: bool,
    /// The implicit transaction around a multi-statement string. It acts
    /// like a block while the string runs and ends with it, unless a BEGIN
    /// inside the string takes it over.
    pub(crate) batch: bool,
    pub(crate) savepoints: Vec<Savepoint>,
    pub(crate) start_time: i64,
    /// Counters to forget when the transaction ends either way.
    pub(crate) reset_counters: Vec<u32>,
    /// Segment objects to delete once the transaction has committed.
    pub(crate) dropped_objects: Vec<String>,
    /// Rows and delta blocks this transaction inserted into columnar
    /// tables.
    pub(crate) columnar_inserted: HashMap<u32, (u64, u64)>,
    /// Segment objects this transaction wrote; deleted if it rolls back.
    pub(crate) written_objects: Vec<String>,
}

pub(crate) struct Savepoint {
    name: String,
    mark: usize,
    catalog: Arc<Catalog>,
    dirty: bool,
    /// The settings to go back to (see [`SessionState::settings_mark`]).
    settings: Option<(Gucs, Gucs)>,
}

/// Transaction status as reported in the wire protocol's ReadyForQuery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Idle,
    InTransaction,
    Failed,
}

pub struct Session {
    pub(crate) shared: Arc<Shared>,
    pub(crate) state: SessionState,
    pub(crate) txn: Option<TxnState>,
    /// Statements created by SQL `PREPARE`.
    pub(crate) named: HashMap<String, Arc<Prepared>>,
    /// The committed catalog as of the last time this session looked.
    catalog_cache: Arc<Catalog>,
    /// Current plans of prepared statements whose original plan went
    /// stale, keyed by the address of the original (which is kept alive
    /// alongside, so the address cannot be reused).
    replans: HashMap<usize, (Arc<Prepared>, Arc<Prepared>)>,
    /// The transaction object of the last finished transaction.
    spare_txn: Option<Txn>,
    /// Keeps the database's background work running while the session
    /// exists (absent for the background work's own sessions).
    _guard: Option<Arc<CloseGuard>>,
    /// The session's entry in `pg_stat_activity` (absent for the
    /// background work's own sessions).
    backend: Option<Arc<crate::backends::Backend>>,
}

fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros() - PG_EPOCH_UNIX_MICROS
}

pub(crate) fn feature(what: &str) -> Error {
    Error::unsupported(what)
}

impl Session {
    pub fn new(shared: Arc<Shared>) -> Session {
        let guard = shared.guard.lock().upgrade();
        let mut session = Session::internal(shared);
        session._guard = guard;
        let work_mem = session.shared.work_mem.load(Ordering::Relaxed);
        session.state.work_mem = work_mem;
        session.state.work_mem_default = work_mem;
        let backends = &session.shared.backends;
        session.backend =
            Some(backends.register(session.state.pid, session.state.cancel.clone(), now_micros(), &session.state.user));
        session
    }

    /// A session that does not keep the database's background work
    /// alive (used by that work itself).
    pub(crate) fn internal(shared: Arc<Shared>) -> Session {
        let pid = shared.next_pid.fetch_add(1, Ordering::Relaxed);
        let catalog_cache = shared.catalog.read().clone();
        Session {
            shared,
            state: SessionState {
                search_path: vec!["public".to_string()],
                user: "squad".to_string(),
                bypass_rls: true,
                read_only_login: false,
                settings: HashMap::new(),
                tz: chrono_tz::UTC,
                lock_timeout: None,
                statement_timeout: None,
                work_mem: crate::spill::DEFAULT_WORK_MEM,
                work_mem_default: crate::spill::DEFAULT_WORK_MEM,
                plan_notes: None,
                currval: HashMap::new(),
                lastval: None,
                default_isolation: Isolation::ReadCommitted,
                default_read_only: false,
                row_security: true,
                pid,
                cancel: CancelHandle::default(),
                notices: Vec::new(),
                guc_txn: GucTxn::default(),
            },
            txn: None,
            named: HashMap::new(),
            catalog_cache,
            replans: HashMap::new(),
            spare_txn: None,
            _guard: None,
            backend: None,
        }
    }

    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    pub fn state(&self) -> &SessionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut SessionState {
        &mut self.state
    }

    /// Makes the session run as `login`: its name is `current_user`, and
    /// its attributes decide whether row-level security applies and
    /// whether it may write. Takes effect with the next transaction.
    pub fn set_login(&mut self, login: Login) {
        self.state.user = login.user;
        self.state.bypass_rls = login.bypass_rls;
        self.state.read_only_login = login.read_only;
    }

    /// The login this session runs as.
    pub fn login(&self) -> Login {
        self.state.login()
    }

    /// True if the next statement runs read-only: the open transaction is
    /// read-only, or a new one would be.
    pub fn is_read_only(&self) -> bool {
        match &self.txn {
            Some(t) => t.txn.is_read_only(),
            None => self.state.forced_read_only(&self.shared) || self.state.default_read_only,
        }
    }

    /// True if row-level security policies of `table` (a possibly
    /// qualified, possibly quoted name) apply to this session. `COPY FROM`
    /// is refused for such tables, as in PostgreSQL.
    pub fn row_security_applies(&self, table: &str) -> bool {
        let (schema, name) = split_qualified(table);
        let catalog = self.catalog();
        catalog
            .table(schema.as_deref(), &name, &self.state.search_path)
            .is_some_and(|t| t.row_security && (!self.state.bypass_rls || t.force_row_security))
    }

    /// Key of the shared plan cache: the SQL text and what the plan
    /// depends on besides the catalog (see [`PlanContext`]).
    fn cache_key(&self, sql: &str) -> String {
        format!(
            "{}\u{0}{}\u{0}{}{}\u{0}{}",
            self.state.search_path.join(","),
            self.state.user,
            u8::from(self.state.bypass_rls),
            u8::from(self.state.row_security),
            sql
        )
    }

    /// A handle with which another thread can cancel the statement this
    /// session is running.
    pub fn cancel_handle(&self) -> CancelHandle {
        self.state.cancel.clone()
    }

    /// True once another session ended this one with
    /// `pg_terminate_backend`: every statement now fails with `57P01`, and
    /// the owner should close the session.
    pub fn is_terminated(&self) -> bool {
        self.state.cancel.is_terminated()
    }

    /// Installs a function that `pg_terminate_backend` calls after it
    /// marked this session as terminated, so that an owner waiting for
    /// something else (a server connection waiting for its client) can
    /// notice. It runs on the terminating session's thread.
    pub fn set_terminate_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        if let Some(b) = &self.backend {
            b.set_terminate_hook(Box::new(hook));
        }
    }

    /// Updates this session's row in `pg_stat_activity` from its state,
    /// e.g. after the owner changed [`SessionState::user`].
    pub fn refresh_activity(&self) {
        self.report_activity(None);
    }

    /// Records in `pg_stat_activity` that a statement starts (`Some`) or
    /// ended (`None`).
    fn report_activity(&self, query: Option<&str>) {
        let Some(backend) = &self.backend else { return };
        let now = now_micros();
        let mut a = backend.activity.lock();
        a.state = match (query, &self.txn) {
            (Some(_), _) => "active",
            (None, Some(t)) if t.failed => "idle in transaction (aborted)",
            (None, Some(t)) if t.explicit => "idle in transaction",
            (None, _) => "idle",
        };
        if let Some(q) = query {
            a.query.clear();
            a.query.push_str(q);
            a.query_start = Some(now);
        }
        a.xact_start = match &self.txn {
            Some(t) if query.is_none() && !t.explicit => None,
            Some(t) => Some(t.start_time),
            None if query.is_some() => Some(now),
            None => None,
        };
        a.state_change = now;
        if a.user != self.state.user {
            a.user.clone_from(&self.state.user);
        }
        let app = self.state.settings.get("application_name").map_or("", String::as_str);
        if a.application_name != app {
            a.application_name = app.to_string();
        }
    }

    /// Discards a cancel request that arrived while no statement ran.
    /// Call it before starting a new request.
    pub fn clear_cancel(&self) {
        self.state.cancel.clear();
    }

    /// Fails with 57014 if a cancel request is pending (and clears it).
    pub fn check_canceled(&self) -> Result<()> {
        if self.state.cancel.take() || self.state.cancel.is_terminated() {
            return Err(self.state.cancel.error());
        }
        Ok(())
    }

    pub fn status(&self) -> TxnStatus {
        match &self.txn {
            None => TxnStatus::Idle,
            Some(t) if t.failed => TxnStatus::Failed,
            Some(t) if t.explicit => TxnStatus::InTransaction,
            Some(_) => TxnStatus::Idle,
        }
    }

    /// The catalog visible to the next statement.
    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        match &self.txn {
            Some(t) if t.dirty || t.txn.isolation() != Isolation::ReadCommitted => t.catalog.clone(),
            _ if self.catalog_cache.version == self.shared.catalog_version.load(Ordering::Acquire) => {
                self.catalog_cache.clone()
            }
            _ => self.shared.catalog.read().clone(),
        }
    }

    /// Version of the catalog the next statement will see.
    fn catalog_version(&self) -> u64 {
        match &self.txn {
            Some(t) if t.dirty || t.txn.isolation() != Isolation::ReadCommitted => t.catalog.version,
            _ => self.shared.catalog_version.load(Ordering::Acquire),
        }
    }

    /// The latest committed catalog, through the session's own reference.
    fn latest_catalog(&mut self) -> Arc<Catalog> {
        if self.catalog_cache.version != self.shared.catalog_version.load(Ordering::Acquire) {
            // A private copy: its reference count is then only touched by
            // this session, not by every session on every statement.
            self.catalog_cache = Arc::new((**self.shared.catalog.read()).clone());
        }
        self.catalog_cache.clone()
    }

    // ---------------------------------------------------------------
    // Preparing
    // ---------------------------------------------------------------

    /// Parses and plans one statement. `param_types` gives the types of
    /// `$n` parameters where the client knows them.
    pub fn prepare(&mut self, sql: &str, param_types: &[Option<Type>]) -> Result<Arc<Prepared>> {
        let parsed = parse_sql(sql);
        let mut stmts = self.abort_on_error(parsed)?;
        match stmts.len() {
            0 => Ok(Arc::new(self.empty_statement(sql))),
            1 => {
                let raw = stmts.remove(0);
                self.prepare_raw(sql, raw, param_types).map(Arc::new)
            }
            _ => Err(Error::syntax("cannot insert multiple commands into a prepared statement")),
        }
    }

    fn empty_statement(&self, sql: &str) -> Prepared {
        Prepared {
            sql: sql.to_string(),
            columns: Vec::new(),
            param_types: Vec::new(),
            tag: Tag::Other(""),
            kind: Kind::Empty,
            catalog_version: 0,
            context: PlanContext::default(),
        }
    }

    /// An error while parsing or analyzing a statement aborts the open
    /// transaction, just like an error while running it.
    fn abort_on_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            if let Some(t) = &mut self.txn {
                t.failed = true;
                self.report_activity(None);
            }
        }
        result
    }

    fn prepare_raw(&mut self, sql: &str, raw: pb::RawStmt, param_types: &[Option<Type>]) -> Result<Prepared> {
        let prepared = self.analyze(sql, raw, param_types);
        self.abort_on_error(prepared)
    }

    fn analyze(&mut self, sql: &str, raw: pb::RawStmt, param_types: &[Option<Type>]) -> Result<Prepared> {
        let Some(stmt) = raw.stmt.and_then(|s| s.node) else { return Ok(self.empty_statement(sql)) };
        // In an aborted transaction nothing but transaction control is
        // analyzed, so the error is 25P02 rather than whatever the
        // statement would have failed with.
        if !matches!(stmt, N::TransactionStmt(_)) {
            self.check_not_failed()?;
        }
        let catalog = self.catalog();
        let mut binder = Binder::new(&catalog, &self.state.search_path, param_types.to_vec());
        binder.rls = Some(crate::bind::RowSecurityUser {
            user: self.state.user.clone(),
            bypass_rls: self.state.bypass_rls,
            row_security: self.state.row_security,
        });
        let finish_params = |b: &Binder| -> Vec<Type> {
            b.param_types.iter().map(|t| t.clone().filter(|t| *t != Type::Unknown).unwrap_or(Type::Text)).collect()
        };
        let (kind, columns, tag, params) = match &stmt {
            N::SelectStmt(sel) => {
                let (plan, columns) = binder.bind_query(sel)?;
                let read_only = !plan_writes(&plan);
                let kind = Kind::Plan {
                    plan,
                    subqueries: binder.subquery_count(),
                    tables: binder.tables_used.clone(),
                    read_only,
                };
                (kind, columns, Tag::Select, finish_params(&binder))
            }
            N::InsertStmt(_) | N::UpdateStmt(_) | N::DeleteStmt(_) => {
                let (plan, columns) = binder.bind_dml(&stmt)?;
                let tag = match &stmt {
                    N::InsertStmt(_) => Tag::Insert,
                    N::UpdateStmt(_) => Tag::Update,
                    _ => Tag::Delete,
                };
                let kind = Kind::Plan {
                    plan,
                    subqueries: binder.subquery_count(),
                    tables: binder.tables_used.clone(),
                    read_only: false,
                };
                (kind, columns, tag, finish_params(&binder))
            }
            other => {
                let (tag, columns) = crate::ddl::utility_shape(self, other)?;
                (Kind::Utility(Box::new(pb::Node { node: Some(stmt.clone()) })), columns, Tag::Other(tag), Vec::new())
            }
        };
        Ok(Prepared {
            sql: sql.to_string(),
            columns,
            param_types: params,
            tag,
            kind,
            catalog_version: catalog.version,
            context: PlanContext::of(&self.state),
        })
    }

    /// Returns a plan for ad-hoc SQL, reusing a cached one when the
    /// catalog has not changed.
    fn cache_lookup(&self, sql: &str) -> Option<Arc<Prepared>> {
        if self.txn.as_ref().is_some_and(|t| t.dirty) {
            return None;
        }
        let key = self.cache_key(sql);
        let hit = self.shared.plan_cache.lock().get(&key).cloned()?;
        (hit.catalog_version == self.catalog_version()).then_some(hit)
    }

    fn cached(&mut self, sql: &str) -> Result<Arc<Prepared>> {
        let catalog = self.catalog();
        let private = self.txn.as_ref().is_some_and(|t| t.dirty);
        let key = self.cache_key(sql);
        if !private {
            if let Some(p) = self.shared.plan_cache.lock().get(&key) {
                if p.catalog_version == catalog.version {
                    return Ok(p.clone());
                }
            }
        }
        let prepared = self.prepare(sql, &[])?;
        // Statements with parameters or utility commands are not worth
        // caching here; prepared statements keep their own plan.
        if !private && matches!(prepared.kind, Kind::Plan { .. }) {
            let mut cache = self.shared.plan_cache.lock();
            if cache.len() >= PLAN_CACHE_LIMIT {
                cache.clear();
            }
            cache.insert(key, prepared.clone());
        }
        Ok(prepared)
    }

    // ---------------------------------------------------------------
    // Executing
    // ---------------------------------------------------------------

    /// Runs SQL text that may contain several statements (the simple query
    /// protocol). Several statements run as one implicit transaction.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();
        self.execute_each(sql, &mut |r| {
            results.push(r);
            Ok(())
        })?;
        Ok(results)
    }

    /// Like [`execute`](Session::execute), but hands every statement's
    /// result to `on_result` as soon as it is available, so that results
    /// of the statements before a failing one are not lost.
    pub fn execute_each(&mut self, sql: &str, on_result: &mut dyn FnMut(QueryResult) -> Result<()>) -> Result<()> {
        // A cache hit means the text is a single, already planned statement:
        // skip parsing altogether.
        if let Some(prepared) = self.cache_lookup(sql) {
            return on_result(self.execute_prepared(&prepared, &[])?);
        }
        let parsed = parse_sql(sql);
        let stmts = self.abort_on_error(parsed)?;
        if stmts.len() <= 1 {
            let prepared = self.cached(sql)?;
            return on_result(self.execute_prepared(&prepared, &[])?);
        }
        let implicit_block = self.txn.is_none();
        if implicit_block {
            self.begin(None, false)?;
            let t = self.txn.as_mut().unwrap();
            t.explicit = true;
            t.batch = true;
        }
        let mut outcome = Ok(());
        for raw in stmts {
            let text = statement_text(sql, &raw);
            let step = self
                .prepare_raw(text, raw, &[])
                .and_then(|p| self.execute_prepared(&Arc::new(p), &[]))
                .and_then(&mut *on_result);
            if let Err(e) = step {
                outcome = Err(e);
                break;
            }
        }
        // The string may have ended its transaction itself (COMMIT) or
        // opened a real block (BEGIN), which then stays open.
        if let Some(t) = self.txn.as_ref().filter(|t| implicit_block && t.batch) {
            if outcome.is_ok() && !t.failed {
                outcome = self.commit();
            } else {
                self.rollback();
            }
        }
        outcome
    }

    /// Converts client-supplied parameters to the statement's parameter
    /// types.
    fn convert_params(&self, stmt: &Prepared, params: &[Value]) -> Result<Vec<Value>> {
        if params.len() < stmt.param_types.len() {
            return Err(Error::new(
                sqlstate::PROTOCOL_VIOLATION,
                format!(
                    "bind message supplies {} parameters, but prepared statement requires {}",
                    params.len(),
                    stmt.param_types.len()
                ),
            ));
        }
        let ctx = self.fn_ctx();
        let mut out = Vec::with_capacity(params.len());
        for (i, v) in params.iter().enumerate() {
            let want = stmt.param_types.get(i);
            out.push(match (v, want) {
                (Value::Null, _) | (_, None) => v.clone(),
                (v, Some(Type::Enum(name))) => match v {
                    Value::Enum(..) => v.clone(),
                    other => {
                        let catalog = self.catalog();
                        let ty = catalog.enums.values().find(|e| e.name == *name).ok_or_else(|| {
                            Error::new(sqlstate::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist"))
                        })?;
                        crate::expr::enum_value(ty, other.as_str().unwrap_or_default())?
                    }
                },
                (v, Some(t)) if v.type_of() == t.base() => v.clone(),
                (v, Some(t)) => crate::value::cast(v.clone(), &t.base(), &ctx)?,
            });
        }
        Ok(out)
    }

    pub(crate) fn fn_ctx(&self) -> FnCtx {
        let now = now_micros();
        FnCtx { txn_time: self.txn.as_ref().map_or(now, |t| t.start_time), stmt_time: now, tz: self.state.tz }
    }

    /// Runs a prepared statement.
    pub fn execute_prepared(&mut self, stmt: &Arc<Prepared>, params: &[Value]) -> Result<QueryResult> {
        // Re-plan if the schema or the search path changed since planning.
        let current_version = self.catalog_version();
        let stale = match &stmt.kind {
            Kind::Plan { .. } => stmt.catalog_version != current_version || !stmt.context.matches(&self.state),
            _ => false,
        };
        let replanned;
        let stmt = if stale {
            // The caller keeps its handle to the outdated plan, so remember
            // the new one instead of planning again on every execution.
            let key = Arc::as_ptr(stmt) as usize;
            let reusable = self.replans.get(&key).filter(|(original, new)| {
                Arc::ptr_eq(original, stmt)
                    && new.catalog_version == current_version
                    && new.context.matches(&self.state)
            });
            replanned = match reusable {
                Some((_, new)) => new.clone(),
                None => {
                    let types: Vec<Option<Type>> = stmt.param_types.iter().cloned().map(Some).collect();
                    let new = self.prepare(&stmt.sql, &types)?;
                    if new.columns.iter().map(|c| &c.ty).ne(stmt.columns.iter().map(|c| &c.ty)) {
                        return Err(Error::new(
                            sqlstate::FEATURE_NOT_SUPPORTED,
                            "cached plan must not change result type",
                        ));
                    }
                    if self.replans.len() >= 1024 {
                        self.replans.clear();
                    }
                    // Plans made inside a transaction that changed the
                    // schema are only valid there.
                    if !self.txn.as_ref().is_some_and(|t| t.dirty) {
                        self.replans.insert(key, (stmt.clone(), new.clone()));
                    }
                    new
                }
            };
            &replanned
        } else {
            stmt
        };

        self.state.notices.clear();
        if self.state.cancel.is_terminated() {
            return Err(terminated_error());
        }
        self.report_activity(Some(&stmt.sql));
        let result = self.run_prepared(stmt, params);
        self.report_activity(None);
        let notices = std::mem::take(&mut self.state.notices);
        result.map(|mut r| {
            // `EXECUTE` returns the result of the statement it ran, which
            // already carries that statement's notices.
            r.notices.extend(notices);
            r
        })
    }

    fn run_prepared(&mut self, stmt: &Prepared, params: &[Value]) -> Result<QueryResult> {
        match &stmt.kind {
            Kind::Empty => Ok(QueryResult::default()),
            Kind::Utility(node) => {
                let n = node.node.as_ref().ok_or_else(|| Error::internal("empty utility statement"))?;
                // Transaction control works even in a failed transaction.
                if !matches!(n, N::TransactionStmt(_)) {
                    self.check_not_failed()?;
                }
                let params = self.convert_params(stmt, params)?;
                let result = crate::ddl::run_utility(self, n, &params);
                self.after_statement(result)
            }
            Kind::Plan { plan, subqueries, tables, read_only } => {
                self.check_not_failed()?;
                let params = self.convert_params(stmt, params)?;
                let result = self.run_plan(stmt, plan, *subqueries, tables, *read_only, &params);
                self.after_statement(result)
            }
        }
    }

    /// Queues a notice for the result of the running statement.
    pub(crate) fn notice(&mut self, notice: crate::Notice) {
        self.state.notices.push(notice);
    }

    fn check_not_failed(&self) -> Result<()> {
        match &self.txn {
            Some(t) if t.failed => Err(Error::new(
                sqlstate::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            )),
            _ => Ok(()),
        }
    }

    /// Ends the implicit transaction of a single statement, or marks an
    /// explicit transaction as failed.
    fn after_statement(&mut self, result: Result<QueryResult>) -> Result<QueryResult> {
        match &mut self.txn {
            Some(t) if !t.explicit => match result {
                Ok(r) => {
                    self.commit()?;
                    Ok(r)
                }
                Err(e) => {
                    self.rollback();
                    Err(e)
                }
            },
            Some(t) => {
                if result.is_err() {
                    t.failed = true;
                }
                result
            }
            None => result,
        }
    }

    /// Makes sure a transaction is open and its snapshot and catalog are
    /// current for a new statement.
    pub(crate) fn start_statement(&mut self) -> Result<&mut TxnState> {
        if self.txn.is_none() {
            // A new transaction starts with the latest snapshot and catalog.
            self.begin(None, false)?;
            return Ok(self.txn.as_mut().unwrap());
        }
        let shared = self.shared.clone();
        let _gate = shared.replay_shared();
        let latest = self.latest_catalog();
        let t = self.txn.as_mut().unwrap();
        t.txn.refresh_snapshot();
        if !t.dirty && t.txn.isolation() == Isolation::ReadCommitted {
            t.catalog = latest;
        }
        Ok(t)
    }

    fn run_plan(
        &mut self,
        stmt: &Prepared,
        plan: &Plan,
        subqueries: usize,
        tables: &[u32],
        read_only: bool,
        params: &[Value],
    ) -> Result<QueryResult> {
        let fn_ctx = self.fn_ctx();
        let wait = WaitPolicy::Wait(self.state.lock_timeout);
        self.start_statement()?;
        let fn_ctx = FnCtx { txn_time: self.txn.as_ref().unwrap().start_time, ..fn_ctx };
        let t = self.txn.as_mut().unwrap();
        if !read_only {
            if t.txn.is_read_only() {
                return Err(Error::new(
                    sqlstate::READ_ONLY_SQL_TRANSACTION,
                    format!("cannot execute {} in a read-only transaction", tag_name(&stmt.tag)),
                ));
            }
            // Writers share a table lock that DDL takes exclusively.
            for id in tables {
                t.txn.lock(&table_lock_key(*id), LockMode::Shared, wait)?;
            }
        }
        let mut cx = ExecCx::new(&mut t.txn, &t.catalog, params, &mut self.state, &self.shared, fn_ctx, subqueries);
        let rows = collect(plan, &mut cx)?;
        let affected = cx.rows_affected;
        let (dropped, inserted) = (std::mem::take(&mut cx.dropped_objects), std::mem::take(&mut cx.columnar_inserted));
        t.written_objects.append(&mut cx.written_objects);
        t.dropped_objects.extend(dropped);
        for (table, (rows, blocks)) in inserted {
            let e = t.columnar_inserted.entry(table).or_default();
            e.0 += rows;
            e.1 += blocks;
        }
        let (tag, rows_affected) = match stmt.tag {
            Tag::Select => (format!("SELECT {}", rows.len()), rows.len() as u64),
            Tag::Insert => (format!("INSERT 0 {affected}"), affected),
            Tag::Update => (format!("UPDATE {affected}"), affected),
            Tag::Delete => (format!("DELETE {affected}"), affected),
            Tag::Other(t) => (t.to_string(), affected),
        };
        Ok(QueryResult { columns: stmt.columns.clone(), rows, tag, rows_affected, notices: Vec::new() })
    }

    /// Inserts rows given as values, one per visible column of `table`
    /// in table order, as one statement. See
    /// [`Connection::append`](crate::Connection::append).
    pub fn append(&mut self, table: &str, rows: &[Vec<Value>]) -> Result<u64> {
        let result = self.append_rows(table, rows);
        self.after_statement(result).map(|r| r.rows_affected)
    }

    fn append_rows(&mut self, table: &str, rows: &[Vec<Value>]) -> Result<QueryResult> {
        self.check_not_failed()?;
        let quoted = match table.split_once('.') {
            Some((schema, name)) => format!("{}.{}", crate::copy::quote_ident(schema), crate::copy::quote_ident(name)),
            None => crate::copy::quote_ident(table),
        };
        // The statement `INSERT INTO t VALUES ($1, …)` is planned once per
        // catalog version; its value expressions do the conversions,
        // defaults and length checks of an ordinary insert.
        let catalog = self.catalog();
        let (schema, name) = match table.split_once('.') {
            Some((s, n)) => (Some(s), n),
            None => (None, table),
        };
        let def = catalog
            .table(schema, name, &self.state.search_path)
            .cloned()
            .ok_or_else(|| Error::new(sqlstate::UNDEFINED_TABLE, format!("relation \"{table}\" does not exist")))?;
        let width = def.visible_columns().count();
        let mut sql = format!("INSERT INTO {quoted} VALUES (");
        for i in 1..=width {
            if i > 1 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("${i}"));
        }
        sql.push(')');
        let stmt = match self.cache_lookup(&sql) {
            Some(p) => p,
            None => {
                let p = self.prepare(&sql, &[])?;
                if !self.txn.as_ref().is_some_and(|t| t.dirty) {
                    let key = self.cache_key(&sql);
                    let mut cache = self.shared.plan_cache.lock();
                    if cache.len() >= PLAN_CACHE_LIMIT {
                        cache.clear();
                    }
                    cache.insert(key, p.clone());
                }
                p
            }
        };
        let Kind::Plan { plan: Plan::Insert(insert), tables, .. } = &stmt.kind else {
            return Err(Error::internal("append did not plan an insert"));
        };
        let Plan::Values(exprs) = &insert.source else { return Err(Error::internal("append without values")) };
        let exprs = exprs.first().ok_or_else(|| Error::internal("append without values"))?;
        let mut params = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != width {
                return Err(Error::new(
                    sqlstate::PROTOCOL_VIOLATION,
                    format!("append to \"{table}\" needs {width} values per row, got {}", row.len()),
                ));
            }
            params.push(self.convert_params(&stmt, row)?);
        }
        let fn_ctx = self.fn_ctx();
        let wait = WaitPolicy::Wait(self.state.lock_timeout);
        self.start_statement()?;
        let fn_ctx = FnCtx { txn_time: self.txn.as_ref().unwrap().start_time, ..fn_ctx };
        let t = self.txn.as_mut().unwrap();
        if t.txn.is_read_only() {
            return Err(Error::new(
                sqlstate::READ_ONLY_SQL_TRANSACTION,
                "cannot execute INSERT in a read-only transaction",
            ));
        }
        for id in tables {
            t.txn.lock(&table_lock_key(*id), LockMode::Shared, wait)?;
        }
        let mut cx = ExecCx::new(&mut t.txn, &t.catalog, &[], &mut self.state, &self.shared, fn_ctx, 0);
        let mut full = Vec::with_capacity(rows.len());
        for p in &params {
            cx.params = p;
            full.push(exprs.iter().map(|e| e.eval(&[], &mut cx)).collect::<Result<Row>>()?);
        }
        cx.params = &[];
        if insert.table.columnar.is_some() {
            crate::dml::insert_columnar(insert, full, &mut cx)?;
        } else {
            let values =
                Plan::Values(full.into_iter().map(|r| r.into_iter().map(crate::expr::Expr::Const).collect()).collect());
            let direct = crate::plan::InsertPlan {
                table: insert.table.clone(),
                source: values,
                on_conflict: None,
                checks: insert.checks.clone(),
                rls_checks: insert.rls_checks.clone(),
                returning: None,
            };
            crate::dml::insert(&direct, &mut cx)?;
        }
        let affected = cx.rows_affected;
        let (dropped, inserted) = (std::mem::take(&mut cx.dropped_objects), std::mem::take(&mut cx.columnar_inserted));
        t.written_objects.append(&mut cx.written_objects);
        t.dropped_objects.extend(dropped);
        for (table, (rows, blocks)) in inserted {
            let e = t.columnar_inserted.entry(table).or_default();
            e.0 += rows;
            e.1 += blocks;
        }
        Ok(QueryResult { tag: format!("INSERT 0 {affected}"), rows_affected: affected, ..Default::default() })
    }

    // ---------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------

    pub(crate) fn begin(&mut self, isolation: Option<Isolation>, explicit: bool) -> Result<()> {
        let isolation = isolation.unwrap_or(self.state.default_isolation);
        let shared = self.shared.clone();
        let recovery = shared.in_recovery();
        let gate = shared.replay_shared();
        let catalog = self.latest_catalog();
        // Reuse the previous transaction object where there is one.
        let mut txn = match self.spare_txn.take() {
            Some(mut txn) => {
                txn.restart(isolation);
                txn
            }
            None => self.shared.store.begin(isolation),
        };
        drop(gate);
        // A replica only reads, and so does a read-only login.
        txn.set_read_only(recovery || self.state.read_only_login || self.state.default_read_only);
        self.txn = Some(TxnState {
            txn,
            catalog,
            dirty: false,
            failed: false,
            explicit,
            batch: false,
            savepoints: Vec::new(),
            start_time: now_micros(),
            reset_counters: Vec::new(),
            dropped_objects: Vec::new(),
            columnar_inserted: HashMap::new(),
            written_objects: Vec::new(),
        });
        Ok(())
    }

    /// Ends the open transaction and starts a new one with `isolation`,
    /// keeping the settings it changed (`SET TRANSACTION ISOLATION LEVEL`
    /// after `BEGIN`).
    pub(crate) fn restart_transaction(&mut self, isolation: Option<Isolation>, explicit: bool) -> Result<()> {
        let settings = std::mem::take(&mut self.state.guc_txn);
        self.rollback();
        self.state.guc_txn = settings;
        self.begin(isolation, explicit)
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        let Some(t) = self.txn.take() else { return Ok(()) };
        self.shared.advisory.release_all(self.state.pid, true, false);
        if t.failed {
            self.state.end_settings(false);
            return Ok(());
        }
        let TxnState { mut txn, catalog, dirty, dropped_objects, columnar_inserted, written_objects, .. } = t;
        let result = txn.end(true);
        self.spare_txn = Some(txn);
        self.state.end_settings(result.is_ok());
        if let Err(e) = result {
            // A failed commit may still have reached the log (an error
            // while syncing it), so the objects it wrote may be named by
            // a manifest after all. They stay; if they are garbage,
            // `VACUUM` removes them.
            self.shared.columnar.pending_done(&written_objects);
            return Err(e.into());
        }
        if dirty {
            self.shared.publish_catalog(catalog);
        }
        let columnar = &self.shared.columnar;
        // Segment objects the transaction replaced are no longer named by
        // any manifest a new snapshot can read.
        // They are deleted once the snapshots that began before this
        // commit, and may still read the old manifest, are gone.
        if !dropped_objects.is_empty() {
            columnar.retire(self.shared.store.last_committed(), dropped_objects);
        }
        if !written_objects.is_empty() {
            columnar.pending_done(&written_objects);
        }
        if columnar.has_retired() {
            columnar.sweep(self.shared.store.oldest_snapshot(), &*self.shared.objects());
        }
        // The background compactor decides when deltas are compacted.
        if !columnar_inserted.is_empty() {
            let config = columnar.config.lock().clone();
            columnar.compactor.note_inserted(&config, &columnar_inserted);
        }
        Ok(())
    }

    /// Deletes segment objects of a transaction that did not commit.
    fn discard_objects(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        let store = self.shared.objects();
        for k in keys {
            let _ = store.delete(k);
        }
        self.shared.columnar.pending_done(keys);
    }

    /// Moves the whole delta of a columnar table into segments and merges
    /// small segments (`VACUUM`). Returns the number of rows moved.
    pub fn compact_table(&mut self, table_id: u32) -> Result<u64> {
        let mut moved = 0;
        loop {
            let n = self.compact_step(table_id, Step::Delta, None)?;
            moved += n;
            if n == 0 {
                break;
            }
        }
        while self.compact_step(table_id, Step::Merge, None)? > 0 {}
        self.collect_table_garbage(table_id);
        Ok(moved)
    }

    /// One background compaction of a table: a bounded part of the delta,
    /// then merges of small segments.
    pub(crate) fn compact_background(&mut self, table_id: u32) -> Result<u64> {
        // Do not wait behind DDL for long; the table is retried later.
        let wait = Some(Duration::from_millis(200));
        let moved = self.compact_step(table_id, Step::Delta, wait)?;
        while self.compact_step(table_id, Step::Merge, wait)? > 0 {}
        // More rows than one run takes: run again soon.
        if moved as usize >= crate::columnar::COMPACT_MAX_ROWS {
            self.shared.columnar.compactor.mark_due(table_id);
        }
        self.collect_table_garbage(table_id);
        Ok(moved)
    }

    /// Removes the tombstones compaction leaves in a table's key range.
    fn collect_table_garbage(&self, table_id: u32) {
        if let Some(next) = table_id.checked_add(1) {
            let lo = crate::catalog::object_prefix(table_id);
            let hi = crate::catalog::object_prefix(next);
            let _ = self.shared.store.gc_range(&lo, &hi);
        }
    }

    /// Runs one compaction step in a transaction of its own. Returns rows
    /// moved (delta) or segments merged.
    fn compact_step(&mut self, table_id: u32, step: Step, wait: Option<Duration>) -> Result<u64> {
        self.shared.check_writable("compaction")?;
        if self.txn.is_some() {
            return Err(Error::new(
                sqlstate::ACTIVE_SQL_TRANSACTION,
                "compaction cannot run inside a transaction block",
            ));
        }
        self.begin(Some(Isolation::ReadCommitted), false)?;
        // Compaction changes no data; it runs for read-only sessions too
        // (`VACUUM`), but not on a replica (checked above).
        if let Some(t) = &mut self.txn {
            t.txn.set_read_only(false);
        }
        let result = (|| {
            let wait = match wait {
                Some(d) => WaitPolicy::Wait(Some(d)),
                None => WaitPolicy::Wait(self.state.lock_timeout),
            };
            let t = self.txn.as_mut().unwrap();
            // Like a writer: DDL on the table waits, and the definition
            // read below is the one in force while the lock is held.
            t.txn.lock(&table_lock_key(table_id), LockMode::Shared, wait)?;
            t.txn.refresh_snapshot();
            let catalog = match t.txn.get_current(CATALOG_KEY)? {
                Some(bytes) => Arc::new(Catalog::from_bytes(&bytes)?),
                None => Arc::new(Catalog::bootstrap()),
            };
            let Some(table) = catalog.table_by_id(table_id).filter(|t| t.columnar.is_some()).cloned() else {
                return Ok(0);
            };
            t.catalog = catalog;
            self.with_cx(|cx| match step {
                Step::Delta => crate::columnar::compact_delta(&table, cx),
                Step::Merge => crate::columnar::merge_segments(&table, cx).map(|n| n as u64),
            })
        })();
        match result {
            Ok(n) => {
                self.commit()?;
                Ok(n)
            }
            Err(e) => {
                self.rollback();
                Err(e)
            }
        }
    }

    pub(crate) fn rollback(&mut self) {
        if let Some(mut t) = self.txn.take() {
            self.shared.advisory.release_all(self.state.pid, true, false);
            self.state.end_settings(false);
            let _ = t.txn.end(false);
            self.spare_txn = Some(t.txn);
            for id in t.reset_counters {
                let _ = self.shared.reset_counter(id);
            }
            self.discard_objects(&t.written_objects);
        }
    }

    pub(crate) fn savepoint(&mut self, name: &str) -> Result<()> {
        let t = self.txn.as_mut().filter(|t| t.explicit).ok_or_else(|| {
            Error::new(sqlstate::NO_ACTIVE_SQL_TRANSACTION, "SAVEPOINT can only be used in transaction blocks")
        })?;
        let mark = t.txn.savepoint();
        let settings = self.state.settings_mark();
        t.savepoints.push(Savepoint {
            name: name.to_string(),
            mark,
            catalog: t.catalog.clone(),
            dirty: t.dirty,
            settings,
        });
        Ok(())
    }

    pub(crate) fn rollback_to(&mut self, name: &str) -> Result<()> {
        let t = self.txn.as_mut().ok_or_else(|| {
            Error::new(
                sqlstate::NO_ACTIVE_SQL_TRANSACTION,
                "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
            )
        })?;
        let pos = t.savepoints.iter().rposition(|s| s.name == name).ok_or_else(|| {
            Error::new(sqlstate::INVALID_SAVEPOINT_SPECIFICATION, format!("savepoint \"{name}\" does not exist"))
        })?;
        t.savepoints.truncate(pos + 1);
        let sp = &t.savepoints[pos];
        t.txn.rollback_to(sp.mark);
        t.catalog = sp.catalog.clone();
        t.dirty = sp.dirty;
        t.failed = false;
        self.state.rollback_settings_to(&sp.settings);
        Ok(())
    }

    pub(crate) fn release(&mut self, name: &str) -> Result<()> {
        let t = self.txn.as_mut().ok_or_else(|| {
            Error::new(sqlstate::NO_ACTIVE_SQL_TRANSACTION, "RELEASE SAVEPOINT can only be used in transaction blocks")
        })?;
        let pos = t.savepoints.iter().rposition(|s| s.name == name).ok_or_else(|| {
            Error::new(sqlstate::INVALID_SAVEPOINT_SPECIFICATION, format!("savepoint \"{name}\" does not exist"))
        })?;
        t.savepoints.truncate(pos);
        Ok(())
    }

    // ---------------------------------------------------------------
    // Catalog changes
    // ---------------------------------------------------------------

    /// Runs a schema change: `f` edits a private copy of the catalog,
    /// which is stored and becomes this transaction's view.
    pub(crate) fn alter_catalog<R>(
        &mut self,
        f: impl FnOnce(&mut Catalog, &mut Txn, &Arc<Shared>, &mut SessionState) -> Result<R>,
    ) -> Result<R> {
        let wait = WaitPolicy::Wait(self.state.lock_timeout);
        self.start_statement()?;
        let shared = self.shared.clone();
        let t = self.txn.as_mut().unwrap();
        if t.txn.is_read_only() {
            return Err(Error::new(
                sqlstate::READ_ONLY_SQL_TRANSACTION,
                "cannot execute DDL in a read-only transaction",
            ));
        }
        // One schema-changing transaction at a time; after acquiring the
        // lock, start from the committed catalog rather than a cached one.
        t.txn.lock(DDL_LOCK_KEY, LockMode::Exclusive, wait)?;
        if !t.dirty {
            t.catalog = match t.txn.get_current(CATALOG_KEY)? {
                Some(bytes) => Arc::new(Catalog::from_bytes(&bytes)?),
                None => Arc::new(Catalog::bootstrap()),
            };
        }
        let mut catalog = (*t.catalog).clone();
        let before = catalog.to_bytes();
        let result = f(&mut catalog, &mut t.txn, &shared, &mut self.state)?;
        // A statement that changed nothing (`IF NOT EXISTS` on an existing
        // object) must not invalidate every cached plan.
        if catalog.to_bytes() == before {
            return Ok(result);
        }
        catalog.version += 1;
        t.txn.put_blind(CATALOG_KEY, &catalog.to_bytes())?;
        t.catalog = Arc::new(catalog);
        t.dirty = true;
        Ok(result)
    }

    /// Takes the exclusive table lock that keeps writers out during DDL.
    pub(crate) fn lock_table_exclusive(txn: &mut Txn, id: u32, wait: WaitPolicy) -> Result<()> {
        txn.lock(&table_lock_key(id), LockMode::Exclusive, wait)?;
        Ok(())
    }

    /// Takes the table lock in `mode` until the end of the transaction:
    /// shared is what writers hold, exclusive what DDL holds.
    pub(crate) fn lock_table(txn: &mut Txn, id: u32, mode: LockMode, wait: WaitPolicy) -> Result<()> {
        txn.lock(&table_lock_key(id), mode, wait)?;
        Ok(())
    }

    /// Runs a closure with an execution context inside the current
    /// transaction (used by DDL that must evaluate expressions or scan).
    pub(crate) fn with_cx<R>(&mut self, f: impl FnOnce(&mut ExecCx) -> Result<R>) -> Result<R> {
        let fn_ctx = self.fn_ctx();
        let t = self.txn.as_mut().ok_or_else(|| Error::internal("no transaction"))?;
        let mut cx = ExecCx::new(&mut t.txn, &t.catalog, &[], &mut self.state, &self.shared, fn_ctx, 0);
        let result = f(&mut cx);
        let dropped = std::mem::take(&mut cx.dropped_objects);
        t.dropped_objects.extend(dropped);
        t.written_objects.append(&mut cx.written_objects);
        for (table, (rows, blocks)) in std::mem::take(&mut cx.columnar_inserted) {
            let e = t.columnar_inserted.entry(table).or_default();
            e.0 += rows;
            e.1 += blocks;
        }
        result
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.rollback();
        self.shared.advisory.release_all(self.state.pid, true, true);
        if self.backend.is_some() {
            self.shared.backends.unregister(self.state.pid);
        }
    }
}

#[derive(Clone, Copy)]
enum Step {
    Delta,
    Merge,
}

fn tag_name(tag: &Tag) -> &'static str {
    match tag {
        Tag::Select => "SELECT",
        Tag::Insert => "INSERT",
        Tag::Update => "UPDATE",
        Tag::Delete => "DELETE",
        Tag::Other(t) => t,
    }
}

/// True if the plan contains a data-modifying node (a writing CTE) or
/// takes row locks.
fn plan_writes(plan: &Plan) -> bool {
    match plan {
        Plan::Insert(_) | Plan::Update(_) | Plan::Delete(_) | Plan::LockRows { .. } => true,
        Plan::Filter(i, _)
        | Plan::Project(i, _)
        | Plan::Sort { input: i, .. }
        | Plan::Limit { input: i, .. }
        | Plan::Distinct { input: i, .. }
        | Plan::Aggregate { input: i, .. }
        | Plan::Window { input: i, .. }
        | Plan::ProjectSet { input: i, .. } => plan_writes(i),
        Plan::NestedLoop { left, right, .. } | Plan::HashJoin { left, right, .. } | Plan::SetOp { left, right, .. } => {
            plan_writes(left) || plan_writes(right)
        }
        Plan::With { ctes, body } => {
            plan_writes(body)
                || ctes.iter().any(|c| plan_writes(&c.plan) || c.recursive.as_ref().is_some_and(|r| plan_writes(&r.0)))
        }
        _ => false,
    }
}

/// The source text of one statement of a multi-statement string.
fn statement_text<'s>(sql: &'s str, raw: &pb::RawStmt) -> &'s str {
    let start = raw.stmt_location.max(0) as usize;
    let end = if raw.stmt_len > 0 { start + raw.stmt_len as usize } else { sql.len() };
    sql.get(start..end.min(sql.len())).unwrap_or(sql).trim()
}

/// Splits `schema.name` (with optional double quotes) into its parts.
pub(crate) fn split_qualified(text: &str) -> (Option<String>, String) {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = text.trim().chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            c if quoted => cur.push(c),
            c => cur.extend(c.to_lowercase()),
        }
    }
    parts.push(cur);
    let name = parts.pop().unwrap_or_default();
    (parts.pop(), name)
}

impl ExecCx<'_> {
    fn sequence_by_text(&self, text: &str) -> Result<Arc<Sequence>> {
        let (schema, name) = split_qualified(text);
        self.catalog
            .sequence(schema.as_deref(), &name, &self.session.search_path)
            .cloned()
            .ok_or_else(|| Error::new(sqlstate::UNDEFINED_TABLE, format!("relation \"{text}\" does not exist")))
    }

    /// Fails in a read-only transaction, as functions with effects do in
    /// PostgreSQL.
    fn check_txn_writable(&self, what: &str) -> Result<()> {
        if self.txn.is_read_only() && !self.shared.in_recovery() {
            return Err(Error::new(
                sqlstate::READ_ONLY_SQL_TRANSACTION,
                format!("cannot execute {what} in a read-only transaction"),
            ));
        }
        Ok(())
    }

    pub(crate) fn session_fn(&mut self, func: &SessionFn, args: Vec<Value>) -> Result<Value> {
        let text = |i: usize| args.get(i).and_then(Value::as_str).unwrap_or_default();
        Ok(match func {
            SessionFn::RowId(table) => Value::Int8(self.shared.next_rowid(*table)?),
            SessionFn::NextVal => {
                if args[0].is_null() {
                    return Ok(Value::Null);
                }
                let seq = self.sequence_by_text(text(0))?;
                self.check_txn_writable("nextval()")?;
                let v = self.shared.nextval(&seq)?;
                self.session.currval.insert(seq.id, v);
                self.session.lastval = Some(v);
                Value::Int8(v)
            }
            SessionFn::CurrVal => {
                let seq = self.sequence_by_text(text(0))?;
                match self.session.currval.get(&seq.id) {
                    Some(v) => Value::Int8(*v),
                    None => {
                        return Err(Error::new(
                            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
                            format!("currval of sequence \"{}\" is not yet defined in this session", seq.name),
                        ))
                    }
                }
            }
            SessionFn::LastVal => match self.session.lastval {
                Some(v) => Value::Int8(v),
                None => {
                    return Err(Error::new(
                        sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        "lastval is not yet defined in this session",
                    ))
                }
            },
            SessionFn::SetVal => {
                if args.iter().take(2).any(Value::is_null) {
                    return Ok(Value::Null);
                }
                let seq = self.sequence_by_text(text(0))?;
                self.check_txn_writable("setval()")?;
                let v = args[1].as_i64().ok_or_else(|| Error::internal("setval value is not an integer"))?;
                let called = args.get(2).and_then(Value::as_bool).unwrap_or(true);
                self.shared.setval(&seq, v, called)?;
                if called {
                    self.session.currval.insert(seq.id, v);
                    self.session.lastval = Some(v);
                }
                Value::Int8(v)
            }
            SessionFn::CurrentSchema => {
                let found = self.session.search_path.iter().find(|s| self.catalog.has_schema(s));
                found.map_or(Value::Null, |s| Value::Text(s.clone()))
            }
            SessionFn::CurrentSchemas => {
                let mut list: Vec<Value> = Vec::new();
                if args.first().and_then(Value::as_bool) == Some(true) {
                    list.push(Value::text("pg_catalog"));
                }
                list.extend(
                    self.session
                        .search_path
                        .iter()
                        .filter(|s| self.catalog.has_schema(s))
                        .map(|s| Value::Text(s.clone())),
                );
                Value::Array(list)
            }
            SessionFn::CurrentUser => Value::Text(self.session.user.clone()),
            SessionFn::CurrentDatabase => Value::Text(self.shared.database_name.clone()),
            SessionFn::Version => Value::text(crate::VERSION_STRING),
            SessionFn::TxidCurrent => Value::Int8(self.txn.id() as i64),
            SessionFn::BackendPid => Value::Int4(self.session.pid),
            SessionFn::CurrentSetting => {
                if args[0].is_null() {
                    return Ok(Value::Null);
                }
                let missing_ok = args.get(1).and_then(Value::as_bool).unwrap_or(false);
                match crate::ddl::show_setting(self.session, text(0)) {
                    Ok(v) => Value::Text(v),
                    Err(_) if missing_ok => Value::Null,
                    Err(e) => return Err(e),
                }
            }
            SessionFn::SetConfig => {
                let name = match &args[0] {
                    Value::Null => {
                        return Err(Error::new(sqlstate::NULL_VALUE_NOT_ALLOWED, "SET requires parameter name"))
                    }
                    v => v.as_str().unwrap_or_default().to_string(),
                };
                // A NULL value resets the setting.
                let value = args.get(1).and_then(Value::as_str).map(str::to_string);
                let local = args.get(2).and_then(Value::as_bool).unwrap_or(false);
                crate::ddl::check_settable(self.session, &name)?;
                self.session.change_settings(true, local, &mut |state| {
                    crate::ddl::apply_setting(state, &name, value.as_deref())
                })?;
                Value::Text(crate::ddl::show_setting(self.session, &name)?)
            }
            SessionFn::Catalog(name) => crate::system::catalog_fn(name, args, self)?,
        })
    }
}

/// Maps between types and the OIDs clients see on the wire, including
/// user-defined enum types, which get their OID from the catalog.
#[derive(Clone)]
pub struct TypeResolver(Arc<Catalog>);

/// Array types of user-defined enums live above this OID.
const ENUM_ARRAY_OID_BASE: u32 = 2_000_000_000;

impl TypeResolver {
    /// OID to report for a value of type `ty`.
    pub fn oid(&self, ty: &Type) -> u32 {
        let find = |name: &str| self.0.enums.values().find(|e| e.name == name);
        match ty {
            Type::Enum(name) => find(name).map_or(25, |e| crate::catalog::FIRST_USER_OID + e.id),
            Type::Array(elem) => match &**elem {
                Type::Enum(name) => find(name).map_or(1009, |e| ENUM_ARRAY_OID_BASE + e.id),
                _ => ty.oid(),
            },
            _ => ty.oid(),
        }
    }

    /// The type a client means by `oid`, if it is known.
    pub fn from_oid(&self, oid: u32) -> Option<Type> {
        if let Some(t) = Type::from_oid(oid) {
            return Some(t);
        }
        let by_id = |id: u32| self.0.enums.values().find(|e| e.id == id).map(|e| Type::Enum(e.name.clone()));
        if oid >= ENUM_ARRAY_OID_BASE {
            return by_id(oid - ENUM_ARRAY_OID_BASE).map(|t| Type::Array(Box::new(t)));
        }
        oid.checked_sub(crate::catalog::FIRST_USER_OID).and_then(by_id)
    }
}

impl Session {
    /// A resolver for type OIDs against the catalog this session sees.
    pub fn types(&self) -> TypeResolver {
        TypeResolver(self.catalog())
    }
}
