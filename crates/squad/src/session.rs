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
            database_name: database_name.to_string(),
            catalog_version: Arc::new(std::sync::atomic::AtomicU64::new(version)),
            recovery: AtomicBool::new(false),
            replay_gate: Arc::new(RwLock::new(())),
            replica: std::sync::OnceLock::new(),
        }))
    }

    /// True while this database is a read replica that follows a writer.
    pub fn in_recovery(&self) -> bool {
        self.recovery.load(Ordering::Acquire)
    }

    pub(crate) fn set_recovery(&self, on: bool) -> bool {
        self.recovery.swap(on, Ordering::AcqRel)
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

/// Per-session settings and state.
pub struct SessionState {
    pub search_path: Vec<String>,
    pub user: String,
    pub settings: HashMap<String, String>,
    pub tz: chrono_tz::Tz,
    pub lock_timeout: Option<Duration>,
    pub currval: HashMap<u32, i64>,
    pub lastval: Option<i64>,
    pub default_isolation: Isolation,
    pub pid: i32,
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
    search_path: Vec<String>,
}

impl Prepared {
    /// True for statements that return rows.
    pub fn returns_rows(&self) -> bool {
        !self.columns.is_empty()
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
                settings: HashMap::new(),
                tz: chrono_tz::UTC,
                lock_timeout: None,
                currval: HashMap::new(),
                lastval: None,
                default_isolation: Isolation::ReadCommitted,
                pid,
            },
            txn: None,
            named: HashMap::new(),
            catalog_cache,
            replans: HashMap::new(),
            spare_txn: None,
            _guard: None,
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
            search_path: Vec::new(),
        }
    }

    /// An error while parsing or analyzing a statement aborts the open
    /// transaction, just like an error while running it.
    fn abort_on_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            if let Some(t) = &mut self.txn {
                t.failed = true;
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
            search_path: self.state.search_path.clone(),
        })
    }

    /// Returns a plan for ad-hoc SQL, reusing a cached one when the
    /// catalog has not changed.
    fn cache_lookup(&self, sql: &str) -> Option<Arc<Prepared>> {
        if self.txn.as_ref().is_some_and(|t| t.dirty) {
            return None;
        }
        let key = format!("{}\u{0}{}", self.state.search_path.join(","), sql);
        let hit = self.shared.plan_cache.lock().get(&key).cloned()?;
        (hit.catalog_version == self.catalog_version()).then_some(hit)
    }

    fn cached(&mut self, sql: &str) -> Result<Arc<Prepared>> {
        let catalog = self.catalog();
        let private = self.txn.as_ref().is_some_and(|t| t.dirty);
        let key = format!("{}\u{0}{}", self.state.search_path.join(","), sql);
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
            Kind::Plan { .. } => stmt.catalog_version != current_version || stmt.search_path != self.state.search_path,
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
                    && new.search_path == self.state.search_path
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
        Ok(QueryResult { columns: stmt.columns.clone(), rows, tag, rows_affected })
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
                    let key = format!("{}\u{0}{}", self.state.search_path.join(","), sql);
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
        // A replica only reads.
        txn.set_read_only(recovery);
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

    pub(crate) fn commit(&mut self) -> Result<()> {
        let Some(t) = self.txn.take() else { return Ok(()) };
        if t.failed {
            return Ok(());
        }
        let TxnState { mut txn, catalog, dirty, dropped_objects, columnar_inserted, written_objects, .. } = t;
        let result = txn.end(true);
        self.spare_txn = Some(txn);
        if let Err(e) = result {
            // Nothing was applied: objects written for it are garbage.
            self.discard_objects(&written_objects);
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
        t.savepoints.push(Savepoint { name: name.to_string(), mark, catalog: t.catalog.clone(), dirty: t.dirty });
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

    pub(crate) fn session_fn(&mut self, func: &SessionFn, args: Vec<Value>) -> Result<Value> {
        let text = |i: usize| args.get(i).and_then(Value::as_str).unwrap_or_default();
        Ok(match func {
            SessionFn::RowId(table) => Value::Int8(self.shared.next_rowid(*table)?),
            SessionFn::NextVal => {
                if args[0].is_null() {
                    return Ok(Value::Null);
                }
                let seq = self.sequence_by_text(text(0))?;
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
                let (name, value) = (text(0).to_string(), text(1).to_string());
                crate::ddl::apply_setting(self.session, &name, Some(&value))?;
                Value::Text(value)
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
