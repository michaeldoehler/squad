//! The embedding API: [`Database`], [`Connection`], [`Statement`], [`Row`].

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use squad_kv::Store;
use squad_object::ObjectStore;
use squad_storage::{
    DiskEngine, DiskOptions, Engine, EngineStats, MemEngine, RemoteConfig, SyncMode, TieredEngine, VerifyReport,
};

use crate::bind::OutputColumn;
use crate::convert::{FromSql, ToSql};
use crate::session::{Prepared, QueryResult, Session, Shared};
use crate::types::{Type, Value};
use crate::{sqlstate, Error, Result};

/// Options for opening a database.
#[derive(Debug, Clone)]
pub struct Config {
    /// How commits are made durable. See [`SyncMode`].
    pub sync_mode: SyncMode,
    /// Size of the page cache in bytes.
    pub cache_size: usize,
    /// Write-ahead log size that triggers an automatic checkpoint.
    pub checkpoint_wal_bytes: u64,
    /// Keeps a complete copy of the database in an object store (a
    /// directory or an S3-compatible bucket), from which it is restored
    /// when the local directory is empty. `None`: local storage only.
    pub remote: Option<RemoteConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Config { sync_mode: SyncMode::Sync, cache_size: 128 << 20, checkpoint_wal_bytes: 64 << 20, remote: None }
    }
}

/// An open database. Cloning yields another handle to the same database;
/// it is closed when the last handle and connection are dropped.
#[derive(Clone)]
pub struct Database {
    shared: Arc<Shared>,
}

impl Database {
    /// Opens the database stored in directory `path`, creating it if it
    /// does not exist. The directory can be open in one process at a time.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Database> {
        let options = DiskOptions {
            sync_mode: config.sync_mode,
            cache_pages: (config.cache_size / 8192).max(64),
            checkpoint_wal_bytes: config.checkpoint_wal_bytes,
            ..DiskOptions::default()
        };
        // Segments of columnar tables live in the remote store if there
        // is one, otherwise in a directory next to the row store.
        let (db, objects): (Database, Arc<dyn ObjectStore>) = match config.remote {
            Some(remote) => {
                let objects = remote.store.clone();
                (Database::with_engine(Arc::new(TieredEngine::open(path.as_ref(), options, remote)?))?, objects)
            }
            None => {
                let db = Database::with_engine(Arc::new(DiskEngine::open(path.as_ref(), options)?))?;
                let local = squad_object::LocalStore::new(path.as_ref().join("objects"))
                    .map_err(|e| Error::new(sqlstate::IO_ERROR, e.to_string()))?;
                (db, Arc::new(local))
            }
        };
        db.shared.set_objects(objects);
        Ok(db)
    }

    /// Downloads the database kept in `store` into the empty or missing
    /// directory `dir`, without becoming its writer. Afterwards the
    /// directory is an ordinary local database. To continue the remote
    /// copy instead, open the directory with [`Config::remote`] set.
    pub fn restore_from(store: &dyn ObjectStore, dir: impl AsRef<Path>) -> Result<()> {
        squad_storage::restore_from(store, dir.as_ref(), DiskOptions::default())?;
        Ok(())
    }

    /// Opens a read replica of the database a writer keeps in
    /// `config.store` (the store of that writer's [`Config::remote`]).
    ///
    /// An empty or missing `path` is filled from the newest snapshot; a
    /// directory holding the replica's own earlier copy is brought up to
    /// date. The replica then applies what the writer ships, every
    /// `config.poll_interval`, while it serves queries. All its sessions
    /// are read-only: statements that would write fail with SQLSTATE
    /// `25006`. See [`promote`](Database::promote) for turning it into
    /// the writer.
    pub fn open_replica(path: impl AsRef<Path>, config: crate::ReplicaConfig) -> Result<Database> {
        Ok(Database { shared: crate::replica::open(path.as_ref(), config)? })
    }

    /// Where this replica stands relative to its writer. `None` for a
    /// database that was not opened with
    /// [`open_replica`](Database::open_replica).
    pub fn replication_status(&self) -> Option<crate::ReplicationStatus> {
        crate::replica::status(&self.shared)
    }

    /// Applies everything the writer has shipped so far instead of
    /// waiting for the next poll, and returns the resulting position.
    pub fn replication_catch_up(&self) -> Result<crate::ReplicationStatus> {
        crate::replica::catch_up(&self.shared)
    }

    /// Makes this replica the writer: it claims the database in the
    /// object store (the previous writer is refused from then on),
    /// applies every commit that writer shipped, and starts shipping its
    /// own. New transactions are read-write afterwards. Does nothing if
    /// the replica was promoted before; on error it stays a replica.
    pub fn promote(&self) -> Result<()> {
        crate::replica::promote(&self.shared)
    }

    /// True while this database is a read replica.
    pub fn is_replica(&self) -> bool {
        self.shared.in_recovery()
    }

    /// Opens a volatile database that lives in memory only.
    pub fn open_in_memory() -> Result<Database> {
        Database::with_engine(Arc::new(MemEngine::new()))
    }

    /// Opens a database on a caller-supplied storage engine.
    pub fn with_engine(engine: Arc<dyn Engine>) -> Result<Database> {
        let store = Store::new(engine)?;
        Ok(Database { shared: Shared::open(store, "squad")? })
    }

    /// Opens a new connection (session). Connections are independent:
    /// each has its own transaction state and settings.
    pub fn connect(&self) -> Connection {
        Connection { inner: Arc::new(Mutex::new(Session::new(self.shared.clone()))) }
    }

    /// Opens a raw session, the interface protocol servers build on.
    pub fn session(&self) -> Session {
        Session::new(self.shared.clone())
    }

    /// Writes all changes to the data file and truncates the log.
    pub fn checkpoint(&self) -> Result<()> {
        Ok(self.shared.store.engine().checkpoint()?)
    }

    /// Writes a consistent copy of the database into directory `dest`
    /// while the database stays in use.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        Ok(self.shared.store.engine().backup(dest.as_ref())?)
    }

    /// Checks the integrity of the stored data.
    pub fn verify(&self) -> Result<VerifyReport> {
        Ok(self.shared.store.engine().verify()?)
    }

    /// Removes row versions no transaction can see any more. This also
    /// happens incrementally during normal operation.
    pub fn vacuum(&self) -> Result<u64> {
        self.shared.check_writable("VACUUM")?;
        Ok(self.shared.store.gc()?)
    }

    /// Storage engine counters.
    pub fn stats(&self) -> EngineStats {
        self.shared.store.engine().stats()
    }
}

/// A database session. A connection can be sent to another thread; to use
/// the database from several threads at once, open one connection each.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Mutex<Session>>,
}

fn to_values(params: &[&dyn ToSql]) -> Vec<Value> {
    params.iter().map(|p| p.to_value()).collect()
}

impl Connection {
    /// Prepares a statement for repeated execution.
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        let prepared = self.inner.lock().prepare(sql, &[])?;
        Ok(Statement { conn: self.clone(), prepared })
    }

    /// Runs a statement and returns the number of rows it changed.
    pub fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        Ok(self.run(sql, params)?.rows_affected)
    }

    /// Runs a query and returns all rows.
    pub fn query(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        Ok(rows_of(self.run(sql, params)?))
    }

    /// Runs a query that must return exactly one row.
    pub fn query_one(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Row> {
        exactly_one(self.query(sql, params)?)
    }

    /// Runs a query that returns at most one row.
    pub fn query_opt(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Option<Row>> {
        at_most_one(self.query(sql, params)?)
    }

    /// Runs one or more statements separated by semicolons, without
    /// parameters. Several statements run as one transaction unless the
    /// text controls transactions itself.
    pub fn batch_execute(&self, sql: &str) -> Result<()> {
        self.inner.lock().execute(sql).map(|_| ())
    }

    /// Like [`batch_execute`](Connection::batch_execute), returning the
    /// result of every statement.
    pub fn simple_query(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.inner.lock().execute(sql)
    }

    fn run(&self, sql: &str, params: &[&dyn ToSql]) -> Result<QueryResult> {
        let mut session = self.inner.lock();
        if params.is_empty() {
            let mut results = session.execute(sql)?;
            return results.pop().ok_or_else(|| Error::syntax("empty statement"));
        }
        let values = to_values(params);
        let types: Vec<Option<Type>> =
            values.iter().map(|v| if v.is_null() { None } else { Some(v.type_of()) }).collect();
        let prepared = session.prepare(sql, &types)?;
        session.execute_prepared(&prepared, &values)
    }

    /// Starts a transaction that is rolled back when the returned guard
    /// is dropped without [`commit`](Transaction::commit).
    pub fn transaction(&self) -> Result<Transaction<'_>> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction { conn: self, done: false })
    }

    /// True while a transaction block is open on this connection.
    pub fn in_transaction(&self) -> bool {
        self.inner.lock().status() != crate::session::TxnStatus::Idle
    }
}

/// An open transaction. Dereferences to the [`Connection`] it runs on.
pub struct Transaction<'a> {
    conn: &'a Connection,
    done: bool,
}

impl Transaction<'_> {
    pub fn commit(mut self) -> Result<()> {
        self.done = true;
        self.conn.batch_execute("COMMIT")
    }

    pub fn rollback(mut self) -> Result<()> {
        self.done = true;
        self.conn.batch_execute("ROLLBACK")
    }
}

impl std::ops::Deref for Transaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.conn.batch_execute("ROLLBACK");
        }
    }
}

/// A prepared statement, bound to the connection that created it.
#[derive(Clone)]
pub struct Statement {
    conn: Connection,
    prepared: Arc<Prepared>,
}

impl Statement {
    fn run(&self, params: &[&dyn ToSql]) -> Result<QueryResult> {
        self.conn.inner.lock().execute_prepared(&self.prepared, &to_values(params))
    }

    /// Runs the statement and returns the number of rows it changed.
    pub fn execute(&self, params: &[&dyn ToSql]) -> Result<u64> {
        Ok(self.run(params)?.rows_affected)
    }

    pub fn query(&self, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        Ok(rows_of(self.run(params)?))
    }

    pub fn query_one(&self, params: &[&dyn ToSql]) -> Result<Row> {
        exactly_one(self.query(params)?)
    }

    pub fn query_opt(&self, params: &[&dyn ToSql]) -> Result<Option<Row>> {
        at_most_one(self.query(params)?)
    }

    /// Result columns of the statement.
    pub fn columns(&self) -> &[OutputColumn] {
        &self.prepared.columns
    }

    /// Types of the statement's `$n` parameters.
    pub fn param_types(&self) -> &[Type] {
        &self.prepared.param_types
    }
}

fn rows_of(result: QueryResult) -> Vec<Row> {
    let columns = Arc::new(result.columns);
    result.rows.into_iter().map(|values| Row { columns: columns.clone(), values }).collect()
}

fn exactly_one(mut rows: Vec<Row>) -> Result<Row> {
    match rows.len() {
        1 => Ok(rows.pop().unwrap()),
        0 => Err(Error::new(sqlstate::CARDINALITY_VIOLATION, "query returned no rows")),
        n => Err(Error::new(sqlstate::CARDINALITY_VIOLATION, format!("query returned {n} rows, expected one"))),
    }
}

fn at_most_one(mut rows: Vec<Row>) -> Result<Option<Row>> {
    match rows.len() {
        0 | 1 => Ok(rows.pop()),
        n => Err(Error::new(sqlstate::CARDINALITY_VIOLATION, format!("query returned {n} rows, expected at most one"))),
    }
}

/// One row of a query result.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Arc<Vec<OutputColumn>>,
    values: Vec<Value>,
}

/// A way to pick a column of a [`Row`]: by position or by name.
pub trait RowIndex {
    fn index(&self, columns: &[OutputColumn]) -> Option<usize>;
    fn describe(&self) -> String;
}

impl RowIndex for usize {
    fn index(&self, columns: &[OutputColumn]) -> Option<usize> {
        (*self < columns.len()).then_some(*self)
    }
    fn describe(&self) -> String {
        format!("index {self}")
    }
}

impl RowIndex for &str {
    fn index(&self, columns: &[OutputColumn]) -> Option<usize> {
        columns.iter().position(|c| c.name == *self)
    }
    fn describe(&self) -> String {
        format!("\"{self}\"")
    }
}

impl Row {
    /// Converts the value of a column. Panics if the column does not exist
    /// or the value cannot be converted; see [`try_get`](Row::try_get).
    pub fn get<T: FromSql>(&self, index: impl RowIndex) -> T {
        match self.try_get(index) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn try_get<T: FromSql>(&self, index: impl RowIndex) -> Result<T> {
        let i = index
            .index(&self.columns)
            .ok_or_else(|| Error::new(sqlstate::UNDEFINED_COLUMN, format!("no column {} in row", index.describe())))?;
        T::from_value(&self.values[i])
    }

    pub fn columns(&self) -> &[OutputColumn] {
        &self.columns
    }

    /// The raw values of the row.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
