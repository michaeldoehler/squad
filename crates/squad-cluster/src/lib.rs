//! Sharding for squad.
//!
//! A [`Cluster`] is a fixed set of squad databases ("shards"), local
//! directories in this process or remote squad servers, that together hold
//! one logical database. Tables are either *distributed* (every row lives
//! on the shard its distribution column hashes to) or *reference* tables
//! (every shard holds a full copy).
//!
//! ```no_run
//! use squad_cluster::{Cluster, ClusterConfig, ShardSpec};
//!
//! # fn main() -> squad::Result<()> {
//! let cluster = Cluster::open(ClusterConfig::new(vec![
//!     ShardSpec::local("data/shard-0"),
//!     ShardSpec::local("data/shard-1"),
//! ]))?;
//! let db = cluster.connect();
//! db.batch_execute(
//!     "CREATE TABLE events (project_id bigint, name text, at timestamptz) USING columnar;
//!      SELECT squad_distribute('events', 'project_id');",
//! )?;
//! db.execute("INSERT INTO events VALUES ($1, $2, now())", &[&7i64, &"signup"])?;
//! // One shard:
//! db.query("SELECT count(*) FROM events WHERE project_id = $1", &[&7i64])?;
//! // All shards, merged:
//! db.query("SELECT name, count(*) FROM events GROUP BY name ORDER BY 2 DESC", &[])?;
//! # Ok(())
//! # }
//! ```
//!
//! Statements are routed by their parse tree. A statement that names the
//! distribution key of every distributed table it reads runs unchanged on
//! one shard; a SELECT without it is scattered to all shards and merged;
//! what cannot be answered correctly is rejected with SQLSTATE `0A000`
//! and a hint. There are no cross-shard transactions and the shard count
//! is fixed. `docs/sharding.md` in the repository describes the model.

mod route;
mod scatter;
mod shard;
mod tree;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use squad::{sqlstate, Database, Error, FnCtx, FromSql, OutputColumn, Result, RowIndex, ToSql, Type, Value};

use route::{InsertRouting, KeyGroups, Plan};
use scatter::Scatter;
use shard::{Output, ShardConn};
use tree::unsupported;

/// Default number of virtual slots keys hash into.
pub const VIRTUAL_SLOTS: usize = 4096;

pub(crate) const META_TABLE: &str = "squad_cluster_meta";

/// Cached plans and parsed statements kept per cluster.
const PLAN_CACHE: usize = 4096;

/// Where one shard lives.
// A spec is built once per shard when a cluster opens; the size of the
// `Local` variant (it carries the whole database configuration) does not
// matter there, and boxing it would make the spec harder to write.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ShardSpec {
    /// A database directory opened in this process.
    Local { dir: PathBuf, config: squad::Config },
    /// A database in memory, for tests.
    Memory,
    /// A squad server, as a `postgres://` URL.
    Remote { url: String },
}

impl ShardSpec {
    /// A local shard with the default configuration.
    pub fn local(dir: impl Into<PathBuf>) -> ShardSpec {
        ShardSpec::Local { dir: dir.into(), config: squad::Config::default() }
    }

    pub fn remote(url: impl Into<String>) -> ShardSpec {
        ShardSpec::Remote { url: url.into() }
    }
}

/// Configuration of [`Cluster::open`].
#[derive(Clone)]
pub struct ClusterConfig {
    /// The shards, in their fixed order. The order is part of the
    /// cluster's identity: it is recorded on every shard.
    pub shards: Vec<ShardSpec>,
    /// Number of virtual slots, used when the cluster is created. An
    /// existing cluster keeps the number it was created with.
    pub virtual_slots: usize,
}

impl ClusterConfig {
    pub fn new(shards: Vec<ShardSpec>) -> ClusterConfig {
        ClusterConfig { shards, virtual_slots: VIRTUAL_SLOTS }
    }
}

/// One shard of an open cluster.
pub struct Shard {
    index: usize,
    kind: ShardKind,
}

enum ShardKind {
    Local(Database),
    Remote(String),
}

impl Shard {
    pub fn index(&self) -> usize {
        self.index
    }

    /// The database of a local shard, for backups, checkpoints and
    /// statistics.
    pub fn database(&self) -> Option<&Database> {
        match &self.kind {
            ShardKind::Local(db) => Some(db),
            ShardKind::Remote(_) => None,
        }
    }

    /// The URL of a remote shard.
    pub fn url(&self) -> Option<&str> {
        match &self.kind {
            ShardKind::Local(_) => None,
            ShardKind::Remote(url) => Some(url),
        }
    }

    fn connect(&self) -> Result<ShardConn> {
        match &self.kind {
            ShardKind::Local(db) => Ok(ShardConn::Local { conn: db.connect(), stmts: HashMap::new() }),
            ShardKind::Remote(url) => ShardConn::connect_remote(url),
        }
    }
}

/// How the values of a distribution column are hashed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyKind {
    Int,
    Text,
    Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Class {
    Distributed { column: String, kind: KeyKind },
    Reference,
}

impl Class {
    fn encode(&self) -> String {
        match self {
            Class::Distributed { column, kind } => {
                let kind = match kind {
                    KeyKind::Int => "int",
                    KeyKind::Text => "text",
                    KeyKind::Uuid => "uuid",
                };
                format!("distributed:{kind}:{column}")
            }
            Class::Reference => "reference".into(),
        }
    }

    fn decode(s: &str) -> Option<Class> {
        if s == "reference" {
            return Some(Class::Reference);
        }
        let mut parts = s.splitn(3, ':');
        if parts.next()? != "distributed" {
            return None;
        }
        let kind = match parts.next()? {
            "int" => KeyKind::Int,
            "text" => KeyKind::Text,
            "uuid" => KeyKind::Uuid,
            _ => return None,
        };
        Some(Class::Distributed { column: parts.next()?.to_string(), kind })
    }
}

/// The class of a table, as [`Cluster::tables`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableClass {
    /// Rows live on the shard their distribution column hashes to.
    Distributed { column: String },
    /// Every shard holds all rows.
    Reference,
}

/// Cluster metadata, the same on every shard.
pub(crate) struct Meta {
    pub tables: HashMap<String, Class>,
    /// Shard of every virtual slot.
    pub slots: Vec<u32>,
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Finalizer, so that small integers spread over the slots.
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

fn key_hash(kind: KeyKind, value: &Value) -> Result<u64> {
    let bad = |v: &Value| {
        Error::new(
            sqlstate::DATATYPE_MISMATCH,
            format!("a value of type {} cannot be used as a distribution key of this table", v.type_of().name()),
        )
    };
    if value.is_null() {
        return Err(Error::new(sqlstate::NOT_NULL_VIOLATION, "the distribution column cannot be null"));
    }
    Ok(match kind {
        KeyKind::Int => {
            let n = match value {
                Value::Int2(i) => *i as i64,
                Value::Int4(i) => *i as i64,
                Value::Int8(i) => *i,
                Value::Text(s) => s.trim().parse::<i64>().map_err(|_| Error::invalid_text("integer", s))?,
                Value::Numeric(_) => {
                    let s = squad::value::format_value(value, &FnCtx::default());
                    s.parse::<i64>().map_err(|_| bad(value))?
                }
                other => return Err(bad(other)),
            };
            fnv(&n.to_le_bytes())
        }
        KeyKind::Text => match value {
            Value::Text(s) => fnv(s.as_bytes()),
            other => fnv(squad::value::format_value(other, &FnCtx::default()).as_bytes()),
        },
        KeyKind::Uuid => match value {
            Value::Uuid(b) => fnv(b),
            Value::Text(_) => match squad::value::cast(value.clone(), &Type::Uuid, &FnCtx::default())? {
                Value::Uuid(b) => fnv(&b),
                other => return Err(bad(&other)),
            },
            other => return Err(bad(other)),
        },
    })
}

impl Meta {
    fn shard_of(&self, kind: KeyKind, value: &Value) -> Result<usize> {
        let slot = key_hash(kind, value)? % self.slots.len() as u64;
        Ok(self.slots[slot as usize] as usize)
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn cross_shard(detail: impl Into<String>) -> Error {
    Error::new(sqlstate::FEATURE_NOT_SUPPORTED, "cross-shard transactions are not supported")
        .with_detail(detail)
        .with_hint("A transaction is bound to the shard of its first statement. Keep it to one distribution key, or run the statement outside of the transaction.")
}

struct Caches {
    generation: u64,
    parsed: HashMap<String, Arc<Vec<tree::Parsed>>>,
    plans: HashMap<String, Arc<Plan>>,
    columns: HashMap<String, Vec<String>>,
}

struct Inner {
    shards: Vec<Shard>,
    meta: RwLock<Arc<Meta>>,
    /// Counts schema and metadata changes; cached plans and prepared
    /// statements are valid for one generation.
    generation: AtomicU64,
    caches: Mutex<Caches>,
}

fn read_meta(conn: &mut ShardConn) -> Result<HashMap<String, String>> {
    conn.run(&format!("CREATE TABLE IF NOT EXISTS {META_TABLE} (key text PRIMARY KEY, value text NOT NULL)"), &[])?;
    let out = conn.run(&format!("SELECT key, value FROM {META_TABLE}"), &[])?;
    let mut map = HashMap::new();
    for row in out.rows {
        if let [Value::Text(k), Value::Text(v)] = row.as_slice() {
            map.insert(k.clone(), v.clone());
        }
    }
    Ok(map)
}

fn put_meta(conn: &mut ShardConn, key: &str, value: &str) -> Result<()> {
    conn.run(
        &format!("INSERT INTO {META_TABLE} (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value"),
        &[Value::Text(key.into()), Value::Text(value.into())],
    )?;
    Ok(())
}

fn config_error(message: impl Into<String>) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, message)
}

impl Inner {
    /// Reads the metadata of every shard, initializes a new cluster, and
    /// checks that the shards belong together.
    fn load(&self, virtual_slots: usize) -> Result<Meta> {
        let n = self.shards.len();
        let mut conns = Vec::with_capacity(n);
        let mut maps = Vec::with_capacity(n);
        for s in &self.shards {
            let mut conn = s.connect()?;
            maps.push(read_meta(&mut conn)?);
            conns.push(conn);
        }
        // `shards` is written last; without it a shard is uninitialized.
        let slot_map = match maps.iter().find(|m| m.contains_key("shards")) {
            Some(m) => m.get("slot_map").cloned().unwrap_or_default(),
            None => (0..virtual_slots).map(|s| (s % n).to_string()).collect::<Vec<_>>().join(","),
        };
        for (i, (map, conn)) in maps.iter_mut().zip(&mut conns).enumerate() {
            if !map.contains_key("shards") {
                let leftovers = map.keys().any(|k| k.starts_with("table:"));
                if leftovers {
                    return Err(config_error(format!("shard {i} holds table metadata of another cluster")));
                }
                put_meta(conn, "slot_map", &slot_map)?;
                put_meta(conn, "shard", &i.to_string())?;
                put_meta(conn, "shards", &n.to_string())?;
                continue;
            }
            let get = |k: &str| map.get(k).map(String::as_str).unwrap_or("");
            if get("shards") != n.to_string() {
                return Err(config_error(format!(
                    "shard {i} belongs to a cluster of {} shards, but {n} are configured; the number of shards of a cluster is fixed",
                    get("shards")
                )));
            }
            if get("shard") != i.to_string() {
                return Err(config_error(format!(
                    "the shard configured at position {i} is shard {} of its cluster; shards must be listed in their original order",
                    get("shard")
                )));
            }
            if get("slot_map") != slot_map {
                return Err(config_error(format!("shard {i} has a different slot map than the other shards")));
            }
        }
        let slots: Vec<u32> = slot_map
            .split(',')
            .map(|s| s.parse::<u32>().ok().filter(|s| (*s as usize) < n))
            .collect::<Option<_>>()
            .filter(|s: &Vec<u32>| !s.is_empty())
            .ok_or_else(|| Error::new(sqlstate::DATA_CORRUPTED, "the slot map of the cluster is malformed"))?;
        // A table class counts once every shard has recorded it; a
        // declaration that stopped half way is completed by repeating it.
        let mut tables = HashMap::new();
        for (key, value) in &maps[0] {
            let Some(table) = key.strip_prefix("table:") else { continue };
            if maps.iter().all(|m| m.get(key) == Some(value)) {
                if let Some(class) = Class::decode(value) {
                    tables.insert(table.to_string(), class);
                }
            }
        }
        Ok(Meta { tables, slots })
    }

    fn reload(&self) -> Result<()> {
        let slots = self.meta.read().slots.len();
        let meta = self.load(slots)?;
        *self.meta.write() = Arc::new(meta);
        self.bump();
        Ok(())
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// The caches, emptied if the generation moved on.
    fn caches(&self) -> parking_lot::MutexGuard<'_, Caches> {
        let mut c = self.caches.lock();
        let generation = self.generation.load(Ordering::SeqCst);
        if c.generation != generation || c.plans.len() > PLAN_CACHE || c.parsed.len() > PLAN_CACHE {
            c.generation = generation;
            c.parsed.clear();
            c.plans.clear();
            c.columns.clear();
        }
        c
    }
}

/// A set of shards that hold one logical database. Cheap to clone.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl Cluster {
    /// Opens the shards and checks that they form one cluster. Shards
    /// without cluster metadata are initialized, so the first `open` of a
    /// set of empty databases creates the cluster.
    pub fn open(config: ClusterConfig) -> Result<Cluster> {
        if config.shards.is_empty() {
            return Err(config_error("a cluster needs at least one shard"));
        }
        if config.virtual_slots < config.shards.len() || config.virtual_slots > 1 << 20 {
            return Err(config_error("virtual_slots must be between the number of shards and 1048576"));
        }
        let mut shards = Vec::with_capacity(config.shards.len());
        for (index, spec) in config.shards.into_iter().enumerate() {
            let kind = match spec {
                ShardSpec::Local { dir, config } => ShardKind::Local(Database::open(dir, config)?),
                ShardSpec::Memory => ShardKind::Local(Database::open_in_memory()?),
                ShardSpec::Remote { url } => ShardKind::Remote(url),
            };
            shards.push(Shard { index, kind });
        }
        let inner = Inner {
            shards,
            meta: RwLock::new(Arc::new(Meta { tables: HashMap::new(), slots: Vec::new() })),
            generation: AtomicU64::new(0),
            caches: Mutex::new(Caches {
                generation: 0,
                parsed: HashMap::new(),
                plans: HashMap::new(),
                columns: HashMap::new(),
            }),
        };
        let meta = inner.load(config.virtual_slots)?;
        *inner.meta.write() = Arc::new(meta);
        Ok(Cluster { inner: Arc::new(inner) })
    }

    /// A new connection. Connections to the shards are opened when a
    /// statement first needs them.
    pub fn connect(&self) -> ClusterConnection {
        ClusterConnection {
            inner: self.inner.clone(),
            state: Mutex::new(State {
                conns: self.inner.shards.iter().map(|_| None).collect(),
                txn: None,
                scratch: None,
                settings: Vec::new(),
                generation: self.inner.generation.load(Ordering::SeqCst),
            }),
        }
    }

    /// The shards, in their fixed order.
    pub fn shards(&self) -> &[Shard] {
        &self.inner.shards
    }

    /// The shard a distribution key lives on. Integers, text and UUIDs
    /// hash like the column types of the same kind; other values hash by
    /// their text form.
    pub fn shard_for(&self, key: &Value) -> usize {
        let kind = match key {
            Value::Int2(_) | Value::Int4(_) | Value::Int8(_) => KeyKind::Int,
            Value::Uuid(_) => KeyKind::Uuid,
            _ => KeyKind::Text,
        };
        self.inner.meta.read().shard_of(kind, key).unwrap_or(0)
    }

    /// The shard that holds the rows of `table` with distribution key
    /// `key`, converting the key like a statement on that table would.
    pub fn shard_for_table(&self, table: &str, key: &Value) -> Result<usize> {
        let meta = self.inner.meta.read().clone();
        match meta.tables.get(table) {
            Some(Class::Distributed { kind, .. }) => meta.shard_of(*kind, key),
            _ => Err(Error::new(sqlstate::UNDEFINED_TABLE, format!("\"{table}\" is not a distributed table"))),
        }
    }

    /// Number of virtual slots of the cluster.
    pub fn virtual_slots(&self) -> usize {
        self.inner.meta.read().slots.len()
    }

    /// Distributed and reference tables, sorted by name.
    pub fn tables(&self) -> Vec<(String, TableClass)> {
        let meta = self.inner.meta.read().clone();
        let mut out: Vec<(String, TableClass)> = meta
            .tables
            .iter()
            .map(|(name, class)| {
                let class = match class {
                    Class::Distributed { column, .. } => TableClass::Distributed { column: column.clone() },
                    Class::Reference => TableClass::Reference,
                };
                (name.clone(), class)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Reads the table classes again from the shards. Needed only when
    /// another process changed them on shared remote shards.
    pub fn reload(&self) -> Result<()> {
        self.inner.reload()
    }
}

/// One row of a result.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Arc<Vec<OutputColumn>>,
    values: Vec<Value>,
}

impl Row {
    /// Converts the value of a column. Panics if the column does not
    /// exist or the value cannot be converted; see [`try_get`](Row::try_get).
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

fn rows_of(out: Output) -> Vec<Row> {
    let columns = Arc::new(out.columns);
    out.rows.into_iter().map(|values| Row { columns: columns.clone(), values }).collect()
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

struct Txn {
    /// The shard the transaction runs on, once a statement chose one.
    shard: Option<usize>,
    /// BEGIN and savepoint commands to replay on that shard.
    pending: Vec<String>,
}

struct State {
    conns: Vec<Option<ShardConn>>,
    txn: Option<Txn>,
    /// In-memory database that merges partial aggregates.
    scratch: Option<squad::Connection>,
    /// Session settings, replayed on shard connections opened later.
    settings: Vec<String>,
    generation: u64,
}

/// A connection to the cluster. Mirrors `squad::Connection`.
pub struct ClusterConnection {
    inner: Arc<Inner>,
    state: Mutex<State>,
}

/// Combines the results of a write that ran on several shards.
fn combine(what: &str, results: Vec<(usize, Result<Output>)>) -> Result<Output> {
    let done: Vec<String> = results.iter().filter(|(_, r)| r.is_ok()).map(|(s, _)| s.to_string()).collect();
    let mut total = Output::default();
    for (shard, result) in results {
        match result {
            Ok(out) => {
                if total.columns.is_empty() {
                    total.columns = out.columns;
                }
                total.affected += out.affected;
                total.rows.extend(out.rows);
            }
            Err(e) if done.is_empty() => return Err(e),
            Err(e) => {
                let detail = format!(
                    "{what} failed on shard {shard} after it was applied on shard(s) {}. Shards do not share a transaction, so the applied part stays.",
                    done.join(", ")
                );
                let detail = match e.detail() {
                    Some(d) => format!("{d} {detail}"),
                    None => detail,
                };
                return Err(e.with_detail(detail));
            }
        }
    }
    Ok(total)
}

impl State {
    fn conn(&mut self, inner: &Inner, shard: usize) -> Result<&mut ShardConn> {
        // A lost connection to a remote shard is replaced, unless a
        // transaction was open on it: that one must fail.
        let in_txn = self.txn.as_ref().is_some_and(|t| t.shard == Some(shard));
        if !in_txn && self.conns[shard].as_ref().is_some_and(ShardConn::is_broken) {
            self.conns[shard] = None;
        }
        if self.conns[shard].is_none() {
            let mut conn = inner.shards[shard].connect()?;
            for s in &self.settings {
                conn.run(s, &[])?;
            }
            self.conns[shard] = Some(conn);
        }
        Ok(self.conns[shard].as_mut().unwrap())
    }

    /// Binds an open transaction to `shard`, or fails if it is bound to
    /// another one.
    fn pin(&mut self, inner: &Inner, shard: usize) -> Result<()> {
        let Some(txn) = self.txn.as_mut() else { return Ok(()) };
        match txn.shard {
            Some(s) if s == shard => Ok(()),
            Some(s) => {
                Err(cross_shard(format!("The transaction runs on shard {s}; this statement needs shard {shard}.")))
            }
            None => {
                let pending = std::mem::take(&mut txn.pending);
                txn.shard = Some(shard);
                let conn = self.conn(inner, shard)?;
                for sql in &pending {
                    conn.run(sql, &[])?;
                }
                Ok(())
            }
        }
    }

    fn outside_txn(&self, what: &str) -> Result<()> {
        match &self.txn {
            None => Ok(()),
            Some(_) => Err(cross_shard(format!("{what} involves several shards and cannot run inside a transaction."))),
        }
    }

    fn on_shard(&mut self, inner: &Inner, shard: usize, sql: &str, params: &[Value]) -> Result<Output> {
        self.pin(inner, shard)?;
        self.conn(inner, shard)?.run(sql, params)
    }

    /// Runs statements on several shards at once, one thread per shard.
    fn fan_out(
        &mut self,
        inner: &Inner,
        jobs: &[(usize, &str)],
        params: &[Value],
    ) -> Result<Vec<(usize, Result<Output>)>> {
        let jobs: Vec<(usize, &str, &[Value])> = jobs.iter().map(|(s, q)| (*s, *q, params)).collect();
        self.fan_out_each(inner, &jobs)
    }

    /// Like [`fan_out`](Self::fan_out), with parameters per shard.
    fn fan_out_each(
        &mut self,
        inner: &Inner,
        jobs: &[(usize, &str, &[Value])],
    ) -> Result<Vec<(usize, Result<Output>)>> {
        for (shard, _, _) in jobs {
            self.conn(inner, *shard)?;
        }
        let mut work: Vec<(usize, &str, &[Value], &mut ShardConn)> = Vec::with_capacity(jobs.len());
        for (i, conn) in self.conns.iter_mut().enumerate() {
            if let (Some((_, sql, params)), Some(conn)) = (jobs.iter().find(|(s, _, _)| *s == i), conn.as_mut()) {
                work.push((i, sql, params, conn));
            }
        }
        if work.len() <= 1 {
            return Ok(work.into_iter().map(|(i, sql, params, conn)| (i, conn.run(sql, params))).collect());
        }
        Ok(std::thread::scope(|scope| {
            let mut work = work.into_iter();
            let first = work.next();
            let handles: Vec<_> =
                work.map(|(i, sql, params, conn)| (i, scope.spawn(move || conn.run(sql, params)))).collect();
            let mut results = Vec::with_capacity(handles.len() + 1);
            if let Some((i, sql, params, conn)) = first {
                results.push((i, conn.run(sql, params)));
            }
            for (i, h) in handles {
                results.push((i, h.join().unwrap_or_else(|_| Err(Error::internal("a shard query panicked")))));
            }
            results
        }))
    }

    fn all_shards(inner: &Inner) -> Vec<usize> {
        (0..inner.shards.len()).collect()
    }

    /// The shard to describe statements on and to read reference tables
    /// from.
    fn home(&self) -> usize {
        self.txn.as_ref().and_then(|t| t.shard).unwrap_or(0)
    }

    fn plan(&mut self, inner: &Inner, stmt: &tree::Parsed) -> Result<Arc<Plan>> {
        if let Some(p) = inner.caches().plans.get(&stmt.text) {
            return Ok(p.clone());
        }
        let meta = inner.meta.read().clone();
        let home = self.home();
        let conn = RefCell::new(self.conn(inner, home)?);
        let columns = |table: &str| -> Result<Vec<String>> {
            if let Some(c) = inner.caches().columns.get(table) {
                return Ok(c.clone());
            }
            let cols = conn.borrow_mut().describe(&format!("SELECT * FROM {} LIMIT 0", quote_ident(table)))?;
            let names: Vec<String> = cols.into_iter().map(|c| c.name).collect();
            inner.caches().columns.insert(table.to_string(), names.clone());
            Ok(names)
        };
        let describe = |sql: &str| conn.borrow_mut().describe(sql);
        let env = route::Env { meta: &meta, columns: &columns, describe: &describe, shard_count: inner.shards.len() };
        let plan = Arc::new(route::analyze(&stmt.node, &stmt.text, &env)?);
        inner.caches().plans.insert(stmt.text.clone(), plan.clone());
        Ok(plan)
    }

    fn run(&mut self, inner: &Inner, sql: &str, params: &[Value]) -> Result<Output> {
        let generation = inner.generation.load(Ordering::SeqCst);
        if self.generation != generation {
            self.generation = generation;
            self.conns.iter_mut().flatten().for_each(ShardConn::reset);
        }
        let cached = inner.caches().parsed.get(sql).cloned();
        let stmts = match cached {
            Some(s) => s,
            None => {
                let s = Arc::new(tree::parse(sql)?);
                inner.caches().parsed.insert(sql.to_string(), s.clone());
                s
            }
        };
        if stmts.len() > 1 && !params.is_empty() {
            return Err(Error::new(sqlstate::SYNTAX_ERROR, "cannot use parameters with multiple statements"));
        }
        let mut last = Output::default();
        for stmt in stmts.iter() {
            let plan = self.plan(inner, stmt)?;
            last = self.exec(inner, &plan, &stmt.text, params)?;
        }
        Ok(last)
    }

    fn exec(&mut self, inner: &Inner, plan: &Plan, text: &str, params: &[Value]) -> Result<Output> {
        match plan {
            Plan::Distribute { table, column } => self.declare(inner, table, Some(column)),
            Plan::Reference { table } => self.declare(inner, table, None),
            Plan::Begin => {
                if self.txn.is_none() {
                    self.txn = Some(Txn { shard: None, pending: vec![text.to_string()] });
                }
                Ok(Output::default())
            }
            Plan::TxnControl { ends } => {
                let Some(txn) = self.txn.as_mut() else {
                    if *ends {
                        return Ok(Output::default());
                    }
                    return Err(Error::new(
                        sqlstate::NO_ACTIVE_SQL_TRANSACTION,
                        "savepoints can only be used in transaction blocks",
                    ));
                };
                match txn.shard {
                    Some(shard) => {
                        if *ends {
                            self.txn = None;
                        }
                        self.conn(inner, shard)?.run(text, &[])
                    }
                    None => {
                        if *ends {
                            self.txn = None;
                        } else {
                            txn.pending.push(text.to_string());
                        }
                        Ok(Output::default())
                    }
                }
            }
            Plan::Everywhere { ddl: false, .. } => {
                // Session settings: every open connection now, the others
                // when they are opened.
                let mut out = Output::default();
                for conn in self.conns.iter_mut().flatten() {
                    out = conn.run(text, &[])?;
                }
                self.settings.push(text.to_string());
                Ok(out)
            }
            Plan::Everywhere { ddl: true, dropped } => {
                if self.txn.is_some() {
                    return Err(unsupported("schema changes inside a transaction")
                        .with_hint("Shards do not share a transaction. Run schema changes outside of transactions."));
                }
                let mut results = Vec::new();
                for shard in Self::all_shards(inner) {
                    let r = self.conn(inner, shard).and_then(|c| c.run(text, params));
                    let failed = r.is_err();
                    results.push((shard, r));
                    if failed {
                        break;
                    }
                }
                inner.bump();
                let out = combine("The schema change", results).map_err(|e| {
                    if e.detail().is_some_and(|d| d.contains("after it was applied")) {
                        e.with_hint("Repeat the statement in a form that tolerates the applied part (IF NOT EXISTS, IF EXISTS), or undo it, so that all shards have the same schema.")
                    } else {
                        e
                    }
                })?;
                let meta = inner.meta.read().clone();
                if dropped.iter().any(|t| meta.tables.contains_key(t)) {
                    for shard in Self::all_shards(inner) {
                        let conn = self.conn(inner, shard)?;
                        for t in dropped {
                            conn.run(
                                &format!("DELETE FROM {META_TABLE} WHERE key = $1"),
                                &[Value::Text(format!("table:{t}"))],
                            )?;
                        }
                    }
                    inner.reload()?;
                }
                Ok(Output { rows: Vec::new(), ..out })
            }
            Plan::Anywhere => {
                let home = self.home();
                self.conn(inner, home)?.run(text, params)
            }
            Plan::ReferenceWrite => {
                if self.txn.is_some() {
                    return Err(unsupported("writing to a reference table inside a transaction").with_hint(
                        "A reference table is written on every shard, and shards do not share a transaction.",
                    ));
                }
                // One shard after the other, so that a statement that
                // fails (a constraint, a type error) fails on the first
                // shard and changes nothing.
                let mut results = Vec::new();
                for shard in Self::all_shards(inner) {
                    let r = self.conn(inner, shard).and_then(|c| c.run(text, params));
                    let failed = r.is_err();
                    results.push((shard, r));
                    if failed {
                        break;
                    }
                }
                let first = results.first().and_then(|(_, r)| r.as_ref().ok()).cloned();
                combine("The write to the reference table", results)?;
                Ok(first.unwrap_or_default())
            }
            Plan::Insert(routing) => self.insert(inner, routing, text, params),
            Plan::Keyed { groups, write, scatter } => {
                let shards = Self::resolve(inner, groups, params)?;
                match shards.as_slice() {
                    [] => self.on_shard(inner, 0, text, params),
                    [one] => self.on_shard(inner, *one, text, params),
                    _ => match scatter {
                        Some(Ok(sc)) => self.scatter(inner, sc, &shards, params),
                        Some(Err(e)) => Err(e.clone()),
                        // UPDATE or DELETE with keys on several shards.
                        None if *write && groups.len() == 1 => {
                            self.outside_txn("A write to several distribution keys")?;
                            let jobs: Vec<(usize, &str)> = shards.iter().map(|s| (*s, text)).collect();
                            combine("The statement", self.fan_out(inner, &jobs, params)?)
                        }
                        None => Err(unsupported("a statement whose distribution keys live on different shards").with_hint(
                            "Tables joined on their distribution column must be filtered to keys of one shard; use one key per statement.",
                        )),
                    },
                }
            }
            Plan::WriteAll => {
                self.outside_txn("A write without a condition on the distribution column")?;
                let jobs: Vec<(usize, &str)> = Self::all_shards(inner).into_iter().map(|s| (s, text)).collect();
                combine("The statement", self.fan_out(inner, &jobs, params)?)
            }
            Plan::Scatter(Ok(sc)) => self.scatter(inner, sc, &Self::all_shards(inner), params),
            Plan::Scatter(Err(e)) => Err(e.clone()),
        }
    }

    /// The shards the key sets of a statement name: empty if there are no
    /// keys, one shard if all sets agree on it, several for one set of
    /// keys on different shards.
    fn resolve(inner: &Inner, groups: &KeyGroups, params: &[Value]) -> Result<Vec<usize>> {
        let meta = inner.meta.read().clone();
        let mut sets: Vec<Vec<usize>> = Vec::with_capacity(groups.len());
        for (table, ops) in groups {
            let Some(Class::Distributed { kind, .. }) = meta.tables.get(table) else {
                return Err(Error::internal(format!("\"{table}\" is not distributed any more")));
            };
            let mut set = Vec::new();
            for op in ops {
                let value = op.resolve(params)?;
                // `column = NULL` and `IN (NULL)` match no row.
                if value.is_null() {
                    continue;
                }
                let s = meta.shard_of(*kind, value)?;
                if !set.contains(&s) {
                    set.push(s);
                }
            }
            if set.is_empty() {
                set.push(0);
            }
            set.sort_unstable();
            sets.push(set);
        }
        match sets.as_slice() {
            [] => Ok(Vec::new()),
            [one] => Ok(one.clone()),
            many => {
                let first = &many[0];
                if first.len() == 1 && many.iter().all(|s| s == first) {
                    Ok(first.clone())
                } else {
                    Err(unsupported("a statement whose distribution keys live on different shards")
                        .with_hint("Use the same distribution key for every distributed table of the statement."))
                }
            }
        }
    }

    fn insert(&mut self, inner: &Inner, routing: &InsertRouting, text: &str, params: &[Value]) -> Result<Output> {
        let meta = inner.meta.read().clone();
        let Some(Class::Distributed { kind, .. }) = meta.tables.get(&routing.table) else {
            return Err(Error::internal(format!("\"{}\" is not distributed any more", routing.table)));
        };
        let mut by_shard: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (row, key) in routing.keys.iter().enumerate() {
            by_shard.entry(meta.shard_of(*kind, key.resolve(params)?)?).or_default().push(row);
        }
        if by_shard.len() == 1 {
            let shard = *by_shard.keys().next().unwrap();
            return self.on_shard(inner, shard, text, params);
        }
        self.outside_txn("An INSERT of rows with distribution keys on different shards")?;
        use pg_query::protobuf::{self as pb, node::Node as N};
        let mut sqls = Vec::with_capacity(by_shard.len());
        for (shard, rows) in &by_shard {
            let mut stmt = routing.stmt.clone();
            if let Some(N::SelectStmt(sel)) = stmt.select_stmt.as_mut().and_then(|s| s.node.as_mut()) {
                let all = std::mem::take(&mut sel.values_lists);
                sel.values_lists = rows.iter().map(|r| all[*r].clone()).collect();
            }
            sqls.push((*shard, tree::deparse(&pb::Node { node: Some(N::InsertStmt(Box::new(stmt))) })?));
        }
        let jobs: Vec<(usize, &str)> = sqls.iter().map(|(s, q)| (*s, q.as_str())).collect();
        combine("The INSERT", self.fan_out(inner, &jobs, params)?)
    }

    fn scatter(&mut self, inner: &Inner, sc: &Scatter, shards: &[usize], params: &[Value]) -> Result<Output> {
        self.outside_txn("A query that reads several shards")?;
        let sql = match sc {
            Scatter::Concat(c) => c.sql.as_str(),
            Scatter::Merge(m) => m.sql.as_str(),
        };
        let jobs: Vec<(usize, &str)> = shards.iter().map(|s| (*s, sql)).collect();
        let mut outputs = Vec::with_capacity(jobs.len());
        for (_, result) in self.fan_out(inner, &jobs, params)? {
            outputs.push(result?);
        }
        match sc {
            Scatter::Concat(c) => c.finish(outputs, params),
            Scatter::Merge(m) => {
                let rows = m.partial_rows(outputs)?;
                if self.scratch.is_none() {
                    self.scratch = Some(Database::open_in_memory()?.connect());
                }
                let scratch = self.scratch.as_ref().unwrap();
                scratch.batch_execute(&format!("DROP TABLE IF EXISTS {}", scatter::PARTIAL_TABLE))?;
                scratch.batch_execute(&m.create_sql)?;
                let insert = scratch.prepare(&m.insert_sql)?;
                scratch.batch_execute("BEGIN")?;
                for row in &rows {
                    let refs: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
                    if let Err(e) = insert.execute(&refs) {
                        let _ = scratch.batch_execute("ROLLBACK");
                        return Err(e);
                    }
                }
                scratch.batch_execute("COMMIT")?;
                let refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
                let merged = scratch.prepare(&m.merge_sql)?.query(&refs);
                let _ = scratch.batch_execute(&format!("DROP TABLE IF EXISTS {}", scatter::PARTIAL_TABLE));
                let rows: Vec<Vec<Value>> = merged?.iter().map(|r| r.values().to_vec()).collect();
                Ok(Output { columns: m.columns.clone(), affected: rows.len() as u64, rows })
            }
        }
    }

    /// Bulk insert: rows are grouped by shard and sent as one statement
    /// per shard, column by column.
    fn insert_rows(&mut self, inner: &Inner, table: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> Result<u64> {
        if columns.is_empty() {
            return Err(Error::new(sqlstate::SYNTAX_ERROR, "insert_rows needs the names of the columns"));
        }
        if let Some(bad) = rows.iter().find(|r| r.len() != columns.len()) {
            return Err(Error::new(
                sqlstate::SYNTAX_ERROR,
                format!("a row has {} values, but {} columns are named", bad.len(), columns.len()),
            ));
        }
        if rows.is_empty() {
            return Ok(0);
        }
        let generation = inner.generation.load(Ordering::SeqCst);
        if self.generation != generation {
            self.generation = generation;
            self.conns.iter_mut().flatten().for_each(ShardConn::reset);
        }
        let meta = inner.meta.read().clone();
        let list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let home = self.home();
        let types = self.conn(inner, home)?.describe(&format!("SELECT {list} FROM {} LIMIT 0", quote_ident(table)))?;
        let mut casts = Vec::with_capacity(types.len());
        for (i, c) in types.iter().enumerate() {
            if matches!(c.ty, Type::Array(_) | Type::Record | Type::Void | Type::Unknown) {
                return Err(unsupported(format!("insert_rows into column \"{}\" of type {}", c.name, c.ty.name()))
                    .with_hint("Use INSERT … VALUES for tables with array columns."));
            }
            casts.push(format!("${}::{}[]", i + 1, c.ty.name()));
        }
        let sql = format!("INSERT INTO {} ({list}) SELECT * FROM unnest({})", quote_ident(table), casts.join(", "));

        // Shard of every row.
        let all = Self::all_shards(inner);
        let mut by_shard: BTreeMap<usize, Vec<Vec<Value>>> = BTreeMap::new();
        let reference = match meta.tables.get(table) {
            Some(Class::Distributed { column, kind }) => {
                let pos = columns.iter().position(|c| c == column).ok_or_else(|| {
                    unsupported(format!(
                        "insert_rows into distributed table \"{table}\" without its distribution column \"{column}\""
                    ))
                })?;
                if all.len() == 1 {
                    by_shard.insert(0, rows);
                } else {
                    for row in rows {
                        by_shard.entry(meta.shard_of(*kind, &row[pos])?).or_default().push(row);
                    }
                }
                false
            }
            Some(Class::Reference) => {
                by_shard.insert(0, rows);
                true
            }
            None if all.len() == 1 => {
                by_shard.insert(0, rows);
                false
            }
            None => return Err(route::undeclared_error(table)),
        };
        // Rows to arrays, one per column.
        let mut params: BTreeMap<usize, Vec<Value>> = BTreeMap::new();
        for (shard, rows) in by_shard {
            let mut cols: Vec<Vec<Value>> = (0..columns.len()).map(|_| Vec::with_capacity(rows.len())).collect();
            for row in rows {
                for (col, v) in cols.iter_mut().zip(row) {
                    col.push(v);
                }
            }
            params.insert(shard, cols.into_iter().map(Value::Array).collect());
        }
        if reference {
            if self.txn.is_some() {
                return Err(unsupported("writing to a reference table inside a transaction")
                    .with_hint("A reference table is written on every shard, and shards do not share a transaction."));
            }
            let p = &params[&0];
            let mut results = Vec::new();
            for shard in all {
                let r = self.conn(inner, shard).and_then(|c| c.run(&sql, p));
                let failed = r.is_err();
                results.push((shard, r));
                if failed {
                    break;
                }
            }
            let first = results.first().and_then(|(_, r)| r.as_ref().ok()).map_or(0, |o| o.affected);
            combine("The write to the reference table", results)?;
            return Ok(first);
        }
        if params.len() == 1 {
            let (shard, p) = params.iter().next().unwrap();
            return Ok(self.on_shard(inner, *shard, &sql, p)?.affected);
        }
        self.outside_txn("An insert of rows with distribution keys on different shards")?;
        let jobs: Vec<(usize, &str, &[Value])> = params.iter().map(|(s, p)| (*s, sql.as_str(), p.as_slice())).collect();
        Ok(combine("The insert", self.fan_out_each(inner, &jobs)?)?.affected)
    }

    /// `squad_distribute` (with a column) and `squad_reference`.
    fn declare(&mut self, inner: &Inner, table: &str, column: Option<&str>) -> Result<Output> {
        let function = if column.is_some() { "squad_distribute" } else { "squad_reference" };
        if self.txn.is_some() {
            return Err(unsupported(format!("{function} inside a transaction")));
        }
        let shards = Self::all_shards(inner);
        let class = match column {
            None => Class::Reference,
            Some(column) => {
                let mut kind = None;
                for shard in &shards {
                    let cols = self.conn(inner, *shard)?.describe(&format!(
                        "SELECT {} FROM {} LIMIT 0",
                        quote_ident(column),
                        quote_ident(table)
                    ))?;
                    let k = match cols.first().map(|c| &c.ty) {
                        Some(Type::Int2 | Type::Int4 | Type::Int8) => KeyKind::Int,
                        Some(Type::Text) => KeyKind::Text,
                        Some(Type::Uuid) => KeyKind::Uuid,
                        other => {
                            return Err(unsupported(format!(
                                "a distribution column of type {}; use an integer, text or uuid column",
                                other.map_or("unknown".to_string(), |t| t.name())
                            )))
                        }
                    };
                    if kind.is_some_and(|prev| prev != k) {
                        return Err(Error::new(
                            sqlstate::DATATYPE_MISMATCH,
                            format!(
                                "column \"{column}\" of table \"{table}\" does not have the same type on all shards"
                            ),
                        ));
                    }
                    kind = Some(k);
                }
                Class::Distributed { column: column.to_string(), kind: kind.unwrap_or(KeyKind::Text) }
            }
        };
        let meta = inner.meta.read().clone();
        let describe = |c: &Class| match c {
            Class::Distributed { column, .. } => format!("distributed by \"{column}\""),
            Class::Reference => "a reference table".to_string(),
        };
        match meta.tables.get(table) {
            Some(existing) if *existing == class => {}
            Some(existing) => {
                return Err(Error::new(
                    sqlstate::DUPLICATE_OBJECT,
                    format!("table \"{table}\" is already {}", describe(existing)),
                ))
            }
            None => {
                // Rows that are already there would be on the wrong shard.
                for shard in &shards {
                    let out =
                        self.conn(inner, *shard)?.run(&format!("SELECT count(*) FROM {}", quote_ident(table)), &[])?;
                    let rows = matches!(out.rows.first().and_then(|r| r.first()), Some(Value::Int8(n)) if *n > 0);
                    if rows && shards.len() > 1 {
                        return Err(Error::new(
                            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
                            format!("table \"{table}\" has rows on shard {shard}; a table must be empty when it is declared"),
                        ));
                    }
                }
            }
        }
        let mut results = Vec::new();
        for shard in &shards {
            let r = self
                .conn(inner, *shard)
                .and_then(|c| put_meta(c, &format!("table:{table}"), &class.encode()))
                .map(|_| Output::default());
            let failed = r.is_err();
            results.push((*shard, r));
            if failed {
                break;
            }
        }
        combine("Recording the table class", results).map_err(|e| {
            e.with_hint(format!("Repeat {function}; the table counts as declared once every shard has recorded it."))
        })?;
        inner.reload()?;
        Ok(Output {
            columns: vec![OutputColumn { name: function.to_string(), ty: Type::Text }],
            rows: vec![vec![Value::Text(format!("{table}: {}", describe(&class)))]],
            affected: 1,
        })
    }
}

impl ClusterConnection {
    fn run(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Output> {
        let values: Vec<Value> = params.iter().map(|p| p.to_value()).collect();
        self.state.lock().run(&self.inner, sql, &values)
    }

    /// Runs a statement and returns the number of rows it changed (for a
    /// query, returned).
    pub fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        Ok(self.run(sql, params)?.affected)
    }

    pub fn query(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        Ok(rows_of(self.run(sql, params)?))
    }

    pub fn query_one(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Row> {
        exactly_one(self.query(sql, params)?)
    }

    pub fn query_opt(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Option<Row>> {
        at_most_one(self.query(sql, params)?)
    }

    /// Runs one or more statements separated by semicolons.
    pub fn batch_execute(&self, sql: &str) -> Result<()> {
        self.run(sql, &[]).map(|_| ())
    }

    /// Inserts many rows at once: the bulk path, in place of `COPY`.
    ///
    /// `columns` names the columns the values of each row belong to; for a
    /// distributed table the distribution column must be among them. Rows
    /// are grouped by shard and every shard receives its rows as one
    /// statement, all shards at the same time. Like a multi-row INSERT
    /// that spans shards, this is not atomic across shards: if one shard
    /// fails, the rows of the others stay.
    pub fn insert_rows(&self, table: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> Result<u64> {
        self.state.lock().insert_rows(&self.inner, table, columns, rows)
    }

    /// Checks a statement and returns a handle that runs it. Routing is
    /// decided for each execution, from the parameter values.
    pub fn prepare(&self, sql: &str) -> Result<ClusterStatement<'_>> {
        let stmts = tree::parse(sql)?;
        let [stmt] = stmts.as_slice() else {
            return Err(Error::new(sqlstate::SYNTAX_ERROR, "cannot prepare multiple statements"));
        };
        let mut state = self.state.lock();
        let plan = state.plan(&self.inner, stmt)?;
        let columns = match &*plan {
            Plan::Scatter(Err(e)) => return Err(e.clone()),
            Plan::Scatter(Ok(Scatter::Merge(m))) => m.columns.clone(),
            Plan::Distribute { .. }
            | Plan::Reference { .. }
            | Plan::Begin
            | Plan::TxnControl { .. }
            | Plan::Everywhere { .. } => Vec::new(),
            _ => {
                let home = state.home();
                state.conn(&self.inner, home)?.describe(&stmt.text)?
            }
        };
        Ok(ClusterStatement { conn: self, sql: stmt.text.clone(), columns })
    }

    /// Starts a transaction. It is bound to the shard of its first
    /// statement that names a distribution key.
    pub fn transaction(&self) -> Result<ClusterTransaction<'_>> {
        self.batch_execute("BEGIN")?;
        Ok(ClusterTransaction { conn: self, done: false })
    }

    pub fn in_transaction(&self) -> bool {
        self.state.lock().txn.is_some()
    }

    /// The shard the open transaction is bound to, if it is bound yet.
    pub fn transaction_shard(&self) -> Option<usize> {
        self.state.lock().txn.as_ref().and_then(|t| t.shard)
    }
}

/// A checked statement of a [`ClusterConnection`].
pub struct ClusterStatement<'a> {
    conn: &'a ClusterConnection,
    sql: String,
    columns: Vec<OutputColumn>,
}

impl ClusterStatement<'_> {
    pub fn execute(&self, params: &[&dyn ToSql]) -> Result<u64> {
        self.conn.execute(&self.sql, params)
    }

    pub fn query(&self, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        self.conn.query(&self.sql, params)
    }

    pub fn query_one(&self, params: &[&dyn ToSql]) -> Result<Row> {
        self.conn.query_one(&self.sql, params)
    }

    pub fn query_opt(&self, params: &[&dyn ToSql]) -> Result<Option<Row>> {
        self.conn.query_opt(&self.sql, params)
    }

    /// The columns of the result; empty for statements without one.
    pub fn columns(&self) -> &[OutputColumn] {
        &self.columns
    }
}

/// An open transaction. Dereferences to the [`ClusterConnection`] it runs
/// on, and rolls back when dropped without [`commit`](Self::commit).
pub struct ClusterTransaction<'a> {
    conn: &'a ClusterConnection,
    done: bool,
}

impl ClusterTransaction<'_> {
    pub fn commit(mut self) -> Result<()> {
        self.done = true;
        self.conn.batch_execute("COMMIT")
    }

    pub fn rollback(mut self) -> Result<()> {
        self.done = true;
        self.conn.batch_execute("ROLLBACK")
    }
}

impl std::ops::Deref for ClusterTransaction<'_> {
    type Target = ClusterConnection;
    fn deref(&self) -> &ClusterConnection {
        self.conn
    }
}

impl Drop for ClusterTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.conn.batch_execute("ROLLBACK");
        }
    }
}
