//! Runs the same OLTP workloads against squad (embedded), SQLite (embedded)
//! and PostgreSQL (client/server), and prints latencies and throughput.
//!
//! ```text
//! cargo run --release -p squad-bench -- [--rows N] [--ops N] [--threads N] [--postgres "host=/tmp user=postgres"]
//!                                       [--only squad,sqlite,postgres] [--json FILE]
//! ```
//!
//! All three systems are configured for the same durability: every commit
//! is forced to disk before it is acknowledged (squad `SyncMode::Sync`,
//! SQLite WAL with `synchronous=FULL`, PostgreSQL `synchronous_commit=on`
//! with `fsync=on`). PostgreSQL is reached through the given connection
//! string; use a Unix socket to measure it at its best.

use std::fmt::Write as _;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use squad::{Config, Database, SyncMode};

/// One system under test, reduced to the operations the workloads need.
trait Target: Send {
    fn name(&self) -> &'static str;
    /// Creates and fills `bench (id bigint primary key, name text, score int)`.
    fn load(&mut self, rows: i64);
    fn point_select(&mut self, id: i64) -> String;
    fn range_select(&mut self, score: i32) -> usize;
    fn update(&mut self, id: i64);
    fn insert(&mut self, id: i64);
    /// Read-modify-write transaction touching two rows.
    fn transfer(&mut self, from: i64, to: i64);
    /// A second connection for multi-threaded runs.
    fn fork(&self) -> Box<dyn Target>;
}

const SCHEMA: &str = "CREATE TABLE bench (id bigint PRIMARY KEY, name text NOT NULL, score int NOT NULL)";
const INDEX: &str = "CREATE INDEX bench_score_idx ON bench (score)";

// ---------------------------------------------------------------- squad

struct Squad {
    db: Database,
    conn: squad::Connection,
    select: squad::Statement,
    range: squad::Statement,
    update: squad::Statement,
    insert: squad::Statement,
    debit: squad::Statement,
    credit: squad::Statement,
}

impl Squad {
    fn connect(db: &Database) -> Squad {
        let conn = db.connect();
        conn.batch_execute(&format!(
            "{};{}",
            SCHEMA.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
            INDEX.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
        ))
        .unwrap();
        Squad {
            db: db.clone(),
            select: conn.prepare("SELECT name FROM bench WHERE id = $1").unwrap(),
            range: conn.prepare("SELECT id FROM bench WHERE score = $1 LIMIT 10").unwrap(),
            update: conn.prepare("UPDATE bench SET score = score + 1 WHERE id = $1").unwrap(),
            insert: conn.prepare("INSERT INTO bench VALUES ($1, 'new row', 7)").unwrap(),
            debit: conn.prepare("UPDATE bench SET score = score - 1 WHERE id = $1").unwrap(),
            credit: conn.prepare("UPDATE bench SET score = score + 1 WHERE id = $1").unwrap(),
            conn,
        }
    }
}

impl Target for Squad {
    fn name(&self) -> &'static str {
        "squad"
    }
    fn load(&mut self, rows: i64) {
        self.conn
            .execute("INSERT INTO bench SELECT g, 'user ' || g, g % 1000 FROM generate_series(1, $1) g", &[&rows])
            .unwrap();
        self.db.checkpoint().unwrap();
    }
    fn point_select(&mut self, id: i64) -> String {
        self.select.query_one(&[&id]).unwrap().get(0)
    }
    fn range_select(&mut self, score: i32) -> usize {
        self.range.query(&[&score]).unwrap().len()
    }
    fn update(&mut self, id: i64) {
        self.update.execute(&[&id]).unwrap();
    }
    fn insert(&mut self, id: i64) {
        self.insert.execute(&[&id]).unwrap();
    }
    fn transfer(&mut self, from: i64, to: i64) {
        loop {
            let attempt = (|| -> squad::Result<()> {
                self.conn.batch_execute("BEGIN")?;
                self.debit.execute(&[&from])?;
                self.credit.execute(&[&to])?;
                self.conn.batch_execute("COMMIT")
            })();
            match attempt {
                Ok(()) => return,
                Err(e) if matches!(e.code(), "40001" | "40P01") => {
                    let _ = self.conn.batch_execute("ROLLBACK");
                }
                Err(e) => panic!("{e}"),
            }
        }
    }
    fn fork(&self) -> Box<dyn Target> {
        Box::new(Squad::connect(&self.db))
    }
}

// --------------------------------------------------------------- SQLite

struct Sqlite {
    path: std::path::PathBuf,
    conn: rusqlite::Connection,
}

impl Sqlite {
    fn connect(path: &std::path::Path) -> Sqlite {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.busy_timeout(Duration::from_secs(60)).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", "FULL").unwrap();
        conn.execute_batch(&format!(
            "{};{}",
            SCHEMA.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").replace("bigint", "integer"),
            INDEX.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
        ))
        .unwrap();
        Sqlite { path: path.to_path_buf(), conn }
    }
}

impl Target for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }
    fn load(&mut self, rows: i64) {
        let tx = self.conn.transaction().unwrap();
        {
            let mut stmt = tx.prepare("INSERT INTO bench VALUES (?1, ?2, ?3)").unwrap();
            for g in 1..=rows {
                stmt.execute(rusqlite::params![g, format!("user {g}"), g % 1000]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    fn point_select(&mut self, id: i64) -> String {
        let mut stmt = self.conn.prepare_cached("SELECT name FROM bench WHERE id = ?1").unwrap();
        stmt.query_row([id], |r| r.get(0)).unwrap()
    }
    fn range_select(&mut self, score: i32) -> usize {
        let mut stmt = self.conn.prepare_cached("SELECT id FROM bench WHERE score = ?1 LIMIT 10").unwrap();
        let rows = stmt.query_map([score], |r| r.get::<_, i64>(0)).unwrap();
        rows.count()
    }
    fn update(&mut self, id: i64) {
        self.conn.prepare_cached("UPDATE bench SET score = score + 1 WHERE id = ?1").unwrap().execute([id]).unwrap();
    }
    fn insert(&mut self, id: i64) {
        self.conn.prepare_cached("INSERT INTO bench VALUES (?1, 'new row', 7)").unwrap().execute([id]).unwrap();
    }
    fn transfer(&mut self, from: i64, to: i64) {
        let tx = self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).unwrap();
        tx.prepare_cached("UPDATE bench SET score = score - 1 WHERE id = ?1").unwrap().execute([from]).unwrap();
        tx.prepare_cached("UPDATE bench SET score = score + 1 WHERE id = ?1").unwrap().execute([to]).unwrap();
        tx.commit().unwrap();
    }
    fn fork(&self) -> Box<dyn Target> {
        Box::new(Sqlite::connect(&self.path))
    }
}

// ----------------------------------------------------------- PostgreSQL

struct Postgres {
    config: String,
    client: postgres::Client,
    select: postgres::Statement,
    range: postgres::Statement,
    update: postgres::Statement,
    insert: postgres::Statement,
    debit: postgres::Statement,
}

impl Postgres {
    fn connect(config: &str) -> Postgres {
        let mut client = postgres::Client::connect(config, postgres::NoTls).expect("connect to PostgreSQL");
        client
            .batch_execute(&format!(
                "SET synchronous_commit = on; {};{}",
                SCHEMA.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
                INDEX.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
            ))
            .unwrap();
        Postgres {
            config: config.to_string(),
            select: client.prepare("SELECT name FROM bench WHERE id = $1").unwrap(),
            range: client.prepare("SELECT id FROM bench WHERE score = $1 LIMIT 10").unwrap(),
            update: client.prepare("UPDATE bench SET score = score + 1 WHERE id = $1").unwrap(),
            insert: client.prepare("INSERT INTO bench VALUES ($1, 'new row', 7)").unwrap(),
            debit: client.prepare("UPDATE bench SET score = score - 1 WHERE id = $1").unwrap(),
            client,
        }
    }
}

impl Target for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }
    fn load(&mut self, rows: i64) {
        self.client
            .execute(
                "INSERT INTO bench SELECT g, 'user ' || g, g % 1000 FROM generate_series(1, $1::bigint) g",
                &[&rows],
            )
            .unwrap();
        self.client.batch_execute("VACUUM ANALYZE bench").unwrap();
        self.client.batch_execute("CHECKPOINT").unwrap();
    }
    fn point_select(&mut self, id: i64) -> String {
        self.client.query_one(&self.select, &[&id]).unwrap().get(0)
    }
    fn range_select(&mut self, score: i32) -> usize {
        self.client.query(&self.range, &[&score]).unwrap().len()
    }
    fn update(&mut self, id: i64) {
        self.client.execute(&self.update, &[&id]).unwrap();
    }
    fn insert(&mut self, id: i64) {
        self.client.execute(&self.insert, &[&id]).unwrap();
    }
    fn transfer(&mut self, from: i64, to: i64) {
        loop {
            let mut tx = self.client.transaction().unwrap();
            let step = tx.execute(&self.debit, &[&from]).and_then(|_| tx.execute(&self.update, &[&to]));
            match step.and_then(|_| tx.commit()) {
                Ok(()) => return,
                Err(e) if e.code().is_some_and(|c| matches!(c.code(), "40001" | "40P01")) => {}
                Err(e) => panic!("{e}"),
            }
        }
    }
    fn fork(&self) -> Box<dyn Target> {
        Box::new(Postgres::connect(&self.config))
    }
}

// ------------------------------------------------------------- harness

struct Stats {
    p50: f64,
    p99: f64,
    ops_per_sec: f64,
}

fn summarize(mut nanos: Vec<u64>, wall: Duration) -> Stats {
    nanos.sort_unstable();
    let at = |p: f64| nanos[((nanos.len() as f64 * p) as usize).min(nanos.len() - 1)] as f64 / 1e3;
    Stats { p50: at(0.50), p99: at(0.99), ops_per_sec: nanos.len() as f64 / wall.as_secs_f64() }
}

/// Number of loaded rows, for the worker threads.
static ROWS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);

/// Cheap deterministic key stream.
fn key(i: u64, rows: i64) -> i64 {
    ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) % rows as u64) as i64 + 1
}

fn single(target: &mut dyn Target, ops: usize, mut f: impl FnMut(&mut dyn Target, u64)) -> Stats {
    for i in 0..(ops / 10).max(100) {
        f(target, i as u64 + 1_000_000_007);
    }
    let mut nanos = Vec::with_capacity(ops);
    let start = Instant::now();
    for i in 0..ops {
        let t = Instant::now();
        f(target, i as u64);
        nanos.push(t.elapsed().as_nanos() as u64);
    }
    summarize(nanos, start.elapsed())
}

fn parallel(target: &dyn Target, threads: usize, ops: usize, f: fn(&mut dyn Target, u64)) -> Stats {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let workers: Vec<_> = (0..threads)
        .map(|t| {
            let mut conn = target.fork();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut nanos = Vec::with_capacity(ops);
                barrier.wait();
                for i in 0..ops {
                    let start = Instant::now();
                    f(&mut *conn, (t * 1_000_003 + i) as u64);
                    nanos.push(start.elapsed().as_nanos() as u64);
                }
                nanos
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    let mut all = Vec::new();
    for w in workers {
        all.extend(w.join().unwrap());
    }
    summarize(all, start.elapsed())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    let rows: i64 = flag("--rows").map_or(100_000, |v| v.parse().expect("--rows"));
    let ops: usize = flag("--ops").map_or(20_000, |v| v.parse().expect("--ops"));
    let threads: usize = flag("--threads").map_or(8, |v| v.parse().expect("--threads"));
    let only = flag("--only").unwrap_or_else(|| "squad,sqlite,postgres".to_string());
    let pg = flag("--postgres");
    let json_path = flag("--json");

    let dir = tempfile::tempdir().expect("temp dir");
    let mut targets: Vec<Box<dyn Target>> = Vec::new();
    if only.contains("squad") {
        // SQUAD_BENCH_MEMORY=1 runs squad on its in-memory engine, which
        // helps to tell storage effects from everything above it.
        let db = if std::env::var_os("SQUAD_BENCH_MEMORY").is_some() {
            Database::open_in_memory().unwrap()
        } else {
            Database::open(dir.path().join("squad"), Config { sync_mode: SyncMode::Sync, ..Config::default() }).unwrap()
        };
        targets.push(Box::new(Squad::connect(&db)));
    }
    if only.contains("sqlite") {
        targets.push(Box::new(Sqlite::connect(&dir.path().join("sqlite.db"))));
    }
    match (&pg, only.contains("postgres")) {
        (Some(config), true) => {
            let mut p = Postgres::connect(config);
            p.client.batch_execute("TRUNCATE bench").unwrap();
            targets.push(Box::new(p));
        }
        (None, true) => eprintln!("note: no --postgres connection string given, skipping PostgreSQL"),
        _ => {}
    }

    println!("rows: {rows}, operations per measurement: {ops}, threads for concurrent runs: {threads}\n");
    println!("{:<44} {:<9} {:>10} {:>10} {:>12}", "workload", "system", "p50 (µs)", "p99 (µs)", "ops/s");
    let mut json = String::from("[");
    let mut emit = |workload: &str, system: &str, s: &Stats| {
        println!("{workload:<44} {system:<9} {:>10.2} {:>10.2} {:>12.0}", s.p50, s.p99, s.ops_per_sec);
        write!(
            json,
            "{}{{\"workload\":\"{workload}\",\"system\":\"{system}\",\"p50_us\":{:.3},\"p99_us\":{:.3},\"ops_per_sec\":{:.1}}}",
            if json.len() > 1 { "," } else { "" },
            s.p50,
            s.p99,
            s.ops_per_sec
        )
        .unwrap();
    };

    for target in &mut targets {
        target.load(rows);
    }
    let durable_ops = (ops / 10).max(200);
    type Work = (&'static str, usize, fn(&mut dyn Target, u64, i64));
    let workloads: [Work; 5] = [
        ("point select by primary key", ops, |t, i, rows| {
            std::hint::black_box(t.point_select(key(i, rows)));
        }),
        ("index lookup, 10 rows", ops, |t, i, _| {
            std::hint::black_box(t.range_select((i % 1000) as i32));
        }),
        ("update one row + durable commit", durable_ops, |t, i, rows| t.update(key(i, rows))),
        ("insert one row + durable commit", durable_ops, |t, i, rows| t.insert(rows + 1_000_000 + i as i64)),
        ("2-row transaction + durable commit", durable_ops, |t, i, rows| t.transfer(key(i, rows), key(i + 7, rows))),
    ];
    for (name, n, work) in workloads {
        for target in &mut targets {
            let stats = single(&mut **target, n, |t, i| work(t, i, rows));
            emit(name, target.name(), &stats);
        }
        println!();
    }

    // Concurrent runs: every thread has its own connection.
    ROWS.store(rows, std::sync::atomic::Ordering::Relaxed);
    let loaded = || ROWS.load(std::sync::atomic::Ordering::Relaxed);
    let read: fn(&mut dyn Target, u64) = |t, i| {
        std::hint::black_box(t.point_select(key(i, ROWS.load(std::sync::atomic::Ordering::Relaxed))));
    };
    let write: fn(&mut dyn Target, u64) = |t, i| {
        let rows = ROWS.load(std::sync::atomic::Ordering::Relaxed);
        t.transfer(key(i, rows), key(i + 13, rows))
    };
    let _ = loaded;
    for target in &targets {
        let stats = parallel(&**target, threads, ops, read);
        emit(&format!("point select, {threads} threads"), target.name(), &stats);
    }
    println!();
    for target in &targets {
        let stats = parallel(&**target, threads, durable_ops, write);
        emit(&format!("2-row transaction, {threads} threads"), target.name(), &stats);
    }
    json.push(']');
    if let Some(path) = json_path {
        std::fs::write(&path, json).expect("write json");
        println!("\nwrote {path}");
    }
}
