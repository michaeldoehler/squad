//! Ingestion throughput into a columnar events table, the write path of an
//! analytics or error-tracking service.
//!
//! ```text
//! cargo run --release -p squad-bench --bin ingest -- [--mode sql|prepared|append|copy] [--threads N]
//!     [--events N] [--batch N] [--dir PATH] [--async] [--no-reader]
//! ```
//!
//! Every writer thread inserts batches of `--batch` events (default
//! 1,000) into one columnar table with a `jsonb` properties column. Commits
//! are durable (`SyncMode::Sync`) unless `--async` is given. Modes:
//!
//! * `sql`: one multi-row `INSERT … VALUES (…), (…)` statement as text per
//!   batch, parsed every time.
//! * `prepared`: a prepared `INSERT` with one parameter per value.
//! * `append`: [`squad::Connection::append`], no SQL at all.
//! * `copy`: `COPY events FROM STDIN` through the wire-protocol server
//!   (in-process, over TCP on localhost) with the `postgres` client.
//!
//! A reader thread runs a small aggregation every 200 ms and its latency
//! is reported, which shows what compaction costs readers and writers.

use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use squad::{Config, Database, SyncMode, Value};

const SCHEMA: &str = "CREATE TABLE events (
    ts timestamptz NOT NULL,
    project_id int NOT NULL,
    event text NOT NULL,
    distinct_id text NOT NULL,
    properties jsonb
) USING columnar WITH (order_by = 'project_id, ts')";

const EVENTS: [&str; 8] =
    ["$pageview", "$autocapture", "$pageleave", "signup", "purchase", "$exception", "search", "login"];
const BROWSERS: [&str; 4] = ["Chrome", "Firefox", "Safari", "Edge"];
const OS: [&str; 4] = ["Mac OS X", "Windows", "Linux", "iOS"];

/// 2026-11-14 00:00:00 UTC in microseconds since 2000-01-01.
const BASE_TS: i64 = 816_307_200_000_000;

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

struct Event {
    ts: i64,
    project: i32,
    event: &'static str,
    distinct_id: String,
    properties: String,
}

fn event(i: u64) -> Event {
    let h = mix(i);
    Event {
        ts: BASE_TS + i as i64 * 1000,
        project: (h % 50) as i32,
        event: EVENTS[(h >> 8) as usize % EVENTS.len()],
        distinct_id: format!("user-{}", (h >> 16) % 100_000),
        properties: format!(
            "{{\"$browser\": \"{}\", \"$os\": \"{}\", \"$current_url\": \"https://example.com/app/page-{}\", \
             \"$screen_width\": {}, \"plan\": \"{}\", \"utm_source\": \"{}\"}}",
            BROWSERS[(h >> 24) as usize % 4],
            OS[(h >> 26) as usize % 4],
            (h >> 28) % 500,
            [1280, 1440, 1920, 390][(h >> 40) as usize % 4],
            ["free", "pro", "enterprise"][(h >> 42) as usize % 3],
            ["google", "twitter", "newsletter", "direct"][(h >> 44) as usize % 4],
        ),
    }
}

fn ts_text(ts: i64) -> String {
    let unix = ts / 1_000_000 + 946_684_800;
    let micros = ts % 1_000_000;
    let days = unix.div_euclid(86_400);
    let secs = unix.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{micros:06}+00", secs / 3600, secs / 60 % 60, secs % 60)
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Runs batches through one of the embedded modes or the server.
trait Writer: Send {
    fn write(&mut self, first: u64, n: usize);
}

struct SqlWriter(squad::Connection);

impl Writer for SqlWriter {
    fn write(&mut self, first: u64, n: usize) {
        let mut sql = String::with_capacity(n * 300);
        sql.push_str("INSERT INTO events VALUES ");
        for i in 0..n as u64 {
            let e = event(first + i);
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({}, {}, {}, {}, {})",
                quote(&ts_text(e.ts)),
                e.project,
                quote(e.event),
                quote(&e.distinct_id),
                quote(&e.properties)
            ));
        }
        self.0.execute(&sql, &[]).unwrap();
    }
}

fn values(first: u64, n: usize) -> Vec<Vec<Value>> {
    (0..n as u64)
        .map(|i| {
            let e = event(first + i);
            vec![
                Value::TimestampTz(e.ts),
                Value::Int4(e.project),
                Value::Text(e.event.to_string()),
                Value::Text(e.distinct_id),
                Value::Text(e.properties),
            ]
        })
        .collect()
}

struct PreparedWriter {
    stmt: squad::Statement,
    batch: usize,
}

impl Writer for PreparedWriter {
    fn write(&mut self, first: u64, n: usize) {
        assert_eq!(n, self.batch, "prepared mode needs whole batches");
        let rows = values(first, n);
        let params: Vec<&dyn squad::ToSql> = rows.iter().flatten().map(|v| v as &dyn squad::ToSql).collect();
        self.stmt.execute(&params).unwrap();
    }
}

struct AppendWriter(squad::Connection);

impl Writer for AppendWriter {
    fn write(&mut self, first: u64, n: usize) {
        self.0.append("events", &values(first, n)).unwrap();
    }
}

struct CopyWriter(postgres::Client);

impl Writer for CopyWriter {
    fn write(&mut self, first: u64, n: usize) {
        let mut data = Vec::with_capacity(n * 300);
        for i in 0..n as u64 {
            let e = event(first + i);
            // Text format: tab-separated, backslash escapes (the JSON holds none).
            writeln!(data, "{}\t{}\t{}\t{}\t{}", ts_text(e.ts), e.project, e.event, e.distinct_id, e.properties)
                .unwrap();
        }
        let mut w = self.0.copy_in("COPY events FROM STDIN").unwrap();
        w.write_all(&data).unwrap();
        w.finish().unwrap();
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    let has = |name: &str| args.iter().any(|a| a == name);
    let mode = flag("--mode").unwrap_or_else(|| "prepared".into());
    let threads: usize = flag("--threads").map_or(4, |v| v.parse().expect("--threads"));
    let total: u64 = flag("--events").map_or(2_000_000, |v| v.parse().expect("--events"));
    let batch: usize = flag("--batch").map_or(1000, |v| v.parse().expect("--batch"));
    let sync_mode = if has("--async") { SyncMode::Async } else { SyncMode::Sync };
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = flag("--dir").map_or_else(|| tmp.path().join("db"), std::path::PathBuf::from);

    let db = Database::open(&dir, Config { sync_mode, ..Config::default() }).unwrap();
    db.connect().batch_execute(SCHEMA).unwrap();
    let server = (mode == "copy").then(|| {
        let mut server = squad_server::Server::new(db.clone(), squad_server::ServerConfig::default());
        let addr = server.bind("127.0.0.1:0").unwrap();
        (server.spawn(), addr)
    });

    let mut writers: Vec<Box<dyn Writer>> = Vec::new();
    for _ in 0..threads {
        writers.push(match mode.as_str() {
            "sql" => Box::new(SqlWriter(db.connect())),
            "prepared" => {
                let conn = db.connect();
                let mut sql = String::from("INSERT INTO events VALUES ");
                for r in 0..batch {
                    let p = r * 5;
                    if r > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!("(${}, ${}, ${}, ${}, ${}::jsonb)", p + 1, p + 2, p + 3, p + 4, p + 5));
                }
                Box::new(PreparedWriter { stmt: conn.prepare(&sql).unwrap(), batch })
            }
            "append" => Box::new(AppendWriter(db.connect())),
            "copy" => {
                let addr = server.as_ref().unwrap().1;
                let config = format!("host=127.0.0.1 port={} user=squad dbname=squad", addr.port());
                Box::new(CopyWriter(postgres::Client::connect(&config, postgres::NoTls).unwrap()))
            }
            other => panic!("unknown mode {other}"),
        });
    }

    let next = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let reader = (!has("--no-reader")).then(|| {
        let (conn, stop) = (db.connect(), stop.clone());
        std::thread::spawn(move || {
            let mut lat = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let start = Instant::now();
                conn.query("SELECT event, count(*) FROM events WHERE project_id = 7 GROUP BY event", &[]).unwrap();
                lat.push(start.elapsed().as_secs_f64() * 1e3);
                std::thread::sleep(Duration::from_millis(200));
            }
            lat
        })
    });
    let handles: Vec<_> = writers
        .into_iter()
        .map(|mut w| {
            let (next, barrier, latencies) = (next.clone(), barrier.clone(), latencies.clone());
            std::thread::spawn(move || {
                let mut lat = Vec::new();
                barrier.wait();
                loop {
                    let first = next.fetch_add(batch as u64, Ordering::Relaxed);
                    if first >= total {
                        break;
                    }
                    let start = Instant::now();
                    w.write(first, batch);
                    lat.push(start.elapsed().as_secs_f64() * 1e3);
                }
                latencies.lock().unwrap().extend(lat);
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let wall = start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let mut reads = reader.map(|r| r.join().unwrap()).unwrap_or_default();

    let inserted = total.div_ceil(batch as u64) * batch as u64;
    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_by(f64::total_cmp);
    reads.sort_by(f64::total_cmp);
    println!(
        "mode={mode} threads={threads} batch={batch} events={inserted} sync={:?}",
        if sync_mode == SyncMode::Sync { "Sync" } else { "Async" }
    );
    println!("throughput:            {:>10.0} events/s ({wall:.2} s)", inserted as f64 / wall);
    println!(
        "batch latency (ms):    p50 {:>8.2}  p99 {:>8.2}  max {:>8.2}",
        percentile(&lat, 0.5),
        percentile(&lat, 0.99),
        lat.last().copied().unwrap_or(0.0)
    );
    if !reads.is_empty() {
        println!(
            "reader latency (ms):   p50 {:>8.2}  p99 {:>8.2}  max {:>8.2}  ({} queries)",
            percentile(&reads, 0.5),
            percentile(&reads, 0.99),
            reads.last().copied().unwrap_or(0.0),
            reads.len()
        );
    }
    let conn = db.connect();
    let n: i64 = conn.query_one("SELECT count(*) FROM events", &[]).unwrap().get(0);
    assert_eq!(n as u64, inserted, "every event is stored once");
    let start = Instant::now();
    conn.query("SELECT event, count(*) FROM events WHERE project_id = 7 GROUP BY event", &[]).unwrap();
    println!("query after ingest:    {:>10.2} ms", start.elapsed().as_secs_f64() * 1e3);
    if let Some((handle, _)) = server {
        handle.shutdown();
    }
}
