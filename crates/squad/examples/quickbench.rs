//! A quick latency check of the hot paths, without a benchmark harness.
//!
//! ```text
//! cargo run --release -p squad --example quickbench [-- <dir>]
//! ```
//!
//! Without a directory the database lives in memory.

use std::time::Instant;

use squad::{Config, Database, SyncMode};

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn report(name: &str, mut nanos: Vec<u64>) {
    nanos.sort_unstable();
    let total: u64 = nanos.iter().sum();
    println!(
        "{name:<34} p50 {:>7.2} µs   p99 {:>7.2} µs   mean {:>7.2} µs   {:>9.0} ops/s",
        percentile(&nanos, 0.50) as f64 / 1e3,
        percentile(&nanos, 0.99) as f64 / 1e3,
        total as f64 / nanos.len() as f64 / 1e3,
        nanos.len() as f64 / (total as f64 / 1e9),
    );
}

fn time<T>(n: usize, mut f: impl FnMut(usize) -> T) -> Vec<u64> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        std::hint::black_box(f(i));
        out.push(start.elapsed().as_nanos() as u64);
    }
    out
}

fn main() -> squad::Result<()> {
    let dir = std::env::args().nth(1);
    let db = match &dir {
        Some(d) => Database::open(d, Config { sync_mode: SyncMode::Async, ..Config::default() })?,
        None => Database::open_in_memory()?,
    };
    let conn = db.connect();
    const ROWS: i64 = 100_000;
    conn.batch_execute(
        "DROP TABLE IF EXISTS bench;
         CREATE TABLE bench (id bigint PRIMARY KEY, name text NOT NULL, score int NOT NULL, tag text);
         CREATE INDEX bench_score_idx ON bench (score);",
    )?;
    let start = Instant::now();
    conn.execute(
        "INSERT INTO bench SELECT g, 'user ' || g, g % 1000, CASE WHEN g % 3 = 0 THEN 'x' END FROM generate_series(1, $1) g",
        &[&ROWS],
    )?;
    println!("loaded {ROWS} rows in {:.0} ms", start.elapsed().as_secs_f64() * 1e3);

    // Cheap deterministic pseudo-random keys.
    let key = |i: usize| ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) as i64 % ROWS + 1;

    let select = conn.prepare("SELECT name FROM bench WHERE id = $1")?;
    time(50_000, |i| select.query_one(&[&key(i)]).unwrap());
    report("point select by primary key", time(500_000, |i| select.query_one(&[&key(i)]).unwrap()));

    let by_index = conn.prepare("SELECT id FROM bench WHERE score = $1 LIMIT 10")?;
    report("index range, 10 rows", time(100_000, |i| by_index.query(&[&((i % 1000) as i32)]).unwrap()));

    let update = conn.prepare("UPDATE bench SET score = score + 1 WHERE id = $1")?;
    report("update by primary key", time(100_000, |i| update.execute(&[&key(i)]).unwrap()));

    let insert = conn.prepare("INSERT INTO bench VALUES ($1, $2, $3, NULL)")?;
    report("insert one row", time(100_000, |i| insert.execute(&[&(ROWS + 1 + i as i64), &"new", &7i32]).unwrap()));

    report("ad-hoc select (plan cache)", time(100_000, |i| conn.query(&format!("SELECT name FROM bench WHERE id = {}", key(i) % 64 + 1), &[]).unwrap()));
    report("aggregate over 100k rows", time(20, |_| conn.query_one("SELECT count(*), sum(score) FROM bench", &[]).unwrap()));
    Ok(())
}
