//! Loads the same events into clusters of 1, 2 and 4 local shards and
//! measures ingest, single-tenant queries and queries across all shards.
//!
//! ```sh
//! cargo run --release -p squad-cluster --example cluster_bench -- 2000000 1,2,4 100000
//! ```
//!
//! All shards live on the machine that runs the example, so the numbers
//! show the cost of routing and merging and how well shards use the cores
//! of one machine; they say nothing about adding machines.

use std::time::Instant;

use squad::Value;
use squad_cluster::{Cluster, ClusterConfig, ClusterConnection, ShardSpec};

const PROJECTS: i64 = 200;
const EVENTS: [&str; 8] = ["pageview", "click", "signup", "purchase", "error", "scroll", "search", "login"];
const COUNTRIES: [&str; 10] = ["DE", "US", "GB", "FR", "NL", "ES", "IT", "PL", "SE", "JP"];
/// 2024-01-01 in microseconds since 2000-01-01.
const START: i64 = 8766 * 86_400 * 1_000_000;
/// The events cover 90 days.
const SPAN: i64 = 90 * 86_400 * 1_000_000;

fn row(g: i64, rows: i64) -> Vec<Value> {
    vec![
        Value::Timestamp(START + (g as i128 * SPAN as i128 / rows as i128) as i64),
        Value::Int8((g * 7919) % PROJECTS),
        Value::Text(EVENTS[((g * 31) % 8) as usize].into()),
        Value::Int8((g * 15_485_863) % 5_000_000),
        Value::Text(format!("/app/section-{}/page-{}", (g * 13) % 40, (g * 17) % 250)),
        Value::Text(COUNTRIES[((g * 3) % 10) as usize].into()),
        Value::Int4(((g * 2_654_435_761) % 30_000) as i32),
    ]
}

const COLUMNS: [&str; 7] = ["ts", "project_id", "event", "user_id", "path", "country", "duration_ms"];

/// Best of five runs, in milliseconds, and the number of rows returned.
fn time(conn: &ClusterConnection, sql: &str) -> (f64, usize) {
    let mut best = f64::MAX;
    let mut n = 0;
    for _ in 0..5 {
        let start = Instant::now();
        n = conn.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).len();
        best = best.min(start.elapsed().as_secs_f64() * 1e3);
    }
    (best, n)
}

/// Single-row inserts with a durable commit each, from `threads` connections.
fn single_inserts(cluster: &Cluster, threads: i64, per_thread: i64) -> f64 {
    let start = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..threads {
            let cluster = cluster.clone();
            scope.spawn(move || {
                let conn = cluster.connect();
                let insert = conn.prepare("INSERT INTO hits (project_id, id, path) VALUES ($1, $2, $3)").unwrap();
                for i in 0..per_thread {
                    let g = t * per_thread + i;
                    insert.execute(&[&((g * 7919) % PROJECTS), &g, &"/pricing"]).unwrap();
                }
            });
        }
    });
    (threads * per_thread) as f64 / start.elapsed().as_secs_f64()
}

const QUERIES: [(&str, &str); 9] = [
    (
        "one project: events per day, 30 days",
        "SELECT date_trunc('day', ts), count(*) FROM events WHERE project_id = 42 \
         AND ts >= '2024-02-01' AND ts < '2024-03-02' GROUP BY 1 ORDER BY 1",
    ),
    (
        "one project: unique users per day (approx)",
        "SELECT date_trunc('day', ts), approx_count_distinct(user_id) FROM events WHERE project_id = 42 GROUP BY 1 ORDER BY 1",
    ),
    (
        "one project: top 10 paths of one week",
        "SELECT path, count(*) FROM events WHERE project_id = 42 AND ts >= '2024-02-01' AND ts < '2024-02-08' \
         GROUP BY path ORDER BY 2 DESC, 1 LIMIT 10",
    ),
    ("one project: p50/p95 duration", "SELECT approx_percentile(duration_ms, 0.5), approx_percentile(duration_ms, 0.95) FROM events WHERE project_id = 42"),
    ("all shards: count(*)", "SELECT count(*) FROM events"),
    ("all shards: count and avg duration by event", "SELECT event, count(*), avg(duration_ms) FROM events GROUP BY event ORDER BY 1"),
    (
        "all shards: events per day by country, one week",
        "SELECT date_trunc('day', ts), country, count(*) FROM events WHERE ts >= '2024-02-01' AND ts < '2024-02-08' GROUP BY 1, 2 ORDER BY 1, 2",
    ),
    (
        "all shards: p95 duration per project",
        "SELECT project_id, approx_percentile(duration_ms, 0.95) FROM events GROUP BY project_id ORDER BY 1",
    ),
    ("all shards: 20 slowest events", "SELECT project_id, ts, duration_ms FROM events ORDER BY duration_ms DESC, ts LIMIT 20"),
];

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(2_000_000, |v| v.parse().expect("row count"));
    let shard_counts: Vec<usize> =
        args.next().map_or(vec![1, 2, 4], |v| v.split(',').map(|n| n.parse().expect("shard count")).collect());
    let batch: i64 = args.next().map_or(100_000, |v| v.parse().expect("bulk batch size"));

    println!("rows: {rows}, projects: {PROJECTS}, bulk batch: {batch} rows per shard");
    let mut results: Vec<Vec<(f64, usize)>> = Vec::new();
    for &shards in &shard_counts {
        let dir = tempfile::tempdir().unwrap();
        let specs = (0..shards).map(|i| ShardSpec::local(dir.path().join(format!("shard-{i}")))).collect();
        let cluster = Cluster::open(ClusterConfig::new(specs)).unwrap();
        let conn = cluster.connect();
        conn.batch_execute(
            "CREATE TABLE events (ts timestamp NOT NULL, project_id bigint NOT NULL, event text NOT NULL, user_id bigint,
                                  path text, country text, duration_ms int)
                 USING columnar WITH (order_by = 'project_id, ts');
             CREATE TABLE hits (project_id bigint NOT NULL, id bigint NOT NULL, path text, PRIMARY KEY (project_id, id));
             SELECT squad_distribute('events', 'project_id');
             SELECT squad_distribute('hits', 'project_id');",
        )
        .unwrap();

        println!("\n== {shards} shard(s) ==");
        let start = Instant::now();
        let mut from = 0;
        while from < rows {
            // Each shard receives about `batch` rows per call. (Batches of
            // 50,000 rows or more are written as column segments directly.)
            let to = (from + batch * shards as i64).min(rows);
            conn.insert_rows("events", &COLUMNS, (from..to).map(|g| row(g, rows)).collect()).unwrap();
            from = to;
        }
        let load = start.elapsed().as_secs_f64();
        println!("bulk load (insert_rows):          {load:7.1} s  {:9.0} rows/s", rows as f64 / load);
        let start = Instant::now();
        conn.batch_execute("VACUUM").unwrap();
        println!("VACUUM (compaction, all shards):  {:7.1} s", start.elapsed().as_secs_f64());
        let per_shard: Vec<i64> = cluster
            .shards()
            .iter()
            .map(|s| s.database().unwrap().connect().query_one("SELECT count(*) FROM events", &[]).unwrap().get(0))
            .collect();
        println!("rows per shard:                   {per_shard:?}");

        println!("single-row inserts, durable commit each:");
        for threads in [1, 4, 8] {
            println!(
                "  {threads} connection(s):                {:9.0} rows/s",
                single_inserts(&cluster, threads, 4000 / threads)
            );
            conn.batch_execute("TRUNCATE hits").unwrap();
        }

        results.push(QUERIES.iter().map(|(_, sql)| time(&conn, sql)).collect());
    }

    println!("\nquery times in ms (best of 5), by number of shards");
    print!("{:<50}", "query");
    for s in &shard_counts {
        print!("{:>10}", format!("{s} shard{}", if *s == 1 { "" } else { "s" }));
    }
    println!("{:>8}", "rows");
    for (i, (name, _)) in QUERIES.iter().enumerate() {
        print!("{name:<50}");
        for r in &results {
            print!("{:>10.2}", r[i].0);
        }
        println!("{:>8}", results[0][i].1);
        assert!(results.iter().all(|r| r[i].1 == results[0][i].1), "{name}: row counts differ between clusters");
    }
}
