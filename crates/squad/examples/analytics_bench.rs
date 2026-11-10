//! Analytics workload through SQL: the same events in an ordinary table
//! and in a columnar table, and the queries a product-analytics service
//! runs on them.
//!
//! ```text
//! cargo run --release -p squad --example analytics_bench [-- <rows> [<dir>]]
//! ```

use std::time::Instant;

use squad::{Config, Connection, Database, SyncMode};

const COLUMNS: &str = "ts timestamp NOT NULL, project_id int NOT NULL, event text NOT NULL, user_id bigint NOT NULL, \
                       session_id bigint NOT NULL, path text NOT NULL, country text NOT NULL, duration_ms int NOT NULL, \
                       revenue numeric(12,2)";

fn load(conn: &Connection, table: &str, rows: i64) -> f64 {
    let start = Instant::now();
    let step = 250_000;
    let mut from = 1;
    while from <= rows {
        let to = (from + step - 1).min(rows);
        conn.execute(
            &format!(
                "INSERT INTO {table}
                 SELECT timestamp '2024-01-01' + (g * interval '1 second') * (7776000.0 / $3),
                        (g * 7919) % 100,
                        (ARRAY['pageview','click','signup','purchase','error','scroll','search','login'])[1 + (g * 31) % 8],
                        (g * 104729) % 1000000,
                        (g * 15485863) % 5000000,
                        '/app/section-' || ((g * 13) % 40) || '/page-' || ((g * 17) % 250),
                        (ARRAY['DE','US','GB','FR','NL','ES','IT','PL','SE','JP'])[1 + (g * 3) % 10],
                        (g * 2654435761) % 30000,
                        CASE WHEN g % 50 = 0 THEN ((g * 97) % 100000) / 100.0 END
                 FROM generate_series($1::bigint, $2::bigint) g"
            ),
            &[&from, &to, &(rows as f64)],
        )
        .unwrap();
        from = to + 1;
    }
    start.elapsed().as_secs_f64()
}

fn time(conn: &Connection, sql: &str) -> (f64, usize) {
    // Best of three: the machine may be busy.
    let mut best = f64::MAX;
    let mut n = 0;
    for _ in 0..3 {
        let start = Instant::now();
        n = conn.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).len();
        best = best.min(start.elapsed().as_secs_f64() * 1e3);
    }
    (best, n)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(2_000_000, |v| v.parse().expect("row count"));
    let db = match args.next() {
        Some(dir) => Database::open(dir, Config { sync_mode: SyncMode::Async, ..Config::default() }).unwrap(),
        None => Database::open_in_memory().unwrap(),
    };
    let conn = db.connect();
    conn.batch_execute(&format!(
        "CREATE TABLE events_row ({COLUMNS});
         CREATE TABLE events ({COLUMNS}) USING columnar WITH (order_by = 'project_id, ts');"
    ))
    .unwrap();

    let t_row = load(&conn, "events_row", rows);
    let t_col = load(&conn, "events", rows);
    let start = Instant::now();
    conn.batch_execute("VACUUM events").unwrap();
    let t_compact = start.elapsed().as_secs_f64();
    println!("rows: {rows}");
    println!("load row table:            {:>8.1} s  {:>9.0} rows/s", t_row, rows as f64 / t_row);
    println!("load columnar table:       {:>8.1} s  {:>9.0} rows/s (insert, including automatic compaction)", t_col, rows as f64 / t_col);
    println!("final VACUUM (compaction): {:>8.1} s\n", t_compact);

    let queries: &[(&str, &str)] = &[
        ("count(*) of one week", "SELECT count(*) FROM {t} WHERE ts >= '2024-02-01' AND ts < '2024-02-08'"),
        (
            "events per day, one project, 30 days",
            "SELECT date_trunc('day', ts) AS day, count(*) FROM {t} WHERE project_id = 42 AND ts >= '2024-02-01' AND ts < '2024-03-02' GROUP BY 1 ORDER BY 1",
        ),
        (
            "top 10 paths, one project, one week",
            "SELECT path, count(*) AS n FROM {t} WHERE project_id = 42 AND ts >= '2024-02-01' AND ts < '2024-02-08' GROUP BY path ORDER BY n DESC, path LIMIT 10",
        ),
        (
            "unique users per day, one project (exact)",
            "SELECT date_trunc('day', ts) AS day, count(DISTINCT user_id) FROM {t} WHERE project_id = 42 GROUP BY 1 ORDER BY 1",
        ),
        (
            "unique users per day, one project (approx)",
            "SELECT date_trunc('day', ts) AS day, approx_count_distinct(user_id) FROM {t} WHERE project_id = 42 GROUP BY 1 ORDER BY 1",
        ),
        (
            "p50/p95/p99 duration per event, all data",
            "SELECT event, approx_percentile(duration_ms, 0.5), approx_percentile(duration_ms, 0.95), approx_percentile(duration_ms, 0.99) FROM {t} GROUP BY event ORDER BY event",
        ),
        ("count and avg by country, all data", "SELECT country, count(*), avg(duration_ms) FROM {t} GROUP BY country ORDER BY country"),
        ("revenue per project, all data", "SELECT project_id, sum(revenue), count(revenue) FROM {t} GROUP BY project_id ORDER BY project_id"),
        ("one user's events", "SELECT ts, event, path FROM {t} WHERE user_id = 123457 ORDER BY ts"),
        ("sum over all data", "SELECT sum(duration_ms), count(*) FROM {t}"),
        ("errors per hour, last day of one project", "SELECT date_trunc('hour', ts), count(*) FROM {t} WHERE project_id = 7 AND event = 'error' AND ts >= '2024-03-30' GROUP BY 1 ORDER BY 1"),
    ];
    println!("{:<46} {:>12} {:>12} {:>9}", "query", "row (ms)", "columnar (ms)", "speed-up");
    for (name, sql) in queries {
        let (row_ms, n1) = time(&conn, &sql.replace("{t}", "events_row"));
        let (col_ms, n2) = time(&conn, &sql.replace("{t}", "events"));
        assert_eq!(n1, n2, "{name}: different number of result rows");
        println!("{name:<46} {row_ms:>12.1} {col_ms:>12.2} {:>8.0}x", row_ms / col_ms);
    }
}
