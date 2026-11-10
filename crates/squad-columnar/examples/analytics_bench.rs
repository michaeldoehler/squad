//! Benchmark on a synthetic product-analytics events table.
//!
//! ```text
//! cargo run --release -p squad-columnar --example analytics_bench -- [rows] [threads]
//! ```
//!
//! Generates `rows` events (default 20 million) spread over 90 days,
//! writes them as segments of one million rows sorted by
//! `(project_id, ts)`, and times typical dashboard queries on one thread
//! and on `threads` threads (default 4). Prints a Markdown table.

use std::sync::Arc;
use std::time::Instant;

use squad_columnar::*;

const TS: usize = 0;
const PROJECT: usize = 1;
const EVENT: usize = 2;
const USER: usize = 3;
const SESSION: usize = 4;
const PATH: usize = 5;
const COUNTRY: usize = 6;
const BROWSER: usize = 7;
const DURATION: usize = 8;
const REVENUE: usize = 9;
const PROPS: usize = 10;

const DAY: i64 = 86_400_000_000;
const START: i64 = 1_700_000_000_000_000;
const SEGMENT_ROWS: usize = 1_000_000;

struct Rng(u64);

impl Rng {
    #[inline]
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    #[inline]
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// Skewed towards small values, like popularity distributions.
    #[inline]
    fn skewed(&mut self, n: u64) -> u64 {
        let u = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        ((u * u * u) * n as f64) as u64
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", ColumnType::Int64, false),
        Field::new("project_id", ColumnType::Int64, false),
        Field::new("event", ColumnType::Utf8, false),
        Field::new("user_id", ColumnType::Int64, false),
        Field::new("session_id", ColumnType::Utf8, false),
        Field::new("path", ColumnType::Utf8, false),
        Field::new("country", ColumnType::Utf8, false),
        Field::new("browser", ColumnType::Utf8, false),
        Field::new("duration_ms", ColumnType::Int64, false),
        Field::new("revenue", ColumnType::Decimal { scale: 2 }, true),
        Field::new("properties", ColumnType::Utf8, false),
    ]))
}

/// One segment's worth of events in arrival (time) order.
fn generate(rng: &mut Rng, first_row: usize, rows: usize, total: usize) -> Batch {
    let step = 90 * DAY / total as i64;
    let mut ts = Vec::with_capacity(rows);
    let (mut project, mut user, mut duration) =
        (Vec::with_capacity(rows), Vec::with_capacity(rows), Vec::with_capacity(rows));
    let (mut event, mut session, mut path) = (BytesData::new(), BytesData::new(), BytesData::new());
    let (mut country, mut browser, mut props) = (BytesData::new(), BytesData::new(), BytesData::new());
    let mut revenue = Vec::with_capacity(rows);
    let mut revenue_valid = Vec::with_capacity(rows);
    let mut buf = String::new();
    use std::fmt::Write;
    for i in 0..rows {
        let t = START + (first_row + i) as i64 * step + rng.below(1000) as i64;
        ts.push(t);
        project.push(rng.skewed(100) as i64);
        let u = rng.below(1_000_000);
        user.push(u as i64);
        duration.push(((rng.skewed(1000) + 1) * (rng.below(60) + 1)) as i64);
        let e = rng.skewed(50);
        buf.clear();
        write!(buf, "event_{e:02}").unwrap();
        event.push(buf.as_bytes());
        buf.clear();
        write!(buf, "{:08x}-{:04x}-4{:03x}-{:012x}", u, (t / DAY) & 0xffff, u % 4096, u * 2_654_435_761 % (1 << 48))
            .unwrap();
        session.push(buf.as_bytes());
        buf.clear();
        write!(buf, "/app/section-{}/page-{}", rng.skewed(100), rng.skewed(100)).unwrap();
        path.push(buf.as_bytes());
        buf.clear();
        write!(buf, "C{:03}", rng.skewed(200)).unwrap();
        country.push(buf.as_bytes());
        buf.clear();
        write!(buf, "browser-{}", rng.skewed(20)).unwrap();
        browser.push(buf.as_bytes());
        let paid = rng.below(20) == 0;
        revenue.push(if paid { rng.below(50_000) as i128 } else { 0 });
        revenue_valid.push(paid);
        buf.clear();
        write!(buf, "{{\"plan\":\"p{}\",\"ab\":{},\"w\":{}}}", rng.below(4), rng.below(2), 320 + rng.below(8) * 160)
            .unwrap();
        props.push(buf.as_bytes());
    }
    let plain = |d: BytesData| Column::new(ColumnData::Bytes(BytesColumn::Plain(d)), None);
    Batch::new(
        schema(),
        vec![
            Column::new(ColumnData::Int64(ts), None),
            Column::new(ColumnData::Int64(project), None),
            plain(event),
            Column::new(ColumnData::Int64(user), None),
            plain(session),
            plain(path),
            plain(country),
            plain(browser),
            Column::new(ColumnData::Int64(duration), None),
            Column::new(ColumnData::Decimal(revenue), Some(revenue_valid)),
            plain(props),
        ],
    )
    .unwrap()
}

fn timed<T>(f: impl Fn() -> T) -> (f64, T) {
    // Best of three: other jobs may be running on the machine.
    let mut best = f64::MAX;
    let mut out = None;
    for _ in 0..3 {
        let start = Instant::now();
        let r = f();
        best = best.min(start.elapsed().as_secs_f64());
        out = Some(r);
    }
    (best, out.unwrap())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let total: usize = args.first().map_or(20_000_000, |a| a.parse().expect("rows"));
    let threads: usize = args.get(1).map_or(4, |a| a.parse().expect("threads"));
    let schema = schema();
    let mut rng = Rng(0x5EED_1234_5678_9ABC);

    // Ingest.
    let mut source = TableSource::new(schema.clone());
    let (mut encode_secs, mut raw_bytes, mut stored_bytes) = (0.0, 0u64, 0u64);
    let mut sample: Option<Batch> = None;
    let mut done = 0;
    while done < total {
        let n = SEGMENT_ROWS.min(total - done);
        let batch = generate(&mut rng, done, n, total);
        raw_bytes += batch
            .columns
            .iter()
            .map(|c| match &c.data {
                ColumnData::Int64(v) => v.len() as u64 * 8,
                ColumnData::Decimal(v) => v.len() as u64 * 16,
                ColumnData::Bytes(BytesColumn::Plain(d)) => d.data.len() as u64 + d.len() as u64 * 4,
                _ => 0,
            })
            .sum::<u64>();
        let start = Instant::now();
        let mut w =
            SegmentWriter::new(schema.clone(), WriterOptions { sort_key: vec![PROJECT, TS], ..Default::default() });
        w.write_batch(&batch).unwrap();
        let bytes = w.finish().unwrap();
        encode_secs += start.elapsed().as_secs_f64();
        stored_bytes += bytes.len() as u64;
        source.segments.push(Arc::new(Segment::open(Arc::new(bytes)).unwrap()));
        if sample.is_none() {
            sample = Some(batch);
        }
        done += n;
    }
    let rows = source.num_rows();
    println!("## Data\n");
    println!("| | |\n| --- | --- |");
    println!("| rows | {rows} |");
    println!(
        "| segments | {} of up to {SEGMENT_ROWS} rows, sorted by (project_id, ts), 65 536 rows per row group |",
        source.segments.len()
    );
    println!(
        "| raw size | {:.1} bytes/row ({:.2} GiB) |",
        raw_bytes as f64 / rows as f64,
        raw_bytes as f64 / (1u64 << 30) as f64
    );
    println!(
        "| stored size | {:.1} bytes/row ({:.2} GiB), {:.1}x smaller |",
        stored_bytes as f64 / rows as f64,
        stored_bytes as f64 / (1u64 << 30) as f64,
        raw_bytes as f64 / stored_bytes as f64
    );
    println!(
        "| encode rate (sort + encode + compress, one thread) | {:.2} million rows/s |",
        rows as f64 / encode_secs / 1e6
    );

    // Baseline: the same aggregation as "events per day for one project",
    // row at a time over materialized rows.
    let sample_rows = sample.unwrap().to_rows();
    let (naive_secs, _) = timed(|| {
        let mut per_day: std::collections::HashMap<i64, u64> = std::collections::HashMap::new();
        for r in &sample_rows {
            if r[PROJECT] == Datum::Int(3) {
                if let Datum::Int(t) = r[TS] {
                    *per_day.entry(t.div_euclid(DAY)).or_default() += 1;
                }
            }
        }
        per_day.len()
    });
    let naive_rate = sample_rows.len() as f64 / naive_secs;
    drop(sample_rows);

    let last_30 = Predicate::Cmp { column: TS, op: CmpOp::Ge, value: Datum::Int(START + 60 * DAY) };
    let week =
        Predicate::Between { column: TS, low: Datum::Int(START + 40 * DAY), high: Datum::Int(START + 47 * DAY - 1) };
    let project = |p: i64| Predicate::Cmp { column: PROJECT, op: CmpOp::Eq, value: Datum::Int(p) };
    let day = GroupKey::TimeBucket { column: TS, unit: TimeUnit::Day, utc_offset_micros: 0, epoch_unix_micros: 0 };
    // A session id that exists, taken from the middle of the table.
    let middle = &source.segments[source.segments.len() / 2];
    let existing_session = middle.read_column(middle.num_row_groups() / 2, SESSION).unwrap().datum(1234);
    let queries: Vec<(&str, AggregateRequest)> = vec![
        (
            "count(*) for one week, all projects",
            AggregateRequest {
                predicate: Some(week.clone()),
                aggregates: vec![Aggregate::CountStar],
                ..Default::default()
            },
        ),
        (
            "events per day, one project, last 30 days",
            AggregateRequest {
                predicate: Some(Predicate::And(vec![project(3), last_30.clone()])),
                group_by: vec![day.clone()],
                aggregates: vec![Aggregate::CountStar],
                order_by: vec![OrderBy { column: 0, descending: false }],
                ..Default::default()
            },
        ),
        (
            "top 10 paths by views, one project, one week",
            AggregateRequest {
                predicate: Some(Predicate::And(vec![project(3), week.clone()])),
                group_by: vec![GroupKey::Column(PATH)],
                aggregates: vec![Aggregate::CountStar],
                order_by: vec![OrderBy { column: 1, descending: true }],
                limit: Some(10),
                ..Default::default()
            },
        ),
        (
            "unique users per day (exact), one project, last 30 days",
            AggregateRequest {
                predicate: Some(Predicate::And(vec![project(3), last_30.clone()])),
                group_by: vec![day.clone()],
                aggregates: vec![Aggregate::CountDistinct(USER)],
                ..Default::default()
            },
        ),
        (
            "unique users per day (approximate), one project, last 30 days",
            AggregateRequest {
                predicate: Some(Predicate::And(vec![project(3), last_30.clone()])),
                group_by: vec![day.clone()],
                aggregates: vec![Aggregate::ApproxCountDistinct(USER)],
                ..Default::default()
            },
        ),
        (
            "unique users per day (approximate), all projects, 90 days",
            AggregateRequest {
                group_by: vec![day.clone()],
                aggregates: vec![Aggregate::ApproxCountDistinct(USER)],
                ..Default::default()
            },
        ),
        (
            "p50/p95/p99 duration per event name, whole table",
            AggregateRequest {
                group_by: vec![GroupKey::Column(EVENT)],
                aggregates: vec![
                    Aggregate::ApproxPercentile { column: DURATION, percentile: 0.5 },
                    Aggregate::ApproxPercentile { column: DURATION, percentile: 0.95 },
                    Aggregate::ApproxPercentile { column: DURATION, percentile: 0.99 },
                ],
                ..Default::default()
            },
        ),
        (
            "events by country and browser, whole table",
            AggregateRequest {
                group_by: vec![GroupKey::Column(COUNTRY), GroupKey::Column(BROWSER)],
                aggregates: vec![Aggregate::CountStar, Aggregate::Avg(DURATION)],
                ..Default::default()
            },
        ),
        (
            "revenue per project (sum of decimal), whole table",
            AggregateRequest {
                group_by: vec![GroupKey::Column(PROJECT)],
                aggregates: vec![Aggregate::Sum(REVENUE)],
                ..Default::default()
            },
        ),
        (
            "all events of one user (bloom filter)",
            AggregateRequest {
                predicate: Some(Predicate::Cmp { column: USER, op: CmpOp::Eq, value: Datum::Int(424_242) }),
                aggregates: vec![Aggregate::CountStar, Aggregate::Max(TS)],
                ..Default::default()
            },
        ),
        (
            "all events of one session (bloom filter on text)",
            AggregateRequest {
                predicate: Some(Predicate::Cmp { column: SESSION, op: CmpOp::Eq, value: existing_session.clone() }),
                aggregates: vec![Aggregate::CountStar],
                ..Default::default()
            },
        ),
        (
            "sum(duration_ms), whole table",
            AggregateRequest { aggregates: vec![Aggregate::Sum(DURATION)], ..Default::default() },
        ),
        (
            "events with path LIKE '/app/section-1/%', whole table",
            AggregateRequest {
                predicate: Some(Predicate::Like {
                    column: PATH,
                    pattern: "/app/section-1/%".into(),
                    case_insensitive: false,
                }),
                aggregates: vec![Aggregate::CountStar],
                ..Default::default()
            },
        ),
    ];

    println!("\n## Queries\n");
    println!(
        "Row-at-a-time baseline (hash map over materialized rows, one thread): {:.1} million rows/s.\n",
        naive_rate / 1e6
    );
    println!("| Query | groups | 1 thread | {threads} threads | table rows/s ({threads} threads) | vs. baseline |");
    println!("| --- | ---: | ---: | ---: | ---: | ---: |");
    let fmt = |secs: f64| {
        if secs < 1e-3 {
            format!("{:.0} µs", secs * 1e6)
        } else if secs < 1.0 {
            format!("{:.1} ms", secs * 1e3)
        } else {
            format!("{secs:.2} s")
        }
    };
    for (name, mut request) in queries {
        request.parallelism = 1;
        let (one, result) = timed(|| aggregate(&source, &request).unwrap());
        request.parallelism = threads;
        let (many, _) = timed(|| aggregate(&source, &request).unwrap());
        let rate = rows as f64 / many;
        println!(
            "| {name} | {} | {} | {} | {:.0} million | {:.0}x |",
            result.num_rows(),
            fmt(one),
            fmt(many),
            rate / 1e6,
            rate / naive_rate
        );
    }

    // A filtered scan returning rows, as the generic SQL executor would
    // request for queries the aggregation path does not cover.
    let request = ScanRequest {
        projection: vec![TS, EVENT, PATH, PROPS],
        predicate: Some(Predicate::And(vec![project(3), week])),
        limit: None,
    };
    let (secs, returned) = timed(|| scan(&source, &request).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>());
    println!(
        "\nScan returning 4 columns of one project's week: {returned} rows in {} ({:.1} million rows/s returned).",
        fmt(secs),
        returned as f64 / secs / 1e6
    );
}
