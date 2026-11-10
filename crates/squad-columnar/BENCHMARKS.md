# squad-columnar benchmarks

```sh
cargo run --release -p squad-columnar --example analytics_bench -- 20000000 4
```

The example generates a synthetic product-analytics events table, writes
it as column segments and times typical dashboard queries. It prints the
tables below; re-run it to reproduce.

## Read this first

These numbers were measured **on a busy machine**: 4 virtual cores
(Intel Xeon @ 2.30 GHz, 15 GiB RAM), with a load average between 60 and
66 from unrelated jobs during the run. Each timing is the best of three
runs, which filters some of the noise, but the multi-thread column in
particular understates what four idle cores deliver, and the encode rate
is lower than on an idle machine. Treat the results as a lower bound and
re-measure on quiet hardware before quoting them.

## Data

Events over 90 days in arrival order: timestamp, `project_id` (100,
skewed), event name (50, skewed), `user_id` (1 million), session id
(36-character text), URL path (10 000, skewed), country (200), browser
(20), duration in ms, revenue (decimal, 5 % non-null), and a small JSON
properties text.

| | |
| --- | --- |
| rows | 20 000 000 |
| segments | 20 of 1 000 000 rows, sorted by `(project_id, ts)`, 65 536 rows per row group |
| raw size | 174.7 bytes/row (3.25 GiB) |
| stored size | 43.6 bytes/row (0.81 GiB), 4.0× smaller |
| encode rate (sort + encode + compress, one thread) | 0.8–0.9 million rows/s |

Most of the stored size is the two high-cardinality text columns
(session id and JSON properties), which only LZ4 can reduce; the
dimension and metric columns take a few bytes per row together.

## Queries

All data is in memory (`Vec<u8>` segments), so the timings are CPU time
for pruning, decompression, decoding, filtering and aggregation; no I/O.

| Query | groups | 1 thread | 4 threads | table rows/s (4 threads) |
| --- | ---: | ---: | ---: | ---: |
| `count(*)` for one week, all projects | 1 | 9.4 ms | 4.6 ms | 4.4 billion |
| events per day, one project, last 30 days | 31 | 4.3 ms | 4.0 ms | 5.0 billion |
| top 10 paths by views, one project, one week | 10 | 5.7 ms | 5.6 ms | 3.6 billion |
| unique users per day (exact), one project, last 30 days | 31 | 9.3 ms | 6.8 ms | 2.9 billion |
| unique users per day (approximate), one project, last 30 days | 31 | 14.8 ms | 5.8 ms | 3.4 billion |
| unique users per day (approximate), all projects, 90 days | 91 | 223.9 ms | 124.4 ms | 161 million |
| p50/p95/p99 duration per event name, whole table | 50 | 177.9 ms | 83.4 ms | 240 million |
| events by country and browser (count, avg), whole table | 4000 | 300.7 ms | 107.6 ms | 186 million |
| revenue per project (sum of decimal), whole table | 100 | 114.9 ms | 39.9 ms | 502 million |
| all events of one user (bloom filter) | 1 | 8.5 ms | 2.8 ms | 7.2 billion |
| all events of one session (bloom filter on text) | 1 | 9.1 ms | 4.1 ms | 4.9 billion |
| `sum(duration_ms)`, whole table | 1 | 30.1 ms | 10.5 ms | 1.9 billion |
| events with `path LIKE '/app/section-1/%'`, whole table | 1 | 94.7 ms | 33.4 ms | 598 million |

"Table rows/s" is the table size divided by the time: it counts rows
that pruning skipped, which is the point — it is the rate at which a
table of that size is answered.

A filtered scan that returns rows (4 columns of one project's week,
48 389 rows) takes 3.3 ms.

## How to read the two groups of queries

**Selective queries** — a project and a time range, a user, a session —
run in single-digit milliseconds on 20 million rows, because the sort
key `(project_id, ts)` lets zone maps skip nearly all row groups and
bloom filters rule out the rest. Their cost grows with the data they
select, not with the table.

**Whole-table aggregations** process every row of the columns involved:
between 65 and 670 million rows per second on one thread, depending on
how much work a row needs (a sum is cheapest; a hash lookup per row for
a two-column group or a sketch update costs more).

## Compared with processing rows

- squad's row store aggregates about 1.2 million rows per second on one
  core (`SELECT count(*), sum(score)` over 100 000 rows takes 84 ms, see
  the repository's benchmark document). The whole-table queries above
  are 55 to 550 times faster per core, and the selective ones do not
  touch most of the table at all.
- A hand-written Rust loop over rows that are already materialized in
  memory, counting events per day for one project with a hash map,
  processes 67 million rows per second on one thread. That is an upper
  bound for any row-at-a-time executor, with no decoding or expression
  evaluation. The columnar whole-table queries are 1 to 10 times faster
  than that loop per thread, and the selective ones 40 to 100 times
  faster for the table as a whole, while reading compressed data.

## Known costs

- **Ingest.** Under a million rows per second per writer thread in this
  run. Sorting, dictionary building, statistics and bloom filters each
  take a share; columns are encoded one after another. Several writers
  can run in parallel on different segments.
- **Group-by on two high-traffic keys** (country × browser) pays one
  hash lookup per row whenever consecutive rows differ.
- **Exact distinct counts** keep every distinct value per group in
  memory; the approximate variant is bounded at 4 KiB per group.
