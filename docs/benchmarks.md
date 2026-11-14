# Benchmarks

Every number on this page was measured, the raw output is in
[`benchmarks/`](../benchmarks), and the commands are given so the runs can
be repeated. Where a result misses a goal, it says so.

**Machine.** A 4-vCPU virtual machine with 15 GiB of RAM and an ext4
disk, Linux 6.18. It was shared with build jobs during most runs (the
load average is noted with each), so treat absolute numbers as
conservative and ratios between systems on the same run as the
meaningful part. On a virtual disk, `fsync` cost dominates every durable
commit, for all systems alike.

## OLTP: squad, SQLite and PostgreSQL

`squad-bench` runs identical workloads against squad (embedded), SQLite
(embedded, WAL mode) and PostgreSQL 16 (client/server over a Unix
socket), all configured to force every commit to disk before
acknowledging it. One table of 100,000 rows with a primary key and one
secondary index; prepared statements everywhere.

```sh
cargo run --release -p squad-bench -- --rows 100000 --ops 20000 --threads 4 \
    --postgres "host=/path/to/socket/dir port=5432 user=postgres dbname=bench"
```

Run of 2026-11-10, load average 2.7 at start
([raw](../benchmarks/oltp-2026-11-10.txt)):

| Workload | | squad | SQLite | PostgreSQL |
| --- | --- | ---: | ---: | ---: |
| Point select by primary key | p50 | **1.5 µs** | 1.5 µs | 50 µs |
| | p99 | 2.7 µs | 3.1 µs | 97 µs |
| Index lookup, 10 rows | p50 | 12 µs | **1.9 µs** | 66 µs |
| Update one row, durable commit | p50 | **125 µs** | 123 µs | 200 µs |
| | p99 | 287 µs | 412 µs | 620 µs |
| Insert one row, durable commit | p50 | 124 µs | 123 µs | 159 µs |
| Two-row transaction, durable commit | p50 | 153 µs | 146 µs | 423 µs |
| Point select, 4 threads | ops/s | 301,000 | 341,000 | 290,000 |
| Two-row transaction, 4 threads | ops/s | **9,490** | 5,050 | 6,960 |

An earlier run on a quieter machine measured the point select at
1.1–1.3 µs for squad and 10 µs for PostgreSQL.

How to read this:

- **Point reads** cost about a microsecond in-process — the same as
  SQLite, and one to two orders of magnitude less than a round trip to a
  PostgreSQL server on the same machine. This is the reason to embed a
  database. The design target (p50 below 2 µs, p99 below 10 µs) is
  met.
- **Durable single-row writes** are bounded by `fsync`; all three systems
  are within a factor of two, with squad at or ahead of PostgreSQL. The
  goal (insert throughput at least PostgreSQL's at equal durability) is
  met.
- **Concurrent writers** are where squad differs from SQLite: several
  connections write at once and share an `fsync`, so four threads commit
  1.9× as many transactions per second as SQLite's single writer, and
  1.4× as many as PostgreSQL.
- **Index lookups returning several rows** are squad's weak spot: each
  index entry leads to a separate row fetch, and whole rows are decoded.
  SQLite answers this query from the index alone. Index-only scans are
  on the [roadmap](roadmap.md).
- **Read scaling across threads is poor.** Four reader threads deliver
  about half of one thread's throughput each. Readers do not block each
  other, but they still write to shared memory (reference counts of the
  B+tree's upper pages, reader counts of locks), and on this machine
  they also competed with other jobs. The B+tree needs optimistic
  latching to scale; that is the first item on the roadmap.

## pgbench

`pgbench` drives squad through the wire-protocol server, so this
measures the server path, not the embedded one. Scale 10 (one million
accounts), 16 clients on 4 threads, prepared statements, 20 seconds,
synchronous commit on both sides
([raw](../benchmarks/pgbench-2026-11-10.txt)):

| Script | squad | PostgreSQL 16 | Ratio |
| --- | ---: | ---: | ---: |
| TPC-B-like (5 statements per transaction) | 7,641 tps | 6,192 tps | 1.23× |
| Select-only | 176,880 tps | 197,012 tps | 0.90× |

```sh
squad serve ./data --sync sync &
pgbench -h 127.0.0.1 -U squad -i -s 10 squad
pgbench -h 127.0.0.1 -U squad -c 16 -j 4 -T 20 -M prepared squad
pgbench -h 127.0.0.1 -U squad -c 16 -j 4 -T 20 -M prepared -S squad
```

The design target was three times PostgreSQL's throughput on this
benchmark. **That target is not met.** Over the wire protocol squad
does the same work as PostgreSQL — parse messages, run a statement, wait
for the log — plus a thread per connection, and its advantage of having
no network hop does not exist here. The result says that squad's server
mode is competitive with PostgreSQL, not that it is faster. The
advantage of squad is in the embedded numbers above.

## Cold start

Opening an existing database that was closed cleanly takes about 0.1 ms
in the storage engine (measured with `storage_bench`); the goal was
below 50 ms. After a crash, opening additionally replays the log written
since the last checkpoint.

## Analytics

`analytics_bench` loads the same two million synthetic events (90 days,
100 projects, 8 event names, one million users, 10,000 paths) into an
ordinary table and into a columnar table, and runs product-analytics
queries on both through SQL. In-memory database, best of three runs,
load average about 1
([raw](../benchmarks/analytics-2m-2026-11-10.txt)):

```sh
cargo run --release -p squad --example analytics_bench -- 2000000
```

| Query | Row table | Columnar table | Speed-up |
| --- | ---: | ---: | ---: |
| `count(*)` of one week | 3,762 ms | 2.6 ms | 1,400× |
| Events per day, one project, 30 days | 3,467 ms | 0.7 ms | 5,100× |
| Top 10 paths, one project, one week | 3,394 ms | 0.8 ms | 4,300× |
| Unique users per day, one project, exact | 3,466 ms | 1.3 ms | 2,800× |
| Unique users per day, one project, approximate | 3,464 ms | 1.2 ms | 3,000× |
| p50/p95/p99 duration per event, all data | 4,538 ms | 5.8 ms | 790× |
| Count and average by country, all data | 3,690 ms | 7.1 ms | 520× |
| Revenue per project, all data | 3,851 ms | 3.6 ms | 1,060× |
| One user's events | 3,523 ms | 1.3 ms | 2,600× |
| Sum over all data | 3,654 ms | 1.5 ms | 2,400× |
| Errors per hour, one project, last day | 4,285 ms | 0.7 ms | 6,500× |

Loading: 145,000 rows/s into the row table, 208,000 rows/s into the
columnar table (bulk inserts written directly as segments).

The speed-ups are large because the baseline is squad's own row store,
which scans about 0.6 million rows per second and is not built for this.
The meaningful statement is the absolute one: typical dashboard queries
over two million events answer in single-digit milliseconds.

The column engine on its own, on 20 million events in 20 segments
(`crates/squad-columnar/BENCHMARKS.md`; measured under very heavy
machine load, best of three):

| Query | 1 thread | 4 threads |
| --- | ---: | ---: |
| `count(*)`, one week, all projects | 9.4 ms | 4.6 ms |
| Events per day, one project, 30 days | 4.3 ms | 4.0 ms |
| Unique users per day, approximate, all projects, 90 days | 224 ms | 124 ms |
| p50/p95/p99 duration per event name, whole table | 178 ms | 83 ms |
| Count and average by country × browser, whole table | 301 ms | 108 ms |
| `sum(duration_ms)`, whole table | 30 ms | 10.5 ms |
| One user's events (bloom filter) | 8.5 ms | 2.8 ms |

Storage: 43.6 bytes per event against 174.7 bytes raw (4.0× smaller).
Whole-table aggregations run at 65–670 million rows per second per
thread.

These are not comparisons with ClickHouse or DuckDB; none has been run.

### Segments on S3 (cold cache)

The same two million events with the segments in a local Garage v2.5
(S3 API on 127.0.0.1, row store on local disk), every query run once
right after reopening the database, with an empty segment cache, and
then warm ([raw](../benchmarks/analytics-2m-2026-11-14.txt)):

```sh
eval "$(scripts/garage-dev.sh start)"
cargo run --release -p squad --features s3 --example analytics_bench -- 2000000 --s3 "$SQUAD_TEST_S3_URL"
```

| Query | Cold | Warm | GETs | Read |
| --- | ---: | ---: | ---: | ---: |
| `count(*)` of one week | 223 ms | 5.9 ms | 32 | 10.3 MiB |
| Events per day, one project, 30 days | 112 ms | 3.3 ms | 7 | 0.7 MiB |
| Top 10 paths, one project, one week | 133 ms | 3.1 ms | 9 | 0.7 MiB |
| p50/p95/p99 duration per event, all data | 364 ms | 15.8 ms | 63 | 0.2 MiB |
| One user's events | 241 ms | 1.5 ms | 35 | 2.1 MiB |
| Errors per hour, one project, last day | 106 ms | 1.9 ms | 4 | 0.4 MiB |

A cold query fetches the segment footer and then only the column chunks
of the row groups that its filter does not rule out. Most of the cold
time is request latency (about 20–50 ms
per request on this busy machine with the current S3 client); requests
for different ranges run in parallel. Loading through bulk inserts ran
at 208,000 rows/s; the final `VACUUM` (merging eight segments of
250,000 rows read back from Garage) took 2.6 s.

The in-memory run of the same benchmark after the changes of this
release gives the same query times as before within the noise of the
shared machine (1.4–20 ms per columnar query).

## Ingestion

`squad-bench --bin ingest` inserts events of a product-analytics
service (`ts`, `project_id`, `event`, `distinct_id`, a `jsonb`
`properties` of about 200 bytes) into a columnar table from several
threads, in batches of 1,000, with durable commits (`SyncMode::Sync`),
while a reader runs a small aggregation every 200 ms
([raw](../benchmarks/ingest-2026-11-14.txt)). The "before" and "after"
columns were measured while other build jobs ran (load average 7–9);
the last column is a rerun of the merged code on the otherwise idle
4-vCPU machine, one million events each:

```sh
cargo run --release -p squad-bench --bin ingest -- --mode append --threads 8 --events 1000000
```

| Way in | Before (busy) | After (busy) | After (idle) | Batch p50 / p99, idle |
| --- | ---: | ---: | ---: | ---: |
| `Connection::append`, 8 threads | — | 211,000 events/s | 496,000 events/s | 15 / 33 ms |
| `Connection::append`, 4 threads | — | 192,000 events/s | | |
| Prepared `INSERT … VALUES` of 1,000 rows, 4 threads | 46,000 events/s | 187,000 events/s | 480,000 events/s | 8 / 17 ms |
| `COPY FROM STDIN` through the server, 4 clients | 46,000 events/s | 115,000 events/s | 261,000 events/s | 14 / 30 ms |
| SQL text `INSERT … VALUES` of 1,000 rows, 4 threads | 28,000 events/s | 72,000 events/s | 153,000 events/s | 24 / 55 ms |

These commits are durable on the local disk. With the log shipped to an
object store in `Sync` mode, each batch additionally waits for one
upload round trip (see below); that was not measured with this
benchmark.

Before, every row was a key in the row store and the session whose
commit pushed a table's delta over 100,000 rows compacted it before
returning: batches stalled for up to 4.7 s and the reader's query took
0.5–1.7 s while the delta was large. Now each batch is one delta block,
compaction runs in the background, the longest batch took 0.13 s and
the reader's query 16–220 ms (p99) during ingestion, a few ms once it
settled. SQL text is limited by parsing the statement; `COPY` through
the server inserts in statements of 64 rows.

## Remote storage

Commit throughput and latency with the write-ahead log shipped to a
single-node Garage 2.5 on the same machine, one 120-byte row per commit
and a local `fsync` on every commit, rerun on the otherwise idle
machine (an earlier run while other builds ran is in the same file;
latency to a local single-node Garage varies by a factor of about two
between runs). Raw output:
`benchmarks/remote-sync-2026-11-14.txt` (`eval "$(scripts/garage-dev.sh
start)"; cargo run --release -p squad-storage --example
remote_sync_bench`).

| Mode | Writers | Commits/s | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Local only | 1 | 8,337 | 0.11 ms | 0.29 ms |
| Remote, `Async` (100 ms) | 1 | 7,394 | 0.12 ms | 0.34 ms |
| Remote, `Sync` | 1 | 122 | 7.73 ms | 19.05 ms |
| Local only | 8 | 24,678 | 0.31 ms | 1.05 ms |
| Remote, `Async` (100 ms) | 8 | 23,812 | 0.31 ms | 1.17 ms |
| Remote, `Sync` | 8 | 930 | 8.29 ms | 18.11 ms |
| Local only | 32 | 65,353 | 0.44 ms | 1.46 ms |
| Remote, `Async` (100 ms) | 32 | 60,612 | 0.46 ms | 1.85 ms |
| Remote, `Sync` | 32 | 1,899 | 13.69 ms | 38.42 ms |

A `Sync` commit waits for one `PUT` and one `LIST`; concurrent commits
share them (22 commits per upload with 32 writers). The full table,
with 8 writers, is in [cloud storage](cloud-storage.md#request-and-storage-cost).
`remote_bench` (same crate) still measures snapshot upload volume.

## Replication

Writer, replica and object store (a local directory) on the same
machine, synchronous shipping; other builds were running during the
measurement. Raw output: `benchmarks/replication-2026-11-10.txt`
(`cargo run --release -p squad --example replication_bench`).

| | |
| --- | ---: |
| Replica applies a backlog | 25,400 commits/s, 279,000 rows/s |
| Replica opens from a snapshot of 220,000 rows | 0.27 s |
| Commit visible on replica, 1 ms poll, p50 / p99 | 13 ms / 37 ms |
| Commit visible on replica, 10 ms poll, p50 / p99 | 22 ms / 37 ms |
| Commit visible on replica, 100 ms poll, p50 / p99 | 114 ms / 133 ms |
| Promotion until the new writer's first commit | 0.10 s |

## Sharding

2 million columnar events distributed by project over 1, 2 and 4 shards
**on the same 4-vCPU machine and disk** — this measures the overhead of
routing and the gain from parallel scans, not scale-out across machines.
Raw output: `benchmarks/sharding-2026-11-10.txt` (`cargo run --release
-p squad-cluster --example cluster_bench`).

| | 1 shard | 2 shards | 4 shards |
| --- | ---: | ---: | ---: |
| Bulk load, rows/s | 304,000 | 391,000 | 422,000 |
| Single-row durable inserts, 8 connections, rows/s | 28,700 | 23,900 | 18,600 |
| One project: events per day | 0.70 ms | 0.69 ms | 0.72 ms |
| All shards: `count(*)` | 0.14 ms | 0.83 ms | 1.45 ms |
| All shards: count and average by event | 6.7 ms | 7.7 ms | 8.6 ms |
| All shards: 20 slowest events (full scan and sort) | 3,111 ms | 1,637 ms | 891 ms |

Single-tenant queries cost the same on any number of shards. Durable
single-row inserts get slower with more shards because the shards share
one disk and lose group commit; on separate machines each shard has its
own log.

## Against the design targets

| Target | Result | Met |
| --- | --- | --- |
| Point select p50 < 2 µs, p99 < 10 µs | 1.5 µs / 2.7 µs under load; 1.1 µs / 1.5 µs quiet | yes |
| Insert with commit: throughput ≥ PostgreSQL at equal durability | 7,580/s vs 5,520/s | yes |
| pgbench, 16 clients: ≥ 3× PostgreSQL | 1.23× | **no** |
| Cold start to first query < 50 ms | ~0.1 ms after clean shutdown | yes |
| Regression gate in CI on fixed hardware | not set up; benchmarks are run by hand | **no** |

## Running your own

Results depend heavily on the storage device. To compare on your
hardware, run `squad-bench` with your PostgreSQL's connection string and
`analytics_bench` with a row count that matches your data. Run each
twice on an otherwise idle machine and compare ratios, not single
numbers.
