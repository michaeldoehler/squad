# Status, limits and roadmap

squad is at version 0.1. It is a working database with a large test
suite behind it, and it is young: it has not yet run in production
anywhere. Treat it accordingly — it is a good fit today for tests, tools,
prototypes and applications that can tolerate an early-stage dependency,
and the place to try it is next to PostgreSQL, not instead of your only
copy of important data.

## Where things stand

| Area | State |
| --- | --- |
| Storage core | B+tree, buffer pool, WAL with group commit, crash-atomic checkpoints, recovery, MVCC, deterministic crash simulation: done. Damage in the middle of the log is detected and refused instead of silently truncating it. Commits become visible only once they are durable. |
| SQL | parser, binder, optimizer, executor, Rust API, catalogs and SQLSTATEs: done; an sqlx application runs unchanged over the wire protocol. A native in-process sqlx driver is open. |
| Server | wire protocol, `COPY`, SCRAM, TLS, cancel requests, `statement_timeout`, connection limits and timeouts, container image: done. Point-in-time recovery is open. |
| Analytics | columnar tables with background compaction, range reads of column chunks with a byte-bounded cache, manifest pruning, `ALTER TABLE ADD/DROP/RENAME COLUMN`, `jsonb` pushdown, an append API for ingestion: done. `UPDATE` and spilling are open. |
| Remote storage | durable copy in object stores including S3 and Garage, fencing that needs no conditional writes, read replicas with manual promotion: done. Automatic failover is open. |
| Sharding | static sharding over several databases through the Rust API: done. |

Details are in [analytics](analytics.md), [cloud storage](cloud-storage.md),
[replication](replication.md), [sharding](sharding.md) and
[deployment](deployment.md).

## Known limits

Correctness and compatibility limits are listed in
[compatibility](compatibility.md). The engineering limits that matter
when sizing a workload:

- **Scans of ordinary tables are slow.** The row store is built for
  point lookups, short range scans and small transactions; a full scan
  decodes every row and processes well under a million rows per second
  per core. Data that is aggregated in bulk belongs in a columnar table,
  where the same queries are hundreds to thousands of times faster — as
  long as they have the shape the column engine handles (see
  [analytics](analytics.md)). Joins, window functions and expressions
  inside aggregates over columnar tables still run in the row engine.
- **Columnar tables** have no `UPDATE`, no column type changes and no
  back-pressure when inserts outpace compaction for a long time; large
  group-bys and exact `count(DISTINCT)` are bounded by memory. A query
  that has not yet read a segment that a concurrent compaction replaced
  fails and must be retried.
- **The planner has no statistics.** Access paths are chosen by rules and
  joins run in the order they are written. Queries joining many large
  tables may need to be written with the selective table first.
- **One writer applies commits at a time.** Transactions run and wait for
  row locks concurrently, and their `fsync`s are batched, but the final
  step of a commit is serialized behind one lock. Write throughput scales
  with group commit and batching rather than with cores. Serializable
  isolation is optimistic validation at commit, not SSI, and can abort
  more transactions than PostgreSQL.
- **Remote durability costs a round trip.** In `RemoteSync::Sync` mode
  every commit (or group of concurrent commits) waits for one upload and
  one listing in the object store; ingest in batches (see
  [deployment](deployment.md#remote-sync-modes-and-their-latency)).
- **Read scaling across cores is limited** by shared reference counts
  and reader locks on the tree. Single-thread point lookups take about
  a microsecond; the gain from additional threads is modest (see
  [benchmarks](benchmarks.md)).
- **Memory.** Modified pages stay in memory until the next checkpoint,
  and a transaction's own writes are buffered in memory until commit.
  Very large single transactions (bulk loads of many gigabytes) should
  be split.
- **`ALTER TABLE` that changes row layout rewrites an ordinary table**
  inside one transaction, as does adding a column with a default.
  Columnar tables add, drop and rename columns without a rewrite.
- **Secondary index lookups fetch the row** even when the index holds
  every requested column (no index-only scans).
- **One writer per database.** Replicas are read-only and asynchronous;
  promotion is a manual step (or your orchestrator's). See
  [replication](replication.md). Fencing needs an object store with
  read-after-write consistent `PUT` and `LIST` (Garage in its default
  consistency mode, AWS S3, MinIO).
- **Advisory locks** coordinate the sessions of one process; deadlocks
  between them are not detected.
- **Sharding is static.** The shard count is fixed when a cluster is
  created, there are no transactions across shards, and a cluster is
  reached through the Rust API only. See [sharding](sharding.md).
- **The data file does not shrink**; freed pages are reused.

## Roadmap

Roughly in order of expected value. Nothing here is scheduled.

**Performance**

1. Optimistic latching in the B+tree and pointer swizzling in the buffer
   pool, so that readers on different cores share no writes.
2. Index-only scans and decoding only the columns a query needs.
3. Table statistics (`ANALYZE`) and cost-based join ordering.
4. Parallel commit application for transactions that touch disjoint keys.
5. `io_uring` on Linux for log and checkpoint I/O.

**Compatibility**

6. A native sqlx driver (`squad://path`) and Diesel/SeaORM backends, to
   remove the protocol hop for in-process use.
7. Expression and partial indexes, generated columns, `GROUPING SETS`,
   ordered-set aggregates, correlated subqueries above `GROUP BY`.
8. `EXPLAIN (FORMAT JSON)`, which also unlocks sqlx's checked macros.
9. Arbitrary-precision `numeric`, multidimensional arrays, range types,
   domains.
10. Triggers and SQL-language functions, then PL/pgSQL.

**Operations**

11. Point-in-time recovery from archived log segments.
12. Automatic failover (leader election through the object store),
    synchronous replicas, and logical decoding in `pgoutput` format for
    change-data-capture tools.
13. Shard rebalancing, a wire-protocol front end for clusters, and
    two-phase commit across shards.
14. `LISTEN`/`NOTIFY`.
15. A C ABI for bindings in other languages.

**Analytics**

Columnar tables cover the core of an event-analytics and error-tracking
workload today. What that workload still needs:

- more queries on the fast path: expressions inside aggregates,
  `FILTER`, joins of a columnar table with small ordinary tables,
  time zones other than UTC for time buckets;
- spilling large aggregations and exact `count(DISTINCT)` to disk;
- back-pressure on ingestion when compaction falls behind;
- `UPDATE` and column type changes on columnar tables;
- a local-disk cache tier for segments kept in a bucket;
- funnel, retention and sessionization functions;
- materialized rollups maintained incrementally.
