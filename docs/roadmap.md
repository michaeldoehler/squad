# Status, limits and roadmap

squad is at version 0.1. It is a working database with a large test
suite behind it, and it is young: it has not yet run in production
anywhere. Treat it accordingly — it is a good fit today for tests, tools,
prototypes and applications that can tolerate an early-stage dependency,
and the place to try it is next to PostgreSQL, not instead of your only
copy of important data.

## Where things stand

The original specification planned four phases, each with a gate.

| Phase | Content | State |
| --- | --- | --- |
| 0 Storage core | B+tree, buffer pool, WAL, recovery, MVCC, key-value API, deterministic simulation | done; simulation runs on every commit and at scale on demand |
| 1 SQL core (L1, L2) | parser, binder, optimizer, executor, Rust API | done |
| 2 Drop-in (L3, L4) | catalogs, SQLSTATEs, sqlx | done through the wire protocol: an sqlx application runs unchanged. A native in-process sqlx driver is open. |
| 3 Server and operations (L5) | wire protocol, backup, `COPY` | done, except TLS, point-in-time recovery and query cancellation |

## Known limits

Correctness and compatibility limits are listed in
[compatibility](compatibility.md). The engineering limits that matter
when sizing a workload:

- **Analytical queries are slow.** squad is built for OLTP: point
  lookups, short range scans, small transactions. A full scan decodes
  every row and currently processes on the order of one million rows per
  second per core. There is no columnar storage, no vectorized execution
  and no parallel query. Large `GROUP BY` reports over tens of millions
  of rows are the wrong workload today.
- **The planner has no statistics.** Access paths are chosen by rules and
  joins run in the order they are written. Queries joining many large
  tables may need to be written with the selective table first.
- **One writer applies commits at a time.** Transactions run and wait for
  row locks concurrently, and their `fsync`s are batched, but the final
  step of a commit is serialized. Write throughput scales with group
  commit rather than with cores.
- **Read scaling across cores is limited** by shared reference counts
  and reader locks on the tree. Single-thread point lookups take about
  a microsecond; the gain from additional threads is modest (see
  [benchmarks](benchmarks.md)).
- **Memory.** Modified pages stay in memory until the next checkpoint,
  and a transaction's own writes are buffered in memory until commit.
  Very large single transactions (bulk loads of many gigabytes) should
  be split.
- **`ALTER TABLE` that changes row layout rewrites the table** inside
  one transaction, as does adding a column with a default.
- **Secondary index lookups fetch the row** even when the index holds
  every requested column (no index-only scans).
- **Single node.** No replication, no failover.
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
10. TLS and query cancellation in the server.
11. Triggers and SQL-language functions, then PL/pgSQL.

**Operations**

12. Point-in-time recovery from archived log segments.
13. Log shipping to read replicas; logical decoding in `pgoutput` format
    for change-data-capture tools.
14. `LISTEN`/`NOTIFY`.
15. A C ABI for bindings in other languages.

**Analytics**

squad's first planned large application is event analytics — product and
web analytics, error tracking — which stresses exactly what the engine
is weakest at today: wide scans and aggregations over append-mostly
tables. That workload needs work beyond the list above:

- a column-oriented, compressed storage format for append-mostly tables,
  alongside the row store;
- vectorized execution for scans, filters and aggregations, and parallel
  query;
- partitioning by time, with cheap dropping of old partitions;
- approximate aggregates (distinct counts, quantiles) and materialized
  rollups that are maintained incrementally;
- beyond one machine: sharding by tenant, and replicas for read
  scale-out.

Until then, an analytics product on squad should keep raw events
compact, query through pre-aggregated tables maintained at ingest, and
keep any single query's scan to what an index range can deliver.
