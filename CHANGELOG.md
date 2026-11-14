# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [semantic versioning](https://semver.org/). Before 1.0, minor
versions may change the API and the on-disk format.

## [Unreleased]

### Added

- **Columnar tables in production use**: rows are inserted into the
  delta as blocks (one key-value entry per insert statement, in
  columnar form) and compacted by a background thread per database
  (thresholds by rows, blocks and age in `ColumnarConfig`); merges of
  small segments stream with bounded memory; dropping the last handle
  stops the compactor cleanly. Segments are read in ranges (footer,
  then the needed column chunks) through a byte-bounded LRU cache with
  single-flight misses, and pruned by manifest statistics before any
  I/O. `ALTER TABLE ADD/DROP/RENAME COLUMN` on columnar tables without
  rewriting segments. `jsonb` columns get field bloom filters; `->>`
  comparisons, `IN`, `IS [NOT] NULL` and `GROUP BY col ->> 'key'` run in
  the column engine and skip row groups. `VACUUM` removes unreferenced
  segment objects after a grace period; rolled-back transactions delete
  the segments they wrote.
- `Connection::append` / `Session::append`: insert rows given as values
  without SQL text.
- `Config::columnar` (`ColumnarConfig`), `Database::open_in_memory_with`,
  `Database::with_engine_config`, `Database::with_engine_and_objects`,
  `Database::wait_for_compaction`, `Database::columnar_cache_stats`,
  `ReplicaConfig::columnar`.
- `squad-columnar`: `ColumnType::Json`, `Predicate::JsonIn` /
  `JsonIsNull`, `GroupKey::JsonField`, `Segment::project`,
  `SegmentStreamWriter`, `merge`, `encode_batch` / `decode_batch`,
  `ChunkSource::prefetch`, column ids in `SegmentMeta`.
- Benchmarks: `squad-bench --bin ingest` (ingestion through SQL,
  prepared statements, `append` and `COPY`), cold-cache runs of
  `analytics_bench` against an S3 bucket (`--s3`).

### Changed

- Columnar on-disk formats: manifests are written as `SQMANI02` (with
  column ids), `jsonb` columns as the new `Json` column type, and new
  delta rows as blocks. Older manifests, segments and one-row-per-key
  delta rows are still read; databases written by this version cannot
  be read by earlier ones.
- Committing no longer compacts a columnar table synchronously in the
  committing session.
- `squad_columnar::TableSource::delta` holds `Arc<Batch>`.
- `DELETE` on a columnar table reads the delta after taking the
  table's segment lock; in Repeatable Read and Serializable it fails
  with `40001` if the table changed since the snapshot.

### Fixed

- `SELECT … FOR UPDATE/SHARE` on a columnar table silently skipped rows
  held in segments; it is now rejected with `0A000`.

- **Storage engine** (`squad-storage`): page-based B+tree with a buffer
  pool, write-ahead log with group commit and preallocated segments,
  crash-atomic checkpoints through a double-write journal, recovery on
  open, online backup, integrity verification, and a simulated file
  system for deterministic fault injection.
- **Transactions** (`squad-kv`): multi-version concurrency control with
  Read Committed, Repeatable Read and Serializable isolation, row locks
  with deadlock detection, savepoints, and garbage collection of old
  versions.
- **SQL engine** (`squad`): the PostgreSQL parser, a typed binder, a
  rule-based optimizer and a pull-based executor. Tables, indexes,
  sequences, views, enums and schemas with transactional DDL; joins,
  subqueries, recursive CTEs, aggregates, window functions, set
  operations; `INSERT … ON CONFLICT`, `RETURNING`; primary key, unique,
  check, not-null and foreign key constraints; PostgreSQL's SQLSTATE
  codes; `pg_catalog` and `information_schema`.
- **Embedding API**: `Database`, `Connection`, `Statement`, `Transaction`,
  `Row`, with conversions for the common Rust types.
- **Server** (`squad-server`): PostgreSQL wire protocol v3 with simple
  and extended query, binary formats, `COPY`, SCRAM-SHA-256, TCP and
  Unix sockets; the `squad` command with `serve`, `shell`, `backup` and
  `verify`.
- **Test infrastructure**: SQL regression suite with expected output
  from PostgreSQL 16, sqllogictest runner, differential fuzzer against
  PostgreSQL, concurrency tests, crash simulations at the storage and
  SQL level, and integration tests with psql, pgbench, pg_dump output,
  tokio-postgres and sqlx.
- **Benchmarks** against PostgreSQL and SQLite (`squad-bench`).
- **Columnar tables** (`CREATE TABLE … USING columnar`): rows are
  inserted transactionally into a delta and compacted into immutable,
  compressed column segments; aggregations run vectorized and in
  parallel in `squad-columnar`; `approx_count_distinct` and
  `approx_percentile`.
- **Object storage** (`squad-object`): memory, local directory and
  S3-compatible stores. The row store can keep a durable, restorable
  copy in an object store, with synchronous or asynchronous shipping
  and writer fencing.
- **Read replicas**: `Database::open_replica` follows a writer through
  the object store and serves read-only sessions (writes fail with
  SQLSTATE `25006`); `replication_status`, `squad_replication_status()`
  and `pg_is_in_recovery()` report position and lag; `promote` /
  `squad_promote()` turn a replica into the writer and fence the old
  one; `squad serve --replica-of`.
- **Sharding** (`squad-cluster`): a fixed set of local, in-memory or
  remote databases as one logical database, with distributed and
  reference tables, single-shard routing by distribution key,
  scatter-gather queries with merged aggregates, and transactions bound
  to one shard.
