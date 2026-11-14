# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [semantic versioning](https://semver.org/). Before 1.0, minor
versions may change the API and the on-disk format.

## [Unreleased]

### Added

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

### Added (server hardening)

- Statement cancellation: `Connection::cancel_handle()` / `CancelHandle`
  and `Session::cancel_handle()`; the executor stops with SQLSTATE
  `57014`. `SET statement_timeout` is honoured (also `57014`).
- Notices: `QueryResult::notices` (`squad::Notice`, `squad::Severity`).

### Changed (server hardening)

- `GRANT`, `REVOKE`, `CREATE/ALTER/DROP ROLE`, `ALTER DEFAULT
  PRIVILEGES`, `ENABLE ROW LEVEL SECURITY` and `SET CONSTRAINTS …
  DEFERRED` are still accepted without effect but now send a WARNING;
  `CREATE EXTENSION`, `COMMENT ON` and `ALTER … OWNER TO` send a NOTICE.
  `GRANT`/`REVOKE` on a missing table fail with `42P01`.
- `LOCK TABLE` takes real table locks in the modes `ACCESS SHARE`, `ROW
  SHARE`, `ROW EXCLUSIVE` and `EXCLUSIVE`, requires a transaction block
  (`25P01`) and fails with `0A000` in the other modes, including the
  default `ACCESS EXCLUSIVE`; it used to succeed without effect.
- `QueryResult` has a new public field `notices`.
