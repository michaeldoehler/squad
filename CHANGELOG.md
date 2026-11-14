# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [semantic versioning](https://semver.org/). Before 1.0, minor
versions may change the API and the on-disk format.

## [Unreleased]

The first version, not yet released. The sections below list what it
contains; entries after the first list record changes made while
preparing it for production use.

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
- `RemoteConfig::upload_concurrency`: optional second (or further) log
  upload while one is in flight.
- Tests against a real S3 service when `SQUAD_TEST_S3_URL` is set
  (conformance, restore, replica, concurrent promotions), run against
  Garage 2.5; AWS SigV4 test-suite vectors for the signer; the S3
  emulator and the memory store can ignore `If-None-Match`.
- `examples/remote_sync_bench.rs` in `squad-storage`: commits/s and
  p50/p99 latency against a real store for 1, 8 and 32 writers.
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
- Statement cancellation: `Connection::cancel_handle()` / `CancelHandle`
  and `Session::cancel_handle()`; the executor stops with SQLSTATE
  `57014`. `SET statement_timeout` is honoured (also `57014`).
- Notices: `QueryResult::notices` (`squad::Notice`, `squad::Severity`);
  the server sends them as NoticeResponse, `squad shell` prints them.
- Server TLS (rustls, ring provider): `--tls-cert`, `--tls-key`,
  `--require-tls`; `ServerConfig::tls`, `TlsConfig`, `require_tls`.
- Server cancel requests: real BackendKeyData with a random secret per
  connection.
- Server limits: `--max-connections` (`53300` beyond it),
  `--authentication-timeout` (default 60 s), `--idle-timeout` (`57P05`
  / `25P03`), at most 10,000 bytes per message before login and
  `--max-message-size` after it.
- Server logins from `SQUAD_PASSWORD` / `SQUAD_PASSWORD_FILE` /
  `SQUAD_USER` and `--users-file` (passwords or PostgreSQL SCRAM
  verifiers); `Auth::Verifiers`; `squad hash-password`.
- `squad ping`, a health check for scripts and containers.
- `--remote-file`, `--replica-of-file` and `AWS_*_FILE` variables, so
  that object store credentials can come from secret files.
- A multi-stage `Dockerfile` (Debian slim, non-root user, `/data`
  volume, health check with `squad ping`) and `.dockerignore`.
- `squad serve` shuts down cleanly on SIGTERM and SIGINT.
- `Database::remote_status()` and the fields `fenced` and `local_lsn`
  of `RemoteStats`, for health checks of services that embed squad.
- `docs/deployment.md`: running on Docker Swarm with Garage.

### Changed

- **Writer fencing no longer relies on conditional writes.** Garage and
  other S3-compatible stores accept `If-None-Match: *` and overwrite
  anyway, so two writers could hold the same epoch. Epochs are now
  claimed with uniquely named claim objects and a listing afterwards; a
  writer acknowledges a commit only after a listing that follows its
  upload shows no newer claim. Safety needs only read-after-write
  consistent `PUT` and `LIST`. `RemoteStats::conditional_writes` reports
  whether the store enforces conditional writes (informational).
- **Remote format 2.** Claim, base, log and snapshot objects carry the
  claim id in their keys, and each base names the writer it continues.
  Object stores written in format 1 by earlier development versions
  are refused with an error; they are not read or migrated.
- **Commits become visible only once they are durable** (local `fsync`,
  and the upload in remote `Sync` mode). Group commit is kept: the wait
  happens outside the commit lock.
- **Log format 2** (segment magic `SQWAL002`): each record carries the
  highest durable log position at the time it was written. Directories
  whose log still holds records of the earlier format are refused with an
  error; opening them once with the earlier version checkpoints the log,
  and the empty segment it leaves is replaced on the next open.
- The S3 client classifies errors (transient: connection errors,
  timeouts, 5xx, 429, 408, `RequestTimeout`; other 4xx are not
  retried), retries with exponential backoff and full jitter, has
  configurable timeouts, uploads objects above 16 MiB in parts, and
  signs header values with runs of spaces collapsed, as SigV4 requires.
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
- The server no longer trusts every client on addresses other than
  loopback: `squad serve` refuses to start and `Server::bind` /
  `Serve::serve` fail with `PermissionDenied` unless passwords are
  configured or trust is requested (`--trust`,
  `ServerConfig::trust_remote`).
- `--password` on the command line prints a warning; `--user` without
  `--password` takes the password from `SQUAD_PASSWORD`.
- `ServerConfig` no longer derives `Default` but implements it with the
  new limits; its `Debug` output and that of `Auth` omit passwords.
- Errors opening an object store no longer echo the URL, which may carry
  credentials.
- The example `analytics_bench` of `squad-columnar` is now
  `engine_bench`, so that example names are unique in the workspace.

### Fixed

- Damage in the middle of the write-ahead log (a record that later
  records show was already durable) made recovery drop every later
  commit silently; the open now fails with a corruption error and
  leaves the log untouched. A torn tail is still discarded.
- A write batch whose log record exceeds 4 GiB was written with a
  truncated length; it is now refused with an error.
- A replaced writer in the middle of a cleanup could delete chunks of
  the new writer's first snapshot; cleanup now lists before it checks
  for a newer claim.
- A commit waiting for its upload in `Sync` mode could be acknowledged
  because a snapshot covered it, without a fence check after the
  snapshot was stored.
- Log records were uploaded before they were durable locally, so a
  power loss could leave the local directory behind the store.
- `LocalStore` did not sync directories it created for an object, so a
  durable object could vanish with its directory entry after a power
  loss.
- `SELECT … FOR UPDATE/SHARE` on a columnar table silently skipped rows
  held in segments; it is now rejected with `0A000`.
- Advisory locks (`pg_advisory_lock` and relatives) reported success
  and locked nothing; they now lock as in PostgreSQL (exclusive and
  shared, session- and transaction-level, counted). Deadlocks between
  them are not detected.
