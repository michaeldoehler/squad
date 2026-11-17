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
- **Row-level security** with PostgreSQL 16's semantics: `ALTER TABLE …
  ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY`, `CREATE
  POLICY` (permissive and restrictive, per command, `TO` login names,
  `USING`, `WITH CHECK`), `ALTER POLICY`, `DROP POLICY`, on ordinary and
  columnar tables; enforcement in queries, joins, subqueries, views,
  prepared statements, `COPY … TO`, `INSERT`, `UPDATE` and `DELETE`
  (`42501` for rows that fail `WITH CHECK`); `row_security`;
  `pg_class.relrowsecurity`/`relforcerowsecurity`, `pg_policy`,
  `pg_policies` and policies in psql's `\d`. Policy conditions with
  `current_setting` use indexes and the column engine. See
  `docs/security.md`.
- **Logins and their attributes**: `squad::Login`,
  `Database::connect_as`, `Session::set_login`; `bypass_rls` (default
  on: treated as the owner of every table) and `read_only` (every
  transaction read-only, `25006`). On the server:
  `squad_server::LoginAttributes`, `ServerConfig::logins`, attributes in
  the users file (`user read_only no_bypass_rls:secret`), `--read-only`
  and `--no-bypass-rls`. `is_superuser` reports whether the login
  bypasses row-level security.
- `SET LOCAL`, `set_config(…, true)`, custom settings that stay known
  (empty) after `RESET` as in PostgreSQL, `default_transaction_read_only`
  and `SET transaction_read_only`.
- `Session::is_read_only` and `Session::row_security_applies`.
- `int2vector` and `oidvector`: `pg_index.indkey`, `indoption`,
  `indclass` and `indcollation` have these types, with PostgreSQL's
  space-separated text form (`'1 2 4'`, also for `indkey::varchar`) and
  subscripts from 0, so that the index introspection of SQLAlchemy 1.4
  and 2.x (Apache Superset) works. `indclass` holds the default B-tree
  operator class of each column, `indcollation` the column's collation,
  and `pg_opclass` lists those classes.
- `approx_distinct(x)`, another name of `approx_count_distinct(x)` with
  the same fast path on columnar tables; Apache Superset writes it for
  its approximate distinct count.
- `pg_cancel_backend(pid)` and `pg_terminate_backend(pid [, timeout])`.
  `pg_stat_activity` lists every open session of the database (server
  connections and connections of the embedding application) with pid,
  user, application name, state and statement. A terminated server
  connection gets `FATAL 57P01` and is closed; a terminated embedded
  connection fails every statement with `57P01`.
  `Session::is_terminated`, `Session::set_terminate_hook` and
  `Session::refresh_activity`.
- Ordered-set aggregates `percentile_cont`, `percentile_disc` and `mode`
  with `WITHIN GROUP (ORDER BY …)`, for one fraction or an array of
  fractions, as in PostgreSQL. They used to fail with `0A000`.
- `scripts/sqlalchemy-check.py`: compares SQLAlchemy's schema
  introspection of squad with that of PostgreSQL.

### Changed

- `SET` inside a transaction is undone when the transaction rolls back
  (also by an error or `ROLLBACK TO SAVEPOINT`), and `SET LOCAL` ends with
  the transaction, as in PostgreSQL; both used to last for the session.
  Outside a transaction block, `SET LOCAL` now sends a warning and has no
  effect.
- `ALTER TABLE … ENABLE ROW LEVEL SECURITY` takes effect instead of
  sending a warning; `DISABLE ROW LEVEL SECURITY` is no longer ignored.
  The default login bypasses the policies of tables without `FORCE ROW
  LEVEL SECURITY`, so existing applications see no change.
- `current_setting`, `current_user` and the other session functions
  that do not change anything are treated as stable: they are evaluated
  once per statement and can bound index scans.
- `nextval` and `setval` fail with `25006` in a read-only transaction.
- `SET ROLE` and `SET SESSION AUTHORIZATION` to a name other than the
  login fail with `0A000`; they used to be stored and ignored.
- `RESET ALL` also resets `default_transaction_isolation`; `DISCARD ALL`
  resets every setting and fails inside a transaction block (`25001`).
- `squad_server::users::parse_users` and `read_users_file` return
  `Users { verifiers, logins }`. In the users file, words after the user
  name are attributes, so user names can no longer contain spaces.
- `pg_roles` lists the names that policies apply to besides the
  session's login, and reports `rolsuper`/`rolbypassrls` according to
  the login.

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
- **Remote snapshots are taken often enough to keep restores short.**
  The log since the newest snapshot used to be counted in memory from
  zero at every start, so a writer restarted every few hours never
  reached `snapshot_wal_bytes` and a restore had to replay tens of
  thousands of log objects. A writer now counts the log after the newest
  snapshot from the store when it opens, and takes a snapshot also after
  `RemoteConfig::snapshot_wal_objects` log objects (default 2000) and
  `RemoteConfig::snapshot_interval` (default 15 minutes, if there is new
  log); after a restore at open that replayed more than the thresholds
  it takes one in the background. `RemoteStats` reports the log since
  the newest snapshot, the trigger of the last snapshot and what the
  restore at open did (`RestoreStats`).
- **Restores fetch in parallel.** Snapshot chunks and log objects are
  downloaded with up to `RemoteConfig::restore_concurrency` requests at
  a time (default 16; `ReplicaConfig::restore_concurrency`), chunks are
  verified on the fetching threads, and log objects are applied strictly
  in order; replicas prefetch new log objects the same way. Against a
  store with 2 ms per request, a restore of 6 chunks and 300 log objects
  went from 668 ms to 59 ms. `restore_from_with` takes the concurrency
  and returns `RestoreStats`.
- Restores and snapshots report progress and results through the `log`
  crate (level info): start, every 5 seconds, end; `squad` writes these
  messages to stderr, filtered by `SQUAD_LOG`.

### Fixed

- **Columnar tables no longer rewrite whole tables over and over.** Small
  segments were merged smallest first, and every segment under 2 million
  rows counted as small, so whenever eight new small segments had gathered
  (about every 100 s with steady inserts) the whole table was merged into
  one new segment again. Merges now combine segments of similar size
  (tiers of a factor of 4), so each row is rewritten about once per tier.
- **Memory of columnar tables is bounded.** The cache of opened segments
  was bounded by count (4096 segments), not bytes; it is now a sixteenth
  of `cache_bytes`. Deleted segments are dropped from the caches at once.
  A compaction moves at most `compact_max_bytes` (new, default 64 MiB) of
  decoded rows per run and sorts them without copying them into one batch
  first. Parallel range reads use a fixed set of 8 threads per process
  instead of new threads per read. `Database::columnar_memory()` (new)
  reports what the caches hold.
- **A merge of columnar segments no longer holds its output in memory.**
  It writes the merged segment into a temporary file
  (`ColumnarConfig::temp_dir`, new) and stores it from there; S3 uploads it
  in parts, one part in memory at a time. `ObjectStore::put_file` (new)
  stores a local file; the stores of `squad-object` copy or upload it in
  pieces.
- **Read replicas no longer lose segments right after a compaction.**
  With segments in a remote object store, replaced segment objects are
  deleted only after `ColumnarConfig::retire_grace` (new, default 10
  minutes).
- `pg_index.indkey` and `indoption` were `smallint[]` (`'{1,2,4}'`)
  instead of `int2vector` (`'1 2 4'`); SQLAlchemy failed to parse them.
- `pg_stat_activity` showed only the calling session, and
  `pg_cancel_backend` / `pg_terminate_backend` did not exist.
- Parameters of the catalog helper functions were always typed `text`.
  `pg_cancel_backend($1)` and `pg_terminate_backend($1, $2)` now take
  `integer` and `bigint`, and the advisory lock functions `bigint` (one
  argument) or `integer` (two), as in PostgreSQL; drivers that send
  binary parameters (tokio-postgres) failed with a type mismatch.
- `squad serve --help` (and `--help`/`-h` after any subcommand) failed
  with `unknown option --help`; it now prints the usage.

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
