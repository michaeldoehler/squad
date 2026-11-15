# How squad is tested

A database is only as trustworthy as the ways it has been tried to
break. This page lists every test suite, what it proves, what it was
measured at, and how to run it. Numbers are from the development of
version 0.1; "run on demand" suites are not part of `cargo test`.

| Question | Suite | Scale |
| --- | --- | --- |
| Does SQL behave like PostgreSQL? | SQL regression suite | 15 scripts, ~950 statements, output identical to PostgreSQL 16 |
| | sqllogictest, own suite | 468 records with results from PostgreSQL 16 |
| | sqllogictest, public corpus (on demand) | 5,663,445 of 5,675,004 records pass (99.80 %); differs from PostgreSQL 16 on 6 |
| | Differential fuzzer (on demand) | ~1.3 M random statements against PostgreSQL 16 |
| | Value semantics | several hundred expected values taken from PostgreSQL 16 |
| Do real tools and drivers work? | Server integration tests | psql, pgbench, pg_dump output, tokio-postgres, sqlx |
| | Catalog queries | 43 psql and 47 ORM introspection queries replayed |
| Is concurrent access correct? | Concurrency tests | 13 scenarios at all isolation levels |
| Does committed data survive? | Crash simulation, storage | 10,000 + 4,000 seeds on demand; 150 + 40 per test run |
| | Crash simulation, SQL | 150 seeds on demand; 6 per test run |
| | Remote tiering fault injection | 1,500 seeds on demand; 60 per test run |
| | Damaged write-ahead log | mid-log damage refused, torn tails tolerated (`squad-storage/tests/wal.rs`) |
| Is there only ever one writer? | Fencing and claim tests | concurrent claimants and writers, stores with and without conditional writes |
| Does it work against real S3? | `SQUAD_TEST_S3_URL` suites (on demand) | conformance, restore, replica, concurrent promotions; run against Garage 2.5 |
| Are columnar tables equivalent to row tables? | Columnar tests | 35 queries × 10 table states; concurrent compaction |
| Is the column engine itself correct? | Model tests, corruption fuzzing | randomized against a row-at-a-time reference; 3,000 corruptions |

Everything under `cargo test --workspace` takes about a minute after the
first build.

## PostgreSQL as the oracle

squad does not define its own SQL semantics; PostgreSQL 16 does. Three
suites compare against a real server.

### SQL regression suite

`crates/squad/tests/regress/*.sql` are scripts in the style of
PostgreSQL's own regression tests. Each has a `.out` file holding what
`psql` printed for it against PostgreSQL 16 — rows, column names, and
for errors the SQLSTATE. The test runs every script against squad and
requires byte-identical output.

The scripts cover expressions and literals, tables and constraints,
joins, aggregates, subqueries and set operations, window functions,
CTEs, data types, schema changes, transactions and settings, sequences,
views and prepared statements, custom settings (`SET LOCAL`,
`set_config`), row-level security, plus one script of reproductions for
every bug the fuzzer found.

`rls.sql` tests row-level security from the point of view of a table's
owner, which is how squad treats its default login. Its expected output
must be produced by a PostgreSQL role that owns the tables and is not a
superuser (superusers bypass every policy), e.g.
`PGUSER=regress_rls scripts/regress-expected.sh rls` after `CREATE ROLE
regress_rls LOGIN CREATEDB`. Other logins, read-only logins and
columnar tables are tested in `crates/squad/tests/rls.rs` and, over the
wire with tokio-postgres and psql, in `crates/squad-server/tests/rls.rs`.

```sh
cargo test -p squad --test regress
scripts/regress-expected.sh [name…]     # regenerate .out from a PostgreSQL 16
```

### sqllogictest

`crates/squad-sqllogictest` runs files in the sqllogictest format.

- The checked-in suite (`slt/`, 468 records) covers the SQL core in
  PostgreSQL's dialect; its expected results were generated from
  PostgreSQL 16.
- The public corpus written for SQLite — 622 files, 5.67 million records
  executed — runs on demand with `scripts/sqllogictest-corpus.sh`.

Result against corpus revision `c93ac416`:

| Directory | Passed | Failed | Pass rate |
| --- | ---: | ---: | ---: |
| select1–5 | 10,706 | 0 | 100 % |
| evidence | 137 | 11 | 92.57 % |
| index/between, commute, delete, in, orderby | 1,740,910 | 0 | 100 % |
| index/random | 191,958 | 6,425 | 96.76 % |
| index/view | 104,369 | 4,860 | 95.55 % |
| random/aggregates | 1,174,483 | 1 | 100 % |
| random/expr | 1,200,104 | 262 | 99.98 % |
| random/groupby, random/select | 1,240,778 | 0 | 100 % |
| **total** | **5,663,445** | **11,559** | **99.80 %** |

The corpus encodes SQLite's behaviour, so its failures are not
automatically squad's. `scripts/sqllogictest-pg-compare.py` replays the
corpus through PostgreSQL and compares verdicts record by record:

| | Records |
| --- | ---: |
| squad and PostgreSQL both pass | 5,663,442 |
| both fail (SQLite-only behaviour: unary plus on text, `IN ()`, …) | 11,556 |
| only squad fails | 3 |
| only PostgreSQL fails | 3 |

The three records only squad fails are cases where PostgreSQL skips a
subexpression that would raise an error (inside a `CASE` branch ruled
out by constants; by evaluating the cheaper `WHERE` condition first).

### Differential fuzzer

`crates/squad-difftest` generates, from a seed, a random schema (2–4
tables with keys, constraints, indexes and foreign keys), random data,
and a stream of random statements — queries built from a typed
expression grammar with joins, grouping, window functions, subqueries,
CTEs and set operations, data changes, schema changes and transactions.
Every statement runs against squad and against PostgreSQL; results,
column names, SQLSTATEs and, after data changes, table contents must be
equal. A difference is reduced to a minimal script automatically.

About 1.3 million statements over 9,000 seeds were run during
development. They exposed 18 bugs, each fixed and kept as a regression
case. In the last 211,000 statements one real difference remained (the
28-digit limit of `numeric`). Known remaining differences are listed in
the crate's README and in [compatibility](compatibility.md).

```sh
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres     # a PostgreSQL 16, C locale
cargo run --release -p squad-difftest -- --seed 1 --iterations 100 --statements 200
```

## Tools and drivers

`crates/squad-server/tests` start a server on a free port and drive it
with real clients; tests that need `psql` or `pgbench` skip themselves
when the tools are missing.

- A hand-written protocol client checks startup, both query protocols,
  binary formats, portals with row limits, `COPY`, authentication and
  the server's behaviour on malformed and random input.
- `psql` runs scripts including `\copy`; a plain-format `pg_dump` taken
  from PostgreSQL loads; `pgbench` initializes and runs its TPC-B-like
  and select-only scripts and the resulting balances are verified.
- `tokio-postgres` and `sqlx` run typed queries, transactions, array
  parameters, custom enum types and sqlx's migrator.

`crates/squad/tests/catalog.rs` replays the catalog queries behind
psql's backslash commands and typical ORM introspection queries, with
results taken from PostgreSQL where they are deterministic.

## Concurrency

`crates/squad/tests/concurrency.rs` runs several connections on
different threads and asserts what PostgreSQL would do at the same
isolation level: no lost updates under Read Committed, re-evaluation of
the predicate after a lock wait, stable snapshots and `40001` under
Repeatable Read, write skew prevented under Serializable (and possible
under Repeatable Read), a constant total under concurrent serializable
transfers with a concurrent reader, deadlock detection with exactly one
victim, unique constraints and upserts under contention, `SKIP LOCKED`
queues that hand out every job once, `NOWAIT` and `lock_timeout`,
foreign keys against concurrent deletes of the parent, and schema
changes under load.

The transaction layer has its own model test (random interleavings of
transactions against a reference) and a test that readers always see
complete snapshots while writers replace every value and garbage
collection runs.

## Durability: simulated crashes

All file access goes through a small interface with two
implementations: the real file system and a simulated one. The
simulation keeps files in memory, tracks which bytes have been synced,
and can — deterministically from a seed —

- kill the process at any write,
- keep, drop or tear writes that were not yet synced, and drop directory
  operations that were not synced,
- make any operation fail with an I/O error.

**Storage level** (`crates/squad-storage/tests/crash_sim.rs`). Each seed
runs random write batches, checkpoints and reopenings with crashes
armed at random operations, including in the middle of checkpoints and
log appends. After every crash the engine is reopened and checked: each
batch acknowledged as durable is present; each batch is present
completely or not at all; the state equals the model at some prefix of
the batch sequence; the structure verifies. A second variant runs
several writers and a checkpointer concurrently. Removing any single
`fsync` or ordering step from the engine makes these tests fail — that
was checked by deleting seven of them one at a time.

**SQL level** (`crates/squad/tests/crash_sim.rs`). The same simulated
file system under the whole database: random transfers between accounts
with a ledger, schema changes, checkpoints and vacuums; after each crash
every acknowledged transaction must be present, the ledger must explain
every balance, indexes must agree with their tables, and acknowledged
schema changes must be in place.

**Remote tiering** (`crates/squad-storage/tests/remote.rs`). The object
store fails, duplicates or half-applies uploads at random; a restore
into an empty directory must always produce a state the database
actually had, with every commit acknowledged in sync mode. Further
tests start several writers at once on stores that enforce and that
ignore conditional writes (exactly one survives, no acknowledged commit
is lost), and run twelve committers against a slow, failing store.

**Damaged log** (`crates/squad-storage/tests/wal.rs`). A bit flipped in
a record that later records vouch for makes the open fail; damage to
the last record, or among records synced together, is a torn tail.
The crash simulations above check that real torn tails are never taken
for corruption.

```sh
SQUAD_SIM_SEEDS=10000 SQUAD_SIM_CONCURRENT_SEEDS=2000 \
    cargo test --release -p squad-storage --test crash_sim
SQUAD_SIM_SEEDS=500 cargo test --release -p squad --test crash_sim
SQUAD_REMOTE_SEEDS=1500 SQUAD_REMOTE_CONCURRENT_SEEDS=100 SQUAD_REPLICA_SEEDS=200 \
    cargo test --release -p squad-storage --test remote --test replica
```

A failing seed is reproduced with `SQUAD_SIM_START=<seed>
SQUAD_SIM_SEEDS=1`.

What the simulation does not cover: bugs in the real file system or
device, and devices that acknowledge a flush without performing it.

## Columnar tables

`crates/squad/tests/columnar.rs` keeps the same rows in an ordinary
table and a columnar table and requires identical answers to 35 queries
— fast-path aggregations and general queries — in every state: empty,
delta only, one segment, segments plus delta, after deletes that rewrite
segments, after a rollback, after truncation, and after reopening from
disk. A concurrency test inserts whole batches from three threads while
another thread compacts continuously and a reader checks that every
batch is visible completely and every row exactly once. (This test found
a bug in garbage collection that predated columnar tables.)

`crates/squad-columnar` tests the engine itself: every encoding and
every predicate and aggregate against a row-at-a-time reference on
random data including NULLs, NaN and extreme values, with and without
parallelism; error bounds of the approximate aggregates; and 3,000
random corruptions of segment files, each of which must be detected.

## Replication

`crates/squad-storage/tests/replica.rs` drives the replica engine
against a writer on an in-memory object store: bootstrap from snapshot
and log, restart, falling behind the writer's log retention, and
promotion. Randomized tests make one in six store reads fail and crash
the replica at random points, and run chains of six promotions; after
each, replica and writer must hold identical data and no synchronously
acknowledged commit may be missing.

`crates/squad/tests/replica.rs` tests the SQL level: schema changes and
columnar tables arriving on a replica, every kind of write being refused
with `25006`, repeatable-read snapshots staying consistent while replay
proceeds (readers check an invariant over concurrent transfers), and
concurrent random writers with a replica polling every millisecond
followed by a promotion. `crates/squad-server/tests/cli.rs` runs writer
and replica as separate processes, kills the writer, promotes the
replica over the wire protocol and lets the old writer rejoin as a
replica.

## Sharding

`crates/squad-cluster/tests/model.rs` applies the same random inserts,
updates, deletes and queries to a cluster and to a single database and
requires equal answers, on 1, 2, 4 and 7 shards and on a cluster that
mixes local shards with shards behind the wire protocol.
`tests/cluster.rs` covers what the model cannot: cluster identity on
reopen (a wrong shard count or order is refused), routing decisions,
transactions bound to a shard, prepared statements, error propagation,
and that every query shape that cannot be answered correctly is
rejected with `0A000` instead of answered wrongly. `tests/remote.rs`
tests remote shards with enums, numerics, arrays and JSON, and shards
that are unreachable or come back.

## Real S3

Tests against a real S3 service run when `SQUAD_TEST_S3_URL` is set to
an `s3://bucket?endpoint=…&region=…&access_key=…&secret_key=…` URL;
otherwise they pass and print `skipped …: SQUAD_TEST_S3_URL not set`
(visible also without `--nocapture`). Each test works under a key
prefix of its own and removes it afterwards, so a shared bucket is
fine.

```sh
eval "$(scripts/garage-dev.sh start)"   # a local single-node Garage
cargo test -p squad-object --features test-util --test conformance
cargo test -p squad --test real_s3
```

- `squad-object` conformance: the same suite as for the other stores,
  plus a multipart upload; it reports whether the service enforces
  `If-None-Match: *` (Garage 2.5 does not) and expects the
  corresponding behaviour.
- `crates/squad/tests/real_s3.rs`: restore after losing the machine, a
  replica following a writer, two concurrent promotions.

The benchmark of commit throughput against a store is
`cargo run --release -p squad-storage --example remote_sync_bench`
(see [cloud storage](cloud-storage.md#request-and-storage-cost)).

## What is not tested yet

- Replication and sharding across real machines and networks: all tests
  run the nodes on one host.

- Long-running soak tests and production workloads: squad has not run in
  production.
- The S3 client against Amazon S3 or MinIO. It is tested against an
  in-repository emulator that verifies request signatures, against
  Garage 2.5 (`SQUAD_TEST_S3_URL`, see below), and its signer against
  the published examples of the AWS SigV4 test suite.
- Platforms other than Linux x86-64 (CI also builds and tests on macOS).
- Performance regressions: benchmarks are run by hand; there is no
  automated gate, because shared CI machines are too noisy to enforce
  one.
