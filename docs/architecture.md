# Architecture

squad is built as four layers, each a crate with a narrow interface to the
one below. A statement travels down through them; nothing reaches around a
layer.

```text
 application (Rust)                    psql, drivers, ORMs, other languages
        │  function calls                         │  Postgres wire protocol v3
        ▼                                         ▼
 ┌─────────────────────┐              ┌──────────────────────────┐
 │ squad: embedding API│◄─────────────│ squad-server             │
 │ Database, Connection│   Session    │ protocol, auth, COPY, CLI│
 └─────────┬───────────┘              └──────────────────────────┘
           ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ squad: SQL engine                                            │
 │ parser (libpg_query) → binder → optimizer → executor         │
 │ catalog · types and functions · system catalogs · plan cache │
 └─────────┬────────────────────────────────────────────────────┘
           ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ squad-kv: transactions                                       │
 │ MVCC snapshots · buffered writes · row locks · deadlock      │
 │ detection · savepoints · serializable validation · GC        │
 └─────────┬────────────────────────────────────────────────────┘
           ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ squad-storage: durable ordered key-value engine              │
 │ B+tree on 8 KiB pages · buffer pool · WAL with group commit  │
 │ checkpoints · crash recovery · backup · VFS (real/simulated) │
 └──────────────────────────────────────────────────────────────┘
```

Both entry points — the embedded API and the wire-protocol server — drive
the same `Session` type, so they cannot drift apart: what psql sees is what
the Rust API sees.

## Storage engine (`squad-storage`)

The storage engine is an ordered map from byte strings to byte strings
behind the `Engine` trait. It knows nothing about SQL or transactions.

- **One B+tree for the whole database.** Keys carry a four-byte object id
  as prefix, so every table and every index is a key range of the same
  tree. Dropping a table is a range delete.
- **Pages** are 8 KiB with a CRC32C checksum each. Keys are limited to
  2048 bytes; values of any size spill into overflow page chains.
- **Buffer pool.** squad manages its own page cache rather than using
  `mmap`. Pages are immutable snapshots that a writer copies before
  changing if a reader still holds them. Dirty pages are never written to
  the data file before a checkpoint ("no-steal"), which is what keeps the
  data file equal to the last checkpoint at all times.
- **Write-ahead log.** Every write batch is one log record (length,
  CRC32C, sequence number, operations). Records are logical redo records;
  there is no undo in the log because uncommitted data never reaches the
  storage engine (see below). Concurrent commits share one `fsync` (group
  commit). Segments are preallocated so that a commit's `fsync` only
  writes the record and never waits for a file system journal commit.
- **Checkpoints** write all dirty pages through a double-write journal:
  first to a journal file, `fsync`, then into the data file, `fsync`, then
  the journal is removed and the log truncated. A crash at any point
  leaves either the old or the new state, even on storage that can tear a
  page write. Writers are paused only while the set of dirty pages is
  snapshotted, not during the I/O.
- **Recovery** on open: finish or discard the journal, then replay the log
  records after the last checkpoint, stopping at the first torn record.
- **VFS.** All file access goes through a small trait. The production
  implementation uses the file system; the simulated one keeps files in
  memory, tracks which bytes were synced, and can kill the process at any
  write, tear unsynced writes and inject I/O errors — deterministically
  from a seed. See [testing](testing.md).

`MemEngine` implements the same trait on a `BTreeMap` for in-memory
databases.

## Transactions (`squad-kv`)

`squad-kv` turns the plain map into a multi-version store.

- **Versions live in the keys.** A committed write is stored under
  `(key, commit timestamp)`, newest first. A transaction reads at a
  snapshot timestamp and sees, for each key, the newest version at or
  below it.
- **Writes are buffered** in the transaction and applied at commit as a
  single atomic storage batch stamped with a new timestamp. Readers never
  block writers and writers never block readers. Because only committed
  data reaches storage, rollback is free and recovery needs no undo pass.
- **Row locks** serialize writers on the same row. They are taken before a
  row is changed (and by `SELECT … FOR UPDATE/SHARE`) and held to the end
  of the transaction. A wait-for graph detects deadlocks and fails one
  participant with SQLSTATE `40P01`.
- **Isolation levels.** Read Committed takes a new snapshot per statement;
  Repeatable Read keeps one snapshot per transaction (snapshot isolation);
  Serializable additionally records what the transaction read (keys and
  ranges) and at commit rejects it if a concurrent transaction changed any
  of that. Details in [transactions](transactions.md).
- **Garbage collection.** Versions no snapshot can see are removed when
  their key is written again and by `VACUUM`. There is no bloat that
  needs an operator: the horizon is the oldest registered snapshot.
- **Snapshot registry.** Starting a statement must not take a global lock.
  Snapshots register in an array of padded atomic slots; the garbage
  collector reads the slots to find its horizon.

## SQL engine (`squad`)

- **Parser.** squad embeds the PostgreSQL parser itself (libpg_query via
  the `pg_query` crate). Whatever Postgres parses, squad parses, with the
  same precedence and the same error positions. The price is a C
  dependency in the build.
- **Catalog.** Schemas, tables, indexes, sequences, views and enum types
  form one immutable snapshot that is stored as a single record. DDL
  writes a modified copy inside the surrounding transaction, so DDL is
  transactional: `BEGIN; CREATE TABLE …; ROLLBACK;` leaves nothing behind.
- **Binder.** Resolves names against the catalog, infers and checks types
  with Postgres' rules for implicit casts, operator and function overload
  resolution, and produces typed expressions. Untyped literals and
  parameters take their type from context as in Postgres.
- **Optimizer.** Rule based: predicate pushdown through joins, choice of
  primary key or secondary index from equality and range predicates,
  index nested-loop joins when the inner side has a matching key, hash
  joins for other equi-joins, and elimination of sorts that an index
  already provides (including backward scans for `ORDER BY … DESC
  LIMIT n`). There are no table statistics yet; join order follows the
  query text.
- **Executor.** A pull-based (Volcano) executor. Scans, filters,
  projections, limits and nested-loop joins stream rows; sorting,
  aggregation, window functions and set operations materialize their
  input. Uncorrelated subqueries run once per statement and are cached.
- **Rows and indexes.** A table is clustered by its primary key: the row
  lives under `table id ++ encoded key`. Tables without a primary key get
  a hidden 64-bit row id. A secondary index entry maps `index id ++
  encoded columns` to the row's key. Key encodings preserve order, so
  range scans and `ORDER BY` can run on the encoded bytes.
- **Plan cache.** Prepared statements keep their plan. Ad-hoc statements
  are cached by text and search path, so repeating the same SQL string
  skips parsing and planning. Plans are tied to the catalog version and
  re-made after DDL.
- **System catalogs.** `pg_catalog` and `information_schema` relations
  are computed on demand from the catalog snapshot.

### Life of a query

`SELECT name FROM users WHERE id = $1`, prepared once and executed:

1. The session checks that the plan's catalog version is current.
2. It starts an implicit transaction: registers a snapshot (one atomic
   store) — no lock is taken.
3. The plan is a primary-key point lookup. The executor evaluates `$1`,
   encodes it to a key and asks `squad-kv` for the newest version at the
   snapshot.
4. The storage engine walks the B+tree in cached pages and returns the
   row bytes; the executor decodes the row and projects `name`.
5. The transaction ends; nothing was written, so there is no commit work.

On one core this takes about 1.1 µs end to end (see
[benchmarks](benchmarks.md)).

## Server (`squad-server`)

The server is a thin adapter: one thread per connection reads protocol
messages and calls the session. It implements the simple and extended
query protocols with text and binary formats, portals, `COPY`, and trust,
cleartext or SCRAM-SHA-256 authentication. See [server](server.md).

## What squad deliberately does differently from PostgreSQL

| Topic | PostgreSQL | squad |
| --- | --- | --- |
| Process model | server process per connection | library in your process; optional server threads |
| Table storage | heap + separate primary key index | clustered by primary key |
| Old row versions | in the heap, removed by VACUUM | separate versions keyed by timestamp, removed when the key is rewritten |
| Uncommitted data | written to the heap | kept in the transaction until commit |
| Collation | per-database locale | bytewise (`C`) |
| Extensions | C ABI | none yet |

These are internal choices. Where they would show up in SQL behaviour the
aim is still to match Postgres; the known exceptions are listed in
[compatibility](compatibility.md).
