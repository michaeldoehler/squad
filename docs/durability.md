# Durability, recovery and backup

The contract: once `COMMIT` has returned, the transaction survives any
crash of the process, and — in the `sync` and `group` modes — loss of
power. A transaction is applied completely or not at all, across all
tables and indexes it touched.

## On disk

```text
data/
├── data.db              B+tree pages, 8 KiB each, checksummed
├── wal/
│   └── 00000007.wal     write-ahead log segments
├── checkpoint.journal   exists only while a checkpoint is being written
├── objects/             segments of columnar tables (unless a remote store is configured)
└── LOCK                 held while the database is open
```

The format is squad's own. It is not PostgreSQL's on-disk format, and a
PostgreSQL data directory cannot be opened; use `pg_dump` to move data
(see below).

## Commit

1. The transaction's writes are appended to the log as one record with a
   checksum.
2. They are applied to the in-memory B+tree.
3. The commit waits until the log is on stable storage. Only then does
   it become visible to other transactions, and `COMMIT` returns.
   Commits that wait at the same time share one `fsync` and become
   visible together (see
   [transactions](transactions.md#when-a-commit-becomes-visible)).

A transaction's log record is limited to 4 GiB; a larger transaction
fails with an error before anything is written.

| Mode | A commit returns after | Survives process crash | Survives power loss |
| --- | --- | --- | --- |
| `sync` (default) | `fsync` of the log; concurrent commits share one | yes | yes |
| `group` | the same, after waiting up to ~200 µs for company | yes | yes |
| `async` | the record is handed to the operating system; `fsync` follows within milliseconds | yes | the last few milliseconds of commits may be lost; never a partial transaction |

`sync` is the mode to use unless you have measured a reason not to. With
many concurrent writers group commit happens by itself, because commits
that arrive while an `fsync` is running share the next one.

Like every database, squad relies on the device honouring `fsync`. Drives
and virtualization layers that acknowledge a flush before the data is
stable can lose acknowledged commits on power loss; no software can
detect that.

## Checkpoints

The data file changes only during a checkpoint. A checkpoint is started
automatically when the log exceeds `checkpoint_wal_bytes` (64 MiB by
default) or the share of modified pages in the cache grows large, and on
clean shutdown. It can be requested with `CHECKPOINT` or
`Database::checkpoint`.

A checkpoint writes the modified pages twice: first into
`checkpoint.journal`, which is synced, then into `data.db`. If the
process dies while pages are being written into `data.db`, recovery
re-applies the journal. If it dies while the journal is being written,
the journal is discarded and the data file is still the previous
checkpoint. Torn page writes therefore cannot corrupt the database.

Commits continue during a checkpoint. They are paused only for the
moment the set of modified pages is captured.

## Recovery

Opening a database always runs recovery; there is no separate step and
nothing to configure:

1. If a complete checkpoint journal exists, its pages are written into
   the data file. An incomplete one is deleted.
2. Log records newer than the last checkpoint are replayed in order.
3. Replay stops at the first record that is incomplete or fails its
   checksum — normally the tail of a commit that was never
   acknowledged, which is discarded.
4. Unless the log shows that the missing record had been made durable:
   every record carries the highest log position that was on stable
   storage when it was written. If an intact record after the damage
   says the damaged one was already synced, the damage happened later
   (a failing disk, a stray write), and the open **fails** with a
   corruption error (SQLSTATE `XX001`) that names the segment and
   position. Nothing is deleted; restore from a backup or from the
   remote copy. Replaying up to the damage and dropping the rest, as a
   torn tail is handled, would silently lose committed transactions.

A crash can only tear records that were not yet synced, but the device
may persist unsynced writes in any order, so intact records after a
torn one are normal and are not taken as proof of corruption. The limit
of the detection follows from that: damage to the last records written
before the latest `fsync` — those no later record vouches for — cannot
be told apart from a torn tail and is treated as one. On a database
that keeps committing, every record is soon followed by records that
vouch for it.

The log format changed for this (segment magic `SQWAL002`). A directory
whose log still holds segments of the earlier development format is
refused with an error; open it once with the version that wrote it, so
that it checkpoints and empties the log.

Because uncommitted transactions never write to the log or the data
file, there is nothing to undo. Recovery time is proportional to the log
written since the last checkpoint.

## Integrity

Every page and every log record carries a CRC32C checksum that is
verified when it is read. A mismatch is reported as SQLSTATE `XX001`
instead of returning wrong data. `Database::verify` (or `squad verify
<dir>`) reads the whole database and checks checksums, key order, the
links between pages, overflow chains and the free list.

After an I/O error during a write, the engine refuses further writes
until the database is reopened, rather than continuing in an unknown
state.

## Backup

```rust
db.backup("backups/2026-11-09")?;
```

```sh
squad backup ./data ./backups/2026-11-09     # when no process has it open
```

A backup is a complete database directory containing a consistent
snapshot. It is taken while the database is in use; commits are not
blocked while the files are copied. To restore, open the backup
directory (or copy it into place).

A backup made this way contains the row store. Segments of columnar
tables are separate files under `objects/`; copy that directory as well
(segments are immutable, so a plain file copy taken after the backup is
consistent with it or newer).

For a continuously maintained copy in an object store, including S3, and
restore on another machine, see [cloud storage](cloud-storage.md).
Point-in-time recovery is not implemented yet.

## Moving data in and out

From PostgreSQL to squad:

```sh
pg_dump --no-owner --no-privileges mydb > dump.sql
squad serve ./data &
psql -h 127.0.0.1 -U squad squad -v ON_ERROR_STOP=1 -f dump.sql
```

Plain-format dumps (schema plus `COPY … FROM stdin` blocks) load as long
as they use features squad has; see [compatibility](compatibility.md).
Functions, triggers and extensions in a dump are reported as unsupported
(SQLSTATE `0A000`).

`COPY` works in both directions over the wire, in text and CSV format:

```sql
\copy users TO 'users.csv' WITH (FORMAT csv, HEADER)
\copy users FROM 'users.csv' WITH (FORMAT csv, HEADER)
```

## How this is tested

Durability claims are only as good as their tests. squad runs the
storage engine and the SQL layer on a simulated file system that kills
the process at random writes, tears unsynced writes, drops unsynced
directory operations and injects I/O errors, then recovers and checks
that every acknowledged commit is present and every transaction is all
or nothing. See [testing](testing.md) for the method and the number of
scenarios run.
