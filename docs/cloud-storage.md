# Remote storage: a database that lives in an object store

By default a squad database lives in a local directory. With a *remote*
configured, the directory becomes a working copy, and a complete,
restorable copy of the database is kept in an object store — a second
directory, or a bucket in S3 or a compatible service. If the machine is
lost, a new one opens the same remote and has the database back.

Reads and writes still happen in memory and on the local disk, at the
same speed as before. What changes is where the durable truth is.

```text
            application
                │
        ┌───────▼────────┐
        │ squad (memory) │   page cache, MVCC
        └───────┬────────┘
                │ commit
        ┌───────▼────────┐        ┌──────────────────────────┐
        │ local directory│ ─────► │ object store             │
        │ data.db + WAL  │  log,  │ file:// or s3://         │
        └────────────────┘ snap-  │ log objects + snapshots  │
                           shots  └──────────────────────────┘
```

## Using it

```rust
use std::sync::Arc;
use squad::{object, Config, Database, RemoteConfig, RemoteSync};

let store = object::open_url("s3://my-bucket/prod/app?region=eu-central-1")?;
let db = Database::open("data/", Config {
    remote: Some(RemoteConfig::new(store, RemoteSync::Sync)),
    ..Config::default()
})?;
```

```sh
squad serve ./data --remote 's3://my-bucket/prod/app?region=eu-central-1' --remote-sync sync
squad restore 's3://my-bucket/prod/app?region=eu-central-1' ./copy
```

The S3 client is behind the `s3` feature of the `squad` crate; the
`squad` binary includes it. Credentials come from the URL
(`access_key`, `secret_key`) or from `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`; `endpoint=http://host:9000` selects a
compatible service. `file:///path` uses a directory, for example a
network mount.

Opening a database with a remote:

| Local directory | Object store | What happens |
| --- | --- | --- |
| empty or missing | empty | a new database is created |
| empty or missing | has a database | it is downloaded, then opened |
| has the database, same state | — | opened |
| has commits the store lacks (a crash before they were shipped) | — | opened; a snapshot brings the store up to date |
| behind the store, or last written by a writer that has since been replaced | — | **refused**, unless `RemoteConfig::restore` / `--remote-restore` is set, which replaces the directory with the remote state |

`Database::restore_from(store, dir)` (and `squad restore`) downloads the
database into an empty directory without taking over as its writer; the
result is an ordinary local database.

## Modes and guarantees

| | `RemoteSync::Async { interval }` | `RemoteSync::Sync` |
| --- | --- | --- |
| A commit returns after | the local `fsync` | the local `fsync` **and** the upload of its log record |
| Process crash, disk intact | nothing lost | nothing lost |
| Machine and disk lost | commits since the last upload are lost: at most about `interval`, more while the store is unreachable | nothing acknowledged is lost |
| Recovery point objective | ≈ `interval` | 0 |
| Commit latency added | none | one upload round trip, shared by concurrent commits |
| Store unreachable | commits continue; the backlog is shipped later | commits fail with SQLSTATE `XX000` (`remote storage: …`) until it is back; nothing is acknowledged that is not stored |

In both modes the database on restore is always a state it really had:
a prefix of the commit history, never a mixture.

A commit that fails in `Sync` mode because the upload failed may still
have been applied locally, and it reaches the store later. As with any
failed commit, the application must treat the outcome as unknown.

**Recovery time** is the download of the newest snapshot plus replay of
the log written since. Both grow with the database size and with
`snapshot_wal_bytes` (default 64 MiB of log between snapshots).

## What is stored

```text
writer/claim-<epoch>            one per process that ever opened the database as writer
writer/base-<epoch>             the log position at which that writer took over
wal/<epoch>/<first>-<last>      log records first..=last
snapshot/<epoch>-<lsn>          manifest: the list of chunks of the data file at <lsn>
chunk/<sha256>                  1 MiB pieces of data file images, named by content
```

- **Log shipping.** Every commit is one log record. Records are
  uploaded as immutable objects named by the range of sequence numbers
  they contain: in `Sync` mode one object per group of concurrent
  commits, in `Async` mode one per interval.
- **Snapshots.** When enough log has been shipped (or on `CHECKPOINT`),
  squad takes a consistent copy of the data file, cuts it into 1 MiB
  chunks, and uploads the chunks whose content is not already in the
  store, followed by a manifest. The manifest is written last, so a
  snapshot either exists completely or not at all.
- **Cleanup.** After a new manifest is stored, older manifests, chunks
  no manifest refers to, and log objects the snapshot covers are
  deleted. The store therefore holds one snapshot plus the log since.
- Every object is written once and never changed. A retried or
  duplicated upload writes identical bytes to the same key, which is
  what makes uploads safe to repeat after a failure.

Restore reads the newest valid snapshot and replays the log objects
after it in order, checking sequence numbers and checksums; a missing
record ends the replay at the last complete commit before it.

### Request and storage cost

| Event | Requests |
| --- | --- |
| Commit group in `Sync` mode | 1 `PUT` + 2 `HEAD` |
| Shipping interval in `Async` mode (if there were commits) | 1 `PUT` + 2 `HEAD` |
| Snapshot | 1 `PUT` per new chunk, 1 `PUT` for the manifest, 3 `LIST`, 1 `DELETE` per obsolete object |
| Open | a handful of `LIST`/`GET`/`PUT`; plus the download when restoring |

Storage is the size of the data file plus the log since the last
snapshot. How much a snapshot uploads depends on where the changes are:
chunks are compared by content, so an append-mostly table re-uploads
only the chunks that grew, while updates scattered across the key space
touch most chunks and re-upload most of the file. Measured with
`cargo run --release -p squad-storage --example remote_bench`:
a 23.5 MiB database with 1% of its rows updated evenly across the key
space re-uploaded all 24 chunks.

Measured overhead on the development machine, against the in-process S3
emulator (so without network latency — add your round-trip time to the
`Sync` figures):

| Commit of one small row | p50 | p99 | commits/s |
| --- | --- | --- | --- |
| local only | 130 µs | 396 µs | 6,400 |
| remote, `Async` | 111 µs | 424 µs | 7,750 |
| remote, `Sync`, 1 thread | 352 µs | 912 µs | 2,430 |
| remote, `Sync`, 16 threads (shared uploads) | — | — | 4,340 |

## One writer at a time

A database must have exactly one writer. Locally the `LOCK` file
ensures that. For the object store squad uses *epochs*:

1. A process that opens the database claims the next epoch by creating
   `writer/claim-<n>` with a conditional put, which only one process can
   win. It does this before reading the remote state.
2. It records the log position it starts from in `writer/base-<n>`.
3. Every writer checks before and after each upload whether a later
   claim exists. If so, it stops uploading and fails all further writes
   with `remote storage: another writer took over`. In `Sync` mode the
   commit in flight is **not** acknowledged.
4. On restore, an epoch's log records count only up to the position at
   which the next epoch took over. Whatever a replaced writer still
   managed to upload after that point is ignored.

The result: starting a second writer on the same remote — deliberately,
to fail over, or by mistake — never mixes two histories, and never loses
a commit that was acknowledged in `Sync` mode.

A directory that cannot continue the remote history (it is behind, or
was left by a writer that has been replaced) is recognised *before*
step 1 and refused without claiming anything, so starting a stale copy
by mistake does not stop the writer that is running.

The same mechanism is what makes promoting a
[read replica](replication.md#promotion-and-failover) safe: the replica
claims the next epoch, applies everything the previous writer uploaded,
and continues from there.

Residual risks, stated plainly:

- The claim relies on the store's conditional put (`If-None-Match: *`).
  AWS S3 (since 2024) and MinIO support it, as do local directories.
  On a service without it, two processes starting at the same instant
  could both believe they hold the same epoch.
- A replaced writer checks for a newer claim *before* it deletes
  obsolete objects, but the check and the deletes are not atomic. A
  replaced writer that was in the middle of a cleanup at the instant of
  takeover could delete chunks the new writer's first snapshot reuses.
  The window is the duration of one cleanup; the new writer's own log
  objects are never affected. If a restore finds a chunk missing, it
  fails loudly rather than returning partial data.
- In `Async` mode a replaced writer keeps accepting commits locally
  until its next upload attempt (at most one interval). Those commits
  are not in the remote history.

## Failure behaviour

| Failure | Effect |
| --- | --- |
| Upload fails (network, 5xx) | retried with backoff by the S3 client; then as in the table of modes above. Pending records are kept and sent with the next attempt. |
| Response to a successful upload is lost | the retry writes the same object again |
| Process crashes mid-snapshot | the manifest was not written, so the snapshot does not exist; leftover chunks are removed by the next cleanup |
| Process crashes mid-cleanup | some obsolete objects remain until the next cleanup |
| Local disk lost | restore from the store |
| Object store loses or corrupts an object | restore verifies every chunk against its SHA-256 name and every log record against its checksum, and fails with an error naming the object |
| Both the machine and the bucket are lost | the data is gone; replicate the bucket if that matters |

## What this is not

- **Not multi-writer.** One process writes; a second one takes over
  rather than joining.
- **Not synchronous replication.** Read replicas that follow the
  writer through the object store, and promotion of a replica when the
  writer is lost, are described in [replication](replication.md). A
  commit never waits for a replica.
- **Not storage separated from compute.** The whole database is on the
  local disk and its hot part in memory; the object store is not read
  during normal operation. A database larger than the local disk does
  not fit.
- **Not point-in-time recovery.** Only the latest state can be
  restored; older snapshots and log are deleted once superseded.
- **Not encrypted by squad.** Use the bucket's server-side encryption
  and TLS endpoints.

## How it is tested

`crates/squad-storage/tests/remote.rs` runs the storage engine on the
simulated file system against an object store that fails requests and
loses confirmations at random, kills the machine at a random point —
with and without a clean shutdown — and restores on a fresh machine,
three generations per scenario. It checks that the restored state is a
state the database really had and that no commit acknowledged in `Sync`
mode is missing. 1,500 seeded scenarios were run three times each (thread
timing differs between runs) and pass; 60 run with every `cargo test`. Further tests cover
fencing of a replaced writer, refusal of stale directories, cleanup
(exactly one manifest, no unreferenced chunks, no covered log objects),
and an end-to-end run against the S3 emulator with injected failures.

The S3 client has not been run against a real S3 service; see
[the crate's README](../crates/squad-object/README.md) for exactly what
was and was not verified.
