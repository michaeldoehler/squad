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
| Commit latency added | none | one upload and one listing round trip, shared by concurrent commits |
| Store unreachable | commits continue; the backlog is shipped later | commits fail with SQLSTATE `XX000` (`remote storage: …`) until it is back; nothing is acknowledged that is not stored |

In both modes the database on restore is always a state it really had:
a prefix of the commit history, never a mixture.

What `Async` mode guarantees, precisely: a commit is acknowledged after
the local `fsync`. The background thread uploads the records written
since its last round once per `interval` (default 1 s for
`squad serve`), as one object, and only records that are already
durable on the local disk. A commit is part of the remote history once
its record was uploaded before a new writer took over; how the takeover
point is chosen is described under
[one writer at a time](#one-writer-at-a-time). If more than 256 MiB of
records wait for upload (the store is unreachable for long), the queue
is dropped and the next snapshot covers those commits instead.

In both modes other transactions see a commit only once it is durable
in the sense of the mode: after the local `fsync`, and in `Sync` mode
also after the upload (see [transactions](transactions.md#when-a-commit-becomes-visible)).

A commit that fails in `Sync` mode because the upload failed may still
have been applied locally, and it reaches the store later. As with any
failed commit, the application must treat the outcome as unknown.

**Recovery time** is the download of the newest snapshot plus replay of
the log written since. Both grow with the database size and with
`snapshot_wal_bytes` (default 64 MiB of log between snapshots).

## What is stored

```text
writer/claim-<epoch>-<id>          one per attempt to become the writer
writer/base-<epoch>-<id>           where the writer of that claim took over, and whose history it continues
wal/<epoch>-<id>/<first>-<last>    log records first..=last
snapshot/<epoch>-<id>-<lsn>        manifest: the list of chunks of the data file at <lsn>
chunk/<sha256>                     1 MiB pieces of data file images, named by content
probe/<id>                         written and removed again when a writer opens
```

This is remote format 2. `<epoch>` and the sequence numbers are
zero-padded to 20 digits; `<id>` is 16 random hex digits chosen by each
claimant, so that no two processes ever write the same key. A base
object holds the log position the writer started from and the claim
(`<epoch>-<id>`) of the writer whose history it continues. Object stores
written in format 1 by earlier development versions of squad (keys
`writer/claim-<epoch>` without an id) are refused with an error that
says so; restore such a database with the version that wrote it and
open the result with a fresh remote.

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
| Commit group in `Sync` mode | 1 `PUT` + 1 `LIST` (claims from the writer's own on) |
| Shipping interval in `Async` mode (if there were commits) | 1 `PUT` + 1 `LIST` |
| Snapshot | 1 `PUT` per new chunk, 1 `PUT` for the manifest, 4 `LIST` of claims, 3 `LIST` for cleanup, 1 `DELETE` per obsolete object |
| Open | the claim (2 `LIST` + 1 `PUT`), the probe for conditional writes (2 `PUT` + 1 `DELETE`), a handful of `LIST`/`GET` and 1 `PUT` for the base; plus the download when restoring |

Objects larger than 16 MiB (chunks never are; a log object can be after
a long outage in `Async` mode) are uploaded with a multipart upload in
8 MiB parts.

Storage is the size of the data file plus the log since the last
snapshot. How much a snapshot uploads depends on where the changes are:
chunks are compared by content, so an append-mostly table re-uploads
only the chunks that grew, while updates scattered across the key space
touch most chunks and re-upload most of the file. Measured with
`cargo run --release -p squad-storage --example remote_bench`:
a 23.5 MiB database with 1% of its rows updated evenly across the key
space re-uploaded all 24 chunks.

Indicative numbers against a single-node Garage 2.5 on the same
machine (4 cores shared with other builds, so noisy; raw output in
`benchmarks/remote-sync-2026-11-14.txt`), one 120-byte row per commit,
local `fsync` on every commit:

| Mode | writers | commits/s | p50 | p99 | commits per upload |
| --- | ---: | ---: | ---: | ---: | ---: |
| local only | 1 | 6,811 | 0.12 ms | 0.46 ms | — |
| remote, `Async` (100 ms) | 1 | 7,093 | 0.12 ms | 0.43 ms | 788 |
| remote, `Sync` | 1 | 202 | 4.85 ms | 8.64 ms | 1.0 |
| local only | 8 | 24,672 | 0.31 ms | 1.09 ms | — |
| remote, `Async` (100 ms) | 8 | 21,294 | 0.24 ms | 4.06 ms | 2,598 |
| remote, `Sync` | 8 | 1,110 | 5.46 ms | 30.88 ms | 7.9 |
| local only | 32 | 38,958 | 0.47 ms | 4.33 ms | — |
| remote, `Async` (100 ms) | 32 | 52,064 | 0.47 ms | 4.05 ms | 6,680 |
| remote, `Sync` | 32 | 2,629 | 10.40 ms | 26.48 ms | 23.8 |

A `Sync` commit costs one `PUT` and one `LIST` against the store (about
4–5 ms against this Garage, which syncs its metadata on every `PUT`);
concurrent commits share both, so throughput grows with the number of
writers while latency stays around one or two round trips. `Async`
costs nothing per commit. Reproduce with

```sh
eval "$(scripts/garage-dev.sh start)"
cargo run --release -p squad-storage --example remote_sync_bench -- --seconds 5 --writers 1,8,32
```

`RemoteConfig::upload_concurrency` (default 1) lets a second upload
start while one is in flight. Against this single-node Garage that was
not faster (groups get smaller, the store does not speed up); it may
help against a store with high latency that serves concurrent requests
well. Each upload is still certified by a listing of the claims that
started after its `PUT`, so the guarantees do not change.

## One writer at a time

A database must have exactly one writer. Locally the `LOCK` file
ensures that. For the object store squad uses *epochs* and a claim
protocol that needs nothing from the store but `PUT`s that are atomic
and durable when they return, and `LIST`s that show every object whose
`PUT` has returned (read-after-write consistency for writes and
listings). AWS S3, Garage in its default consistency mode, MinIO and
local directories provide that. Conditional writes are **not** needed:
Garage, for one, accepts `If-None-Match: *` and overwrites anyway.

1. **Claim.** A process that opens the database as writer (or a
   replica that is promoted) lists the claims, picks the epoch *n* one
   above the highest it sees, writes `writer/claim-<n>-<id>` with a
   random id, and lists the claims again. If that listing shows any
   other claim for epoch *n* or higher, it removes its claim, waits a
   random moment and tries again above everything it saw. Otherwise it
   holds epoch *n*. Of two claimants for the same epoch, the one whose
   write took effect second sees the other in its listing and loses, so
   no epoch ever has two winners (both may lose and retry). The full
   argument is in the source: `claim_epoch` in
   `crates/squad-storage/src/remote.rs`.
2. **Start.** Only after winning does it read the remote state. It
   records where it starts in `writer/base-<n>-<id>`: the log position
   and the writer whose history it continues.
3. **Fence.** A writer acknowledges a commit in `Sync` mode only after
   the object holding the commit's log record is stored *and* a
   listing of the claims that started afterwards shows no claim above
   its own epoch. If the new writer's claim was written first, that
   listing sees it and the commit is **not** acknowledged; if it was
   written later, the new writer read the state after the record was
   stored and has it in its history. A writer that sees a newer claim
   stops uploading and fails all further writes with
   `remote storage: another writer took over`.
4. **One history.** Readers (restore, replicas, the next writer)
   follow the chain of base objects from the highest epoch back through
   the writers each one continues. A writer's records count only up to
   where its successor in the chain took over. Whatever a replaced
   writer uploads after that point is ignored, and so is everything of
   a writer that won a claim but recorded its start only after a later
   writer had read the state: it is not in the chain.

The result: starting a second writer on the same remote — deliberately,
to fail over, or by mistake — never mixes two histories, and never loses
a commit that was acknowledged in `Sync` mode.

When it opens, a writer also checks whether the store enforces
`If-None-Match: *` (it writes `probe/<id>` twice with the condition and
removes it) and reports the answer in `RemoteStats::conditional_writes`.
The answer is for information only; nothing depends on it.

A directory that cannot continue the remote history (it is behind, or
was left by a writer that has been replaced) is recognised *before*
step 1 and refused without claiming anything, so starting a stale copy
by mistake does not stop the writer that is running.

The same mechanism is what makes promoting a
[read replica](replication.md#promotion-and-failover) safe: the replica
claims the next epoch, applies everything the previous writer uploaded,
and continues from there.

Cleanup is safe against a takeover in the middle of it: a writer lists
the obsolete objects first, then checks for a newer claim, and deletes
only what it listed. An object of a newer writer can only be in that
listing if the newer writer claimed before it, and then the check stops
the cleanup.

Residual risks, stated plainly:

- The argument needs read-after-write consistent listings. A store
  whose listings may lag behind completed writes (some older
  S3-compatible services; a Garage cluster with `consistency_mode` set
  to `degraded` or `dangerous`) could let two writers win the same
  epoch. Readers then refuse the store (two bases for one epoch) rather
  than mix histories, but acknowledged commits of one of the two could
  be lost.
- Every claim, also a losing one, fences the current writer. Two
  processes that keep starting at the same instant can both lose a
  round; each retries after a random delay, and an open fails after 20
  lost rounds.
- In `Async` mode a replaced writer keeps accepting commits locally
  until its next upload attempt (at most one interval). Those commits
  are not in the remote history.

## Failure behaviour

| Failure | Effect |
| --- | --- |
| Upload fails transiently (connection error, timeout, 5xx, 429, 408, S3 `RequestTimeout`) | retried by the S3 client, up to 5 attempts, each after a random delay below a bound that doubles from 50 ms up to 5 s (full jitter); then as in the table of modes above. Records that were not uploaded are kept and sent again, under the same key, before anything newer. |
| Request refused (other 4xx, for example a wrong key or a missing bucket) | not retried; the commit (`Sync`) or the background upload (`Async`) fails with the service's message |
| Response to a successful upload is lost | the retry writes the same object again |
| The store ignores conditional writes (Garage) | no effect: fencing does not use them |
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
timing differs between runs) and pass; 60 run with every `cargo test`.
Further tests cover fencing of a replaced writer, refusal of stale
directories, cleanup (exactly one manifest, no unreferenced chunks, no
covered log objects), and an end-to-end run against the S3 emulator with
injected failures.

Fencing is tested against stores that enforce conditional writes and
stores that ignore them (the memory store and the S3 emulator both have
that mode): eight threads claiming at once with random request delays
never produce two winners for one epoch; three writers started
concurrently while committing leave exactly one that can commit, and
every acknowledged commit is restored; a writer that recorded its start
too late is left out of the history; buckets in the old format are
refused.

`crates/squad/tests/real_s3.rs` and the conformance suite of
`squad-object` run against a real S3 service when `SQUAD_TEST_S3_URL`
is set; they were run against Garage 2.5 (`scripts/garage-dev.sh`):
restore after losing the machine, a replica following its writer, and
two replicas promoted at the same instant while the old writer keeps
committing. See [the crate's README](../crates/squad-object/README.md)
for what was and was not verified of the S3 client.
