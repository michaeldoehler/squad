# Replication: read replicas and failover

A squad database that keeps its durable copy in an object store (see
[cloud storage](cloud-storage.md)) can be followed by any number of
**read replicas**. A replica is a second squad database — in another
process, on another machine — that holds a complete local copy, applies
every commit the writer ships, and answers read-only queries from memory
and local disk at the same speed as the writer does. If the writer is
lost, a replica is **promoted** and becomes the writer.

There is no connection between the writer and its replicas. They meet in
the object store: the writer uploads its log there, replicas download it.
The writer does not know how many replicas exist, is never slowed down by
one, and needs no open port.

```text
   application (read/write)              application (read-only)
            │                                      │
   ┌────────▼────────┐                    ┌────────▼────────┐
   │  squad: writer  │                    │ squad: replica  │  × N
   │  memory + disk  │                    │  memory + disk  │
   └────────┬────────┘                    └────────▲────────┘
            │ log objects, snapshots               │ poll, download, apply
            │                                      │
   ┌────────▼──────────────────────────────────────┴────────┐
   │ object store: file:// or s3://                          │
   │ wal/…   snapshot/…   chunk/…   writer/claim-…, base-…   │
   └─────────────────────────────────────────────────────────┘
```

## Using it

The writer is an ordinary database with a remote:

```rust
use squad::{object, Config, Database, RemoteConfig, RemoteSync};

let store = object::open_url("s3://my-bucket/prod/app?region=eu-central-1")?;
let db = Database::open("data/", Config {
    remote: Some(RemoteConfig::new(store, RemoteSync::Sync)),
    ..Config::default()
})?;
```

A replica names the same store:

```rust
use std::time::Duration;
use squad::{object, Database, ReplicaConfig};

let store = object::open_url("s3://my-bucket/prod/app?region=eu-central-1")?;
let replica = Database::open_replica("replica-data/", ReplicaConfig {
    poll_interval: Duration::from_millis(200),
    ..ReplicaConfig::new(store)
})?;

let conn = replica.connect();
let n: i64 = conn.query_one("SELECT count(*) FROM events", &[])?.get(0);
```

`open_replica` fills an empty directory from the newest snapshot and the
log after it, and returns once the replica has applied everything that
was in the store at that moment. From then on a background thread asks
the store for new log objects every `poll_interval` (default: one
second) and applies them. Opening the same directory again later
continues from the local copy; only what is missing is downloaded.

With the command-line tool:

```sh
# writer
squad serve ./data --remote 's3://my-bucket/prod/app?region=eu-central-1' --remote-sync sync

# replica, on another machine
squad serve ./replica --replica-of 's3://my-bucket/prod/app?region=eu-central-1' \
      --poll-interval 200 --listen 0.0.0.0:5432
```

`squad shell ./replica --replica-of <url>` opens a SQL prompt on a
replica the same way.

`ReplicaConfig`:

| Field | Default | Meaning |
| --- | --- | --- |
| `store` | — | the object store the writer ships to |
| `poll_interval` | 1 s | how often the store is asked for new commits |
| `promoted_sync` | `Sync` | how this node ships its own log after promotion |
| `snapshot_wal_bytes` | 64 MiB | snapshot interval after promotion |
| `sync_mode`, `cache_size`, `checkpoint_wal_bytes` | as in `Config` | local durability and memory |

## What a replica does and does not do

**Reads.** Every `SELECT` works, with all isolation levels, prepared
statements, views, joins between ordinary and columnar tables, and the
system catalogs. `pg_is_in_recovery()` returns `true`, and
`SHOW transaction_read_only` reports `on`, as on a PostgreSQL standby.

**No writes.** Every statement that would change the database fails with
SQLSTATE `25006` (`read_only_sql_transaction`), the code PostgreSQL uses
on a standby, and leaves the session usable:

```text
INSERT (also … ON CONFLICT), UPDATE, DELETE, COPY … FROM, TRUNCATE
CREATE, ALTER, DROP, CREATE INDEX
VACUUM, compaction of columnar tables
nextval(), setval()
SELECT … FOR UPDATE / FOR SHARE
BEGIN READ WRITE, SET TRANSACTION READ WRITE
```

Session settings (`SET search_path`, `SET timezone`, …) work, because
they are not stored in the database.

**Schema changes arrive like data.** When the writer creates a table,
adds a column or drops an index, the replica switches to the new schema
at the same instant the commit that made the change becomes visible.
Statements prepared on the replica before the change are planned again
on their next use.

## Consistency

A replica shows **a state the writer was in**: all commits up to some
point, in the writer's commit order, and nothing else.

- **Transactions appear whole.** A commit is one log record; the replica
  writes it to its own engine in one atomic step and only then lets new
  snapshots see it. A reader never sees half a transaction, and never
  sees a row without the index entries or the schema change that
  belong to it.
- **Snapshots are stable.** A `REPEATABLE READ` or `SERIALIZABLE`
  transaction on a replica keeps reading the state it started on while
  the replica goes on applying commits, including commits by which the
  writer vacuumed row versions that snapshot still reads: the replica
  holds such removals back until no local snapshot needs them. Under
  `READ COMMITTED` each statement sees the newest state applied.
- **Replicas are behind.** A commit acknowledged by the writer becomes
  visible on a replica after it was uploaded and the replica polled. A
  read on a replica directly after a write on the writer may not see
  that write. Applications that need to read their own writes read from
  the writer, or wait until the replica's `applied_lsn` has reached the
  position they need.
- **Different replicas may be at different points**, but each moves
  forward only. A replica never shows a commit and later a state
  without it — also not across a failover (see *Promotion* below).

How far behind a replica is depends on three delays that add up:

| Delay | `RemoteSync::Sync` writer | `RemoteSync::Async { interval }` writer |
| --- | --- | --- |
| commit → log object in the store | none: the commit is acknowledged only after the upload | up to `interval` |
| upload → next poll | up to `poll_interval` | up to `poll_interval` |
| download and apply | one `GET` per log object plus a local write | same |

### Queries that are cancelled

In two situations a replica cancels a running transaction with SQLSTATE
`40001` and a message starting with `canceling statement due to
conflict with recovery`. The transaction can be run again at once.

1. **The replica replaced its local copy.** The writer deletes log
   objects once a snapshot covers them (it keeps them one snapshot
   longer than a restore needs, for replicas that are reading them). A
   replica that was stopped or cut off for longer than that finds a gap
   between its own position and the log. It then downloads the newest
   snapshot into a staging directory — still answering queries from the
   old copy — swaps the two, and continues. Transactions that were
   reading the old copy are cancelled; `rebases` in the status counts
   how often this happened. The same happens to snapshots that kept
   more than about a million removed row versions waiting.
2. **A columnar segment is gone.** The writer deletes a segment of a
   [columnar table](analytics.md) as soon as a compaction or `DELETE`
   has replaced it. A replica that has not yet applied that commit may
   still plan a query against the old segment list and find the object
   missing. Segments a replica has already opened are held in its
   segment cache and stay readable; the window is the replication lag
   after a compaction.

## Monitoring

```rust
let status = replica.replication_status().unwrap();
println!("applied {} of {}", status.applied_lsn, status.remote_lsn);
```

```sql
SELECT squad_replication_status();
-- {"role": "replica", "epoch": 3, "applied_lsn": 48211, "remote_lsn": 48211,
--  "seconds_since_last_poll": 0.412, "seconds_since_last_apply": 2.87,
--  "rebases": 0, "waiting_for_takeover": false, "last_error": null}
```

| Field | Meaning |
| --- | --- |
| `role` | `replica`, or `writer` once promoted |
| `epoch` | the writer generation whose commits were applied last |
| `applied_lsn` | log position of the last commit applied locally |
| `remote_lsn` | highest log position found in the store at the last poll |
| `seconds_since_last_poll` | time since the store was last read successfully |
| `seconds_since_last_apply` | time since a commit was last applied (large on an idle database) |
| `rebases` | how often the local copy was replaced by a newer snapshot |
| `waiting_for_takeover` | a new writer is starting; see below |
| `last_error` | why the last poll failed, if it did |

The function returns `NULL` on a database that was not opened as a
replica. What to alert on:

- `seconds_since_last_poll` growing: the replica cannot reach the store.
  `last_error` says why. It keeps answering queries from the state it
  has and catches up when the store is reachable again.
- `remote_lsn - applied_lsn` staying above zero: the replica cannot keep
  up with the writer.
- `rebases` increasing during normal operation: the replica is
  regularly further behind than the log the writer retains.

`Database::replication_catch_up()` polls immediately and returns the
resulting status; tests use it to wait for a known state.

## Promotion and failover

```rust
replica.promote()?;        // or, from any client:  SELECT squad_promote();
```

Promotion turns a replica into the writer, in this order:

1. It **claims the database** in the object store: it writes a claim
   for the next epoch under a name of its own and wins only if a
   listing afterwards shows no competing claim (see
   [one writer at a time](cloud-storage.md#one-writer-at-a-time); no
   conditional writes are needed, so this works on Garage). From this
   moment the old writer is **fenced**: its commit in flight is not
   acknowledged (`Sync` mode), and it fails all further writes with
   `remote storage: another writer took over`.
2. It **applies everything the old writer uploaded**, to the last
   record.
3. It records its starting position and the writer it continues
   (`writer/base-<n>-<id>`), starts
   shipping its own log, and accepts writes. Sessions that were open
   before the promotion can write without reconnecting; sequences
   continue after the values the old writer handed out.

`promote()` returns when step 3 is done — about 0.1 s on a local
directory, a few requests' latency on S3. If it fails (the store is
unreachable, another node claimed the database at the same time) the
node stays a replica and promotion can be tried again.

**What is guaranteed:**

- With a `Sync` writer, **no acknowledged commit is lost** by a
  failover: every acknowledged commit is in the store, and the promoted
  node applies all of it before it writes.
- **Histories never mix.** Whatever a fenced writer still manages to
  upload after the takeover point is ignored by every reader.
- **Other replicas follow the new writer** without being told. While a
  takeover is in progress (a claim exists, the new writer has not
  recorded its starting position yet) they hold back the old writer's
  newest records, because those may end up after the takeover point;
  `waiting_for_takeover` is `true` for that moment.

**What is not:**

- With an `Async` writer, commits of up to one upload interval that
  were acknowledged but not yet shipped when the writer died are **not**
  in the store and therefore not on the promoted node. They still exist
  in the old writer's directory, if that survives, but they are not part
  of the database's history any more.
- squad does not decide *when* to fail over. Nothing watches the writer
  and promotes a replica automatically; an operator or the
  application's orchestration does. Promoting while the old writer is
  still alive is safe (it is fenced), so a false alarm costs a writer
  restart, not data.
- Clients are not redirected. Applications connect to the promoted node
  under whatever name or address their environment provides.

### Runbook

The writer `A` is gone or unreachable; `B` is a replica.

1. Check `B`: `SELECT squad_replication_status()` — `last_error` should
   be `null` (it can reach the store).
2. Promote it: `SELECT squad_promote();` on `B`. It returns `t`;
   `pg_is_in_recovery()` is now `f` and `role` is `writer`.
3. Point the application's writes at `B`.
4. Make sure `A` does not serve stale reads. If `A` is still running it
   refuses writes, but it would answer reads from its last state.
   Stop it.
5. Bring `A` back as a replica of the new writer:

   ```sh
   squad serve ./data --replica-of 's3://my-bucket/prod/app?region=eu-central-1'
   ```

   If `A` had shipped everything it wrote, its directory is an earlier
   state of the same history and is simply brought up to date. If it
   holds log records that never reached the store (`Async` mode, or a
   commit that was in flight and never acknowledged when it died), the
   directory is refused; move it away and start `A` with an empty one.
6. When `B` is restarted later, start it as a writer on the same
   directory: `squad serve ./replica --remote <url>` (or
   `Database::open` with `Config::remote`). Starting it with
   `--replica-of` would make it a replica again — of itself, if no
   other writer exists.

Starting `A` as a *writer* again by mistake (`--remote` instead of
`--replica-of`) is refused before anything is claimed: its directory
does not continue the history in the store. The running writer and its
replicas are not disturbed.

## Topologies

- **Read scaling.** One writer, several replicas behind a load balancer
  for dashboards and reports. Each replica is a complete copy and
  answers from local memory; adding one costs the writer nothing.
- **Standby.** One replica in another zone, promoted by hand or by
  orchestration when the writer's machine is lost. With a `Sync` writer
  this loses no acknowledged commit.
- **Embedded read replica.** A service that embeds squad opens a
  replica of another service's database and reads it in-process, at
  in-process speed, without that service being involved.
- Combined with [sharding](sharding.md): every shard is a database of
  its own and can have its own replicas.

## Limits

- **One writer.** Replicas do not accept writes and do not forward
  them. There is no multi-writer or multi-region write setup.
- **Promotion is manual**: there is no failure detector and no
  automatic election.
- **Replicas are asynchronous.** A `Sync` writer waits for the object
  store, never for a replica; there is no mode in which a commit waits
  until a replica has applied it. Lag is bounded by the upload interval
  plus the poll interval plus the time to apply, as long as the replica
  keeps up.
- **Every replica holds the whole database** on local disk (segments
  of columnar tables excepted: those are read from the object store on
  first use and cached in memory, as on the writer). There is no
  partial replica and no replica that reads pages from the object store
  on demand.
- **Each poll is a handful of requests** against the object store
  (about five, mostly `LIST`), also when nothing changed. At one poll
  per second that is some twenty thousand requests per hour per
  replica; choose `poll_interval` with the store's request pricing in
  mind.
- **A replica cannot be cascaded** from another replica; all replicas
  read the writer's store.
- **Columnar queries can be cancelled** in the window after a
  compaction, as described above.
- The fencing that makes promotion safe relies on the store's listings
  showing every completed write (read-after-write consistency, which
  AWS S3 and Garage in its default mode provide); see the residual risks
  listed under [one writer at a time](cloud-storage.md#one-writer-at-a-time).
- If two promotions race and the node that applied the old writer's
  last records loses, it stays a replica and has applied records that
  the winner's history may not contain (records the old writer
  uploaded but never got acknowledged). Its next poll notices that its
  copy is not part of the history and replaces it with a fresh
  snapshot; reads in between can see those unacknowledged commits.

## How it is tested

- `crates/squad-storage/tests/replica.rs` runs writer and replicas on
  the simulated file system and an in-memory object store: start from
  snapshot and log, following, falling behind past the retained log,
  restart and crash of a replica, promotion with fencing of the old
  writer, late uploads of a fenced writer, a stale writer started by
  mistake. Two randomized tests run hundreds of seeds: replicas under
  an object store that fails one read request in six (whatever the
  replica holds must be a state the writer went through, and it must
  reach the writer's state once the faults stop), and chains of six
  promotions in a row with an observer replica following through all
  of them. The promotion and fencing tests run against a store that
  enforces conditional writes and against one that ignores them.
- `crates/squad/tests/real_s3.rs` runs against a real S3 service when
  `SQUAD_TEST_S3_URL` is set (tested with Garage 2.5): a replica
  following its writer, and two replicas promoted at the same instant
  while the old writer commits — exactly one node can commit afterwards
  and no acknowledged commit is lost.
- `crates/squad/tests/replica.rs` tests the SQL level: data and schema
  changes arriving, every kind of write being refused with `25006`,
  stable snapshots while the writer updates, deletes and vacuums,
  money transfers on the writer while replica readers check that the
  total never changes, columnar tables, replacement of the local copy,
  restart, promotion with open sessions and sequences, and a
  randomized workload of concurrent writers with a replica polling
  every millisecond, after which both databases must hold the same
  rows — then the replica is promoted and the test repeats with the
  roles moved on.
- `crates/squad-server/tests/cli.rs` runs the `squad` binary: a writer
  and a replica as separate processes, writes refused over the simple
  and extended protocol, the writer killed, the replica promoted by a
  client, the old writer's directory rejoining as a replica.

## Measurements

`cargo run --release -p squad --example replication_bench`, on a
4-vCPU virtual machine with a local directory as the object store (so
without network latency); raw output in
[`benchmarks/replication-2026-11-10.txt`](../benchmarks/replication-2026-11-10.txt).

| | |
| --- | ---: |
| Replica applying a backlog of 20,003 commits (220,000 rows) | 0.79 s — 25,400 commits/s, 279,000 rows/s |
| Opening a replica from a snapshot of 220,000 rows | 0.27 s |
| Commit visible on replica, 1 ms poll interval, p50 / p99 | 13 ms / 37 ms |
| Commit visible on replica, 10 ms poll interval, p50 / p99 | 22 ms / 37 ms |
| Commit visible on replica, 100 ms poll interval, p50 / p99 | 114 ms / 133 ms |
| Promotion until the first commit of the new writer | 0.10 s |

The writer in this run shipped each commit synchronously (about 1,200
single-row commits per second to a local directory, each one a file
created and synced). A replica applies commits about twenty times
faster than such a writer produces them. The floor of roughly 10 ms
under the lag is the cost of one poll against a directory that holds
tens of thousands of log files plus the local sync after applying; on
S3 a poll is a few `LIST` requests that return only keys after the last
one applied, and the lag is the poll interval plus the store's request
latency.
