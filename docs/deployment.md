# Deployment: services on Docker Swarm with Garage

This chapter describes how to run a service that embeds squad (or the
`squad` server) on Docker Swarm, with the durable copy of the database in
[Garage](https://garagehq.deuxfleurs.fr), a self-hosted S3-compatible
object store. The local disk of the node is a cache: a task can be
rescheduled to any node and rebuilds its working copy from the bucket.

Read [cloud storage](cloud-storage.md) and [replication](replication.md)
first; this chapter applies them and does not repeat the guarantees.

```text
          Swarm node A                      Swarm node B, C …
 ┌──────────────────────────────┐   ┌──────────────────────────────┐
 │ service "ingest" (writer)    │   │ service "query" (replicas)   │
 │ replicas: 1                  │   │ replicas: N                  │
 │ ┌──────────────┐  ┌────────┐ │   │ ┌──────────────┐  ┌────────┐ │
 │ │ your service │──│ volume │ │   │ │ your service │──│ volume │ │
 │ │ + squad      │  │ (cache)│ │   │ │ + squad      │  │ (cache)│ │
 │ └──────┬───────┘  └────────┘ │   │ └──────▲───────┘  └────────┘ │
 └────────┼─────────────────────┘   └────────┼─────────────────────┘
          │ log objects, snapshots,          │ poll, download
          │ column segments                  │
 ┌────────▼──────────────────────────────────┴─────────────────────┐
 │ Garage: bucket "squad", one key prefix per database             │
 └──────────────────────────────────────────────────────────────────┘
```

## Garage

- **Consistency.** squad's fencing needs `PUT`s that are durable when
  they return and `LIST`s that show every completed `PUT`
  (read-after-write consistency). Garage provides that in its default
  `consistency_mode = "consistent"`. Do **not** run a Garage cluster
  that holds squad data with `consistency_mode` set to `degraded` or
  `dangerous`: listings may then miss completed writes, and two writers
  could win the same epoch (see
  [one writer at a time](cloud-storage.md#one-writer-at-a-time)).
- **Conditional writes.** Garage accepts `If-None-Match: *` and
  overwrites anyway. squad does not rely on it; a writer reports what it
  found in `RemoteStats::conditional_writes`, which is `Some(false)` on
  Garage. That is expected.
- **Durability of the bucket.** squad's `Sync` mode makes a commit as
  durable as one `PUT` to Garage. Use `replication_factor = 3` (or
  2 with care) on separate nodes or zones in production; a single-node
  Garage is fine for development only.
- **One prefix per database.** Each database needs a key prefix of its
  own, e.g. `s3://squad/errors/prod` and `s3://squad/analytics/prod`.
  Two databases must never share a prefix. Column segments of columnar
  tables are stored under the same prefix.
- **Access key.** Give each service its own key with read and write
  access to the bucket (`garage bucket allow --read --write`). Replicas
  only need read access, except when they are promoted.

The S3 URL for Garage:

```text
s3://<bucket>/<prefix>?endpoint=http://garage:3900&region=garage&path_style=true
```

`region` must match `s3_region` in `garage.toml`. Credentials go in the
environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) or in the URL
(`access_key`, `secret_key`); see [secrets](#secrets) for how to keep
them out of the service definition.

## Exactly one writer per database

A database has one writer at a time. squad enforces this in the object
store: a writer that starts claims the next *epoch*, and the previous
writer is fenced, i.e. its next commit fails with
`remote storage: another writer took over`. That makes a second writer
harmless for the data, but every takeover fails the in-flight commits of
the old writer. The Swarm service of the writer should therefore never
run two tasks at once:

```yaml
services:
  ingest:
    image: registry.example.org/ingest:1.4.2
    deploy:
      replicas: 1
      update_config:
        order: stop-first        # the old task stops before the new one starts
        failure_action: rollback
      rollback_config:
        order: stop-first
      restart_policy:
        condition: any
        delay: 5s
    stop_grace_period: 60s       # time for the final upload on shutdown
    volumes:
      - ingest-cache:/var/lib/ingest
    secrets:
      - garage_access_key_id
      - garage_secret_access_key
    environment:
      SQUAD_URL: "s3://squad/analytics/prod?endpoint=http://garage:3900&region=garage&path_style=true"

volumes:
  ingest-cache:
    driver: local

secrets:
  garage_access_key_id:
    external: true
  garage_secret_access_key:
    external: true
```

- `replicas: 1` with `order: stop-first` (for updates and rollbacks)
  means Swarm never runs the old and the new task at the same time. With
  `start-first`, the new task would claim the database while the old one
  still serves requests, and the old task's commits would fail until it
  is stopped.
- When a node fails, Swarm starts the task on another node. Swarm may do
  so before the old node is really gone (a network partition). The new
  task claims the next epoch, and the old one is fenced at its next
  commit: in `Sync` mode nothing it acknowledged is lost, and nothing it
  commits afterwards is acknowledged.
- On SIGTERM your service should stop taking requests and drop its
  `Database` handles. Closing the database uploads whatever is not yet
  in the bucket (in `Async` mode that can be up to one interval of
  commits) and stops background work, including columnar compaction.
  `stop_grace_period` must be long enough for that; Swarm's default of
  10 s is usually enough for `Sync` mode, where nothing is pending.

## The local volume is a cache

The directory passed to `Database::open` holds the working copy: data
file, write-ahead log, and a cache of column segments. Use a local
volume on each node (named volume, `driver: local`), not a network
file system.

When a task starts on a node:

| Local volume | What happens |
| --- | --- |
| empty (first start on this node) | the database is downloaded from the bucket: newest snapshot, then the log written since |
| left by this task's previous run on this node, still current | opened as is; no download |
| left by an earlier writer that has since been replaced (the task ran here before, moved away, and came back) | refused, unless `RemoteConfig::restore` is set, which replaces it with the state in the bucket |

On Swarm, where a task can come back to a node it ran on before, set
`restore: true`:

```rust
use squad::{object, Config, Database, RemoteConfig, RemoteSync};

let store = object::open_url(&std::env::var("SQUAD_URL")?)?;
let db = Database::open("/var/lib/ingest/db", Config {
    remote: Some(RemoteConfig { restore: true, ..RemoteConfig::new(store, RemoteSync::Sync) }),
    ..Config::default()
})?;
```

Losing the volume loses nothing that is in the bucket. The start-up time
after a move is the download of the newest snapshot plus the replay of
the log since; a new snapshot is uploaded after every
`snapshot_wal_bytes` (default 64 MiB) of log. Column segments are not
downloaded at start; they are read in ranges as queries need them and
kept in an in-memory cache (`ColumnarConfig::cache_bytes`).

## Remote sync modes and their latency

| | `RemoteSync::Sync` | `RemoteSync::Async { interval }` |
| --- | --- | --- |
| A commit returns after | local `fsync` and the upload of its log record, plus one listing | local `fsync` |
| Lost when the node and its volume are lost | nothing acknowledged | commits of up to about one `interval` |
| Store unreachable | commits fail until it is back | commits continue; the backlog is uploaded later |

Measured against a single-node Garage on the same 4-vCPU machine (one
small row per commit, local `fsync` on every commit; from
[benchmarks](benchmarks.md#remote-storage),
`benchmarks/remote-sync-2026-11-14.txt`):

| Writers | Mode | Commits/s | p50 | p99 |
| ---: | --- | ---: | ---: | ---: |
| 1 | `Async` | 7,400 | 0.12 ms | 0.34 ms |
| 1 | `Sync` | 120 | 7.7 ms | 19 ms |
| 8 | `Sync` | 930 | 8.3 ms | 18 ms |
| 32 | `Sync` | 1,900 | 13.7 ms | 38 ms |

In `Sync` mode, concurrent commits share one upload, so throughput grows
with the number of writers while each commit waits about one round trip
to Garage. Across a network, add the network latency to every commit.

For ingestion this means: **batch**. An API that accepts events should
collect them for a few milliseconds and commit them together (for
example 1,000 events per `Connection::append` call, see
[analytics](analytics.md)); one durable round trip then covers the whole
batch. Without a remote, batches of 1,000 events through
`Connection::append` reach about 500,000 events per second with durable
local commits on that machine; in `Sync` mode each batch also waits for
one round trip to Garage, so throughput then depends on how many
batches are in flight at once. That combination has not been measured
yet. Use `Async` only where
losing up to one interval of acknowledged writes when a node dies is
acceptable.

## Read replicas

Read replicas run as a second Swarm service with any number of tasks.
Each task has its own local volume and follows the writer through the
bucket; there is no network connection between writer and replicas.

```rust
use std::time::Duration;
use squad::{object, Database, ReplicaConfig};

let store = object::open_url(&std::env::var("SQUAD_URL")?)?;
let replica = Database::open_replica("/var/lib/query/db", ReplicaConfig {
    poll_interval: Duration::from_millis(200),
    ..ReplicaConfig::new(store)
})?;
```

- Replicas are read-only (writes fail with `25006`) and asynchronous:
  a commit becomes visible on a replica one poll interval plus one
  download after the writer acknowledged it. `replication_status()`
  reports the lag.
- Replicas can use `deploy.replicas: N` and `order: start-first`; they
  never claim anything unless promoted.
- A replica can serve reads of columnar tables; see
  [replication](replication.md) for what happens when the writer's
  compaction replaces a segment a replica query is reading.

## Failover

There are two ways to replace a lost writer. Use one of them, not both:

1. **Let Swarm reschedule the writer** (simplest). The new task
   restores from the bucket on its node and claims the next epoch.
   Recovery time is the download of the newest snapshot plus log replay.
2. **Promote a replica** (`Database::promote`, or `SELECT
   squad_promote()`), which is faster because the replica already holds
   the data. Then scale the writer service to 0 first, or the next
   start of its task would take the database back and fence the
   promoted replica.

Promotion is always a manual or orchestrated decision: squad has no
leader election.

## Secrets

Keep S3 credentials in Docker secrets, not in environment variables of
the service definition or in command lines (both are visible in
`docker service inspect` or the process list). Swarm mounts secrets as
files under `/run/secrets/`.

- **Services that embed squad** read the files at start and pass the
  values on, either in the URL (`access_key`, `secret_key`) or as
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the process environment
  before calling `object::open_url`:

  ```rust
  fn secret(name: &str) -> std::io::Result<String> {
      Ok(std::fs::read_to_string(format!("/run/secrets/{name}"))?.trim().to_string())
  }
  let url = format!(
      "{}&access_key={}&secret_key={}",
      std::env::var("SQUAD_URL")?,
      secret("garage_access_key_id")?,
      secret("garage_secret_access_key")?,
  );
  let store = squad::object::open_url(&url)?;
  ```

  Errors of an opened store do not repeat its URL, but `open_url`
  quotes a URL it cannot parse at all (no `scheme://`); do not log
  such errors verbatim.
- **The `squad` server image** reads `AWS_ACCESS_KEY_ID_FILE`,
  `AWS_SECRET_ACCESS_KEY_FILE`, `SQUAD_PASSWORD_FILE` and the URL from
  `--remote-file`/`--replica-of-file`; see [server](server.md).

## Health checks

A Swarm health check should tell whether the task can do its job. For a
service that embeds squad, expose an HTTP endpoint that

1. runs `SELECT 1` on a connection, and
2. for the writer, reads `Database::remote_status()`: `fenced` is true
   once another writer has taken over; `local_lsn - shipped_lsn` is how
   far the bucket is behind (always 0 after a `Sync` commit returns,
   up to one interval in `Async` mode, growing while Garage is
   unreachable);
3. for a replica, reads `Database::replication_status()`
   (`since_last_poll`, `since_last_apply`, `remote_lsn - applied_lsn`).

Be careful with what an unhealthy writer leads to. Swarm restarts an
unhealthy task, and the restarted task claims the database again. If the
writer was fenced because a replica was promoted on purpose, a restart
takes the database back from it. Report `fenced` in your monitoring,
but make it fail the health check only if rescheduling the writer
(failover method 1 above) is how you replace writers.

For the `squad` server image, the built-in `HEALTHCHECK` runs
`squad ping`, which logs in and runs `SELECT 1`.

## Checklist

- Garage: `consistency_mode = "consistent"`, `replication_factor` ≥ 2
  in production, one key prefix per database.
- Writer service: `replicas: 1`, `update_config.order: stop-first`,
  `rollback_config.order: stop-first`, a local volume, `restore: true`,
  `stop_grace_period` long enough for the final upload, `Database`
  dropped on SIGTERM.
- `RemoteSync::Sync` for data that must survive the loss of a node;
  ingestion in batches.
- S3 credentials from Docker secrets.
- A health endpoint that checks `SELECT 1` and the remote or
  replication status.

## Limits

- One writer per database. Write throughput of one database is bounded
  by one node; spread tenants over several databases
  ([sharding](sharding.md)) to go beyond it.
- No automatic failover or leader election.
- Replicas are asynchronous; there are no synchronous replicas.
- A cold start on a new node downloads the newest snapshot of the row
  store; for large databases that takes minutes. Column segments are
  fetched lazily, so the first queries over cold data are slower
  (tens to hundreds of milliseconds per query against Garage in the
  benchmark).
- The bucket is the only durable copy. Replicate or back up the Garage
  cluster if losing it would be unacceptable.
