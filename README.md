# squad

**An embeddable database for Rust that behaves like PostgreSQL** — with
transactions for your application's state, columnar tables for its
events, and durable storage on local disk or in an S3-compatible bucket.

squad runs inside your process as a library. There is no server to
operate and no network hop between your code and your data: a point
lookup takes about a microsecond. SQL dialect, data types, error codes
and transaction semantics follow PostgreSQL 16, and unlike single-writer
embedded databases many connections can write at the same time.

```rust
use squad::{Config, Database};

let db = Database::open("data/", Config::default())?;
let conn = db.connect();

conn.batch_execute("
    CREATE TABLE users (id bigserial PRIMARY KEY, email text UNIQUE NOT NULL, name text);
    INSERT INTO users (email, name) VALUES ('ada@example.org', 'Ada');
")?;

let stmt = conn.prepare("SELECT name FROM users WHERE email = $1")?;
let name: String = stmt.query_one(&[&"ada@example.org"])?.get(0);
```

> **Status: 0.1, not yet used in production.** squad has an extensive
> test suite behind it (below), including crash simulation and tests
> against a real S3 service (Garage), but it is young. Try it next to
> PostgreSQL, not in place of your only copy of important data. What is
> missing is listed in [docs/roadmap.md](docs/roadmap.md); how to run it
> in containers is in [docs/deployment.md](docs/deployment.md).

## What you get

- **PostgreSQL's SQL.** squad embeds the PostgreSQL parser itself. Joins,
  subqueries, recursive CTEs, window functions, `INSERT … ON CONFLICT`,
  `RETURNING`, constraints and foreign keys, transactional DDL, several
  hundred built-in functions, `jsonb`, arrays, enums, and the same
  SQLSTATE error codes. → [compatibility](docs/compatibility.md)
- **Real concurrency.** Multi-version concurrency control with Read
  Committed, Repeatable Read and Serializable isolation, row locks,
  `SELECT … FOR UPDATE SKIP LOCKED`, deadlock detection. Readers never
  block writers. → [transactions](docs/transactions.md)
- **Durability you can check.** Write-ahead log with group commit,
  crash-atomic checkpoints, checksums on every page, online backup. The
  recovery path is tested by killing a simulated process at tens of
  thousands of random points. → [durability](docs/durability.md)
- **Analytics in the same database.** `CREATE TABLE … USING columnar`
  stores events in compressed column segments. Dashboard queries over
  millions of rows answer in milliseconds, in the same SQL and the same
  transactions as the rest of your data. → [analytics](docs/analytics.md)
- **Cloud-native storage.** Work in memory and on local disk, keep the
  durable copy in an S3-compatible object store such as AWS S3 or
  Garage, restore on a fresh machine. Writer fencing needs no
  conditional writes from the store. → [cloud storage](docs/cloud-storage.md)
- **Read replicas and failover.** Any number of replicas follow the
  writer through the object store, serve read-only queries and can be
  promoted; the old writer is fenced. → [replication](docs/replication.md)
- **Sharding.** `squad-cluster` spreads tenants over several databases,
  routes single-tenant statements to one shard and merges aggregations
  across all of them. → [sharding](docs/sharding.md)
- **The Postgres wire protocol, when you want it.** One call serves the
  embedded database to psql, pgAdmin, migration tools and services in
  other languages, with SCRAM, TLS and query cancellation. An application written for sqlx and PostgreSQL runs
  against it unchanged. → [server](docs/server.md)

## A taste

Transactional state and event analytics side by side:

```sql
CREATE TABLE projects (id int PRIMARY KEY, name text NOT NULL, plan text NOT NULL DEFAULT 'free');

CREATE TABLE events (
    ts          timestamp NOT NULL,
    project_id  int       NOT NULL,
    event       text      NOT NULL,
    user_id     bigint,
    duration_ms int
) USING columnar WITH (order_by = 'project_id, ts');

-- daily active users and p95 latency per paying project, last 30 days
SELECT p.name, d.day, d.users, d.p95
FROM projects p
JOIN (
    SELECT project_id,
           date_trunc('day', ts)                 AS day,
           approx_count_distinct(user_id)        AS users,
           approx_percentile(duration_ms, 0.95)  AS p95
    FROM events
    WHERE ts >= now() - interval '30 days'
    GROUP BY project_id, date_trunc('day', ts)
) d ON d.project_id = p.id
WHERE p.plan <> 'free'
ORDER BY p.name, d.day;
```

Serve the same database to other tools:

```rust
use squad_server::Serve;

let server = db.serve("127.0.0.1:5432")?;
// psql -h 127.0.0.1 -U squad squad
// sqlx::PgPool::connect(&server.url().unwrap()).await?
```

Keep the durable copy in a bucket:

```rust
use squad::{object::open_url, Config, Database, RemoteConfig, RemoteSync};

let store = open_url("s3://my-bucket/prod?region=eu-central-1")?;
let db = Database::open("cache/", Config {
    remote: Some(RemoteConfig::new(store, RemoteSync::Sync)),
    ..Config::default()
})?;
```

Follow it from another machine, and take over when the writer is gone:

```rust
use squad::{Database, ReplicaConfig};

let replica = Database::open_replica("replica/", ReplicaConfig::new(store))?;
replica.connect().query("SELECT count(*) FROM events", &[])?;   // read-only
replica.promote()?;                                              // now the writer
```

Spread tenants over several databases:

```rust
use squad_cluster::{Cluster, ClusterConfig, ShardSpec};

let cluster = Cluster::open(ClusterConfig::new(vec![
    ShardSpec::local("data/shard-0"),
    ShardSpec::remote("postgres://squad@10.0.0.2:5432/squad"),
]))?;
let db = cluster.connect();
db.batch_execute("SELECT squad_distribute('events', 'project_id')")?;
db.query("SELECT event, count(*) FROM events GROUP BY event", &[])?;  // all shards, merged
```

## Performance

Measured on a 4-vCPU virtual machine; all systems force every commit to
disk. Full results, commands and raw output are in
[docs/benchmarks.md](docs/benchmarks.md), including the goals squad does
not meet.

| | squad | SQLite | PostgreSQL 16 |
| --- | ---: | ---: | ---: |
| Point select by primary key, p50 | 1.5 µs | 1.5 µs | 50 µs |
| Insert one row with durable commit, p50 | 124 µs | 123 µs | 159 µs |
| Two-row transactions, 4 writer threads | 9,490 /s | 5,050 /s | 6,960 /s |
| pgbench TPC-B, 16 clients (server mode) | 7,640 tps | — | 6,190 tps |

| Analytics on 2 million events | Row table | Columnar table |
| --- | ---: | ---: |
| Events per day, one project, 30 days | 3.5 s | 0.7 ms |
| Unique users per day, one project | 3.5 s | 1.2 ms |
| p50/p95/p99 duration per event, all data | 4.5 s | 5.8 ms |

A replica that polls every 10 ms shows a commit 22 ms after the writer
acknowledged it (p50) and applies a backlog at 279,000 rows per second;
promotion takes 0.1 s. Four shards on the same machine load 422,000
events per second and cut a full scan from 3.1 s to 0.9 s.

Not yet good: lookups through a secondary index that return many rows,
read throughput scaling across cores, and large scans of ordinary (row)
tables.

## How it is tested

squad's reference is PostgreSQL 16 itself. → [testing](docs/testing.md)

- **SQL regression suite**: scripts whose expected output was produced
  by PostgreSQL; squad must print the same, byte for byte.
- **sqllogictest**: 99.80 % of the public corpus (5.68 million records);
  squad and PostgreSQL disagree on 6 records.
- **Differential fuzzing**: about 1.3 million random statements run
  against squad and PostgreSQL and compared.
- **Real clients**: psql, pgbench, pg_dump output, tokio-postgres and
  sqlx in the test suite.
- **Crash simulation**: a simulated file system kills the process at
  random writes, tears unsynced data and injects I/O errors; after
  recovery every acknowledged commit must be there. Tens of thousands of
  scenarios at the storage and SQL level.
- **Concurrency tests** for every isolation level, and equivalence tests
  between columnar and ordinary tables.
- **Replication tests** with failing object-store requests, crashing
  replicas and chains of promotions; **cluster tests** that run a random
  workload on 1 to 7 shards and require the answers of a single
  database.

## Installing

```toml
[dependencies]
squad = "0.1"                                         # add features = ["s3"] for S3 storage
squad-server = "0.1"   # optional: wire protocol and the `squad` binary
squad-cluster = "0.1"  # optional: sharding
```

Building needs a C compiler, because the PostgreSQL parser is compiled
from source. The command-line tool:

```sh
cargo install --path crates/squad-server
squad serve ./data          # serve a database directory on 127.0.0.1:5432
squad shell ./data          # interactive SQL prompt
squad backup ./data ./copy
squad serve ./replica --replica-of s3://my-bucket/prod   # read replica
```

## Documentation

| | |
| --- | --- |
| [Embedding](docs/embedding.md) | the Rust API: connections, statements, types, transactions, errors |
| [Server](docs/server.md) | wire protocol, the `squad` command, sqlx and other clients |
| [Analytics](docs/analytics.md) | columnar tables: how they work, what is fast, limits |
| [Cloud storage](docs/cloud-storage.md) | object stores, S3, remote durability, restore |
| [Replication](docs/replication.md) | read replicas, lag, promotion and fencing, failover runbook |
| [Deployment](docs/deployment.md) | running on Docker Swarm with Garage: one writer, replicas, secrets, health checks |
| [Sharding](docs/sharding.md) | `squad-cluster`: distributed and reference tables, routing, limits |
| [Transactions](docs/transactions.md) | isolation levels, locking, differences from PostgreSQL |
| [Durability](docs/durability.md) | on-disk layout, commit, checkpoints, recovery, backup |
| [Compatibility](docs/compatibility.md) | what is supported, what is not, every known difference |
| [Architecture](docs/architecture.md) | the layers and the reasons behind them |
| [Testing](docs/testing.md) | every test suite and what it proves |
| [Benchmarks](docs/benchmarks.md) | measurements, method, raw data |
| [Roadmap](docs/roadmap.md) | status, known limits, what comes next |

## Repository layout

| Crate | Content |
| --- | --- |
| `squad` | SQL engine and embedding API |
| `squad-server` | wire-protocol server and the `squad` binary |
| `squad-storage` | durable key-value engine: B+tree, buffer pool, WAL, recovery, remote tiering, replicas |
| `squad-kv` | transactions: MVCC, row locks, isolation levels |
| `squad-cluster` | sharding: routing and scatter-gather over several databases |
| `squad-columnar` | column segment format and vectorized aggregation |
| `squad-object` | object stores: memory, local directory, S3 |
| `squad-sqllogictest`, `squad-difftest`, `squad-bench` | test and benchmark tools |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version: PostgreSQL is
the reference, every change needs a test that fails without it, and
features that are missing must say so instead of being ignored.

## License

MIT. See [LICENSE](LICENSE).
