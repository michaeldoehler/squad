# Sharding: one logical database over several squad databases

A single squad database uses one machine: one write-ahead log, one page
cache, one disk. The `squad-cluster` crate spreads a database over
several squad databases, called *shards*, and gives the application one
connection that behaves like a connection to a single database for the
statements a multi-tenant application runs most.

The model is the one of Citus and Vitess, reduced to its core: rows are
placed by a *distribution key* (the tenant, project or account they
belong to), statements that name the key run on one shard, and queries
that do not are sent to all shards and merged.

```text
                    application
                         │  ClusterConnection (Rust API)
              ┌──────────▼───────────┐
              │ coordinator          │  parses the statement, picks the
              │ (library, in-process)│  shard(s), merges results
              └───┬────────┬─────┬───┘
     project 7,12 │        │     │ project 3,9,…
           ┌──────▼──┐ ┌───▼────┐┌▼────────┐
           │ shard 0 │ │ shard 1││ shard 2 │   each one a complete squad
           │ local   │ │ local  ││ remote  │   database: a directory in this
           │ dir     │ │ dir    ││ server  │   process or a squad server
           └─────────┘ └────────┘└─────────┘
```

The coordinator is a library. It lives in the application process, like
squad itself; several processes can each run a coordinator over the same
remote shards.

## When to use it

Use a cluster when one machine is not enough for the writes or the data,
*and* the data divides naturally by a key that nearly every statement
names: the project of an analytics event, the organization of an issue,
the tenant of a SaaS application.

Do not use it for data without such a key, or when transactions must
span keys. A single database with a [read replica](replication.md) and
[remote storage](cloud-storage.md) is simpler and has no restrictions on
SQL. Shards on one machine do help with one thing: every shard has its
own write-ahead log and its own locks, and queries across shards run on
all of them in parallel (see [Measurements](#measurements)).

## Setting it up

```rust
use squad_cluster::{Cluster, ClusterConfig, ShardSpec};

let cluster = Cluster::open(ClusterConfig::new(vec![
    ShardSpec::local("data/shard-0"),
    ShardSpec::local("data/shard-1"),
    ShardSpec::remote("postgres://squad@10.0.0.7:5432/squad"),
]))?;

let db = cluster.connect();
db.batch_execute("
    CREATE TABLE plans  (id int PRIMARY KEY, label text NOT NULL);
    CREATE TABLE users  (project_id bigint NOT NULL, id int NOT NULL, email text, plan_id int,
                         PRIMARY KEY (project_id, id));
    CREATE TABLE events (project_id bigint NOT NULL, ts timestamptz NOT NULL, name text, user_id int,
                         duration_ms int) USING columnar WITH (order_by = 'project_id, ts');

    SELECT squad_distribute('users',  'project_id');
    SELECT squad_distribute('events', 'project_id');
    SELECT squad_reference('plans');
")?;

db.execute("INSERT INTO events VALUES ($1, now(), 'signup', $2, 12)", &[&7i64, &1i32])?;

// one shard
let rows = db.query("SELECT name, count(*) FROM events WHERE project_id = $1 GROUP BY name", &[&7i64])?;
// all shards, merged
let rows = db.query("SELECT name, count(*) FROM events GROUP BY name ORDER BY 2 DESC", &[])?;
```

A shard is one of:

| `ShardSpec` | What it is |
| --- | --- |
| `local(dir)` or `Local { dir, config }` | a database directory opened in this process, with its own [`Config`](embedding.md) (cache size, sync mode, remote storage) |
| `remote(url)` | a [squad server](server.md), reached over the Postgres wire protocol |
| `Memory` | a database in memory, for tests |

Kinds can be mixed. `Cluster` is cheap to clone and can be shared
between threads; each thread takes its own `ClusterConnection`, whose
API mirrors `squad::Connection`: `execute`, `query`, `query_one`,
`query_opt`, `batch_execute`, `prepare`, `transaction`, and `insert_rows`
for bulk loads. Connections to the shards are opened when a statement
first needs them.

### The cluster's identity

The first `Cluster::open` on empty databases creates the cluster. It
writes, into a small table on every shard, the number of shards, the
position of that shard, and the *slot map*. Later opens check all three:

- a different number of shards is refused,
- shards listed in another order are refused,
- shards with different slot maps are refused.

A key is hashed to one of 4,096 *virtual slots* (configurable when the
cluster is created with `ClusterConfig::virtual_slots`), and the slot map
assigns every slot to a shard. Table classes are recorded the same way,
so every coordinator that opens the shards sees the same cluster. A
coordinator that is already running learns about tables another
coordinator declared with `Cluster::reload()`.

## Distributed and reference tables

Every table used through a cluster of more than one shard has a class.

**Distributed tables** hold the data that grows. `SELECT
squad_distribute('events', 'project_id')` declares that every row lives
on the shard its `project_id` hashes to. The distribution column must be
an integer, `text` or `uuid` column and is never NULL. Include it in the
primary key and in unique constraints: uniqueness is checked by each
shard for its own rows, so a constraint without the distribution column
is not enforced across shards.

**Reference tables** hold the small, shared data that distributed tables
are joined with: plans, countries, feature flags. `SELECT
squad_reference('plans')` declares that every shard holds a full copy.
Reads use the copy of whichever shard the statement runs on; writes are
applied to every shard.

A table must be declared while it is empty. Statements on a table that
has no class are rejected with a hint that names the two functions. On a
cluster of one shard nothing needs to be declared; declaring anyway
keeps the application ready for more shards.

Schema changes (`CREATE`, `ALTER`, `DROP`, `CREATE INDEX`, `TRUNCATE`,
`VACUUM`) run on every shard, one after the other. `DROP TABLE` forgets
the class. Renaming a declared table, and renaming, dropping or retyping
a distribution column, are not supported.

## How statements are routed

The coordinator works on the PostgreSQL parse tree of the statement; it
does not guess from the text. Routing is decided for every execution, so
a prepared statement with the key as a parameter goes to a different
shard each time.

| Statement | Where it runs |
| --- | --- |
| `SELECT`, `UPDATE`, `DELETE` with `key = constant or $n` on every distributed table it reads | the shard of that key |
| … with `key IN (…)` | the shards of the listed keys; one shard if they agree |
| Joins of distributed tables on their distribution columns (`ON a.project_id = b.project_id` or `USING`) | one key filter covers all joined tables |
| Joins with reference tables | wherever the distributed side runs |
| `INSERT … VALUES` into a distributed table | each row on the shard of its key; a multi-row INSERT is split |
| `ClusterConnection::insert_rows` | rows grouped by shard, all shards at once |
| `SELECT` on distributed tables without a key | all shards, merged (next section) |
| `UPDATE`, `DELETE` on a distributed table without a key | all shards, each changes its own rows |
| `SELECT` that reads only reference tables, `SHOW`, `EXPLAIN` | one shard |
| Writes to a reference table | every shard |
| `SET`, `RESET`, `DISCARD` | every shard connection of this cluster connection, including ones opened later |
| Schema changes | every shard |

A key filter is an equality or `IN` list on the distribution column, in
the top-level `AND` conditions of `WHERE` or `ON`, with constants or
parameters on the other side. `project_id = 1 OR project_id = 2` and
`project_id BETWEEN 1 AND 2` are correct but not recognized as filters;
such a query reads all shards. Subqueries, CTEs and the sides of a
`UNION` are analysed the same way: each must have its own key filter,
and all filters must name keys on one shard.

`INSERT` needs the key as a constant or parameter in the `VALUES` list;
`INSERT … SELECT` into a distributed table is not supported. The
distribution column of a row cannot be updated: delete the row and
insert it again.

### Bulk loads

`insert_rows` is the cluster's bulk path, in place of `COPY`:

```rust
use squad::Value;

let rows: Vec<Vec<Value>> = batch.iter().map(|e| vec![
    Value::Int8(e.project_id), Value::TimestampTz(e.micros), Value::Text(e.name.clone()),
]).collect();
db.insert_rows("events", &["project_id", "ts", "name"], rows)?;
```

Rows are grouped by shard and every shard receives its rows as one
statement, column by column, all shards in parallel. Values are
converted to the column types like parameters. For
[columnar tables](analytics.md), give each call enough rows that a shard
receives 50,000 or more: such batches are written as column segments
directly.

## Queries across shards

A `SELECT` without a key runs on every shard at the same time, one
thread per shard, and the coordinator combines the results. Which of two
ways it uses depends on the query.

**Rows that are complete on their shard** are concatenated. That covers
queries without aggregation, and aggregate queries that group by the
distribution column (a group then never spans shards, so *any*
aggregate works, including `approx_count_distinct`, `approx_percentile`
and `string_agg`). The coordinator sorts (`ORDER BY`, also by
expressions that are not in the select list), removes duplicates
(`DISTINCT`) and applies `LIMIT` and `OFFSET`. With a `LIMIT`, each shard
returns only its first `limit + offset` rows.

**Groups that span shards** are merged from partial aggregates. Each
shard computes one row per group; the coordinator combines them in an
in-memory squad database with a second query, so `HAVING`, `ORDER BY`,
`LIMIT`, `DISTINCT` and expressions over aggregates behave exactly as on
one database:

| Aggregate | How it is merged |
| --- | --- |
| `count(*)`, `count(x)` | sum of the counts |
| `sum(x)` | sum of the sums |
| `avg(x)` | sum of the sums divided by sum of the counts |
| `min`, `max`, `bool_and`, `bool_or`, `every`, `bit_and`, `bit_or` | the same function over the partial results |
| `count(DISTINCT x)` | each shard sends its distinct values per group; exact; limited to 1,000,000 values per query |
| any of these with `FILTER (WHERE …)` | the filter is applied on the shards |

Results have the same column names and types as on a single database.
The test suite checks that by running every query on a cluster and on
one database (see [How it is tested](#how-it-is-tested)).

### What is rejected

A cluster never returns an answer that a single database would not
return. A query it cannot answer correctly fails with SQLSTATE `0A000`
and a hint, usually: add a condition on the distribution column, or
group by it. Rejected across shards:

- joins of distributed tables that are not on their distribution
  columns, and subqueries or CTEs that read a distributed table without
  a key filter;
- `approx_count_distinct` and `approx_percentile` unless the query
  groups by the distribution column (sketches of different shards are
  not combined yet);
- `string_agg`, `array_agg`, `json_agg`, statistical aggregates
  (`stddev`, `variance`), `sum(DISTINCT …)`, aggregates with `ORDER BY`;
- window functions, `UNION` / `INTERSECT` / `EXCEPT`, `DISTINCT ON`,
  `GROUPING SETS`, `FETCH … WITH TIES`;
- `LIMIT` and `OFFSET` that are expressions rather than constants or
  parameters;
- `SELECT … FOR UPDATE` without a key;
- views over distributed tables, `CREATE TABLE AS`, `SELECT INTO`, SQL
  `PREPARE` / `EXECUTE` (use `ClusterConnection::prepare`), two-phase
  commit.

All of these work when the statement is restricted to one key.

## Transactions

Shards do not share transactions. A cluster transaction is a transaction
on one shard:

```rust
let tx = db.transaction()?;
tx.execute("INSERT INTO users VALUES ($1, $2, $3, 1)", &[&7i64, &1i32, &"ada@example.org"])?;
tx.execute("INSERT INTO events VALUES ($1, now(), 'signup', $2, 0)", &[&7i64, &1i32])?;
tx.commit()?;
```

`BEGIN` does nothing yet. The first statement that names a key binds the
transaction to that key's shard, where `BEGIN` (and any savepoints set
so far) are replayed. From then on the transaction has everything a
squad transaction has: isolation levels, savepoints, row locks. A
statement that needs another shard, all shards, a reference table write
or a schema change fails with `0A000` and leaves the transaction open.
An error of the shard aborts the transaction as usual (`25P02` until
`ROLLBACK`).

Reading only reference tables before the transaction is bound runs
outside of it.

Outside of transactions every statement commits on its own, and a
statement that writes to several shards commits on each shard
separately:

| Write | If one shard fails |
| --- | --- |
| Multi-row `INSERT` or `insert_rows` across shards, `UPDATE` / `DELETE` without a key | the other shards keep their part. The error says on which shards the statement was applied. |
| Write to a reference table | shards are written one after the other, so an error of the statement itself (a constraint, a type) happens on the first shard and nothing is changed. If a *later* shard fails (it is unreachable), the copies differ until the write is repeated; the error says which shards have it. Write reference tables with statements that can be repeated (`INSERT … ON CONFLICT`, `UPDATE` by key). |
| Schema change | as for reference tables; repeat with `IF NOT EXISTS` / `IF EXISTS`. |

There is no two-phase commit and no distributed snapshot: a query across
shards reads each shard at its own latest state.

## Durability, object storage and replicas

Each shard is a complete squad database and keeps all of squad's
guarantees for its own data. Configure it like one:

```rust
use squad::{object::open_url, Config, RemoteConfig, RemoteSync};
use squad_cluster::ShardSpec;

let shard = |i: usize| -> squad::Result<ShardSpec> {
    Ok(ShardSpec::Local {
        dir: format!("cache/shard-{i}").into(),
        config: Config {
            // one prefix per shard
            remote: Some(RemoteConfig::new(open_url(&format!("s3://bucket/prod/shard-{i}"))?, RemoteSync::Sync)),
            ..Config::default()
        },
    })
};
```

A machine that lost its disk restores every shard from its prefix and
opens the cluster again; the cluster's identity is part of each shard's
data. For remote shards, run each server with `--remote`, add
[read replicas](replication.md) per shard, and after a failover open the
cluster with the URL of the promoted replica in the same position.

`Shard::database()` gives access to a local shard for backups,
checkpoints and statistics.

## Limits

- **The number of shards is fixed.** There is no rebalancing yet. The
  slot map exists so that slots can be moved between shards later
  without rehashing; until then, start with as many shards as you expect
  to need. Several shards can share a machine and be moved to their own
  machines by restoring them elsewhere.
- **No transactions across shards**, no two-phase commit, no consistent
  snapshot across shards. Writes to several shards are not atomic.
- **Reference tables are kept equal by writing them everywhere**, not by
  replication. An unreachable shard blocks writes to them.
- **Sequences are per shard.** `serial` and `bigserial` columns of a
  distributed table produce values that repeat across shards. Use UUIDs,
  or make the key part of the primary key and generate ids in the
  application.
- **Uniqueness and foreign keys are checked per shard.** They hold when
  they include the distribution column (and for foreign keys into
  reference tables).
- **The query shapes listed under [What is rejected](#what-is-rejected).**
- **Rust API only.** The coordinator is a library; there is no
  wire-protocol front end for a cluster yet. Other languages can still
  query each shard through its server.
- **Merging happens in memory** on the coordinator. A query across
  shards that returns millions of rows, or groups into millions of
  groups, needs that memory.
- **A shard that is down fails the statements that need it** (SQLSTATE
  `58030`). Statements for keys on other shards keep working, and
  connections to a remote shard are re-established when it is back.
  Opening a cluster needs every shard, and statement shapes that were
  not seen before are analysed with the help of shard 0.

## Measurements

2 million events (columnar, distributed by `project_id`, 200 projects)
in clusters of 1, 2 and 4 local shards **on one 4-vCPU machine**. All
shards share its cores and its disk, so this measures what routing and
merging cost and how well shards use one machine. It is not a scale-out
measurement. Raw output:
[benchmarks/sharding-2026-11-10.txt](../benchmarks/sharding-2026-11-10.txt).

| | 1 shard | 2 shards | 4 shards |
| --- | ---: | ---: | ---: |
| Bulk load with `insert_rows`, rows/s | 304,000 | 391,000 | 422,000 |
| Single-row inserts, 1 connection, rows/s | 8,050 | 8,090 | 7,670 |
| Single-row inserts, 8 connections, rows/s | 28,700 | 23,900 | 18,600 |
| One project: events per day, 30 days | 0.70 ms | 0.69 ms | 0.72 ms |
| One project: top 10 paths of a week | 0.63 ms | 0.78 ms | 1.10 ms |
| All shards: `count(*)` | 0.14 ms | 0.83 ms | 1.45 ms |
| All shards: count and average by event | 6.7 ms | 7.7 ms | 8.6 ms |
| All shards: p95 per project | 7.0 ms | 6.5 ms | 9.2 ms |
| All shards: 20 slowest events (full scan and sort) | 3,111 ms | 1,637 ms | 891 ms |

What the numbers say:

- **Single-tenant statements cost the same on any number of shards.**
  Routing adds a parse-tree lookup that is cached per statement text.
- **Scatter-gather costs about a millisecond of fixed overhead** (a
  thread per shard, and a merge query for aggregates). Queries that are
  already fast on one shard do not get faster by splitting them.
- **Scans that are CPU-bound scale with the shards**, because every
  shard works on its part in parallel: the full scan with sort is 3.5
  times faster on 4 shards.
- **Durable single-row inserts do not get faster on one disk.** One
  database already groups the commits of concurrent connections into
  one disk flush; several shards on the same disk compete for it. This
  is the number that changes when shards have their own machines.

## How it is tested

- **Model test** (`tests/model.rs`): a random multi-tenant workload
  (inserts for one and several tenants, updates and deletes with and
  without key, reference table writes) runs on a cluster and on one
  plain squad database. Every write must change the same number of rows,
  and about 40 queries — single-tenant, grouped by tenant, merged
  aggregates, sorted and limited rows, joins — must return the same
  rows, in the same order, with the same column names and types. This
  runs for clusters of 1, 2, 4 and 7 shards, and for a cluster that
  mixes in-process shards with shards behind squad servers. At the end
  every row must be on the shard its key hashes to and every shard must
  hold the same reference tables.
- **Behaviour tests** (`tests/cluster.rs`): cluster identity across
  reopen and refusal of wrong shard counts and orders; declarations;
  schema changes on all shards; routing by literal, parameter and `IN`
  list (checked by removing a shard's data and observing who notices);
  split inserts; bulk inserts; transactions bound to a shard; the list of
  rejected statements; prepared statements; error codes from one shard
  and from partial failures; concurrent connections.
- **Remote shards** (`tests/remote.rs`): the same operations over the
  wire protocol, with enums, numerics, timestamps, arrays, JSON and
  NULLs, a shard that is unreachable when the cluster is opened, and a
  shard that goes away and comes back while connections are open.

```sh
cargo test -p squad-cluster
SQUAD_CLUSTER_SEEDS=0..200 SQUAD_CLUSTER_STEPS=300 cargo test -p squad-cluster --test model --release
```
