# squad-cluster

Sharding for [squad](../../README.md): one logical database over several
squad databases.

Rows are placed by a distribution key (a tenant, project or account).
Statements that name the key run on one shard; queries that do not are
sent to all shards in parallel and merged; what cannot be answered
correctly is rejected with SQLSTATE `0A000` and a hint.

```rust
use squad_cluster::{Cluster, ClusterConfig, ShardSpec};

let cluster = Cluster::open(ClusterConfig::new(vec![
    ShardSpec::local("data/shard-0"),                           // a directory in this process
    ShardSpec::remote("postgres://squad@10.0.0.7:5432/squad"),  // a squad server
]))?;
let db = cluster.connect();

db.batch_execute("
    CREATE TABLE events (project_id bigint NOT NULL, ts timestamptz, name text) USING columnar;
    CREATE TABLE plans  (id int PRIMARY KEY, label text);
    SELECT squad_distribute('events', 'project_id');   -- rows live on the shard of their project
    SELECT squad_reference('plans');                    -- every shard holds a copy
")?;

db.execute("INSERT INTO events VALUES ($1, now(), 'signup')", &[&7i64])?;
db.query("SELECT count(*) FROM events WHERE project_id = $1", &[&7i64])?;       // one shard
db.query("SELECT name, count(*) FROM events GROUP BY name ORDER BY 2 DESC", &[])?; // all shards
```

What it does:

- hash routing through a slot map that is stored on every shard and
  checked on open;
- single-shard statements, joins on the distribution column, joins with
  reference tables;
- multi-row inserts split by shard, and `insert_rows` for bulk loads;
- scatter-gather `SELECT` with merged `count`, `sum`, `avg`, `min`,
  `max`, `count(DISTINCT)`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`,
  `DISTINCT`;
- transactions bound to the shard of their first key;
- shards in this process or behind squad servers, mixed freely.

What it does not do: transactions across shards, rebalancing (the
number of shards is fixed), and the query shapes listed in the
documentation.

Full documentation: [docs/sharding.md](../../docs/sharding.md).

```sh
cargo test -p squad-cluster
cargo run --release -p squad-cluster --example cluster_bench -- 2000000 1,2,4
```
