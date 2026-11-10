//! A cluster whose shards are squad servers: the coordinator talks to
//! them over the Postgres wire protocol.

use squad::{Database, Value};
use squad_cluster::{Cluster, ClusterConfig, ShardSpec};
use squad_server::{Serve, ServerHandle};

struct Servers {
    dbs: Vec<Database>,
    handles: Vec<ServerHandle>,
}

impl Servers {
    fn start(n: usize) -> Servers {
        let dbs: Vec<Database> = (0..n).map(|_| Database::open_in_memory().unwrap()).collect();
        let handles = dbs.iter().map(|db| db.serve("127.0.0.1:0").unwrap()).collect();
        Servers { dbs, handles }
    }

    fn specs(&self) -> Vec<ShardSpec> {
        self.handles.iter().map(|h| ShardSpec::remote(h.url().unwrap())).collect()
    }

    fn count(&self, shard: usize, table: &str) -> i64 {
        self.dbs[shard].connect().query_one(&format!("SELECT count(*) FROM {table}"), &[]).unwrap().get(0)
    }
}

impl Drop for Servers {
    fn drop(&mut self) {
        self.handles.drain(..).for_each(|h| h.shutdown());
    }
}

const SCHEMA: &str = "
    CREATE TYPE level AS ENUM ('info', 'warn', 'error');
    CREATE TABLE issues (
        project_id bigint NOT NULL, id uuid NOT NULL, title text, lvl level, seen timestamptz,
        cost numeric(12, 2), tags text[], meta jsonb, open bool, hits int, ratio float8,
        PRIMARY KEY (project_id, id));
    CREATE TABLE events (project_id bigint NOT NULL, user_id int, name text, value float8, at timestamptz) USING columnar;
    CREATE TABLE plans (id int PRIMARY KEY, label text NOT NULL);
    SELECT squad_distribute('issues', 'project_id');
    SELECT squad_distribute('events', 'project_id');
    SELECT squad_reference('plans');
";

#[test]
fn remote_shards() {
    let servers = Servers::start(3);
    let cluster = Cluster::open(ClusterConfig::new(servers.specs())).unwrap();
    assert!(cluster.shards().iter().all(|s| s.database().is_none() && s.url().unwrap().starts_with("postgres://")));
    let conn = cluster.connect();
    conn.batch_execute(SCHEMA).unwrap();

    // Values of every common type travel to the shards and back.
    let insert = conn
        .prepare(
            "INSERT INTO issues VALUES ($1, $2::uuid, $3, $4::level, $5::timestamptz, $6::numeric, $7::text[], $8::jsonb, $9, $10, $11)",
        )
        .unwrap();
    for t in 0..30i64 {
        let id = format!("00000000-0000-4000-8000-{t:012}");
        let title: Option<String> = if t % 5 == 0 { None } else { Some(format!("issue \"{t}\" — ünïcode")) };
        insert
            .execute(&[
                &t,
                &id,
                &title,
                &["info", "warn", "error"][(t % 3) as usize],
                &"2026-05-01 10:00:00+00",
                &format!("{t}.25"),
                &"{a,b}",
                &format!("{{\"n\": {t}}}"),
                &(t % 2 == 0),
                &(t as i32 * 3),
                &(t as f64 / 4.0),
            ])
            .unwrap();
    }
    assert_eq!((0..3).map(|s| servers.count(s, "issues")).sum::<i64>(), 30);
    assert!((0..3).all(|s| servers.count(s, "issues") > 0));
    let row = conn
        .query_one(
            "SELECT id::text, title, lvl::text, seen::text, cost::text, tags, meta ->> 'n', open, hits, ratio FROM issues WHERE project_id = $1",
            &[&7i64],
        )
        .unwrap();
    assert_eq!(row.get::<String>(0), "00000000-0000-4000-8000-000000000007");
    assert_eq!(row.get::<String>(1), "issue \"7\" — ünïcode");
    assert_eq!(row.get::<String>(2), "warn");
    assert_eq!(row.get::<String>(3), "2026-05-01 10:00:00+00");
    assert_eq!(row.get::<String>(4), "7.25");
    assert_eq!(row.values()[5], Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())]));
    assert_eq!(row.get::<String>(6), "7");
    assert!(!row.get::<bool>(7));
    assert_eq!(row.get::<i32>(8), 21);
    assert_eq!(row.get::<f64>(9), 1.75);
    let none: Option<String> = conn.query_one("SELECT title FROM issues WHERE project_id = 5", &[]).unwrap().get(0);
    assert_eq!(none, None);

    // Scatter-gather over the wire.
    let row = conn.query_one("SELECT count(*), sum(cost)::text, avg(hits), max(seen)::text, count(DISTINCT lvl::text), count(title) FROM issues", &[]).unwrap();
    assert_eq!(row.get::<i64>(0), 30);
    assert_eq!(row.get::<String>(1), "442.50");
    assert_eq!(row.get::<i64>(4), 3);
    assert_eq!(row.get::<i64>(5), 24);
    let rows = conn.query("SELECT lvl::text AS l, count(*), sum(hits) FROM issues WHERE open GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 2", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().map(|r| r.get::<i64>(1)).sum::<i64>(), 10);
    let ids: Vec<i64> = conn
        .query("SELECT project_id FROM issues WHERE hits > $1 ORDER BY cost DESC LIMIT 3", &[&10i32])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![29, 28, 27]);

    // Bulk insert: arrays in binary format.
    let rows: Vec<Vec<Value>> = (0..5000i64)
        .map(|i| {
            vec![
                Value::Int8(i % 41),
                if i % 9 == 0 { Value::Null } else { Value::Int4((i % 300) as i32) },
                Value::Text(["view", "click"][(i % 2) as usize].into()),
                Value::Float8(i as f64),
                Value::Text("2026-05-02 00:00:00+00".into()),
            ]
        })
        .collect();
    assert_eq!(conn.insert_rows("events", &["project_id", "user_id", "name", "value", "at"], rows).unwrap(), 5000);
    assert_eq!((0..3).map(|s| servers.count(s, "events")).sum::<i64>(), 5000);
    let row = conn.query_one("SELECT count(*), count(user_id), sum(value), count(DISTINCT project_id) FROM events", &[]).unwrap();
    assert_eq!((row.get::<i64>(0), row.get::<i64>(1), row.get::<f64>(2), row.get::<i64>(3)), (5000, 4444, 12_497_500.0, 41));
    let n: i64 = conn.query_one("SELECT count(*) FROM events WHERE project_id = $1 AND name = 'click'", &[&4i64]).unwrap().get(0);
    assert_eq!(n, (0..5000).filter(|i| i % 41 == 4 && i % 2 == 1).count() as i64);

    // Reference tables and transactions.
    conn.execute("INSERT INTO plans VALUES (1, 'free'), (2, 'team')", &[]).unwrap();
    assert!((0..3).all(|s| servers.count(s, "plans") == 2));
    let tx = conn.transaction().unwrap();
    tx.execute("UPDATE issues SET hits = 0 WHERE project_id = $1", &[&3i64]).unwrap();
    let elsewhere = (4..30i64).find(|t| cluster.shard_for(&Value::Int8(*t)) != cluster.shard_for(&Value::Int8(3))).unwrap();
    assert_eq!(
        tx.execute("UPDATE issues SET hits = 0 WHERE project_id = $1", &[&elsewhere]).unwrap_err().code(),
        squad::sqlstate::FEATURE_NOT_SUPPORTED
    );
    tx.rollback().unwrap();
    let hits: i32 = conn.query_one("SELECT hits FROM issues WHERE project_id = 3", &[]).unwrap().get(0);
    assert_eq!(hits, 9);
    let tx = conn.transaction().unwrap();
    tx.execute("DELETE FROM issues WHERE project_id = $1", &[&3i64]).unwrap();
    tx.commit().unwrap();
    let n: i64 = conn.query_one("SELECT count(*) FROM issues", &[]).unwrap().get(0);
    assert_eq!(n, 29);

    // Errors keep their SQLSTATE, message and detail.
    let err = conn.execute("INSERT INTO issues (project_id, id) VALUES (7, '00000000-0000-4000-8000-000000000007')", &[]).unwrap_err();
    assert_eq!(err.code(), squad::sqlstate::UNIQUE_VIOLATION);
    assert!(err.message().contains("duplicate key"), "{err:?}");
    assert_eq!(conn.query("SELECT 1 / (hits - 21) FROM issues", &[]).unwrap_err().code(), squad::sqlstate::DIVISION_BY_ZERO);
    assert_eq!(conn.query("SELECT nope FROM issues", &[]).unwrap_err().code(), squad::sqlstate::UNDEFINED_COLUMN);

    // A second coordinator on the same servers sees the same cluster.
    let again = Cluster::open(ClusterConfig::new(servers.specs())).unwrap();
    assert_eq!(again.tables().len(), 3);
    let n: i64 = again.connect().query_one("SELECT count(*) FROM issues WHERE project_id = 7", &[]).unwrap().get(0);
    assert_eq!(n, 1);
    // Wrong order is refused for remote shards as well.
    let mut specs = servers.specs();
    specs.swap(0, 2);
    assert_eq!(Cluster::open(ClusterConfig::new(specs)).err().unwrap().code(), squad::sqlstate::INVALID_PARAMETER_VALUE);
    // A table declared through one coordinator is known to another after reload().
    conn.batch_execute("CREATE TABLE notes (project_id bigint, body text); SELECT squad_distribute('notes', 'project_id')").unwrap();
    again.reload().unwrap();
    again.connect().execute("INSERT INTO notes VALUES (1, 'a'), (2, 'b'), (3, 'c')", &[]).unwrap();
    let n: i64 = conn.query_one("SELECT count(*) FROM notes", &[]).unwrap().get(0);
    assert_eq!(n, 3);
}

#[test]
fn a_shard_that_is_down() {
    let servers = Servers::start(2);
    let mut specs = servers.specs();
    specs.push(ShardSpec::remote("postgres://squad@127.0.0.1:1/squad"));
    let err = Cluster::open(ClusterConfig::new(specs)).err().unwrap();
    assert_eq!(err.code(), squad::sqlstate::IO_ERROR, "{err}");
    assert!(err.message().contains("cannot connect to shard"), "{err}");
}
