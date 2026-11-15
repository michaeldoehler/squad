//! Behaviour of a cluster: metadata, routing, bulk inserts, transactions,
//! what is rejected, and how errors of a shard surface.

use squad::{ToSql, Value};
use squad_cluster::{Cluster, ClusterConfig, ClusterConnection, ShardSpec, TableClass};

const SCHEMA: &str = "
    CREATE TABLE plans (id int PRIMARY KEY, label text NOT NULL);
    CREATE TABLE users (project_id bigint NOT NULL, id int NOT NULL, email text, plan_id int, score int,
                        PRIMARY KEY (project_id, id));
    CREATE TABLE events (project_id bigint NOT NULL, user_id int, name text, value float8, at timestamptz) USING columnar;
    SELECT squad_distribute('users', 'project_id');
    SELECT squad_distribute('events', 'project_id');
    SELECT squad_reference('plans');
";

fn memory(shards: usize) -> (Cluster, ClusterConnection) {
    let cluster = Cluster::open(ClusterConfig::new(vec![ShardSpec::Memory; shards])).unwrap();
    let conn = cluster.connect();
    conn.batch_execute(SCHEMA).unwrap();
    (cluster, conn)
}

/// Rows of `table` on every shard, read from the shard itself.
fn rows_per_shard(cluster: &Cluster, table: &str) -> Vec<i64> {
    cluster
        .shards()
        .iter()
        .map(|s| {
            s.database().unwrap().connect().query_one(&format!("SELECT count(*) FROM {table}"), &[]).unwrap().get(0)
        })
        .collect()
}

/// Every row of the distributed tables is on the shard its key hashes to.
fn assert_placement(cluster: &Cluster) {
    for shard in cluster.shards() {
        let conn = shard.database().unwrap().connect();
        for table in ["users", "events"] {
            for row in conn.query(&format!("SELECT DISTINCT project_id FROM {table}"), &[]).unwrap() {
                assert_eq!(
                    cluster.shard_for_table(table, &row.values()[0]).unwrap(),
                    shard.index(),
                    "{table}: row on the wrong shard"
                );
            }
        }
    }
}

fn code(result: squad::Result<impl Sized>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) => e.code(),
    }
}

/// Two tenants that live on different shards.
fn two_tenants(cluster: &Cluster) -> (i64, i64) {
    let a = 1i64;
    let home = cluster.shard_for(&Value::Int8(a));
    let b = (2..1000).find(|k| cluster.shard_for(&Value::Int8(*k)) != home).unwrap();
    (a, b)
}

#[test]
fn cluster_is_recorded_on_its_shards_and_checked_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let specs = |order: &[usize]| -> Vec<ShardSpec> {
        order.iter().map(|i| ShardSpec::local(dir.path().join(format!("shard-{i}")))).collect()
    };
    {
        let cluster = Cluster::open(ClusterConfig::new(specs(&[0, 1, 2]))).unwrap();
        let conn = cluster.connect();
        conn.batch_execute(SCHEMA).unwrap();
        for tenant in 0..40i64 {
            conn.execute("INSERT INTO users VALUES ($1, 1, 'a', NULL, 5)", &[&tenant]).unwrap();
        }
        conn.execute("INSERT INTO plans VALUES (1, 'free'), (2, 'team')", &[]).unwrap();
        assert_eq!(cluster.virtual_slots(), squad_cluster::VIRTUAL_SLOTS);
    }
    // Same shards, same order: everything is still there.
    {
        let cluster = Cluster::open(ClusterConfig::new(specs(&[0, 1, 2]))).unwrap();
        assert_eq!(
            cluster.tables(),
            vec![
                ("events".to_string(), TableClass::Distributed { column: "project_id".into() }),
                ("plans".to_string(), TableClass::Reference),
                ("users".to_string(), TableClass::Distributed { column: "project_id".into() }),
            ]
        );
        let conn = cluster.connect();
        let n: i64 = conn.query_one("SELECT count(*) FROM users", &[]).unwrap().get(0);
        assert_eq!(n, 40);
        let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = 17", &[]).unwrap().get(0);
        assert_eq!(n, 1);
        let n: i64 = conn.query_one("SELECT count(*) FROM plans", &[]).unwrap().get(0);
        assert_eq!(n, 2);
        assert_placement(&cluster);
        let per_shard = rows_per_shard(&cluster, "users");
        assert!(per_shard.iter().all(|n| *n > 0), "40 tenants should reach all three shards: {per_shard:?}");
    }
    // Fewer shards, more shards, another order: refused.
    for order in [&[0usize, 1][..], &[0, 1, 2, 3], &[1, 0, 2], &[0, 2, 1]] {
        let err =
            Cluster::open(ClusterConfig::new(specs(order))).err().unwrap_or_else(|| panic!("{order:?} was accepted"));
        assert_eq!(err.code(), squad::sqlstate::INVALID_PARAMETER_VALUE, "{order:?}: {err}");
    }
    // A refused open leaves the shards as they were. (Shard 3 was created
    // empty by the attempt above and is not part of the cluster.)
    let cluster = Cluster::open(ClusterConfig::new(specs(&[0, 1, 2]))).unwrap();
    let n: i64 = cluster.connect().query_one("SELECT count(*) FROM users", &[]).unwrap().get(0);
    assert_eq!(n, 40);

    assert_eq!(code(Cluster::open(ClusterConfig::new(Vec::new()))), squad::sqlstate::INVALID_PARAMETER_VALUE);
}

#[test]
fn declarations() {
    let (cluster, conn) = memory(3);
    // Declaring again is fine; changing the class is not.
    conn.batch_execute("SELECT squad_distribute('users', 'project_id')").unwrap();
    assert_eq!(code(conn.batch_execute("SELECT squad_reference('users')")), squad::sqlstate::DUPLICATE_OBJECT);
    assert_eq!(code(conn.batch_execute("SELECT squad_distribute('users', 'id')")), squad::sqlstate::DUPLICATE_OBJECT);
    // The column must exist and have a hashable type.
    assert_eq!(code(conn.batch_execute("SELECT squad_distribute('nope', 'x')")), squad::sqlstate::UNDEFINED_TABLE);
    conn.batch_execute("CREATE TABLE m (at timestamptz, v float8, k uuid, t text)").unwrap();
    assert_eq!(code(conn.batch_execute("SELECT squad_distribute('m', 'missing')")), squad::sqlstate::UNDEFINED_COLUMN);
    assert_eq!(code(conn.batch_execute("SELECT squad_distribute('m', 'v')")), squad::sqlstate::FEATURE_NOT_SUPPORTED);
    // An undeclared table cannot be used on several shards.
    let err = conn.execute("INSERT INTO m VALUES (now(), 1, NULL, 'x')", &[]).unwrap_err();
    assert_eq!(err.code(), squad::sqlstate::FEATURE_NOT_SUPPORTED);
    assert!(err.hint().unwrap().contains("squad_distribute"), "{err:?}");
    assert_eq!(code(conn.query("SELECT * FROM m", &[])), squad::sqlstate::FEATURE_NOT_SUPPORTED);

    // uuid and text keys.
    conn.batch_execute("SELECT squad_distribute('m', 'k')").unwrap();
    conn.batch_execute("CREATE TABLE s (tenant text, n int); SELECT squad_distribute('s', 'tenant')").unwrap();
    for i in 0..30 {
        let id = format!("00000000-0000-4000-8000-0000000000{i:02}");
        conn.execute("INSERT INTO m (k, t) VALUES ($1::uuid, 'x')", &[&id]).unwrap();
        conn.execute("INSERT INTO s VALUES ($1, $2)", &[&format!("tenant-{i}"), &i]).unwrap();
        let hit: i64 = conn.query_one("SELECT count(*) FROM m WHERE k = $1::uuid", &[&id]).unwrap().get(0);
        assert_eq!(hit, 1);
        let n: i32 = conn.query_one("SELECT n FROM s WHERE tenant = $1", &[&format!("tenant-{i}")]).unwrap().get(0);
        assert_eq!(n, i);
    }
    for table in ["m", "s"] {
        let per_shard = rows_per_shard(&cluster, table);
        assert_eq!(per_shard.iter().sum::<i64>(), 30);
        assert!(per_shard.iter().all(|n| *n > 0), "{table}: {per_shard:?}");
    }
    // A table with rows cannot be declared afterwards.
    conn.batch_execute("CREATE TABLE late (id int)").unwrap();
    cluster.shards()[1].database().unwrap().connect().execute("INSERT INTO late VALUES (1)", &[]).unwrap();
    assert_eq!(
        code(conn.batch_execute("SELECT squad_distribute('late', 'id')")),
        squad::sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE
    );
    // NULL is not a key.
    assert_eq!(code(conn.execute("INSERT INTO s VALUES (NULL, 1)", &[])), squad::sqlstate::NOT_NULL_VIOLATION);
}

#[test]
fn schema_changes_reach_every_shard() {
    let (cluster, conn) = memory(4);
    conn.batch_execute(
        "CREATE INDEX users_email ON users (email);
         ALTER TABLE users ADD COLUMN country text DEFAULT 'de';
         CREATE TABLE scratch (project_id bigint, x int);
         SELECT squad_distribute('scratch', 'project_id');",
    )
    .unwrap();
    for shard in cluster.shards() {
        let c = shard.database().unwrap().connect();
        c.query("SELECT country FROM users", &[]).unwrap();
        let n: i64 =
            c.query_one("SELECT count(*) FROM pg_indexes WHERE indexname = 'users_email'", &[]).unwrap().get(0);
        assert_eq!(n, 1);
    }
    // A cached plan does not survive a schema change.
    conn.execute("INSERT INTO users (project_id, id, country) VALUES (1, 1, 'fr'), (2, 1, 'fr'), (3, 1, 'it')", &[])
        .unwrap();
    let q = "SELECT country, count(*) FROM users GROUP BY country ORDER BY 1";
    assert_eq!(conn.query(q, &[]).unwrap().len(), 2);
    conn.batch_execute("ALTER TABLE users DROP COLUMN country").unwrap();
    assert_eq!(code(conn.query(q, &[])), squad::sqlstate::UNDEFINED_COLUMN);

    // Dropping a table forgets its class.
    assert!(cluster.tables().iter().any(|(t, _)| t == "scratch"));
    conn.batch_execute("DROP TABLE scratch").unwrap();
    assert!(!cluster.tables().iter().any(|(t, _)| t == "scratch"));
    conn.batch_execute("CREATE TABLE scratch (id int PRIMARY KEY); SELECT squad_reference('scratch')").unwrap();
    assert!(cluster.tables().contains(&("scratch".to_string(), TableClass::Reference)));

    // TRUNCATE and VACUUM run everywhere.
    conn.batch_execute("TRUNCATE users; VACUUM").unwrap();
    assert_eq!(rows_per_shard(&cluster, "users"), vec![0; 4]);

    // What would detach a table from its class is refused.
    for sql in [
        "ALTER TABLE users RENAME TO people",
        "ALTER TABLE users RENAME COLUMN project_id TO tenant",
        "ALTER TABLE users DROP COLUMN project_id",
        "ALTER TABLE users ALTER COLUMN project_id TYPE text",
        "ALTER TABLE plans RENAME TO tariffs",
        "CREATE TABLE copy AS SELECT * FROM users",
    ] {
        assert_eq!(code(conn.batch_execute(sql)), squad::sqlstate::FEATURE_NOT_SUPPORTED, "{sql}");
    }
    conn.batch_execute("ALTER TABLE users RENAME COLUMN email TO mail").unwrap();
    // A schema change that fails everywhere changes nothing.
    assert_eq!(code(conn.batch_execute("CREATE TABLE users (id int)")), squad::sqlstate::DUPLICATE_TABLE);
}

#[test]
fn statements_with_a_key_run_on_one_shard() {
    let (cluster, conn) = memory(4);
    let (a, b) = two_tenants(&cluster);
    conn.execute(
        "INSERT INTO users VALUES ($1, 1, 'a1', 1, 10), ($1, 2, 'a2', 2, 20), ($2, 1, 'b1', 1, 30)",
        &[&a, &b],
    )
    .unwrap();
    conn.execute("INSERT INTO events VALUES ($1, 1, 'click', 1.5, now()), ($2, 1, 'click', 2.5, now())", &[&a, &b])
        .unwrap();
    conn.execute("INSERT INTO plans VALUES (1, 'free'), (2, 'team')", &[]).unwrap();
    assert_placement(&cluster);

    // Remove tenant b's shard from the picture: statements for tenant a
    // must not notice, statements that need b's shard must see the hole.
    let shard_b = cluster.shard_for(&Value::Int8(b));
    cluster.shards()[shard_b]
        .database()
        .unwrap()
        .connect()
        .batch_execute("DELETE FROM users; DELETE FROM events")
        .unwrap();

    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&a]).unwrap().get(0);
    assert_eq!(n, 2);
    let n: i64 = conn.query_one(&format!("SELECT count(*) FROM users WHERE project_id = {a}"), &[]).unwrap().get(0);
    assert_eq!(n, 2);
    let n: i64 = conn
        .query_one(&format!("SELECT count(*) FROM users WHERE {a}::bigint = users.project_id AND score > 5"), &[])
        .unwrap()
        .get(0);
    assert_eq!(n, 2);
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&b]).unwrap().get(0);
    assert_eq!(n, 0);
    // IN lists: one shard, or the shards of the listed keys.
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id IN ($1)", &[&a]).unwrap().get(0);
    assert_eq!(n, 2);
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id IN ($1, $2)", &[&a, &b]).unwrap().get(0);
    assert_eq!(n, 2);
    // A join on the distribution column, with a reference table on the side.
    let rows = conn
        .query(
            "SELECT u.email, p.label, e.name FROM users u JOIN events e ON e.project_id = u.project_id AND e.user_id = u.id \
             JOIN plans p ON p.id = u.plan_id WHERE u.project_id = $1",
            &[&a],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(0), "a1");
    assert_eq!(rows[0].get::<String>("label"), "free");
    // Subqueries with the same key.
    let n: i64 = conn
        .query_one(
            "SELECT count(*) FROM users u WHERE u.project_id = $1 AND EXISTS \
             (SELECT 1 FROM events e WHERE e.project_id = $1 AND e.user_id = u.id)",
            &[&a],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
    // Writes with a key.
    assert_eq!(conn.execute("UPDATE users SET score = score + 1 WHERE project_id = $1", &[&a]).unwrap(), 2);
    assert_eq!(conn.execute("DELETE FROM users WHERE project_id = $1 AND id = 2", &[&a]).unwrap(), 1);
    let rows = conn.query("UPDATE users SET score = 0 WHERE project_id = $1 RETURNING id, score", &[&a]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32>("score"), 0);
    // A NULL key matches nothing.
    let none: Option<i64> = None;
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&none]).unwrap().get(0);
    assert_eq!(n, 0);
}

#[test]
fn inserts_are_split_by_shard() {
    let (cluster, conn) = memory(4);
    // A multi-row INSERT with constants, parameters and casts as keys.
    let n = conn
        .execute(
            "INSERT INTO users (id, project_id, email) VALUES (1, 1, 'a'), (1, 2, 'b'), (1, $1, 'c'), (1, '4', 'd'), (1, 5::bigint, 'e'), (2, 1, 'f')",
            &[&3i64],
        )
        .unwrap();
    assert_eq!(n, 6);
    assert_eq!(rows_per_shard(&cluster, "users").iter().sum::<i64>(), 6);
    assert_placement(&cluster);
    // RETURNING comes back from every shard.
    let rows = conn
        .query("INSERT INTO users (project_id, id) VALUES (10, 1), (11, 1), (12, 1), (13, 1) RETURNING project_id", &[])
        .unwrap();
    let mut keys: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    keys.sort();
    assert_eq!(keys, vec![10, 11, 12, 13]);
    // ON CONFLICT works per row.
    conn.execute("INSERT INTO users (project_id, id, score) VALUES (10, 1, 7), (11, 1, 7) ON CONFLICT (project_id, id) DO UPDATE SET score = excluded.score", &[]).unwrap();
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE score = 7", &[]).unwrap().get(0);
    assert_eq!(n, 2);

    // What cannot be routed is refused, and nothing is inserted.
    for sql in [
        "INSERT INTO users (id, email) VALUES (1, 'x')",
        "INSERT INTO users (project_id, id) VALUES (1 + 1, 9)",
        "INSERT INTO users (project_id, id) SELECT 1, 99",
        "INSERT INTO users DEFAULT VALUES",
    ] {
        assert_eq!(code(conn.execute(sql, &[])), squad::sqlstate::FEATURE_NOT_SUPPORTED, "{sql}");
    }
    assert_eq!(rows_per_shard(&cluster, "users").iter().sum::<i64>(), 10);
}

#[test]
fn bulk_insert() {
    let (cluster, conn) = memory(4);
    let rows: Vec<Vec<Value>> = (0..20_000i64)
        .map(|i| {
            vec![
                // Values need not have the exact column type.
                Value::Int4((i % 97) as i32),
                if i % 10 == 0 { Value::Null } else { Value::Int8(i % 500) },
                Value::Text(["view", "click", "buy"][(i % 3) as usize].into()),
                Value::Float8(i as f64 * 0.5),
                Value::Text(format!("2026-03-{:02} 12:00:00+00", 1 + i % 28)),
            ]
        })
        .collect();
    let n = conn.insert_rows("events", &["project_id", "user_id", "name", "value", "at"], rows).unwrap();
    assert_eq!(n, 20_000);
    assert_placement(&cluster);
    let per_shard = rows_per_shard(&cluster, "events");
    assert_eq!(per_shard.iter().sum::<i64>(), 20_000);
    assert!(per_shard.iter().all(|n| *n > 1000), "{per_shard:?}");
    let row = conn
        .query_one(
            "SELECT count(*), count(user_id), sum(value), count(DISTINCT project_id), min(at)::text FROM events",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<i64>(0), 20_000);
    assert_eq!(row.get::<i64>(1), 18_000);
    assert_eq!(row.get::<f64>(2), (0..20_000).map(|i| i as f64 * 0.5).sum::<f64>());
    assert_eq!(row.get::<i64>(3), 97);
    assert_eq!(row.get::<String>(4), "2026-03-01 12:00:00+00");
    let n: i64 =
        conn.query_one("SELECT count(*) FROM events WHERE project_id = 5 AND name = 'click'", &[]).unwrap().get(0);
    assert_eq!(n, (0..20_000).filter(|i| i % 97 == 5 && i % 3 == 1).count() as i64);

    // A subset of the columns, in another order.
    conn.insert_rows(
        "users",
        &["id", "project_id"],
        (0..300).map(|i| vec![Value::Int4(i), Value::Int8((i % 7) as i64)]).collect(),
    )
    .unwrap();
    assert_eq!(rows_per_shard(&cluster, "users").iter().sum::<i64>(), 300);
    assert_placement(&cluster);

    // Reference tables receive every row on every shard.
    let n = conn
        .insert_rows(
            "plans",
            &["id", "label"],
            (0..50).map(|i| vec![Value::Int4(i), Value::Text(format!("p{i}"))]).collect(),
        )
        .unwrap();
    assert_eq!(n, 50);
    assert_eq!(rows_per_shard(&cluster, "plans"), vec![50; 4]);

    // Errors.
    assert_eq!(conn.insert_rows("events", &["project_id"], Vec::new()).unwrap(), 0);
    assert_eq!(
        code(conn.insert_rows("events", &["name"], vec![vec![Value::Text("x".into())]])),
        squad::sqlstate::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        code(conn.insert_rows("events", &["project_id", "name"], vec![vec![Value::Int8(1)]])),
        squad::sqlstate::SYNTAX_ERROR
    );
    assert_eq!(
        code(conn.insert_rows("events", &["project_id", "nope"], vec![vec![Value::Int8(1), Value::Null]])),
        squad::sqlstate::UNDEFINED_COLUMN
    );
    assert_eq!(
        code(conn.insert_rows("events", &["project_id"], vec![vec![Value::Null]])),
        squad::sqlstate::NOT_NULL_VIOLATION
    );
    assert_eq!(code(conn.insert_rows("missing", &["a"], vec![vec![Value::Int8(1)]])), squad::sqlstate::UNDEFINED_TABLE);
    // A primary key violation on one shard; the other shards keep their rows.
    let err = conn
        .insert_rows(
            "users",
            &["project_id", "id"],
            (0..7).map(|t| vec![Value::Int8(t), Value::Int4(if t == 3 { 3 } else { 1000 })]).collect(),
        )
        .unwrap_err();
    assert_eq!(err.code(), squad::sqlstate::UNIQUE_VIOLATION);
    assert!(err.detail().unwrap().contains("Shards do not share a transaction"), "{err:?}");
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE id = 1000", &[]).unwrap().get(0);
    assert!(n > 0 && n < 7, "{n}");
}

#[test]
fn transactions_are_bound_to_one_shard() {
    let (cluster, conn) = memory(4);
    let (a, b) = two_tenants(&cluster);
    conn.execute("INSERT INTO plans VALUES (1, 'free')", &[]).unwrap();

    // Commit.
    let tx = conn.transaction().unwrap();
    assert!(tx.in_transaction());
    assert_eq!(tx.transaction_shard(), None);
    tx.execute("INSERT INTO users VALUES ($1, 1, 'a', 1, 1)", &[&a]).unwrap();
    assert_eq!(tx.transaction_shard(), Some(cluster.shard_for(&Value::Int8(a))));
    tx.execute("INSERT INTO events VALUES ($1, 1, 'signup', 1, now())", &[&a]).unwrap();
    tx.execute("UPDATE users SET score = score + 1 WHERE project_id = $1", &[&a]).unwrap();
    // Reference tables can be read on the transaction's shard.
    let label: String = tx
        .query_one("SELECT p.label FROM users u JOIN plans p ON p.id = u.plan_id WHERE u.project_id = $1", &[&a])
        .unwrap()
        .get(0);
    assert_eq!(label, "free");
    // Not visible to others before the commit.
    let other = cluster.connect();
    let n: i64 = other.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&a]).unwrap().get(0);
    assert_eq!(n, 0);
    // Another shard, all shards, reference writes and schema changes are refused …
    for (sql, params) in [
        ("INSERT INTO users VALUES ($1, 1, 'b', 1, 1)", vec![&b as &dyn ToSql]),
        ("SELECT count(*) FROM users WHERE project_id = $1", vec![&b as &dyn ToSql]),
        ("SELECT count(*) FROM users", vec![]),
        ("UPDATE users SET score = 0", vec![]),
        ("INSERT INTO plans VALUES (2, 'team')", vec![]),
        ("CREATE TABLE t (id int)", vec![]),
        ("SELECT squad_reference('users')", vec![]),
    ] {
        let err = tx.execute(sql, &params).unwrap_err();
        assert_eq!(err.code(), squad::sqlstate::FEATURE_NOT_SUPPORTED, "{sql}: {err}");
    }
    assert_eq!(
        code(tx.insert_rows(
            "users",
            &["project_id", "id"],
            vec![vec![Value::Int8(a), Value::Int4(5)], vec![Value::Int8(b), Value::Int4(5)]]
        )),
        squad::sqlstate::FEATURE_NOT_SUPPORTED
    );
    // … without ending the transaction.
    tx.insert_rows("users", &["project_id", "id"], vec![vec![Value::Int8(a), Value::Int4(2)]]).unwrap();
    tx.commit().unwrap();
    assert!(!conn.in_transaction());
    let n: i64 = other
        .query_one("SELECT count(*) FROM users WHERE project_id = $1 AND (score = 2 OR id = 2)", &[&a])
        .unwrap()
        .get(0);
    assert_eq!(n, 2);

    // Rollback, explicit and by dropping the transaction.
    let tx = conn.transaction().unwrap();
    tx.execute("DELETE FROM users WHERE project_id = $1", &[&a]).unwrap();
    tx.rollback().unwrap();
    {
        let tx = conn.transaction().unwrap();
        tx.execute("DELETE FROM users WHERE project_id = $1", &[&a]).unwrap();
    }
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&a]).unwrap().get(0);
    assert_eq!(n, 2);

    // Savepoints, also ones set before the shard is known.
    conn.batch_execute("BEGIN; SAVEPOINT s0").unwrap();
    conn.execute("INSERT INTO users VALUES ($1, 10, 'x', 1, 1)", &[&b]).unwrap();
    conn.batch_execute("SAVEPOINT s1").unwrap();
    conn.execute("INSERT INTO users VALUES ($1, 11, 'y', 1, 1)", &[&b]).unwrap();
    conn.batch_execute("ROLLBACK TO s1").unwrap();
    conn.execute("INSERT INTO users VALUES ($1, 12, 'z', 1, 1)", &[&b]).unwrap();
    conn.batch_execute("COMMIT").unwrap();
    let ids: Vec<i32> = conn
        .query("SELECT id FROM users WHERE project_id = $1 ORDER BY id", &[&b])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![10, 12]);

    // An error inside the transaction aborts it on its shard, as in a
    // single database.
    conn.batch_execute("BEGIN").unwrap();
    assert_eq!(
        code(conn.execute("INSERT INTO users VALUES ($1, 10, 'dup', 1, 1)", &[&b])),
        squad::sqlstate::UNIQUE_VIOLATION
    );
    assert_eq!(
        code(conn.query("SELECT 1 FROM users WHERE project_id = $1", &[&b])),
        squad::sqlstate::IN_FAILED_SQL_TRANSACTION
    );
    conn.batch_execute("ROLLBACK").unwrap();
    assert_eq!(conn.query("SELECT 1 FROM users WHERE project_id = $1", &[&b]).unwrap().len(), 2);

    // An empty transaction, and transaction control without one.
    conn.batch_execute("BEGIN; COMMIT; COMMIT; ROLLBACK").unwrap();
    assert_eq!(code(conn.batch_execute("SAVEPOINT s")), squad::sqlstate::NO_ACTIVE_SQL_TRANSACTION);

    // Isolation between two transactions on the same shard: the second
    // writer of a row waits or fails, never loses an update.
    let t1 = conn.transaction().unwrap();
    t1.execute("UPDATE users SET score = 100 WHERE project_id = $1 AND id = 10", &[&b]).unwrap();
    let seen: i32 = other.query_one("SELECT score FROM users WHERE project_id = $1 AND id = 10", &[&b]).unwrap().get(0);
    assert_eq!(seen, 1);
    t1.commit().unwrap();
    let seen: i32 = other.query_one("SELECT score FROM users WHERE project_id = $1 AND id = 10", &[&b]).unwrap().get(0);
    assert_eq!(seen, 100);
}

#[test]
fn queries_across_shards() {
    let (cluster, conn) = memory(5);
    conn.execute("INSERT INTO plans VALUES (1, 'free'), (2, 'team'), (3, 'business')", &[]).unwrap();
    let users: Vec<Vec<Value>> = (0..400)
        .map(|i| {
            vec![
                Value::Int8((i % 23) as i64),
                Value::Int4(i),
                Value::Text(format!("u{i}@x")),
                Value::Int4(1 + i % 3),
                Value::Int4(i % 50),
            ]
        })
        .collect();
    conn.insert_rows("users", &["project_id", "id", "email", "plan_id", "score"], users).unwrap();
    assert!(rows_per_shard(&cluster, "users").iter().all(|n| *n > 0));

    // Merged aggregates.
    let row = conn
        .query_one(
            "SELECT count(*), sum(score), avg(score), min(score), max(id), count(DISTINCT plan_id) FROM users",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<i64>(0), 400);
    assert_eq!(row.get::<i64>(1), (0..400).map(|i| i % 50).sum::<i64>());
    assert_eq!(row.get::<i32>(3), 0);
    assert_eq!(row.get::<i32>(4), 399);
    assert_eq!(row.get::<i64>(5), 3);
    let avg: f64 = conn.query_one("SELECT avg(score)::float8 FROM users", &[]).unwrap().get(0);
    assert!((avg - 24.5).abs() < 1e-9, "{avg}");

    // GROUP BY, HAVING, ORDER BY, LIMIT and OFFSET on merged groups.
    let rows = conn
        .query(
            "SELECT p.label, count(*) AS n, sum(u.score) FROM users u JOIN plans p ON p.id = u.plan_id \
             GROUP BY p.label HAVING count(*) > 100 ORDER BY n DESC, p.label LIMIT 2 OFFSET 1",
            &[],
        )
        .unwrap();
    let got: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![("business".to_string(), 133), ("team".to_string(), 133)]);
    assert_eq!(rows[0].columns().iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["label", "n", "sum"]);

    // Plain rows: sorted, de-duplicated and limited on the coordinator.
    let ids: Vec<i32> = conn
        .query("SELECT id FROM users ORDER BY id DESC LIMIT 3 OFFSET 2", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![397, 396, 395]);
    let plans: Vec<i32> = conn
        .query("SELECT DISTINCT plan_id FROM users ORDER BY plan_id", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(plans, vec![1, 2, 3]);
    assert_eq!(conn.query("SELECT email FROM users WHERE score = 49 ORDER BY project_id", &[]).unwrap().len(), 8);
    assert_eq!(conn.query("SELECT * FROM users", &[]).unwrap().len(), 400);

    // Writes without a key change every shard's rows.
    assert_eq!(conn.execute("UPDATE users SET score = score + 1 WHERE plan_id = 2", &[]).unwrap(), 133);
    assert_eq!(
        conn.execute("DELETE FROM users WHERE score = 0", &[]).unwrap(),
        (0..400).filter(|i| i % 50 == 0 && i % 3 != 1).count() as u64
    );
    let n = conn.query("DELETE FROM users WHERE project_id IN (1, 2, 3) RETURNING id", &[]).unwrap().len();
    assert!(n > 40, "{n}");

    // Reference tables read like ordinary tables, from one shard.
    let n: i64 = conn.query_one("SELECT count(*) FROM plans", &[]).unwrap().get(0);
    assert_eq!(n, 3);
    assert_eq!(conn.execute("UPDATE plans SET label = upper(label)", &[]).unwrap(), 3);
    for shard in cluster.shards() {
        let label: String =
            shard.database().unwrap().connect().query_one("SELECT label FROM plans WHERE id = 2", &[]).unwrap().get(0);
        assert_eq!(label, "TEAM");
    }
}

#[test]
fn what_cannot_be_answered_correctly_is_rejected() {
    let (_cluster, conn) = memory(3);
    conn.batch_execute("CREATE VIEW v AS SELECT * FROM users").unwrap();
    for sql in [
        // Sketches of different shards cannot be combined.
        "SELECT approx_count_distinct(user_id) FROM events",
        "SELECT approx_distinct(user_id) FROM events",
        "SELECT name, approx_percentile(value, 0.5) FROM events GROUP BY name",
        // Aggregates that need all rows in one place.
        "SELECT string_agg(email, ',' ORDER BY id) FROM users",
        "SELECT array_agg(id) FROM users",
        "SELECT stddev(score) FROM users",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score) FROM users",
        "SELECT sum(DISTINCT score) FROM users",
        "SELECT count(DISTINCT (id, score)) FROM users",
        // Window functions, set operations, DISTINCT ON.
        "SELECT id, row_number() OVER (ORDER BY id) FROM users",
        "SELECT id FROM users UNION SELECT user_id FROM events",
        "SELECT DISTINCT ON (plan_id) plan_id, id FROM users ORDER BY plan_id, id",
        "SELECT plan_id, count(*) FROM users GROUP BY ROLLUP (plan_id)",
        // Joins and subqueries that would need rows of other shards.
        "SELECT count(*) FROM users u JOIN events e ON e.user_id = u.id",
        "SELECT count(*) FROM users WHERE id IN (SELECT user_id FROM events)",
        "SELECT * FROM (SELECT project_id, count(*) AS n FROM users GROUP BY project_id) t WHERE n > 1",
        "WITH big AS (SELECT * FROM users WHERE score > 3) SELECT count(*) FROM big",
        "SELECT count(*) FROM users u, events e WHERE u.project_id = 1 AND e.project_id = 2",
        "UPDATE users SET score = 1 WHERE id IN (SELECT user_id FROM events)",
        "DELETE FROM plans WHERE id IN (SELECT plan_id FROM users)",
        // Rows cannot move between shards.
        "UPDATE users SET project_id = 2 WHERE project_id = 1",
        "SELECT * FROM users FOR UPDATE",
        // Others.
        "SELECT * FROM v",
        "SELECT * INTO copy FROM users",
        "PREPARE p AS SELECT 1",
        "SELECT id FROM users ORDER BY id LIMIT 1 + 1",
    ] {
        let err = match conn.query(sql, &[]) {
            Ok(rows) => panic!("{sql}\nwas answered with {} rows", rows.len()),
            Err(e) => e,
        };
        assert_eq!(err.code(), squad::sqlstate::FEATURE_NOT_SUPPORTED, "{sql}: {err}");
        assert!(err.hint().is_some() || err.message().contains("not supported"), "{sql}: {err:?}");
    }
    // Shapes that can never run are refused when they are prepared.
    assert_eq!(
        code(conn.prepare("SELECT id, row_number() OVER (ORDER BY id) FROM users")),
        squad::sqlstate::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        code(conn.prepare("SELECT stddev(score) FROM users WHERE score > $1")),
        squad::sqlstate::FEATURE_NOT_SUPPORTED
    );
    // With the key (or grouped by it) the same shapes work.
    for sql in [
        "SELECT approx_count_distinct(user_id) FROM events WHERE project_id = 1",
        "SELECT project_id, approx_distinct(user_id) FROM events GROUP BY project_id",
        "SELECT project_id, approx_percentile(value, 0.5) FROM events GROUP BY project_id",
        "SELECT string_agg(email, ',' ORDER BY id) FROM users WHERE project_id = 1",
        "SELECT id, row_number() OVER (ORDER BY id) FROM users WHERE project_id = 1",
        "SELECT id FROM users WHERE project_id = 1 UNION SELECT user_id FROM events WHERE project_id = 1",
        "SELECT count(*) FROM users WHERE project_id = 1 AND id IN (SELECT user_id FROM events WHERE project_id = 1)",
        "WITH big AS (SELECT * FROM users WHERE project_id = 1 AND score > 3) SELECT count(*) FROM big",
        "SELECT * FROM users WHERE project_id = 1 FOR UPDATE",
        "SELECT project_id, array_agg(id) FROM users GROUP BY project_id",
        "SELECT project_id, mode() WITHIN GROUP (ORDER BY score) FROM users GROUP BY project_id",
    ] {
        conn.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[test]
fn prepared_statements_route_per_execution() {
    let (cluster, conn) = memory(4);
    let insert = conn.prepare("INSERT INTO users (project_id, id, score) VALUES ($1, $2, $3)").unwrap();
    assert!(insert.columns().is_empty());
    for tenant in 0..20i64 {
        for id in 0..5i32 {
            assert_eq!(insert.execute(&[&tenant, &id, &(id * 10)]).unwrap(), 1);
        }
    }
    assert_placement(&cluster);
    let by_key = conn.prepare("SELECT id, score FROM users WHERE project_id = $1 AND id = $2").unwrap();
    assert_eq!(by_key.columns().iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["id", "score"]);
    for tenant in 0..20i64 {
        let row = by_key.query_one(&[&tenant, &3i32]).unwrap();
        assert_eq!(row.get::<i32>("score"), 30);
        assert!(by_key.query_opt(&[&tenant, &99i32]).unwrap().is_none());
    }
    let total = conn.prepare("SELECT count(*) AS n, sum(score) AS s FROM users WHERE score >= $1").unwrap();
    assert_eq!(total.columns().iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["n", "s"]);
    let row = total.query_one(&[&20i32]).unwrap();
    assert_eq!((row.get::<i64>(0), row.get::<i64>(1)), (60, 20 * 90));
    assert_eq!(code(conn.prepare("SELECT 1; SELECT 2")), squad::sqlstate::SYNTAX_ERROR);
    assert_eq!(code(conn.prepare("SELECT nope FROM users")), squad::sqlstate::UNDEFINED_COLUMN);
    assert_eq!(code(conn.query("SELECT 1; SELECT $1", &[&1i32])), squad::sqlstate::SYNTAX_ERROR);
}

#[test]
fn errors_of_a_shard_surface_with_their_code() {
    let (cluster, conn) = memory(4);
    let (a, b) = two_tenants(&cluster);
    conn.execute("INSERT INTO users VALUES ($1, 1, 'a', 1, 0), ($2, 1, 'b', 1, 5)", &[&a, &b]).unwrap();
    // One shard.
    assert_eq!(
        code(conn.execute("INSERT INTO users VALUES ($1, 1, 'dup', 1, 0)", &[&a])),
        squad::sqlstate::UNIQUE_VIOLATION
    );
    assert_eq!(
        code(conn.execute("INSERT INTO users VALUES ($1, NULL, 'x', 1, 0)", &[&a])),
        squad::sqlstate::NOT_NULL_VIOLATION
    );
    assert_eq!(
        code(conn.query("SELECT nope FROM users WHERE project_id = $1", &[&a])),
        squad::sqlstate::UNDEFINED_COLUMN
    );
    assert_eq!(code(conn.query("SELEC 1", &[])), squad::sqlstate::SYNTAX_ERROR);
    // A scatter query that fails on one shard only (division by the score
    // of tenant a) fails as a whole.
    assert_eq!(code(conn.query("SELECT 10 / score FROM users", &[])), squad::sqlstate::DIVISION_BY_ZERO);
    assert_eq!(code(conn.query("SELECT sum(10 / score) FROM users", &[])), squad::sqlstate::DIVISION_BY_ZERO);
    assert_eq!(
        code(conn.query("SELECT count(*) FROM users WHERE project_id IN ($1, $2) AND 1 / score > 0", &[&a, &b])),
        squad::sqlstate::DIVISION_BY_ZERO
    );
    // A write to several shards that fails on one of them says what stayed.
    let err =
        conn.execute("INSERT INTO users VALUES ($1, 1, 'dup', 1, 0), ($2, 2, 'new', 1, 0)", &[&a, &b]).unwrap_err();
    assert_eq!(err.code(), squad::sqlstate::UNIQUE_VIOLATION);
    assert!(err.detail().unwrap().contains("Shards do not share a transaction"), "{err:?}");
    let n: i64 = conn.query_one("SELECT count(*) FROM users WHERE project_id = $1", &[&b]).unwrap().get(0);
    assert_eq!(n, 2);
    // A write to a reference table that violates a constraint fails on the
    // first shard and changes none.
    conn.execute("INSERT INTO plans VALUES (1, 'free')", &[]).unwrap();
    let err = conn.execute("INSERT INTO plans VALUES (2, 'team'), (1, 'again')", &[]).unwrap_err();
    assert_eq!(err.code(), squad::sqlstate::UNIQUE_VIOLATION);
    assert!(err.detail().is_none_or(|d| !d.contains("after it was applied")), "{err:?}");
    assert_eq!(rows_per_shard(&cluster, "plans"), vec![1; 4]);
    // The connection is usable afterwards.
    let n: i64 = conn.query_one("SELECT count(*) FROM users", &[]).unwrap().get(0);
    assert_eq!(n, 3);
}

#[test]
fn connections_share_the_cluster_across_threads() {
    let (cluster, conn) = memory(4);
    conn.execute("INSERT INTO plans VALUES (1, 'free')", &[]).unwrap();
    std::thread::scope(|scope| {
        for t in 0..6i64 {
            let cluster = cluster.clone();
            scope.spawn(move || {
                let conn = cluster.connect();
                for i in 0..150i32 {
                    let tenant = (t * 31 + i as i64) % 40;
                    conn.execute("INSERT INTO users VALUES ($1, $2, 'x', 1, 1)", &[&tenant, &(t as i32 * 1000 + i)])
                        .unwrap();
                    if i % 10 == 0 {
                        let n: i64 = conn.query_one("SELECT count(*) FROM users", &[]).unwrap().get(0);
                        assert!(n > 0);
                        let tx = conn.transaction().unwrap();
                        tx.execute("UPDATE users SET score = score + 1 WHERE project_id = $1", &[&tenant]).unwrap();
                        tx.commit().unwrap();
                    }
                }
            });
        }
    });
    let n: i64 = conn.query_one("SELECT count(*) FROM users", &[]).unwrap().get(0);
    assert_eq!(n, 900);
    assert_placement(&cluster);
}

#[test]
fn settings_apply_to_every_shard_connection() {
    let (_cluster, conn) = memory(3);
    conn.batch_execute("SET timezone = 'Europe/Berlin'").unwrap();
    conn.execute("INSERT INTO events VALUES (1, 1, 'a', 1, '2026-01-01 00:00:00+00'), (2, 1, 'a', 1, '2026-01-01 00:00:00+00'), (3, 1, 'a', 1, '2026-01-01 00:00:00+00')", &[]).unwrap();
    // Shard connections opened after the SET see it too.
    for tenant in 1..=3i64 {
        let at: String =
            conn.query_one("SELECT at::text FROM events WHERE project_id = $1", &[&tenant]).unwrap().get(0);
        assert_eq!(at, "2026-01-01 01:00:00+01");
    }
    let shown: String = conn.query_one("SHOW timezone", &[]).unwrap().get(0);
    assert_eq!(shown, "Europe/Berlin");
}
