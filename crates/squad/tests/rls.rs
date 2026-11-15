//! Logins, row-level security and read-only logins through the embedding
//! API. The behaviour of an owner (squad's default login) is compared
//! with PostgreSQL 16 in `tests/regress/rls.sql`; these tests cover what a
//! single PostgreSQL session cannot show: other logins, the plan cache
//! shared between them, columnar tables and persistence. Expected results
//! follow PostgreSQL 16's documentation of row security policies.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use squad::{sqlstate, ColumnarConfig, Config, Connection, Database, Login};

fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).iter().map(|r| r.get::<i64>(0)).collect()
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_one(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

fn text(conn: &Connection, sql: &str) -> String {
    conn.query_one(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

fn code(conn: &Connection, sql: &str) -> String {
    match conn.batch_execute(sql) {
        Ok(()) => panic!("{sql}: succeeded"),
        Err(e) => e.code().to_string(),
    }
}

fn tenant(name: &str) -> Login {
    Login { user: name.to_string(), bypass_rls: false, read_only: false }
}

const SCHEMA: &str = "
    CREATE TABLE docs (id bigint PRIMARY KEY, tenant text NOT NULL, body text);
    INSERT INTO docs VALUES (1, 'acme', 'a1'), (2, 'globex', 'g1'), (3, 'acme', 'a2'), (4, 'initech', 'i1');
    ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
    CREATE POLICY tenant_isolation ON docs USING (tenant = current_setting('app.current_tenant', true));
";

#[test]
fn logins_are_subject_to_policies_unless_they_bypass_them() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    assert_eq!(text(&owner, "SELECT current_user"), "squad");

    let superset = db.connect_as(tenant("superset"));
    assert_eq!(text(&superset, "SELECT current_user"), "superset");
    assert_eq!(text(&superset, "SELECT session_user"), "superset");
    assert_eq!(text(&superset, "SHOW is_superuser"), "off");
    // An unset tenant shows nothing.
    assert_eq!(count(&superset, "SELECT count(*) FROM docs"), 0);
    superset.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    assert_eq!(ints(&superset, "SELECT id FROM docs ORDER BY id"), [1, 3]);
    superset.query("SELECT set_config('app.current_tenant', 'globex', false)", &[]).unwrap();
    assert_eq!(ints(&superset, "SELECT id FROM docs ORDER BY id"), [2]);
    // The owner is not subject to its own policies without FORCE.
    assert_eq!(count(&owner, "SELECT count(*) FROM docs"), 4);
    owner.batch_execute("ALTER TABLE docs FORCE ROW LEVEL SECURITY").unwrap();
    assert_eq!(count(&owner, "SELECT count(*) FROM docs"), 0);
    owner.batch_execute("ALTER TABLE docs NO FORCE ROW LEVEL SECURITY").unwrap();
    // Without RLS on the table, the policies are kept but not applied.
    owner.batch_execute("ALTER TABLE docs DISABLE ROW LEVEL SECURITY").unwrap();
    assert_eq!(count(&superset, "SELECT count(*) FROM docs"), 4);
    owner.batch_execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY").unwrap();
    assert_eq!(count(&superset, "SELECT count(*) FROM docs"), 1);

    // A login that bypasses row-level security, like the default one.
    let admin = db.connect_as(Login::new("admin"));
    assert_eq!(count(&admin, "SELECT count(*) FROM docs"), 4);
    assert_eq!(text(&admin, "SHOW is_superuser"), "on");
}

#[test]
fn policies_to_a_login_apply_to_that_login_only() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    owner
        .batch_execute(
            "CREATE POLICY auditor_reads ON docs FOR SELECT TO auditor USING (true);
             CREATE POLICY no_initech ON docs AS RESTRICTIVE TO auditor, superset USING (tenant <> 'initech');",
        )
        .unwrap();
    let auditor = db.connect_as(tenant("auditor"));
    let superset = db.connect_as(tenant("superset"));
    assert_eq!(ints(&auditor, "SELECT id FROM docs ORDER BY id"), [1, 2, 3]);
    assert_eq!(count(&superset, "SELECT count(*) FROM docs"), 0);
    superset.batch_execute("SET app.current_tenant = 'initech'").unwrap();
    assert_eq!(count(&superset, "SELECT count(*) FROM docs"), 0);
    let other = db.connect_as(tenant("other"));
    other.batch_execute("SET app.current_tenant = 'initech'").unwrap();
    assert_eq!(ints(&other, "SELECT id FROM docs"), [4]);
    // The auditor may read every tenant's rows but change none of them:
    // the SELECT policy does not apply to UPDATE.
    assert_eq!(auditor.execute("UPDATE docs SET body = 'x'", &[]).unwrap(), 0);

    // The catalogs show the policies and the logins they name.
    let roles: Vec<String> = owner
        .query("SELECT roles::text FROM pg_policies WHERE tablename = 'docs' ORDER BY policyname", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(roles, ["{auditor}", "{auditor,superset}", "{public}"]);
    let quals: Vec<String> = owner
        .query("SELECT qual FROM pg_policies WHERE tablename = 'docs' ORDER BY policyname", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(quals, ["true", "(tenant <> 'initech')", "(tenant = current_setting('app.current_tenant', true))"]);
    // psql's \d asks this (PostgreSQL 16 describe.c).
    let described: Vec<(String, bool, Option<String>, String)> = owner
        .query(
            "SELECT pol.polname, pol.polpermissive,
               CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
               CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE 'ALL' END AS cmd
             FROM pg_catalog.pg_policy pol
             WHERE pol.polrelid = 'docs'::regclass ORDER BY 1",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(
        described,
        [
            ("auditor_reads".to_string(), true, Some("auditor".to_string()), "SELECT".to_string()),
            ("no_initech".to_string(), false, Some("auditor,superset".to_string()), "ALL".to_string()),
            ("tenant_isolation".to_string(), true, None, "ALL".to_string()),
        ]
    );
}

#[test]
fn policies_on_indexed_columns_use_the_index() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    owner.batch_execute("CREATE INDEX docs_tenant ON docs (tenant)").unwrap();
    let app = db.connect_as(tenant("app"));
    app.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    let plan = text(&app, "EXPLAIN SELECT * FROM docs");
    assert_eq!(plan, "Index Scan using docs_tenant on docs  (Index Cond: tenant = ?)");
    assert_eq!(ints(&app, "SELECT id FROM docs ORDER BY id"), [1, 3]);
    app.batch_execute("RESET app.current_tenant").unwrap();
    assert_eq!(count(&app, "SELECT count(*) FROM docs"), 0);
    app.batch_execute("SET app.current_tenant = 'initech'").unwrap();
    assert_eq!(ints(&app, "SELECT id FROM docs WHERE id > 0"), [4]);
    // The owner's plan of the same text has no policy.
    assert_eq!(text(&owner, "EXPLAIN SELECT * FROM docs"), "Seq Scan on docs");
}

#[test]
fn the_shared_plan_cache_keeps_logins_apart() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    let sql = "SELECT count(*) FROM docs";
    assert_eq!(count(&owner, sql), 4);
    let superset = db.connect_as(tenant("superset"));
    assert_eq!(count(&superset, sql), 0);
    assert_eq!(count(&owner, sql), 4);
    // A prepared statement follows the setting, and the login.
    let stmt = superset.prepare("SELECT id FROM docs ORDER BY id").unwrap();
    assert!(stmt.query(&[]).unwrap().is_empty());
    superset.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    assert_eq!(stmt.query(&[]).unwrap().len(), 2);
    // `SET LOCAL` inside a transaction, as poolers recommend.
    {
        let txn = superset.transaction().unwrap();
        txn.batch_execute("SET LOCAL app.current_tenant = 'globex'").unwrap();
        assert_eq!(stmt.query(&[]).unwrap().len(), 1);
        txn.commit().unwrap();
    }
    assert_eq!(stmt.query(&[]).unwrap().len(), 2);
}

#[test]
fn writes_follow_the_policies() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    let app = db.connect_as(tenant("app"));
    app.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    app.execute("INSERT INTO docs VALUES (5, 'acme', 'a3')", &[]).unwrap();
    assert_eq!(code(&app, "INSERT INTO docs VALUES (6, 'globex', 'g2')"), sqlstate::INSUFFICIENT_PRIVILEGE);
    let err = app.execute("INSERT INTO docs VALUES (6, 'globex', 'g2')", &[]).unwrap_err();
    assert_eq!(err.message(), "new row violates row-level security policy for table \"docs\"");
    assert_eq!(app.execute("UPDATE docs SET body = 'x'", &[]).unwrap(), 3);
    assert_eq!(code(&app, "UPDATE docs SET tenant = 'globex' WHERE id = 1"), sqlstate::INSUFFICIENT_PRIVILEGE);
    assert_eq!(app.execute("DELETE FROM docs WHERE id IN (1, 2)", &[]).unwrap(), 1);
    // ON CONFLICT DO UPDATE is not supported where policies apply.
    assert_eq!(
        code(&app, "INSERT INTO docs VALUES (3, 'acme', 'u') ON CONFLICT (id) DO UPDATE SET body = 'u'"),
        sqlstate::FEATURE_NOT_SUPPORTED
    );
    owner.batch_execute("CREATE POLICY drafts ON docs AS RESTRICTIVE USING (body <> 'draft')").unwrap();
    let err = app.execute("INSERT INTO docs VALUES (7, 'acme', 'draft')", &[]).unwrap_err();
    assert_eq!(err.message(), "new row violates row-level security policy \"drafts\" for table \"docs\"");
    assert_eq!(ints(&owner, "SELECT id FROM docs ORDER BY id"), [2, 3, 4, 5]);
}

#[test]
fn logins_subject_to_policies_cannot_change_protected_tables() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    owner.batch_execute("CREATE TABLE free (a int)").unwrap();
    let app = db.connect_as(tenant("app"));
    for sql in [
        "ALTER TABLE docs DISABLE ROW LEVEL SECURITY",
        "ALTER TABLE free ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE docs ADD COLUMN x int",
        "ALTER TABLE docs RENAME TO d2",
        "ALTER TABLE docs RENAME COLUMN body TO b",
        "CREATE POLICY p ON docs USING (true)",
        "CREATE POLICY p ON free USING (true)",
        "ALTER POLICY tenant_isolation ON docs USING (true)",
        "ALTER POLICY tenant_isolation ON docs RENAME TO p",
        "DROP POLICY tenant_isolation ON docs",
        "DROP TABLE docs",
        "TRUNCATE docs",
        "CREATE INDEX ON docs (body)",
        "SET ROLE squad",
        "SET SESSION AUTHORIZATION squad",
    ] {
        let expected =
            if sql.starts_with("SET") { sqlstate::FEATURE_NOT_SUPPORTED } else { sqlstate::INSUFFICIENT_PRIVILEGE };
        assert_eq!(code(&app, sql), expected, "{sql}");
    }
    // Tables without row-level security stay as open as before.
    app.batch_execute("ALTER TABLE free ADD COLUMN b int; INSERT INTO free VALUES (1, 2); DROP TABLE free").unwrap();
    app.batch_execute("SET ROLE NONE; SET ROLE app; RESET ROLE").unwrap();
    // `row_security = off` makes a filtered query fail instead.
    app.batch_execute("SET row_security = off").unwrap();
    assert_eq!(code(&app, "SELECT * FROM docs"), sqlstate::INSUFFICIENT_PRIVILEGE);
    app.batch_execute("RESET row_security").unwrap();
    assert_eq!(count(&app, "SELECT count(*) FROM docs"), 0);
}

#[test]
fn views_apply_the_policies_for_the_querying_login() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner.batch_execute(SCHEMA).unwrap();
    owner
        .batch_execute(
            "CREATE TABLE notes (doc_id bigint, note text);
             INSERT INTO notes VALUES (1, 'n1'), (2, 'n2'), (3, 'n3');
             CREATE VIEW doc_notes AS SELECT d.id, d.tenant, n.note FROM docs d JOIN notes n ON n.doc_id = d.id;",
        )
        .unwrap();
    let app = db.connect_as(tenant("app"));
    app.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    // In PostgreSQL a view runs with its owner's rights unless it is
    // created WITH (security_invoker); squad always applies the policies
    // for the login running the query.
    assert_eq!(ints(&app, "SELECT id FROM doc_notes ORDER BY id"), [1, 3]);
    assert_eq!(count(&owner, "SELECT count(*) FROM doc_notes"), 3);
    // Subqueries, CTEs and set operations see the same rows.
    assert_eq!(count(&app, "SELECT count(*) FROM notes WHERE doc_id IN (SELECT id FROM docs)"), 2);
    assert_eq!(count(&app, "WITH d AS (SELECT * FROM docs) SELECT count(*) FROM d, d AS e"), 4);
    assert_eq!(count(&app, "SELECT count(*) FROM (SELECT id FROM docs UNION SELECT doc_id FROM notes) u"), 3);
    // CREATE TABLE AS copies what the login can see.
    app.batch_execute("CREATE TABLE mine AS SELECT * FROM docs").unwrap();
    assert_eq!(count(&owner, "SELECT count(*) FROM mine"), 2);
}

#[test]
fn read_only_logins_cannot_write() {
    let db = Database::open_in_memory().unwrap();
    let owner = db.connect();
    owner
        .batch_execute(
            "CREATE TABLE t (a int PRIMARY KEY); INSERT INTO t VALUES (1); CREATE SEQUENCE s;
             CREATE TABLE c (a int) USING columnar; INSERT INTO c VALUES (1), (2);",
        )
        .unwrap();
    let reader = db.connect_as(Login { user: "reader".into(), bypass_rls: true, read_only: true });
    assert_eq!(count(&reader, "SELECT count(*) FROM t"), 1);
    assert_eq!(text(&reader, "SHOW transaction_read_only"), "on");
    assert_eq!(text(&reader, "SHOW default_transaction_read_only"), "on");
    for sql in [
        "INSERT INTO t VALUES (2)",
        "UPDATE t SET a = 2",
        "DELETE FROM t",
        "INSERT INTO t VALUES (2) ON CONFLICT DO NOTHING",
        "WITH x AS (DELETE FROM t RETURNING a) SELECT * FROM x",
        "SELECT * FROM t FOR UPDATE",
        "SELECT nextval('s')",
        "SELECT setval('s', 5)",
        "CREATE TABLE u (a int)",
        "CREATE TABLE u AS SELECT 1",
        "DROP TABLE t",
        "TRUNCATE t",
        "ALTER TABLE t ADD COLUMN b int",
        "CREATE INDEX ON t (a)",
        "CREATE VIEW v AS SELECT 1",
        "CREATE SEQUENCE s2",
        "CREATE POLICY p ON t USING (true)",
        "BEGIN READ WRITE",
        "BEGIN; SET TRANSACTION READ WRITE",
        "SET transaction_read_only = off",
        "SET default_transaction_read_only = off; INSERT INTO t VALUES (3)",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE; INSERT INTO t VALUES (3)",
    ] {
        assert_eq!(code(&reader, sql), sqlstate::READ_ONLY_SQL_TRANSACTION, "{sql}");
        reader.batch_execute("ROLLBACK").unwrap();
    }
    reader.append("t", &[vec![squad::Value::Int4(9)]]).unwrap_err();
    // Reading, settings, transactions, prepared statements, advisory
    // locks and VACUUM work.
    reader.batch_execute("BEGIN; SET LOCAL app.x = '1'; SELECT * FROM t; COMMIT; PREPARE q AS SELECT 1").unwrap();
    reader.batch_execute("SELECT pg_advisory_lock(1); SELECT pg_advisory_unlock(1)").unwrap();
    reader.batch_execute("VACUUM").unwrap();
    assert_eq!(count(&reader, "SELECT count(*) FROM c"), 2);
    assert_eq!(ints(&owner, "SELECT a::bigint FROM t"), [1]);
}

/// Counts which segment objects a query reads.
struct Counting {
    inner: squad::object::MemoryStore,
    ranges: AtomicU64,
    keys: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl Counting {
    fn take(&self) -> (u64, usize) {
        let keys = std::mem::take(&mut *self.keys.lock().unwrap()).len();
        (self.ranges.swap(0, Ordering::SeqCst), keys)
    }
}

impl squad::object::ObjectStore for Counting {
    fn put(&self, key: &str, data: &[u8]) -> squad::object::Result<()> {
        self.inner.put(key, data)
    }
    fn get(&self, key: &str) -> squad::object::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> squad::object::Result<Vec<u8>> {
        if key.starts_with("columnar/") {
            self.ranges.fetch_add(1, Ordering::SeqCst);
            self.keys.lock().unwrap().insert(key.to_string());
        }
        self.inner.get_range(key, offset, len)
    }
    fn head(&self, key: &str) -> squad::object::Result<Option<squad::object::ObjectMeta>> {
        self.inner.head(key)
    }
    fn list(&self, prefix: &str) -> squad::object::Result<Vec<squad::object::ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> squad::object::Result<()> {
        self.inner.delete(key)
    }
}

#[test]
fn columnar_tables_apply_policies_in_the_column_engine() {
    let store = Arc::new(Counting {
        inner: squad::object::MemoryStore::new(),
        ranges: AtomicU64::new(0),
        keys: Default::default(),
    });
    let engine: Arc<dyn squad_storage::Engine> = Arc::new(squad_storage::MemEngine::new());
    let manual = ColumnarConfig { background_compaction: false, ..ColumnarConfig::default() };
    let db = Database::with_engine_and_objects(engine.clone(), store.clone(), manual.clone()).unwrap();
    let owner = db.connect();
    owner
        .batch_execute(
            "CREATE TABLE events (tenant text NOT NULL, n bigint NOT NULL, kind text)
                 USING columnar WITH (order_by = 'tenant, n');
             CREATE TABLE plain (tenant text NOT NULL, n bigint NOT NULL, kind text);",
        )
        .unwrap();
    // One segment per tenant, then rows in the delta.
    for (i, t) in ["acme", "globex", "initech", "umbrella"].iter().enumerate() {
        for table in ["events", "plain"] {
            owner
                .execute(
                    &format!("INSERT INTO {table} SELECT $1, g, 'k' || (g % 3) FROM generate_series(1, 5000 + $2) g"),
                    &[t, &(i as i64)],
                )
                .unwrap();
        }
        owner.batch_execute("VACUUM events").unwrap();
    }
    for table in ["events", "plain"] {
        owner
            .execute(&format!("INSERT INTO {table} VALUES ('acme', -1, 'late'), ('globex', -2, 'late')"), &[])
            .unwrap();
        owner
            .batch_execute(&format!(
                "ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
                 CREATE POLICY tenant_isolation ON {table} USING (tenant = current_setting('app.current_tenant', true));",
            ))
            .unwrap();
    }
    let app = db.connect_as(tenant("superset"));
    let queries = [
        "SELECT count(*) FROM {t}",
        "SELECT count(*), sum(n), min(n), max(n) FROM {t}",
        "SELECT kind, count(*) FROM {t} GROUP BY kind ORDER BY kind",
        "SELECT count(*) FROM {t} WHERE n > 100 AND kind = 'k1'",
        "SELECT tenant, count(*) FROM {t} GROUP BY tenant ORDER BY tenant",
        "SELECT n FROM {t} WHERE n < 3 ORDER BY n",
    ];
    let answer = |conn: &Connection, sql: &str| -> Vec<String> {
        conn.simple_query(sql)
            .unwrap()
            .iter()
            .flat_map(|r| r.rows.iter())
            .map(|row| row.iter().map(|v| format!("{v:?}")).collect::<Vec<_>>().join("|"))
            .collect()
    };
    for setting in ["RESET app.current_tenant", "SET app.current_tenant = 'acme'", "SET app.current_tenant = 'initech'"]
    {
        app.batch_execute(setting).unwrap();
        for q in queries {
            let plain = answer(&app, &q.replace("{t}", "plain"));
            let col = answer(&app, &q.replace("{t}", "events"));
            assert_eq!(col, plain, "{setting}: {q}");
        }
    }
    // The aggregation runs in the column engine, and the policy prunes
    // the segments of other tenants before they are read (a fresh handle
    // starts with an empty cache).
    drop((app, owner, db));
    let db = Database::with_engine_and_objects(engine.clone(), store.clone(), manual.clone()).unwrap();
    let owner = db.connect();
    let app = db.connect_as(tenant("superset"));
    app.batch_execute("SET app.current_tenant = 'initech'").unwrap();
    let plan = answer(&app, "EXPLAIN SELECT count(*), sum(n) FROM events");
    assert!(plan[0].contains("Columnar Aggregate"), "{plan:?}");
    store.take();
    let sum: i64 = app.query_one("SELECT sum(n)::bigint FROM events", &[]).unwrap().get(0);
    assert_eq!(sum, 5002 * 5003 / 2);
    let (_, segments) = store.take();
    assert_eq!(segments, 1, "only the segment of one tenant is read");

    // Inserts are checked, deletes are filtered.
    app.batch_execute("SET app.current_tenant = 'acme'").unwrap();
    assert_eq!(code(&app, "INSERT INTO events VALUES ('globex', 1, 'x')"), sqlstate::INSUFFICIENT_PRIVILEGE);
    app.execute("INSERT INTO events VALUES ('acme', 0, 'x')", &[]).unwrap();
    assert_eq!(app.execute("DELETE FROM events WHERE n <= 0", &[]).unwrap(), 2);
    assert_eq!(count(&owner, "SELECT count(*) FROM events WHERE n <= 0"), 1);
    assert_eq!(code(&app, "UPDATE events SET kind = 'y'"), sqlstate::FEATURE_NOT_SUPPORTED);
}

#[test]
fn policies_survive_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path(), Config::default()).unwrap();
        db.connect().batch_execute(SCHEMA).unwrap();
        db.connect().batch_execute("ALTER TABLE docs FORCE ROW LEVEL SECURITY").unwrap();
    }
    let db = Database::open(dir.path(), Config::default()).unwrap();
    let conn = db.connect();
    assert_eq!(count(&conn, "SELECT count(*) FROM docs"), 0);
    conn.batch_execute("SET app.current_tenant = 'globex'").unwrap();
    assert_eq!(ints(&conn, "SELECT id FROM docs"), [2]);
    assert_eq!(text(&conn, "SELECT policyname::text FROM pg_policies"), "tenant_isolation");
}

#[test]
fn renaming_what_a_policy_uses_is_refused() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connect();
    conn.batch_execute(SCHEMA).unwrap();
    assert_eq!(code(&conn, "ALTER TABLE docs RENAME COLUMN tenant TO t"), sqlstate::FEATURE_NOT_SUPPORTED);
    conn.batch_execute("ALTER TABLE docs RENAME COLUMN body TO text; ALTER TABLE docs RENAME TO documents").unwrap();
    conn.batch_execute("CREATE POLICY own ON documents FOR SELECT USING (documents.id > 0)").unwrap();
    assert_eq!(code(&conn, "ALTER TABLE documents RENAME TO d"), sqlstate::FEATURE_NOT_SUPPORTED);
    // DROP COLUMN ... CASCADE drops the policies that use the column.
    conn.batch_execute("ALTER TABLE documents DROP COLUMN tenant CASCADE").unwrap();
    assert_eq!(text(&conn, "SELECT string_agg(policyname, ',') FROM pg_policies"), "own");
}
