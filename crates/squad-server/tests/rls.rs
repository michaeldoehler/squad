//! Login attributes and row-level security over the wire, the way an
//! analytics tool such as Superset uses them: one login per tool, the
//! tenant set per query with `SET` or `set_config`.

mod common;

use std::collections::HashMap;

use common::{have, start, start_with, Client};
use squad::Database;
use squad_server::{Auth, LoginAttributes, ServerConfig};
use tokio_postgres::{Client as PgClient, NoTls};

async fn connect(port: u16, user: &str, password: &str) -> PgClient {
    let config = format!("host=127.0.0.1 port={port} user={user} password={password} dbname=squad");
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn db_code(e: &tokio_postgres::Error) -> String {
    e.as_db_error().map(|d| d.code().code().to_string()).unwrap_or_else(|| format!("not a database error: {e}"))
}

const SCHEMA: &str = "
    CREATE TABLE events (id bigint PRIMARY KEY, tenant_id text NOT NULL, kind text, amount int);
    INSERT INTO events VALUES (1, 'acme', 'click', 5), (2, 'globex', 'click', 7),
                              (3, 'acme', 'view', 11), (4, 'initech', 'view', 13);
    CREATE TABLE metrics (tenant_id text NOT NULL, value bigint) USING columnar;
    INSERT INTO metrics SELECT CASE WHEN g % 2 = 0 THEN 'acme' ELSE 'globex' END, g FROM generate_series(1, 1000) g;
    ALTER TABLE events ENABLE ROW LEVEL SECURITY;
    ALTER TABLE metrics ENABLE ROW LEVEL SECURITY;
    CREATE POLICY tenant_isolation ON events USING (tenant_id = current_setting('app.current_tenant', true));
    CREATE POLICY tenant_isolation ON metrics USING (tenant_id = current_setting('app.current_tenant', true));
";

/// A users file with an owner login and a read-only login that is
/// subject to the policies.
fn config(dir: &std::path::Path) -> ServerConfig {
    let path = dir.join("users");
    std::fs::write(&path, "beats:owner-pw\nsuperset read_only no_bypass_rls:bi-pw\ningest no_bypass_rls:in-pw\n")
        .unwrap();
    let users = squad_server::users::read_users_file(&path).unwrap();
    ServerConfig { auth: Auth::Verifiers { users: users.verifiers }, logins: users.logins, ..Default::default() }
}

#[test]
fn superset_sees_only_the_tenant_it_sets() {
    let dir = tempfile::tempdir().unwrap();
    let (server, addr) = start(config(dir.path()));
    runtime().block_on(async {
        let beats = connect(addr.port(), "beats", "owner-pw").await;
        beats.batch_execute(SCHEMA).await.unwrap();
        let superset = connect(addr.port(), "superset", "bi-pw").await;
        let who: String = superset.query_one("SELECT current_user", &[]).await.unwrap().get(0);
        assert_eq!(who, "superset");

        // No tenant set: nothing, also through a prepared statement.
        let stmt = superset.prepare("SELECT id FROM events WHERE amount > $1 ORDER BY id").await.unwrap();
        assert!(superset.query(&stmt, &[&0i32]).await.unwrap().is_empty());
        let total: i64 = superset.query_one("SELECT count(*) FROM metrics", &[]).await.unwrap().get(0);
        assert_eq!(total, 0);

        superset.batch_execute("SET app.current_tenant = 'acme'").await.unwrap();
        let ids: Vec<i64> = superset.query(&stmt, &[&0i32]).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, [1, 3]);
        let row = superset.query_one("SELECT count(*), sum(value)::bigint FROM metrics", &[]).await.unwrap();
        assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (500, 250_500));

        // set_config in the query itself, as Superset's per-query hooks do.
        let rows = superset
            .simple_query("SELECT set_config('app.current_tenant', 'globex', false); SELECT id FROM events")
            .await
            .unwrap();
        let ids: Vec<String> = rows
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
                _ => None,
            })
            .filter(|v| v != "globex")
            .collect();
        assert_eq!(ids, ["2"]);
        let joined: i64 = superset
            .query_one("SELECT count(*) FROM events e JOIN metrics m ON m.tenant_id = e.tenant_id", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(joined, 500);

        // Transaction-local tenant.
        superset.batch_execute("BEGIN; SET LOCAL app.current_tenant = 'initech'").await.unwrap();
        let ids: Vec<i64> =
            superset.query("SELECT id FROM events", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, [4]);
        superset.batch_execute("COMMIT").await.unwrap();
        let tenant: String = superset.query_one("SHOW app.current_tenant", &[]).await.unwrap().get(0);
        assert_eq!(tenant, "globex");

        // COPY TO reads through the policies.
        let stream = superset.copy_out("COPY events TO STDOUT").await.unwrap();
        let mut stream = std::pin::pin!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            out.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(out, b"2\tglobex\tclick\t7\n");

        // The read-only login cannot write or change anything.
        for sql in [
            "INSERT INTO events VALUES (9, 'globex', 'x', 1)",
            "UPDATE events SET amount = 0",
            "DELETE FROM events",
            "CREATE TABLE t (a int)",
            "BEGIN READ WRITE",
            "SET default_transaction_read_only = off; INSERT INTO events VALUES (9, 'globex', 'x', 1)",
        ] {
            let err = superset.batch_execute(sql).await.unwrap_err();
            assert_eq!(db_code(&err), "25006", "{sql}");
            superset.batch_execute("ROLLBACK").await.unwrap();
        }
        let err = superset.batch_execute("ALTER TABLE events DISABLE ROW LEVEL SECURITY").await.unwrap_err();
        assert_eq!(db_code(&err), "25006");
        let err = superset.copy_in::<_, bytes::Bytes>("COPY events FROM STDIN").await.err().expect("refused");
        assert_eq!(db_code(&err), "25006");

        // The owner sees everything.
        let n: i64 = beats.query_one("SELECT count(*) FROM events", &[]).await.unwrap().get(0);
        assert_eq!(n, 4);
    });
    server.shutdown();
}

#[test]
fn a_login_subject_to_policies_writes_only_its_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let (server, addr) = start(config(dir.path()));
    runtime().block_on(async {
        let beats = connect(addr.port(), "beats", "owner-pw").await;
        beats.batch_execute(SCHEMA).await.unwrap();
        let ingest = connect(addr.port(), "ingest", "in-pw").await;
        ingest.batch_execute("SET app.current_tenant = 'acme'").await.unwrap();
        let insert = ingest.prepare("INSERT INTO events VALUES ($1, $2, 'x', 1)").await.unwrap();
        ingest.execute(&insert, &[&10i64, &"acme"]).await.unwrap();
        let err = ingest.execute(&insert, &[&11i64, &"globex"]).await.unwrap_err();
        assert_eq!(db_code(&err), "42501");
        assert_eq!(
            err.as_db_error().unwrap().message(),
            "new row violates row-level security policy for table \"events\""
        );
        assert_eq!(ingest.execute("UPDATE events SET amount = amount + 1", &[]).await.unwrap(), 3);
        let err = ingest.batch_execute("DROP POLICY tenant_isolation ON events").await.unwrap_err();
        assert_eq!(db_code(&err), "42501");
        // COPY FROM is refused for tables whose policies apply, as in
        // PostgreSQL.
        let err = ingest.copy_in::<_, bytes::Bytes>("COPY events FROM STDIN").await.err().expect("COPY FROM refused");
        assert_eq!(db_code(&err), "0A000");
        let n: i64 = beats.query_one("SELECT count(*) FROM events WHERE amount > 5", &[]).await.unwrap().get(0);
        assert_eq!(n, 4);
    });
    server.shutdown();
}

#[test]
fn logins_without_attributes_keep_full_access() {
    // A trusting server with attributes for one name.
    let config = ServerConfig {
        logins: HashMap::from([
            ("reader".to_string(), LoginAttributes { bypass_rls: true, read_only: true }),
            ("tenant".to_string(), LoginAttributes { bypass_rls: false, read_only: false }),
        ]),
        ..Default::default()
    };
    let (server, addr) = start_with(Database::open_in_memory().unwrap(), config);
    let mut squad = Client::startup(addr, &[("user", "squad")]);
    squad.until_ready();
    squad.rows("CREATE TABLE t (a int); INSERT INTO t VALUES (1)");
    assert_eq!(squad.rows("SHOW is_superuser"), [[Some("on".to_string())]]);
    let mut reader = Client::startup(addr, &[("user", "reader"), ("options", "-c default_transaction_read_only=off")]);
    let (startup, _) = reader.until_ready();
    assert!(startup.iter().any(|m| m.ty == b'S' && m.body == b"is_superuser\0on\0"), "{startup:?}");
    assert_eq!(reader.rows("SELECT a FROM t"), [[Some("1".to_string())]]);
    assert_eq!(reader.error("INSERT INTO t VALUES (2)"), "25006");
    assert_eq!(reader.rows("SHOW transaction_read_only"), [[Some("on".to_string())]]);
    let mut tenant = Client::startup(addr, &[("user", "tenant")]);
    let (startup, _) = tenant.until_ready();
    assert!(startup.iter().any(|m| m.ty == b'S' && m.body == b"is_superuser\0off\0"), "{startup:?}");
    assert_eq!(tenant.rows("SELECT current_user"), [[Some("tenant".to_string())]]);
    server.shutdown();
}

/// `psql -c \d table`, with footers (the policies are one).
fn describe(addr: std::net::SocketAddr, table: &str) -> String {
    let out = std::process::Command::new("psql")
        .args(["-h", "127.0.0.1", "-p", &addr.port().to_string(), "-U", "squad", "-d", "squad", "-X"])
        .args(["-c", &format!("\\d {table}")])
        .env("PGCONNECT_TIMEOUT", "10")
        .output()
        .expect("run psql");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.is_empty(), "{err}");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn psql_describes_policies() {
    if !have("psql") {
        eprintln!("skipped: psql not installed");
        return;
    }
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::startup(addr, &[("user", "squad")]);
    c.until_ready();
    c.rows(SCHEMA);
    c.rows("CREATE POLICY readers ON events AS RESTRICTIVE FOR SELECT TO superset USING (amount > 0)");
    let out = describe(addr, "events");
    assert!(out.contains("Policies:"), "{out}");
    assert!(out.contains("POLICY \"tenant_isolation\""), "{out}");
    assert!(out.contains("USING ((tenant_id = current_setting('app.current_tenant', true)))"), "{out}");
    assert!(out.contains("POLICY \"readers\" AS RESTRICTIVE FOR SELECT"), "{out}");
    assert!(out.contains("TO superset"), "{out}");
    c.rows("ALTER TABLE events FORCE ROW LEVEL SECURITY");
    let out = describe(addr, "events");
    assert!(out.contains("Policies (forced row security enabled):"), "{out}");
    server.shutdown();
}
