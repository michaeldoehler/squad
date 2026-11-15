//! The server as seen by a real driver: tokio-postgres uses the extended
//! protocol with binary parameters and results.

mod common;

use std::time::{Duration, Instant};

use common::start;
use squad_server::{Auth, ServerConfig};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

async fn connect(config: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(config, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn typed_parameters_and_results() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let client = connect(&format!("host=127.0.0.1 port={} user=squad dbname=squad", addr.port())).await;
        client
            .batch_execute(
                "CREATE TABLE t (
                    id bigint PRIMARY KEY, small smallint, n integer, name text NOT NULL,
                    ok boolean, ratio double precision, r real, data bytea, tags text[], nums integer[]
                 )",
            )
            .await
            .unwrap();

        let insert = client.prepare("INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)").await.unwrap();
        assert_eq!(
            insert.params(),
            &[
                Type::INT8,
                Type::INT2,
                Type::INT4,
                Type::TEXT,
                Type::BOOL,
                Type::FLOAT8,
                Type::FLOAT4,
                Type::BYTEA,
                Type::TEXT_ARRAY,
                Type::INT4_ARRAY
            ]
        );
        let n = client
            .execute(
                &insert,
                &[
                    &1i64,
                    &7i16,
                    &-5i32,
                    &"Grüße",
                    &true,
                    &1.25f64,
                    &0.5f32,
                    &vec![0u8, 1, 255],
                    &vec!["a", "b c"],
                    &vec![1i32, 2, 3],
                ],
            )
            .await
            .unwrap();
        assert_eq!(n, 1);
        let none_i16: Option<i16> = None;
        let none_bytes: Option<Vec<u8>> = None;
        client
            .execute(
                &insert,
                &[
                    &2i64,
                    &none_i16,
                    &Option::<i32>::None,
                    &"second",
                    &Option::<bool>::None,
                    &Option::<f64>::None,
                    &Option::<f32>::None,
                    &none_bytes,
                    &Option::<Vec<String>>::None,
                    &Option::<Vec<i32>>::None,
                ],
            )
            .await
            .unwrap();

        let select = client.prepare("SELECT * FROM t WHERE id = $1").await.unwrap();
        let columns: Vec<(&str, &Type)> = select.columns().iter().map(|c| (c.name(), c.type_())).collect();
        assert_eq!(columns[0], ("id", &Type::INT8));
        assert_eq!(columns[3], ("name", &Type::TEXT));
        assert_eq!(columns[8], ("tags", &Type::TEXT_ARRAY));
        let row = client.query_one(&select, &[&1i64]).await.unwrap();
        assert_eq!(row.get::<_, i64>("id"), 1);
        assert_eq!(row.get::<_, i16>("small"), 7);
        assert_eq!(row.get::<_, i32>("n"), -5);
        assert_eq!(row.get::<_, String>("name"), "Grüße");
        assert!(row.get::<_, bool>("ok"));
        assert_eq!(row.get::<_, f64>("ratio"), 1.25);
        assert_eq!(row.get::<_, f32>("r"), 0.5);
        assert_eq!(row.get::<_, Vec<u8>>("data"), vec![0u8, 1, 255]);
        assert_eq!(row.get::<_, Vec<String>>("tags"), vec!["a", "b c"]);
        assert_eq!(row.get::<_, Vec<i32>>("nums"), vec![1, 2, 3]);
        let row = client.query_one(&select, &[&2i64]).await.unwrap();
        assert_eq!(row.get::<_, Option<i16>>("small"), None);
        assert_eq!(row.get::<_, Option<Vec<u8>>>("data"), None);
        assert_eq!(row.get::<_, Option<Vec<String>>>("tags"), None);
        assert!(client.query_opt(&select, &[&3i64]).await.unwrap().is_none());

        // Aggregates and expressions with inferred parameter types.
        let row = client
            .query_one("SELECT count(*), sum(n), $1::text || name FROM t WHERE id = $2 GROUP BY name", &[&"> ", &1i64])
            .await
            .unwrap();
        assert_eq!(
            (row.get::<_, i64>(0), row.get::<_, i64>(1), row.get::<_, String>(2)),
            (1, -5, "> Grüße".to_string())
        );

        // Errors carry SQLSTATE and detail.
        let err = client
            .execute(
                &insert,
                &[
                    &1i64,
                    &none_i16,
                    &Option::<i32>::None,
                    &"dup",
                    &Option::<bool>::None,
                    &Option::<f64>::None,
                    &Option::<f32>::None,
                    &none_bytes,
                    &Option::<Vec<String>>::None,
                    &Option::<Vec<i32>>::None,
                ],
            )
            .await
            .unwrap_err();
        let db = err.as_db_error().expect("database error");
        assert_eq!(db.code().code(), "23505");
        assert_eq!(db.detail(), Some("Key (id)=(1) already exists."));
        // The connection keeps working after an error.
        assert_eq!(client.query_one("SELECT 1::int4", &[]).await.unwrap().get::<_, i32>(0), 1);
    });
    server.shutdown();
}

#[test]
fn transactions_and_portals() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let mut client = connect(&format!("host=127.0.0.1 port={} user=squad", addr.port())).await;
        client.batch_execute("CREATE TABLE acct (id int PRIMARY KEY, balance bigint NOT NULL)").await.unwrap();
        client.execute("INSERT INTO acct VALUES (1, 100), (2, 0)", &[]).await.unwrap();

        // A committed transfer.
        let tx = client.transaction().await.unwrap();
        tx.execute("UPDATE acct SET balance = balance - $1 WHERE id = 1", &[&30i64]).await.unwrap();
        tx.execute("UPDATE acct SET balance = balance + $1 WHERE id = 2", &[&30i64]).await.unwrap();
        tx.commit().await.unwrap();
        // A transaction dropped without commit is rolled back.
        let tx = client.transaction().await.unwrap();
        tx.execute("DELETE FROM acct", &[]).await.unwrap();
        drop(tx);
        // Savepoints.
        let mut tx = client.transaction().await.unwrap();
        let sp = tx.transaction().await.unwrap();
        sp.execute("UPDATE acct SET balance = 0", &[]).await.unwrap();
        sp.rollback().await.unwrap();
        tx.execute("UPDATE acct SET balance = balance + 1 WHERE id = 2", &[]).await.unwrap();
        tx.commit().await.unwrap();
        let rows = client.query("SELECT id, balance FROM acct ORDER BY id", &[]).await.unwrap();
        let got: Vec<(i32, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        assert_eq!(got, vec![(1, 70), (2, 31)]);

        // A portal fetched in pieces.
        let tx = client.transaction().await.unwrap();
        let stmt = tx.prepare("SELECT g FROM generate_series(1, 10) g").await.unwrap();
        let portal = tx.bind(&stmt, &[]).await.unwrap();
        let first = tx.query_portal(&portal, 4).await.unwrap();
        let second = tx.query_portal(&portal, 4).await.unwrap();
        let third = tx.query_portal(&portal, 4).await.unwrap();
        assert_eq!((first.len(), second.len(), third.len()), (4, 4, 2));
        assert_eq!(third[1].get::<_, i32>(0), 10);
        tx.commit().await.unwrap();

        // The simple protocol through the driver.
        let messages = client.simple_query("SELECT 1 AS a; SELECT 'x' AS b").await.unwrap();
        let values: Vec<String> = messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        assert_eq!(values, vec!["1", "x"]);
    });
    server.shutdown();
}

#[test]
fn copy_through_the_driver() {
    use futures_util::SinkExt;
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let client = connect(&format!("host=127.0.0.1 port={} user=squad", addr.port())).await;
        client.batch_execute("CREATE TABLE t (id int PRIMARY KEY, name text)").await.unwrap();
        let sink = client.copy_in("COPY t FROM STDIN").await.unwrap();
        let mut sink = std::pin::pin!(sink);
        sink.send(bytes::Bytes::from_static(b"1\talpha\n2\t\\N\n")).await.unwrap();
        sink.send(bytes::Bytes::from_static(b"3\tgamma\n")).await.unwrap();
        assert_eq!(sink.finish().await.unwrap(), 3);
        let rows = client.query("SELECT id, name FROM t ORDER BY id", &[]).await.unwrap();
        let got: Vec<(i32, Option<String>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        assert_eq!(got, vec![(1, Some("alpha".into())), (2, None), (3, Some("gamma".into()))]);
    });
    server.shutdown();
}

#[test]
fn scram_login_with_the_driver() {
    let users = std::collections::HashMap::from([("app".to_string(), "secret".to_string())]);
    let (server, addr) = start(ServerConfig { auth: Auth::Password { users }, ..Default::default() });
    runtime().block_on(async {
        let ok = format!("host=127.0.0.1 port={} user=app password=secret", addr.port());
        let client = connect(&ok).await;
        assert_eq!(client.query_one("SELECT current_user", &[]).await.unwrap().get::<_, String>(0), "app");
        let bad = format!("host=127.0.0.1 port={} user=app password=nope", addr.port());
        let err = tokio_postgres::connect(&bad, NoTls).await.err().expect("wrong password must fail");
        assert_eq!(err.as_db_error().map(|e| e.code().code().to_string()).as_deref(), Some("28P01"));
    });
    server.shutdown();
}

/// A query that takes far longer than any test.
const ENDLESS: &str = "SELECT count(*) FROM generate_series(1, 100000) a, generate_series(1, 100000) b";

/// Connects and also returns the connection task, whose result tells how
/// the server ended the connection.
async fn connect_watched(config: &str) -> (Client, tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
    let (client, connection) = tokio_postgres::connect(config, NoTls).await.expect("connect");
    (client, tokio::spawn(connection))
}

fn sqlstate(e: &tokio_postgres::Error) -> Option<&str> {
    e.code().map(|c| c.code())
}

#[test]
fn pg_cancel_backend_through_pg_stat_activity() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let conninfo = format!("host=127.0.0.1 port={} user=squad", addr.port());
        let lab = connect(&format!("{conninfo} application_name=sql_lab")).await;
        let admin = connect(&conninfo).await;
        let pid: i32 = lab.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        let row = admin
            .query_one("SELECT usename, application_name, state, query FROM pg_stat_activity WHERE pid = $1", &[&pid])
            .await
            .unwrap();
        let seen: (String, String, String, String) = (row.get(0), row.get(1), row.get(2), row.get(3));
        assert_eq!(seen, ("squad".into(), "sql_lab".into(), "idle".into(), "SELECT pg_backend_pid()".into()));
        let pids: Vec<i32> = admin
            .query("SELECT pid FROM pg_stat_activity ORDER BY pid", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(pids.len(), 2, "{pids:?}");

        // How Apache Superset stops a query of SQL Lab, with the pid as a
        // string literal.
        let stop = format!("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid = '{pid}'");
        let canceller = async {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let state: String =
                    admin.query_one("SELECT state FROM pg_stat_activity WHERE pid = $1", &[&pid]).await.unwrap().get(0);
                if state == "active" {
                    break;
                }
            }
            let query: String =
                admin.query_one("SELECT query FROM pg_stat_activity WHERE pid = $1", &[&pid]).await.unwrap().get(0);
            assert_eq!(query, ENDLESS);
            let ok: bool = admin.query_one(stop.as_str(), &[]).await.unwrap().get(0);
            assert!(ok);
        };
        let started = Instant::now();
        let (result, ()) = tokio::join!(lab.query_one(ENDLESS, &[]), canceller);
        let err = result.unwrap_err();
        assert_eq!(sqlstate(&err), Some("57014"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(30));
        // Only the statement ends; the connection stays usable.
        assert_eq!(lab.query_one("SELECT 7", &[]).await.unwrap().get::<_, i32>(0), 7);
    });
    server.shutdown();
}

#[test]
fn pg_terminate_backend_closes_the_connection() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let conninfo = format!("host=127.0.0.1 port={} user=squad", addr.port());
        let admin = connect(&conninfo).await;
        let terminate =
            |pid: i32| format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = '{pid}'");

        // A connection that is running a statement: the statement fails
        // with 57P01 and the server closes the connection.
        let (busy, busy_conn) = connect_watched(&conninfo).await;
        let pid: i32 = busy.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        let killer = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let ok: bool = admin.query_one(terminate(pid).as_str(), &[]).await.unwrap().get(0);
            assert!(ok);
        };
        let (result, ()) = tokio::join!(busy.query_one(ENDLESS, &[]), killer);
        let err = result.unwrap_err();
        assert_eq!(sqlstate(&err), Some("57P01"), "{err}");
        assert_eq!(
            err.as_db_error().map(|e| (e.severity(), e.message())),
            Some(("FATAL", "terminating connection due to administrator command"))
        );
        let _ = busy_conn.await.unwrap();
        assert!(busy.is_closed());
        assert!(busy.query_one("SELECT 1", &[]).await.is_err());

        // An idle connection is closed at once, with the same error.
        let (idle, idle_conn) = connect_watched(&conninfo).await;
        let pid: i32 = idle.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        let ok: bool = admin.query_one("SELECT pg_terminate_backend($1, $2)", &[&pid, &5000i64]).await.unwrap().get(0);
        assert!(ok, "the session should be gone within the timeout");
        let err = tokio::time::timeout(Duration::from_secs(10), idle_conn).await.unwrap().unwrap().unwrap_err();
        assert_eq!(sqlstate(&err), Some("57P01"), "{err}");
        let left: i64 =
            admin.query_one("SELECT count(*) FROM pg_stat_activity WHERE pid = $1", &[&pid]).await.unwrap().get(0);
        assert_eq!(left, 0);

        // Terminating oneself.
        let (me, me_conn) = connect_watched(&conninfo).await;
        let err = me.query_one("SELECT pg_terminate_backend(pg_backend_pid())", &[]).await.unwrap_err();
        assert_eq!(sqlstate(&err), Some("57P01"), "{err}");
        let _ = me_conn.await.unwrap();
        assert!(me.is_closed());

        // Parameters get PostgreSQL's argument types, as drivers that
        // describe a statement before binding (tokio-postgres, JDBC) need.
        let st = admin
            .prepare(
                "SELECT pg_cancel_backend($1), pg_terminate_backend($2), pg_advisory_lock($3),
                        pg_try_advisory_lock($4, $5), pg_size_pretty(1024)",
            )
            .await
            .unwrap();
        use tokio_postgres::types::Type;
        assert_eq!(st.params(), [Type::INT4, Type::INT4, Type::INT8, Type::INT4, Type::INT4]);

        // Unknown pids give false, and the other connections live on.
        let ok: bool = admin.query_one("SELECT pg_terminate_backend(2147483647)", &[]).await.unwrap().get(0);
        assert!(!ok);
        assert_eq!(admin.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    });
    server.shutdown();
}
