//! An application written against sqlx and PostgreSQL, pointed at squad
//! by nothing more than its connection URL: connection pool, typed binds,
//! `FromRow`, transactions, custom enum types and the migrator.

mod common;

use common::start;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use squad_server::ServerConfig;

#[derive(Debug, sqlx::FromRow, PartialEq)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
    active: bool,
    score: f64,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::Type, PartialEq, Clone, Copy)]
#[sqlx(type_name = "mood", rename_all = "lowercase")]
enum Mood {
    Sad,
    Happy,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
}

async fn pool(port: u16) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!("postgres://squad@127.0.0.1:{port}/squad"))
        .await
        .expect("connect pool")
}

#[test]
fn queries_binds_and_transactions() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let pool = pool(addr.port()).await;
        sqlx::query(
            "CREATE TABLE users (
                id bigserial PRIMARY KEY, name text NOT NULL, email text UNIQUE,
                active boolean NOT NULL DEFAULT true, score double precision NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let id: i64 = sqlx::query_scalar("INSERT INTO users (name, email, score) VALUES ($1, $2, $3) RETURNING id")
            .bind("Ada")
            .bind(Some("ada@example.org"))
            .bind(1.5f64)
            .fetch_one(&pool)
            .await
            .unwrap();
        let done = sqlx::query("INSERT INTO users (name, email) VALUES ($1, $2)")
            .bind("Bob")
            .bind(None::<String>)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(done.rows_affected(), 1);

        let users: Vec<User> = sqlx::query_as("SELECT * FROM users ORDER BY id").fetch_all(&pool).await.unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!((users[0].id, users[0].name.as_str(), users[0].score), (id, "Ada", 1.5));
        assert_eq!(users[1].email, None);
        assert!(users[1].active);

        let one: Option<User> =
            sqlx::query_as("SELECT * FROM users WHERE id = $1").bind(id).fetch_optional(&pool).await.unwrap();
        assert_eq!(one.as_ref(), Some(&users[0]));
        let none: Option<User> =
            sqlx::query_as("SELECT * FROM users WHERE id = $1").bind(-1i64).fetch_optional(&pool).await.unwrap();
        assert!(none.is_none());

        // Errors carry the Postgres SQLSTATE and constraint kind.
        match sqlx::query("INSERT INTO users (name, email) VALUES ('x', 'ada@example.org')").execute(&pool).await {
            Err(sqlx::Error::Database(e)) => {
                assert_eq!(e.code().as_deref(), Some("23505"));
                assert!(e.is_unique_violation());
            }
            other => panic!("expected a unique violation, got {other:?}"),
        }

        // A transaction that is rolled back leaves no trace; a committed
        // one does.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE users SET score = score + 1 WHERE name = $1").bind("Ada").execute(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        let score: f64 =
            sqlx::query_scalar("SELECT score FROM users WHERE name = 'Ada'").fetch_one(&pool).await.unwrap();
        assert_eq!(score, 1.5);
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE users SET score = score + 1 WHERE name = $1").bind("Ada").execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let score: f64 =
            sqlx::query_scalar("SELECT score FROM users WHERE name = 'Ada'").fetch_one(&pool).await.unwrap();
        assert_eq!(score, 2.5);

        // Array parameters.
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE id = ANY($1)")
            .bind(vec![1i64, 2, 3])
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 2);
        sqlx::query("INSERT INTO users (name, score) SELECT * FROM UNNEST($1::text[], $2::float8[])")
            .bind(vec!["u1".to_string(), "u2".to_string()])
            .bind(vec![1.0f64, 2.0])
            .execute(&pool)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM users").fetch_one(&pool).await.unwrap();
        assert_eq!(n, 4);
        pool.close().await;
    });
    server.shutdown();
}

#[test]
fn column_types_round_trip() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let pool = pool(addr.port()).await;
        sqlx::query(
            "CREATE TABLE items (
                id uuid PRIMARY KEY, data jsonb NOT NULL, price numeric(10,2), tags text[] NOT NULL,
                n int, small smallint, day date, raw bytea, ts timestamp
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4();
        let data = serde_json::json!({"a": [1, 2, 3], "b": "x"});
        let price = rust_decimal::Decimal::new(12345, 2);
        let day = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let ts = day.and_hms_opt(1, 2, 3).unwrap();
        sqlx::query("INSERT INTO items VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
            .bind(id)
            .bind(&data)
            .bind(price)
            .bind(vec!["a".to_string(), "b".to_string()])
            .bind(None::<i32>)
            .bind(3i16)
            .bind(day)
            .bind(vec![0u8, 1, 255])
            .bind(ts)
            .execute(&pool)
            .await
            .unwrap();
        let row = sqlx::query("SELECT * FROM items WHERE id = $1").bind(id).fetch_one(&pool).await.unwrap();
        assert_eq!(row.get::<uuid::Uuid, _>("id"), id);
        assert_eq!(row.get::<serde_json::Value, _>("data"), data);
        assert_eq!(row.get::<rust_decimal::Decimal, _>("price"), price);
        assert_eq!(row.get::<Vec<String>, _>("tags"), vec!["a", "b"]);
        assert_eq!(row.get::<Option<i32>, _>("n"), None);
        assert_eq!(row.get::<i16, _>("small"), 3);
        assert_eq!(row.get::<chrono::NaiveDate, _>("day"), day);
        assert_eq!(row.get::<Vec<u8>, _>("raw"), vec![0, 1, 255]);
        assert_eq!(row.get::<chrono::NaiveDateTime, _>("ts"), ts);
        pool.close().await;
    });
    server.shutdown();
}

#[test]
fn custom_enum_types() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let pool = pool(addr.port()).await;
        sqlx::query("CREATE TYPE mood AS ENUM ('sad', 'happy')").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE kv (k text PRIMARY KEY, m mood NOT NULL)").execute(&pool).await.unwrap();
        // sqlx resolves the type through pg_type and pg_enum.
        sqlx::query("INSERT INTO kv VALUES ($1, $2), ($3, $4)")
            .bind("a")
            .bind(Mood::Happy)
            .bind("b")
            .bind(Mood::Sad)
            .execute(&pool)
            .await
            .unwrap();
        let m: Mood = sqlx::query_scalar("SELECT m FROM kv WHERE k = 'a'").fetch_one(&pool).await.unwrap();
        assert_eq!(m, Mood::Happy);
        let k: String =
            sqlx::query_scalar("SELECT k FROM kv WHERE m = $1").bind(Mood::Sad).fetch_one(&pool).await.unwrap();
        assert_eq!(k, "b");
        let text: String = sqlx::query_scalar("SELECT m::text FROM kv WHERE k = 'a'").fetch_one(&pool).await.unwrap();
        assert_eq!(text, "happy");
        pool.close().await;
    });
    server.shutdown();
}

#[test]
fn migrations() {
    let (server, addr) = start(ServerConfig::default());
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("1_init.sql"),
        "CREATE TABLE mig (id serial PRIMARY KEY, v text);\nCREATE INDEX mig_v ON mig (v);",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("2_more.sql"),
        "ALTER TABLE mig ADD COLUMN extra int DEFAULT 1;\nINSERT INTO mig (v) VALUES ('x');",
    )
    .unwrap();
    runtime().block_on(async {
        let pool = pool(addr.port()).await;
        let migrator = sqlx::migrate::Migrator::new(dir.path()).await.unwrap();
        migrator.run(&pool).await.unwrap();
        // Running again applies nothing.
        migrator.run(&pool).await.unwrap();
        let (count, extra): (i64, i32) =
            sqlx::query_as("SELECT count(*), max(extra) FROM mig").fetch_one(&pool).await.unwrap();
        assert_eq!((count, extra), (1, 1));
        let applied: i64 =
            sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success").fetch_one(&pool).await.unwrap();
        assert_eq!(applied, 2);
        pool.close().await;
    });
    server.shutdown();
}
