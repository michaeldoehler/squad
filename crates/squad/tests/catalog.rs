//! System catalog compatibility: the catalog queries that psql, ORMs and
//! migration tools send, with the results PostgreSQL 16 gives for the
//! same schema.
//!
//! The cases in `tests/data` were recorded from a real server: the SQL is
//! what `psql -E` prints for its backslash commands, respectively what
//! sqlx, Diesel, Rails and Prisma run to inspect a schema. Object OIDs
//! differ between servers, so they are written as `{{oid:name}}` in the
//! SQL and as `<name>` in the expected output.

use std::collections::HashMap;

use regex::Regex;
use serde::Deserialize;
use squad::{Connection, Database, FnCtx, Value};

const SCHEMA: &str = "
    CREATE TYPE mood AS ENUM ('sad','ok','happy');
    CREATE TABLE users (id serial PRIMARY KEY, email varchar(100) NOT NULL UNIQUE, name text DEFAULT 'anon',
                        age int CHECK (age > 0), mood mood, created_at timestamptz NOT NULL DEFAULT now(),
                        score numeric(10,2));
    CREATE TABLE posts (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        user_id int NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        title text NOT NULL, tags text[], UNIQUE (user_id, title));
    CREATE INDEX posts_title_idx ON posts (title);
    CREATE VIEW user_posts AS SELECT u.id, u.name, p.title FROM users u JOIN posts p ON p.user_id = u.id;
    CREATE SEQUENCE my_seq START 10 INCREMENT 5;
";

#[derive(Deserialize)]
struct Case {
    name: String,
    sql: String,
    expected: String,
}

fn setup() -> Connection {
    let conn = Database::open_in_memory().unwrap().connect();
    conn.batch_execute(SCHEMA).unwrap();
    conn
}

/// Runs a query and renders the rows like `psql -At -F ' | '`.
fn run(conn: &Connection, sql: &str) -> Result<String, squad::Error> {
    let ctx = FnCtx::default();
    let mut results = conn.simple_query(sql)?;
    let result = results.pop().expect("one result");
    let lines: Vec<String> = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    Value::Null => String::new(),
                    v => squad::value::format_value(v, &ctx),
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    Ok(lines.join("\n"))
}

/// Name → OID of every object the recorded cases may refer to.
fn object_oids(conn: &Connection) -> HashMap<String, String> {
    let listing = run(
        conn,
        "SELECT c.relname, c.oid FROM pg_class c WHERE c.relnamespace = 2200
         UNION ALL SELECT typname, oid FROM pg_type WHERE typname IN ('mood', '_mood')
         UNION ALL SELECT conname, oid FROM pg_constraint WHERE connamespace = 2200",
    )
    .unwrap();
    // Relations win over equally named constraints, as in the recording.
    let mut map = HashMap::new();
    for line in listing.lines() {
        let (name, oid) = line.split_once(" | ").unwrap();
        map.entry(name.to_string()).or_insert_with(|| oid.to_string());
    }
    map
}

fn check_cases(json: &str, extra_setup: &str) {
    let cases: Vec<Case> = serde_json::from_str(json).unwrap();
    let conn = setup();
    if !extra_setup.is_empty() {
        conn.batch_execute(extra_setup).unwrap();
    }
    let oids = object_oids(&conn);
    let names: HashMap<&str, &str> = oids.iter().map(|(n, o)| (o.as_str(), n.as_str())).collect();
    let placeholder = Regex::new(r"\{\{oid:([^}]+)\}\}").unwrap();
    let number = Regex::new(r"\b\d{5,}\b").unwrap();
    let mut failures = Vec::new();
    for case in &cases {
        let sql = placeholder.replace_all(&case.sql, |c: &regex::Captures| oids[&c[1]].clone());
        // The recording ran as role "postgres"; squad's role is "squad".
        let expected = case.expected.replace("postgres", "squad");
        match run(&conn, &sql) {
            Ok(got) => {
                let got = number.replace_all(&got, |c: &regex::Captures| match names.get(&c[0]) {
                    Some(name) => format!("<{name}>"),
                    None => c[0].to_string(),
                });
                if got != expected {
                    failures.push(format!("[{}]\n{}\n--- expected\n{}\n--- got\n{}", case.name, sql, expected, got));
                }
            }
            Err(e) => failures.push(format!("[{}]\n{}\n--- error\n{}", case.name, sql, e)),
        }
    }
    assert!(failures.is_empty(), "{} of {} cases differ:\n\n{}", failures.len(), cases.len(), failures.join("\n\n"));
}

/// The queries behind `\dt`, `\d <table>`, `\di`, `\ds`, `\dv`, `\dT`,
/// `\du` and `\d` on a view, an index and a sequence.
#[test]
fn psql_describe_queries() {
    check_cases(include_str!("data/psql_describe.json"), "");
}

/// Introspection as done by sqlx, Diesel, Rails, Prisma and SeaORM.
#[test]
fn orm_introspection_queries() {
    check_cases(
        include_str!("data/introspection.json"),
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL)",
    );
}

#[test]
fn every_system_relation_can_be_scanned() {
    let conn = setup();
    for line in include_str!("../src/system/relations.txt").lines().filter(|l| !l.is_empty()) {
        let (name, columns) = line.split_once('|').unwrap();
        let rows = conn.query(&format!("SELECT * FROM {name}"), &[]).unwrap_or_else(|e| panic!("{name}: {e}"));
        let width = columns.split(' ').count();
        assert!(rows.iter().all(|r| r.len() == width), "{name} has rows of the wrong width");
    }
}

#[test]
fn unqualified_names_reach_pg_catalog_but_not_information_schema() {
    let conn = setup();
    assert_eq!(run(&conn, "SELECT count(*) FROM pg_class WHERE relname = 'users'").unwrap(), "1");
    assert_eq!(run(&conn, "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'users'").unwrap(), "1");
    // `tables` is an ordinary name outside information_schema.
    let err = run(&conn, "SELECT * FROM tables").unwrap_err();
    assert_eq!(err.code(), "42P01");
    conn.batch_execute("CREATE TABLE tables (id int)").unwrap();
    assert_eq!(run(&conn, "SELECT count(*) FROM tables").unwrap(), "0");
    assert_eq!(run(&conn, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'tables'").unwrap(), "1");
}

#[test]
fn system_catalogs_are_read_only() {
    let conn = setup();
    for sql in
        ["INSERT INTO pg_class (relname) VALUES ('x')", "DELETE FROM pg_namespace", "UPDATE pg_type SET typname = 'x'"]
    {
        assert_eq!(run(&conn, sql).unwrap_err().code(), "42501", "{sql}");
    }
}

#[test]
fn reg_casts() {
    let conn = setup();
    // Name to OID, and back to the name.
    assert_eq!(
        run(&conn, "SELECT 'users'::regclass = (SELECT oid FROM pg_class WHERE relname = 'users')").unwrap(),
        "t"
    );
    assert_eq!(run(&conn, "SELECT 'public.users'::regclass::oid::regclass").unwrap(), "users");
    assert_eq!(
        run(&conn, "SELECT '\"users\"'::regclass::oid::regclass, 'users_pkey'::regclass::oid::regclass").unwrap(),
        "users | users_pkey"
    );
    assert_eq!(run(&conn, "SELECT 0::regclass, 0::regtype").unwrap(), "- | -");
    assert_eq!(
        run(&conn, "SELECT 'int4'::regtype::oid, 'integer[]'::regtype::oid, 'mood'::regtype::oid::regtype").unwrap(),
        "23 | 1007 | mood"
    );
    assert_eq!(run(&conn, "SELECT 'pg_catalog'::regnamespace::oid, 2200::regnamespace").unwrap(), "11 | public");
    assert_eq!(run(&conn, "SELECT 'pg_class'::regclass::oid::regclass").unwrap(), "pg_class");

    // Missing objects raise the errors Postgres raises.
    let err = run(&conn, "SELECT 'nope'::regclass").unwrap_err();
    assert_eq!((err.code(), err.message()), ("42P01", "relation \"nope\" does not exist"));
    let err = run(&conn, "SELECT 'nope'::regtype").unwrap_err();
    assert_eq!((err.code(), err.message()), ("42704", "type \"nope\" does not exist"));
    let err = run(&conn, "SELECT 'nope'::regnamespace").unwrap_err();
    assert_eq!((err.code(), err.message()), ("3F000", "schema \"nope\" does not exist"));
    assert_eq!(run(&conn, "SELECT to_regclass('nope'), to_regtype('nope')").unwrap(), " | ");
}

#[test]
fn objects_outside_the_search_path_are_qualified() {
    let conn = setup();
    conn.batch_execute(
        "CREATE SCHEMA app; CREATE TABLE app.items (id int PRIMARY KEY, owner int REFERENCES public.users(id))",
    )
    .unwrap();
    assert_eq!(run(&conn, "SELECT 'app.items'::regclass::oid::regclass").unwrap(), "app.items");
    assert_eq!(
        run(&conn, "SELECT pg_table_is_visible('app.items'::regclass), pg_table_is_visible('users'::regclass)")
            .unwrap(),
        "f | t"
    );
    assert_eq!(
        run(&conn, "SELECT pg_get_indexdef('app.items_pkey'::regclass), pg_get_indexdef('app.items_pkey'::regclass, 0, true)").unwrap(),
        "CREATE UNIQUE INDEX items_pkey ON app.items USING btree (id) | CREATE UNIQUE INDEX items_pkey ON app.items USING btree (id)"
    );
    assert_eq!(
        run(&conn, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'items_owner_fkey'").unwrap(),
        "FOREIGN KEY (owner) REFERENCES users(id)"
    );
    conn.batch_execute("SET search_path = app").unwrap();
    assert_eq!(
        run(&conn, "SELECT 'items'::regclass::oid::regclass, 'public.users'::regclass::oid::regclass").unwrap(),
        "items | public.users"
    );
    assert_eq!(
        run(&conn, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'items_owner_fkey'").unwrap(),
        "FOREIGN KEY (owner) REFERENCES public.users(id)"
    );
    assert_eq!(run(&conn, "SELECT nspname FROM pg_namespace WHERE oid = 'app'::regnamespace").unwrap(), "app");
}

#[test]
fn catalog_follows_schema_changes_inside_a_transaction() {
    let conn = setup();
    conn.batch_execute("BEGIN").unwrap();
    conn.batch_execute("CREATE TABLE fresh (a int, b text)").unwrap();
    conn.batch_execute("ALTER TABLE users DROP COLUMN score").unwrap();
    assert_eq!(
        run(&conn, "SELECT attname FROM pg_attribute WHERE attrelid = 'fresh'::regclass ORDER BY attnum").unwrap(),
        "a\nb"
    );
    assert_eq!(
        run(
            &conn,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'score'"
        )
        .unwrap(),
        "0"
    );
    conn.batch_execute("ROLLBACK").unwrap();
    assert_eq!(run(&conn, "SELECT to_regclass('fresh') IS NULL").unwrap(), "t");
    assert_eq!(
        run(
            &conn,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'score'"
        )
        .unwrap(),
        "1"
    );
}

#[test]
fn hidden_row_id_is_not_exposed() {
    let conn = setup();
    conn.batch_execute("CREATE TABLE nopk (a int, b text)").unwrap();
    assert_eq!(
        run(&conn, "SELECT attname, attnum FROM pg_attribute WHERE attrelid = 'nopk'::regclass ORDER BY attnum")
            .unwrap(),
        "a | 1\nb | 2"
    );
    assert_eq!(run(&conn, "SELECT relnatts, relhasindex FROM pg_class WHERE relname = 'nopk'").unwrap(), "2 | f");
    assert_eq!(run(&conn, "SELECT count(*) FROM pg_index WHERE indrelid = 'nopk'::regclass").unwrap(), "0");
    assert_eq!(run(&conn, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'nopk'").unwrap(), "2");
}

#[test]
fn sqlx_migration_flow() {
    let conn = setup();
    // The statements sqlx-cli / `sqlx::migrate!` run, in order.
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL)",
    )
    .unwrap();
    assert_eq!(run(&conn, "SELECT current_database()").unwrap(), "squad");
    conn.query("SELECT pg_advisory_lock($1)", &[&4242i64]).unwrap();
    assert_eq!(
        run(&conn, "SELECT version FROM _sqlx_migrations WHERE success = false ORDER BY version LIMIT 1").unwrap(),
        ""
    );
    assert_eq!(run(&conn, "SELECT version, checksum FROM _sqlx_migrations ORDER BY version").unwrap(), "");
    conn.execute(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, $2, FALSE, $3, -1)",
        &[&1i64, &"init", &vec![1u8, 2, 3]],
    )
    .unwrap();
    conn.execute("UPDATE _sqlx_migrations SET success = TRUE WHERE version = $1", &[&1i64]).unwrap();
    conn.execute("UPDATE _sqlx_migrations SET execution_time = $1 WHERE version = $2", &[&12345i64, &1i64]).unwrap();
    assert_eq!(
        run(&conn, "SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version").unwrap(),
        "1 | \\x010203 | t"
    );
    let unlocked: bool = conn.query_one("SELECT pg_advisory_unlock($1)", &[&4242i64]).unwrap().get(0);
    assert!(unlocked);
}

#[test]
fn settings_and_session_relations() {
    let conn = setup();
    conn.batch_execute("SET application_name = 'catalog-test'").unwrap();
    assert_eq!(
        run(&conn, "SELECT setting FROM pg_settings WHERE name = 'server_version'").unwrap(),
        squad::SERVER_VERSION
    );
    assert_eq!(run(&conn, "SELECT setting FROM pg_settings WHERE name = 'application_name'").unwrap(), "catalog-test");
    assert_eq!(
        run(&conn, "SELECT application_name, state, pid = pg_backend_pid() FROM pg_stat_activity").unwrap(),
        "catalog-test | active | t"
    );
    assert_eq!(run(&conn, "SELECT datname, pg_encoding_to_char(encoding) FROM pg_database").unwrap(), "squad | UTF8");
    assert_eq!(run(&conn, "SELECT rolname, rolsuper FROM pg_roles").unwrap(), "squad | t");
    assert_eq!(run(&conn, "SELECT amname FROM pg_am ORDER BY oid").unwrap(), "heap\nbtree\nhash");
}
