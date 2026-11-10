//! Integration through the real Postgres client tools. Each test is
//! skipped when the tool is not installed.

mod common;

use std::path::PathBuf;
use std::process::Command;

use common::{have, psql, psql_file, start, Client};
use squad_server::ServerConfig;

fn skip(tool: &str) -> bool {
    if have(tool) {
        return false;
    }
    eprintln!("skipping: {tool} is not installed");
    true
}

#[test]
fn psql_queries() {
    if skip("psql") {
        return;
    }
    let (server, addr) = start(ServerConfig::default());
    let (out, err) = psql(addr, &["-c", "SELECT 1 + 1 AS two, 'text' AS t, NULL AS n, true AS b"]);
    assert_eq!((out.as_str(), err.as_str()), ("2|text||t\n", ""));

    let (out, err) = psql(
        addr,
        &[
            "-c",
            "CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, price numeric(8,2), added timestamptz DEFAULT now())",
            "-c",
            "INSERT INTO items (name, price) VALUES ('apple', 1.5), ('pear', 2), ('fig', NULL)",
            "-c",
            "SELECT id, name, price FROM items ORDER BY id",
            "-c",
            "UPDATE items SET price = price * 2 WHERE price IS NOT NULL RETURNING name, price",
            "-c",
            "SELECT count(*), sum(price), max(added) IS NOT NULL FROM items",
        ],
    );
    assert_eq!(err, "");
    assert_eq!(
        out,
        "CREATE TABLE\nINSERT 0 3\n1|apple|1.50\n2|pear|2.00\n3|fig|\napple|3.00\npear|4.00\nUPDATE 2\n3|7.00|t\n"
    );

    // Errors are reported with psql's usual decoration.
    let (out, err) = psql(addr, &["-c", "INSERT INTO items (id, name) VALUES (1, 'dup')"]);
    assert_eq!(out, "");
    assert!(err.contains("ERROR:  duplicate key value violates unique constraint \"items_pkey\""), "{err}");
    assert!(err.contains("DETAIL:  Key (id)=(1) already exists."), "{err}");

    // A script with transactions, \copy and psql variables.
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("script.sql");
    let csv = dir.path().join("out.csv");
    std::fs::write(
        &script,
        format!(
            "\\set ON_ERROR_STOP on\n\
             BEGIN;\n\
             CREATE TABLE log (n int, note text);\n\
             COPY log FROM stdin;\n1\tfirst\n2\t\\N\n\\.\n\
             \\copy log from stdin with (format csv)\n3,\"quoted, value\"\n\\.\n\
             COMMIT;\n\
             \\set limit 2\n\
             SELECT n FROM log ORDER BY n LIMIT :limit;\n\
             \\copy (SELECT * FROM log ORDER BY n) to '{}' with (format csv, header)\n\
             BEGIN; DELETE FROM log; ROLLBACK;\n\
             SELECT count(*) FROM log;\n",
            csv.display()
        ),
    )
    .unwrap();
    let (out, err) = psql_file(addr, &script);
    assert_eq!(err, "");
    assert_eq!(out, "1\n2\n3\n");
    assert_eq!(std::fs::read_to_string(&csv).unwrap(), "n,note\n1,first\n2,\n3,\"quoted, value\"\n");
    server.shutdown();
}

#[test]
fn psql_over_unix_socket_and_password() {
    if skip("psql") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let users = std::collections::HashMap::from([("app".to_string(), "pa ss".to_string())]);
    let config = ServerConfig {
        auth: squad_server::Auth::Password { users },
        unix_socket: Some(dir.path().to_path_buf()),
        ..Default::default()
    };
    let (server, addr) = start(config);
    let run = |password: &str| {
        Command::new("psql")
            .args(["-h", dir.path().to_str().unwrap(), "-p", &addr.port().to_string(), "-U", "app", "-d", "squad"])
            .args(["-X", "-A", "-t", "-c", "SELECT current_user"])
            .env("PGPASSWORD", password)
            .output()
            .expect("run psql")
    };
    let ok = run("pa ss");
    assert_eq!(String::from_utf8_lossy(&ok.stdout), "app\n", "{}", String::from_utf8_lossy(&ok.stderr));
    let bad = run("wrong");
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("password authentication failed for user \"app\""));
    server.shutdown();
}

/// A dump written by `pg_dump` 16 (plain format) loads through psql.
#[test]
fn pg_dump_output_loads() {
    if skip("psql") {
        return;
    }
    let (server, addr) = start(ServerConfig::default());
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pg_dump.sql");
    let (_, err) = psql(addr, &["-q", "-v", "ON_ERROR_STOP=1", "-f", fixture.to_str().unwrap()]);
    assert_eq!(err, "", "loading the dump must not report errors");

    let (out, err) = psql(
        addr,
        &[
            "-c",
            "SELECT id, name, mood, born, meta FROM authors ORDER BY id",
            "-c",
            "SELECT id, author_id, title, price, tags, published, blob FROM books ORDER BY id",
            "-c",
            "SELECT name, n FROM author_books ORDER BY n DESC, name",
        ],
    );
    assert_eq!(err, "");
    assert_eq!(
        out,
        "1|Ada|happy|1815-12-10|{\"k\": [1, 2]}\n\
         2|Tab\tBob|sad||\n\
         3|New\nLine\\|ok|2000-01-01|{}\n\
         1|1|Notes|12.50|{math,\"with space\"}|2024-01-02 03:04:05+00|\\x00ff10\n\
         2|2|Empty||||\n\
         Ada|1\nTab\tBob|1\nNew\nLine\\|0\n"
    );
    // Sequences continue after the loaded data, and constraints from the
    // dump are in force.
    let mut c = Client::connect(addr);
    assert_eq!(
        c.rows("INSERT INTO authors (name) VALUES ('Zed') RETURNING id, mood"),
        vec![vec![Some("4".to_string()), Some("ok".to_string())]]
    );
    assert_eq!(c.error("INSERT INTO authors (name) VALUES ('Ada')"), "23505");
    assert_eq!(c.error("INSERT INTO books (author_id, title) VALUES (99, 'x')"), "23503");
    assert_eq!(c.error("INSERT INTO books (author_id, title, price) VALUES (1, 'x', -1)"), "23514");
    c.rows("DELETE FROM authors WHERE id = 1");
    assert_eq!(c.rows("SELECT count(*) FROM books"), vec![vec![Some("1".to_string())]]);
    server.shutdown();
}

#[test]
fn pgbench_runs() {
    if skip("pgbench") {
        return;
    }
    let (server, addr) = start(ServerConfig::default());
    let port = addr.port().to_string();
    let pgbench = |args: &[&str]| {
        let out = Command::new("pgbench")
            .args(["-h", "127.0.0.1", "-p", &port, "-U", "squad"])
            .args(args)
            .arg("squad")
            .env("PGPASSWORD", "unused")
            .output()
            .expect("run pgbench");
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success(), "pgbench {args:?} failed:\n{text}");
        text
    };
    pgbench(&["-i", "-s", "1", "-q"]);
    let mut c = Client::connect(addr);
    let count = |c: &mut Client, sql: &str| c.rows(sql)[0][0].clone().unwrap().parse::<i64>().unwrap();
    assert_eq!(count(&mut c, "SELECT count(*) FROM pgbench_accounts"), 100_000);

    // TPC-B-like load in both query modes; every transaction must succeed
    // and the books must balance afterwards.
    let mut total = 0;
    for mode in ["simple", "prepared"] {
        let out = pgbench(&["-c", "4", "-j", "2", "-t", "200", "-n", "-M", mode]);
        assert!(out.contains("number of failed transactions: 0 "), "{out}");
        assert!(out.contains("number of transactions actually processed: 800/800"), "{out}");
        total += 800;
        eprintln!("pgbench -M {mode}: {}", out.lines().find(|l| l.starts_with("tps")).unwrap_or_default());
    }
    assert_eq!(count(&mut c, "SELECT count(*) FROM pgbench_history"), total);
    let delta = count(&mut c, "SELECT sum(delta) FROM pgbench_history");
    assert_eq!(count(&mut c, "SELECT sum(abalance) FROM pgbench_accounts"), delta);
    assert_eq!(count(&mut c, "SELECT sum(bbalance) FROM pgbench_branches"), delta);
    assert_eq!(count(&mut c, "SELECT sum(tbalance) FROM pgbench_tellers"), delta);
    let out = pgbench(&["-c", "4", "-j", "2", "-t", "500", "-n", "-S", "-M", "prepared"]);
    assert!(out.contains("number of failed transactions: 0 "), "{out}");
    server.shutdown();
}
