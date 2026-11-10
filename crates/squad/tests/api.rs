//! Tests of the embedding API.

use squad::{sqlstate, Config, Database, Value};

fn memory() -> squad::Connection {
    Database::open_in_memory().unwrap().connect()
}

#[test]
fn typed_parameters_and_results() {
    let conn = memory();
    conn.batch_execute(
        "CREATE TABLE t (id bigint PRIMARY KEY, name text, score float8, ok boolean, data bytea, tags text[])",
    )
    .unwrap();
    let n = conn
        .execute(
            "INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6)",
            &[&1i64, &"Ada", &1.5f64, &true, &vec![1u8, 2, 3], &vec!["a".to_string(), "b".to_string()]],
        )
        .unwrap();
    assert_eq!(n, 1);
    conn.execute("INSERT INTO t (id, name) VALUES ($1, $2)", &[&2i64, &None::<String>]).unwrap();

    let row = conn.query_one("SELECT * FROM t WHERE id = $1", &[&1i64]).unwrap();
    assert_eq!(row.get::<i64>(0), 1);
    assert_eq!(row.get::<String>("name"), "Ada");
    assert_eq!(row.get::<f64>("score"), 1.5);
    assert!(row.get::<bool>("ok"));
    assert_eq!(row.get::<Vec<u8>>("data"), vec![1, 2, 3]);
    assert_eq!(row.get::<Vec<String>>("tags"), vec!["a", "b"]);

    let row = conn.query_one("SELECT name, score FROM t WHERE id = 2", &[]).unwrap();
    assert_eq!(row.get::<Option<String>>(0), None);
    assert!(row.try_get::<String>(0).is_err());
    assert!(row.try_get::<i64>("nosuch").is_err());

    assert!(conn.query_opt("SELECT 1 FROM t WHERE id = 99", &[]).unwrap().is_none());
    assert_eq!(conn.query_one("SELECT id FROM t", &[]).unwrap_err().code(), sqlstate::CARDINALITY_VIOLATION);
    // An int4 argument for a bigint parameter is converted.
    assert_eq!(conn.query("SELECT 1 FROM t WHERE id = $1", &[&1i32]).unwrap().len(), 1);
}

#[test]
fn prepared_statements_survive_schema_changes() {
    let conn = memory();
    conn.batch_execute("CREATE TABLE t (id int PRIMARY KEY, a text); INSERT INTO t VALUES (1, 'x')").unwrap();
    let stmt = conn.prepare("SELECT a FROM t WHERE id = $1").unwrap();
    assert_eq!(stmt.query_one(&[&1i32]).unwrap().get::<String>(0), "x");
    conn.batch_execute("ALTER TABLE t ADD COLUMN b int; CREATE INDEX t_a ON t (a)").unwrap();
    assert_eq!(stmt.query_one(&[&1i32]).unwrap().get::<String>(0), "x");
    conn.batch_execute("DROP TABLE t").unwrap();
    assert_eq!(stmt.query(&[&1i32]).unwrap_err().code(), sqlstate::UNDEFINED_TABLE);
}

#[test]
fn transactions_and_batches() {
    let conn = memory();
    conn.batch_execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();

    // A failing batch leaves nothing behind.
    let err = conn.batch_execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (1)").unwrap_err();
    assert_eq!(err.code(), sqlstate::UNIQUE_VIOLATION);
    assert_eq!(conn.query("SELECT * FROM t", &[]).unwrap().len(), 0);

    // BEGIN inside a batch opens a real transaction that stays open.
    conn.batch_execute("BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)").unwrap();
    assert!(conn.in_transaction());
    conn.batch_execute("ROLLBACK").unwrap();
    assert_eq!(conn.query("SELECT * FROM t", &[]).unwrap().len(), 0);

    // A batch that commits by itself.
    conn.batch_execute("BEGIN; INSERT INTO t VALUES (1); COMMIT; INSERT INTO t VALUES (2)").unwrap();
    assert!(!conn.in_transaction());
    assert_eq!(conn.query("SELECT * FROM t", &[]).unwrap().len(), 2);

    // The guard rolls back unless committed.
    {
        let tx = conn.transaction().unwrap();
        tx.execute("DELETE FROM t", &[]).unwrap();
    }
    assert_eq!(conn.query("SELECT * FROM t", &[]).unwrap().len(), 2);
    let tx = conn.transaction().unwrap();
    tx.execute("DELETE FROM t WHERE id = 1", &[]).unwrap();
    tx.commit().unwrap();
    assert_eq!(conn.query("SELECT * FROM t", &[]).unwrap().len(), 1);

    // After an error the transaction only accepts ROLLBACK.
    let tx = conn.transaction().unwrap();
    assert!(tx.execute("SELECT 1 / 0", &[]).is_err());
    assert_eq!(tx.execute("SELECT 1", &[]).unwrap_err().code(), sqlstate::IN_FAILED_SQL_TRANSACTION);
    tx.rollback().unwrap();
    assert_eq!(conn.query_one("SELECT 1", &[]).unwrap().get::<i32>(0), 1);
}

#[test]
fn data_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path(), Config::default()).unwrap();
        let conn = db.connect();
        conn.batch_execute(
            "CREATE TYPE color AS ENUM ('red', 'green');
             CREATE TABLE t (id serial PRIMARY KEY, name text UNIQUE, c color, n numeric(8,2));
             CREATE INDEX t_n ON t (n);
             CREATE VIEW v AS SELECT name FROM t WHERE c = 'green';",
        )
        .unwrap();
        for i in 0..500 {
            conn.execute(
                "INSERT INTO t (name, c, n) VALUES ($1, 'green', $2)",
                &[&format!("row {i}"), &(i as f64 / 4.0)],
            )
            .unwrap();
        }
        conn.execute("DELETE FROM t WHERE id % 2 = 0", &[]).unwrap();
        assert!(Database::open(dir.path(), Config::default()).is_err(), "second open must be refused");
    }
    let db = Database::open(dir.path(), Config::default()).unwrap();
    let conn = db.connect();
    assert_eq!(conn.query_one("SELECT count(*), max(id) FROM t", &[]).unwrap().get::<i64>(0), 250);
    assert_eq!(conn.query("SELECT * FROM v", &[]).unwrap().len(), 250);
    assert_eq!(conn.query("SELECT id FROM t WHERE n = 0.5", &[]).unwrap().len(), 1);
    // The sequence continues after the highest value handed out.
    let id: i32 = conn.query_one("INSERT INTO t (name) VALUES ('new') RETURNING id", &[]).unwrap().get(0);
    assert!(id > 500, "sequence restarted at {id}");
    assert_eq!(
        conn.execute("INSERT INTO t (name) VALUES ('new')", &[]).unwrap_err().code(),
        sqlstate::UNIQUE_VIOLATION
    );
    assert!(db.verify().unwrap().is_ok());

    let backup = dir.path().join("backup");
    db.backup(&backup).unwrap();
    let copy = Database::open(&backup, Config::default()).unwrap();
    assert_eq!(copy.connect().query_one("SELECT count(*) FROM t", &[]).unwrap().get::<i64>(0), 251);
    assert_eq!(copy.connect().query_one("SELECT 1", &[]).unwrap().values(), &[Value::Int4(1)]);
}
