# Embedding squad in a Rust program

```toml
[dependencies]
squad = "0.1"
```

Building squad compiles the PostgreSQL parser, so a C compiler must be
available.

## Opening a database

```rust
use squad::{Config, Database, SyncMode};

// A directory on disk; created if missing.
let db = Database::open("data/", Config::default())?;

// Tuned: 512 MiB page cache, group commit.
let db = Database::open("data/", Config {
    cache_size: 512 << 20,
    sync_mode: SyncMode::Group,
    ..Config::default()
})?;

// Volatile, for tests.
let db = Database::open_in_memory()?;
```

A database directory can be open in one process at a time; a second
`open` fails with SQLSTATE `55000`. Inside the process, `Database` is
cheap to clone and safe to share between threads.

| `Config` field | Default | Meaning |
| --- | --- | --- |
| `sync_mode` | `Sync` | `Sync`: every commit waits for `fsync`. `Group`: the same guarantee, waiting up to ~200 µs to share an `fsync` with other commits. `Async`: commits return at once and are flushed in the background; a process crash loses nothing, a power failure may lose the last few milliseconds. |
| `cache_size` | 128 MiB | Page cache size in bytes. |
| `checkpoint_wal_bytes` | 64 MiB | Log size that triggers an automatic checkpoint. |

## Connections

A `Connection` is a session: it has its own transaction state, settings
and prepared statements. Open one per thread or task that works with the
database; connections are cheap.

```rust
let conn = db.connect();

conn.batch_execute("
    CREATE TABLE users (
        id    bigserial PRIMARY KEY,
        email text NOT NULL UNIQUE,
        name  text,
        created_at timestamptz NOT NULL DEFAULT now()
    );
")?;

let n = conn.execute(
    "INSERT INTO users (email, name) VALUES ($1, $2)",
    &[&"ada@example.org", &"Ada"],
)?;
assert_eq!(n, 1);

for row in conn.query("SELECT id, email FROM users ORDER BY id", &[])? {
    let id: i64 = row.get(0);
    let email: String = row.get("email");
    println!("{id} {email}");
}
```

| Method | Use |
| --- | --- |
| `execute(sql, params) -> u64` | statements that change rows; returns the count |
| `query(sql, params) -> Vec<Row>` | all rows of a query |
| `query_one` / `query_opt` | exactly one / at most one row, otherwise SQLSTATE `21000` |
| `batch_execute(sql)` | several statements separated by `;`, no parameters |
| `simple_query(sql)` | like `batch_execute`, returning every statement's result |
| `prepare(sql) -> Statement` | plan once, run many times |
| `transaction() -> Transaction` | a transaction that rolls back unless committed |

## Prepared statements

```rust
let by_id = conn.prepare("SELECT name FROM users WHERE id = $1")?;
let name: Option<String> = by_id.query_one(&[&42i64])?.get(0);
```

A `Statement` keeps its plan. If the schema changes, the plan is rebuilt
transparently on the next use. Ad-hoc SQL passed to `query`/`execute`
without parameters is also cached by its text, so repeating the same
string is nearly as fast as a prepared statement.

## Parameters and results

Parameters are passed as `&[&dyn ToSql]`; columns are read with
`Row::get::<T>(index_or_name)` (panics on mismatch) or `Row::try_get`.

| SQL type | Rust types |
| --- | --- |
| `boolean` | `bool` |
| `smallint`, `integer`, `bigint` | `i16`, `i32`, `i64` (reading also `u32`, `u64`, `usize`) |
| `real`, `double precision` | `f32`, `f64` |
| `numeric` | `rust_decimal::Decimal` |
| `text`, `varchar`, `char`, enums | `String`, `&str` |
| `bytea` | `Vec<u8>`, `&[u8]` |
| `date`, `time`, `timestamp` | `chrono::NaiveDate`, `NaiveTime`, `NaiveDateTime` |
| `timestamptz` | `chrono::DateTime<Utc>` (any time zone when writing) |
| `uuid` | `[u8; 16]` |
| `json`, `jsonb` | `serde_json::Value` |
| arrays | `Vec<T>` |
| any, nullable | `Option<T>`; `squad::Value` reads anything |

A parameter is converted to the type the statement expects: passing an
`i32` for a `bigint` column works.

## Transactions

```rust
let tx = conn.transaction()?;
tx.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", &[&10i64, &1i32])?;
tx.execute("UPDATE accounts SET balance = balance + $1 WHERE id = $2", &[&10i64, &2i32])?;
tx.commit()?;            // dropping `tx` without commit rolls back
```

Plain SQL works as well (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`,
`BEGIN ISOLATION LEVEL SERIALIZABLE`, …). As in Postgres, after an error
inside a transaction block every statement fails with SQLSTATE `25P02`
until the block is rolled back, and several statements passed as one
string run as one transaction.

Under Repeatable Read and Serializable — and on deadlocks under any
level — a transaction can fail with `40001` or `40P01`. That is the
signal to run it again:

```rust
fn transfer(conn: &squad::Connection) -> squad::Result<()> {
    loop {
        let attempt = (|| {
            let tx = conn.transaction()?;
            tx.batch_execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")?;
            // … work …
            tx.commit()
        })();
        match attempt {
            Err(e) if matches!(e.code(), "40001" | "40P01") => continue,
            other => return other,
        }
    }
}
```

## Errors

`squad::Error` carries the fields of a Postgres error: `code()` (the
five-character SQLSTATE), `message()`, `detail()`, `hint()` and
`position()`. Match on the code, never on the message:

```rust
use squad::sqlstate;

match conn.execute("INSERT INTO users (email) VALUES ($1)", &[&email]) {
    Err(e) if e.code() == sqlstate::UNIQUE_VIOLATION => { /* already registered */ }
    other => { other?; }
}
```

## Using several threads

```rust
let db = Database::open("data/", Config::default())?;
let workers: Vec<_> = (0..8).map(|_| {
    let db = db.clone();
    std::thread::spawn(move || {
        let conn = db.connect();
        // each thread: its own connection
    })
}).collect();
```

Readers never wait for writers. Writers wait for each other only when
they touch the same row. With `tokio`, run database work in
`spawn_blocking` or on a dedicated thread pool; the API is synchronous
and calls return in microseconds unless they wait for a durable commit
or a row lock.

## Serving other processes

Only one process owns the database, but it can serve others over the
Postgres protocol:

```rust
use squad_server::Serve;

let server = db.serve("127.0.0.1:5432")?;
// psql, pgAdmin, migration tools and services in other languages connect
// to postgres://squad@127.0.0.1:5432/squad while this process keeps using
// the embedded API.
```

See [server](server.md) for authentication, Unix sockets and tool
compatibility.

## Maintenance

```rust
db.checkpoint()?;          // flush to the data file, truncate the log
db.backup("backup-dir/")?; // consistent copy while the database is in use
let report = db.verify()?; // checksums and structure
assert!(report.is_ok());
db.vacuum()?;              // remove dead row versions now
```

None of these is required for normal operation: checkpoints run
automatically, and dead versions are removed as rows are rewritten.
