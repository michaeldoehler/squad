# Server mode and the `squad` command

squad's primary form is a library, but everything that speaks the
Postgres wire protocol can use it through `squad-server`: psql, GUI
tools, migration tools, and applications in any language.

## Running a server

```sh
cargo install --path crates/squad-server     # installs the `squad` binary

squad serve ./data                           # 127.0.0.1:5432, every client trusted
squad serve ./data --listen 0.0.0.0:5433 --user app --password secret
squad serve ./data --unix-socket-dir /tmp    # also /tmp/.s.PGSQL.5432
squad serve --memory                         # volatile, for tests and CI

psql -h 127.0.0.1 -U squad squad
```

| Option | Meaning |
| --- | --- |
| `--listen <addr>` | TCP address (default `127.0.0.1:5432`) |
| `--unix-socket-dir <dir>` | additionally listen on `<dir>/.s.PGSQL.<port>` |
| `--user <name> --password <pw>` | require this login (SCRAM-SHA-256); repeatable. Without it every client is trusted. |
| `--cleartext-password` | ask for the password in clear text, for clients without SCRAM |
| `--sync sync\|group\|async` | commit durability, see [durability](durability.md) |
| `--memory` | serve an in-memory database |

The database name and, without `--user`, the user name a client sends
are accepted as given; there is one database per directory.

Other subcommands:

```sh
squad shell ./data            # interactive SQL prompt on the embedded API
squad backup ./data ./copy    # consistent copy of a database directory
squad verify ./data           # integrity check
```

`backup` and `verify` open the directory themselves, so the database must
not be open elsewhere; a running application calls `Database::backup`
instead.

## Embedding the server

```rust
use squad::Database;
use squad_server::{Auth, Serve, Server, ServerConfig};

let db = Database::open("data/", Default::default())?;

// Simple: trust every client on this address.
let handle = db.serve("127.0.0.1:5432")?;
println!("{}", handle.url().unwrap());   // postgres://squad@127.0.0.1:5432/squad

// Configured: password authentication and a Unix socket.
let mut server = Server::new(db.clone(), ServerConfig {
    auth: Auth::Password { users: [("app".into(), "secret".into())].into() },
    unix_socket: Some("/tmp".into()),
    ..ServerConfig::default()
});
server.bind("127.0.0.1:5432")?;
let handle = server.spawn();
// …
handle.shutdown();
```

## Using squad from an sqlx application

The stated goal of squad is that an application written for sqlx and
PostgreSQL runs after changing its connection URL. Through the server
this works today:

```rust
use squad_server::Serve;

let db = squad::Database::open("data/", Default::default())?;
let server = db.serve("127.0.0.1:0")?;              // any free port
let pool = sqlx::PgPool::connect(&server.url().unwrap()).await?;

sqlx::migrate!().run(&pool).await?;
let users: Vec<User> = sqlx::query_as("SELECT * FROM users WHERE active")
    .fetch_all(&pool).await?;
```

The integration test `crates/squad-server/tests/sqlx.rs` runs such an
application: connection pool, typed binds, `FromRow`, transactions,
array parameters, `uuid`/`jsonb`/`numeric`/date types, custom enum types
via `#[derive(sqlx::Type)]`, and the migrator.

Two caveats:

- The compile-time checked macros (`sqlx::query!`) ask the server for
  `EXPLAIN (FORMAT JSON)` output, which squad does not produce yet. Use
  the function forms (`sqlx::query`, `query_as`), or check the macros
  against a real Postgres.
- Queries still pass through the protocol on a local socket. That costs
  some tens of microseconds per query compared to the embedded API. A
  native in-process sqlx driver that removes this hop is on the
  [roadmap](roadmap.md).

## What has been tested against the server

| Client | Result |
| --- | --- |
| `psql` 16 | queries, scripts, `\copy`, `\d`, `\dt`, `\di`, `\dn`, `\ds`, `\dv`, `\dT`, `\l`, `\du` |
| `pg_dump` plain format | a dump with enum, serial and identity columns, jsonb, arrays, bytea, views, constraints and indexes loads through psql |
| `pgbench` | initialization, TPC-B-like and select-only scripts, simple and prepared mode, no failed transactions |
| `tokio-postgres` | typed parameters and results, transactions, savepoints, portals, `COPY IN`, SCRAM |
| `sqlx` 0.8 | see above |

## Protocol coverage

Implemented: startup and parameter status, SSL and GSS requests (declined),
simple query with multiple statements, extended query (Parse, Bind,
Describe, Execute with row limits and portal suspension, Close, Flush,
Sync), text and binary parameter and result formats, error responses with
SQLSTATE, detail, hint and position, `COPY … FROM STDIN` and
`COPY … TO STDOUT` in text and CSV format, trust, cleartext and
SCRAM-SHA-256 authentication, Unix domain sockets.

Not implemented:

- **TLS.** Connections are unencrypted. Bind to localhost or a Unix
  socket, or put a TLS-terminating proxy in front.
- **Query cancellation.** Cancel requests are accepted and ignored.
- **`LISTEN`/`NOTIFY`**, logical replication, and notices.
- **`COPY` binary format**, `COPY` from or to server-side files.
- In the extended protocol each `Execute` outside a transaction block
  commits on its own, whereas Postgres commits at `Sync`. This only
  matters for clients that pipeline several statements before one `Sync`
  and rely on them being atomic without `BEGIN`.
