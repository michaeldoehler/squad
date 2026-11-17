# Server mode and the `squad` command

squad's primary form is a library, but everything that speaks the
Postgres wire protocol can use it through `squad-server`: psql, GUI
tools, migration tools, and applications in any language.

## Running a server

```sh
cargo install --path crates/squad-server     # installs the `squad` binary

squad serve ./data                           # 127.0.0.1:5432, local clients trusted
SQUAD_PASSWORD=secret squad serve ./data --listen 0.0.0.0:5433
squad serve ./data --unix-socket-dir /tmp    # also /tmp/.s.PGSQL.5432
squad serve --memory                         # volatile, for tests and CI

psql -h 127.0.0.1 -U squad squad
```

| Option | Meaning |
| --- | --- |
| `--listen <addr>` | TCP address (default `127.0.0.1:5432`) |
| `--unix-socket-dir <dir>` | additionally listen on `<dir>/.s.PGSQL.<port>` |
| `--users-file <file>` | logins, see [authentication](#authentication) |
| `--user <name>` | login name for the password in `SQUAD_PASSWORD` |
| `--user <name> --password <pw>` | a login given on the command line (repeatable); prints a warning, because other users of the machine can read it in the process list |
| `--read-only <name>` | the login may only read (repeatable); see [login attributes](#login-attributes) |
| `--no-bypass-rls <name>` | row-level security policies apply to the login (repeatable) |
| `--trust` | let every client in without a password, also on an address other than loopback |
| `--cleartext-password` | ask for the password in clear text, for clients without SCRAM |
| `--tls-cert <file> --tls-key <file>` | PEM certificate chain and private key; see [TLS](#tls) |
| `--require-tls` | refuse TCP connections that did not switch to TLS |
| `--max-connections <n>` | open connections at most (default 100) |
| `--authentication-timeout <d>` | time from connecting to being logged in (default `60s`) |
| `--idle-timeout <d>` | close connections whose client sends nothing for this long (default off) |
| `--max-message-size <bytes>` | largest message a logged-in client may send (default 1 GiB) |
| `--sync sync\|group\|async` | commit durability, see [durability](durability.md) |
| `--memory` | serve an in-memory database |
| `--remote <url>`, `--remote-file <file>`, `--remote-sync async\|sync`, `--remote-restore` | keep the durable copy in an object store, see [cloud storage](cloud-storage.md); `--remote-file` reads the URL from a file, so that credentials in it stay out of the process list |
| `--replica-of <url>`, `--replica-of-file <file>` | serve a **read replica** of the database whose writer runs with `--remote <url>`; `<dir>` holds the replica's local copy |
| `--poll-interval <ms>` | how often a replica looks for new commits (default 1000) |
| `--work-mem <size>` | default `work_mem` of sessions, e.g. `16MB` (default `4MB`); see [embedding](embedding.md#memory-for-queries-work_mem-and-temporary-files) |
| `--temp-dir <dir>` | directory for the temporary files of queries that exceed `work_mem` (default `<dir>/tmp`, or a directory under the system's temporary directory with `--memory`) |

Durations are written `30s`, `500ms`, `5min` or `1h`; a bare number
means seconds. `--help` (or `-h`) after any subcommand prints the
usage.

| Environment variable | Meaning |
| --- | --- |
| `SQUAD_PASSWORD`, `SQUAD_PASSWORD_FILE` | password of the login `SQUAD_USER` (or `--user`); the `_FILE` form reads it from a file, as Docker and Kubernetes secrets provide it |
| `SQUAD_USER` | login name for `SQUAD_PASSWORD` (default `squad`) |
| `SQUAD_LOG` | which messages of the database go to stderr: `off`, `error`, `warn`, `info` (default) or `debug`; at `info` these are the progress of a restore from the object store and the snapshots uploaded to it |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL` | S3 settings for `--remote`/`--replica-of`; each also as `<NAME>_FILE` |

A replica answers queries and refuses writes with SQLSTATE `25006`;
`SELECT squad_promote()` from any client turns it into the writer. See
[replication](replication.md) for guarantees, monitoring and the
failover procedure.

The database name a client sends is accepted as given; there is one
database per directory.

Other subcommands:

```sh
squad shell ./data            # interactive SQL prompt on the embedded API
squad backup ./data ./copy    # consistent copy of a database directory
squad verify ./data           # integrity check
squad ping --host 127.0.0.1 --port 5432   # health check, exit code 0 or 1
echo 'secret' | squad hash-password       # SCRAM verifier for a users file
```

`squad serve` stops on SIGTERM or SIGINT (Ctrl-C): it closes the client
connections and the database cleanly and exits with status 0.

`backup` and `verify` open the directory themselves, so the database must
not be open elsewhere; a running application calls `Database::backup`
instead.

## Authentication

Without any password configured, the server trusts every client: the
user name a client sends is accepted as given. This is only allowed on
loopback addresses (`127.0.0.1`, `::1`) and the Unix socket. On any other
address, `squad serve` refuses to start, and `Server::bind` fails with
`PermissionDenied`, unless trust is asked for explicitly (`--trust`,
`ServerConfig::trust_remote`).

Logins come from three places, which can be combined:

- `SQUAD_PASSWORD` or `SQUAD_PASSWORD_FILE`, for the user `SQUAD_USER`
  (default `squad`) or the single `--user`.
- `--users-file <file>`: one login per line, `user:password` or
  `user:SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`. The
  second form is PostgreSQL's verifier format: it can be copied from
  `pg_authid.rolpassword` of a PostgreSQL server or produced with `squad
  hash-password`, and the file then holds no password. Attributes may
  follow the user name, separated by spaces (below). Empty lines and
  lines starting with `#` are ignored. Passwords are turned into
  verifiers when the file is read. A user name cannot contain spaces or
  colons.
- `--user <name> --password <pw>`, with a warning (process list).

Clients authenticate with SCRAM-SHA-256, or with a clear-text password
under `--cleartext-password` (protect it with TLS). An unknown user and a
wrong password fail the same way, with `28P01`. There are no roles or
privileges in SQL (`GRANT` and `CREATE ROLE` are accepted with a warning
and do nothing, see
[compatibility](compatibility.md#accepted-without-effect)); what a login
may do is set by its attributes.

### Login attributes

| Attribute | Default | Effect |
| --- | --- | --- |
| `bypass_rls` | on | Row-level security policies apply to the login only on tables with `FORCE ROW LEVEL SECURITY`, as for a table's owner in PostgreSQL. Turned off with `no_bypass_rls`: the policies of every table with row-level security apply, and the login cannot change those tables or any policy. |
| `read_only` | off | Every transaction is read-only; writes and DDL fail with `25006`, and `BEGIN READ WRITE` is refused. |

A login without attributes has full access, as before attributes
existed. The semantics are described in [security](security.md).
Attributes are given in the users file after the name, on the command
line, or in `ServerConfig::logins`:

```text
# the application, which owns its tables
beats:SCRAM-SHA-256$4096:…
# the analytics tool: reads, and only what the policies allow
superset read_only no_bypass_rls:SCRAM-SHA-256$4096:…
```

```sh
squad serve ./data --users-file users --read-only reporting --no-bypass-rls reporting
```

The attributes apply with trust authentication too (the client's user
name decides). `current_user` and `session_user` return the login name;
the server reports `is_superuser` as `on` for logins with `bypass_rls`
and `off` for the others.

## TLS

With `--tls-cert` and `--tls-key` (PEM; the key in PKCS#8, PKCS#1 or SEC1
form), the server answers a client's SSLRequest with `S` and switches the
connection to TLS 1.2 or 1.3 (rustls with the ring provider). Clients
choose with libpq's `sslmode`: `prefer` (libpq's default) and `require`
encrypt, `verify-ca` and `verify-full` also check the certificate.
Without a certificate, the request is declined and the connection
continues unencrypted, as before.

`--require-tls` refuses TCP connections that did not switch to TLS with
`FATAL 28000` ("… no encryption …"). Connections over the Unix socket
are not affected. Data a client sends in clear text together with its
SSLRequest is rejected as a protocol violation (the issue of
CVE-2021-23214). The newer "direct" TLS handshake of PostgreSQL 17
(`sslnegotiation=direct`) is not supported; client certificates are not
checked unless the application builds its own rustls configuration and
passes it with `TlsConfig::from_rustls`. Certificates are read at start;
a renewed certificate takes effect after a restart.

## Limits and timeouts

| Limit | Default | When exceeded |
| --- | --- | --- |
| open connections | 100 | `FATAL 53300` "sorry, too many clients already" after the client's startup packet. Cancel requests are still served. At most 16 refused clients are answered at a time; beyond that the socket is closed. |
| time to log in, TLS handshake included | 60 s | the connection is closed |
| message size before login | 10,000 bytes | `FATAL 08P01`, as for the startup packet in PostgreSQL |
| message size after login | 1 GiB | `FATAL 08P01` |
| silence of a logged-in client | off | `FATAL 57P05`, or `25P03` inside a transaction block |

The idle timeout applies whenever the server waits for the client,
including between the messages of an extended-protocol exchange and
during `COPY FROM STDIN`. There is no write timeout: a client that stops
reading a large result holds its connection thread until the operating
system gives up on the socket.

## Cancelling queries

Every connection receives a process id and a random secret key
(BackendKeyData). A cancel request with both, as `psql` sends on Ctrl-C
and drivers send through their cancel APIs (`cancel_token()` in
tokio-postgres), stops the statement running on that connection with
`57014` ("canceling statement due to user request"). A request with a
wrong key is ignored; the requester never gets an answer, as in
PostgreSQL. `SET statement_timeout` works as well. What can be
interrupted is described in
[transactions](transactions.md#cancelling-statements-and-statement_timeout).

`pg_stat_activity` lists every connection of the server (and every
connection the embedding application opened itself) with the pid of its
BackendKeyData, user, `application_name`, state and current or last
statement; `client_addr`, `client_port` and the wait columns are NULL.
Tools that stop queries with SQL work as with PostgreSQL, for example
Apache Superset's SQL Lab
(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = '…'`):

- `pg_cancel_backend(pid)` cancels the statement running on that
  connection like a cancel request (`57014`) and returns true.
- `pg_terminate_backend(pid [, timeout_ms])` ends that connection: a
  running statement stops, the server sends `FATAL 57P01`
  ("terminating connection due to administrator command") and closes the
  connection, also when it was idle. With a timeout the call waits until
  the connection is gone and returns false with a warning if it is not
  gone in time.
- An unknown pid gives false and the warning `PID … is not a PostgreSQL
  backend process`.

There are no roles, so every login may cancel or terminate every
connection (PostgreSQL allows it for the same role, superusers and
members of `pg_signal_backend`). A statement waiting for a row lock
notices the request only when the wait ends, as described for cancel
requests.

## Health checks

`squad ping` connects like a client and exits with 0 if the server
accepts connections, 1 otherwise:

- With `SQUAD_PASSWORD` (or `SQUAD_PASSWORD_FILE`) set, or with a server
  that trusts the client, it logs in and runs `SELECT 1`.
- If the server asks for a password it does not have, refuses the
  password, or requires TLS, it still reports the server as up (the
  server answered correctly) and says why it did not log in.
- No connection, no or a malformed answer, "too many clients" (`53300`)
  and a failing `SELECT 1` count as not ready.

Options: `--host` (a directory means a Unix socket), `--port`, `--user`
(default `SQUAD_USER` or `squad`), `--database`, `--timeout` (default
`5s`). `ping` does not use TLS.

## Container image

The `Dockerfile` at the repository root builds a small image (Debian
slim, the stripped `squad` binary, user `squad` with uid 10001):

```sh
docker build -t squad .
docker run -d --name squad -p 5432:5432 -v squad-data:/data \
    -e SQUAD_PASSWORD=secret squad
```

It runs `squad serve /data --listen 0.0.0.0:5432`. Because that address
is not loopback, the container exits with an error unless a password is
configured (`SQUAD_PASSWORD`, `SQUAD_PASSWORD_FILE` with a Docker or
Kubernetes secret, or `--users-file`) or `--trust` is passed. Arguments
after the image name replace the command, e.g. `serve /data --listen
0.0.0.0:5432 --remote-file /run/secrets/remote_url`. The health check
runs `squad ping` every 10 seconds; with `SQUAD_PASSWORD(_FILE)` set it
logs in and runs `SELECT 1`. `/data` is a volume and must be writable
by uid 10001.

## Embedding the server

```rust
use squad::Database;
use squad_server::{Auth, Serve, Server, ServerConfig, TlsConfig};

let db = Database::open("data/", Default::default())?;

// Simple: trust every client on a loopback address.
let handle = db.serve("127.0.0.1:5432")?;
println!("{}", handle.url().unwrap());   // postgres://squad@127.0.0.1:5432/squad

// Configured: passwords, TLS, limits and a Unix socket.
let mut server = Server::new(db.clone(), ServerConfig {
    auth: Auth::Password { users: [("app".into(), "secret".into())].into() },
    tls: Some(TlsConfig::from_pem_files("server.crt", "server.key")?),
    require_tls: true,
    max_connections: 50,
    idle_timeout: Some(std::time::Duration::from_secs(600)),
    unix_socket: Some("/tmp".into()),
    ..ServerConfig::default()
});
server.bind("0.0.0.0:5432")?;
let handle = server.spawn();
// …
handle.shutdown();
```

`Serve::serve` trusts every client and therefore only accepts loopback
addresses; anything else fails with `PermissionDenied`. `Auth::Verifiers`
takes SCRAM verifiers instead of passwords, e.g. from
`squad_server::users::read_users_file`, which also returns the login
attributes of the file:

```rust
let users = squad_server::users::read_users_file("users")?;
let config = ServerConfig {
    auth: Auth::Verifiers { users: users.verifiers },
    logins: users.logins,           // HashMap<String, LoginAttributes>
    ..ServerConfig::default()
};
``` `ServerConfig` prints passwords
as their user names only (`Debug`).

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
| `psql` 16 | queries, scripts, `\copy`, `\d` (with policies), `\dt`, `\di`, `\dn`, `\ds`, `\dv`, `\dT`, `\l`, `\du`; TLS with `sslmode=verify-full`, notices |
| `pg_dump` plain format | a dump with enum, serial and identity columns, jsonb, arrays, bytea, views, constraints and indexes loads through psql |
| `pgbench` 16 | initialization, TPC-B-like and select-only scripts, simple and prepared mode, no failed transactions |
| `tokio-postgres` | typed parameters and results, transactions, savepoints, portals, `COPY IN`, SCRAM, TLS (tokio-postgres-rustls), cancel tokens, notices |
| `sqlx` 0.8 | see above |
| SQLAlchemy 1.4, 2.0, 2.1 with psycopg2 | `inspect(engine)`: tables, views, columns, primary, foreign and unique keys, indexes, check constraints, compared with PostgreSQL 16 by `scripts/sqlalchemy-check.py` (run by hand). Remaining differences: check constraint text lacks PostgreSQL's casts (`score >= 0` instead of `score >= 0::numeric`), and view columns of type `varchar(n)` are reported as `text`. |

## Protocol coverage

Implemented: startup and parameter status, TLS through SSLRequest, GSS
requests (declined), cancel requests, simple query with multiple
statements, extended query (Parse, Bind, Describe, Execute with row
limits and portal suspension, Close, Flush, Sync), text and binary
parameter and result formats, error responses with SQLSTATE, detail,
hint and position, notice responses (WARNING and NOTICE for statements
accepted without effect), `COPY … FROM STDIN` and `COPY … TO STDOUT` in
text and CSV format, trust, cleartext and SCRAM-SHA-256 authentication,
Unix domain sockets.

Not implemented:

- **Direct TLS** (PostgreSQL 17's `sslnegotiation=direct`), client
  certificate authentication from the command line, GSSAPI encryption,
  SCRAM channel binding (`SCRAM-SHA-256-PLUS`).
- **`LISTEN`/`NOTIFY`**, PostgreSQL's replication protocols (streaming
  and logical; squad's own [replication](replication.md) goes through
  an object store).
- **`COPY` binary format**, `COPY` from or to server-side files.
- In the extended protocol each `Execute` outside a transaction block
  commits on its own, whereas Postgres commits at `Sync`. This only
  matters for clients that pipeline several statements before one `Sync`
  and rely on them being atomic without `BEGIN`.
- One thread per connection: `max_connections` bounds the threads, but
  thousands of connections need a pooler in front, as with PostgreSQL.
