# Contributing

Thank you for considering a contribution. This page explains how the
repository is organized and what a change needs before it can be merged.

## Layout

| Path | Content |
| --- | --- |
| `crates/squad-storage` | durable ordered key-value engine: pages, B+tree, buffer pool, WAL, recovery, simulated file system |
| `crates/squad-kv` | transactions: MVCC, row locks, deadlock detection, serializable validation |
| `crates/squad` | SQL engine and the embedding API |
| `crates/squad-server` | wire-protocol server and the `squad` binary |
| `crates/squad-sqllogictest` | sqllogictest runner and suite |
| `crates/squad-difftest` | differential fuzzer against PostgreSQL |
| `crates/squad-bench` | benchmarks against PostgreSQL and SQLite |
| `docs/` | documentation |
| `scripts/` | maintenance scripts |

[docs/architecture.md](docs/architecture.md) describes how the layers
fit together.

## Building and testing

You need a Rust toolchain (see `rust-version` in `Cargo.toml`) and a C
compiler, because the PostgreSQL parser is compiled from source.

```sh
cargo test --workspace          # everything; about a minute after the first build
cargo test -p squad             # the SQL engine only
cargo fmt --all
cargo clippy --workspace --all-targets
```

Tests that drive `psql` and `pgbench` skip themselves when the tools are
not installed.

## What a change needs

- **A test that fails without it.** For SQL behaviour, the preferred
  form is a statement in a script under `crates/squad/tests/regress/`,
  with expected output generated from a real PostgreSQL (below). For
  storage and transaction changes, extend the model-based or simulation
  tests.
- **PostgreSQL as the reference.** When in doubt about what a statement
  should return or which SQLSTATE an error should carry, run it against
  PostgreSQL 16 and match it. Deliberate differences belong in
  [docs/compatibility.md](docs/compatibility.md).
- **No silent gaps.** A feature that is not implemented must fail with
  SQLSTATE `0A000` and a message naming it, not be ignored — unless
  ignoring it is harmless and listed under "Accepted without effect".
- **Formatting and lints clean**, and documentation updated when
  behaviour changes.

## The SQL regression suite

Each `crates/squad/tests/regress/NAME.sql` has a `NAME.out` holding what
PostgreSQL 16 printed for it. The test `regress` runs the script against
squad and requires identical output; errors are compared by SQLSTATE.

To add or change a case, edit the `.sql` file (one statement per line,
or ending with `;` at the end of a line), then regenerate the expected
output from a PostgreSQL 16 server that was initialized with the C
locale:

```sh
initdb --locale=C -D /tmp/pgref && pg_ctl -D /tmp/pgref -o "-p 5433" start
PGHOST=127.0.0.1 PGPORT=5433 PGUSER=$USER scripts/regress-expected.sh NAME
cargo test -p squad --test regress
```

New scripts must be added to the list in `crates/squad/tests/regress.rs`.
Never edit a `.out` file by hand: it is evidence of what PostgreSQL does.

## Durability-sensitive changes

Changes to the write-ahead log, checkpoints, recovery or the commit path
must pass the crash simulations with many seeds before review:

```sh
SQUAD_SIM_SEEDS=10000 SQUAD_SIM_CONCURRENT_SEEDS=2000 \
    cargo test --release -p squad-storage --test crash_sim
SQUAD_SIM_SEEDS=500 cargo test --release -p squad --test crash_sim
```

A failing seed is reproducible with `SQUAD_SIM_START=<seed>
SQUAD_SIM_SEEDS=1`.

## Performance-sensitive changes

Run `cargo run --release -p squad-bench` before and after, on a quiet
machine, and include the numbers. See
[docs/benchmarks.md](docs/benchmarks.md) for how to read them.

## Commit messages

Describe what changed and why in plain sentences. The subject line says
what the commit does in the imperative ("Add …", "Fix …").

## License

By contributing you agree that your contribution is licensed under the
MIT license, like the rest of the project.
