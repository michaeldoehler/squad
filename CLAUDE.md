# squad — notes for coding agents

squad is an embeddable database for Rust with PostgreSQL's SQL dialect and
semantics. Read `README.md`, `CONTRIBUTING.md` and `docs/architecture.md`
before changing code.

## Rules

- PostgreSQL 16 is the reference for results and SQLSTATEs. Never edit
  `crates/squad/tests/regress/*.out` by hand; they are PostgreSQL output
  (`scripts/regress-expected.sh`).
- Every change needs a test that fails without it. Unsupported features fail
  with SQLSTATE `0A000`; anything accepted without effect is listed in
  `docs/compatibility.md`.
- Documentation is plain and factual and states limits honestly. Update the
  chapter in `docs/` that covers the behaviour you change, and `CHANGELOG.md`.
- Git: work on `main` or on descriptive branches (`feat/…`, `fix/…`). Commit
  messages are plain descriptions. No `Co-Authored-By` lines, no "Generated
  with" lines, no branch names starting with `claude/`, no mention of AI tools
  in commits, code or docs.

## Checks before committing

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --release
cargo deny check
```

Changes to the WAL, checkpoints, recovery, MVCC or remote storage also run
the crash simulations with more seeds:

```sh
SQUAD_SIM_SEEDS=500 SQUAD_SIM_CONCURRENT_SEEDS=100 cargo test --release -p squad-storage --test crash_sim
SQUAD_SIM_SEEDS=50 cargo test --release -p squad --test crash_sim
```

Tests against a real S3 implementation run when `SQUAD_TEST_S3_URL` is set.
`eval "$(scripts/garage-dev.sh start)"` starts a local single-node Garage
(binary `garage` on the PATH) and exports it. Tests that drive `psql` and
`pgbench` skip themselves when the tools are not installed.
