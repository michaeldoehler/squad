## What and why

<!-- What does this change, and what problem does it solve? -->

## Checklist

- [ ] A test fails without this change (regression script, model or simulation test)
- [ ] `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` are clean
- [ ] Behaviour differences from PostgreSQL are listed in `docs/compatibility.md`
- [ ] Documentation and `CHANGELOG.md` are updated
- [ ] Changes to the log, checkpoints, recovery or remote storage ran the crash simulations with more seeds (see CONTRIBUTING.md)
