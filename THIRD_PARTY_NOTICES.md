# Third-party notices

squad is licensed under the MIT license (see [LICENSE](LICENSE)). It
builds on the work below. Rust dependencies fetched from crates.io carry
their own licenses, which `cargo deny check licenses` verifies against
the allow list in [deny.toml](deny.toml).

## PostgreSQL parser (bundled in binaries)

squad parses SQL with [`pg_query`](https://crates.io/crates/pg_query)
(MIT), which compiles [libpg_query](https://github.com/pganalyze/libpg_query)
into every binary that links squad. libpg_query contains:

- **PostgreSQL server source code**, used under the
  [PostgreSQL License](https://www.postgresql.org/about/licence/).
  Portions Copyright (c) 1996-2026, The PostgreSQL Global Development
  Group. Portions Copyright (c) 1994, The Regents of the University of
  California.
- **libpg_query** itself, 3-clause BSD license. Copyright (c) 2015,
  Lukas Fittl; Copyright (c) 2016-2026, Duboce Labs, Inc. (pganalyze).
- **upb** protobuf runtime, 3-clause BSD license. Copyright 2008 Google
  Inc. It includes **utf8_range**, MIT license, Copyright (c) 2019 Yibo
  Cai, Copyright 2022 Google LLC.
- **xxHash**, 2-clause BSD license. Copyright (C) 2012-2020 Yann Collet.

The bundled parser is the one of PostgreSQL 18; squad's SQL semantics,
results and error codes are verified against PostgreSQL 16 (see
[docs/compatibility.md](docs/compatibility.md)).

## Test data

- The expected output of the SQL regression suite
  (`crates/squad/tests/regress/*.out`) was produced by running the
  scripts against PostgreSQL 16.
- The public [sqllogictest](https://www.sqlite.org/sqllogictest/) corpus
  is not part of this repository; `scripts/sqllogictest-corpus.sh`
  downloads it on demand.
