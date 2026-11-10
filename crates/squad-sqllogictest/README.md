# squad-sqllogictest

A [sqllogictest](https://www.sqlite.org/sqllogictest/) driver for squad,
built on the [`sqllogictest`](https://crates.io/crates/sqllogictest) crate.
It serves two purposes:

* **Our own suite** (`slt/*.slt`, about 470 records) covers the SQL core:
  DDL and DML, joins, subqueries, common table expressions (also
  recursive), aggregates, window functions, `ON CONFLICT`, `RETURNING` and
  transactions. Every expected result was produced by PostgreSQL 16.
  `cargo test -p squad-sqllogictest` runs it.
* **The public corpus** written for SQLite (about 5.7 million executed
  records) is run on demand with `scripts/sqllogictest-corpus.sh`.

## Running

```sh
cargo test -p squad-sqllogictest                              # the checked-in suite
cargo run --release -p squad-sqllogictest -- path/to/file.slt # any file or directory
scripts/sqllogictest-corpus.sh                                # the public corpus
```

The runner prints passed / failed / skipped records per file and per
directory, and the first failures of every file. Options: `--jobs N`,
`--failures N`, `--group-depth N`, `--dump FILE`, `--verdicts FILE`.
Each file runs against a fresh in-memory database.

## Format

Scripts use the sqllogictest format. Values are rendered as the original
SQLite harness renders them, so the public corpus runs unchanged:

| Type letter | Rendering |
| --- | --- |
| `I` | integer (a real is truncated, text is read like C `atoi`) |
| `R` | real with three decimals |
| `T` | text; the empty string is `(empty)`, characters outside printable ASCII are `@` |

NULL is `NULL`. Results may be listed one row per line (values separated by
spaces) or one value per line. `rowsort`, `valuesort`, `hash-threshold` and
result hashing (`N values hashing to <md5>`) work as in the original; files
with the extension `.test` start with the original default threshold of 8.
In `skipif` / `onlyif` conditions squad is called `postgresql`. Errors can
be matched by SQLSTATE: `statement error (23505)`.

## Writing tests

Write the record headers and the SQL, then let PostgreSQL fill in the
expectations:

```
statement
CREATE TABLE t (a int PRIMARY KEY, b text)

query IT rowsort
SELECT a, b FROM t
```

```sh
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres ./generate.py slt/my_file.slt
```

`generate.py` runs every record through `psql` against a scratch database
and rewrites the file with `statement ok`, `statement error (SQLSTATE)`,
`query error (SQLSTATE)` or the result rows. Run without arguments it
regenerates the whole suite. The server must use the C locale
(`initdb --locale=C`), because squad sorts text bytewise.

## Results on the public corpus

Measured on 2026-11-10 against corpus revision
`c93ac416f05635ff5cc490009add936d9753d01d` of
<https://github.com/gregrahn/sqllogictest> (622 files). "Skipped" records
are excluded by the corpus's own `skipif postgresql` / `onlyif <other
engine>` conditions.

| Directory | Files | Passed | Failed | Skipped | Pass rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| select1.test – select5.test | 5 | 10,706 | 0 | 0 | 100.00 % |
| evidence | 12 | 137 | 11 | 11 | 92.57 % |
| index/between | 13 | 124,618 | 0 | 0 | 100.00 % |
| index/commute | 52 | 514,321 | 0 | 0 | 100.00 % |
| index/delete | 14 | 160,942 | 0 | 0 | 100.00 % |
| index/in | 13 | 132,907 | 0 | 0 | 100.00 % |
| index/orderby | 31 | 312,955 | 0 | 0 | 100.00 % |
| index/orderby_nosort | 49 | 495,167 | 0 | 0 | 100.00 % |
| index/random | 26 | 191,958 | 6,425 | 51,160 | 96.76 % |
| index/view | 16 | 104,369 | 4,860 | 14,580 | 95.55 % |
| random/aggregates | 130 | 1,174,483 | 1 | 628,069 | 100.00 % |
| random/expr | 120 | 1,200,104 | 262 | 683,071 | 99.98 % |
| random/groupby | 14 | 118,925 | 0 | 25,816 | 100.00 % |
| random/select | 127 | 1,121,853 | 0 | 342,667 | 100.00 % |
| **total** | **622** | **5,663,445** | **11,559** | **1,745,374** | **99.80 %** |

### Relative to PostgreSQL

The corpus records what SQLite answers. squad is meant to answer what
PostgreSQL answers, so every file was also replayed against PostgreSQL
16.15 with `scripts/sqllogictest-pg-compare.py`, which judges PostgreSQL by
the same expectations and compares the verdicts record by record:

| | Records |
| --- | ---: |
| squad and PostgreSQL both pass | 5,663,442 |
| squad and PostgreSQL both fail | 11,556 |
| only squad fails | 3 |
| only PostgreSQL fails | 3 |

squad and PostgreSQL agree on 99.9999 % of the records. Nearly all failed
records are therefore expectations that only hold for SQLite:

* `index/random`: unary plus on a text column (`SELECT + col2 ...`), which
  PostgreSQL rejects with 42883.
* `index/view`: `DROP VIEW` of a view another view is defined on, which
  PostgreSQL rejects with 2BP01, and the statements that follow from it.
* `random/expr`, `random/aggregates`: integer overflow and division by
  zero that SQLite evaluates to NULL or a wider type; `NULLIF` and `CASE`
  result types (numeric instead of integer division).
* `evidence`: empty `IN ()` lists and repeated assignments to one column in
  `UPDATE`, both syntax errors in PostgreSQL.

The six disagreements:

* Three queries pass in PostgreSQL and fail in squad. PostgreSQL does not
  evaluate a failing subexpression there: twice inside a `CASE` branch that
  constants rule out while planning, once because it evaluates the cheaper
  of two `WHERE` conditions first.
* Three records fail in PostgreSQL and pass in squad: PostgreSQL accepts
  `UPDATE` and `DELETE` on a simple view, which the corpus expects to fail
  and squad does not support; and one constant expression where PostgreSQL
  reports a division by zero while planning.
