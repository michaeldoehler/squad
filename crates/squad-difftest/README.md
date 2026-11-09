# squad-difftest

A differential fuzzer: it generates a random schema, random data and a
stream of random SQL statements from a seed, runs every statement against
an in-memory squad database and against a real PostgreSQL server, and
reports every difference.

Compared per statement:

- success versus error, and the SQLSTATE of the error;
- column count and column names;
- the text of every value. Rows are compared as a multiset unless the
  query sorts by all of its output columns, in which case the order must
  match too;
- after every data-modifying statement, the full contents of the table it
  touched.

When a difference is found the tool replays the statements so far on fresh
databases and removes everything that is not needed to reproduce it (delta
debugging), then prints the minimal script and both results.

## Running

A PostgreSQL 16 server with the C locale is needed (`initdb --locale=C`;
squad sorts text bytewise). The tool creates and drops a scratch database
named `fuzz_difftest_<pid>`.

```sh
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres
cargo run --release -p squad-difftest -- --seed 1 --iterations 100 --statements 100
```

| Option | Meaning |
| --- | --- |
| `--seed N` | Seed of the first iteration; iteration `i` uses `N + i`. A seed reproduces its run exactly. |
| `--iterations N` | Number of schemas to generate (default 10). |
| `--statements N` | Statements per schema, not counting setup (default 200). |
| `--only select\|dml\|ddl\|txn` | Restrict the statement mix. |
| `--pg CONNINFO` | libpq-style connection string without `dbname`; defaults to `PGHOST`/`PGPORT`/`PGUSER`. |
| `--strict` | Also report and fail on statements that fail on both sides with different SQLSTATEs. |
| `--no-minimize` | Print the whole script instead of a minimized one. |
| `--max-reports N` | Stop printing after N reports (default 20); counting continues. |
| `--echo` | Print every statement and the first line of squad's result. |

The exit status is non-zero if any difference was found.

## What is generated

- **Schema:** 2–4 tables with single or composite primary keys, columns of
  `smallint`, `integer`, `bigint`, `numeric(9,2)`, `double precision`,
  `text`, `varchar(6)`, `boolean`, `date`, `timestamp`, `timestamptz`,
  `uuid`, `bytea`, `jsonb`, `integer[]`, `text[]` and an enum; NOT NULL,
  UNIQUE, DEFAULT, CHECK, foreign keys with different delete actions,
  secondary and unique indexes.
- **Data:** NULLs, duplicates, empty strings, multi-byte text, boundary
  integers.
- **Queries:** typed expressions (arithmetic, comparisons across numeric
  types, three-valued logic, CASE, COALESCE, NULLIF, GREATEST/LEAST,
  casts, string, math, date/time, jsonb and array functions and
  operators, IN, BETWEEN, LIKE, regular expressions, IS DISTINCT FROM),
  joins of every kind including USING and LATERAL, derived tables, CTEs
  (also recursive), VALUES, set-returning functions, WHERE, GROUP BY,
  HAVING, aggregates with DISTINCT, FILTER and ORDER BY, window functions
  with ROWS, RANGE and GROUPS frames, DISTINCT and DISTINCT ON, ORDER BY
  with LIMIT/OFFSET, set operations, and scalar, EXISTS, IN, ANY and ALL
  subqueries, correlated or not.
- **Data modification:** INSERT (multi-row, DEFAULT, ON CONFLICT DO
  NOTHING / DO UPDATE, INSERT ... SELECT, RETURNING), UPDATE (also with
  FROM), DELETE (also with USING).
- **DDL and transactions:** ADD/DROP COLUMN, CREATE/DROP INDEX; BEGIN,
  COMMIT, ROLLBACK, savepoints, also with DDL inside.

## What is deliberately not generated or not counted

Some differences are inherent and would only produce noise:

- **Evaluation order.** If exactly one side raises a data-dependent error
  (overflow, division by zero, invalid argument), the statement is counted
  as a "one-sided arithmetic error" rather than a difference: which rows an
  expression is evaluated for depends on the plan, and PostgreSQL does not
  evaluate output columns of subqueries that the outer query does not use.
- **Row order of updates.** PostgreSQL visits rows in heap order, squad in
  primary key order. When several rows of one statement violate different
  constraints, the reported SQLSTATE can differ (shown only with
  `--strict`). For the same reason UNIQUE columns are only ever updated to
  constants.
- **Numeric precision.** squad's `numeric` holds 28 significant digits, so
  the generator keeps values small enough that results fit.
- **Float aggregation.** Sums of floats depend on the order of addition;
  only exactly representable values are summed.
- **Locale.** Only ASCII letters take part in case conversion, because the
  reference server runs in the C locale.
- Anything whose result is not determined by the query: `now()`,
  `random()`, LIMIT without a total order, window functions over ties.
- An internal error (`XX000`) of the reference server is ignored.
