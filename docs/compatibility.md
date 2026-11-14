# PostgreSQL compatibility

squad aims to behave like PostgreSQL 16 for the SQL an OLTP application
sends: the same statements are accepted, return the same results and fail
with the same SQLSTATE. It reports itself as `PostgreSQL 16.4 (squad
<version>)`.

Compatibility is measured, not claimed. The spec for squad defines six
levels; this page says where each stands and lists every known
difference. If something is not listed as unsupported or different and
does not behave like Postgres, that is a bug.

| Level | Scope | Status | Evidence |
| --- | --- | --- | --- |
| L1 SQL core | DDL, DML, joins, subqueries, CTEs (also recursive), aggregates, window functions, `ON CONFLICT`, `RETURNING` | implemented | SQL regression suite compared with PostgreSQL 16; sqllogictest; differential fuzzing |
| L2 Types and functions | integers, numeric, floats, text, bytea, boolean, date/time, uuid, json/jsonb, arrays, enums; operators and several hundred functions | implemented, with the limits below | value tests with results from PostgreSQL 16 |
| L3 Semantics | SQLSTATE codes, NULL logic, implicit casts, isolation behaviour | implemented | regression suite compares SQLSTATEs; concurrency tests |
| L4 Catalog | `pg_catalog`, `information_schema` as tools query them | implemented for psql, sqlx and common ORM introspection | replayed psql and ORM catalog queries |
| L5 Wire protocol | protocol v3, extended query, `COPY`, SCRAM | implemented without TLS and cancellation | psql, pgbench, pg_dump, tokio-postgres, sqlx |
| L6 Extensions | PL/pgSQL, `LISTEN`/`NOTIFY`, full-text search, extension API | not started | — |

How the evidence is produced is described in [testing](testing.md).

## Statements

### Supported

- **Queries:** `SELECT` with `DISTINCT`, `DISTINCT ON`, `WHERE`,
  `GROUP BY`, `HAVING`, `WINDOW`, `ORDER BY … NULLS FIRST/LAST`, `LIMIT`,
  `OFFSET`, `FETCH FIRST`; `VALUES`; `TABLE`; `UNION`, `INTERSECT`,
  `EXCEPT` with and without `ALL`.
- **Joins:** inner, left, right, full, cross, `NATURAL`, `USING`,
  `LATERAL`, set-returning functions in `FROM` with `WITH ORDINALITY`.
- **Subqueries:** scalar, `EXISTS`, `IN`, `ANY`/`ALL`, `ARRAY(…)`, row
  comparisons, correlated or not, in `FROM`, `WHERE` and the select list.
- **CTEs:** `WITH`, `WITH RECURSIVE`, data-modifying CTEs
  (`WITH x AS (INSERT … RETURNING …) SELECT …`).
- **Aggregates:** `count`, `sum`, `avg`, `min`, `max`, `bool_and`,
  `bool_or`, `every`, `bit_and`, `bit_or`, `string_agg`, `array_agg`,
  `json_agg`, `jsonb_agg`, `json_object_agg`, `jsonb_object_agg`,
  `variance`, `var_pop`, `var_samp`, `stddev`, `stddev_pop`,
  `stddev_samp`; with `DISTINCT`, `ORDER BY` and `FILTER`.
- **Window functions:** `row_number`, `rank`, `dense_rank`,
  `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first_value`,
  `last_value`, `nth_value` and every aggregate, with `PARTITION BY`,
  `ORDER BY`, named windows and `ROWS`/`RANGE`/`GROUPS` frames.
- **Data changes:** `INSERT` (multi-row, `DEFAULT VALUES`,
  `INSERT … SELECT`, `OVERRIDING SYSTEM VALUE`), `ON CONFLICT DO
  NOTHING` / `DO UPDATE` with a column or constraint target, `UPDATE`
  (also `FROM`, multi-column `SET (a, b) = (…)`), `DELETE` (also
  `USING`), `RETURNING` on all three, `TRUNCATE` (`RESTART IDENTITY`,
  `CASCADE`).
- **Locking clauses:** `FOR UPDATE`, `FOR SHARE`, `NOWAIT`,
  `SKIP LOCKED`, `OF table`.
- **Tables:** `CREATE TABLE` (`IF NOT EXISTS`, `AS SELECT`), column
  defaults, `NOT NULL`, `PRIMARY KEY`, `UNIQUE` (also `NULLS NOT
  DISTINCT`), `CHECK`, `REFERENCES` / `FOREIGN KEY` with `ON DELETE` and
  `ON UPDATE` actions `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`;
  `serial` types and `GENERATED … AS IDENTITY`; `DROP TABLE`
  (`IF EXISTS`, `CASCADE`).
- **`ALTER TABLE`:** `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN` `SET
  DEFAULT` / `DROP DEFAULT` / `SET NOT NULL` / `DROP NOT NULL` / `TYPE …
  USING` / `ADD GENERATED … AS IDENTITY`, `ADD CONSTRAINT`, `DROP
  CONSTRAINT`, `RENAME TO`, `RENAME COLUMN`, `RENAME CONSTRAINT`.
- **Columnar tables** (not in PostgreSQL): `CREATE TABLE … USING
  columnar [WITH (order_by = 'a, b')]`, with the aggregates
  `approx_count_distinct(x)` and `approx_percentile(x, p)`. `INSERT`,
  `COPY FROM`, `DELETE`, `TRUNCATE`, `DROP TABLE`, and `ALTER TABLE`
  `ADD COLUMN` (constant default), `DROP COLUMN`, `RENAME COLUMN`,
  `RENAME TO`, `ALTER COLUMN SET/DROP DEFAULT`, `DROP NOT NULL`. Not
  supported on columnar tables (`0A000`): `UPDATE`, other `ALTER TABLE`
  actions, `ADD COLUMN` with a volatile default or constraints, indexes,
  primary key, unique and foreign key constraints, `DELETE … USING`,
  `DELETE … RETURNING`, `SELECT … FOR UPDATE/SHARE`. `VACUUM` on a
  columnar table also compacts its delta, merges small segments and
  removes unreferenced segment objects. See [analytics](analytics.md).
- **Other objects:** `CREATE [UNIQUE] INDEX` on columns, `DROP INDEX`;
  `CREATE SEQUENCE`, `ALTER SEQUENCE`, `nextval`, `currval`, `setval`,
  `lastval`; `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`; `CREATE
  [OR REPLACE] VIEW`; `CREATE SCHEMA`, `search_path`.
- **Transactions:** `BEGIN`/`START TRANSACTION` with isolation level and
  `READ ONLY`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`,
  `RELEASE`, `SET TRANSACTION`. DDL is transactional.
- **Session:** `SET`, `SHOW`, `RESET`, `SET TIME ZONE`, `PREPARE`,
  `EXECUTE`, `DEALLOCATE`, `DISCARD ALL`, `EXPLAIN [ANALYZE]`,
  `CHECKPOINT`, `VACUUM`, `ANALYZE`.

### Accepted without effect

So that dumps and migration scripts run: `CREATE EXTENSION`, `COMMENT
ON`, `GRANT`, `REVOKE`, `ALTER … OWNER TO`, `CREATE/ALTER/DROP ROLE`,
`LOCK TABLE`, `SET CONSTRAINTS`, `REINDEX`, `CLUSTER`, `ANALYZE`, table
storage options, `DEFERRABLE`, and `COLLATE` clauses (text always
compares bytewise). There are no roles or privileges: whoever can
connect can do everything.

`CREATE MATERIALIZED VIEW` creates an ordinary table from the query;
there is no `REFRESH`.

### Not supported

These fail with SQLSTATE `0A000` (`feature_not_supported`) or an
"undefined" error naming the missing object, never silently:

| Feature | Notes |
| --- | --- |
| Functions, procedures, `DO`, PL/pgSQL | no user-defined code |
| Triggers, rules | |
| Expression indexes, partial indexes | indexes cover plain columns |
| GIN, GiST, BRIN, hash indexes | all indexes are B-trees; `USING hash` builds a B-tree |
| Generated columns (`GENERATED ALWAYS AS (expr) STORED`) | identity columns are supported |
| Partitioning, inheritance | |
| `MERGE` | use `INSERT … ON CONFLICT` |
| `GROUPING SETS`, `ROLLUP`, `CUBE` | |
| Ordered-set aggregates (`percentile_cont`, `mode`, `WITHIN GROUP`) | |
| Correlated subqueries above `GROUP BY` | a subquery in the select list or `HAVING` of a grouped query that refers to the outer query's columns |
| SQL cursors (`DECLARE`, `FETCH`) | protocol-level portals work |
| `LISTEN` / `NOTIFY` | |
| Two-phase commit | |
| Domains, composite types, range types | enums are supported |
| Types `money`, `inet`/`cidr`/`macaddr`, geometric types, `xml`, `tsvector`, `bit`, `hstore`, `timetz` | |
| Full-text search, `jsonpath` functions | `->`, `->>`, `#>`, `@>`, `?` and the `jsonb_*` functions are supported |
| `TABLESAMPLE` | |
| Updatable views, `WITH CHECK OPTION` | views are read-only |
| `EXPLAIN (FORMAT JSON)` | text format only |
| Multiple databases, tablespaces | one database per directory |

## Types

| Type | Notes |
| --- | --- |
| `smallint`, `integer`, `bigint`, `serial` types | overflow raises `22003` as in Postgres |
| `numeric(p,s)` | **at most 28 significant digits** (Postgres: 131072). Longer results are rounded; `NaN` and infinity are rejected. |
| `real`, `double precision` | shortest round-trip output, `NaN`, `Infinity` |
| `text`, `varchar(n)`, `char(n)` | length limits enforced. `char(n)` values are returned without trailing padding. `pg_typeof` reports all three as `text`. |
| `bytea` | hex output |
| `boolean` | |
| `date`, `time`, `timestamp`, `timestamptz`, `interval` | microsecond resolution, `infinity`, named time zones (IANA database), `AT TIME ZONE`. Only the `ISO, MDY` date style. |
| `uuid` | `gen_random_uuid()`, `uuid_generate_v4()` |
| `json`, `jsonb` | `jsonb` numbers do not keep trailing zeros (`1.50` is stored as `1.5`) |
| arrays | **one-dimensional only**; lower bound always 1 |
| enums | labels can be appended, not inserted or renamed |
| `oid`, `"char"`, `name`, `regclass`, `regtype` | present as far as catalog queries need them; `regclass` values print as numbers in some places where Postgres prints names |

## Behaviour that differs

- **Collation.** Text is compared and sorted by its UTF-8 bytes, as
  Postgres does with the `C` collation. A Postgres database created with
  a locale such as `en_US.UTF-8` sorts `'a' < 'B'`; squad sorts
  `'B' < 'a'`. `COLLATE` clauses are accepted and ignored. `lower`,
  `upper` and `ILIKE` are Unicode-aware.
- **Serializable** may abort more transactions than Postgres would; see
  [transactions](transactions.md).
- **Constraints** are checked immediately and cannot be deferred.
- **Advisory locks** succeed without locking.
- **Index keys** (the encoded primary key or indexed columns) are
  limited to about 1000 bytes; Postgres allows about 2700. Longer keys
  fail with `54000`.
- **Regular expressions** use Rust's `regex` syntax: no back-references
  or look-around. Invalid patterns fail with `2201B` as in Postgres.
- **Row order without `ORDER BY`** is primary-key order for a single
  table scan and otherwise unspecified, as in Postgres but not identical
  to it.
- **Error messages** follow Postgres' wording for the common cases but
  are not guaranteed to be identical. SQLSTATE codes are.
- **`EXPLAIN`** output has the shape of Postgres' text format without
  cost estimates.
- **Identity columns** created with `GENERATED BY DEFAULT` appear in the
  catalogs like `serial` columns.
- **`current_time`** returns `time` rather than `time with time zone`.

## System catalogs

`pg_catalog` contains `pg_class`, `pg_attribute`, `pg_type`,
`pg_namespace`, `pg_index`, `pg_constraint`, `pg_attrdef`, `pg_enum`,
`pg_sequence(s)`, `pg_tables`, `pg_views`, `pg_indexes`, `pg_database`,
`pg_roles`, `pg_settings`, `pg_am`, `pg_collation`, `pg_extension`,
`pg_stat_activity` and others with content, and about a hundred more
relations with the right columns and no rows, so that queries joining
them still run. `information_schema` has `tables`, `columns`,
`schemata`, `table_constraints`, `key_column_usage`,
`constraint_column_usage`, `referential_constraints`,
`check_constraints`, `views` and `sequences`.

Helper functions include `format_type`, `pg_get_expr`,
`pg_get_indexdef`, `pg_get_constraintdef`, `pg_get_viewdef`,
`pg_get_serial_sequence`, `pg_table_is_visible`, `to_regclass`,
`current_schema(s)`, `current_setting`, `set_config`, `version`,
`pg_typeof`, the `has_*_privilege` family (always true) and the size
functions (which return 0).

## Reporting a difference

A statement that works in PostgreSQL 16 and fails or answers differently
in squad, without being listed on this page, is a bug. The most useful
report is the smallest SQL script that shows it; the regression suite is
made of exactly such scripts (see [testing](testing.md)).
