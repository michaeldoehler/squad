# Analytics: columnar tables

squad has two table formats. Ordinary tables are row-oriented and built
for transactions: point lookups, small updates, constraints, indexes.
**Columnar tables** are built for the other half of an application's
data — events, logs, measurements: rows that are appended in large
numbers, rarely changed, and read by queries that filter on time and a
few dimensions and aggregate over millions of rows.

Both live in the same database, are queried with the same SQL, can be
joined with each other, and take part in the same transactions.

```sql
CREATE TABLE events (
    ts          timestamp NOT NULL,
    project_id  int       NOT NULL,
    event       text      NOT NULL,
    user_id     bigint,
    path        text,
    duration_ms int,
    revenue     numeric(12,2),
    properties  jsonb
) USING columnar WITH (order_by = 'project_id, ts');

INSERT INTO events VALUES (now(), 42, 'pageview', 7, '/pricing', 133, NULL, '{"ref": "ad"}');

SELECT date_trunc('day', ts) AS day,
       count(*)                         AS events,
       approx_count_distinct(user_id)   AS users,
       approx_percentile(duration_ms, 0.95) AS p95
FROM events
WHERE project_id = 42 AND ts >= now() - interval '30 days'
GROUP BY 1 ORDER BY 1;
```

## How it works

```text
            INSERT                          SELECT
              │                               │
              ▼                               ▼
   ┌──────────────────────┐        ┌─────────────────────────┐
   │ delta (row store)    │───────►│  delta rows + segments  │
   │ transactional, WAL   │        │  one consistent snapshot│
   └──────────┬───────────┘        └─────────────────────────┘
              │ compaction (automatic, or VACUUM)
              ▼
   ┌──────────────────────────────────────────────────────┐
   │ segments: immutable, compressed, column-oriented     │
   │ in the object store (local directory or S3 bucket)   │
   │ listed by a versioned manifest in the row store      │
   └──────────────────────────────────────────────────────┘
```

- **Writes are ordinary transactions.** A row inserted into a columnar
  table goes to the row store first — its *delta*. It is durable when the
  transaction commits, visible to the next query, and rolled back with
  its transaction, exactly like a row of any table.
- **Compaction** moves the delta into a **segment**: an immutable file in
  which every column is stored separately, encoded and compressed, in
  row groups of 65,536 rows with min/max statistics and bloom filters. A
  compaction is one transaction that deletes the delta rows and adds the
  segment to the table's manifest, so every query sees each row exactly
  once, before, during and after. It runs automatically once a table's
  delta has collected 100,000 rows, and on `VACUUM [table]`, which also
  merges small segments.
- **Bulk inserts skip the delta.** A single `INSERT … SELECT` of 50,000
  rows or more writes segments directly. They become visible when the
  transaction commits.
- **Segments live in the object store** — a directory `objects/` next to
  the row store, or the configured bucket (see
  [cloud storage](cloud-storage.md)) — and are cached in memory once
  read.
- **Queries read both parts** under one snapshot.

## What runs fast

Aggregations over one columnar table run in the column engine:
vectorized over encoded columns, in parallel on all cores, skipping row
groups and whole segments whose statistics rule them out.

| Part of the query | Fast path when |
| --- | --- |
| `WHERE` | comparisons of a column with a constant or parameter (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN (…)`, `BETWEEN`, `IS [NOT] NULL`, `LIKE`/`ILIKE` without escapes, boolean columns, combined with `AND`/`OR`/`NOT` |
| `GROUP BY` | columns, and `date_trunc(unit, timestamp_column)` for second … year |
| Aggregates | `count(*)`, `count(x)`, `count(DISTINCT x)`, `sum`, `min`, `max`, `avg`, `approx_count_distinct(x)`, `approx_percentile(x, p)` |

`HAVING`, `ORDER BY`, `LIMIT` and expressions over the aggregates are
applied to the result as usual. `EXPLAIN` shows `Columnar Aggregate` /
`Columnar HashAggregate` when the fast path is planned.

Anything else — expressions inside aggregates, `FILTER`, joins, window
functions, `jsonb` operators, subqueries — still works: such queries read
the segments through the ordinary executor. Filters are still pushed
down (statistics, bloom filters), so a selective query such as "all
events of one user" stays fast; a non-selective one processes rows at the
row engine's speed. Rewriting a hot query into the shape above is the
way to make it fast: for example group by `date_trunc('day', ts)` rather
than `ts::date`, and move arithmetic out of the aggregate
(`sum(x) * 2`, not `sum(x * 2)`).

`date_trunc` on `timestamptz` columns uses the fast path only while the
session time zone is UTC; otherwise day boundaries depend on the zone
and the query takes the general path.

### Approximate aggregates

- `approx_count_distinct(x)` estimates the number of distinct values
  with a HyperLogLog sketch (typical error 1–2 %) in constant memory per
  group. Exact `count(DISTINCT x)` is available too and keeps every
  value in memory.
- `approx_percentile(x, p)` estimates the value at percentile `p`
  (0 … 1) with a mergeable quantile sketch.

On ordinary tables both functions exist as well and compute exact
results.

## Choosing `order_by`

Segments are sorted by the `order_by` columns, and statistics can only
skip data that is clustered. Put the column every query filters on
first — in a multi-tenant product the tenant or project id — and the
timestamp second: `order_by = 'project_id, ts'`. Queries for one
project and a time range then touch a small part of each segment.

High-cardinality columns that are looked up by equality (user id,
session id, trace id) do not need to be in `order_by`: each row group has
a bloom filter per text, bytes and integer column.

## Changing and removing data

| Statement | Behaviour |
| --- | --- |
| `INSERT` | supported, transactional |
| `DELETE … WHERE` | supported. Matching delta rows are deleted; every segment containing matching rows is rewritten without them. Deleting by time range or tenant is cheap when it matches the sort order; scattered deletes rewrite many segments. |
| `TRUNCATE`, `DROP TABLE` | supported; segment objects are deleted when the transaction commits |
| `UPDATE` | not supported (`0A000`). Model changes as new events, or keep mutable attributes in an ordinary table and join. |
| `ALTER TABLE`, indexes, primary key, unique and foreign key constraints | not supported. `NOT NULL`, `CHECK` and defaults are enforced on insert. |
| `DELETE … RETURNING`, `DELETE … USING` | not supported |

Retention is a `DELETE` by time:

```sql
DELETE FROM events WHERE ts < now() - interval '90 days';
```

## Column types

| SQL type | Stored as | Fast path |
| --- | --- | --- |
| `smallint`, `integer`, `bigint` | 64-bit integers, bit-packed / delta / run-length encoded | yes |
| `date`, `time`, `timestamp`, `timestamptz` | integers (days or microseconds) | yes |
| `real`, `double precision` | 64-bit floats | yes |
| `numeric(p,s)` | scaled 128-bit integers | yes |
| `boolean` | bits | yes |
| `text`, `varchar`, enums | dictionary or plain, LZ4-compressed | yes |
| `uuid`, `bytea` | bytes | filters, `count`, `min`/`max` |
| `numeric` without precision, `jsonb`, `json`, arrays, `interval` | text form | stored and returned; filtered and aggregated by the row engine only |

Declare `numeric` columns with precision and scale, and keep attributes
you filter or group by in their own columns rather than inside `jsonb`.

## Guarantees

- **Atomicity and isolation** are those of the row store: an insert, a
  delete or a compaction is visible entirely or not at all, and every
  query reads one snapshot across delta and segments.
- **Durability**: an inserted row is as durable as any committed row
  (write-ahead log). A segment object is written and synced before the
  manifest that names it commits.
- **Crash safety**: a crash during compaction or a bulk insert can leave
  a segment object that no manifest names. It is never read; such
  leftovers are not yet cleaned up automatically.

## Limits

- Old segment objects are deleted when the transaction that replaced
  them commits. A query that started earlier and has not yet read that
  segment fails with an error instead of reading a stale copy; retry it.
  (Segments already loaded into the cache keep working.)
- All segments a query needs are loaded into memory whole (the cache
  holds up to 2 GiB). There is no partial fetching of column chunks from
  remote storage yet, so the first query after a restart pays for the
  download.
- A compaction or bulk insert holds its rows in memory while encoding;
  segments are capped at one million rows per bulk chunk.
- Exact `count(DISTINCT)` and group-by with very many groups are bounded
  by memory; nothing spills to disk.
- One compaction or delete per table at a time.
- Time-zone-aware bucketing other than UTC, `UPDATE`, schema changes and
  secondary indexes are not implemented (see [roadmap](roadmap.md)).

## Measurements

See [benchmarks](benchmarks.md#analytics). In short, on two million
events through SQL, aggregations take 0.5–7 ms on the columnar table
against 2.6–4.6 s on the row table, at a quarter of the storage.
