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
   │ delta (row store)    │───────►│ delta blocks + segments │
   │ transactional, WAL   │        │ one consistent snapshot │
   └──────────┬───────────┘        └─────────────────────────┘
              │ compaction (background thread, or VACUUM)
              ▼
   ┌──────────────────────────────────────────────────────┐
   │ segments: immutable, compressed, column-oriented     │
   │ in the object store (local directory or S3 bucket)   │
   │ listed by a versioned manifest in the row store      │
   └──────────────────────────────────────────────────────┘
```

- **Writes are ordinary transactions.** Rows inserted into a columnar
  table go to the row store first — its *delta*. They are durable when
  the transaction commits, visible to the next query, and rolled back
  with their transaction, exactly like rows of any table. Each insert
  statement stores its rows as one *block* (up to 65,536 rows per block):
  a single key-value entry that already holds the rows column by column,
  compressed with LZ4. A 1,000-row insert therefore costs one entry in
  the row store and one in the write-ahead log, not a thousand.
- **Compaction** moves delta blocks into a **segment**: an immutable file
  in which every column is stored separately, encoded and compressed, in
  row groups of 65,536 rows with min/max statistics and bloom filters. A
  compaction is one transaction that deletes the blocks and adds the
  segment to the table's manifest, so every query sees each row exactly
  once, before, during and after. Compaction runs on a **background
  thread** (one per database), see [below](#background-compaction);
  inserting sessions never wait for it. `VACUUM [table]` compacts the
  whole delta and merges small segments right away.
- **Bulk inserts skip the delta.** A single `INSERT` of 50,000 rows or
  more writes segments directly. They become visible when the
  transaction commits.
- **Segments live in the object store** — a directory `objects/` next to
  the row store, or the configured bucket (see
  [cloud storage](cloud-storage.md)). Queries read them in ranges: the
  footer first, then only the column chunks of the row groups they need,
  through an in-memory cache (see [below](#reading-segments)).
- **Queries read both parts** under one snapshot.

### Background compaction

The compactor thread compacts a table once its delta holds
`compact_rows` rows (default 65,536) or `compact_blocks` blocks
(default 512), or once its oldest rows are `compact_age` old (default
10 s). One run moves at most 262,144 rows (four row groups); the rest
follows in the next run. Afterwards it merges small segments: when at least 8
segments have fewer than 2 million rows each, up to 32 of them, at most
4 million rows and `merge_max_bytes` (default 256 MiB) of segment data
in total, are merged into one. The merge streams: it holds one decoded
row group per input and the encoded output, not the inputs.

Compaction never blocks inserts or queries. It takes the table's shared
lock (DDL on the table waits for it, and a compaction waits at most
200 ms for DDL before trying again later) and the table's segment lock,
which `DELETE` and bulk inserts also take. Its commit writes one
tombstone per block and the new manifest, so it holds the global commit
lock only briefly; the tombstones are removed afterwards in the table's
key range.

When the last `Database` handle and connection are dropped, the
compactor is stopped first: a compaction in progress finishes or rolls
back; rows not yet compacted stay in the delta and are compacted after
the next open. Nothing is lost either way.

The default threshold keeps what every query pays for the delta small.
Reading a delta block costs a key-value read and, the first time, LZ4
decoding; decoded blocks are cached. Measured with
`cargo run --release -p squad-bench --bin ingest -- --delta-cost` on
events with a `jsonb` column (4 vCPU, shared machine):

| Delta rows | Count by event, one project | Field filter on `jsonb` |
| ---: | ---: | ---: |
| 16,384 | 0.1 ms | 3.3 ms |
| 65,536 | 0.7 ms | 19 ms |
| 131,072 | 1.2 ms | 26 ms |
| 262,144 | 3.9 ms | 39 ms |

The same rows in segments take 0.07–3.3 ms and 4.6–52 ms: the delta
costs about as much as segments, because both are read in columnar
form; the `jsonb` filter is dominated by reading the field out of every
document (about 0.3 µs per row) in either form. Raise `compact_rows` for
fewer, larger compactions; lower it for fewer rows in the delta right
after bursts of inserts.

Settings are fields of `Config::columnar` (`ColumnarConfig`, see
[embedding](embedding.md#opening-a-database)); with
`background_compaction: false` the delta is compacted only by `VACUUM`.

### Reading segments

A query opens a segment from the size recorded in the manifest: it
fetches the last 64 KiB (footer and row group directory) with one
ranged GET, then only the column chunks it needs. Before anything is
fetched, the per-segment statistics in the manifest rule out segments
that cannot match the filter (min/max per column, null counts), and the
row group statistics and bloom filters rule out row groups. The chunks
of one row group are requested together: neighbouring ranges in one
request, separate requests in parallel.

Fetched ranges are kept in an LRU cache bounded by `cache_bytes`
(default 512 MiB; a quarter of it more for decoded delta blocks).
Concurrent misses of the same range are fetched once. There is no
local-disk cache tier for segments in a bucket: after a restart, the
first queries fetch what they need again.

## What runs fast

Aggregations over one columnar table run in the column engine:
vectorized over encoded columns, in parallel on all cores, skipping row
groups and whole segments whose statistics rule them out.

| Part of the query | Fast path when |
| --- | --- |
| `WHERE` | comparisons of a column with a constant or parameter (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN (…)`, `BETWEEN`, `IS [NOT] NULL`, `LIKE`/`ILIKE` without escapes, boolean columns; on `jsonb` columns `col ->> 'key' = text`, `<>`, `IN (…)`, `IS [NOT] NULL`; combined with `AND`/`OR`/`NOT` |
| `GROUP BY` | columns, `date_trunc(unit, timestamp_column)` for second … year, and `jsonb_column ->> 'key'` |
| Aggregates | `count(*)`, `count(x)`, `count(DISTINCT x)`, `sum`, `min`, `max`, `avg`, `approx_count_distinct(x)`, `approx_percentile(x, p)` |

`HAVING`, `ORDER BY`, `LIMIT` and expressions over the aggregates are
applied to the result as usual. `EXPLAIN` shows `Columnar Aggregate` /
`Columnar HashAggregate` when the fast path is planned.

Anything else — expressions inside aggregates, `FILTER`, joins, window
functions, other `jsonb` operators, subqueries — still works: such queries read
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

### `jsonb` properties

Event payloads (PostHog `properties`, Sentry tags) are usually `jsonb`.
Columnar tables store `jsonb` as its text form and, per row group and
column, a bloom filter over the documents' top-level keys and key/value
pairs. A filter such as

```sql
SELECT count(*) FROM events
WHERE project_id = 42 AND properties ->> '$browser' = 'Firefox';
```

runs in the column engine (the field is read from the text without
building a document) and skips row groups whose bloom filter says no
document has `$browser` = `Firefox`. `->> 'key' IS NOT NULL` skips row
groups where no document has the key. `GROUP BY properties ->> 'plan'`
groups in the column engine too. Only top-level keys and the `->>`
operator with a constant key are handled this way; `->`, `#>`, `@>` and
nested paths go through the row engine. Segments written before this
version have no field bloom filters; their filters still run in the
column engine, without skipping.

A field that almost every query filters or groups by is still best kept
in a column of its own.

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
| `INSERT`, `COPY FROM` | supported, transactional; one delta block per statement |
| `Connection::append` | inserts rows given as values without SQL text (see [embedding](embedding.md)); the fastest way in |
| `DELETE … WHERE` | supported. Matching delta rows are deleted; every segment containing matching rows is rewritten without them. Deleting by time range or tenant is cheap when it matches the sort order; scattered deletes rewrite many segments. |
| `TRUNCATE`, `DROP TABLE` | supported; segment objects are deleted when the transaction commits |
| `UPDATE` | not supported (`0A000`). Model changes as new events, or keep mutable attributes in an ordinary table and join. |
| `ALTER TABLE … ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN … SET/DROP DEFAULT`, `DROP NOT NULL` | supported without rewriting segments, see below |
| other `ALTER TABLE` actions, indexes, primary key, unique and foreign key constraints | not supported (`0A000`). `NOT NULL`, `CHECK` and defaults are enforced on insert. |
| `DELETE … RETURNING`, `DELETE … USING`, `SELECT … FOR UPDATE/SHARE` | not supported (`0A000`) |

`DELETE` serializes with compaction on the table's segment lock. In
Read Committed it then reads the table as of that moment; in Repeatable
Read and Serializable it fails with `40001` if a compaction or another
delete changed the table since the transaction's snapshot — retry it.

### Schema changes

Every column of a columnar table has a stable id. Segments and delta
blocks record the ids of the columns they hold, so schema changes touch
only the catalog:

- `ADD COLUMN` (optionally with a constant default, or `NOT NULL` with a
  default): rows written before read the default as of the moment the
  column was added, or NULL. Later `SET DEFAULT` changes affect new rows
  only, as in PostgreSQL. A volatile default (`random()`,
  `clock_timestamp()`, serial or identity columns) and constraints on
  the new column are not supported (`0A000`). `NOT NULL` without a
  default fails with `23502` if the table has rows.
- `DROP COLUMN`: the column is no longer read; its data stays in
  existing segments until they are rewritten by a merge or a delete. A
  dropped `order_by` column is removed from the sort order of new
  segments. Dropping and adding a column of the same name gives a new,
  empty column.
- `RENAME COLUMN`, `RENAME TO`: names only.

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
| `jsonb` | text form plus field bloom filters | `->> 'key'` filters and grouping (see above) |
| `numeric` without precision, `json`, arrays, `interval` | text form | stored and returned; filtered and aggregated by the row engine only |

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
  a segment object that no manifest names. It is never read. A
  transaction that rolls back deletes the segments it wrote at once;
  `VACUUM` deletes other unreferenced segment objects (left by crashes,
  failed commits or dropped tables) once they are older than
  `orphan_grace` (default one hour), so that a segment another writer is
  about to commit is never removed. `VACUUM table` looks at that table's
  objects, plain `VACUUM` at all of them. Segment ids written by earlier
  versions carry no time and are treated as old.

## Limits

- Old segment objects are deleted when the transaction that replaced
  them commits. A query that started earlier and has not yet read that
  segment fails with an error instead of reading a stale copy; retry it.
  (Segments already loaded into the cache keep working.)
- Segment data is cached in memory only; the first query after a
  restart fetches the column chunks it needs from the object store
  again (see [benchmarks](benchmarks.md#analytics) for cold-cache
  timings against Garage).
- A compaction of the delta holds the rows it moves (at most 262,144)
  in memory while encoding; a bulk insert holds its rows, in
  chunks of one million per segment. A merge holds its encoded output
  (bounded by `merge_max_bytes`).
- The delta has no back-pressure: if inserts outpace compaction for a
  long time, the delta grows and queries slow down accordingly.
- Exact `count(DISTINCT)` and group-by with very many groups are bounded
  by memory; nothing spills to disk.
- One compaction or delete per table at a time.
- Time-zone-aware bucketing other than UTC, `UPDATE`, column type
  changes and secondary indexes are not implemented (see
  [roadmap](roadmap.md)).
- Manifests are written in a new format (`SQMANI02`, with column ids)
  and segments of `jsonb` columns with a new column type; earlier
  versions of squad cannot read databases written by this one. This
  version reads the earlier formats, including delta rows stored one
  per key.

## Measurements

See [benchmarks](benchmarks.md#analytics). In short, on two million
events through SQL, aggregations take 0.5–7 ms on the columnar table
against 2.6–4.6 s on the row table, at a quarter of the storage.
