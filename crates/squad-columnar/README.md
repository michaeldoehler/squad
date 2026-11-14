# squad-columnar

Column-oriented storage and vectorized aggregation for squad.

squad's row store is built for transactions: find a row, change a row.
Analytical queries — events per day, unique users per country, the 95th
percentile of a duration per endpoint — read a few columns of very many
rows. This crate stores such data column by column in compressed,
immutable segments and aggregates it in tight loops over arrays, on all
cores. It has no dependency on the rest of squad: it works on byte
sources and returns batches.

Measured numbers are in [BENCHMARKS.md](BENCHMARKS.md).

## Concepts

```text
 rows ──► SegmentWriter ──► segment bytes ──► file / object store
                                                   │  ChunkSource
 query ──► TableSource { segments, delta } ◄───────┘
              │
              ├─ scan(ScanRequest)            → batches of rows
              └─ aggregate(AggregateRequest)  → one batch of groups
```

- A **segment** is an immutable file holding up to a few million rows.
  Rows are divided into **row groups** (65 536 rows by default); each
  column of each row group is stored as one separately encoded,
  compressed and checksummed **chunk**.
- A **table** is a set of segments plus a **delta**: in-memory batches of
  rows that have not been written to a segment yet. Queries see both.
- A **manifest** lists a table's segments with their statistics.

## Example

```rust
use std::sync::Arc;
use squad_columnar::*;

let schema = Arc::new(Schema::new(vec![
    Field::new("ts", ColumnType::Int64, false),        // microseconds
    Field::new("project", ColumnType::Int64, false),
    Field::new("path", ColumnType::Utf8, false),
    Field::new("user_id", ColumnType::Int64, false),
    Field::new("duration_ms", ColumnType::Int64, true),
]));

// Write a segment, clustered by project and time.
let mut writer = SegmentWriter::new(
    schema.clone(),
    WriterOptions { sort_key: vec![1, 0], ..Default::default() },
);
writer.write_batch(&Batch::from_rows(schema.clone(), &rows)?)?;
let bytes: Vec<u8> = writer.finish()?;       // store anywhere

// Open it through any ChunkSource and query.
let mut table = TableSource::new(schema);
table.segments.push(Arc::new(Segment::open(Arc::new(bytes))?));

// Page views and unique users per day for project 7.
let per_day = aggregate(&table, &AggregateRequest {
    predicate: Some(Predicate::Cmp { column: 1, op: CmpOp::Eq, value: Datum::Int(7) }),
    group_by: vec![GroupKey::TimeBucket {
        column: 0,
        unit: TimeUnit::Day,
        utc_offset_micros: 0,
        epoch_unix_micros: 0,
    }],
    aggregates: vec![Aggregate::CountStar, Aggregate::ApproxCountDistinct(3)],
    order_by: vec![OrderBy { column: 0, descending: false }],
    parallelism: 4,
    ..Default::default()
})?;
```

## Data model

| `ColumnType` | Values | `Datum` |
| --- | --- | --- |
| `Bool` | booleans | `Datum::Bool` |
| `Int64` | integers of any width, dates, timestamps in microseconds, enum ordinals | `Datum::Int` |
| `Float64` | floats; NaN sorts above everything and equals itself, as in PostgreSQL | `Datum::Float` |
| `Decimal { scale }` | `i128` unscaled value | `Datum::Decimal` |
| `Bytes`, `Utf8` | byte strings, text, UUIDs; compared bytewise | `Datum::Bytes` |
| `Json` | JSON documents as text, stored like `Utf8`; bloom filters cover top-level keys and key/value pairs | `Datum::Bytes` |

Every column may contain NULLs. A `Batch` is a schema plus one `Column`
per field; `Batch::from_rows` / `to_rows` convert from and to rows of
`Datum`, `ColumnBuilder` builds a column value by value, and columns can
be constructed directly from typed vectors (`ColumnData`).

## Segment format

All integers are little-endian.

```text
"SQCOL001"                                   8 bytes
chunk …                                      column chunks and bloom filters
footer
footer_len:u32  footer_crc:u32               CRC32C of the footer
"SQCOL001"                                   8 bytes
```

**Column chunk**

```text
crc:u32            CRC32C of everything that follows
compression:u8     0 none, 1 LZ4 block
raw_len:u32        length of the payload after decompression
data               the (compressed) payload
```

**Payload**

```text
has_nulls:u8  [validity bitmap, 1 bit per row]  body
```

| Type | Body |
| --- | --- |
| `Int64` | `tag:u8`, then: `0` plain, 8 bytes per value · `1` frame of reference: `min:i64 width:u8`, values minus `min` bit-packed · `2` delta: `first:i64 min_delta:i64 width:u8`, deltas minus `min_delta` bit-packed · `3` run length: `runs:u32`, then `value:i64 count:u32` per run |
| `Bool` | bitmap |
| `Float64` | 8 bytes per value |
| `Decimal` | `tag:u8`: `0` 16 bytes per value · `1` all values fit 64 bits: integer body |
| `Bytes`, `Utf8` | `tag:u8`: `0` plain: lengths (integer body), `data_len:u32`, data · `1` dictionary: `entries:u32`, the entries in plain form, `width:u8`, bit-packed codes |

Bit-packed sequences are followed by eight zero bytes so that a reader
can load unaligned 64-bit words.

**Bloom filter chunk:** `crc:u32 hashes:u8 bits`. Built for integer and
byte columns with ten bits per distinct value and seven probes (about 1 %
false positives); for `Json` columns over the hashes of top-level keys
and key/value pairs instead.

**Footer**

```text
version:u32 (1)
columns:u32, per column: name (u32 length + bytes), type:u8, scale:u8, nullable:u8
    (type: 0 Bool, 1 Int64, 2 Float64, 3 Decimal, 4 Bytes, 5 Utf8, 6 Json)
sort_key:u32, per key: column:u32
rows:u64
row_groups:u32, per row group:
    rows:u32
    per column: offset:u64 len:u32  bloom_offset:u64 bloom_len:u32
                min:value max:value null_count:u64 distinct:u64
```

`value` is a tag byte (`0` NULL, `1` bool, `2` int, `3` float, `4`
decimal, `5` bytes) followed by the value.

A reader fetches the last 64 KiB of the file, which normally contains
the whole footer, and afterwards only the chunks it needs: one read per
column chunk (and bloom filter) of the row groups that survive pruning,
announced beforehand through `ChunkSource::prefetch` so that a remote
source can combine and parallelize the requests.

### Choosing encodings

The writer computes the encoded size of every candidate for each chunk
and takes the smallest:

| Data | Typical choice |
| --- | --- |
| timestamps of sorted events, sequences | delta |
| ids and counters in a limited range | frame of reference |
| a sort-key column, constant columns | run length |
| low-cardinality text (event names, countries, browsers) | dictionary |
| high-cardinality text (session ids, JSON) | plain, usually LZ4 |

LZ4 is applied on top when it saves at least a tenth.

### Statistics

Per column chunk the footer records minimum, maximum, NULL count and the
number of distinct values; per segment these are combined. Integer and
byte columns get a bloom filter over their values (about 1 % false
positives); `Json` columns one over the documents' top-level keys and
`key`/`value text` pairs, which `JsonIn` and `JsonIsNull` consult. Bounds of
long byte strings are shortened to 64 bytes (the upper bound is rounded
up), so they are bounds rather than actual values.

Zone maps only help for columns the data is clustered by. Set
`WriterOptions::sort_key` to the columns most queries filter on — for
multi-tenant event data typically `(tenant, timestamp)`.

### Integrity

Every chunk and the footer carry a CRC32C. A damaged segment yields
`Error::Corrupt`; the test suite flips bits, truncates and splices
segments thousands of times and requires an error or the original data,
never a panic or different data. The leading magic is not verified.

## Execution

A query is split into units — one per row group, one per delta batch —
and each unit goes through the same steps:

1. **Prune.** Whole segments, then row groups, are skipped when their
   zone maps show the predicate cannot be true. For equality and `IN`
   conditions the bloom filter of the row group is consulted.
2. **Filter.** The predicate's columns are decoded and evaluated a column
   at a time into one truth value per row (false / unknown / true, so
   that `AND` is `min`, `OR` is `max` and `NOT` is `2 - x`). On
   dictionary columns comparisons and `LIKE` are evaluated once per
   dictionary entry.
3. **Decode** the remaining columns, only if any row qualified.
4. **Aggregate** (or return the selected rows, for a scan).

### Aggregation

Group keys are mapped to dense ids — per key column, then per key
combination — so that aggregate states are plain arrays indexed by group.
Dictionary columns are mapped once per dictionary entry; integer keys
remember the previous row's lookup, which makes time buckets over sorted
data nearly free.

Worker threads take units from a shared counter and build their own
partial result; partials are merged at the end. Every aggregate state is
mergeable, which is also what distributed execution needs.

| Aggregate | Result type | Notes |
| --- | --- | --- |
| `CountStar`, `Count(col)` | Int64 | |
| `Sum(col)` | Decimal (scale 0) for Int64, Float64, or Decimal | integer sums cannot overflow; decimal overflow is an error |
| `Avg(col)` | Float64 | |
| `Min(col)`, `Max(col)` | the column's type | |
| `CountDistinct(col)` | Int64 | exact; byte strings by 128-bit hash |
| `ApproxCountDistinct(col)` | Int64 | HyperLogLog, 4096 registers, standard error 1.6 %; exact up to 256 values |
| `ApproxPercentile { column, percentile }` | Float64 | DDSketch, relative error ≤ 1 %; percentiles of the same column share one sketch |

`GroupKey::TimeBucket` truncates microsecond timestamps to second,
minute, hour, day, ISO week, month, quarter or year, with a fixed UTC
offset for the period boundaries and a configurable epoch
(`epoch_unix_micros`: 0 for Unix time, 946 684 800 000 000 for
PostgreSQL-style timestamps counted from 2000-01-01).

`having` filters the result, `order_by` and `limit` give top-N. Result
columns are the group keys, then the aggregates. Without `order_by` the
row order is unspecified.

**Memory.** A partial result holds one state per group: a few bytes for
counts, sums and extremes; every distinct value for exact distinct
counts; 4 KiB per group for an approximate distinct count once it has
seen more than 256 values; a few KiB for a quantile sketch. There is no
spilling to disk: a query with hundreds of millions of groups will use
memory in proportion.

### Predicates

`Cmp` (`=`, `<>`, `<`, `<=`, `>`, `>=`), `In`, `Between`, `IsNull`,
`Like` (`%`, `_`, `\` escape, optionally case-insensitive),
`BoolColumn`, `JsonIn` / `JsonIsNull` (the text of a top-level field of
a JSON column, as PostgreSQL's `->>` returns it), `And`, `Or`, `Not`,
`Const`, with SQL three-valued logic. `GroupKey::JsonField` groups by
such a field.
Constants must have the column's type (`Datum::Int` or `Datum::Float`
for numeric columns, `Datum::Decimal` at the column's scale).

## Managing a table

```rust
let mut manifest = Manifest::new((*schema).clone(), vec![1, 0]);
manifest.segments.push(SegmentMeta::describe("seg-000017", &segment));
let bytes = manifest.to_bytes();                     // checksummed
let manifest = Manifest::from_bytes(&bytes)?;

// Retention: segments whose newest row is older than the cutoff.
let drop = manifest.expired(0, &Datum::Int(cutoff_micros));

// Compaction: merge small segments into one sorted segment, streaming
// (one decoded row group per input in memory).
let small = manifest.merge_candidates(4_000_000, 256 << 20, 32, 4);
let merged: Vec<u8> = merge(&segments_to_merge, WriterOptions { sort_key: vec![1, 0], ..Default::default() })?;
```

Manifests record caller-defined column ids per segment
(`SegmentMeta::with_columns`, format `SQMANI02`; `SQMANI01` is still
read). `Segment::project` reads a segment in another column layout,
filling columns it does not store with a constant, so that a table can
add and drop columns without rewriting its segments. `encode_batch` and
`decode_batch` serialize a batch, for rows kept outside segments.

## Limits

- Segments are immutable. Updates and deletes of individual rows have to
  be handled above this crate (for example with a delete mask applied as
  a predicate, or by rewriting a segment).
- `SegmentWriter` and `compact` hold their whole input in memory and
  sort it there; size segments accordingly (millions of rows, not
  billions). `merge` streams, but holds its encoded output in memory;
  `SegmentStreamWriter` writes pre-sorted input with one row group in
  memory.
- Encoding runs on one thread per writer.
- Arrays and nested types are not supported; store JSON as text (`Json`
  for field filters on top-level keys).
- Decoded chunks are not cached between queries; a `ChunkSource` for
  remote storage should cache, and can fetch ahead what
  `ChunkSource::prefetch` announces.
- Run-length and bit-packed integer chunks are decoded before filters
  are evaluated; only dictionary columns are filtered in encoded form.
- Group keys: any number, any column type; time buckets need Int64.
- `Avg` returns a float; compute `Sum` and `Count` for an exact decimal
  average.
- Only fixed UTC offsets for time buckets, no daylight-saving rules.
