# Time-series collections

NonoDB stores time-series data in **buckets of columns**, not in one document per
sample. A bucket groups one series (the `metaField` value) and one time window,
keeps every field as a typed column, and encodes each column with the cheapest
of several codecs. The result is a document API (inserts and reads look like
ordinary JSON documents) with a storage footprint closer to a purpose-built
columnar TSDB.

## Why it compresses better

| Layer | MongoDB time-series | TimescaleDB | NonoDB time-series |
|---|---|---|---|
| Grouping | bucket document per series/window | chunk per time range | bucket per series/window |
| Physical layout | row-oriented bucket document | columnar per chunk | columnar per bucket |
| Time column | stored per measurement | delta-of-delta | delta-of-delta + varint |
| Integer counters | stored per measurement | delta / simple | delta, delta-of-delta, or frame-of-reference bit packing |
| Floats | stored per measurement | Gorilla XOR | Gorilla XOR |
| Repeated strings | stored per measurement | dictionary | dictionary + bit-packed indexes |
| Booleans | stored per measurement | bit-packed | bit-packed |
| Sparse fields | nulls per measurement | null bitmap | null bitmap, column omitted when absent |
| Final stage | snappy/zstd per bucket document | optional compression | byte shuffle + zlib per column, only when it wins |
| Meta fields | stored once per bucket | tags | stored once per bucket |

The important difference from MongoDB is that a bucket is **not a JSON document
with arrays**: meta fields are stored once, every field becomes its own column,
and codecs are chosen per column from the actual data. The important difference
from a row store is that a column that never changes (a device id, a status
enum) costs a few bits per row instead of a full string.

The encoder tries every applicable codec for a column and keeps the smallest
payload. It also tries zlib on top of that payload and only accepts it when it
is at least two bytes smaller. `compression` controls the zlib level: `fast` (1),
`balanced` (3, default), `max` (6).

### Measured compression on the bundled benchmark

`nonodb-core/examples/timeseries_compression.rs` generates 10 series, one sample
per second, with three floats, one enum string, one counter, one boolean, and a
nested meta object:

```text
rows                 : 50000
series               : 10
raw JSON bytes       : 9161040
stored columnar bytes: 210374   (4.21 bytes/row, 43.6x smaller than JSON)
row-oriented zlib    : 554825   (11.10 bytes/row)
columnar vs zlib rows : 2.64x smaller
insert wall time     : ~2.0 s (25k inserts/s, release build)
```

Reproduce it:

```bash
ROWS=50000 cargo run --release -p nonodb-core --example timeseries_compression
```

The `row-oriented zlib` line is the shape a document store with whole-row (or
whole-bucket) compression gets: it compresses repetitive JSON well, but every
float and counter still costs its own text. The columnar path is 2.6x smaller on
the same data, and the gap grows with the number of low-cardinality columns and
sampling rate.

## Creating a collection

`profile` must be `timeseries`, and a `timeseries` settings object with
`timeField` is required:

```json
{
  "op": "create_collection",
  "coll": "readings",
  "profile": "timeseries",
  "timeseries": {
    "timeField": "ts",
    "metaField": "device",
    "granularity": "seconds",
    "compression": "balanced"
  }
}
```

The same body works on the HTTP API, the socket API, and the CLI/JSON
playground. Over HTTP:

```bash
curl --unix-socket /run/nonodb.sock http://localhost/v1/write \
  -H 'content-type: application/json' \
  -d '{"op":"create_collection","coll":"readings","profile":"timeseries",
       "timeseries":{"timeField":"ts","metaField":"device"}}'
```

Replicated primaries send the settings with the collection-create operation, so
replicas keep the same bucket layout.

## Settings

| Field | Default | Meaning |
|---|---|---|
| `timeField` | required | Field holding the event time. Accepts dates (`{"$date": ...}`), RFC 3339 strings, or epoch milliseconds. |
| `metaField` | none | Field that groups rows into series. Stored once per bucket and removed from the per-row columns. |
| `granularity` | `seconds` | Bucket width preset: `seconds` = 1 h, `minutes` = 1 day, `hours` = 30 days. |
| `bucketMaxDocs` | `2048` | Seals a bucket when it reaches this many rows (max 16384). |
| `bucketMaxBytes` | `524288` | Seals a bucket when its encoded payload reaches this size (max 4 MiB). |
| `compression` | `balanced` | zlib effort for the final stage: `fast`, `balanced`, `max`. |
| `expireAfterSeconds` | none | Read-time expiry relative to the event time. Expired rows are hidden immediately; sealing drops them from the sealed payload. |

Settings are validated at creation. Invalid values return a normal query error.

## Inserting

Insert documents exactly like any other collection:

```json
{
  "op": "insert",
  "coll": "readings",
  "doc": {
    "ts": {"$date": "2026-01-01T00:00:00Z"},
    "device": {"id": "sensor-7", "site": "plant-1"},
    "temperature": 21.5,
    "status": "ok"
  }
}
```

Behavior:

- the document must contain `timeField`; a missing or non-date value is rejected;
- `_id` is optional. Without it, reads return a deterministic synthetic `_id`
  derived from the series hash and row position. A supplied `_id` is stored in
  its own column and returned as-is. Time-series collections do **not** enforce
  `_id` uniqueness (matching MongoDB time-series semantics);
- rows are appended under a single LMDB key, so an insert is O(1) and does not
  re-encode the bucket;
- a bucket seals (columnar encode) when it reaches `bucketMaxDocs`, when its
  payload exceeds `bucketMaxBytes`, or when the time window advances.

## Reading

`find`, `count`, and `explain` work with the normal filters. Buckets whose time
range cannot overlap the filter are skipped without decoding, and a filter that
pins `metaField` by equality restricts the scan to that series.

```json
{
  "op": "find",
  "coll": "readings",
  "filter": {
    "device.id": "sensor-7",
    "ts": {"$gte": {"$date": "2026-01-01T00:00:00Z"},
           "$lt":  {"$date": "2026-01-01T01:00:00Z"}},
    "temperature": {"$gt": 20}
  },
  "sort": {"ts": -1},
  "projection": {"temperature": 1, "ts": 1},
  "limit": 1000
}
```

Residual filters, sorting, `skip`/`limit`, and projections are applied after
decoding.

### Aggregation and downsampling

Aggregation pipelines run over the decoded documents, so `$group`, `$bucket`,
`$sort`, `$project`, `$count`, and the rest of the in-memory stage set work on
time-series collections. A leading `$match` is reused for bucket and series
pruning, and the whole bucket time range is decoded before the pipeline runs:

```json
{
  "op": "aggregate",
  "coll": "readings",
  "pipeline": [
    {"$match": {"device.id": "sensor-7",
                "ts": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}},
    {"$group": {"_id": "$status", "samples": {"$sum": 1},
                "avg_temp": {"$avg": "$temperature"}}},
    {"$sort": {"_id": 1}}
  ]
}
```

Downsampling to per-minute buckets works the same way (group by a truncated
time expression computed with `$dateToString`/`$subtract` in `$project`).

Stages that need a second collection or an index host (`$lookup`, `$search`,
`$unionWith`) are outside the in-memory runner.

## Supported operations

| Operation | Time-series collection |
|---|---|
| `insert` / `insert_many` / `write_batch` inserts | yes |
| `find`, `find_raw`, `find_by_key`, ordered lookups, `count`, `explain` | yes |
| projections, sorts, `skip`/`limit`, `maxTimeMS`, hints | yes (residual, after decode) |
| TTL: `expireAfterSeconds` on the TS settings | yes (read-time) |
| TTL: collection/document TTL | not applied to buckets |
| document history | not recorded (keeps the format columnar) |
| secondary, text, fuzzy, geo, vector indexes | rejected with a clear error |
| `update`, `find_and_modify`, `delete` | rejected |
| aggregation pipelines (`$match`, `$group`, `$bucket`, `$sort`, ...) | yes, decoded then run in memory |
| retention: delete rows older than a cutoff | `retain_timeseries` |
| SQL, `$search`, `$lookup`, watches/events | not supported |
| drop collection | removes all buckets and series pointers |
| replication, backup, restore, verify | yes (buckets travel with the database) |

Rejections return `invalid query: `<op>` is not supported on time-series
collection `<name>``, so API clients fail loudly instead of silently returning
empty results.

## Retention

`retain_timeseries` deletes every row older than a cutoff. Buckets whose whole
window is older are dropped by key with **no column decode**; only the boundary
bucket is decoded, filtered, and re-encoded. Pending rows are inspected
individually.

```json
{
  "op": "retain_timeseries",
  "coll": "readings",
  "before": {"$date": "2026-01-01T00:00:00Z"}
}
```

Response:

```json
{"ok": true, "result": {"coll": "readings", "before_ms": 1767225600000,
 "report": {"dropped_buckets": 20, "rewritten_buckets": 20,
            "dropped_rows": 0, "remaining_rows": 64000}}}
```

Retention is replicated as `time_series_retain`, so replicas drop the same
ranges. Run it from a scheduler (workers/cron) for a rolling window, or call it
once before an archive.

## Maintenance

Open buckets keep rows as raw JSON between inserts. That is intentional: an
insert must not rewrite a whole bucket. Sealing is automatic, but an operator
can force it (for example before a cold backup or a compression report):

```rust
store.seal_timeseries("readings")?;
```

`Collection::timeseries_stats()` returns buckets, sealed/open counts, series,
rows, raw bytes, sealed bytes, and pending bytes, plus `compression_ratio()`
based on total stored bytes. `Collection::storage_stats()` reports the bucket
database for the GUI and metrics.

## Format

Buckets live in a dedicated LMDB sub-database `tsb:<collection>`:

```text
0x00 | meta_hash(8) | salt(4)                       -> open bucket pointer + row count
0x01 | meta_hash(8) | salt(4) | window(8) | serial(2) -> sealed columnar bucket
0x02 | meta_hash(8) | salt(4) | window(8) | serial(2) | seq(4) -> pending row (raw JSON)
```

`meta_hash` is a stable FNV-1a 64-bit hash of canonical JSON for the series
value; `salt` disambiguates hash collisions and keeps the on-disk key
independent of the Rust hasher version.

A sealed bucket stores a versioned header (row count, window start/end, meta
value) followed by one block per column: field name, kind (`i64`, `f64`, `bool`,
`string`, `json`), codec, optional null bitmap, and a length-prefixed payload.
Every payload may additionally be zlib-deflated (codec flag `0x80`). Unknown
versions or codecs are reported as corruption instead of being guessed.

## Write and read speed

Measured with `nonodb-core/examples/timeseries_bench.rs` on 100k rows, 10
series, 8 columns (release build, single core, local SSD), compared with a
standard collection holding the same documents:

```text
write
insert (time-series)                 100000 ops   21700 ops/s  p50 32.7us  p95 53.2us  p99 61.6us
insert (standard, baseline)          100000 ops   46700 ops/s  p50 20.6us  p95 25.7us  p99 30.1us
seal open buckets                        10 perm  156 ms total, 15.6 ms/bucket
stored: 3.95 bytes/row, 46.4x smaller than raw JSON

read (time-series collection)
find_by_key (point)                      500 ops     371 ops/s  p50 2.54 ms  p99 4.44 ms
find 1 min, 1 series (60 rows)           500 ops     185 ops/s  p50 5.24 ms  p99 8.03 ms
find 1 h, 1 series (3600 rows)           125 ops      38 ops/s  p50 25.6 ms  p99 36.2 ms
count 1 h, 1 series                      125 ops      32 ops/s  p50 31.1 ms  p99 40.4 ms
aggregate $group 1 h, 1 series            50 ops      23 ops/s  p50 43.7 ms  p99 49.4 ms
find match-all (100000 rows)              10 ops     2.0 ops/s  p50 499 ms   p99 525 ms
retention before +1 h (40 buckets)        1 pass   498 ms, 20 dropped + 20 rewritten

baseline (standard collection, one document per row)
find 1 h, 1 series (no index)             10 ops     3.8 ops/s  p50 262 ms   p99 284 ms
find match-all (100000 rows)              10 ops     2.5 ops/s  p50 400 ms   p99 407 ms
```

Reading a one-hour slice is ~10x faster than the document baseline because
whole buckets outside the time range are skipped instead of parsing 100k JSON
documents. Inserts are ~2x slower than the raw document path: each insert is two
LMDB puts (row + open-bucket pointer) instead of one, and series resolution
reads the pointer.

The weak spot is point lookup by `_id`: sealed buckets decode their columns
sequentially, so a synthetic id costs ~2.5 ms instead of the microsecond a
document store needs. Range scans, counts, and downsampling are the intended
access pattern; if you need millions of single-row lookups, keep a small
document or index mapping outside the collection, or query by time range.

Reproduce: `ROWS=100000 READS=500 cargo run --release -p nonodb-core --example timeseries_bench`

## Limitations

- No updates or deletes of individual rows: use `retain_timeseries` for
  time-based retention, `expireAfterSeconds` for read-time expiry, or drop the
  collection.
- No SQL, secondary/text/fuzzy/geo/vector indexes, `$search`, `$lookup`, or
  watches inside the collection.
- Aggregation decodes the full time range into memory before running stages.
- Point lookup by `_id` decodes the containing bucket (see above).
- Open buckets are uncompressed until they seal.
- `bucketMaxDocs` bounds row count, not wall-clock rate; a very high ingest rate
  simply produces more buckets per window (serial counter in the key).
- Field names containing `.` collide with dotted nested paths when flattening.
- Arrays and `{"$date": ...}` objects are stored as single JSON values, not
  expanded into columns.
