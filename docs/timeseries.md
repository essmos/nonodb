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

### Measured on the bundled benchmark

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
decoding. Aggregation pipelines (`aggregate`, `$group`, `$bucket`, `$densify`,
...) are **not** supported on time-series collections yet; use `find` and
aggregate in the client, or a standard collection if you need `$group`.

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
| aggregation, SQL, `$search`, watches/events | not supported |
| drop collection | removes all buckets and series pointers |
| replication, backup, restore, verify | yes (buckets travel with the database) |

Rejections return `invalid query: `<op>` is not supported on time-series
collection `<name>``, so API clients fail loudly instead of silently returning
empty results.

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

## Limitations

- No updates or deletes: delete individual samples by dropping expired windows
  through `expireAfterSeconds`, or drop the collection.
- No aggregation, SQL, indexes, or search inside the collection.
- Open buckets are uncompressed until they seal.
- `bucketMaxDocs` bounds row count, not wall-clock rate; a very high ingest rate
  simply produces more buckets per window (serial counter in the key).
- Field names containing `.` collide with dotted nested paths when flattening.
- Arrays and `{"$date": ...}` objects are stored as single JSON values, not
  expanded into columns.
