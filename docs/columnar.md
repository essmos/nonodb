# Columnar collections

`profile: "columnar"` is a general-purpose bucket-columnar storage profile.
Unlike [time-series collections](timeseries.md), it is not organized by time:
documents are grouped into immutable buckets by `_id` hash, and the profile
keeps the full standard collection contract.

Use it when documents share a stable field shape and are mostly inserted and
read in batches, while point updates and deletes remain part of the workload.

## When to use it

Choose `columnar` when:

- documents in a collection have a predictable set of fields;
- scans, aggregations, and large inserts dominate writes;
- storage footprint matters more than single-row insert latency;
- you still need standard indexes, TTL, history, search, graph, SQL, and
  transactions without a separate time-series data model.

Prefer `standard` for small or very sparse documents, heavy single-document
updates, or schemas that change on every document. Prefer `timeseries` when the
natural bucket key is a time window and a series/meta field.

## Creating a collection

```json
{
  "op": "create_collection",
  "coll": "events",
  "profile": "columnar",
  "columnar": {
    "bucket_max_docs": 512,
    "bucket_max_bytes": 524288,
    "compression": "balanced"
  },
  "ttl_seconds": 86400
}
```

All `columnar` settings are optional. The defaults are 512 rows per bucket,
512 KiB per bucket, and `balanced` compression.

| Setting | Meaning |
|---|---|
| `bucket_max_docs` | Row limit before the open bucket is sealed. |
| `bucket_max_bytes` | Byte soft limit for the open bucket. |
| `compression` | `fast`, `balanced`, or `max` column codecs. |

Settings can be changed on a live collection. New writes use the new limits;
existing sealed buckets remain readable and are re-encoded by compaction.

```json
{"op": "get_collection_columnar", "coll": "events"}
```

```json
{
  "ok": true,
  "result": {
    "coll": "events",
    "profile": "columnar",
    "columnar": {
      "bucket_max_docs": 512,
      "bucket_max_bytes": 524288,
      "compression": "balanced"
    },
    "stats": {
      "buckets": 4,
      "sealed_buckets": 3,
      "open_buckets": 1,
      "rows": 1700
    }
  }
}
```

```json
{
  "op": "set_collection_columnar",
  "coll": "events",
  "settings": {"bucket_max_docs": 1024, "compression": "max"}
}
```

The daemon accepts either `settings` or `columnar` as the settings payload key,
matching the time-series aliases.

## Storage format

- Each bucket is an immutable ordered column set. Inserting or updating a row
  rewrites only the open bucket; sealed buckets never change in place.
- `_id` uses a deterministic hash locator plus a collision chain, so point
  reads, updates, and deletes do not scan a bucket.
- Updates append a small row overlay in the collection sub-database. Deletes
  append a tombstone. Sealing and compaction fold overlays into fresh buckets.
- Users choose `bucket_max_docs` and `bucket_max_bytes`; the hard format limit
  is 4 MiB per encoded bucket. A row that cannot fit is rejected without
  changing the collection.
- The on-disk magic is versioned (`NCB1`, format version 2). Version 2 uses
  32-row column blocks for cheaper point reads; version 1 remains readable and
  is upgraded by compaction. Readers reject unknown versions instead of
  guessing.

The shared column codec (`bucket_codec`) is the same codec used by time-series
buckets: dictionary, prefix/front coding, delta, delta-of-delta, XOR,
frame-of-reference, boolean bitmap, raw, and deflate paths selected per column.
Columnar indexes without `include` store the id once in a compact key trailer
instead of duplicating it in the index value.

## Documents and API parity

Columnar collections expose the same operations as `standard`:

- `insert`, `insert_many`, `update`, `upsert`, `find_and_modify`, `delete`;
- `find`, `find_one`, `count`, `lookup`, `explain`;
- `aggregate`, including `$match`, `$group`, `$sort`, `$unwind`, `$lookup`,
  `$facet`, `$unionWith`, `$out`, and `$merge`;
- SQL `SELECT`, `INSERT`, `UPDATE`, and `DELETE`;
- secondary, compound, unique, sparse, partial, include, geo, text, fuzzy, and
  vector indexes;
- document TTL, revision history, collection events, subscriptions, live
  watches, webhooks, and outbox streams;
- graph definitions and traversal;
- backup, restore, replication, compaction, and storage accounting.

Filters, projections, sorting, numeric comparison, BSON scalar handling, date
semantics, and error codes match `standard`. The acceptance suite is a
differential harness that runs the same operations against both profiles.

Point lookup by `_id` uses the locator. Unique-index lookup uses the index range
and then resolves the candidate through the locator. `explain` reports
`profile: "columnar"`, bucket counts, and `access: "bucket_scan"` for scans.

## Indexes

```json
{
  "op": "create_index",
  "coll": "events",
  "field": "user",
  "unique": false
}
```

Indexes are maintained on insert, update, delete, TTL expiry, replication, and
`$out`/`$merge`. Creating an index backfills from sealed and open buckets in a
transaction; a failed unique backfill rolls back the definition and all index
entries. Reindex rebuilds from visible documents.

## TTL, history, and events

`ttl_seconds` on collection creation, per-document `_ttl`, and date-field TTL
(`set_collection_ttl_after`) behave as in `standard`. Expired rows are hidden
at read time and removed by expiry passes with index, graph, search, and
optional history cleanup.

History (`set_collection_history`) records inserts, updates, and deletes.
Collection events and live watches fire for columnar writes in the same order
as standard writes.

## Backup, compaction, and replication

Backups use the normal online backup command. The copied database is compacted
so sealed buckets and overlays are consistent; reopening the backup preserves
all visible documents, ids, indexes, TTL metadata, and history.

Use compaction after large update/delete batches:

```json
{"op": "compact_collection", "coll": "events"}
```

Compaction rewrites visible rows into fresh sealed buckets and discards
tombstoned overlay entries. It is crash-safe: an interrupted compaction is
recovered on the next `Store::open`.

The insert path seals buckets with a row-oriented staging encoding so append
throughput stays close to `standard`; an explicit compaction re-encodes those
buckets with the adaptive column codecs. Reads understand both layouts, point
lookups decode one row, and storage comparisons should be made after
compaction. Unsorted scans and order-independent `$group` stages decode bucket
blocks in parallel by default (`NONODB_SCAN_WORKERS` and
`NONODB_AGGREGATE_WORKERS`).

Replication carries collection creation with profile and settings, every
insert/update/delete, TTL changes, and `columnar_settings` operations. Replica
apply uses the same validation as the primary, including duplicate `_id`
rejection and unique-index conflicts.

## MongoDB wire

The `create` command accepts a `columnar` document. It is mutually exclusive
with `timeseries`:

```javascript
db.createCollection("events", {
  columnar: {
    bucketMaxDocs: 512,
    bucketMaxBytes: 524288,
    compression: "balanced"
  }
})
```

`listCollections` returns the settings under `options.columnar` with
`bucketMaxDocs`, `bucketMaxBytes`, and `compression`. The HTTP/socket
`list_collections` response keeps the `collections` name list and adds
`collection_info` entries with `profile` and `columnar` settings. The GUI
collection settings panel reads this data together with
`get_collection_columnar` and shows live row/bucket/id-map/bucket-byte stats.
The JSON `/metrics` snapshot adds `columnar_collections` entries with rows,
open/sealed buckets, id-map entries, tombstone deletes, and open/sealed/id-map
bytes; Prometheus exposes the same values as `nonodb_columnar_*` gauges.
`nonodb-cli inspect` reports the columnar profile and bucket statistics for an
offline database.

## Clients, CLI, and GUI

Python:

```python
db.create_collection(
    "events",
    profile="columnar",
    columnar={"bucket_max_docs": 512, "compression": "balanced"},
)
db.get_collection_columnar("events")
db.set_collection_columnar("events", {"compression": "max"})
```

Node.js:

```js
await db.createCollection("events", "columnar", null, null, null, {
  bucket_max_docs: 512,
  compression: "balanced",
});
await db.getCollectionColumnar("events");
await db.setCollectionColumnar("events", { compression: "max" });
```

Rust:

```rust
db.create_collection_with_columnar(
    "events",
    serde_json::json!({"bucket_max_docs": 512, "compression": "balanced"}),
)?;
db.get_collection_columnar("events")?;
```

Go:

```go
err := client.CreateCollectionWithColumnar(ctx, "events", map[string]any{
    "bucket_max_docs": 512,
    "compression": "balanced",
}, nil)
settings, err := client.GetCollectionColumnar(ctx, "events")
```

CLI:

```bash
nonodb-cli create-collection events --profile columnar \
  --bucket-max-docs 512 --bucket-max-bytes 524288 --compression balanced
nonodb-cli get-collection-columnar events
nonodb-cli set-collection-columnar events --compression max
```

The GUI collection dialog exposes `columnar` in the storage profile selector
with bucket and compression settings, and lists existing columnar settings with
the collection details.

## Migration and rollback

There is no in-place conversion between `standard` and `columnar`. Export the
documents and import them into a new collection, or use an aggregation
`$out`/`$merge` into the target collection.

`scripts/nonodb-migrate.py --target-profile columnar` creates target
collections with the columnar profile and preserves ids, BSON types, indexes,
TTL, and checksums. The same checksum verification runs for both profiles.

Rollback to `standard` is an export/import operation. Preserve `_id`, field
types, index definitions, TTL settings, and history if needed. The exported
visible documents are byte-for-byte equivalent to what `find` returned from
the columnar collection; overlays and tombstones are not part of the export.

## Downgrade and compatibility

A binary that predates `profile: "columnar"` rejects the profile with an
explicit error when the collection is opened; it never falls back to
`standard` and never reinterprets bucket data. The database remains unchanged.

Backups containing columnar collections can only be restored on a binary that
supports this profile. Check the release notes before downgrading a node. The
on-disk bucket magic and format version are independent of the collection
metadata, so an unknown bucket version also fails closed.

## Performance

The canonical 100k-row release gates are reproduced with:

```bash
cargo build --release -p nonodb-core --example columnar_bench
NONODB_AGGREGATE_MAX_DOCS=200000 ./target/release/examples/columnar_bench
```

Measured evidence, including storage with indexes, batch insert, point reads,
updates, scans, aggregation, and compaction reclaim, is recorded in
[columnar performance evidence](columnar-performance.md). The write path uses
row-oriented staging buckets and parallel serialization; scans and
order-independent `$group` stages decode document-count-balanced chunks in
parallel.

## Current limits

- No in-place profile conversion.
- No per-bucket manual rewrite; use collection compaction.
- The hard bucket limit is 4 MiB encoded; keep documents well below the
  configured bucket byte target.
- Index definitions and graph/search sidecars consume additional space and are
  included in `storage_stats`.
