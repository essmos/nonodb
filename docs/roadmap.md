# Roadmap and limits

nonodb is intentionally narrow. This page lists what is supported today, what
is deliberately out of scope, and which gaps are candidates for future work.

## Current status

The project is at `0.20.0`. It is suitable for local and single-host
applications that need a small, durable, queryable JSON store and can accept a
single-writer model.

Implemented:

- JSON documents, collections, automatic UUID `_id`;
- MongoDB-style filters, projections, sorts, `skip`/`limit`;
- SQL `SELECT`, `INSERT`, `UPDATE`, and `DELETE` over one collection; `SELECT`
  supports `DISTINCT`, `WHERE`, `GROUP BY`, `HAVING`, aggregates including
  `COUNT(DISTINCT field)`, `CASE`, scalar functions, `ORDER BY`,
  `LIMIT`/`OFFSET`, positional parameters, and `explain_sql`; simple field-only
  queries use the normal indexed `find` path, while writes use the single writer
  and are atomic per statement, including `UPDATE SET` expressions;
- CRUD, multi-update, replacement updates, upserts;
- `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$pullAll`, `$addToSet`,
  `$setOnInsert`, `$each`, `$position`, `$slice`, `$sort`;
- array updates with positional `$`, `$[]`, `$[identifier]`, and
  `arrayFilters`;
- atomic find-and-modify returning the old or new document;
- `explain` with access path, index/bounds, sort coverage, and actual counts;
- single-field, compound, unique, and covering indexes;
- single-field GeoJSON point geo indexes with `$near`, `$geoWithin`, and
  `$geoIntersects`;
- index-only sorted scans and index-backed counts;
- BM25 full-text search with diacritic folding and phrases;
- fuzzy search with trigrams and bounded Levenshtein distance;
- exact brute-force vector search with cosine, dot, and L2 metrics;
- read batches and atomic write batches;
- ordered point lookups by `_id` or a unique single-field or compound index,
  with covering projections, `_id` exclusion, key deduplication, and a response
  size cap;
- durable background jobs through an ordinary `_jobs` collection and external
  workers, with leases, retries, and idempotent enqueue;
- outbox events through an ordinary `_events` collection, with atomic data +
  event writes, automatic per-collection insert/update/delete events, optional
  field-change filtering, sortable event ids, per-consumer checkpoints, and
  server-side `watch` long-polling and continuous NDJSON/SSE streaming;
- server-side stream filtering by topic and payload, plus count thresholds with
  grouping and time windows;
- persistent conditional subscriptions with document filters, index-backed
  count conditions, level-triggered delivery by default, and optional
  edge-triggered (`once`) mode;
- connection-scoped live subscriptions (`watch` with `mode:"live"`) that push
  matching changes directly to SSE/NDJSON without writing to `_events`;
- an embedded local web console (`nonodb-cli gui`) with document CRUD, collection
  settings, indexes, SQL, map settings, and metrics;
- per-collection append-only document revision history with retention (`0` disables);
- document TTL with collection defaults, per-document overrides, and background expiry;
- BSON-compatible dates with RFC 3339 input, UTC normalization, indexed ranges, and sorting;
- Unix-socket daemon, optional TCP/TLS and mutual TLS, file-based principals and scopes, uid/gid allow-lists;
- group commit, request limits/timeouts, metrics;
- snapshots, online backup, SHA-256 manifests, deep verification, restore drills, CLI;
- optional vault collections with per-tenant whole-document encryption;
- Python client, Docker image, systemd unit;
- unit, property, integration, concurrency, crash, search, and large-data tests.

## Deliberate limitations

### Transactions and concurrency

- **No general interactive transactions.** There is no `begin`/`commit` API
  spanning multiple requests.
- **`write_batch` is the atomic primitive.** It is all-or-nothing, but it is a
  single request and cannot read intermediate results from outside.
- **No cross-request read-modify-write isolation.** Two requests can interleave.
  Use `find_and_modify`, atomic update operators (`$inc`), or `write_batch`.
- **One writer.** All writes are serialized. This is a design choice, not a
  temporary limitation.
- **No replication, sharding, or clustering.** One daemon owns one data
  directory.

### Query language

- The aggregation pipeline is bounded by `NONODB_AGGREGATE_MAX_DOCS` (default
  100,000 intermediate documents) and has no cross-collection joins (`$lookup`,
  `$graphLookup`), `$facet`, or write stages (`$merge`, `$out`). Group state is
  held in memory; there is no disk spill.
- No `$where` or `$text` query operator.
- `$regex` supports `i`, `m`, `s`, `x`, and `u` options, but not MongoDB's
  PCRE-only constructs; patterns use Rust's non-backtracking regex engine.
- `$all`/`$elemMatch` do not accept regex literals because regex values are not
  implemented. Multikey indexes serve array equality, `$all`, ranges, and simple
  `$elemMatch`; `$size` is not index-backed.
- No joins. `lookup` is a point/batch lookup, not a general join, and requires a
  matching unique index for non-`_id` keys. Non-unique lookups are not supported.
- SQL covers one collection. Joins, subqueries, scalar expressions in grouped
  `SELECT`, `RETURNING`, `INSERT ... SELECT`, and `UPDATE ... FROM` are not
  supported yet. Aggregate `DISTINCT` is limited to `COUNT(DISTINCT field)`.
- No stored procedures or daemon plugins. Background jobs and outbox events are
  ordinary client processes. `watch` long-polls or streams append-only outbox
  collections; it is not an automatic change stream for arbitrary writes,
  updates, or deletes.
- TTL is eventual and has no strict read-time filtering.
- Projection is top-level only, not a nested projection tree.
- Sorting and filtering use the documented subset; there is no collation API.
- Dates are instants only. IANA timezone names and timezone-aware aggregation
  expressions are not implemented; clients must send RFC 3339 with `Z`/offset.
- Existing ISO 8601 strings are not migrated to dates automatically.
- Decimal128 support is finite-only: decimal `NaN`/`Infinity` are rejected.
  Decimal arithmetic rounds results to 34 significant digits; values outside
  the Decimal128 exponent range return `invalid_numeric`.
- Long text has no dedicated type; the 1 MiB document limit applies to strings.
- ObjectId is supported as a field value, not as `_id`.
- BSON `Timestamp`, `MinKey`, `MaxKey`, regular-expression values, and legacy
  code/symbol types are not implemented.

### Updates

- No `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$bit`, `$pop`.
- No pipeline updates.
- `_id` is immutable by contract; changing it with update operators is not
  supported.

### Indexes

- Scalar values and array elements are indexed. Numbers use exact decimal keys
  shared by integers, floats and Decimal128; dates use the Extended JSON date
  type; binary/UUID and ObjectId use dedicated type tags. A compound index may
  contain at most one array field; parallel arrays are rejected. Ordinary
  objects are skipped. Indexes created before format 5 must be recreated to
  include array entries.
- No sparse/partial indexes, wildcard indexes, or text indexes with custom
  analyzers. Geo indexes currently cover single-field GeoJSON points;
  polygon/line document indexes are not implemented.
- Vector indexes store one vector per document and scan the whole index for
  each query. There is no ANN (HNSW/IVF/PQ), multi-vector document index, or
  built-in embedding model; clients send ready vectors. Auto-index does not
  create vector indexes automatically.
- Filtered vector search uses an exact `_id` or a secondary index when one is
  available. A highly selective filter on an unindexed field may require a full
  document scan.
- No index intersection or cost-based optimizer. The planner uses a simple
  prefix/range rule.
- A covering projection requires an explicit `include` list and an inclusive
  projection.
- `drop_*` reuses index/search sub-database slots. Legacy databases created by
  older releases remain registered until a compacting restore.
- `MAX_DBS=1024`; collection count plus concurrent index/search resources must
  stay below that limit.

### Search

- No stemming, stop-word removal, synonyms, thesaurus, or per-language
  analyzers.
- BM25 terms are ORed; there is no `AND`/`OR` query parser beyond quoted
  phrases.
- Phrases require contiguous tokens; no slop parameter.
- Fuzzy search caps candidate evaluation at 5,000 documents and supports only
  bounded Levenshtein distance.
- Search indexes cover scalar values only.
- Reindexing is required after analyzer changes.

### Protocol and clients

- No MongoDB wire-protocol compatibility and no official MongoDB driver
  support.
- HTTP/JSON only; no gRPC, binary framing, or CBOR API.
- The Python client is a single module, not a published package.
- The CLI covers common administration, not every possible operation.

### Security

- No built-in user directory, role hierarchy, or credential expiry policy;
  principals and per-collection scopes live in a daemon-managed file.
- Audit logging is append-only JSONL to a local file; there is no remote log
  shipping or tamper-evident chain.
- Failed-login lockout is process-local and keyed by the TCP peer IP. It does
  not trust proxy headers, share state between daemons, or distinguish users
  behind one NAT address.
- Vault encryption is daemon-side, not end-to-end. The daemon sees plaintext
  and the raw key in memory.
- Vault collections have no searchable encryption, blind indexes, range
  queries, or sorting beyond `_id`.
- Vault key rotation is an operational re-encryption procedure; the daemon does
  not keep a keyring of old keys.
- Mutual TLS is supported, but revocation uses the auth registry; there is no
  CRL or OCSP integration.
- Client certificates map to a principal with collection scopes, not to a
  separate user or tenant hierarchy.
- Unix peer checks use uid and primary gid only, not supplementary groups.
- TCP requires `NONODB_AUTH_FILE` but is plaintext unless TLS is configured.

### Storage and operations

- LMDB file size does not shrink after deletes. Use a compacting backup and
  restore to reclaim space.
- `map_size` can grow online; shrinking the reservation requires a restart and
  the file itself only shrinks through a compacting backup/restore.
- No write-ahead log, point-in-time recovery, or incremental backup. Snapshots
  and backups are periodic consistent copies.
- No online schema migration framework.
- No compression at rest.
- No storage encryption; use filesystem/volume encryption if needed.
- No multi-tenant isolation inside one database.
- No built-in TLS certificate rotation or secret management.

## Known trade-offs

| Decision | Consequence |
|---|---|
| LMDB + copy-on-write | Excellent crash safety and read concurrency; file never shrinks |
| One writer + group commit | Predictable durability and good fsync amortization; writes serialize |
| One daemon owns the file | Safe preforking; one more process to supervise |
| JSON on disk | Human-readable and flexible; larger/slower than CBOR for large data |
| Simple optimizer | Predictable; misses some multi-index opportunities |
| Global token auth | Simple; insufficient for multi-tenant roles |
| Snapshot-based backup | Simple; not PITR |
| Top-level projection | Simple; less expressive than MongoDB |

## Candidate future work

These are not commitments. They are ordered roughly by expected value for the
project's target workload.

### High value

1. **Raw JSON passthrough for simple reads.** For `find` without projection or
   sorting, stream stored JSON bytes directly into the response instead of
   parsing to `Value` and reserializing. Expected read-path win.
2. **Backfill memory and thread controls.** Add configurable worker counts and
   incremental writes instead of buffering many entries, reducing peak RAM on
   very large index builds.
3. **More update operators.** `$rename`, `$min`, `$max`,
   `$mul`, `$currentDate`.
4. **Reindex tooling.** A command to rebuild text/fuzzy indexes after analyzer
   changes, with progress and timeout handling.
5. **Operational hardening.** Readiness/liveness separation and more metrics
   (latency histograms, index build status). Structured logs and graceful drain
   are implemented.

### Medium value

6. **Users and roles**, or at least per-collection tokens and mapping mTLS
   certificate identities to authorization scopes.
7. **Point-in-time recovery / WAL** for stricter durability and recovery
   objectives.
8. **Stop words, stemming, and per-index analyzers**; possibly an integration
   with Tantivy for advanced text search.
9. **Timezone-aware date expressions** for aggregation, including IANA
   timezone identifiers for local calendar operations.
10. **Non-finite Decimal128 values and decimal comparison across all numeric
    edge cases**, building on the current finite Decimal128 support.
11. **BSON Timestamp and MinKey/MaxKey**, plus matching `$type` entries and
    global sentinel range behavior.
12. **Compression and/or CBOR storage** for smaller files and faster parsing,
   with a migration path.
13. **Expanded aggregation** beyond the current `$match`/`$group`/`$project`/
    `$sort`/`$limit` subset, including streaming execution and more stages.

### Lower priority

14. Sparse/partial and wildcard indexes.
15. Approximate vector indexes (HNSW/IVF/PQ) and polygon/line document geo indexes.
16. A published Python package and additional language clients.
17. Benchmark automation in CI.
18. A web UI or schema browser.

## Non-goals

- MongoDB wire-protocol compatibility.
- Distributed consensus, replication, or sharding.
- A general-purpose OLAP engine.
- Full MongoDB feature parity.
- Running multiple writers against one data directory.
- Network filesystems for the data directory.

## Versioning

Before 1.0, the HTTP API and on-disk format may change. Changes that affect data
compatibility should be accompanied by migration or reindex instructions.
Backups should be taken before upgrades.

## Contributing

See [Development](development.md) for build/test instructions and contribution
guidelines.
