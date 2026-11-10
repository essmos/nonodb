# Roadmap and limits

nonodb is intentionally narrow. This page lists what is supported today, what
is deliberately out of scope, and which gaps are candidates for future work.

## Current status

The project is at `0.4.0`. It is suitable for local and single-host
applications that need a small, durable, queryable JSON store and can accept a
single-writer model.

Implemented:

- JSON documents, collections, automatic UUID `_id`;
- MongoDB-style filters, projections, sorts, `skip`/`limit`;
- CRUD, multi-update, replacement updates, upserts;
- `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$addToSet`, `$setOnInsert`,
  `$each`;
- single-field, compound, unique, and covering indexes;
- index-only sorted scans and index-backed counts;
- BM25 full-text search with diacritic folding and phrases;
- fuzzy search with trigrams and bounded Levenshtein distance;
- read batches and atomic write batches;
- Unix-socket daemon, optional TCP/TLS, bearer auth, uid/gid allow-lists;
- group commit, request limits/timeouts, metrics;
- snapshots, online backup, restore, inspect, CLI;
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
  Use atomic update operators (`$inc`) or `write_batch`.
- **One writer.** All writes are serialized. This is a design choice, not a
  temporary limitation.
- **No replication, sharding, or clustering.** One daemon owns one data
  directory.

### Query language

- The aggregation pipeline is bounded to 100,000 intermediate documents and
  has no cross-collection joins (`$lookup`, `$graphLookup`), `$facet`, or write
  stages (`$merge`, `$out`).
- No regular-expression filters (`$regex`), `$size`, `$elemMatch`, `$type`,
  `$where`, or `$text` query operator.
- No joins.
- No TTL indexes or scheduled document expiry.
- Projection is top-level only, not a nested projection tree.
- Sorting and filtering use the documented subset; there is no collation API.

### Updates

- No `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$bit`, `$pop`.
- `$pull` accepts only an exact value, not a query predicate.
- No positional `$` update operators or array filters (`$[element]`).
- No pipeline updates.
- `_id` is immutable by contract; changing it with update operators is not
  supported.

### Indexes

- Only scalar values are indexed. Arrays and objects are skipped.
- No sparse/partial indexes, wildcard indexes, geospatial indexes, vector
  indexes, or text indexes with custom analyzers.
- No index intersection or cost-based optimizer. The planner uses a simple
  prefix/range rule.
- A covering projection requires an explicit `include` list and an inclusive
  projection.
- `drop_*` leaves named LMDB sub-databases registered; repeated creation and
  dropping of many different names can exhaust `MAX_DBS`.
- `MAX_DBS=256` means roughly 246 collections plus index/search sub-databases
  before `MDB_DBS_FULL`.

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

- One global bearer token; no users, roles, per-collection permissions, or
  audit log.
- Vault encryption is daemon-side, not end-to-end. The daemon sees plaintext
  and the raw key in memory.
- Vault collections have no searchable encryption, blind indexes, range
  queries, or sorting beyond `_id`.
- Vault key rotation is an operational re-encryption procedure; the daemon does
  not keep a keyring of old keys.
- Mutual TLS is supported, but revocation is limited to an allow-list of
  certificate fingerprints; there is no CRL or OCSP integration.
- Client certificates are not mapped to users, collections, or tenants.
- Unix peer checks use uid and primary gid only, not supplementary groups.
- TCP requires a token but is plaintext unless TLS is configured.

### Storage and operations

- LMDB file size does not shrink after deletes. Use a compacting backup and
  restore to reclaim space.
- `map_size` cannot be resized online; change `NONODB_MAP_SIZE` and restart.
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
3. **More update operators.** `$rename`, `$min`, `$max`, conditional `$pull`,
   `$mul`, `$currentDate`.
4. **Reindex tooling.** A command to rebuild text/fuzzy indexes after analyzer
   changes, with progress and timeout handling.
5. **Operational hardening.** Structured logs, readiness/liveness separation,
   graceful drain, and more metrics (latency histograms, index build status).

### Medium value

6. **Users and roles**, or at least per-collection tokens and mapping mTLS
   certificate identities to authorization scopes.
7. **Point-in-time recovery / WAL** for stricter durability and recovery
   objectives.
8. **Stop words, stemming, and per-index analyzers**; possibly an integration
   with Tantivy for advanced text search.
9. **Compression and/or CBOR storage** for smaller files and faster parsing,
   with a migration path.
10. **TTL indexes** and background expiry.
11. **Expanded aggregation** beyond the current `$match`/`$group`/`$project`/
    `$sort`/`$limit` subset, including streaming execution and more stages.

### Lower priority

12. Sparse/partial and wildcard indexes.
13. Geospatial or vector indexes.
14. A published Python package and additional language clients.
15. Benchmark automation in CI.
16. A web UI or schema browser.

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
