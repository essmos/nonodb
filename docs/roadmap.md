# Roadmap and limits

nonodb is intentionally narrow. This page lists what is supported today, what
is deliberately out of scope, and where active implementation work is tracked.

## Current status

The project is at `0.33.0`. It is suitable for local, single-host, and
primary/replica deployments that need a small, durable, queryable JSON store
and can accept a single-writer model per database.

Implemented:

- JSON documents and collections; automatic UUID `_id` on the HTTP/core path
  and driver-generated ObjectId `_id` on the MongoDB wire path;
- MongoDB-style filters, projections, sorts, `skip`/`limit`;
- SQL `SELECT`, `INSERT`, `UPDATE`, and `DELETE` over one collection; `SELECT`
  supports `DISTINCT`, `WHERE`, `GROUP BY`, `HAVING`, aggregates including
  `COUNT(DISTINCT field)`, `CASE`, scalar functions, `ORDER BY`,
  `LIMIT`/`OFFSET`, positional parameters, and `explain_sql`; simple field-only
  queries use the normal indexed `find` path, while writes use the single writer
  and are atomic per statement, including `UPDATE SET` expressions;
- CRUD, multi-update, replacement updates, pipeline updates, upserts;
- `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`,
  `$currentDate`, `$bit`, `$pop`, `$push`, `$pull`, `$pullAll`, `$addToSet`,
  `$setOnInsert`, `$each`, `$position`, `$slice`, `$sort`;
- array updates with positional `$`, `$[]`, `$[identifier]`, and
  `arrayFilters`;
- atomic find-and-modify returning the old or new document;
- `explain` with access path, index/bounds, sort coverage, and actual counts;
- `comment`, `maxTimeMS`, index hints, and `$natural` sort for find queries and
  cursors; `maxTimeMS` also bounds `count` and aggregation scans;
- single-field, compound, unique, and covering indexes, plus sparse and
  `partialFilterExpression` membership with documented planner behavior;
- single-field GeoJSON point geo indexes with `$near`, `$geoWithin`, and
  `$geoIntersects`;
- index-only sorted scans and index-backed counts;
- BM25 full-text search with diacritic folding and phrases;
- fuzzy search with trigrams and bounded Levenshtein distance;
- exact brute-force vector search with cosine, dot, and L2 metrics, plus an
  optional internal IVF sidecar for approximate unfiltered queries;
- graph traversal over ordinary node/edge documents with a native transactional
  adjacency index, BFS/DFS, label and direction filters, vector seeds, BFS and
  Dijkstra `shortest_path`, all-shortest paths, linear pattern matching,
  PageRank/degree/component/label-propagation/k-core/degree-distribution
  algorithms, bounded background analytics jobs with atomic export, hard result
  budgets, a Python client, and a GUI tree;
- read-only Cypher queries through `cypher` and `Collection.cypher()`, with
  `MATCH`/`WHERE`/`WITH`/`UNWIND`, variable-length and shortest paths, optional
  matches, existential subqueries, a pinned openCypher TCK baseline, and a
  bounded GDS-style procedure subset;
- read batches and atomic write batches;
- ordered point lookups by `_id` or a unique single-field or compound index,
  with covering projections, index-only `_id` projections, `_id` exclusion, key
  deduplication, and a response size cap;
- client-side `lookup_join` helpers in Python, Node.js, Rust, and Go, preserving
  order and missing-key alignment over chunked lookups;
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
  settings, indexes, SQL, graph traversal, map settings, per-collection metrics,
  and authorization management;
- per-collection append-only document revision history with retention (`0` disables);
- document TTL with collection defaults, per-document overrides, and background expiry;
- BSON-compatible dates with RFC 3339 input, UTC normalization, indexed ranges, and sorting;
- stored BSON regular-expression values with `$type: "regex"`, exact `$eq`, and
  documented unindexed scan behavior;
- Unix-socket daemon, optional TCP/TLS and mutual TLS, file-based principals and scopes, uid/gid allow-lists;
- mutual TLS client revocation through the auth registry plus configurable CRL
  files/URLs and OCSP, with refresh, caching, and fail-closed defaults;
- group commit, request limits/timeouts, JSON and Prometheus metrics, health/readiness, disk and snapshot monitoring;
- snapshots, online backup, SHA-256 manifests, deep verification, restore drills,
  snapshot-based `restore --to <timestamp>`, and opt-in snapshot + segmented
  WAL point-in-time recovery with hard size/age limits and `wal_full` write
  rejection;
- verified restore-in-place through the admin API and GUI, with preflight
  verification, typed confirmation, audit entries, and a rollback safety
  snapshot;
- per-database data/index/history/sidecar quotas with API/CLI usage reporting,
  soft warnings, and hard `quota_exceeded` write rejection;
- `scripts/nonodb-migrate.py` for quiesced MongoDB migration with checksums and
  an opt-in MongoDB wire shadow mode with JSONL diff reports;
- offline `nonodb-cli branch` clones with reflink/clonefile, snapshot sources,
  copied sidecars, and a full-copy fallback;
- GUI backup jobs with full/restic engines, multi-database selection, exclusions,
  gzip compression, age/restic encryption, local/S3/R2 destinations, and
  `backup-info.txt`;
- admin-only staged import from local folders or S3/R2 into a new database;
- optional vault collections with per-tenant whole-document encryption;
- asynchronous primary/replica replication over HTTP/TLS with durable ordered
  operations, snapshot bootstrap, metadata/index/history/events/subscriptions,
  replica acks, retention, quorum promotion, leases, automatic election,
  in-place hard resync, pause/resume/disconnect, write routing, and a
  cluster-wide GUI;
- configurable parallel index backfill and automatic index builds;
- optional Network GUI/API with a dedicated nftables table, advertised replica
  URL discovery, and a per-user daemon instance registry;
- Python, Rust, Node.js, and Go clients, Docker image, systemd unit;
- unit, property, integration, concurrency, crash, search, and large-data tests.

## Deliberate limitations

### Transactions and concurrency

- **No general interactive transactions.** There is no `begin`/`commit` API
  spanning multiple requests.
- **`write_batch` is the atomic primitive.** It is all-or-nothing, but it is a
  single request and cannot read intermediate results from outside.
- **`insert_many` is not all-or-nothing.** It is a single request, but a failure
  can leave earlier documents from the same request committed. Use
  `write_batch` for atomic groups.
- **No cross-request read-modify-write isolation.** Two requests can interleave.
  Use `find_and_modify`, atomic update operators (`$inc`), or `write_batch`.
- **One writer.** All writes are serialized. This is a design choice, not a
  temporary limitation.
- **PITR is per database.** There is no atomic multi-database point-in-time
  recovery. PITR is opt-in, bounded by WAL size/age limits, and excludes
  `auth.json`, audit logs, and vault tenant keys.
- **Replication is asynchronous and primary/replica, not multi-writer.** Sharding
  and cross-database transactions are not implemented. The control plane
  replicates auth, backup jobs/state, the database catalog, and the cluster peer
  token; encrypted vault envelopes also replicate. Backup artifacts, TLS private
  keys, mode files, node-local settings, and audit logs do not. See the
  [replication and cluster guide](replication-cluster-guide.md).

### Query language

See [MongoDB compatibility](mongodb-compatibility.md) for a feature-by-feature
comparison and practical migration caveats.

- The aggregation pipeline is bounded by `NONODB_AGGREGATE_MAX_DOCS` (default
  100,000 intermediate documents). It supports equality `$lookup` joins,
  `$lookup` `let` variables, nested lookups, `$unionWith`, `$graphLookup`,
  `$setWindowFields` with document/range frames and advanced analytics,
  `$densify`, `$fill`, `$out`, `$merge` with `let` and `whenMatched` pipelines,
  `$facet`, `$bucket`, and `$bucketAuto`. Group state is held in memory; there
  is no disk spill.
- No `$where` query operator. The HTTP/core query language has no `$text`; MongoDB wire `$text` is a documented lexical subset backed by MongoDB-style text indexes.
- `$jsonSchema` supports the common validation keywords but not every JSON
  Schema draft feature.
- `$expr` uses the aggregation expression evaluator, including variables,
  common comparison, array, string, conditional, and date operators, including
  `$dateDiff` and custom `$dateFromString` formats. Date time zones support UTC,
  fixed offsets, and IANA identifiers with daylight-saving rules. Expression
  errors do not match the document instead of aborting the query.
- Projection supports nested paths, positional `$`, `$slice`, `$elemMatch`, and
  basic `$meta`. Native search supplies `_score`, HTTP `$meta: "textScore"`
  maps it, and MongoDB wire `$text` computes `textScore` with BM25.
- `$regex` supports `i`, `m`, `s`, `x`, and `u` options, but not MongoDB's
  PCRE-only constructs; patterns use Rust's non-backtracking regex engine.
- `$in`, `$nin`, `$all`, and `$elemMatch` accept `{"$regex": ...}` operands.
  Stored BSON regex values support exact `$eq` and `$type`, but are not indexed.
  Multikey indexes serve array equality, `$all`, ranges, and simple `$elemMatch`;
  `$size` is not index-backed.
- No joins. `lookup` is a point/batch lookup, not a general join, and requires a
  matching unique index for non-`_id` keys. Non-unique lookups are not supported.
- SQL covers one collection. Joins, subqueries, scalar expressions in grouped
  `SELECT`, `INSERT ... SELECT`, and `UPDATE ... FROM` are not supported yet.
  Writes support `RETURNING`, and all five aggregate functions support
  `DISTINCT`.
- No stored procedures or daemon plugins. Background jobs and outbox events are
  ordinary client processes. `watch` long-polls or streams append-only outbox
  collections; it is not an automatic change stream for arbitrary writes,
  updates, or deletes.
- TTL read visibility is strict by default.
- Projection expressions are limited; nested paths, positional `$`, and basic
  `$meta` are supported, but arbitrary computed projection expressions are not.
- Sorting and filtering use the documented subset; there is no collation API.
- Dates are stored as instants. Aggregation date expressions accept IANA
  timezone names for local calendar operations; stored values and client input
  still use RFC 3339 with `Z` or a numeric offset.
- Existing ISO 8601 strings are not migrated to dates automatically.
- Decimal128 support includes finite values and `NaN`/`Infinity`. Arithmetic
  results round to 34 significant digits; values outside the Decimal128
  exponent range return `invalid_numeric`.
- Long text has no dedicated type; the 1 MiB document limit applies to strings.
- ObjectId is supported as both a field value and `_id`.
- Legacy code/symbol BSON types are not implemented.

### Updates

- Pipeline updates support `$set`, `$addFields`, `$unset`, `$project`,
  `$replaceRoot`, and `$replaceWith`.
- `_id` is immutable by contract; changing it with update operators is not
  supported.

### Indexes

- Scalar values and array elements are indexed. Numbers use exact decimal keys
  shared by integers, floats and Decimal128; dates use the Extended JSON date
  type; binary/UUID and ObjectId use dedicated type tags. A compound index may
  contain at most one array field; parallel arrays are rejected. Ordinary
  objects are skipped. Indexes created before format 5 must be recreated to
  include array entries.
- A plain index, including a plain unique index, indexes a missing field as
  `null`, matching MongoDB, so only one missing/`null` value can be unique.
  `sparse` indexes omit documents whose key is absent; a compound sparse index
  omits a document only when every key is absent. `partialFilterExpression`
  limits index membership and is used by the planner only when the query repeats
  the predicate. No wildcard indexes or text
  indexes with custom analyzers. `expireAfterSeconds` on a single-field TTL
  index maps to date-field TTL and backfills existing documents. Geo indexes
  currently cover single-field GeoJSON points; polygon/line document indexes are
  not implemented.
- Vector indexes store one vector per document. Exact scan is the default and
  fallback; an optional IVF sidecar accelerates unfiltered queries. There is no
  HNSW/PQ, multi-vector document index, or built-in embedding model; clients
  send ready vectors. Auto-index does not create vector indexes automatically.
- Filtered vector search uses an exact `_id` or a secondary index when one is
  available. A highly selective filter on an unindexed field may require a full
  document scan.
- No index intersection or cost-based optimizer. The planner uses a simple
  prefix/range rule.
- A covering projection requires an explicit `include` list and an inclusive
  top-level projection. Nested and positional projections fetch the full
  document.
- `drop_*` reuses secondary index and vector sub-database slots. Text and fuzzy
  indexes use segment sidecars instead. Legacy databases created by older
  releases remain registered until a compacting restore.
- `MAX_DBS=1024`; collection count plus concurrent index/vector resources must
  stay below that limit.

### Search

- No stemming, stop-word removal, synonyms, thesaurus, or per-language
  analyzers.
- BM25 requires every positive term to match; there is no user-facing
  `AND`/`OR` query parser beyond quoted phrases.
- Phrases require contiguous tokens; no slop parameter.
- Fuzzy search caps candidate evaluation at 5,000 documents and supports only
  bounded Levenshtein distance.
- BM25 common-term queries on the full 8.84M MS MARCO collection still reach a
  p95 of about 30 ms. Block-max WAND is implemented; further pruning and query
  optimization remain the next performance step.
- Text and fuzzy indexes cover exactly one field. MongoDB-style multi-field text
  keys and non-default weights are rejected.
- Legacy LMDB text/fuzzy indexes are no longer read. Drop and recreate them as
  segment indexes before upgrading.
- Search indexes cover scalar values only.
- Reindexing is required after analyzer changes.

### Protocol and clients

- The optional MongoDB-compatible wire listener implements a documented subset
  for official drivers: handshake, CRUD, cursors, count, distinct, aggregate,
  index DDL, collection DDL, MongoDB-style text indexes and `$text`,
  SCRAM-SHA-256/SCRAM-SHA-1 authentication,
  authSource validation, auth-file principal scopes, aggregate cursor batching,
  strict rejection of unsupported driver options, mongoexport/mongodump-compatible
  export, PyMongo/Node.js/Go conformance, and TLS.
  It does not support client-certificate authentication, sessions,
  multi-document transactions, change streams, collation, or full MongoDB
  feature parity.
- The supported API is HTTP/JSON; no gRPC, binary framing, or CBOR API.
- The Python client is a single module. Its `nonodb-client` wheel and sdist are
  prepared, but PyPI publication is a separate manually approved step.
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
- Mutual TLS revocation combines the auth registry with configured CRL/OCSP
  sources. A source that cannot decide is rejected by default; fail-open mode is
  explicit. See [Mutual TLS](mtls.md).
- Client certificates map to a principal with collection scopes, not to a
  separate user or tenant hierarchy.
- Unix peer checks use uid and primary gid only, not supplementary groups.
- TCP requires `NONODB_AUTH_FILE` but is plaintext unless TLS is configured.

### Storage and operations

- LMDB file size does not shrink after deletes. Use a compacting backup and
  restore to reclaim space.
- `map_size` can grow online; shrinking the reservation requires a restart and
  the file itself only shrinks through a compacting backup/restore.
- PITR is opt-in and per database. Without it, snapshots and native backups
  are periodic full consistent copies; deduplicated incremental repositories
  use the restic engine.
- No online schema migration framework.
- No native compression at rest; restic repositories compress their own data.
- No native storage encryption; use [filesystem or volume encryption](encryption-at-rest.md) for the data directory.
- Sidecar copies are skipped when collections are excluded from a backup.
  Rebuild the affected text, fuzzy, or vector indexes after restore.
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
| Snapshot + WAL recovery | Fast snapshot restore and timestamp replay; opt-in disk cost and bounded retention |
| Projection tree model | Nested paths, positional `$`, `$slice`, `$elemMatch`, and basic `$meta` work |

## Future work

No active implementation backlog. Completed work is tracked in
[CHANGELOG.md](../CHANGELOG.md); the remaining entries below are deliberate
non-goals and documented limitations, not planned work.

## Non-goals

- Full MongoDB feature parity. The documented MongoDB-compatible subset targets
  common driver workloads and deliberately excludes sessions, multi-document
  transactions, change streams, collation, and other advanced MongoDB features.
- Distributed consensus, multi-writer clusters, or sharding. Asynchronous
  primary/replica replication is supported; see the
  [replication and cluster guide](replication-cluster-guide.md).
- A general-purpose OLAP engine.
- Running multiple writers against one LMDB environment.
- Cross-database queries or transactions.
- Network filesystems for the instance root.

## Versioning

Before 1.0, the HTTP API and on-disk format may change. Changes that affect data
compatibility should be accompanied by migration or reindex instructions.
Backups should be taken before upgrades.

## Contributing

See [Contributing](../CONTRIBUTING.md) for setup, required checks, and pull
request expectations. [Development](development.md) covers the repository
layout and test scripts in more detail.
