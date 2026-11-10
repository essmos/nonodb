# Architecture

This document describes how nonodb stores data, serves requests, maintains
indexes, and survives failures. It is intentionally implementation-oriented: if
you only want to use nonodb, start with the [README](../README.md) and the
[HTTP API](api.md).

## Design goals

nonodb is built around a small set of explicit goals:

1. **One local database, no external service.** A single daemon owns the data
   directory. Clients do not open LMDB.
2. **Safe for preforking servers.** Any number of uvicorn/Gunicorn workers can
   connect over a Unix socket without sharing file descriptors or mmap state.
3. **Crash safety by default.** A process kill must not corrupt the database.
4. **Predictable indexed reads.** Lookup cost should be effectively independent
   of collection size when an index is available.
5. **A familiar document API.** JSON documents, MongoDB-like filters, and a
   practical subset of update operators.
6. **Small operational footprint.** One binary, one data directory, one socket,
   no JVM, no cluster, no separate cache.
7. **Explicit write semantics.** One writer, group commit, and an explicit
   all-or-nothing `write_batch` instead of general interactive transactions.

Non-goals are listed in [Roadmap and limits](roadmap.md).

## Process model

```text
 application processes                        daemon process
 ─────────────────────                        ──────────────
 uvicorn worker 1 ─┐
 uvicorn worker 2 ─┤  HTTP/1.1 + JSON        ┌────────────────────────────┐
 ...               ├────────────────────────▶│ axum/hyper accept loop     │
 uvicorn worker N ─┘  Unix socket or TCP/TLS  │                            │
                                              │  read path: spawn_blocking │
                                              │  write path: mpsc channel  │
                                              │                            │
                                              │  ┌──────────────────────┐  │
                                              │  │ single writer thread │  │
                                              │  │ group commit         │  │
                                              │  └──────────┬───────────┘  │
                                              └─────────────┼──────────────┘
                                                            │
                                                   ┌────────▼────────┐
                                                   │ LMDB environment│
                                                   │ data.mdb        │
                                                   │ lock.mdb        │
                                                   └─────────────────┘
```

Only `nonodb-daemon` opens the LMDB environment. Read requests are executed on
blocking worker threads (Tokio's `spawn_blocking`); write requests are sent to a
dedicated writer thread over an `mpsc` channel. The HTTP layer never performs
storage work on the async runtime.

A client connection is stateless. There are no sessions, cursors, or
transactions spanning requests. This is what allows the client to be a thin
HTTP wrapper and makes worker restarts trivial.

## Storage engine: LMDB

nonodb uses [LMDB](https://www.symas.com/lmdb) through the
[`heed`](https://docs.rs/heed) crate.

Properties that matter here:

- **Memory-mapped B+ trees.** Reads touch pages on demand; the OS page cache
  keeps hot data. The daemon's own heap does not need to hold the data set.
- **Copy-on-write.** A writer never overwrites pages visible to a reader.
  Readers see a stable snapshot for the lifetime of their transaction.
- **Single writer, many readers.** LMDB serializes writers internally. nonodb
  makes that explicit with one writer thread.
- **Crash safety.** Committed transactions survive process death. An
  interrupted write leaves the last committed state intact.
- **No WAL, no background compaction.** The file grows to a high-water mark and
  does not shrink. `backup` with compaction produces a smaller copy.

The default `map_size` is 1 GiB. This is a maximum address-space reservation
for the database, not an allocation of 1 GiB of RAM or disk. It can be raised
with `NONODB_MAP_SIZE`. The current file size grows as needed.

## Data layout

An LMDB environment contains named sub-databases. nonodb uses this layout:

| Sub-database | Key | Value | Purpose |
|---|---|---|---|
| `meta` | metadata key | JSON or scalar | Collection registry, collection profile, index definitions, search totals, history sequence |
| `history` | collection + `_id` + revision | revision JSON | Append-only document history |
| `ttl` | expiry index / collection + `_id` | expiry timestamp | Document expiry metadata |
| `coll:<name>` | `_id` bytes | JSON document | Primary collection data |
| `idx:<coll>:<fields>` | encoded values + `_id` | encoded `_id` / covering payload | Secondary index |
| `ft:<coll>:<field>` | `term\0_id` | term frequency (u32 LE) | BM25 postings |
| `ftl:<coll>:<field>` | `_id` | document length (u32 LE) | BM25 normalization |
| `fz:<coll>:<field>` | `trigram\0_id` | empty | Fuzzy candidate index |

Collection names are validated. The `_id` is always a string and is also the
primary B+ tree key. Documents are serialized with `serde_json`; reads use
`sonic-rs` for faster parsing.

Dates use MongoDB Extended JSON (`{"$date": {"$numberLong": "<ms>"}}`) and are
canonicalized on write to UTC Unix milliseconds. Index encoding gives dates
their own type tag and an order-preserving signed integer payload, so range
filters, sorting, compound indexes, and unique constraints compare instants
rather than the original string representation. Date strings are not converted
implicitly. See [Dates and time zones](dates.md).

The optional `history` sub-database stores immutable revision snapshots. A key
contains the collection name, `_id`, and a global `rev` assigned by the writer.
The current collection is never modified by history and history entries are
not indexed. See [Document history](operations.md#document-history).

The `ttl` sub-database stores expiry metadata separately from documents. It has
two key families in one named database: an expiry-ordered index
(`e + expire_ms + collection + _id`) for the sweeper and a direct
collection/`_id` lookup (`i + collection + _id`) for updates and deletes.
Collection defaults live in `meta` as `ttl:<name>`. TTL entries are written in
the same transaction as the document, indexes, and history. See
[Document TTL](ttl.md).

### Vault collection layout

A collection profile is stored in `meta` as `profile:<name>`. Missing
means `standard`; the second supported value is `vault`.

A vault document stores only its primary key, tenant, and encrypted envelope:

```json
{"_id":"...", "tenant_id":"acme", "_enc":{"v":1,"alg":"xchacha20poly1305","kid":"acme:v1","n":"...","ct":"..."}}
```

The complete original document is encrypted into `_enc.ct` with
XChaCha20-Poly1305. AAD binds the ciphertext to collection name, tenant, `_id`,
and key id. `_id` and `tenant_id` stay plaintext so primary-key lookup and tenant
isolation do not require decrypting the whole collection.

Vault collections have no secondary, text, or fuzzy indexes. Standard write
methods reject vault collections, and vault requests require the three
encryption headers. See [Vault collections](vault.md) for the API contract and
threat model.

Every collection and every index consumes a named LMDB sub-database.
`MAX_DBS` is 256, which in practice limits a database to roughly 245
collections before `MDB_DBS_FULL` (indexes consume additional slots). Dropping
an index deletes its contents but the named sub-database remains registered in
LMDB; see [Operations](operations.md#capacity-and-memory).

## Read path

At a high level:

1. The HTTP handler checks authentication and parses the request.
2. A read task opens one `RoTxn` (LMDB read transaction).
3. The collection resolves the cheapest access path:
   - exact `_id` → direct B+ tree lookup;
   - usable secondary index → range scan over index entries;
   - otherwise → full collection scan.
4. A residual matcher verifies every candidate against the complete filter.
5. The result is projected, sorted, paginated, and serialized.

A read transaction provides a consistent snapshot for the whole request. The
read-only `batch` operation reuses one `RoTxn` for all of its operations, so a
batch sees a single point-in-time view.

### Query planning

The optimizer is intentionally simple and conservative. It uses an index when:

- the filter has an equality condition on every field of a compound index
  prefix, with an optional range on the last field; or
- a single-field index has an equality or range condition; and
- the indexed field value is not `null` (because `null` also matches a missing
  field, which is absent from the index).

Even when an index supplies candidates, the full filter is evaluated against
each document. Indexes are an access path, not a substitute for query
semantics.

If the sort order exactly matches the full index order, nonodb can stream the
index without materializing and sorting a result set. This is the `index-only
sorted scan` path.

### Full scans and parallel scans

A full scan iterates primary keys in `_id` order. `find_one` and `find` without
a sort stop early once `skip + limit` matches have been produced.

A full-collection sort without a usable index is CPU-bound. For shallow
queries (`skip + limit <= 50,000`), nonodb splits raw primary-key/value ranges
into chunks and parses/filters them on up to 16 worker threads. Each worker
computes a bounded top-K heap; the coordinator merges them with a stable global
sequence number. Deep skips stay single-threaded to avoid keeping a large heap
per worker.

Unsorted scans use the same chunking with an adaptive wave coordinator. The
first wave is small, receives the results, and merges matches in `_id` order.
If it produced enough matches for `skip + limit`, the remaining chunks are never
sent, so broad filters keep the serial early exit. If not, later waves are
larger to amortize synchronization during a selective full scan. `find_one`
and very small limits stay on the serial path to avoid thread overhead.

String and boolean equality conditions also get a raw JSON prefilter. Before
deserializing a document, the scanner checks whether the serialized key and
value occur in the stored bytes. A miss skips the document entirely; a hit
still runs the full matcher, so false positives are harmless and there are no
false negatives. Numeric and `null` conditions are always left to the parser
because their equality semantics do not map safely to raw bytes.

### Primary-key ranges

An `_id` equality or range (`$gt`, `$gte`, `$lt`, `$lte`) is executed directly
on the primary B+ tree, even when the filter has additional fields. This is not
a secondary index, but it narrows a scan to a contiguous key range. `find`,
`count`, `update`, and `delete` all use this path. Sort by `_id` can stream the
range in either direction and stop after `skip + limit`.

Clients can split a large non-indexed scan into disjoint `_id` ranges and issue
them concurrently; each request reads only its range.

### Aggregation

Aggregation is implemented in `nonodb-core/src/aggregate.rs` and is strictly
read-only. The daemon runs it in a blocking task with one `RoTxn`. A safe
pipeline prefix is pushed into `find_in`: a leading `$match`, then a contiguous
`$sort`+`$skip`/`$limit`, a simple `$project`, and `$skip`. This reuses secondary
indexes, primary-key ranges, covering projections, index-only sorts, and the
query-level TopK. `$sort` is only pushed when a following `$limit` keeps the
TopK heap small; otherwise the in-memory `sort_by` path is faster. A lone
`$count` after `$match` calls `count_in` and never materializes documents.

The remaining stages are evaluated in memory. Intermediate results are capped
at 100,000 documents to keep memory bounded; this is a deliberate first-version
limit rather than a query-planning restriction. `$sort` reuses the same document
comparator as `find`, and expressions reuse the path helpers used by filters and
updates. `$addToSet` and grouping keys serialize values to canonical JSON
strings for deduplication and grouping.

### Choosing a read strategy

Use the cheapest access path that fits the query:

1. **Exact `_id`** — direct primary-key lookup, ~sub-millisecond.
2. **Secondary index** — for equality/range filters and supported sorts.
3. **`_id` range** — split a large scan by primary key, or cover time-ordered
   custom ids.
4. **Partitioned collections** — keep hot tenants, months, or types in separate
   collections so a query touches only the relevant subset. This is an
   application-level index with no engine changes.
5. **Full scan with raw prefilter** — fallback for ad-hoc predicates.
6. **Daemon read cache** — repeated identical reads, bounded TTL and
   invalidated after every committed write.

For frequent queries, indexes and collection partitioning remain the only
approaches that avoid linear scans.

### Automatic indexes

Automatic indexing is an opt-in optimization layer, not a replacement for
schema design. It is disabled by default and must be enabled with
`NONODB_AUTO_INDEX=1`.

The daemon maintenance worker keeps in-memory frequency counters for
cache-missed read shapes:

```text
collection + set of indexable fields + equality/range classification
```

Concrete values are deliberately not part of the key. A shape becomes a
candidate after `NONODB_AUTO_INDEX_MIN_HITS` misses inside a rolling window.
The maintenance worker then checks the collection size, existing indexes,
limits, and write queue, and asks the normal writer to create the index:

```text
read cache miss ──▶ shape counter ──▶ candidate
                                         │
                                         ▼
                         maintenance worker (auto-index)
                                         │ CreateIndex / DropIndex
                                         ▼
                                    writer thread
```

The full scan path, raw prefilter, and read cache remain in place. If an
automatic index is dropped or cannot be built, queries still return correct
results through the fallback path.

Automatic indexes are marked in `auto-index.json` in the data directory. Only
those indexes are eligible for TTL eviction; manually created indexes are never
removed. Demand is recorded even on cache hits so a hot query does not lose its
index merely because the cache is serving it.

The same maintenance worker also drives TTL sweeps and history pruning. It
never opens a write transaction itself; every mutation is submitted to the
single writer.

Important limitations:

- automatic index creation uses the normal single-writer path; queries keep
  using the existing scan/cache fallback until the index is available;
- creation is skipped when the write queue is backlogged;
- only simple equality/range predicates are considered;
- there is no selectivity histogram yet, so very broad indexes can be created
  if the query is frequent enough;
- sorting-only queries and `$or` shapes are not auto-indexed.

## Maintenance worker

One background thread coordinates low-priority maintenance:

- TTL sweeper: reads due entries, submits an `Expire` job to the writer, and
  re-checks the expected expiry under the write transaction;
- history pruning: submits a bounded `PruneHistory` job every 30 seconds;
- automatic indexes: runs the conservative candidate/eviction logic on its own
  interval.

The worker is intentionally not a second LMDB writer. This keeps the single
writer model, group commit, and crash-safety guarantees intact.

## Write path and group commit

All writes go through a channel to one writer thread:

```text
 request A ─┐
 request B ─┼──▶ mpsc queue ──▶ writer thread ──▶ one RwTxn ──▶ commit (fsync)
 request C ─┘                       collect up to 512 jobs / adaptive window
```

The writer waits for the first job, then collects more jobs for a short window:

- 1 ms when the queue is short (low latency);
- 3 ms when at least 64 jobs are queued;
- 5 ms when at least 256 jobs are queued.

It executes the collected requests in one LMDB write transaction and commits
once. The `fsync` cost is therefore amortized across many requests. Responses
are sent only after the commit succeeds.

Two details are important:

- **`write_batch` is isolated.** An atomic batch must not share a transaction
  with unrelated requests: if one operation fails, the whole batch must roll
  back without rolling back someone else's write. The writer executes an atomic
  batch in its own transaction.
- **Read-only `batch` never enters the write queue.** It runs entirely on the
  read path with one `RoTxn`.

There is no general interactive transaction API. If you need a set of writes to
be all-or-nothing, use `write_batch`.

## Indexes

### Key encoding

Secondary index keys are ordered so that LMDB byte ordering matches logical
ordering:

- scalar values are tagged by type and encoded in an order-preserving form;
- numbers are encoded through an order-preserving transform of `f64`;
- strings are escaped UTF-8;
- Extended JSON dates are encoded as order-preserving signed millisecond
  integers with a separate type tag;
- compound keys concatenate encoded fields;
- the document `_id` is appended to make each key unique.

Because the encoding is ordered, range queries (`$gt`, `$gte`, `$lt`, `$lte`)
become contiguous LMDB ranges. An index range scan does not deserialize
documents until candidates are verified.

Secondary indexes accept strings, numbers, booleans, dates, and `null` values
that are actually present. Arrays and ordinary objects are skipped. Dates are
canonicalized before indexing, so all offsets for one instant share a key. Text
and fuzzy indexes search string/number/bool scalar values only; dates are not
searchable text.

Index definitions record the key-encoding format. Definitions written before
date support default to format 0 and are not used for filters containing dates,
because those indexes skipped `$date` objects during backfill. Recreating the
index upgrades it to the current format.

### Compound indexes

`fields: ["tenant", "kind", "created_at"]` creates an index named
`tenant+kind+created_at`. The optimizer can use:

- equality on `tenant` + equality on `kind` + a range on `created_at`;
- equality on a prefix, followed by an index-order sort.

It does not attempt arbitrary index intersection or multi-index plans.

### Unique indexes

A unique index rejects a document whose full indexed key already exists. For a
compound unique index, uniqueness applies to the complete key, not to each
field separately. Uniqueness is checked during inserts, updates, and backfill.

### Covering indexes

`include: ["name", "status"]` stores those fields in the index value alongside
the `_id`. If a query's projection is fully covered by the indexed fields and
the included fields, the daemon can build the result without reading the
document. Otherwise it falls back to fetching the document.

Covering is used only for inclusive projections. An exclusive projection always
needs the full document.

### Index maintenance and backfill

Indexes are updated in the same transaction as the document. Creating an index
on a non-empty collection triggers a synchronous backfill:

- the main thread reads raw key/value chunks through the active write
  transaction;
- worker threads parse JSON and encode index entries in parallel;
- the main thread applies entries in that same write transaction;
- duplicate-key and parse errors are reported without deadlocking workers.

Because the index build holds a write transaction, it blocks other writers for
its duration. On large collections, raise the client and server timeouts.

## Full-text search

`create_text_index` builds:

- `ft:<coll>:<field>` — postings `term\0_id → term frequency`;
- `ftl:<coll>:<field>` — document lengths for BM25 normalization;
- a total-length counter in `meta`.

Queries tokenize the search string, look up postings for every term, compute
BM25 scores (`k1=1.2`, `b=0.75`), merge scores by document, and sort descending.
The semantics are **OR** over terms; BM25 decides the ranking. Duplicate query
terms are removed.

Quoted phrases are parsed separately. A document must contain each phrase as a
contiguous token sequence. Phrase words still participate in scoring, and the
phrase check is applied before `skip`/`limit`. An optional residual filter is
also applied before pagination.

Tokenization:

- Unicode lowercase;
- diacritic folding (NFD, removal of combining marks, plus `ł/ø/đ/ß` mappings);
- maximal runs of alphanumeric characters;
- punctuation and whitespace are separators.

There is no stemming and no stop-word list.

## Fuzzy search

`create_fuzzy_index` stores unique trigrams for the field value. A query:

1. normalizes the query;
2. collects documents sharing at least one trigram;
3. ranks candidates by shared-trigram count;
4. caps candidates at 5,000;
5. computes bounded Levenshtein distance to the whole normalized value or to
   the closest token;
6. returns matches with distance ≤ `max_distance` (default 2), sorted by
   `_distance` then `_id`.

Trigrams are an index for candidate generation, not the final distance. The
final Levenshtein comparison is exact within the requested bound.

## Concurrency and consistency

- **Readers never block readers.** MVCC snapshots.
- **Readers do not block the writer.** A writer creates new pages; old pages
  remain valid for active readers.
- **Writers are serialized.** One writer thread and LMDB's single-writer model.
- **No cross-request isolation.** A read-modify-write sequence performed as two
  separate requests can interleave with another client. For atomic updates use
  update operators such as `$inc`, or group writes in `write_batch`.
- **Read-only `batch` is a snapshot.** All reads in a batch see the same state.
- **`write_batch` is all-or-nothing.** A failure at any operation rolls back the
  whole batch.
- **Daemon read cache is shared and invalidated on commit.** It is bounded by
  TTL, entry count, and value size. It never survives a committed write.
- **Client cache is opt-in and local.** It is invalidated on writes through the
  same client instance and bounded by a TTL; it cannot provide cross-process
  freshness.

## Crash safety

LMDB's copy-on-write design means a crash can leave at most the last
uncommitted transaction incomplete. The previous committed root remains valid.
On startup, nonodb clears stale reader slots and opens the environment again.

The daemon does not need a repair step after `kill -9`. Tests kill the process
during active writes and verify that:

- the database opens;
- all documents from committed batches are readable;
- indexes still agree with the documents;
- new writes succeed after restart.

The `lock.mdb` file is part of the LMDB environment. Do not copy it into a
backup or restore it as data. `nonodb-cli backup` copies a consistent LMDB
snapshot, and `restore` removes the stale lock file.

## Memory and disk behavior

- **RSS** reflects pages actually touched by the process and the OS page cache
  attributed to mapped pages. An idle daemon with a small working set is
  typically a few megabytes of RSS.
- **Virtual size** includes the `map_size` reservation and is much larger than
  RSS. This is normal and is not a memory leak.
- **Full scans and large sorts** can raise RSS substantially while the process
  touches many mapped pages. Restarting the daemon drops process RSS, but the
  OS may keep pages cached.
- **The LMDB file never shrinks.** Deletes leave free pages. A compacting
  backup produces a smaller copy; restoring it reduces the working file size.

See [Operations](operations.md#capacity-and-memory) for concrete measurements
and tuning guidance.

## Related documents

- [HTTP API](api.md)
- [Operations](operations.md)
- [Benchmarks](benchmarks.md)
- [Roadmap and limits](roadmap.md)
