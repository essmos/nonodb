# MongoDB compatibility

This page compares NonoDB's MongoDB-compatible API with MongoDB itself. It is a
practical map, not a claim of drop-in replacement.

> **Short answer:** NonoDB is an HTTP/JSON document database with a MongoDB-like
> query language. It also has an **experimental MongoDB wire listener** for a
> practical CRUD subset, with optional SCRAM-SHA-256 authentication. That
> listener is not a drop-in MongoDB replacement: it does not yet implement
> sessions, transactions, or change streams. The HTTP API and the
> Python/Rust/Node.js/Go clients remain the supported interfaces.

The assessment below is verified against NonoDB `0.25.x`.

## Status legend

| Status | Meaning |
|---|---|
| ✅ | Implemented with MongoDB-compatible semantics for common use |
| 🟡 | Implemented partially, with documented differences or missing modes |
| ❌ | Not implemented |

## Overall assessment

| Area | Approximate practical coverage | Main caveat |
|---|---:|---|
| CRUD and find semantics | ~90% | No MongoDB sessions, retryable writes, or multi-document transactions |
| Query filters | ~80% | No `$where`, `$text`, collation, or full BSON regex values |
| Projection | ~75% | `$meta` is approximate; no arbitrary computed projection expressions |
| Updates | ~85% | Pipeline updates support a subset of stages; timestamp `$currentDate` is unsupported |
| Aggregation stages | ~70% | Many common stages work; system, search, geoNear, and change-stream stages are absent |
| Aggregation expressions | ~70% | Broad but not exhaustive; no IANA time zones or user-defined JavaScript |
| Indexes | ~65% | No sparse/partial/wildcard indexes, collation, or TTL index options |
| BSON scalar types | ~75% | ObjectId `_id`, Date, Decimal128, Binary/UUID and numeric mapping; no Timestamp/MinKey/MaxKey |
| Wire protocol and drivers | ~50% | Experimental handshake plus CRUD, sorted cursors, count, distinct, aggregate, index DDL, collection DDL, and dropDatabase; no auth, sessions, or transactions |
| Transactions and change streams | ~10% | Atomic write batches and outbox events exist, but not MongoDB APIs |

These percentages are editorial estimates of the language/API surface that a
typical application uses, not a conformance score.

For an application that uses ordinary CRUD, secondary indexes, common
aggregation stages, and either string or ObjectId `_id`, the practical fit is
usually high through the NonoDB clients. An application that needs a full
MongoDB driver surface, transactions, change streams, or collation is not yet a
direct migration target.

## Protocol and clients

| Feature | Status | Notes |
|---|---|---|
| MongoDB wire protocol | 🟡 | Experimental listener with handshake, CRUD, cursors, count, distinct, aggregate, index DDL, and collection DDL; no sessions or auth |
| Official MongoDB drivers | 🟡 | PyMongo smoke-tested for CRUD, cursors, count, distinct, aggregate, index DDL, collection DDL, and SCRAM-SHA-256 auth |
| `mongosh` | ❌ | Use the NonoDB CLI, GUI, or HTTP API |
| MongoDB URI / `mongodb://` | 🟡 | Single direct connection to `NONODB_MONGO_ADDR`; username/password works with SCRAM-SHA-256 when configured; no replica-set URIs |
| Sessions | ❌ | No server sessions or causal consistency |
| Retryable writes | ❌ | Writes are routed to the single writer |
| Change streams | ❌ | Outbox events, `watch`, and subscriptions are separate APIs |
| Multi-document transactions | ❌ | `write_batch` is atomic, but it is not a general transaction API |
| Read/write concern | 🟡 | NonoDB has per-request durability; no MongoDB concern model |
| Bulk write API | 🟡 | Equivalent batching exists through `write_batch`, not `bulkWrite` |

## MongoDB wire listener

Set `NONODB_MONGO_ADDR=127.0.0.1:27017` and connect with a MongoDB driver using
`directConnection=true`. The listener is experimental. By default it is
unauthenticated; set both `NONODB_MONGO_USERNAME` and
`NONODB_MONGO_PASSWORD` to require SCRAM-SHA-256 authentication. The configured
account has instance-wide administrative access over the wire listener, so use
loopback or a trusted private network. To terminate TLS, set both
`NONODB_MONGO_TLS_CERT` and `NONODB_MONGO_TLS_KEY`, then connect with
`tls=true`. Client-certificate authentication and hot certificate reload are not
implemented for this listener.

| Wire command | Status |
|---|---|
| `hello` / `isMaster` / `ping` / `buildInfo` | ✅ |
| `insert` / `find` / `getMore` / `killCursors` | ✅ |
| `update` / `delete` / `findAndModify` | ✅ |
| `count` / `distinct` / basic `aggregate` | ✅ |
| `createIndexes` / `listIndexes` / `dropIndexes` | ✅ |
| `create` / `drop` / `listCollections` / `listDatabases` / `dropDatabase` | ✅ |
| SCRAM-SHA-256 authentication / TLS | 🟡 | Optional username/password auth and server TLS; no SCRAM-SHA-1 or client-certificate auth |
| Sessions / transactions / change streams | ❌ |

`dropDatabase` cannot remove the built-in `default` database. Wire `find`
supports arbitrary sort keys; `_id`/natural order uses fast keyset paging,
while other sort keys use skip-based cursor paging and re-sort each batch.
Index direction is accepted but NonoDB stores the field order, not
ascending/descending direction. Custom MongoDB index names are rejected; use
default names. `aggregate` returns the full result in the first batch; use the
HTTP aggregation API or cursors for very large result sets. Upserts without an
`_id` in the filter currently receive the core UUID rather than a
driver-style ObjectId.

## CRUD

| Feature | Status | Notes |
|---|---|---|
| `insertOne` / `insertMany` | ✅ | `insert`, `insert_many` |
| `find` / `findOne` | ✅ | Filters, sort, projection, skip, limit, hint, `$natural` |
| `updateOne` / `updateMany` | ✅ | Update operators and upserts |
| `replaceOne` | ✅ | Replacement updates preserve `_id` |
| `deleteOne` / `deleteMany` | ✅ | `delete` with optional `multi` |
| `findOneAndUpdate` / `findOneAndDelete` | ✅ | `find_and_modify` |
| `countDocuments` | ✅ | `count` |
| `distinct` | ✅ | Through SQL and aggregation |
| Array filters | ✅ | `$[identifier]` filters |
| Pipeline updates | 🟡 | `$set`/`$addFields`, `$unset`, `$project`, `$replaceRoot`, `$replaceWith` |
| Upsert with `$setOnInsert` | ✅ | Equality seeding, replacement upserts |
| Multi-document transaction | ❌ | Use a single atomic `write_batch` where applicable |
| Server-side JavaScript | ❌ | No JS execution |

NonoDB `_id` values may be strings or ObjectIds. Existing string IDs remain
unchanged; ObjectIds are stored with an internal UTF-8 key prefix and are
returned as BSON ObjectIds on the wire. Mixed string/ObjectId collections are
allowed. Other BSON types are not accepted as `_id`.

## Query filters

| Feature | Status | Notes |
|---|---|---|
| Equality and range | ✅ | Type-aware, exact numeric comparison |
| `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` | ✅ | `$ne` treats a missing field like `null` |
| `$in`, `$nin`, `$all`, `$size`, `$exists` | ✅ | |
| `$elemMatch` | ✅ | Arrays and object arrays |
| `$regex` / `$options` | 🟡 | `i`, `m`, `s`, `x`, `u`; not every PCRE feature |
| `$type` | ✅ | Common BSON scalar type names and numeric codes |
| `$mod`, bitwise operators | ✅ | `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet` |
| `$expr` | 🟡 | Uses the aggregation expression evaluator; errors are treated as no-match |
| `$jsonSchema` | 🟡 | Common keywords and combinators; not every JSON Schema draft feature |
| `$and`, `$or`, `$nor`, `$not` | ✅ | |
| `$where` | ❌ | No JavaScript execution |
| `$text` | ❌ | Full-text search is a separate NonoDB search API |
| `$comment` as a filter operator | ❌ | `comment` is available as a query option |
| `$rand` | ❌ | |
| Collation | ❌ | No per-query or per-index collation |
| BSON regex document values | ❌ | Use the `$regex` query operator |

## Projection

| Feature | Status | Notes |
|---|---|---|
| Include / exclude fields | ✅ | MongoDB-style inclusion and exclusion rules |
| `_id` inclusion/exclusion | ✅ | |
| Nested and dotted paths | ✅ | Traverses arrays of objects |
| Positional `$` | ✅ | Selects the first matching array element |
| `$slice` | ✅ | Number, negative, and `[skip, limit]` forms |
| `$elemMatch` projection | ✅ | |
| `$meta` | 🟡 | Pragmatic mapping from internal fields; no computed full-text score |
| Arbitrary computed projection expressions | ❌ | Use `$addFields`/`$set` in aggregation |
| `$meta: "textScore"` | 🟡 | Available without a full MongoDB text index |

## Update operators

| Operator | Status |
|---|---|
| `$set`, `$unset`, `$inc`, `$mul` | ✅ |
| `$min`, `$max`, `$rename` | ✅ |
| `$currentDate` (date) | ✅ |
| `$currentDate` (timestamp) | ❌ |
| `$bit` | ✅ |
| `$pop`, `$push`, `$pull`, `$pullAll`, `$addToSet` | ✅ |
| `$push` `$each`, `$position`, `$slice`, `$sort` | ✅ |
| `$setOnInsert` | ✅ |
| Array filters | ✅ |
| Pipeline updates | 🟡 |

## Aggregation stages

| Stage | Status | Notes |
|---|---|---|
| `$match`, `$project`, `$addFields`/`$set`, `$unset` | ✅ | |
| `$unwind` | ✅ | Supports `preserveNullAndEmptyArrays` and `includeArrayIndex` |
| `$group` | ✅ | Common accumulators |
| `$bucket`, `$bucketAuto`, `$sortByCount` | 🟡 | `$bucketAuto` `granularity` is unsupported |
| `$replaceRoot`, `$replaceWith` | ✅ | Result must be an object |
| `$sample` | ✅ | |
| `$facet` | ✅ | |
| `$lookup` | 🟡 | Equality join, pipeline, `let`, nested lookup |
| `$unionWith` | ✅ | Collection or sub-pipeline |
| `$graphLookup` | ✅ | Depth, depth field, restriction, cycle detection |
| `$setWindowFields` | 🟡 | Document and range windows; ranking, accumulators, `$shift`, `$expMovingAvg`, `$derivative`, `$integral` |
| `$densify` | 🟡 | Numeric and date fields; no arbitrary range expressions |
| `$fill` | 🟡 | `value`, `locf`, `linear`; no extrapolation |
| `$sort`, `$skip`, `$limit`, `$count` | ✅ | |
| `$out`, `$merge` | 🟡 | Same-database targets; `$merge` supports `let` and `whenMatched` pipelines |
| `$redact` | ❌ | |
| `$geoNear` | ❌ | Geo queries exist outside the aggregation stage |
| `$search` / Atlas Search | ❌ | Search is a separate NonoDB API |
| `$changeStream` | ❌ | Use outbox events / `watch` |
| `$collStats`, `$indexStats`, `$planCacheStats`, `$currentOp` | ❌ | |
| `$documents`, `$listSessions`, `$listLocalSessions` | ❌ | |

## Aggregation expressions

| Group | Status | Notes |
|---|---|---|
| Arithmetic | ✅ | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$pow`, `$abs`, `$ceil`, `$floor`, `$round` |
| Comparison and boolean | ✅ | Common comparison and `$and`/`$or`/`$not` forms |
| Conditionals | ✅ | `$cond`, `$switch`, `$ifNull` |
| Variables | ✅ | `$$ROOT`, `$$CURRENT`, `$let`, `$map`, `$filter`, `$reduce` |
| Arrays | ✅ | `$in`, `$isArray`, `$arrayElemAt`, `$slice`, `$concatArrays` |
| Objects | ✅ | `$objectToArray`, `$arrayToObject`, `$mergeObjects` |
| Strings | ✅ | Common operators including `$concat`, `$split`, `$substrCP`, `$regexMatch` |
| Dates | 🟡 | Broad date support; UTC and fixed offsets only, no IANA zones |
| Type conversion | 🟡 | `$type`; not every `$convert`/`$to*` form |
| User-defined JavaScript | ❌ | No `$function`, `$accumulator`, or `$where` |
| `$rand` | ❌ | |
| `$getField`, `$setField`, `$unsetField` | ❌ | |
| Full MongoDB operator catalogue | ❌ | The documented subset is intentionally bounded |

## Indexes

| Feature | Status | Notes |
|---|---|---|
| Single-field indexes | ✅ | |
| Compound indexes | ✅ | |
| Multikey indexes | ✅ | One entry per array element; compound parallel arrays rejected |
| Unique indexes | ✅ | Including unique multikey semantics |
| Covered queries / `include` | ✅ | Top-level inclusive projections only |
| Geospatial indexes | 🟡 | GeoJSON point indexes only; no polygon/line indexes |
| Text search indexes | 🟡 | Custom BM25/fuzzy search, not MongoDB `$text` |
| Vector indexes | 🟡 | NonoDB-specific exact/vector search |
| Sparse indexes | ❌ | |
| Partial indexes | ❌ | |
| Wildcard indexes | ❌ | |
| Hidden indexes | ❌ | |
| TTL indexes | ❌ | TTL is a collection/document setting |
| Per-index/per-query collation | ❌ | |
| Index hints | ✅ | Find-shaped queries; unusable hints fail instead of falling back |
| Background index builds | 🟡 | Parallel bounded backfill; not MongoDB's exact build lifecycle |
| Index build progress | ✅ | NonoDB metrics |

## BSON values

| Type | Status | Notes |
|---|---|---|
| String | ✅ | Supported `_id` type |
| Double / int / long | ✅ | Exact numeric comparison |
| Decimal128 (finite) | ✅ | Extended JSON `$numberDecimal` |
| Decimal128 NaN / Infinity | ❌ | Not fully supported |
| Date | ✅ | BSON-compatible instants, indexed ranges, sorting |
| Binary / UUID | ✅ | `$binary` and `$uuid` |
| ObjectId field value | ✅ | `$oid`; indexed and `$type`-aware |
| ObjectId `_id` | ✅ | Accepted from drivers and stored without converting to a string |
| Timestamp | ❌ | |
| MinKey / MaxKey | ❌ | |
| Regular-expression value | ❌ | `$regex` filters work; BSON regex values do not |
| Code / CodeWithScope / Symbol | ❌ | |
| DBPointer / Undefined | ❌ | |

## Operational differences

| Area | NonoDB | MongoDB |
|---|---|---|
| Deployment model | Embedded-style daemon, one writer, LMDB | Server/cluster with multiple engines |
| Scaling | Primary/replica/standby, no sharding | Replica sets and sharding |
| Transactions | Atomic single operation and `write_batch` | Multi-document transactions and sessions |
| Change notifications | Outbox events, `watch`, subscriptions | Change streams |
| Durability | Writer group commit and optional per-request durability | Journaling and write concern |
| Query planning | Index selection with hints and explain | Full MongoDB planner and plan cache |
| Security | Principals and scoped tokens | Users, roles, inherited privileges, LDAP, etc. |

## Migration guidance

A migration from MongoDB to NonoDB should check these items first:

1. **ObjectId `_id`** — supported, including default driver-generated ObjectIds.
2. **Drivers** — the experimental wire listener covers CRUD, cursors, count,
   distinct, aggregate, index DDL, and collection DDL only; use a NonoDB client
   for authentication and server administration.
3. **Transactions and sessions** — redesign around atomic `write_batch`,
   idempotent operations, or application-level compensation.
4. **Change streams** — replace with outbox events, `watch`, or subscriptions.
5. **`$text` and Atlas Search** — use NonoDB's separate search API.
6. **Collation** — normalize strings or enforce ordering in the application.
7. **Sparse/partial/TTL indexes** — remodel as collection settings, separate
   indexes, or application logic.
8. **BSON Timestamp/MinKey/MaxKey/regex values** — convert to supported values
   before import.
9. **Unsupported aggregation expressions/stages** — rewrite with supported
   stages or precompute outside the database.
10. **Wire-protocol tooling** — `mongosh`, BI connectors, and monitoring
    integrations that need more than basic insert/find must use another
    interface until the wire listener grows.

See [Migration and import](migration.md) for the import workflow and
[Roadmap and limits](roadmap.md) for planned compatibility work.
