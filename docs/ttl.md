# Document TTL

nonodb supports optional document expiry with a collection default and
per-document overrides. Read visibility is **strict by default**: once
`_ttl_at <= now`, the document is hidden from find, count, aggregation, SQL,
search, lookups, cursors, and MongoDB-wire reads.

The old eventual-visibility behavior can be restored with
`NONODB_TTL_STRICT_READS=0`. This is an emergency compatibility switch; the
default is strict.

TTL is supported for both standard and vault collections. For vault
collections the expiry metadata is plaintext; the document body is never
decrypted by the sweeper.

NonoDB supports two collection TTL modes:

- **Insertion-based** (the default): each document expires `ttl_seconds` after
  it is inserted.
- **Date-field**: each document expires when a date field plus `ttl_seconds`
  passes. This matches MongoDB TTL indexes.

An explicit `_ttl` document field overrides both modes.

## MongoDB `expireAfterSeconds`

The MongoDB wire listener accepts the standard MongoDB form:

```javascript
db.sessions.createIndex({ createdAt: 1 }, { expireAfterSeconds: 86400 })
```

This creates the requested secondary index, switches the collection to
date-field TTL on `createdAt`, and backfills existing documents in bounded
batches. A document expires at `createdAt + 86400` seconds. Documents with a
missing or non-date `createdAt` never expire.

Limits:

- NonoDB supports one TTL index per collection.
- The TTL index must be single-field and cannot be a geo index.
- `expireAfterSeconds: 0` is rejected because collection TTL must be positive.
- Dropping the translated index does not clear the collection TTL; use
  `set_collection_ttl_after` with another field or `set_collection_ttl` with
  `null` if expiry should stop.

## Date-field TTL

Use date-field TTL when documents already contain a creation, session, or
retention timestamp:

```json
{
  "op": "set_collection_ttl_after",
  "coll": "sessions",
  "field": "createdAt",
  "ttl_seconds": 86400
}
```

The response confirms the selected mode:

```json
{"ok": true, "result": {"coll": "sessions", "ttl_seconds": 86400, "ttl_field": "createdAt", "purge_history": false}}
```

`set_collection_ttl_after` does not rewrite existing documents. Backfill TTL
metadata in bounded pages:

```json
{
  "op": "backfill_collection_ttl",
  "coll": "sessions",
  "field": "createdAt",
  "ttl_seconds": 86400,
  "after": null,
  "limit": 1000,
  "dry_run": false
}
```

The response contains one page:

```json
{
  "ok": true,
  "result": {
    "coll": "sessions",
    "field": "createdAt",
    "dry_run": false,
    "result": {"scanned": 1000, "updated": 998, "cleared": 2, "next_after": "s-1000", "done": false}
  }
}
```

Pass `next_after` as `after` until `done` is true. `dry_run: true` reports what
would change without writing metadata. Backfill is idempotent: rerunning it
recomputes the same expiry from the date field. The MongoDB wire listener and
`scripts/nonodb-migrate.py` perform this backfill automatically. Backfilled
expiry metadata is replicated to replicas, so strict read visibility matches
the primary before any background cleanup.

`get_collection_ttl` reports the active mode:

```json
{"ok": true, "result": {"coll": "sessions", "ttl_seconds": 86400, "ttl_field": "createdAt", "purge_history": false}}
```

Rules:

- The field must contain a BSON/Extended JSON date, for example
  `{"$date": "2026-01-01T00:00:00Z"}`. Missing, `null`, and non-date values
  never expire.
- An explicit `_ttl` on insert or update still overrides date-field TTL.
- Date-field TTL is not supported for vault collections; use the insertion-based
  collection default for vault data.
- Calling `set_collection_ttl` replaces date-field mode with insertion-based
  TTL. Calling `set_collection_ttl_after` replaces insertion-based mode.
- Existing date-field metadata is preserved across restart.

## Collection default

Set a default TTL in seconds when creating a collection:

```json
{
  "op": "create_collection",
  "coll": "sessions",
  "ttl_seconds": 86400
}
```

Or change it later:

```json
{"op": "set_collection_ttl", "coll": "sessions", "ttl_seconds": 3600}
```

Clear the default with `null` (or omit `ttl_seconds`):

```json
{"op": "set_collection_ttl", "coll": "sessions", "ttl_seconds": null}
```

Read the current default:

```json
{"op": "get_collection_ttl", "coll": "sessions"}
```

Response:

```json
{"ok": true, "result": {"coll": "sessions", "ttl_seconds": 3600, "purge_history": false}}
```

`ttl_purge_history`/`purge_history` controls whether an expired document's
stored revisions are deleted instead of appending a delete tombstone:

```json
{
  "op": "create_collection",
  "coll": "sessions",
  "ttl_seconds": 86400,
  "ttl_purge_history": true
}
```

```json
{"op":"set_collection_ttl","coll":"sessions","ttl_seconds":3600,"purge_history":true}
```

`purge_history` defaults to `false`; omitting it on `set_collection_ttl`
preserves the current setting.

A collection default applies only to documents inserted **after** the default
is configured. Changing or clearing the default does not rewrite existing
documents. `ttl_seconds` must be a positive integer. `set_collection_ttl`
selects insertion-based TTL and clears any date-field mode.

## Per-document TTL

Use the reserved top-level `_ttl` field in an inserted document:

```json
{
  "op": "insert",
  "coll": "sessions",
  "doc": {"_id": "s1", "user_id": 7, "_ttl": 300}
}
```

Rules:

- `_ttl` is a non-negative integer number of seconds and overrides the
  collection default or date-field TTL.
- Through the MongoDB wire listener, BSON int32/int64 values such as
  `Int64(30)` are accepted in addition to plain JSON integers.
- `_ttl: 0` or `_ttl: null` disables expiry for that document, even when the
  collection has a default.
- If `_ttl` is absent, the active collection mode applies: insertion-based
  default or date-field TTL.
- If no mode is active, the document never expires.
- Negative, fractional, or non-numeric values are rejected with
  `invalid_ttl`.
- `_ttl` is removed before the document is stored and is not returned by reads.
  It is an input directive, not a document field.

## Updates

With insertion-based TTL, a normal update preserves the existing expiry:

```json
{
  "op": "update",
  "coll": "sessions",
  "filter": {"_id": "s1"},
  "update": {"$set": {"last_seen": "2026-01-01T00:00:00Z"}}
}
```

Set a new TTL relative to the update time:

```json
{"op": "update", "coll": "sessions", "filter": {"_id": "s1"}, "update": {"$set": {"_ttl": 900}}}
```

Disable expiry:

```json
{"op": "update", "coll": "sessions", "filter": {"_id": "s1"}, "update": {"$set": {"_ttl": 0}}}
```

In date-field mode, an update without `_ttl` recomputes expiry from the current
value of the date field. Changing the date field moves the expiry; changing it
to a missing or non-date value clears expiry.

Replacement updates and upserts follow the same rule: an explicit `_ttl` in the
replacement sets the expiry; otherwise the active collection mode applies. A
document created by an upsert uses the normal insert semantics. `$unset:
{"_ttl": ...}` does not clear an expiry; use `$set` with `0` or `null`.

## Deletion and history

An expired document is deleted through the normal write path:

- the document is removed from the primary collection;
- secondary, text, fuzzy, and vector index entries are removed;
- TTL metadata is removed;
- when document history is enabled, a `delete` tombstone is appended, unless
  the collection has `purge_history: true`, in which case all stored revisions
  for that document are removed instead.

The delete and its history change are committed in one LMDB transaction.
Purging history is irreversible and intended for short-lived data where old
versions must not outlive the document.

## Maintenance and visibility

The daemon has one background maintenance worker. It never writes to LMDB
directly; expiry jobs are submitted to the single writer thread.

```text
TTL sub-database (sorted by expiry)
        │
        ▼
maintenance worker ── Expire job ──▶ writer thread ──▶ LMDB commit
```

The worker:

- scans due TTL entries every `NONODB_TTL_SWEEP_INTERVAL` seconds;
- deletes at most `NONODB_TTL_SWEEP_LIMIT` entries per pass;
- re-checks the expected expiry under the write transaction, so a concurrent
  update that refreshed the TTL is not deleted by a stale sweep;
- removes orphaned TTL entries when a collection no longer exists.

Reads apply the expiry check against the same LMDB snapshot they query. The
search paths over-fetch candidates when needed so expired top hits do not
consume the requested limit, and the read cache is capped at the next TTL
deadline so a cached result cannot outlive an expiry.

With strict reads enabled, a document is not returned after `_ttl_at`.
History behavior is unchanged.

History and revision APIs are audit surfaces and are not filtered by strict
TTL reads; they continue to expose retained revisions until normal history
retention or `purge_history` removes them. Use `purge_history` when revisions
must not outlive the document.

## Vault collections

Collection defaults and `_ttl` work for vault collections. The stored envelope
is deleted without decryption, so the sweeper does not need the tenant key.
TTL metadata contains only the collection name, `_id`, and expiry timestamp.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `NONODB_TTL_STRICT_READS` | `1` | Hide expired documents at read time; `0` restores eventual visibility |
| `NONODB_TTL_SWEEP_INTERVAL` | `5` | Seconds between TTL sweeps |
| `NONODB_TTL_SWEEP_LIMIT` | `256` | Maximum due documents processed per sweep |
| `NONODB_MAINTENANCE_INTERVAL_MS` | `1000` | Maintenance loop heartbeat in milliseconds |

Metrics expose `ttl_sweeps_total`, `ttl_expired_total`,
`ttl_sweep_errors_total`, and the last due backlog as `ttl_backlog` on
`/metrics`.

## Interaction with snapshots and backups

TTL timestamps are stored in the same LMDB environment as documents and are
included in snapshots and backups. Restoring an older snapshot restores its TTL
entries as well. Documents whose expiry passed during the gap are removed by the
next maintenance sweep after the daemon starts. See
[Backup and restore](backup.md).

## Limits

- Strict visibility is immediate.
- The background sweeper processes bounded batches; this does not affect read
  visibility.
- `NONODB_TTL_STRICT_READS=0` restores the legacy behavior where reads may
  return a document briefly after expiry.
- Date-field TTL depends on a valid date in the configured field; other values
  never expire.
- TTL is not a substitute for access control or retention compliance. Use normal
  deletes when immediate physical removal is required.
