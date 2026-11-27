# Replacing Redis Streams

Redis Streams is the closest mental model to nonodb streams: an append-only log
per key, consumer groups, a pending list, and `MAXLEN`-style trimming. The
migration is mostly mechanical — the *shapes* match — but four semantics differ
and they change your consumer code.

## Concept mapping

| Redis Streams | nonodb | Notes |
|---|---|---|
| `XADD mystream * field value` | `insert` into the stream collection (or an explicit event doc) | `_seq` is assigned by the writer, in commit order |
| stream key | collection with a stream definition | in this release the stream name is the collection name |
| `XGROUP CREATE s g MKSTREAM` | `create_stream_group` with `start: "earliest" \| "latest"` or `{"seq": n}` | creation also creates the stream? no — define the stream first |
| `XREADGROUP GROUP g c COUNT 100 BLOCK 1000 STREAMS s >` | `watch` with `"mode": "group"`, `limit`, `lease_ms`, `timeout_ms` | long-poll returns as soon as events exist, or `timed_out` |
| `XACK s g id` | `commit_stream_group` with `seq` = **end of the leased range** and `lease_id` | whole-batch acknowledgement, not per message |
| `XPENDING s g` | `stream_leases`, `stream_stats` → `inflight` | in-flight state is a lease, not a PEL entry |
| `XAUTOCLAIM` / `XCLAIM` | automatic: expired leases are redelivered (`attempts + 1`), the janitor runs on the maintenance tick | no manual claim; you can `nack` to release early |
| `XLEN s` | `get_stream` → `head_seq` | monotonic offset, not a count (deleted events leave gaps) |
| `XRANGE s - +` | `find` with `{"_seq": {"$gte": a, "$lte": b}}`, `sort: [["_seq", 1]]` | plain queries, including filters and projections |
| `XTRIM s MAXLEN ~ n` | `retention.max_events` + `prune_stream` (or the background tick) | trimming is bounded by the slowest group offset |
| `XINFO STREAM/GROUPS` | `get_stream`, `stream_stats` | per-group `committed_seq`, `lag`, `inflight`, `dead` |
| `XGROUP DESTROY s g` | `delete_stream_group` | also drops that group's leases and dead letters |
| `XDEL s id` | `delete` with `_id` | leaves a hole in `_seq`; delivery simply skips it |

## Before / after

Redis (pseudo, `redis-py`):

```python
r.xadd("payments", {"id": p.id, "amount": str(p.amount)})

batch = r.xreadgroup("billing", "w1", {"payments": ">"}, count=50, block=1000)
for stream, messages in batch or []:
    for msg_id, fields in messages:
        handle(msg_id, fields)
        r.xack("payments", "billing", msg_id)      # per message
```

nonodb:

```python
call({"op": "insert", "coll": "payments",
      "doc": {"id": p.id, "amount_minor": p.amount}})

batch = call({"op": "watch", "coll": "payments", "mode": "group",
              "group": "billing", "member": "w1",
              "limit": 50, "lease_ms": 30_000, "timeout_ms": 1_000})
if batch["events"]:
    handle_batch(batch["events"])                  # whole batch
    call({"op": "commit_stream_group", "coll": "payments", "group": "billing",
          "seq": batch["to_seq"], "lease_id": batch["lease_id"]})
```

## The four differences that bite

1. **Ack granularity is a contiguous range, not a message id.** `XACK` can
   acknowledge arbitrary ids in any order (that is what a PEL gives you). Here a
   claim is a range, and the commit must name the end of that range; a partial
   acknowledgement fails with `commit_out_of_order` and leaves the batch leased.
   *Design consequence:* make batches the unit of work that is safe to redo.
2. **No pending-entries list; leases with fencing.** A crashed worker's batch is
   reclaimed automatically after `lease_ms`, so tune it to your worst-case
   handler time. A worker that comes back after a reclaim gets `stale_lease` on
   commit instead of silently acking someone else's work.
3. **Retention is pinned by the slowest consumer, not by `MAXLEN` alone.** With
   Redis, `MAXLEN` trims regardless of pending entries (silent loss). Here
   `max_events` is bounded by `min(committed_seq) - min_seq_keep`, so a stopped
   consumer grows the log instead of losing events; if retention does overtake a
   group, the next claim fails with `stream_truncated` and records
   `truncated_below`. Alert on `nonodb_stream_lag_events`.
4. **One stream per request.** There is no blocking read over several streams;
   poll each stream, or use one stream per service and filter server-side with
   `filter`/`types`. Fan-out is served by several *groups* on the same stream.

## What you gain

- **Durability and replication.** The log, the group offsets, and the dead
  letters live in LMDB and are shipped to replicas; Redis Streams with
  `appendonly no` is memory-only, and even AOF does not give you queryable
  history. Leases are node-local: a promoted replica resumes at the committed
  offset and redelivers every range that was claimed but not committed, so
  failover is at-least-once rather than lossy.
- **Queryability.** Streams are ordinary collections: filter, index, aggregate,
  project, and query history with SQL while the log is being consumed.
- **Persistent dead letters.** A poison batch does not block the group and does
  not vanish: `stream_dead`, retry, drop, all durable and replicated, including
  the ones the lease janitor creates on expiry.
- **Atomic data + event.** `write_batch` commits the state change and its event in
  one transaction, which removes the classic "wrote the row, crashed before
  XADD" gap.
- **Observability baked in.** Per-group `committed_seq`, `lag`, `inflight`,
  `dead`, redeliveries, commit failures, retention crossings — as
  `/metrics/prometheus` series.

## What you give up

- **Sub-millisecond latency.** Long-poll delivery was measured at p50 ≈ 1.6 ms
  and p99 ≈ 2.3 ms over UDS on a 4 vCPU test box; Redis is typically lower.
- **Throughput ceiling.** One writer means roughly 40k events/s sustained for
  mixed writes on that box, and ~65–70k events/s draining a stream with a single
  consumer (client-bound in Python). Redis is memory-bound and can exceed this.
- **No Lua / no arbitrary multi-key scripts.** Use `write_batch` for atomicity
  inside one database; anything else is application logic.
- **No built-in pub/sub.** For ephemeral fan-out use
  [conditional subscriptions](../subscriptions.md) or `watch` `mode:"live"`.

## Migration plan

1. Create the collection and the stream; do it before the first insert so every
   event gets `_seq`.
2. Backfill from Redis: `XRANGE` the log streams and insert the documents,
   keeping the original Redis id in a field (for deduplication and audit).
3. Run both paths for a while: producers write to nonodb, consumers read from
   nonodb; compare counts and business invariants.
4. Cut consumers over group by group — offsets are independent, so this can be
   done per consumer.
5. Decommission Redis after the retention window of the old log has passed.

## Checklist

- [ ] `lease_ms` ≥ worst-case handler duration
- [ ] batch size (`limit`) small enough that a replay is cheap
- [ ] idempotency key on the effect of every batch
- [ ] `max_attempts` set, dead-letter alerts routed to on-call
- [ ] retention sized to the longest allowed consumer outage
- [ ] dashboards moved from `XLEN`/`XPENDING` to `nonodb_stream_*`

## Related

- [Streaming outbox events](../streams.md) — full semantics
- [HTTP API — streams](../api.md#streams) — request shapes
- [Financial ledger](financial-ledger.md) — ordered processing and reconciliation
