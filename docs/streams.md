# Streaming outbox events

NonoDB can tail **append-only event collections** in two ways:

- `watch` — a bounded long-poll request over `/v1/exec`;
- `/v1/watch` — a continuous NDJSON or SSE stream.

Both are designed for collections such as `_events`, where documents are
inserted with sortable ULID `_id` values by the outbox producer.

> **Streams:** server-side offsets, consumer groups, and in-flight range leases
> are implemented (see [consumer groups](#consumer-groups-and-durable-offsets)).
> Offset-driven retention is implemented; byte-based retention (`max_bytes`) is
> still planned. Details in the [streams groups plan](streams-groups-plan.md).

The normal mode tails existing documents by `_id`. It is **not** an automatic
change stream for arbitrary collections; updates and deletes are not emitted.
For change notification, write an event document explicitly with
[`EventProducer`](events.md#produce-atomically), or use the connection-scoped
[live mode](#live-subscriptions-connection-scoped).

## Live subscriptions (connection-scoped)

`POST /v1/watch` also supports `mode:"live"`. This does **not** tail `_events`.
It registers an in-memory subscription for the lifetime of the HTTP connection,
evaluates a condition in the writer, and pushes matching changes directly to the
stream.

```json
{
  "op": "watch",
  "coll": "orders",
  "mode": "live",
  "filter": {"status": "paid"},
  "ops": ["insert", "update", "delete"],
  "heartbeat_ms": 10000
}
```

Optional live fields:

| Field | Meaning |
|---|---|
| `filter` | Condition on the after document (`insert`/`update`) or before document (`delete`) |
| `ops` | `insert`/`update`/`delete`; empty means all |
| `count` | Count condition over current collection state, e.g. `{"filter":{"total":{"$gt":100}},"gt":100}` |
| `once` | Optional edge-triggered mode (`false -> true`); default emits every matching write |
| `name` | Subscription name used in the default event type |
| `event_type` | Optional delivered event type; default `subscription.<name>` |
| `heartbeat_ms` | Server heartbeat, default `10,000`, max `10,000` in live mode |
| `timeout_ms` | Optional maximum stream lifetime |

Live events look like ordinary subscription events:

```json
{
  "_id": "01J...",
  "type": "subscription.live",
  "doc_id": "o1",
  "payload": {
    "subscription": "live",
    "coll": "orders",
    "op": "update",
    "before": {"_id": "o1", "status": "new"},
    "after": {"_id": "o1", "status": "paid"},
    "count": null
  }
}
```

Semantics:

- the subscription exists only while the stream is connected;
- closing the stream removes it immediately;
- a janitor removes half-open connections after `30` seconds without a
  successful heartbeat;
- live events are **not written to `_events`**, so they do not grow the database;
- there is no replay: after a disconnect, run a snapshot/`find` and then open a
  new live stream if needed;
- slow consumers are disconnected instead of buffering without bound;
- `after`, `types`, and `threshold` are rejected in live mode;
- vault collections are rejected;
- SQL writes, `find_and_modify`, TTL expiry, and normal writes all trigger live
  evaluation.

Python:

```python
for event in db.collection("orders").watch_live(
    filter={"status": "paid"},
    heartbeat_ms=10_000,
):
    print(event["payload"]["after"])
```

Because the subscription is tied to the connection, `watch_live` does not have a
resume cursor. Reconnect creates a fresh subscription.

## Long-poll request

```json
{
  "op": "watch",
  "coll": "_events",
  "after": "01J...",
  "limit": 100,
  "timeout_ms": 1000,
  "types": ["order.paid"],
  "filter": {"payload.total": {"$gte": 1000}},
  "projection": {"type": 1, "payload": 1}
}
```

| Field | Meaning |
|---|---|
| `coll` | Append-only event collection |
| `after` | Last processed event `_id`; omit to start from the beginning |
| `limit` | Maximum events in this batch, default `100`, max `1,000` |
| `timeout_ms` | Long-poll timeout, default `1,000`, max `25,000` |
| `types` | Optional event `type` allowlist |
| `filter` | Optional MongoDB-style filter on event documents |
| `projection` | Optional projection; `_id` is always retained for resume |

The request is a normal read operation. It requires read access to `coll`, is
not cached, and is not allowed inside a read `batch`. Vault collections are
rejected.

## Long-poll response

```json
{
  "events": [
    {
      "_id": "01J...",
      "type": "order.paid",
      "doc_id": "o1",
      "payload": {"total": 1200},
      "created_at": 1762800000000
    }
  ],
  "next": "01J...",
  "timed_out": false,
  "has_more": false,
  "truncated": false,
  "oldest": null
}
```

| Field | Meaning |
|---|---|
| `events` | Events ordered by `_id` |
| `next` | Resume token for the next request |
| `timed_out` | True when no event arrived before `timeout_ms` |
| `has_more` | True when another page is immediately available |
| `truncated` | True when `after` is older than retained events |
| `oldest` | Oldest retained event id when `truncated` is true |

When `has_more` is true, call again with `after: next`. When it is false, keep the
same cursor and call again; the request will long-poll for new events.

`truncated` means the server could not confirm that `after` still exists. Events
between `after` and `oldest` may have expired or been deleted. A consumer that
needs no gaps should restart from `oldest` after a snapshot/rebuild, not silently
continue.

## Continuous stream

`POST /v1/watch` uses the same request body and returns either:

- `application/x-ndjson` by default — one JSON object per line;
- `text/event-stream` when the request sends `Accept: text/event-stream`.

```bash
curl --unix-socket /run/nonodb/nonodb.sock \
  -H 'content-type: application/json' \
  -H 'accept: application/x-ndjson' \
  -d '{"op":"watch","coll":"_events","after":"01J..."}' \
  http://localhost/v1/watch
```

NDJSON frames:

```text
{"stream":{"truncated":false,"oldest":null}}
{"_id":"01J...","type":"order.paid","payload":{"total":1200}}
{"heartbeat":true,"ts_ms":1762800001000}
```

SSE frames:

```text
event: ready
data: {"truncated":false,"oldest":null}

event: message
id: 01J...
data: {"_id":"01J...","type":"order.paid","payload":{"total":1200}}

event: heartbeat
data: {"heartbeat":true,"ts_ms":1762800001000}
```

Stream fields:

| Field | Meaning |
|---|---|
| `heartbeat_ms` | Keepalive interval, default `15,000`, range `100..60,000` |
| `timeout_ms` | Optional maximum stream lifetime; `0`/absent means no limit |
| `overlap_ms` | Late-commit rescan window, default `2,000`, max `60,000`; `0` disables |
| `filter` | Optional MongoDB-style filter |
| `projection` | Optional projection; `_id` is always retained |
| `limit` | Batch size per internal poll, default `100`, max `1,000` |

Heartbeats keep idle connections alive; they are not events. The stream stays
open until the client disconnects, the maximum lifetime expires, or the daemon
shuts down.

Concurrent producers may commit a lower `_id` after a higher one has already
been delivered. The stream rescans the last `overlap_ms` window, suppresses
already-delivered ids in memory, and can therefore deliver a lower id after a
higher id. Event order is not a global commit order; delivery remains
at-least-once. The bounded long-poll `watch` does not do this rescan — use the
continuous stream or the Python `EventConsumer` when producers run concurrently.

The response is not cached and not audited as an ordinary request-sized
operation; audit logs record `watch_stream` when the stream closes.

## Python

```python
from nonodb import NonoDB, StreamTruncatedError

db = NonoDB()
events = db.collection("_events")

# One batch, useful for manual checkpointing.
batch = events.watch_once(
    after=None,
    limit=100,
    timeout_ms=1000,
    types=["order.paid"],
    filter={"payload.total": {"$gte": 1000}},
    projection={"type": 1, "payload": 1},
)
for event in batch["events"]:
    process(event)  # must be idempotent
cursor = batch["next"]

# Infinite long-polling generator with reconnects.
for event in events.watch(after=cursor, timeout_ms=1000, retry=True):
    process(event)
    if should_stop:
        break

# Continuous NDJSON stream with reconnects.
try:
    for event in events.watch_stream(
        after=cursor,
        heartbeat_ms=15000,
        overlap_ms=2000,
        filter={"type": "order.paid"},
        retry=True,
    ):
        process(event)
except StreamTruncatedError as exc:
    # Rebuild from exc.oldest or take a fresh snapshot.
    raise
```

`watch_once` returns the full response object. `watch` and `watch_stream` yield
event dictionaries, advance their local cursor, and reconnect after transport
errors. Persist your own checkpoint after successful processing. Both generators
raise `StreamTruncatedError` when the server reports a gap.

### Async streaming

`AsyncNonoDB` exposes async versions of the same watch helpers for FastAPI and
other asyncio applications. It is intentionally limited to the watch/stream
surface; use the synchronous `NonoDB` for CRUD and index operations.

```python
from nonodb import AsyncNonoDB

async def tail_events(cursor=None):
    async with AsyncNonoDB("/run/nonodb/nonodb.sock") as db:
        events = db.collection("_events")
        async for event in events.watch(after=cursor):
            process(event)

# watch_once(...) is awaitable:
#   batch = await events.watch_once(after=cursor)
# watch_stream(...) and watch_live(...) are async iterators.
```

The async client reuses the same request payloads, retry/backoff rules, cursor
advancement, and `StreamTruncatedError` handling as the synchronous client. Do
not iterate the blocking `NonoDB` watch generators directly inside `async def`;
use `AsyncNonoDB` instead.

## Rust

```rust
use nonodb_client::{Client, Error};
use serde_json::json;

let db = Client::from_env()?;
let events = db.collection("_events");
let batch = events.watch_with(
    None,
    100,
    1000,
    &["order.paid"],
    Some(&json!({"payload.total": {"$gte": 1000}})),
    Some(&json!({"type": 1, "payload": 1})),
)?;
for event in &batch.events {
    process(event)?;
}
if batch.truncated {
    eprintln!("gap before {:?}", batch.oldest);
}
let next = batch.next;
```

The Rust client exposes the long-poll API. For a continuous stream, POST the same
body to `/v1/watch` and read NDJSON lines from the response.

## Node.js

```js
const { NonoDB, NonoDBError } = require('./client/node');

const db = new NonoDB();
const events = db.collection('_events');

const batch = await events.watchOnce(null, {
  limit: 100,
  timeoutMs: 1000,
  types: ['order.paid'],
  filter: { 'payload.total': { $gte: 1000 } },
  projection: { type: 1, payload: 1 },
});
for (const event of batch.events) await process(event);
const next = batch.next;

// Async generator with reconnect and truncation detection.
try {
  for await (const event of events.watch(next, { timeoutMs: 1000 })) {
    await process(event);
  }
} catch (error) {
  if (error instanceof NonoDBError && error.code === 'stream_truncated') {
    console.error('gap before', error.oldest);
  }
  throw error;
}
```

## Go

```go
package main

import (
    "context"
    "fmt"

    nonodb "github.com/essmos/nonodb/client/go"
)

func main() {
    ctx := context.Background()
    db, _ := nonodb.New()
    events := db.Collection("_events")

    batch, err := events.Watch(ctx, nonodb.WatchOptions{
        Limit:     100,
        TimeoutMs: 1000,
        Types:     []string{"order.paid"},
        Filter:    map[string]any{"payload.total": map[string]any{"$gte": 1000}},
        Projection: map[string]any{"type": 1, "payload": 1},
    })
    if err != nil {
        panic(err)
    }
    if batch.Truncated {
        fmt.Printf("gap before %s\n", batch.Oldest)
    }
    for _, event := range batch.Events {
        fmt.Println(event["_id"], event["type"])
    }
    _ = batch.Next
}
```

The Go client exposes the long-poll API. For a continuous stream, POST the same
body to `/v1/watch` and decode NDJSON lines with `json.Decoder`.

## Checkpoints, delivery, and retention

`watch` and `/v1/watch` are transport-level streams, not durable consumers. They
do **not** store a checkpoint. The application must persist the last processed
`_id` after successful processing, just like the
[outbox consumer](events.md#consume-with-checkpoints).

Delivery is at-least-once:

- a crash after processing but before saving the cursor replays the batch;
- handlers must be idempotent;
- a client restart should resume from its stored cursor;
- replaying retained events is safe for idempotent handlers.

Apply TTL to the event collection so history does not grow forever:

```python
db.create_collection("_events", ttl_seconds=7 * 24 * 3600)
# If it already exists:
db.set_collection_ttl("_events", 7 * 24 * 3600)
```

Choose retention longer than the maximum consumer outage. An expired event
cannot be replayed; `watch` reports that condition as `truncated`.

For the higher-level Python consumer with an overlap rescan for late-committed
lower ids, use `EventConsumer` from `nonodb_worker`.

## Event thresholds

The continuous `/v1/watch` stream can collapse many matching events into one
threshold event. This keeps ordinary events in the outbox but reduces the number
of frames sent to the consumer.

```json
{
  "op": "watch",
  "coll": "_events",
  "types": ["orders.insert"],
  "threshold": {
    "count": 100,
    "window_ms": 60000,
    "group_by": ["payload.tenant"]
  }
}
```

Semantics:

- `count` must be positive; `threshold` is ignored when it is `0` or absent.
- Events are counted per `group_by` key and per `window_ms` window.
- When a group reaches `count`, the triggering event is sent with a
  `threshold` field and all later matching events in that window are suppressed.
- The window resets after `window_ms`, so a later window can emit again.
- `group_by` paths are read from the event document; missing values group as
  `null`.
- Threshold state is in memory for one stream connection. Reconnecting starts a
  new window. Delivery remains at-least-once and is not a replacement for
  durable business counters.
- Long-poll `watch` does not support `threshold`; use `/v1/watch`.

```python
for event in events.watch_stream(
    types=["orders.insert"],
    threshold={"count": 100, "window_ms": 60_000, "group_by": ["payload.tenant"]},
):
    print(event["threshold"], event["payload"])
```

## Metrics

`/metrics` exposes stream counters:

| Metric | Meaning |
|---|---|
| `watch_streams_total` | Continuous `/v1/watch` connections opened |
| `watch_active` | Currently open continuous streams |
| `watch_events_total` | Events delivered by long-poll and streams |
| `watch_timeouts_total` | Long-poll requests that returned no event |
| `watch_truncated_total` | Watch requests with an expired/unknown resume cursor |
| `watch_heartbeats_total` | Keepalive frames sent |
| `watch_threshold_emitted_total` | Threshold events sent |
| `watch_threshold_suppressed_total` | Events suppressed by thresholds |
| `collection_events_emitted_total` | Automatic collection events written |
| `collection_events_suppressed_total` | Events suppressed by source-side filters |

## Validation

```bash
python3 workers/python/tests/events_integration.py
```

The integration test covers the first batch, empty long-poll timeout, an event
arriving during a long poll, type filtering, payload filtering, projection,
pagination with `has_more`, truncation detection, the NDJSON and SSE streams,
the Python generators, and concurrent stream delivery (6 producers × 100 events,
6 independent consumers, no missing or duplicate events).

For a larger repeatable run:

```bash
PRODUCERS=16 PER=1000 CONSUMERS=16 \
  python3 workers/python/tests/stream_stress.py
```

The latest stress run delivered 16,000 events to 16 independent stream
consumers, with 0 missing and 0 duplicate deliveries. `watch_active` returned to
0 after the consumers disconnected.

## Consumer groups and durable offsets

A **stream** turns an append-only collection into an ordered log with
server-side offsets. Consumers commit offsets to the daemon instead of keeping
them client-side, and several processes can share the work of one group.

In this release the stream name is the collection name, and groups are per
database.

### Define a stream

```json
{"op":"create_stream","coll":"_events","retention":{"max_events":1000000,"max_age_ms":604800000}}
```

From then on, every document inserted into `_events` gets a writer-assigned
`_seq` field: a strictly increasing `u64` allocated by the single writer, so it
matches the commit order of the database exactly. `_seq` is reserved — an insert
that supplies it, and any update, pipeline, or find-and-modify that would change
or remove it, is rejected with `invalid_query`.

Events written **before** `create_stream` have no `_seq` and are not delivered
by group reads. Define the stream before writing events, or backfill them.
Automatic change events (collection event configs and subscriptions, which the
daemon writes into `_events`) go through the same writer path and get offsets
too, so a group over `_events` sees both explicit and automatic events in one
order.

If the collection does not exist yet, `create_stream` creates it as a standard
collection. Only standard collections can back a stream: time-series buckets and
vault collections do not carry `_seq`, so the request is rejected instead of
silently delivering nothing.

`create_stream` also builds the `_seq` index used by group reads, so it behaves
like an index build on a large collection.

### Create a group

```json
{"op":"create_stream_group","coll":"_events","group":"order-sync","start":"latest"}
```

`start` is `latest` (default, only new events), `earliest` (offset `0`), or
`{"seq": 1234}`. Creating the same group twice fails with `duplicate_key`.

### Read as a member

```json
{
  "op": "watch",
  "coll": "_events",
  "mode": "group",
  "group": "order-sync",
  "member": "worker-1",
  "limit": 100,
  "lease_ms": 60000,
  "timeout_ms": 1000,
  "filter": {"payload.tenant": "acme"}
}
```

Response:

```json
{
  "events": [{"_id": "01J...", "_seq": 412901, "type": "order.paid"}],
  "group": "order-sync",
  "member": "worker-1",
  "lease_id": "01J...",
  "from_seq": 412901,
  "to_seq": 413000,
  "next_seq": 413000,
  "attempts": 1,
  "max_attempts": 5,
  "timed_out": false,
  "has_more": false
}
```

Group reads order by `_seq`, not `_id`, so out-of-order `_id` commits cannot
reorder events, and the late-commit overlap rescan used by `_id` tails is not
needed: there are no gaps. `filter`, `types`, and `projection` behave as in the
`_id` tail.

The filter is part of the member's subscription. If a claimed range contains no
events matching it, the server **acknowledges that range** and keeps polling: the
group advances, and the batch is not handed back (and eventually dead-lettered)
just because one member filters it out.

Group reads need the `consume:<coll>` scope (or `admin:<coll>`, which implies
it); `read:<coll>` is enough to tail a stream with the default mode but **not**
to claim or acknowledge batches, because claiming writes a lease and advances the
group. `commit_stream_group` and `nack_stream_group` require the same scope;
creating, resetting, deleting, pruning, and retrying dead letters require
`admin:<coll>`.

Every read **claims a contiguous range** of offsets (`from_seq..=to_seq`,
bounded by `limit`) and writes one lease covering the whole range. Several
members of the same group therefore work in parallel: each gets a distinct range,
and no offset is handed out twice. A member that is caught up gets
`"events": []` with `timed_out: true` and no lease.

Redelivery is bounded: a lease is valid for `lease_ms` (default `60000`, minimum
`1000`). If the member neither commits nor nacks before it expires, the range
becomes available again and the next claim picks it up with
`attempts` incremented. After `max_attempts` (group setting, default `5`, set at
creation) the range is dead-lettered instead of looping. A background janitor
(every 60s, `NONODB_STREAM_PRUNE_INTERVAL=0` disables it) also requeues or
dead-letters expired batches even when no member is claiming, so a crashed
worker shows up in `stream_stats` and `stream_dead` without waiting for new
traffic. The janitor goes through the writer (so its committed-offset advances
and dead letters replicate) and runs only on the primary; replicas never
dead-letter a batch on their own.

### Commit and nack

```json
{"op":"commit_stream_group","coll":"_events","group":"order-sync","seq":413000,"lease_id":"01J..."}
{"op":"nack_stream_group","coll":"_events","group":"order-sync","lease_id":"01J...","retry_in_ms":5000}
```

- A commit must present the `lease_id` of the batch, otherwise it fails with
  `stale_lease` (HTTP 409). A worker that lost its lease cannot advance the
  group; `commit_out_of_order` is also 409 and `stream_truncated` is 410.
- The acknowledged `seq` must be the **end** of the leased range, otherwise it
  fails with `commit_out_of_order`; partial, out-of-range, or repeated commits
  never move the offset and leave the batch leased, so the member can finish it.
- The committed offset only advances across a **contiguous** prefix. If member A
  finishes range `11..=20` while member B is still working on `1..=10`, the group
  offset stays at `0` until B commits; then it jumps to `20` in one step.
- `nack_stream_group` releases the range immediately (or after `retry_in_ms`) and
  keeps the attempt counter. Nacking with attempts exhausted dead-letters the
  range.
- Delivery is at-least-once: a crash between processing and committing replays
  the range. Handlers must be idempotent.

### Dead letters

Ranges that exhausted their attempts are removed from the queue, kept for
inspection, and **do not block** the group: the committed offset advances across
them.

```json
{"op":"stream_dead","coll":"_events","group":"order-sync"}
{"op":"retry_stream_dead","coll":"_events","group":"order-sync","from_seq":413001}
{"op":"drop_stream_dead","coll":"_events","group":"order-sync","from_seq":413001}
```

`stream_dead` requires read scope; `retry_stream_dead` and `drop_stream_dead`
require `admin:<coll>`. Retrying resets the attempt counter and puts the range
back in the queue. If the group already moved past the retried range, the replay
is acknowledged without moving the offset backwards, so following the normal
"nack on failure, retry the dead letter" flow always terminates. Deleting a group (`delete_stream_group`) also deletes its
leases and dead letters, so a group recreated with the same name starts clean.

### Inspect and reset

```json
{"op":"stream_stats","coll":"_events"}
{"op":"reset_stream_group","coll":"_events","group":"order-sync","to":"latest"}
{"op":"delete_stream_group","coll":"_events","group":"order-sync"}
{"op":"drop_stream","coll":"_events"}
```

`stream_stats` reports `committed_seq`, `next_seq`, `head_seq`, `lag`, `inflight`
(events leased but not committed), `dead` (dead-lettered **events**), `dead_ranges`
(number of dead-lettered batches), `redeliveries`, `truncations`, `max_attempts`,
and `truncated_below`. `next_seq` is derived from the committed offset plus the
leases known to this node, so after a failover it reports what the next claim will
actually deliver.
`reset_stream_group` accepts `earliest`, `latest`, or `{"seq": n}`, drops the
in-flight batches and dead letters of that group, and clears `truncated_below`.
`drop_stream` keeps the data and the groups; only the definition (and future
offset assignment) is removed.

### Retention

```json
{"op":"create_stream","coll":"_events","retention":{"max_events":1000000,"max_age_ms":604800000,"min_seq_keep":10000}}
```

A stream is pruned by the **lowest committed offset** among its groups, bounded
by the other retention settings:

```
drop everything at or below  min(
    head - max_events,
    events older than max_age_ms,
    slowest group's committed_seq - min_seq_keep,
)
```

Consequences worth knowing:

- A group that stops committing **pins retention**: nothing at or above its
  offset is dropped. A paused consumer is a disk-growth decision, so alert on
  `nonodb_stream_lag_events` rather than letting the log grow silently.
- `min_seq_keep` leaves a safety margin of events behind the slowest consumer;
  setting it explicitly requires it to be at most the current lag.
- A group created behind the retained window (or one that fell behind) fails its
  next claim with `stream_truncated` instead of silently skipping events, and
  records `truncated_below` in `stream_stats`. `reset_stream_group` with
  `"earliest"` resumes at the oldest surviving event.
- `max_bytes` is **not** part of the rule yet; it is validated and stored but
  ignored (`max_events` and `max_age_ms` are the enforced bounds).
- The age boundary is read from each event's numeric `created_at`; when it is
  missing, the timestamp embedded in a 26-character ULID `_id` is used (that is
  the shape [EventProducer](events.md) writes). An event with **neither** is
  "unknown age", not "very old": it stops the boundary, so nothing at or after it
  is pruned by age. If you write plain documents into a stream collection, set
  `created_at` or rely on `max_events`/`min_seq_keep` instead.
- The boundary only covers a **contiguous prefix**: the scan walks `_seq` upwards
  and stops at the first event that is not provably old. Out-of-order
  timestamps therefore delay pruning instead of deleting a fresh event, and the
  cost is bounded by one prune batch rather than the stream length.
- Pruning runs through the writer, so deletions replicate and the stream stays
  consistent on replicas. It removes up to `NONODB_STREAM_PRUNE_BATCH` events
  per pass; the pass repeats while it keeps finding events to drop.

Retention and the lease janitor run in the background by default (every 60s).
Tune or disable them with environment variables; a single pass can also be
triggered by hand:

```
NONODB_STREAM_PRUNE_INTERVAL=60 NONODB_STREAM_PRUNE_BATCH=1000 nonodb-daemon ...
```

```json
{"op":"prune_stream","coll":"_events","batch":1000}
```

A quiet stream costs one small index scan per pass, bounded by
`NONODB_STREAM_PRUNE_BATCH`. Dropped events are counted in
`nonodb_stream_pruned_events_total`; `nonodb_stream_truncated_total` counts the
groups that retention crossed.

### Metrics

`/metrics/prometheus` exposes per-group stream gauges and counters:

| Metric | Meaning |
|---|---|
| `nonodb_stream_committed_seq{database,stream,group}` | group offset |
| `nonodb_stream_lag_events{database,stream,group}` | head minus committed |
| `nonodb_stream_inflight_events{database,stream,group}` | leased events |
| `nonodb_stream_dead_events{database,stream,group}` | dead-lettered events |
| `nonodb_stream_redeliveries_total{database,stream,group}` | ranges delivered again |
| `nonodb_stream_truncated_total{database,stream,group}` | retention crossed the group |
| `nonodb_stream_commit_failures_total{database,stream,group}` | rejected commits |
| `nonodb_stream_claims_total` | batches claimed |
| `nonodb_stream_lease_recoveries_total` | expired leases requeued or dead-lettered by the janitor |
| `nonodb_stream_nacks_total` | batches released with nack |
| `nonodb_stream_dead_letters_total` | events dead-lettered by a nack |
| `nonodb_stream_pruned_events_total` | events dropped by retention |

### Current limits

- `retention.max_bytes` is validated but ignored; use `max_events` and
  `max_age_ms` instead. The
  [streams groups plan](streams-groups-plan.md) has the remaining phase.
- Leases are node-local ownership and are not replicated. The claim position is
  derived from `committed_seq` plus the leases on the current node, never from
  the replicated `next_seq`, so a promoted replica resumes at
  `committed_seq + 1` and **redelivers** ranges the old primary had claimed but
  not committed (at-least-once, never silent loss). A node that takes over a
  group with a copied data directory can call `clear_stream_leases` to drop
  stale ownership.
- A commit must cover the whole leased range. Partial acknowledgements inside a
  range are rejected rather than split.

## Use cases

- [Financial ledger and payment processing](use-cases/financial-ledger.md) —
  per-account streams, atomic double-entry writes, strict per-account execution,
  idempotent handlers, dead-letter handling, reconciliation.
- [Replacing Redis Streams](use-cases/replacing-redis-streams.md) — concept
  mapping, semantics that change your consumer code, migration plan.
- [Replacing Kafka](use-cases/replacing-kafka.md) — topics/partitions/groups as
  meta-mapping, atomic multi-stream publish, and when to keep Kafka.
