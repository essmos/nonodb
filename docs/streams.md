# Streaming outbox events

NonoDB can tail **append-only event collections** in two ways:

- `watch` — a bounded long-poll request over `/v1/exec`;
- `/v1/watch` — a continuous NDJSON or SSE stream.

Both are designed for collections such as `_events`, where documents are
inserted with sortable ULID `_id` values by the outbox producer.

They tail existing documents by `_id`; they are **not** automatic change streams
for arbitrary collections. Updates and deletes are not emitted. For change
notification, write an event document explicitly with
[`EventProducer`](events.md#produce-atomically).

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
