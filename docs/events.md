# Outbox events

NonoDB does not run user code inside the daemon and does not emit events from
`find` or `aggregate`. Events use the **outbox pattern**: an event document is
written in the same transaction as the data change, then an external consumer
polls or streams the event collection and runs application handlers.

There are two producers:

- an application can write data and an event atomically with `write_batch`;
- or a collection can enable **automatic change events** in the writer. The
  daemon then appends `insert`/`update`/`delete` events for that collection.

Automatic events are also ordinary outbox documents in `_events`, so existing
consumers and `watch` filters work unchanged. This keeps the single-writer LMDB
model and authorization boundaries unchanged. For persistent conditions over collection data, see
[Conditional subscriptions](subscriptions.md).

For durable retrying work, see
[Background jobs](workers.md).

## Model

```text
producer ──write_batch(data + event)──▶ _events ──poll _id > checkpoint──▶ consumer
                                                                          │
                                                                          └── handler
```

- The event and the application write commit together or not at all.
- Consumers poll with a normal indexed `_id` range query.
- Each consumer stores its own checkpoint in a normal collection.
- Delivery is at-least-once. Handlers must be idempotent.
- The daemon has no stored procedures, plugin host, or read hooks.

## Event document

```json
{
  "_id": "01J...",
  "type": "order.paid",
  "doc_id": "o1",
  "payload": {"total": 1200},
  "created_at": 1762800000000
}
```

| Field | Meaning |
|---|---|
| `_id` | ULID-style 26-character sortable event id |
| `type` | Application event type, used to select a handler |
| `doc_id` | Optional related document id |
| `payload` | Application data |
| `created_at` | Producer timestamp in Unix milliseconds |
| `metadata` | Optional application metadata |

Event ids embed a millisecond timestamp and are monotonic within one producer
process. Consumers use them only for ordering and checkpointing.

## Produce atomically

```python
from nonodb import NonoDB
from nonodb_worker import EventProducer

db = NonoDB()
events = db.collection("_events")
producer = EventProducer(events)

event_id = producer.emit(
    "order.paid",
    {"total": 1200},
    doc_id="o1",
    data_ops=[
        {
            "op": "update",
            "coll": "orders",
            "filter": {"_id": "o1"},
            "update": {"$set": {"status": "paid"}},
        }
    ],
)
```

Every `data_ops` entry must include its own `coll`. The event insert is appended
to the same batch, so either all writes commit or none do.

To emit only an event, omit `data_ops`:

```python
producer.emit("cache.invalidate", {"key": "catalog"})
```

## Automatic collection change events

Enable events for one collection:

```json
{
  "op": "set_collection_events",
  "coll": "orders",
  "enabled": true,
  "ops": ["insert", "update"],
  "fields": ["status", "total"],
  "filter": {"payload.after.status": "paid"},
  "include_before": false,
  "include_after": true,
  "projection": {"status": 1, "total": 1}
}
```

- `fields=[]` emits updates for any changed top-level field.
- A non-empty `fields` list limits `update` events to changes in those fields.
- `ops=[]` means all operations; otherwise only the listed
  `insert`/`update`/`delete` operations are emitted.
- `filter` is matched against the complete event document before it is stored.
  Non-matching events never reach `_events`.
- `include_before` / `include_after` control whether the payload contains the
  before/after document.
- `projection` limits which before/after fields are stored in the event.
- With the default `fields=[]` and no filter, every `insert`/`update`/`delete`
  emits when events are enabled.
- The event is written in the same writer transaction as the data change.
- Events are written to `_events`; vault collections are not auto-evented.
- `insert`, `insert_many`, `update`, `delete`, `find_and_modify`, TTL expiry,
  and SQL `INSERT`/`UPDATE`/`DELETE` emit events.
- No global daemon switch is required.

Event shape:

```json
{
  "_id": "01J...",
  "type": "orders.update",
  "doc_id": "o1",
  "payload": {
    "coll": "orders",
    "op": "update",
    "before": {"_id": "o1", "status": "new", "total": 10},
    "after": {"_id": "o1", "status": "paid", "total": 10},
    "changed_fields": ["status"]
  },
  "created_at": 1762800000000
}
```

Read the current setting:

```bash
nonodb-cli events orders
nonodb-cli events orders on status total
nonodb-cli events orders off
```

API:

```json
{"op":"set_collection_events","coll":"orders","enabled":true,"fields":["status"]}
{"op":"get_collection_events","coll":"orders"}
```

`set_collection_events` requires `admin:<collection>`;
`get_collection_events` requires `read:<collection>`.

### Server-side topics and filters

Use event `type` as the topic and `filter` for payload conditions. Filtering is
done in Rust before bytes are sent to the consumer:

```json
{
  "op": "watch",
  "coll": "_events",
  "types": ["orders.insert", "orders.update"],
  "filter": {
    "payload.tenant": "abc",
    "payload.after.status": "paid"
  }
}
```

## Consume with checkpoints

```python
from nonodb_worker import EventConsumer, EventRunner

consumer = EventConsumer(
    db.collection("_events"),
    db.collection("_event_checkpoints"),
    "order-sync",
    overlap_seconds=2.0,
)
runner = EventRunner(consumer)


def on_order_paid(event):
    print(event["doc_id"], event["payload"]["total"])


runner.register("order.paid", on_order_paid)
runner.run_forever(poll_interval=1.0)
```

The checkpoint is a normal document:

```json
{
  "_id": "order-sync",
  "last_id": "01J...",
  "late_cursor": "01J...",
  "updated_at": 1762800000000
}
```

`last_id` is the high-water mark. `late_cursor` is internal state for the
overlap scan; do not edit it manually.

`run_once` advances the checkpoint only through successfully processed events.
If a handler raises, the batch stops before that event and the next poll retries
it. Delivery is therefore at-least-once: handlers must be idempotent.

Unknown event types are ignored by default so an older consumer does not block on
new event types. Construct `EventRunner(..., ignore_unknown=False)` to stop at an
unknown event and require an explicit handler update.

### Manual loop

If you do not want the built-in runner:

```python
while True:
    batch = consumer.fetch(limit=100, types=["order.paid"])
    processed = []
    for event in batch:
        handle_order_paid(event)  # must be idempotent
        processed.append(event)
    consumer.advance(processed)
```

Only call `advance` after the corresponding handler succeeds.

## API reference

### `EventProducer(events)`

| Method | Meaning |
|---|---|
| `emit(type, payload=None, *, doc_id=None, data_ops=None, event_id=None, metadata=None)` | Atomically append an event and optional writes. Returns the event id. |

### `EventConsumer(events, checkpoints, consumer_id, overlap_seconds=2.0, late_scan_limit=1000)`

| Method | Meaning |
|---|---|
| `checkpoint()` | Return the last processed event id, or `""`. |
| `fetch(limit=100, types=None)` | Fetch new events plus unseen events from the overlap window. |
| `advance(events)` | Move the checkpoint through successfully processed events. |
| `reset()` | Delete this consumer's checkpoint and start from the beginning. |

`overlap_seconds` controls the safety window for events whose write commits after
a slightly newer event id. Set it to `0` to disable the rescan. Increase it if
the application can have long write-queue delays. `late_scan_limit` bounds how
many overlap-window events are scanned per poll.

### `EventRunner(consumer, ignore_unknown=True)`

| Method | Meaning |
|---|---|
| `register(type, handler)` | Register a synchronous handler for an event type. |
| `run_once(limit=100, types=None)` | Process one batch; return how many events advanced. |
| `run_forever(poll_interval=1.0, limit=100, types=None)` | Poll and dispatch until stopped. |
| `stop()` | Request a graceful stop. |

Helpers:

- `new_event_id()` — generate a sortable event id.
- `event_timestamp_ms(event_id)` — extract the embedded timestamp.

## Server-side watch

Use the daemon-side [`watch`](streams.md) long-poll or the continuous
`/v1/watch` NDJSON/SSE stream when you want the server to push events to a
consumer. `watch` returns `_id`-ordered batches with a `next` resume token; the
stream adds heartbeats and keeps the connection open. Neither replaces the
checkpointing consumer described below.

```python
batch = db.collection("_events").watch_once(after=None, timeout_ms=1000)
for event in batch["events"]:
    handle(event)
after = batch["next"]
```

## Event worker CLI

```bash
PYTHONPATH=client:workers/python:workers/python/examples \
python3 -m nonodb_worker.events_worker \
  --module events_handlers \
  --events _events \
  --checkpoints _event_checkpoints \
  --consumer-id order-sync
```

The handler module must define either:

```python
def register(runner):
    runner.register("order.paid", on_order_paid)
```

or:

```python
HANDLERS = {
    "order.paid": on_order_paid,
}
```

Example `events_handlers.py`:

```python
def order_created(event):
    payload = event.get("payload", {})
    print(f"order created: {event.get('doc_id')} total={payload.get('total')}")


def register(runner):
    runner.register("order.created", order_created)
```

Useful options:

| Option | Meaning |
|---|---|
| `--once` | Process at most one batch and exit |
| `--limit` | Maximum events per batch, default `100` |
| `--poll-interval` | Idle sleep between polls, default `1.0` second |
| `--reset` | Delete this consumer's checkpoint before starting |
| `--consumer-id` | Independent checkpoint identity |
| `--socket` / `--base-url` / `--token` | Connection overrides |
| `--log-level` | Python logging level, default `INFO` |

Each independent consumer needs a different `--consumer-id` and a different
checkpoint document. Two processes with the same consumer id share one
checkpoint and will interfere with each other.

## systemd

A unit example is included as `deploy/nonodb-events.service`:

```ini
[Service]
Environment=NONODB_SOCK=/run/nonodb/nonodb.sock
Environment=PYTHONPATH=/opt/nonodb/client:/opt/nonodb/workers/python:/opt/nonodb/events
WorkingDirectory=/opt/nonodb
ExecStart=/usr/bin/python3 -m nonodb_worker.events_worker --module events_handlers --consumer-id order-sync
Restart=on-failure
RestartSec=5
```

Install one unit per independent consumer, or override `--consumer-id` and the
handler module for each instance.

## Events to jobs

An event handler can enqueue a durable job when work needs retries, longer
timeouts, or scheduling:

```python
from nonodb_worker import JobQueue

jobs = JobQueue(db.collection("_jobs"))


def on_order_paid(event):
    jobs.enqueue(
        "sync_invoice",
        {"order_id": event["doc_id"]},
        job_id=f"sync_invoice:{event['doc_id']}",
    )
```

Use the event consumer for fast routing and the job queue for retryable or
expensive work. See [Background jobs](workers.md) for the job model.

## Retention and indexes

Apply TTL to the event collection so history does not grow forever:

```python
db.create_collection("_events", ttl_seconds=7 * 24 * 3600)
# If the collection already exists:
db.set_collection_ttl("_events", 7 * 24 * 3600)
```

Choose retention longer than the maximum consumer outage you want to survive.
Events already consumed can expire safely after that window.

The event collection needs no extra index for the default consumer: the `_id`
primary key provides the ordered range scan. If you filter by `type`, the query
still uses the `_id` range and applies the type filter to the fetched batch.

The checkpoint collection also needs no extra index; checkpoints are looked up by
`_id`.

## Delivery and ordering

The helper provides **at-least-once** delivery:

- A consumer can crash after a handler succeeds but before the checkpoint is
  written, so the event is processed again.
- Handlers must be idempotent. Use a business key, an idempotent `job_id`, or a
  processed-event marker in application data.
- A failed handler stops the batch before its event; the next poll retries it.

The checkpoint advances to the highest processed event id. With many concurrent
producers, an atomic write can commit after a slightly newer event id. The
consumer therefore re-scans a short overlap window after those ids have aged past
`overlap_seconds`; a recovered event is delivered late, not lost. The default is
two seconds and can be tuned.

This is a practical safety window, not an unbounded commit-order guarantee. If a
write can be delayed longer than the configured overlap, increase the overlap or
use a future built-in change log in the core. The outbox remains a polling
mechanism, not a replacement for a database-level change stream.

Because the overlap is re-scanned after a restart, events from the last few
seconds may be delivered again. That is expected at-least-once behavior.

## Security

- Give producers write access only to the collections they need, including the
  event collection.
- Give consumers read access to the event collection and write access to their
  checkpoint collection.
- Event payloads are normal documents. Do not store secrets in them if the
  collection is broadly readable.
- Treat event types as an allowlist. Never execute a command string from an
  event payload.
- Vault collections can be used, but the consumer needs the same vault headers
  and keys as the producer.

## Troubleshooting

| Symptom | Check |
|---|---|
| Consumer keeps seeing the same event | Handler is failing; inspect logs, fix it, then restart. |
| Events stop at one event | Unknown type with `ignore_unknown=False`, or a permanent handler error. |
| No events consumed | Check `--events`, `--consumer-id`, token scopes, and checkpoint collection permissions. |
| Events were skipped | The event collection TTL may be shorter than the consumer outage. |
| Duplicate processing after restart | Expected at-least-once behavior; make handlers idempotent. |
| Need to replay history | Use `--reset` or `EventConsumer.reset()`, then expect all retained events to be processed again. |

## Validation

```bash
python3 -m unittest discover -s workers/python/tests -v
PYTHONPATH=client:workers/python NONODB_SOCK=/tmp/nonodb.sock \
  python3 workers/python/examples/events_smoke.py
python3 workers/python/tests/events_integration.py
```

The unit tests cover id generation, producer batching, checkpoints, retries, and
the overlap rescan. The integration test starts a real daemon and covers atomic
rollback/commit, checkpoint restart, handler retry, late out-of-order events,
independent consumers, concurrent producers, and the worker CLI.
