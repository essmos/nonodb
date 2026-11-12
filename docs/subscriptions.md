# Conditional subscriptions

Conditional subscriptions are persistent, server-side rules attached to one
collection. They evaluate a **condition over collection data** on every write.
They are not event-count thresholds and they do not run user code.

Example: notify me whenever an `orders` document becomes `paid`, or whenever
there are more than 100 high-value orders in the collection.

```text
client write ──▶ writer transaction
                    ├── write document
                    ├── evaluate subscriptions in Rust
                    └── append matching event to _events
```

The subscription event is written in the same LMDB transaction as the data
change. Delivery uses the normal outbox stream, `watch`, or `/v1/watch`.

## Create a subscription

```json
{
  "op": "create_subscription",
  "coll": "orders",
  "name": "paid",
  "ops": ["insert", "update", "delete"],
  "filter": {"status": "paid"},
  "lease_seconds": 600
}
```

`create_subscription` creates or replaces the named subscription and requires
`admin:orders`. `list_subscriptions` requires read access. `delete_subscription`
requires `admin:orders`.

Fields:

| Field | Meaning |
|---|---|
| `coll` | Collection to observe |
| `name` | Unique subscription name within the collection |
| `ops` | `insert`, `update`, `delete`; empty means all three |
| `filter` | MongoDB-style condition on the written document |
| `count` | Optional condition on a count over collection state |
| `once` | Optional flag; see [Emission modes](#emission-modes) |
| `event_type` | Optional delivered event type; default `subscription.<name>` |
| `lease_seconds` | Client lease in seconds, default `600`; the client must renew it |

### Document condition

`filter` is matched against the **after** document for `insert`/`update` and
against the **before** document for `delete`.

```json
{
  "op": "create_subscription",
  "coll": "orders",
  "name": "big_paid",
  "filter": {"status": "paid", "total": {"$gte": 1000}}
}
```

When a write satisfies the condition, the writer emits an event. By default
**every matching write emits**, with no extra flag:

```python
db.create_subscription("orders", "paid", filter={"status": "paid"})
orders.update({"_id": "o1"}, {"$set": {"total": 11}})   # emits if already paid
orders.update({"_id": "o1"}, {"$set": {"total": 12}})   # emits again
```

### Count condition

`count` evaluates a count over current collection state after the write. It is
index-backed through the normal query planner.

```json
{
  "op": "create_subscription",
  "coll": "orders",
  "name": "many_high_value",
  "count": {
    "filter": {"total": {"$gt": 100}},
    "gt": 100
  }
}
```

This emits when the collection contains **more than 100 documents** matching
`{"total": {"$gt": 100}}` after the write. Supported comparisons are `gt`,
`gte`, `lt`, `lte`, and `eq`. If none is supplied, the default is `gt: 0`
(any matching document).

`filter` and `count` may be combined; both must match. The count is evaluated
for every write to the collection, including writes that do not touch the
matching documents, because the count is a state condition.

## Client lease and expiry

Persistent subscriptions are not eternal. Each subscription has a lease
(`lease_seconds`, default **600 seconds / 10 minutes**). The client must renew it
before it expires:

```json
{"op":"renew_subscription","coll":"orders","name":"paid","lease_seconds":600}
```

A typical keepalive runs every 5 minutes for a 10-minute lease.

- `create_subscription` sets `expires_at = now + lease_seconds`.
- `renew_subscription` extends `expires_at`.
- The writer checks `expires_at` **before** evaluating `filter` or `count`.
- An expired subscription performs no queries and emits no events.
- The writer removes expired subscriptions on the next write to the collection.
- A maintenance sweep also deletes expired subscriptions every 60 seconds, even
  when the collection is idle.
- `renew_subscription` returns `{"subscription": null}` if the subscription is
  already gone.

Expiry deletes the **subscription**, not events already stored in `_events`.
Outbox retention is separate; use a TTL or checkpoint-based cleanup for
`_events`.

Python:

```python
db.create_subscription(
    "orders",
    "paid",
    filter={"status": "paid"},
    lease_seconds=600,
)

# Keepalive task: renew every 5 minutes.
db.renew_subscription("orders", "paid", lease_seconds=600)
```

Rust:

```rust
client.renew_subscription("orders", "paid", Some(600))?;
```

Node:

```js
await db.renewSubscription('orders', 'paid', 600);
```

Go:

```go
lease := uint64(600)
_, err := client.RenewSubscription(ctx, "orders", "paid", &lease)
```

CLI:

```bash
nonodb-cli subscribe orders paid --filter '{"status":"paid"}' --lease 600
nonodb-cli subscription-ping orders paid 600
```

## Emission modes

Default: **level-triggered by condition**, no flag required. Every write that
satisfies the condition emits an event.

Optional: send `once: true` to make the subscription **edge-triggered**. It emits
only on a transition from `false` to `true`. Further matching writes do not emit
until the condition becomes false again.

```python
# Emit only when crossing from <=100 to >100.
db.create_subscription(
    "orders",
    "crossed_high_value",
    count={"filter": {"total": {"$gt": 100}}, "gt": 100},
    once=True,
)
```

`last_matched` is persisted with the subscription and survives a restart. For
`count`, the edge state is the combined result of `filter` and `count`.

## Event shape

```json
{
  "_id": "01J...",
  "type": "subscription.paid",
  "doc_id": "o1",
  "payload": {
    "subscription": "paid",
    "coll": "orders",
    "op": "update",
    "before": {"_id": "o1", "status": "new", "total": 10},
    "after": {"_id": "o1", "status": "paid", "total": 10},
    "count": null
  },
  "created_at": 1762800000000
}
```

For a count subscription, `payload.count` contains the evaluated count. The
event remains an ordinary document in `_events`, so existing consumers,
checkpoints, filters, projections, long-poll `watch`, and `/v1/watch` work
unchanged.

## API

```json
{"op":"create_subscription","coll":"orders","name":"paid","filter":{"status":"paid"},"lease_seconds":600}
{"op":"renew_subscription","coll":"orders","name":"paid","lease_seconds":600}
{"op":"list_subscriptions","coll":"orders"}
{"op":"delete_subscription","coll":"orders","name":"paid"}
```

`create_subscription` returns the normalized subscription with `lease_seconds`
and `expires_at`. `renew_subscription` returns the renewed subscription or
`null`. `delete_subscription` returns `{"deleted": true|false}`.
`list_subscriptions` returns:

```json
{"subscriptions": [{"name": "paid", "ops": [], "filter": {"status": "paid"}, "count": null, "once": false, "last_matched": false, "lease_seconds": 600, "expires_at": 1762800600000}]}
```

## Clients

Python:

```python
db.create_subscription("orders", "paid", filter={"status": "paid"})
db.create_subscription(
    "orders",
    "high_once",
    filter={"total": {"$gt": 100}},
    once=True,
)
db.create_subscription(
    "orders",
    "many",
    count={"filter": {"total": {"$gt": 100}}, "gt": 100},
)
print(db.list_subscriptions("orders"))
db.delete_subscription("orders", "paid")
```

Rust:

```rust
client.create_subscription(
    "orders",
    serde_json::json!({"name": "paid", "filter": {"status": "paid"}}),
)?;
let subscriptions = client.list_subscriptions("orders")?;
client.delete_subscription("orders", "paid")?;
```

Node:

```js
await db.createSubscription('orders', {
  name: 'paid',
  filter: { status: 'paid' },
});
console.log(await db.listSubscriptions('orders'));
await db.deleteSubscription('orders', 'paid');
```

Go:

```go
_, err := client.CreateSubscription(ctx, "orders", map[string]any{
    "name": "paid", "filter": map[string]any{"status": "paid"},
})
subscriptions, err := client.ListSubscriptions(ctx, "orders")
deleted, err := client.DeleteSubscription(ctx, "orders", "paid")
```

## Consumption

Subscribe to the generated event type through the normal outbox stream:

```python
for event in db.collection("_events").watch_stream(
    types=["subscription.paid"],
    filter={"payload.after.total": {"$gte": 1000}},
):
    handle(event)  # idempotent; delivery is at-least-once
```

## Connection-scoped live mode

Persistent subscriptions above write to `_events` and are intended for durable,
replayable consumers. For UI notifications and short-lived consumers, use live
mode instead:

```json
{
  "op": "watch",
  "coll": "orders",
  "mode": "live",
  "filter": {"status": "paid"}
}
```

Live mode:

- registers an in-memory subscription when the SSE/NDJSON stream opens;
- evaluates the same `filter`, `count`, `ops`, and `once` semantics in the writer;
- sends matching events directly to the connection, never to `_events`;
- removes the subscription immediately when the stream closes;
- removes half-open connections after 30 seconds without a server heartbeat;
- has no replay and no resume cursor;
- disconnects slow consumers instead of growing an unbounded queue.

Python:

```python
for event in db.collection("orders").watch_live(
    filter={"status": "paid"},
    heartbeat_ms=10_000,
):
    print(event["payload"])
```

Use a persistent `create_subscription` only when you need at-least-once delivery,
checkpoints, or replay after a disconnect.

## Semantics and limits

- Persistent subscriptions are server-side outbox producers; live subscriptions
  are connection-scoped and never write to `_events`.
- This is **not** a general read trigger, stored procedure, or plugin host.
- Conditions are evaluated in Rust by the single writer, inside the same
  transaction as the write. There is no arbitrary user code.
- Counting uses the collection's query planner and indexes. A count condition
  without a suitable index can scan the collection on every write.
- Vault collections and the `_events` collection cannot have subscriptions.
- `once` controls edge vs level emission. It is optional; omit it to emit on
  every matching write.
- The older stream `threshold` option is different: it counts **delivered
  events** per connection and window. Use a `count` subscription when the
  condition is about collection state.
- Delivery is at-least-once. Consumers must be idempotent.
