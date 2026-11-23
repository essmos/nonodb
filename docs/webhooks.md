# Asynchronous webhooks

NonoDB can deliver change events to an external HTTP endpoint without running
user code in the writer. A webhook rule combines:

- a collection and optional operation list,
- a MongoDB-style event/document filter,
- an optional `count`, `sum`, or `avg` threshold,
- delivery settings such as retries, HMAC signing, headers, and cooldown.

The writer only appends the normal change event. The webhook evaluator reads the
outbox asynchronously, maintains simple aggregate state, and creates durable
delivery records. HTTP requests are made by a background worker after the
database transaction has committed.

```text
write -> writer transaction -> _events
                                  |
                                  v
                         async webhook evaluator
                         filter + count/sum/avg state
                                  |
                                  v
                         _webhook_deliveries
                                  |
                                  v
                         async HTTP delivery
                         retry / HMAC / dead-letter
```

This means a slow or broken endpoint cannot block the single writer.

## Create a rule

```json
{
  "op": "create_webhook",
  "name": "orders-paid",
  "coll": "orders",
  "url": "https://example.test/hooks/orders",
  "enabled": true,
  "ops": ["insert", "update"],
  "filter": {"status": "paid"},
  "secret_env": "ORDER_WEBHOOK_SECRET",
  "max_attempts": 8,
  "timeout_ms": 10000,
  "cooldown_seconds": 0
}
```

`filter` is evaluated by the asynchronous evaluator against the `after`
document for `insert`/`update` and the `before` document for `delete`. It is
never evaluated in the writer.

Creating a webhook enables the normal collection outbox for that collection with
no event filter, projections, or before/after suppression. This is required so
the evaluator can see the complete change. Other outbox consumers continue to
work normally.

## Thresholds

A rule can trigger only when a simple aggregate crosses a threshold:

```json
{
  "op": "create_webhook",
  "name": "paid-volume",
  "coll": "orders",
  "url": "https://example.test/hooks/volume",
  "filter": {"status": "paid"},
  "threshold": {
    "aggregate": "sum",
    "field": "total",
    "gte": 10000,
    "filter": {"status": "paid"}
  },
  "once": true,
  "cooldown_seconds": 300
}
```

Supported aggregates:

| Value | Meaning |
|---|---|
| `count` | Number of documents matching `threshold.filter` |
| `sum` | Sum of a numeric field |
| `avg` | Average of a numeric field |

Exactly one comparison is required: `gt`, `gte`, `lt`, `lte`, or `eq`.
If `threshold.filter` is omitted, it inherits the rule's `filter`.

`once: true` is edge-triggered: the rule fires when the condition changes from
false to true, and re-arms when it becomes false. Without `once`, every matching
change above the threshold can fire, bounded by `cooldown_seconds`.

The evaluator maintains aggregate state incrementally from change events. It
does not run an aggregation query for every write. On startup, a new
`sum`/`avg`/`count` rule takes a consistent snapshot of the collection to
bootstrap its state.

Threshold state is persisted in `webhooks-state.json` in the database directory.
The outbox event id is the replay cursor. If an event was committed but the
evaluator has not saved its cursor yet, the event is replayed and delivery is
at-least-once.

## Delivery

Each trigger creates a document in `_webhook_deliveries`:

```json
{
  "_id": "01J...",
  "rule": "orders-paid",
  "url": "https://example.test/hooks/orders",
  "event_id": "01J...",
  "status": "pending",
  "attempts": 0,
  "max_attempts": 8,
  "next_attempt_ms": 0,
  "created_at": 1762800000000,
  "payload": { "...": "event document" }
}
```

The delivery worker:

- sends JSON with `content-type: application/json`,
- sets `Idempotency-Key` to the source event id,
- retries non-2xx responses and network errors with exponential backoff,
- marks a delivery `dead` after `max_attempts`,
- supports manual retry through the API, CLI, and GUI,
- signs the body with HMAC-SHA256 when `secret_env` is configured.

Signature header:

```text
X-NonoDB-Signature: sha256=<hex digest>
```

The secret is read from the named environment variable at delivery time. Secrets
are not stored in the rule document.

Delivery is **at-least-once**. A crash after the HTTP request but before the
state update can produce a duplicate. Consumers must deduplicate on
`Idempotency-Key`.

## API and CLI

```json
{"op":"list_webhooks"}
{"op":"list_webhook_deliveries","status":"dead","limit":100}
{"op":"retry_webhook_delivery","id":"01J..."}
{"op":"delete_webhook","name":"orders-paid"}
```

CLI:

```bash
nonodb-cli webhooks list
nonodb-cli webhooks create orders-paid orders https://example.test/hooks/orders \
  --filter '{"status":"paid"}' \
  --secret-env ORDER_WEBHOOK_SECRET

nonodb-cli webhooks deliveries dead
nonodb-cli webhooks retry 01J...
nonodb-cli webhooks delete orders-paid
```

## GUI

Open **Webhooks** in the console sidebar to:

- create and edit rules,
- edit the event filter and threshold JSON,
- enable or disable a rule,
- inspect delivery status and errors,
- manually retry a delivery.

## Semantics and limits

- **Asynchronous by design.** Webhook filters and aggregates never run in the
  writer.
- **At-least-once, not exactly-once.** Make endpoints idempotent.
- **Only simple aggregates.** `count`, `sum`, and `avg`; no arbitrary pipelines,
  joins, or user code.
- **One collection per rule.** Use separate rules for multiple collections.
- **Rules are per database.** A rule belongs to the database selected by the
  API/CLI/GUI connection.
- **Primary only.** The worker runs only while this node is writable, so replicas
  do not send duplicate deliveries.
- **No authentication delegation.** NonoDB sends the configured headers and HMAC
  signature; the receiving service must authorize the request.
- **URL safety.** Only `http` and `https` URLs are accepted. The daemon should
  be deployed with network egress controls appropriate for your environment.
- **State file.** `webhooks-state.json` is local to the database directory. It is
  not part of `data.mdb`; if it is lost, threshold and delivery cursor state is
  rebuilt from the current collection/outbox as far as possible.

## Integration examples

### Python receiver

```python
import hashlib
import hmac
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.environ["ORDER_WEBHOOK_SECRET"]

class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"]))
        expected = "sha256=" + hmac.new(
            SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(
            self.headers.get("x-nonodb-signature", ""), expected
        ):
            self.send_response(401)
            self.end_headers()
            return
        # Deduplicate on Idempotency-Key before applying side effects.
        print(self.headers["idempotency-key"], body)
        self.send_response(204)
        self.end_headers()
```

### CI smoke test

```bash
# Start a daemon, create a rule against a local test server, write documents,
# and assert that the expected POSTs arrived.
NONODB_DAEMON=target/debug/nonodb-daemon \
NONODB_CLI=target/debug/nonodb-cli \
python3 scripts/webhook_test.py
```

The test server checks the HMAC signature and idempotency key, returns failures
to exercise retry/dead-letter, and restarts the daemon against the same data
directory to verify durable rules, state, and deliveries.

## Tests

```bash
NONODB_DAEMON=target/debug/nonodb-daemon python3 scripts/webhook_test.py
```

The integration test covers filters, count/sum/avg thresholds, the no-POST-below-
threshold case, `once`, cooldown, HMAC, idempotency keys, request timeouts,
retry/backoff, dead-letter, manual retry, CLI CRUD, and worker restart.
