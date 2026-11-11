# Background jobs and schedulers

NonoDB does not execute stored procedures or load job code into the daemon.
Background work is performed by ordinary client processes that use the same
HTTP/UDS API as the application.

This keeps the single-writer model, LMDB transactions, and authorization
boundaries unchanged. The database stores job documents; a separate worker
claims and executes them. For change notification, see
[Outbox events](events.md).

## Model

```text
producer ──insert/find_and_modify──▶ _jobs collection ──claim──▶ worker process
                                                            │
                                                            ├── handler code
                                                            └── complete/fail
```

Semantics:

- **At-least-once.** A worker can crash after running a handler but before
  recording completion. Handlers must be idempotent.
- **Single execution at a time.** `find_and_modify` atomically moves a job from
  `pending` to `running`.
- **Fenced leases.** Every claim writes a unique `lease_id`; stale workers cannot
  complete or fail a job after its lease was recovered.
- **Retries.** Failed jobs return to `pending` with exponential backoff until
  `max_attempts`, then become `failed`.
- **No arbitrary code from the database.** The job document contains a job type
  and parameters. The handler code is deployed with the worker.

## Job document

```json
{
  "_id": "01J...",
  "type": "sync_customer",
  "status": "pending",
  "payload": {"tenant": "acme", "customer_id": "c1"},
  "attempts": 0,
  "max_attempts": 5,
  "due_at": 1762800000000,
  "created_at": 1762799940000,
  "updated_at": 1762799940000
}
```

Status values:

| Status | Meaning |
|---|---|
| `pending` | Ready to be claimed when `due_at <= now` |
| `running` | Claimed under a lease |
| `done` | Completed successfully |
| `failed` | Retries exhausted or permanent failure |

Runtime fields added by a claim:

- `lease_id` — fencing token,
- `lease_until` — Unix milliseconds,
- `worker_id`,
- `started_at`,
- `attempts`.

## Indexes

The worker helper uses two compound indexes:

```json
{"fields": ["status", "due_at"]}
{"fields": ["status", "lease_until"]}
```

Create them once with an administrative client, or start the worker with
`--create-indexes`:

```bash
python3 -m nonodb_worker \
  --module myjobs \
  --collection _jobs \
  --create-indexes
```

Creating an index rebuilds it, so do not run `--create-indexes` on every worker
start in a large deployment.

## Python worker helper

The repository includes a small helper at `workers/python/nonodb_worker`:

```text
workers/python/nonodb_worker/jobs.py   # JobQueue, JobRunner
workers/python/nonodb_worker/__main__.py
workers/python/examples/jobs_handlers.py
workers/python/examples/jobs_smoke.py
```

It is a normal Python client wrapper, not a daemon extension.

### Handler module

```python
from nonodb_worker import Job


def add_numbers(job: Job):
    return {"sum": int(job.payload["a"]) + int(job.payload["b"])}


def register(runner) -> None:
    runner.register("add", add_numbers)
```

`run_forever` catches handler exceptions, records `last_error`, and retries with
backoff. An unknown job type is marked failed immediately.

### Run the worker

```bash
PYTHONPATH=client:workers/python:workers/python/examples \
NONODB_SOCK=/run/nonodb/nonodb.sock \
python3 -m nonodb_worker \
  --module jobs_handlers \
  --collection _jobs \
  --recover-expired
```

Useful options:

| Option | Meaning |
|---|---|
| `--once` | Claim and run at most one job, then exit |
| `--poll-interval` | Idle sleep between claims, default `1.0` second |
| `--lease-seconds` | Lease duration, default `60` |
| `--recover-expired` | Requeue expired leases at startup |
| `--create-indexes` | Build the job indexes before starting |
| `--worker-id` | Stable worker identity in logs and job documents |

### Enqueue from an application

```python
from nonodb import NonoDB
from nonodb_worker import JobQueue

db = NonoDB()
jobs = JobQueue(db.collection("_jobs"))
jobs.enqueue(
    "sync_customer",
    {"tenant": "acme", "customer_id": "c1"},
    job_id="sync_customer:c1",
)
```

Passing `job_id` makes enqueue idempotent: a duplicate `_id` is treated as an
already-enqueued job.

If a job must be written atomically with application data, use `write_batch`:

```python
order = db.collection("orders")
db.collection("_jobs").write_batch([
    {"op": "insert", "coll": "orders", "doc": {"_id": "o1", "status": "new"}},
    {"op": "insert", "coll": "_jobs", "doc": {
        "_id": "sync_order:o1",
        "type": "sync_order",
        "status": "pending",
        "payload": {"order_id": "o1"},
        "attempts": 0,
        "max_attempts": 5,
        "due_at": 0,
        "created_at": 0,
        "updated_at": 0,
    }},
])
```

`write_batch` is atomic across the batch.

## Scheduling

There are two supported patterns.

### Long-running worker

The worker polls the collection and sleeps when no job is due. This gives the
lowest latency and is the recommended production mode.

```ini
# /etc/systemd/system/nonodb-jobs.service
[Unit]
Description=NonoDB background job worker
After=nonodb.service
Requires=nonodb.service

[Service]
Type=simple
User=nonodb
Group=nonodb
Environment=NONODB_SOCK=/run/nonodb/nonodb.sock
Environment=PYTHONPATH=/opt/nonodb/client:/opt/nonodb/workers/python:/opt/nonodb/jobs
WorkingDirectory=/opt/nonodb
ExecStart=/usr/bin/python3 -m nonodb_worker --module jobs_handlers
Restart=on-failure
RestartSec=5

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/nonodb /run/nonodb

[Install]
WantedBy=multi-user.target
```

### Cron or systemd timer

For infrequent jobs, run `--once` from cron or a systemd timer:

```ini
# nonodb-jobs.timer
[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

The timer only starts the worker. The job queue still provides leases, retries,
and idempotency.

## Other languages

The job helper is Python-only because it is convenient for rapid development,
but the contract is plain HTTP/JSON. A worker can be written in any language:

| Language | Notes |
|---|---|
| Python | Included helper, best for ETL and integrations |
| Go | Good for small static worker binaries |
| Rust | Good for high-throughput or long-running workers |
| Node.js / TypeScript | Good for webhooks and JS integrations |
| Shell + `curl` | Fine for simple administrative jobs |
| Java, C#, PHP, Ruby | Use any HTTP client |

A worker in another language needs to implement the same operations:

1. `find_and_modify` with `status: "pending"`, `due_at <= now`, sorted by
   `due_at`;
2. set `status: "running"`, `lease_id`, `lease_until`, increment `attempts`;
3. run the handler;
4. update `status: "done"` with the lease filter, or retry with backoff.

## Security

- Give workers a token with only the collection scopes they need.
- Do not give workers administrative scopes unless they also manage indexes.
- Job payloads are normal documents. Do not store secrets in them if the
  collection is broadly readable.
- Vault collections can be written by an encrypted client, but the worker needs
  the same vault headers and keys to read them.
- Treat the job type as an allowlist. Never execute a command string from a job
  document.

## Events

For outbox events (atomic data + event writes, polling consumers, checkpoints,
event worker CLI, and systemd), see [Outbox events](events.md). Events are
separate from jobs: use events for change notification and jobs for durable,
retryable work.

## Validation

```bash
python3 -m unittest discover -s workers/python/tests -v
PYTHONPATH=client:workers/python NONODB_SOCK=/tmp/nonodb.sock \
  python3 workers/python/examples/jobs_smoke.py
```

The smoke test covers enqueue, idempotent enqueue, successful completion, failed
handler retry, and final failure state.
