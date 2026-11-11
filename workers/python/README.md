# nonodb_worker

Small, non-invasive background job and outbox event helpers for NonoDB.

Jobs and events are normal documents in regular collections. Separate worker
processes claim jobs or poll events, run Python handlers, and store checkpoints
or retry state. The daemon is unchanged.

See [Background jobs and schedulers](../../docs/workers.md) and
[Outbox events](../../docs/events.md) for the full model, systemd setup, security
notes, and examples.

Quick start:

```bash
PYTHONPATH=client:workers/python:workers/python/examples \
NONODB_SOCK=/run/nonodb/nonodb.sock \
python3 -m nonodb_worker --module jobs_handlers --create-indexes
```

Event consumer:

```bash
PYTHONPATH=client:workers/python:workers/python/examples \
python3 -m nonodb_worker.events_worker --module events_handlers --consumer-id order-sync
```

Tests and smoke test:

```bash
python3 -m unittest discover -s workers/python/tests -v
PYTHONPATH=client:workers/python NONODB_SOCK=/run/nonodb/nonodb.sock \
  python3 workers/python/examples/jobs_smoke.py
PYTHONPATH=client:workers/python NONODB_SOCK=/run/nonodb/nonodb.sock \
  python3 workers/python/examples/events_smoke.py
python3 workers/python/tests/events_integration.py
```

`events_integration.py` starts its own `target/debug/nonodb-daemon` and tests
atomic writes, retries, late events, concurrent producers, and the CLI.
