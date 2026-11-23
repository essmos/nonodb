# MongoDB wire shadow mode

The MongoDB wire listener has an opt-in prototype mode for validating a
migration before cutting traffic over. Reads still come from NonoDB. Write
commands are also sent to a MongoDB server, and each result is appended to a
JSONL diff report.

This is intentionally a prototype, not a production proxy. It is useful for
finding behavioral differences while the real application exercises the new
database.

## Enable

Run the daemon with a MongoDB wire listener and a shadow upstream:

```bash
export NONODB_MONGO_ADDR=127.0.0.1:37017
export NONODB_MONGO_SHADOW_URL='mongodb://127.0.0.1:27017'
export NONODB_MONGO_SHADOW_LOG='/var/log/nonodb/mongo-shadow.jsonl'
```

Point the application's MongoDB driver at the NonoDB listener as usual. Shadow
mode is disabled when `NONODB_MONGO_SHADOW_URL` is unset.

The upstream must be reachable without authentication and without TLS. The
prototype does not support credentials, SRV resolution, replica-set discovery,
or failover.

## What happens

For successful `insert`, `update`, `delete`, `findAndModify`, `createIndexes`,
`dropIndexes`, `create`, and `drop` commands, NonoDB serves the request first.
The original command is then sent to MongoDB with the same database name. The
NonoDB reply is returned to the client.

The report contains one JSON object per attempted shadow write:

```json
{
  "command": "update",
  "database": "app",
  "collection": "users",
  "diff": [],
  "nonodb": {"ok": 1.0, "n": 1, "nModified": 1},
  "mongodb": {"ok": 1.0, "n": 1, "nModified": 1}
}
```

A non-empty `diff` lists reply fields that differ (`ok`, `n`, `nModified`,
`deleted`, `upserted`, `writeErrors`, or `writeConcernError`). Transport and
handshake failures are logged with `error`; partial NonoDB write results are
logged with `skipped` and are not forwarded. Document bodies are not written to
the report.

Reads are never served from MongoDB. If the two stores diverge, the application
continues to see the NonoDB view.

## Limits

- The upstream connection is synchronous per write; a slow or unavailable
  MongoDB can add up to the three-second shadow timeout to a request.
- No authentication, TLS, SRV, replica-set routing, retries, or failover.
- No sessions, transactions, change streams, or `dropDatabase` mirroring.
- No reverse direction: writes made directly to MongoDB are not copied back.
- Reply comparison is intentionally shallow; it compares command result fields,
  not the complete database state.
- The mode does not replace backups, migration verification, or a tested cutover
  plan. Use `scripts/nonodb-migrate.py` for the bulk copy and this mode only for
  observation during a rehearsal.

## Test

`scripts/mongo_shadow_test.py` starts a temporary daemon, performs PyMongo CRUD,
checks both databases, and validates the diff report. It is part of the MongoDB
compatibility CI job.
