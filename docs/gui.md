# GUI console

`nonodb-cli` includes a local browser console. The React build is embedded in the
binary through `rust-embed`, so the GUI does not require Node.js at runtime.

```bash
nonodb-cli gui
```

The command starts a loopback HTTP server on `http://127.0.0.1:8787`, opens the
default browser, serves the embedded React application, and proxies API requests
to the daemon over the configured Unix socket or TCP endpoint.

```bash
nonodb-cli gui --port 8788
nonodb-cli gui --no-open
```

The server binds to `127.0.0.1` by default. To listen on another interface you
must pass both `--host` and `--allow-remote`:

```bash
nonodb-cli gui --host 0.0.0.0 --allow-remote
```

Exposing the GUI beyond loopback is only safe with TLS, authentication, and a
trusted reverse proxy or tunnel. The GUI itself does not terminate TLS.

## Connection

The default endpoint is the local GUI origin. When authentication is enabled,
enter a bearer token on the Connect screen. Vault collections additionally need
the tenant, key id, and vault key.

Credentials are kept in browser `sessionStorage` for the active session. Saved
connection profiles use browser `localStorage`; do not save production tokens on
a shared machine. Use **Disconnect** to clear the active session.

## Features

### Documents

- browse with MongoDB-style JSON filters, sorting, pagination, limit, and
  optional projection;
- visual filter builder with nested AND/OR groups, operators, value types, and a
  live JSON preview;
- field autocomplete in the filter editor, document editor, sort, index, and
  search inputs;
- CodeMirror JSON editor with syntax highlighting, folding, validation, format,
  copy, insert, edit, and delete;
- multi-document selection with bulk delete and bulk field update;
- document history viewer with previous-revision comparison and restore;
- import/export as JSON, NDJSON, or CSV, with optional upsert on import;
- saved per-collection views for filter, sort, projection, and limit.

### Indexes

- secondary and compound indexes;
- unique, include, and geo options;
- text/BM25 indexes;
- fuzzy indexes;
- vector indexes with dimensions and metric;
- index listing and removal for every supported type.

### Events

- persistent conditional subscription management;
- create with filter, count condition, operation selection, `once`, and lease;
- renew and delete subscriptions;
- connection-scoped live event viewer using `mode:"live"`;
- live events are pushed directly over the stream and are not stored in
  `_events`.

### Search

- text/BM25;
- fuzzy;
- vector search;
- filter, field, limit, and result viewer.

Text, fuzzy, and vector search require the corresponding index on the field.

### Aggregation

- JSON aggregation pipeline editor;
- runs `$match`, `$group`, `$sort`, `$project`, `$limit`, `$skip`, `$unwind`,
  and other supported stages;
- result viewer and JSON export.

### Schema

- inferred field paths from up to 500 sampled documents;
- detected value types and coverage percentage;
- click a field path to add an `$exists` filter.

### SQL

- SQL console with JSON parameters;
- `EXPLAIN` for SQL;
- query history stored in browser localStorage;
- JSON and CSV result export.

### Metrics

- daemon-wide request/read/write/cache/error counters;
- graphical bars and time-series samples with optional 5-second auto-refresh;
- average commit time, batch size, queue length, and TTL backlog;
- collection-level document/index/TTL/versioning state;
- raw metrics JSON.

### Database

- LMDB map size, used/free bytes, and page size;
- online map resize;
- database-wide JSON import/export;
- command helper for backup, restore, verification, and auth CLI operations;
- a note explaining that one daemon serves one LMDB database.

### Navigation

- saved connection profiles;
- command palette (`Ctrl+K`) for navigation and collection switching;
- toast notifications;
- recent operation log with backend-side timings.

## Timings

The local GUI proxy adds an `x-nonodb-server-ms` response header measured around
the daemon request. The bottom-right timing badge and the recent-operation log use
that value, so they show backend time plus local socket overhead rather than full
browser round-trip time. Direct connections without the local proxy fall back to
client-side timing.

## Collection settings

Settings are edited in a focused dialog:

- default document TTL in seconds, with an explanation;
- versioning / history retention (`0` disables history);
- automatic collection events.

## Multiple databases

NonoDB runs **one LMDB database per daemon process**. The GUI therefore has no
"new database" button; adding a database means starting another daemon with a
separate data directory and socket, then pointing another GUI instance at it:

```bash
NONODB_DATA=/var/lib/nonodb-2 \
NONODB_SOCK=/run/nonodb/nonodb-2.sock \
nonodb-daemon
```

```bash
NONODB_SOCK=/run/nonodb/nonodb-2.sock \
nonodb-cli gui --port 8788
```

Use a second Compose service with separate `NONODB_DATA`, socket, and GUI port
for a containerised second database.

## Docker

The image includes the GUI. Compose can start it as an optional sidecar sharing
the daemon's Unix socket:

```bash
docker compose -f deploy/docker-compose.yml --profile gui up -d
```

Open `http://127.0.0.1:8787`. The Compose mapping publishes only to loopback. The
GUI container runs:

```text
nonodb-cli gui --host 0.0.0.0 --port 8787 --allow-remote --no-open
```

`0.0.0.0` is required inside the container so Docker can forward the published
port; the host mapping remains `127.0.0.1:8787:8787`.

With plain Docker:

```bash
docker run -d --name nonodb-gui \
  -p 127.0.0.1:8787:8787 \
  -v nonodb-run:/run/nonodb \
  -e NONODB_SOCK=/run/nonodb/nonodb.sock \
  ghcr.io/essmos/nonodb:0.19.0 \
  nonodb-cli gui --host 0.0.0.0 --port 8787 --allow-remote --no-open
```

The GUI sidecar needs the daemon's socket volume. Do not publish `8787` on
`0.0.0.0` without TLS and authentication in front of it.

## How the local proxy works

```text
browser ──HTTP──▶ 127.0.0.1:8787 (nonodb-cli gui)
                         │
                         ├── embedded React assets
                         ├── /v1/exec, /healthz, /metrics
                         └── /v1/watch (streamed)
                                      │
                                      ▼
                              nonodb-daemon
                        Unix socket or TCP
```

The browser uses one origin, so there is no CORS setup. Authorization headers and
vault headers are forwarded to the daemon. The daemon remains the authority for
all scopes and permissions. The local proxy streams `/v1/watch` instead of
buffering the whole response.

## Build

The GUI source lives in `gui/` (Vite + React + TypeScript). To rebuild the
embedded assets:

```bash
cd gui
npm ci
npm run build
```

Then rebuild the CLI:

```bash
cargo build --release -p nonodb-cli
```

CI and Docker build the GUI before compiling `nonodb-cli`. A committed
`gui/dist/.gitkeep` keeps a fresh checkout compilable before the frontend build
runs.

## Limitations

- Whole-collection deletion is not exposed yet.
- Auth management and backup/restore are CLI-only because the HTTP API does not
  expose auth-file writes or filesystem paths.
- Multi-connection profiles exist, but simultaneous multi-daemon tabs do not.
- Runtime metrics are daemon-wide; per-collection counters are not exposed yet.
- A light theme is not included.

## Security notes

- The GUI server listens on loopback by default.
- Non-loopback binding requires explicit `--allow-remote`.
- Static assets are served with CSP, `nosniff`, and no-referrer headers.
- Tokens are never embedded in the frontend bundle.
- The GUI hides actions on API errors, but the daemon still enforces read,
  write, and admin scopes.
- Do not expose the local GUI port through a reverse proxy without TLS and
  authentication.
- For remote access, prefer a Cloudflare Tunnel or another authenticated HTTPS
  tunnel; do not expose `http://127.0.0.1:8787` directly.
