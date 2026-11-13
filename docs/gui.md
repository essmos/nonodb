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

To enforce daemon bearer-token authentication, point the GUI proxy at the
daemon's TCP listener instead of the trusted Unix socket:

```bash
nonodb-cli gui --url http://127.0.0.1:7879
```

Unix sockets are treated as trusted local admin connections, so token auth is not
enforced on that path.

Exposing the GUI beyond loopback is only safe with TLS, authentication, and a
trusted reverse proxy or tunnel. The GUI itself does not terminate TLS.

## Screenshots

Overview with the database selector; the example below uses `perf1` with one
million documents:

![NonoDB GUI overview](assets/gui/overview.png)

Documents browser with keyset pagination, filters, sorting, and the document
editor:

![NonoDB GUI documents](assets/gui/documents-1m.png)

Index management, including the automatic index state and text/vector indexes:

![NonoDB GUI indexes](assets/gui/indexes-1m.png)

Search console for full-text/fuzzy/vector queries:

![NonoDB GUI search](assets/gui/search-1m.png)

Runtime metrics and time-series samples:

![NonoDB GUI metrics](assets/gui/metrics-1m.png)

A short walkthrough of switching databases and opening collections:

[Download the GUI walkthrough video (MP4, 16 s)](assets/gui/demo.mp4)

## Connection

The GUI uses the same database credential as the CLI and API: a NonoDB bearer
token. There is no separate GUI account.

- Local GUI connects without a token when daemon authorization is disabled.
- If the GUI process has `NONODB_TOKEN`, the local GUI session endpoint fills
  the token automatically.
- Otherwise enter the token in the single **Database token** field.
- Vault tenant, key id, key, endpoint, database, and saved profiles are under
  **Advanced**.
- The sidebar database selector switches between databases in the same daemon.
  Requests are routed as `/<database>/v1/exec` and `/<database>/v1/watch`.

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
- a note explaining that each database is a separate LMDB environment and the selector switches between them.

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

One daemon can host several independent databases, each with its own LMDB
environment under `NONODB_DATA/<database>/`. The sidebar selector switches the
active database and the GUI routes requests to
`/<database>/v1/exec` and `/<database>/v1/watch`; `default` uses the same
routes. No second daemon, socket, port, or GUI instance is needed.

Databases are created and dropped with the CLI or the management API. The GUI
can select them but does not yet expose a "new database" button:

```bash
nonodb-cli databases create app1
nonodb-cli databases list
nonodb-cli databases drop app1
```

Each database has separate collections, indexes, TTL/history/event settings,
and subscriptions. Collections and queries never span databases.

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
  ghcr.io/essmos/nonodb:0.20.0 \
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

## Future GUI work

These are planned improvements, not current features.

- **Database management in the GUI.** Create and drop databases directly from
  the console; today the GUI can select databases but creation/drop is CLI-only.
- **Auth management without file writes.** Add daemon APIs for principal/token
  management and expose them in the GUI. The current GUI deliberately cannot
  edit `auth.json` because the HTTP API does not write auth-file paths.
- **Backup and restore UI.** Trigger online backups, list manifests, verify
  checksums, and show restore instructions. Restore itself remains CLI-only
  until a safe daemon-side restore flow exists.
- **Multiple daemon tabs.** Keep several connection profiles open at once
  instead of switching one active connection.
- **Per-collection metrics.** Show collection-level request, write, cache, and
  index-build counters; current runtime metrics are daemon-wide.
- **Progress and resume for large imports.** Stream import progress, allow
  cancellation, and resume large JSON/NDJSON/CSV imports.
- **Better large-result handling.** Virtualized document lists and resumable
  cursors for very large collections.
- **Light theme.** The current console is intentionally dark-only.
- **Accessibility and keyboard navigation.** Improve focus management, ARIA
  labels, and shortcuts beyond the command palette.
- **Shareable saved views and filters.** Export/import saved views without
  sharing credentials.

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
