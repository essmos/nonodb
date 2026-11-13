# Security checks

This page describes how nonodb is checked for security regressions, what the
automated checks cover, and how the external LiveAPISec audit was performed.

Security checking is split into two layers:

1. **Local regression checks** (`scripts/security_check.py`) — run in CI and
   before a release. They start an isolated daemon with a temporary auth
   registry and verify the security invariants that must not regress.
2. **External black-box audits** — optional LiveAPISec scans against a
   development/staging daemon. They are not part of CI because they require an
   external account and a tunnel.

Neither layer replaces TLS termination, network isolation, secret management,
or a deployment-specific threat model.

## Local security regression check

Run it against freshly built binaries:

```bash
cargo build -p nonodb-daemon -p nonodb-cli
python3 scripts/security_check.py
```

`--strict` also fails on known hardening warnings:

```bash
python3 scripts/security_check.py --strict
```

The script never uses production data. It creates a temporary directory,
temporary auth registry, temporary administrator token, and an isolated daemon
on a loopback TCP port. It removes them when the check finishes.

### What it verifies

| Check | Expected result |
|---|---|
| `GET /healthz` without credentials | `200`, public by design |
| `GET /metrics` without credentials | `401` |
| `POST /v1/exec` without credentials | `401` |
| `POST /v1/watch` without credentials | `401` |
| Invalid bearer token | `401`, no data |
| Admin token on `/metrics` | `200` |
| Database creation as admin | succeeds |
| Same collection name in two databases | isolated data |
| `read:app1/orders` scope | can read `app1/orders` only |
| Same token on `default/orders` | `403` |
| Same token on `app1/users` | `403` |
| Same token on `/metrics` | `403` |
| Same token on `create_database` | `403` |
| `OPTIONS` with untrusted `Origin` | no `Access-Control-Allow-Origin` |
| Malformed JSON | client error, no panic or backtrace |
| Unknown operation | client error, not `500` |
| Repeated failed authentication | `429` lockout |
| Valid token from a locked peer | `429` while the lockout is active |
| `X-Content-Type-Options: nosniff` | warning if missing |

Expected daemon behavior is **19 passing checks and 4 hardening warnings** on
`v0.21.0`. CI runs the non-strict form, so the security invariants fail the job
but known hardening gaps do not block unrelated changes.

### Current warnings

- Malformed JSON responses are plain text instead of the normal JSON error
  envelope.
- Unknown-operation errors list the full `op` enum, which exposes the internal
  operation vocabulary.
- `/healthz` and `/metrics` do not send `X-Content-Type-Options: nosniff`.

These are hardening gaps, not authentication bypasses. Use `--strict` when a
change is intended to close them.

### CI

`.github/workflows/ci.yml` runs the check after the existing integration tests:

```bash
python3 scripts/security_check.py
```

The CI job uses release binaries and never calls an external service.

## External LiveAPISec audit

LiveAPISec is an external black-box API scanner. Use it only against a
development or staging daemon with test data. Never send a production admin
token to an external scanner, and never scan a production host without explicit
permission.

### Preparation

Run the daemon on loopback with a temporary auth registry:

```bash
AUDIT_DIR=/tmp/nonodb-liveapisec-audit
rm -rf "$AUDIT_DIR"
mkdir -p "$AUDIT_DIR/data"

export NONODB_AUTH_FILE="$AUDIT_DIR/auth.json"
export NONODB_DATA="$AUDIT_DIR/data"
export NONODB_SOCK="$AUDIT_DIR/nonodb.sock"
export NONODB_TCP=127.0.0.1:7889
export NONODB_LOG_FORMAT=json

nonodb-cli auth init
nonodb-cli auth token create admin > "$AUDIT_DIR/admin-token.txt"
chmod 600 "$AUDIT_DIR/admin-token.txt"

nonodb-daemon > "$AUDIT_DIR/daemon.log" 2>&1 &
```

Scan the source to discover endpoints and create a LiveAPISec project:

```bash
AUDIT_TOKEN="$(cat "$AUDIT_DIR/admin-token.txt")"

liveapisec scan-code \
  --dir . \
  --framework rust \
  --name nonodb-security-audit \
  --base-url http://127.0.0.1:7889 \
  --access internal \
  --auth-type bearer \
  --auth-token "$AUDIT_TOKEN"
```

Start the reverse tunnel in another terminal:

```bash
liveapisec connect --project <project-id>
```

Run the baseline scan:

```bash
liveapisec scan \
  --project <project-id> \
  --tunnel \
  --wait \
  --fail-on critical
```

Optionally run the AI hacker-mode test against the isolated `development`
environment. It is intentionally limited to a scratch daemon:

```bash
liveapisec urls add \
  --project <project-id> \
  --name development \
  --url http://127.0.0.1:7889 \
  --schedule off

liveapisec hacker \
  --project <project-id> \
  --env development \
  --tunnel \
  --wait \
  --destructive
```

`--destructive` allows POST/PUT/PATCH/DELETE. Never use it against a database
whose data matters. Remove the temporary daemon, tunnel, token, and auth file
after the audit.

## Audit snapshot: 2026-11-13

The audit was run against the `v0.21.0` codebase using release/debug binaries
and an isolated daemon. No production data was used.

### Baseline scan

| Result | Value |
|---|---:|
| Endpoints discovered | 6 |
| Tests run | 277 |
| Critical | 0 |
| High | 0 |
| Medium | 0 |
| Low | 4 |
| Info | 2 |

The low/info findings were:

- missing security headers on `/healthz` and `/metrics`,
- missing `/.well-known/security.txt`,
- no throttle headers on rapid requests to `/healthz` and `/metrics`.

### AI hacker mode

The AI hacker scan reported one **high** finding: “SSN / card in response” on
`GET /metrics`.

That finding was a **false positive**. The scanner's regex matched a numeric
metric:

```text
avg_commit_us = 8.689466666666668
```

Manual inspection confirmed that `/metrics` contains only counters, timings,
queue depths, and cache statistics. It did not contain SSNs, card numbers, or
document data.

### Manual verification

The important controls were rechecked independently:

| Check | Observed |
|---|---|
| Anonymous `/v1/exec` | `401` |
| Anonymous `/metrics` | `401` |
| Anonymous `/healthz` | `200` |
| Scoped token on `app1/orders` | `200` |
| Scoped token on `default/orders` | `403` |
| Scoped token on `app1/users` | `403` |
| Scoped token on `/metrics` | `403` |
| `OPTIONS` with untrusted `Origin` | `405`, no CORS allow header |
| Malformed JSON | `400`, plain-text parser message |
| Unknown operation | `422`, exposes the operation enum |
| Repeated bad tokens | `429` lockout |
| Valid token while locked | `429` |
| 100 rapid `/healthz` requests | 100 × `200` |

### Verdict

The audit found **no confirmed authentication bypass, cross-database data
leak, or critical/high-severity vulnerability**. Database-qualified scopes and
`/metrics` protection worked as designed.

The real issues are hardening gaps:

1. Error responses are inconsistent: malformed JSON is plain text, and unknown
   operations expose the full operation enum.
2. Public `/healthz` and authenticated `/v1/exec` have no general rate limit.
   Only failed authentication triggers lockout.
3. Daemon responses lack `X-Content-Type-Options: nosniff`.
4. The embedded GUI has a strong CSP, but `connect-src` includes `http:` and
   `https:`; because the GUI uses a same-origin proxy, `connect-src 'self'`
   would be tighter.

The GUI already sends these headers:

```text
Content-Security-Policy: default-src 'self'; ...; frame-ancestors 'none'
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
```

HSTS is expected at the TLS terminator, not on a plaintext loopback listener.

## Production checklist

- Terminate TCP behind TLS/mTLS or an authenticated reverse proxy.
- Bind the daemon and GUI to loopback unless remote access is intentional.
- Use scoped tokens such as `read:app1/orders`, not a global admin token.
- Keep `auth.json` outside the data directory and back it up separately.
- Do not enable `NONODB_ALLOW_UNSAFE_WRITES=1` for critical data.
- Put rate limits on `/healthz` and `/v1/exec` at the proxy if exposed.
- Keep vault tenant keys outside database backups.
- Run `python3 scripts/security_check.py` before releases.
- Run external scans only against isolated development data.

## Related documentation

- [Authentication and permissions](auth.md)
- [Mutual TLS](mtls.md)
- [Multiple databases](multi-database.md)
- [Operations](operations.md)
- [GUI console](gui.md)
- [Backup and restore](backup.md)
