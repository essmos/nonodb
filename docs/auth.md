# Authentication and authorization

NoNoDB uses a small JSON authorization registry managed by `nonodb-cli` and
watched by the daemon. It is deliberately **outside LMDB**:

- it survives database restores and cannot be rolled back with an old backup;
- it remains available when the database is unavailable;
- it is not copied into database backups or snapshots;
- it contains only token hashes and certificate fingerprints, never plaintext
  tokens.

## Trust model

| Source | Identity | Required configuration |
|---|---|---|
| Unix socket | `local` principal with `*:*` | Socket permissions and uid/gid allow-lists |
| TCP | Bearer token or registered client certificate | `NONODB_AUTH_FILE` is mandatory |
| TCP + mTLS | Registered certificate, optionally combined with a token | `NONODB_AUTH_FILE`, client CA, TLS cert/key |

TCP never starts without `NONODB_AUTH_FILE`. There is no global static token
fallback.

The Unix socket is treated as a trusted local administrative channel. Protect
it with filesystem permissions (`0660`, group `nonodb`) and, if needed,
`NONODB_ALLOWED_UIDS` / `NONODB_ALLOWED_GIDS`.

## Auth file

Default path: `/etc/nonodb/auth.json`. Override with `NONODB_AUTH_FILE`.

Recommended permissions:

```text
/etc/nonodb/auth.json    root:nonodb 0640
/etc/nonodb/             root:nonodb 0750
```

For a Docker bind mount, add the host file's group to the container
(`docker run --group-add "$(id -g)" ...`) or use a Compose secret. A `0640`
file owned by the host user is not readable by the container's unprivileged
user without that group.

Example:

```json
{
  "version": 1,
  "principals": {
    "app": {
      "scopes": ["read:orders", "write:orders"]
    },
    "reporting": {
      "scopes": ["read:*"]
    },
    "admin": {
      "scopes": ["*:*"]
    }
  },
  "tokens": [
    {
      "kid": "8f31a2c9d104",
      "sha256": "…64 hex characters…",
      "principal": "app",
      "disabled": false
    }
  ],
  "certificates": [
    {
      "sha256": "…64 hex characters…",
      "principal": "app",
      "disabled": false
    }
  ]
}
```

The daemon validates the entire file. Unknown fields, duplicate token IDs,
duplicate fingerprints, unknown principals, invalid scopes, or an unsupported
version make the file invalid.

## Scopes

| Scope | Allows |
|---|---|
| `read:<collection>` | find, find_one, lookup, count, aggregate, SQL `SELECT`, explain_sql, explain, search, fuzzy, vector search, history, get_version, get_collection_ttl, get_collection_history, list indexes |
| `write:<collection>` | insert, insert_many, update, find_and_modify, delete, SQL `INSERT`/`UPDATE` |
| `admin:<collection>` | create/drop collection, create/drop secondary/text/fuzzy/vector index, set collection TTL, set collection history |
| `admin:db` | map_info, resize_map, `/metrics` |
| `read:*` / `write:*` / `admin:*` | The action on every collection |
| `*:*` | Everything |

Rules:

- Authorization is deny-by-default.
- `write` does not imply `read`; grant both when the workload needs both.
- `admin:<collection>` does not imply `read` or `write`.
- `write_batch` is checked per operation; one forbidden collection rejects the
  whole request before any write happens.
- `list_collections` returns only collections visible through any collection
  scope. A principal with `read:orders` does not learn that `customers` exists.
- `/healthz` is public. `/metrics` requires `admin:db`.
- If both a client certificate and a bearer token are used, they must resolve to
  the same principal.

## Tokens

Create a token:

```bash
nonodb-cli auth token create app
```

Output is a single line:

```text
nodb_8f31a2c9d104_Xq7...
```

The file stores only `kid` and `sha256(secret)`. The plaintext token is shown
once and cannot be recovered. Use it as:

```http
Authorization: Bearer nodb_8f31a2c9d104_Xq7...
```

Disable a token without deleting its audit history:

```bash
nonodb-cli auth token disable 8f31a2c9d104
nonodb-cli auth token enable 8f31a2c9d104
```

## Certificates and mTLS

Register the SHA-256 fingerprint of the DER-encoded leaf certificate:

```bash
FP=$(openssl x509 -in client.crt -outform DER | sha256sum | awk '{print $1}')
nonodb-cli auth cert add "$FP" app
```

The certificate is accepted only when:

1. its chain validates against `NONODB_TLS_CLIENT_CA`;
2. its fingerprint is present and enabled in `NONODB_AUTH_FILE`;
3. the referenced principal exists.

Certificate authentication is re-checked on every request, so disabling or
removing an entry also blocks existing keep-alive connections. New handshakes
are rejected immediately.

Revoke:

```bash
nonodb-cli auth cert disable "$FP"
```

There is no CRL or OCSP integration. Revocation is handled by this registry and
short-lived client certificates.

## Managing principals

```bash
nonodb-cli auth init
nonodb-cli auth principal add app read:orders,write:orders
nonodb-cli auth principal scopes app read:orders,write:orders,read:customers
nonodb-cli auth principal list
nonodb-cli auth token create app
nonodb-cli auth token list
nonodb-cli auth cert add "$FP" app
nonodb-cli auth cert list
nonodb-cli auth check
```

`auth init` creates an `admin` principal with `*:*`. It does not create a token;
run `auth token create admin` next. Use `--force` to replace an existing file.

The CLI writes the file atomically under an exclusive lock:

1. validate the complete new document;
2. write a temporary file in the same directory;
3. `fsync` the file;
4. rename over the old file;
5. `fsync` the directory.

## Reload and failure behavior

- The daemon loads the file at startup. An invalid file is a startup error.
- When `NONODB_AUTH_FILE` is configured, a watcher checks the file every second.
- A changed, valid file is swapped in atomically.
- A changed, invalid file is rejected; the daemon keeps the last known good
  configuration and logs an error. Fix the file and save it again.
- There is no SIGHUP requirement and no restart for token or certificate
  changes.

## Failed-login lockout

The daemon can block a TCP peer by IP after repeated failed authentication.
Unix socket requests are local administration and are never blocked.

| Variable | Default | Meaning |
|---|---|---|
| `NONODB_AUTH_MAX_FAILURES` | `10` | Failed attempts in the window before a block; `0` disables lockout |
| `NONODB_AUTH_FAILURE_WINDOW` | `60` | Sliding window in seconds for counting failures |
| `NONODB_AUTH_BLOCK_SECS` | `300` | Block duration in seconds; `0` disables lockout |

Behavior:

- Only failed authentication counts: bad/missing bearer token, unregistered or
  disabled client certificate, or a certificate/token principal mismatch.
- Successful requests do **not** clear failures. The counter ages out only after
  `NONODB_AUTH_FAILURE_WINDOW`, so successful traffic cannot mask a brute-force
  attempt.
- When the threshold is reached, the client receives HTTP `429` with error code
  `rate_limited` and the block starts.
- During a block, all HTTP requests from that IP receive `429`, including
  `/metrics`. mTLS handshakes from a blocked IP are dropped before TLS
  negotiation.
- Blocks are in-memory and process-local. Restarting the daemon clears them.
- The peer address is taken from the TCP socket. `X-Forwarded-For` and similar
  headers are ignored, so a reverse proxy requires a separate rate-limit layer
  or a trusted-proxy design.
- A block applies to the whole IP. Multiple applications behind one NAT address
  share the counter and the block.

Lockout settings are read at daemon startup; restart the daemon after changing
them. The auth file itself reloads without a restart.

Typical configuration:

```bash
export NONODB_AUTH_MAX_FAILURES=5
export NONODB_AUTH_FAILURE_WINDOW=300
export NONODB_AUTH_BLOCK_SECS=900
```

Tune this above normal user typo rates. For machine-to-machine deployments, a
small threshold (3–5) and a short window are usually appropriate. For shared
office NAT, use a higher threshold or disable IP lockout and rely on token
rotation plus an upstream WAF.

Metrics:

| Metric | Meaning |
|---|---|
| `auth_failures_total` | Failed authentication attempts |
| `auth_blocks_total` | Number of lockouts started |
| `auth_blocked_requests_total` | Requests rejected because the IP was already blocked |
| `auth_blocked_clients` | Currently blocked IPs |

## Audit log

Enable the append-only JSONL audit log:

```bash
export NONODB_AUDIT_LOG=/var/log/nonodb/audit.jsonl
```

Recommended permissions: `nonodb:nonodb 0640`.

Behavior:

- writes and administrative operations are always logged;
- denied requests are always logged;
- failed logins, lockouts, and blocked requests are always logged;
- reads are optional with `NONODB_AUDIT_READS=1`;
- `NONODB_AUDIT_FSYNC=1` fsyncs after every line (slower, stronger durability).

Normal record:

```json
{"ts_ms":1794389203533,"principal":"app","source":"tcp","peer":"127.0.0.1:54684","op":"insert","collections":["orders"],"result":"ok","error":null,"duration_ms":1.9}
```

Failed-login record:

```json
{"ts_ms":1794389203533,"principal":"anonymous","source":"tcp","peer":"127.0.0.1:54684","op":"find","collections":["orders"],"result":"error","error":"rate_limited","duration_ms":0.3,"auth_failures":5,"blocked_for_secs":900}
```

`auth_failures` is the number of attempts in the current window when the record
was written. `blocked_for_secs` is present when this attempt started a block or
was rejected while blocked. Counting audit records with these fields gives the
number of unauthorized login attempts and lockout events.

mTLS certificate rejection before HTTP is audited as
`"op":"tls_handshake"` with the same fields.

The audit log never contains bearer tokens, vault keys, certificate private
keys, or document bodies. It is outside LMDB, so database restores do not
rewrite audit history.

## Backups

`auth.json` and the audit log are **not** part of `nonodb-cli backup`. Back them
up separately with filesystem tools and protect them as credentials. The full
backup procedure is in [Backup and restore](backup.md#authorization-and-audit-state).

```bash
install -m 0640 -o root -g nonodb auth.json /var/backups/nonodb/auth.json
```

Restoring an old database does not restore old authorization state. This is
intentional: a stale backup must not resurrect a revoked token or certificate.
