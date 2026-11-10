# Mutual TLS (mTLS)

nonodb can verify client certificates in addition to the server certificate.
This is called mutual TLS: both sides prove their identity during the TLS
handshake. It is optional and only applies to TCP. Unix-socket deployments do
not use TLS.

mTLS is useful when:

- the daemon listens on a network instead of a local Unix socket;
- you need to revoke access for one client without rotating a shared bearer
  token;
- you already run a private CA and want machine identity, not only a password;
- you want a second authentication factor in front of the global bearer token.

mTLS does **not** replace authorization. A valid client certificate proves that
the client holds a private key; it does not grant a role, tenant, or collection
scope. The global `NONODB_AUTH_TOKEN` is still required when configured.

## How it works

```text
client certificate ──┐
client private key ──┼── TLS handshake ──> server certificate
                     │                      server private key
                     └── verified by ─────> private client CA
                                             │
                                             └── optional fingerprint allow-list
```

The daemon needs:

1. A server certificate and private key for TLS.
2. A CA bundle used to verify client certificates.
3. Optionally, an allow-list of client certificate fingerprints.

The client needs:

1. A CA bundle used to verify the daemon certificate.
2. A client certificate and private key.
3. A TCP URL such as `https://db.internal:7878`.

If the allow-list is configured, the daemon reads it again for every TLS
handshake. Removing a fingerprint blocks new connections immediately. Existing
keep-alive connections continue until they close.

## Quick test

The repository includes a self-contained test that generates a temporary CA,
server certificate, client certificate, and allow-list, then runs all official
clients:

```bash
cargo build --release --bin nonodb-daemon
bash scripts/mtls_test.sh
```

The script also checks that a client without a certificate is rejected and that
an allow-list revocation takes effect.

For a manual setup, follow the steps below.

## 1. Create a private CA

The CA private key must stay offline or on a tightly controlled machine. The
daemon only needs the CA certificate, never the CA private key.

```bash
mkdir -p /etc/nonodb/tls
cd /etc/nonodb/tls

openssl genrsa -out client-ca.key 4096
chmod 600 client-ca.key

openssl req -x509 -new -nodes -key client-ca.key -sha256 -days 3650 \
  -subj "/CN=nonodb client CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out client-ca.crt
```

The `basicConstraints` and `keyUsage` extensions are required by rustls. A CA
certificate without `keyCertSign` is rejected.

## 2. Create the server certificate

The server certificate must contain a subject alternative name (SAN) that
matches the hostname clients use. Add every DNS name and IP address that will
appear in `NONODB_URL`.

```bash
openssl genrsa -out server.key 2048
chmod 600 server.key

openssl req -new -key server.key -subj "/CN=db.internal" -out server.csr

cat >server.ext <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:db.internal,DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -in server.csr \
  -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 -extfile server.ext
```

The client must verify the hostname or IP address in the certificate. A
certificate for `db.internal` will not validate for `https://127.0.0.1:7878`
unless `127.0.0.1` is also in the SAN list.

## 3. Create a client certificate

One certificate per client or workload. Do not share a client private key
between unrelated applications.

```bash
openssl genrsa -out acme-app.key 2048
chmod 600 acme-app.key

openssl req -new -key acme-app.key -subj "/CN=acme-app" -out acme-app.csr

cat >client.ext <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
EOF

openssl x509 -req -in acme-app.csr \
  -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -out acme-app.crt -days 90 -sha256 -extfile client.ext
```

Short lifetimes are recommended. Use automation to renew certificates before
they expire.

## 4. Create the fingerprint allow-list

The allow-list maps the SHA-256 fingerprint of each client leaf certificate to
a label. The label is only used in logs; it does not grant permissions.

```bash
openssl x509 -in acme-app.crt -outform DER \
  | openssl dgst -sha256
```

Create `/etc/nonodb/tls/allowed-clients.txt`:

```text
# sha256 fingerprint                         label
sha256:ab12cd34...  client:acme-app
sha256:ef56ab78...  client:reporting-job
```

Rules:

- One entry per line.
- `sha256:` is optional.
- Colons inside the fingerprint are optional.
- A fingerprint must contain 64 hexadecimal characters.
- Lines starting with `#` are comments.
- The file is re-read on every handshake. A missing or malformed file fails
  closed: new clients are rejected.

If the allow-list is omitted, any client certificate signed by the configured
client CA is accepted.

## 5. Configure the daemon

```bash
export NONODB_TCP=0.0.0.0:7878
export NONODB_AUTH_TOKEN='long-random-token'

export NONODB_TLS_CERT=/etc/nonodb/tls/server.crt
export NONODB_TLS_KEY=/etc/nonodb/tls/server.key

export NONODB_TLS_CLIENT_CA=/etc/nonodb/tls/client-ca.crt
export NONODB_TLS_CLIENT_AUTH=required
export NONODB_TLS_CLIENT_ALLOW=/etc/nonodb/tls/allowed-clients.txt
```

| Variable | Meaning |
|---|---|
| `NONODB_TLS_CERT` | PEM certificate chain for the server |
| `NONODB_TLS_KEY` | PEM private key for the server |
| `NONODB_TLS_CLIENT_CA` | PEM CA bundle used to verify client certificates |
| `NONODB_TLS_CLIENT_AUTH` | `off`, `required`, or `optional`; defaults to `required` when client CA is set |
| `NONODB_TLS_CLIENT_ALLOW` | Optional fingerprint allow-list; requires `NONODB_TLS_CLIENT_CA` |

Behavior:

- `required` rejects every client that does not present a trusted certificate.
- `optional` accepts clients without a certificate but still validates a
  certificate when one is presented.
- `off` disables client certificate verification. Combining `off` with
  `NONODB_TLS_CLIENT_CA` is a startup error.
- `NONODB_TLS_CLIENT_CA` without `NONODB_TLS_CERT` and `NONODB_TLS_KEY` is a
  startup error.
- If TLS certificate/key are not configured at all, TCP runs in plaintext and
  logs a warning. Do not use plaintext TCP outside a trusted local network.
- The global bearer token is still required whenever `NONODB_AUTH_TOKEN` is
  set, regardless of mTLS.

Start the daemon and check the log. It should report `mtls=true` and
`allowlist=true` when both are enabled:

```bash
RUST_LOG=info ./nonodb-daemon
```

## 6. Configure clients

All official clients support mTLS over TCP. Unix-socket connections ignore the
TLS settings.

### Environment variables

```bash
export NONODB_URL=https://db.internal:7878
export NONODB_TOKEN='long-random-token'
export NONODB_TLS_CA=/etc/nonodb/tls/client-ca.crt
export NONODB_TLS_CLIENT_CERT=/etc/nonodb/tls/acme-app.crt
export NONODB_TLS_CLIENT_KEY=/etc/nonodb/tls/acme-app.key
```

Certificate verification must stay enabled. `NONODB_TLS_VERIFY=0` is intended
only for local development.

### Python

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="https://db.internal:7878",
    token="long-random-token",
    ca_cert="/etc/nonodb/tls/client-ca.crt",
    client_cert="/etc/nonodb/tls/acme-app.crt",
    client_key="/etc/nonodb/tls/acme-app.key",
)
```

The certificate and key must be provided together. The Python client builds an
SSL context explicitly because some `httpx` versions do not load a client
certificate when `verify` is a CA path.

### Rust

```rust
use nonodb_client::Client;

let db = Client::builder()
    .base_url("https://db.internal:7878")
    .token("long-random-token")
    .ca_cert("/etc/nonodb/tls/client-ca.crt")
    .client_cert(
        "/etc/nonodb/tls/acme-app.crt",
        "/etc/nonodb/tls/acme-app.key",
    )
    .build()?;
```

`Client::from_env()` reads the same environment variables.

### Node.js

```js
const { NonoDB } = require('nonodb-client');

const db = new NonoDB({
  baseUrl: 'https://db.internal:7878',
  token: 'long-random-token',
  caCert: '/etc/nonodb/tls/client-ca.crt',
  clientCert: '/etc/nonodb/tls/acme-app.crt',
  clientKey: '/etc/nonodb/tls/acme-app.key',
});
```

The certificate and key must be provided together. Files are loaded on the
first HTTPS request.

### Go

```go
db := nonodb.New(
    nonodb.WithBaseURL("https://db.internal:7878"),
    nonodb.WithToken("long-random-token"),
    nonodb.WithTLSCA("/etc/nonodb/tls/client-ca.crt"),
    nonodb.WithTLSClientCert(
        "/etc/nonodb/tls/acme-app.crt",
        "/etc/nonodb/tls/acme-app.key",
    ),
)
```

Errors from loading the CA or key pair are returned by the first request.

### CLI

`nonodb-cli` does **not** support TLS or mTLS. It uses the Unix socket on Linux
and macOS, and plaintext HTTP over TCP on Windows. Use an official client for
encrypted connections.

## 7. Verify the connection

You can test the server side with `openssl` before involving an application:

```bash
openssl s_client \
  -connect db.internal:7878 \
  -servername db.internal \
  -CAfile /etc/nonodb/tls/client-ca.crt \
  -cert /etc/nonodb/tls/acme-app.crt \
  -key /etc/nonodb/tls/acme-app.key
```

A successful handshake ends with `Verify return code: 0 (ok)`.

A request with `curl`:

```bash
curl --cacert /etc/nonodb/tls/client-ca.crt \
  --cert /etc/nonodb/tls/acme-app.crt \
  --key /etc/nonodb/tls/acme-app.key \
  -H "Authorization: Bearer $NONODB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"op":"list_collections"}' \
  https://db.internal:7878/v1/exec
```

## Revoking a client

Remove the client's fingerprint from the allow-list:

```bash
cp allowed-clients.txt allowed-clients.txt.bak
grep -v 'client:acme-app' allowed-clients.txt.bak >allowed-clients.txt
```

New connections from that certificate are rejected on the next handshake. The
daemon does not need a restart.

Existing TLS keep-alive connections remain usable until they close. If you need
immediate disconnection of an already-connected client, restart the client, the
daemon, or use a network-level control. For high-security environments, keep
client connections short-lived and certificates short-lived.

If no allow-list is configured, revoke a certificate by removing it from the
CA trust or by rotating the client CA. There is no CRL or OCSP support in the
current rustls setup.

## Rotating certificates

### Client certificate rotation

1. Generate a new client key and certificate.
2. Add the new fingerprint to the allow-list.
3. Deploy the new certificate to the client.
4. Verify that the client connects.
5. Remove the old fingerprint from the allow-list.
6. Delete the old private key.

This allows a zero-downtime rotation because the allow-list can contain both
fingerprints during the overlap.

### Server certificate rotation

1. Generate a new server certificate signed by a CA the clients trust.
2. Replace `NONODB_TLS_CERT` and `NONODB_TLS_KEY`.
3. Restart the daemon.

There is no hot reload for the server certificate. If clients trust a private
CA, you can rotate the server certificate without changing client trust, as
long as the same CA signs it.

### Client CA rotation

1. Create the new CA and new client certificates.
2. Put both old and new CA certificates in the file referenced by
   `NONODB_TLS_CLIENT_CA`.
3. Add new client fingerprints to the allow-list.
4. Restart the daemon.
5. Migrate clients to certificates from the new CA.
6. Remove the old CA certificate and old fingerprints.
7. Restart the daemon again.

The daemon reads the client CA bundle only at startup, unlike the allow-list.

## Operational guidance

- Keep the CA private key offline or on a dedicated signing host.
- The daemon needs read access to the CA certificate and allow-list, but not to
  the CA private key.
- Client private keys should be mode `0600` and owned by the client user.
- Do not commit private keys, allow-lists, or generated certificates to the
  repository.
- Use a dedicated CA for nonodb clients, not a general-purpose corporate CA.
- Use short certificate lifetimes and automate renewal.
- Protect the allow-list file from unauthorized writes. An attacker who can edit
  it can add a certificate.
- Keep the bearer token enabled as a second factor unless you have a separate
  authorization layer.
- Monitor daemon logs for `mTLS client rejected` entries.
- Use `RUST_LOG=debug` during setup to see accepted client labels. Do not leave
  debug logging enabled permanently if client labels are sensitive.

## Security model and limitations

- mTLS protects the transport between client and daemon. It does not encrypt
  documents at rest. Use [vault collections](vault.md) or filesystem/volume
  encryption separately.
- The daemon terminates TLS. It sees plaintext HTTP after the handshake.
- There is no mapping from certificate identity to a user, role, tenant, or
  collection. All authenticated clients share the same authorization scope.
- The bearer token is global. mTLS gives per-client revocation, but not
  per-client permissions.
- The allow-list identifies the leaf certificate fingerprint. It does not
  identify a subject name or a CA.
- There is no CRL or OCSP support. Revocation relies on the fingerprint
  allow-list and short-lived certificates.
- Existing keep-alive connections are not closed when an allow-list entry is
  removed.
- If TLS is terminated by a reverse proxy, the daemon sees plaintext and cannot
  perform client certificate verification. In that setup, the proxy is
  responsible for mTLS.
- The Windows CLI does not support TLS. Use the Python, Rust, Node.js, or Go
  client on Windows when encryption is required.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `TLSV13_ALERT_CERTIFICATE_REQUIRED` | Client did not send a certificate | Set `NONODB_TLS_CLIENT_CERT` and `NONODB_TLS_CLIENT_KEY`, or pass client cert options |
| `unknown ca` / certificate verify failed | Client cert not signed by `NONODB_TLS_CLIENT_CA` | Check the CA bundle and client certificate chain |
| Hostname mismatch on client | Server SAN does not contain the URL host | Add `DNS:` or `IP:` entries to the server certificate |
| `mTLS client rejected: fingerprint ...` | Certificate is valid but not allow-listed | Add the leaf certificate SHA-256 fingerprint to `NONODB_TLS_CLIENT_ALLOW` |
| Daemon exits at startup with `NONODB_TLS_CLIENT_CA` error | Client CA configured without server cert/key | Set `NONODB_TLS_CERT` and `NONODB_TLS_KEY` |
| Daemon exits with `NONODB_TLS_CLIENT_AUTH` error | `off` combined with client CA, or auth without CA | Remove the contradictory variable |
| Revoked client still works | Existing keep-alive connection | Close the connection or restart the client/daemon |
| Works with `curl`, fails in Python | Client certificate not loaded by the HTTP library | Use the current client; pass `ca_cert`, `client_cert`, and `client_key` |
| Works on Linux, fails on Windows | CLI does not support TLS | Use an official client, not `nonodb-cli` |

## Related documentation

- [Operations](operations.md#tcp-and-tls) — deployment and environment reference
- [Clients](clients.md) — client installation and common configuration
- [Python client](python-client.md#mutual-tls) — Python-specific mTLS usage
- [Vault collections](vault.md) — storage encryption, a separate concern from
  transport encryption
