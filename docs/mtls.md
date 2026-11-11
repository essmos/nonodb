# Mutual TLS

NoNoDB supports mutual TLS over TCP. The server verifies the client certificate
chain against a configured CA and maps the client certificate fingerprint to a
principal in the authorization registry.

See [Authentication and authorization](auth.md) for the registry format,
scopes, tokens, revocation, and audit logging. This document covers certificates
and daemon configuration.

## 1. Create a client CA

```bash
openssl genrsa -out client-ca.key 2048
chmod 600 client-ca.key

openssl req -x509 -new -nodes -key client-ca.key -sha256 -days 3650 \
  -subj "/CN=nonodb client CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out client-ca.crt
```

The `basicConstraints` and `keyUsage` extensions are required. A CA certificate
without `keyCertSign` is rejected by rustls.

## 2. Create the server certificate

The server certificate must contain a SAN matching the hostname or IP used by
clients.

```bash
openssl genrsa -out server.key 2048
chmod 600 server.key

openssl req -new -key server.key -subj "/CN=db.internal" -out server.csr

cat >server.ext <<'EXT'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:db.internal,DNS:localhost,IP:127.0.0.1
EXT

openssl x509 -req -in server.csr \
  -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 -extfile server.ext
```

A certificate for `db.internal` will not validate for
`https://127.0.0.1:7878` unless that IP is also listed in the SAN.

## 3. Create client certificates

One certificate per workload. Do not share a private key between unrelated
applications.

```bash
openssl genrsa -out acme-app.key 2048
chmod 600 acme-app.key

openssl req -new -key acme-app.key -subj "/CN=acme-app" -out acme-app.csr

cat >client.ext <<'EXT'
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
EXT

openssl x509 -req -in acme-app.csr \
  -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -out acme-app.crt -days 90 -sha256 -extfile client.ext
```

Use short lifetimes and automate renewal.

## 4. Register the certificate

Create the auth registry and a principal:

```bash
export NONODB_AUTH_FILE=/etc/nonodb/auth.json
nonodb-cli auth init
nonodb-cli auth principal add acme-app read:orders,write:orders
```

Register the leaf certificate fingerprint:

```bash
FP=$(openssl x509 -in acme-app.crt -outform DER | sha256sum | awk '{print $1}')
nonodb-cli auth cert add "$FP" acme-app
```

A certificate signed by the CA but absent from the registry is rejected after
the TLS handshake. Disabling the entry blocks both new handshakes and further
requests on existing keep-alive connections.

## 5. Configure the daemon

```bash
export NONODB_TCP=0.0.0.0:7878
export NONODB_AUTH_FILE=/etc/nonodb/auth.json

export NONODB_TLS_CERT=/etc/nonodb/tls/server.crt
export NONODB_TLS_KEY=/etc/nonodb/tls/server.key

export NONODB_TLS_CLIENT_CA=/etc/nonodb/tls/client-ca.crt
export NONODB_TLS_CLIENT_AUTH=required
```

| Variable | Purpose |
|---|---|
| `NONODB_TLS_CERT` | PEM server certificate chain |
| `NONODB_TLS_KEY` | PEM server private key |
| `NONODB_TLS_CLIENT_CA` | PEM CA bundle used to verify client certificates |
| `NONODB_TLS_CLIENT_AUTH` | `required`, `optional`, or `off` |
| `NONODB_AUTH_FILE` | Token and certificate registry; mandatory for TCP |

`NONODB_TLS_CLIENT_AUTH` defaults to `required` when a client CA is configured
and `off` otherwise. A client CA cannot be combined with `off`.

## 6. Connect

Python:

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="https://db.internal:7878",
    token="nodb_...",  # optional when the certificate already identifies the principal
    ca_cert="/etc/nonodb/tls/client-ca.crt",
    client_cert="/etc/nonodb/tls/acme-app.crt",
    client_key="/etc/nonodb/tls/acme-app.key",
    timeout=30,
)
print(db.collection("orders").count())
db.close()
```

If both a token and a client certificate are supplied, they must resolve to the
same principal.

Environment variables used by the official clients:

```text
NONODB_URL=https://db.internal:7878
NONODB_TOKEN=nodb_...
NONODB_TLS_CA=/etc/nonodb/tls/client-ca.crt
NONODB_TLS_CLIENT_CERT=/etc/nonodb/tls/acme-app.crt
NONODB_TLS_CLIENT_KEY=/etc/nonodb/tls/acme-app.key
```

## Revocation and rotation

- Disable a certificate: `nonodb-cli auth cert disable "$FP"`.
- Disable a token: `nonodb-cli auth token disable "$KID"`.
- Changes are picked up within about one second without restarting the daemon.
- There is no CRL/OCSP; the registry is the revocation mechanism.
- Rotate by registering the new fingerprint, deploying the new certificate, and
  disabling the old fingerprint.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| TLS handshake fails | Wrong CA, missing SAN, expired certificate, or missing `clientAuth` usage |
| `certificate is not registered` | Fingerprint missing or disabled in `NONODB_AUTH_FILE` |
| `401 unauthorized` | Token missing/invalid or certificate mapping disabled |
| `403 forbidden` | Principal exists but lacks the required scope |
| Daemon refuses to start | Missing/invalid `NONODB_AUTH_FILE`, or missing server cert/key pair |
| Existing connection suddenly gets `401` | Token or certificate was disabled and the reload completed |
