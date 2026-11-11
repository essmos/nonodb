#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BIN="${NONODB_BIN:-$ROOT/target/release/nonodb-daemon}"
if [[ ! -x "$BIN" ]]; then
  echo "missing $BIN; run: cargo build --release --bin nonodb-daemon" >&2
  exit 1
fi

TMP=$(mktemp -d)
PID=""
cleanup() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

# ---------------------------------------------------------------- certificates
openssl genrsa -out "$TMP/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -nodes -key "$TMP/ca.key" -sha256 -days 2 \
  -subj "/CN=nonodb test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "$TMP/ca.crt" >/dev/null 2>&1

openssl genrsa -out "$TMP/server.key" 2048 >/dev/null 2>&1
openssl req -new -key "$TMP/server.key" -subj "/CN=localhost" \
  -out "$TMP/server.csr" >/dev/null 2>&1
cat >"$TMP/server.ext" <<'EXT'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EXT
openssl x509 -req -in "$TMP/server.csr" -CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" \
  -CAcreateserial -out "$TMP/server.crt" -days 2 -sha256 \
  -extfile "$TMP/server.ext" >/dev/null 2>&1

openssl genrsa -out "$TMP/client.key" 2048 >/dev/null 2>&1
openssl req -new -key "$TMP/client.key" -subj "/CN=client-a" \
  -out "$TMP/client.csr" >/dev/null 2>&1
cat >"$TMP/client.ext" <<'EXT'
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
EXT
openssl x509 -req -in "$TMP/client.csr" -CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" \
  -CAcreateserial -out "$TMP/client.crt" -days 2 -sha256 \
  -extfile "$TMP/client.ext" >/dev/null 2>&1

CLIENT_FP=$(openssl x509 -in "$TMP/client.crt" -outform DER | openssl dgst -sha256 | awk '{print $NF}')
TOKEN_SECRET="mtls-client-secret"
TOKEN_HASH=$(printf '%s' "$TOKEN_SECRET" | openssl dgst -sha256 | awk '{print $NF}')
export NONODB_TOKEN="nodb_mtls_${TOKEN_SECRET}"
python3 - "$TMP/auth.json" "$CLIENT_FP" "$TOKEN_HASH" <<'PY'
import json, sys
path, fingerprint, token_hash = sys.argv[1:4]
doc = {
    "version": 1,
    "principals": {"client-a": {"scopes": ["*:*"]}},
    "tokens": [{
        "kid": "mtls",
        "sha256": token_hash,
        "principal": "client-a",
        "disabled": False,
    }],
    "certificates": [{
        "sha256": fingerprint.lower(),
        "principal": "client-a",
        "disabled": False,
    }],
}
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY

# ---------------------------------------------------------------- daemon start
PORT=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)

export NONODB_HISTORY=1
export NONODB_HISTORY_MAX_VERSIONS=5
NONODB_DATA="$TMP/data" \
NONODB_SOCK="$TMP/nonodb.sock" \
NONODB_TCP="127.0.0.1:$PORT" \
NONODB_AUTH_FILE="$TMP/auth.json" \
NONODB_TLS_CERT="$TMP/server.crt" \
NONODB_TLS_KEY="$TMP/server.key" \
NONODB_TLS_CLIENT_CA="$TMP/ca.crt" \
NONODB_TLS_CLIENT_AUTH=required \
RUST_LOG=warn \
"$BIN" >"$TMP/daemon.log" 2>&1 &
PID=$!

for _ in $(seq 1 100); do
  if python3 - "$PORT" <<'PY'
import socket, sys
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", int(sys.argv[1])))
except OSError:
    raise SystemExit(1)
finally:
    s.close()
PY
  then
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "daemon exited during startup:" >&2
    cat "$TMP/daemon.log" >&2
    exit 1
  fi
  sleep 0.05
done

export NONODB_URL="https://localhost:$PORT"
export NONODB_TLS_CA="$TMP/ca.crt"
export NONODB_TLS_CLIENT_CERT="$TMP/client.crt"
export NONODB_TLS_CLIENT_KEY="$TMP/client.key"
unset NONODB_SOCK NONODB_TLS_VERIFY

echo "== Python mTLS"
python3 - <<'PY'
import os
import sys
sys.path.insert(0, os.path.join(os.environ["PWD"], "client"))
from nonodb import NonoDB

db = NonoDB(base_url=os.environ["NONODB_URL"], token=os.environ["NONODB_TOKEN"])
db.collections()
coll = db.collection("mtls_python")
doc_id = coll.insert({"name": "Ada"})
assert coll.find_one({"_id": doc_id})["name"] == "Ada"
coll.delete({"_id": doc_id})
db.close()
print("python mTLS client: ok")
PY

echo "== Rust mTLS"
cargo run -q -p nonodb-client --example quickstart >/dev/null

echo "== Node.js mTLS"
node client/node/smoke_test.js >/dev/null

echo "== Go mTLS"
(
  cd client/go
  go run ./cmd/smoke >/dev/null
)

echo "== mTLS without client certificate is rejected"
if env -u NONODB_TLS_CLIENT_CERT -u NONODB_TLS_CLIENT_KEY \
  python3 - <<'PY' 2>/dev/null
import os
import sys
sys.path.insert(0, os.path.join(os.environ["PWD"], "client"))
from nonodb import NonoDB

db = NonoDB(base_url=os.environ["NONODB_URL"], token=os.environ["NONODB_TOKEN"])
db.exec({"op": "list_collections"})
raise SystemExit(0)
PY
then
  echo "anonymous TLS connection unexpectedly succeeded" >&2
  exit 1
else
  echo "anonymous TLS connection rejected: ok"
fi

echo "== certificate registry revocation on existing connection"
python3 - "$TMP/auth.json" <<'PY'
import json, os, sys, time
sys.path.insert(0, os.path.join(os.environ["PWD"], "client"))
from nonodb import NonoDB, NonoDBError

path = sys.argv[1]
db = NonoDB(base_url=os.environ["NONODB_URL"], token=os.environ["NONODB_TOKEN"])
db.exec({"op": "list_collections"})  # keep-alive connection established

with open(path) as fh:
    doc = json.load(fh)
doc["certificates"][0]["disabled"] = True
tmp = path + ".tmp"
with open(tmp, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
os.replace(tmp, path)

time.sleep(2)
try:
    db.exec({"op": "list_collections"})
except NonoDBError as exc:
    assert exc.code == "unauthorized", (exc.code, exc)
else:
    raise SystemExit("revoked client unexpectedly succeeded")
finally:
    db.close()
print("revoked client on existing connection rejected: ok")
PY

echo "all mTLS tests passed"
