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

mkdir -p "$TMP/run"
export NONODB_TTL_SWEEP_INTERVAL=1
NONODB_DATA="$TMP/data" \
NONODB_SOCK="$TMP/run/nonodb.sock" \
RUST_LOG=warn \
"$BIN" >"$TMP/daemon.log" 2>&1 &
PID=$!

for _ in $(seq 1 100); do
  if [[ -S "$TMP/run/nonodb.sock" ]]; then
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "daemon exited during startup:" >&2
    cat "$TMP/daemon.log" >&2
    exit 1
  fi
  sleep 0.05
done

if [[ ! -S "$TMP/run/nonodb.sock" ]]; then
  echo "daemon socket did not appear" >&2
  cat "$TMP/daemon.log" >&2
  exit 1
fi

export NONODB_SOCK="$TMP/run/nonodb.sock"
unset NONODB_URL NONODB_TOKEN NONODB_TLS_VERIFY

python3 - <<'PY'
import os
import sys
sys.path.insert(0, os.path.join(os.getcwd(), "client"))
from nonodb import NonoDB
db = NonoDB(os.environ["NONODB_SOCK"])
try:
    db.exec({"op": "create_database", "name": "app1"})
finally:
    db.close()
PY

echo "== Rust client (database app1)"
NONODB_DB=app1 cargo run -q -p nonodb-client --example quickstart

echo "== Node.js client (database app1)"
NONODB_DB=app1 node client/node/smoke_test.js

echo "== Go client (database app1)"
(
  cd client/go
  NONODB_DB=app1 go run ./cmd/smoke
)

echo "== Dates"
python3 scripts/dates_test.py

echo "== Numerics"
python3 scripts/numerics_test.py

echo "== BSON types"
python3 scripts/bson_types_test.py

echo "== Regex"
python3 scripts/regex_test.py

echo "== Arrays"
python3 scripts/arrays_test.py

echo "== Array updates"
python3 scripts/array_updates_test.py

echo "== Find and modify"
python3 scripts/find_and_modify_test.py

echo "== Explain"
python3 scripts/explain_test.py

echo "== SQL"
python3 scripts/sql_test.py
python3 scripts/sql_matrix_test.py

echo "== Sub-database slots"
python3 scripts/db_slots_test.py

echo "== Online map resize"
python3 scripts/resize_test.py

echo "== Graceful shutdown"
python3 scripts/shutdown_test.py

echo "== TTL"
python3 scripts/ttl_test.py

echo "== Per-collection history"
python3 scripts/history_test.py

echo "== Automatic collection events"
python3 scripts/events_auto_test.py

echo "== Vault regression"
python3 scripts/vault_regression_test.py

echo "== Vault benchmark smoke"
python3 scripts/vault_bench.py --docs 1000 --iters 100 --payload 64

echo "== Authentication and authorization"
NONODB_DAEMON="$BIN" python3 scripts/auth_test.py

echo "== mTLS"
NONODB_BIN="$BIN" bash scripts/mtls_test.sh

echo "all client smoke tests passed"
