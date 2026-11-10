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
export NONODB_HISTORY=1
export NONODB_HISTORY_MAX_VERSIONS=5
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

echo "== Rust client"
cargo run -q -p nonodb-client --example quickstart

echo "== Node.js client"
node client/node/smoke_test.js

echo "== Go client"
(
  cd client/go
  go run ./cmd/smoke
)

echo "== Vault regression"
python3 scripts/vault_regression_test.py

echo "== Vault benchmark smoke"
python3 scripts/vault_bench.py --docs 1000 --iters 100 --payload 64

echo "all client smoke tests passed"
