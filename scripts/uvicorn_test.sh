#!/usr/bin/env bash
# Integration test: 12 uvicorn workers (fork/spawn) -> daemon over UDS.
# Verifies that the "daemon is the sole LMDB owner" architecture works
# with a multi-process HTTP server, and that the counter grows exactly.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/nonodb-daemon}"
D="${D:-/tmp/nonodb-uvicorn-test}"
S="$D/nonodb.sock"
PORT="${PORT:-8123}"
WORKERS="${WORKERS:-12}"
REQS="${REQS:-60}"
URL="http://127.0.0.1:$PORT"

cleanup() {
  [ -n "${UV_PID:-}" ] && kill "$UV_PID" 2>/dev/null || true
  [ -n "${PID:-}" ] && kill "$PID" 2>/dev/null || true
}
trap cleanup EXIT

rm -rf "$D"
mkdir -p "$D"

NONODB_DATA="$D/data" NONODB_SOCK="$S" RUST_LOG=warn "$BIN" >"$D/daemon.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do [ -S "$S" ] && break; sleep 0.05; done

python3 - "$S" <<'PY'
import sys
sys.path.insert(0, "client")
from nonodb import NonoDB
db = NonoDB(sys.argv[1])
db.collection("counters").insert({"name": "hits", "n": 0})
db.close()
PY

NONODB_SOCK="$S" python3 -m uvicorn example_fastapi:app \
  --app-dir client --workers "$WORKERS" --port "$PORT" --log-level warning \
  >"$D/uvicorn.log" 2>&1 &
UV_PID=$!

for _ in $(seq 1 200); do
  curl -sf "$URL/users" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -sf "$URL/users" >/dev/null || { echo "FAIL: uvicorn nie odpowiada" >&2; tail -20 "$D/uvicorn.log"; exit 1; }

seq "$REQS" | xargs -P "$WORKERS" -I{} curl -sf -X POST \
  "$URL/counters/hits/inc" >/dev/null

GOT="$(curl -sf -X POST "$URL/counters/hits/inc")"
echo "licznik: $GOT (oczekiwano $((REQS + 1)))"
echo "$GOT" | grep -q "\"n\":$((REQS + 1))" || { echo "FAIL: zgubione inkrementacje" >&2; exit 1; }

echo "OK: $WORKERS workerów uvicorn współdzieli daemon; atomowe \$inc działa"
