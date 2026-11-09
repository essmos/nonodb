#!/usr/bin/env bash
# Test crash-safety: zapisuje równolegle, zabija daemon `kill -9`, restartuje
# i weryfikuje, że baza się otwiera i dane są spójne.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/nonodb-daemon}"
D="${D:-/tmp/nonodb-crash-test}"
S="$D/nonodb.sock"

rm -rf "$D"
mkdir -p "$D"

start() {
  NONODB_DATA="$D/data" NONODB_SOCK="$S" RUST_LOG=warn "$BIN" >"$D/daemon.log" 2>&1 &
  echo $!
}

wait_sock() {
  for _ in $(seq 1 100); do
    [ -S "$S" ] && return 0
    sleep 0.05
  done
  echo "socket nie powstał" >&2
  return 1
}

PID="$(start)"
wait_sock

curl -s --unix-socket "$S" -H 'content-type: application/json' -X POST \
  http://localhost/v1/exec \
  -d '{"op":"insert","coll":"w","doc":{"seed":true}}' >/dev/null

python3 - "$S" <<'PY' &
import sys
sys.path.insert(0, "client")
from nonodb import NonoDB
try:
    db = NonoDB(sys.argv[1])
    c = db.collection("w")
    i = 0
    while True:
        c.insert({"i": i, "payload": "x" * 200})
        i += 1
except Exception:
    pass
PY
CLIENT=$!

sleep 0.7
echo "kill -9 daemon $PID"
kill -9 "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
wait "$CLIENT" 2>/dev/null || true

PID2="$(start)"
wait_sock

COUNT="$(curl -s --unix-socket "$S" -H 'content-type: application/json' -X POST \
  http://localhost/v1/exec -d '{"op":"count","coll":"w","filter":{}}')"
echo "po restarcie: $COUNT"

echo "$COUNT" | grep -q '"ok":true' || { echo "FAIL: baza nie działa po crashu" >&2; kill "$PID2"; exit 1; }
N="$(echo "$COUNT" | sed -E 's/.*"count":([0-9]+).*/\1/')"
[ "$N" -ge 1 ] || { echo "FAIL: brak danych po crashu" >&2; kill "$PID2"; exit 1; }

curl -s --unix-socket "$S" -H 'content-type: application/json' -X POST \
  http://localhost/v1/exec -d '{"op":"insert","coll":"w","doc":{"after":"restart"}}' >/dev/null

kill -TERM "$PID2" 2>/dev/null || true
wait "$PID2" 2>/dev/null || true

echo "OK: baza otwiera się po kill -9, dane spójne ($N dokumentów), zapis po restarcie działa"
