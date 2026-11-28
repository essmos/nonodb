#!/usr/bin/env bash
# Offline CLI inspect/verify/stats/compact against a columnar database.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DAEMON=${NONODB_DAEMON:-$ROOT/target/debug/nonodb-daemon}
CLI=${NONODB_CLI:-$ROOT/target/debug/nonodb-cli}
for path in "$DAEMON" "$CLI"; do
  [[ -x "$path" ]] || { echo "missing binary: $path" >&2; exit 2; }
done

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
mkdir -p "$TMP/data" "$TMP/run"
export NONODB_DATA="$TMP/data"
export NONODB_SOCK="$TMP/run/n.sock"
export RUST_LOG=warn
"$DAEMON" >"$TMP/daemon.log" 2>&1 &
PID=$!
for _ in $(seq 1 200); do
  [[ -S "$NONODB_SOCK" ]] && break
  sleep 0.05
done
[[ -S "$NONODB_SOCK" ]] || { cat "$TMP/daemon.log" >&2; exit 1; }

"$CLI" create-collection events --profile columnar \
  --bucket-max-docs 8 --compression max >/dev/null
for index in 0 1 2; do
  "$CLI" exec "{\"op\":\"insert\",\"coll\":\"events\",\"doc\":{\"_id\":\"e$index\",\"v\":$index}}" \
    >/dev/null
done
"$CLI" exec '{"op":"update","coll":"events","filter":{"_id":"e1"},"update":{"$set":{"v":10}}}' >/dev/null
"$CLI" exec '{"op":"delete","coll":"events","filter":{"_id":"e2"}}' >/dev/null
"$CLI" exec '{"op":"create_index","coll":"events","field":"v"}' >/dev/null || true
kill "$PID"
wait "$PID" 2>/dev/null || true
PID=""

"$CLI" inspect "$TMP/data" events >"$TMP/inspect.txt"
grep -q 'columnar' "$TMP/inspect.txt"
grep -q 'deletes' "$TMP/inspect.txt"
"$CLI" verify "$TMP/data" --json >"$TMP/verify.json"
python3 - "$TMP/verify.json" <<'PY'
import json
import sys
report = json.load(open(sys.argv[1]))
event = report["collections"]["events"]
assert event["documents"] == 2, event
PY
"$CLI" compact "$TMP/data" --force >/dev/null

"$DAEMON" >"$TMP/daemon2.log" 2>&1 &
PID=$!
for _ in $(seq 1 200); do
  [[ -S "$NONODB_SOCK" ]] && break
  sleep 0.05
done
[[ -S "$NONODB_SOCK" ]] || { cat "$TMP/daemon2.log" >&2; exit 1; }
[[ "$("$CLI" count events)" == *'"count": 2'* ]]
[[ "$("$CLI" find events '{"_id":"e1"}')" == *'"v": 10'* ]]
echo "columnar CLI inspect/verify/compact: ok"
