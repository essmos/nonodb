#!/usr/bin/env bash
# Verify that a pre-columnar daemon fails closed on a columnar data directory.
#
#   NONODB_OLD_DAEMON=/path/to/v0.34.0/nonodb-daemon \
#     NONODB_DAEMON=target/debug/nonodb-daemon \
#     NONODB_CLI=target/debug/nonodb-cli \
#     bash scripts/columnar_old_binary_test.sh
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
NEW=${NONODB_DAEMON:-$ROOT/target/debug/nonodb-daemon}
OLD=${NONODB_OLD_DAEMON:-}
CLI=${NONODB_CLI:-$ROOT/target/debug/nonodb-cli}
if [[ -z "$OLD" || ! -x "$OLD" ]]; then
  echo "set NONODB_OLD_DAEMON to a pre-columnar daemon binary" >&2
  exit 2
fi
for path in "$NEW" "$CLI"; do
  if [[ ! -x "$path" ]]; then
    echo "missing binary: $path" >&2
    exit 2
  fi
done

TMP=$(mktemp -d)
NEW_PID=""
cleanup() {
  if [[ -n "$NEW_PID" ]]; then
    kill "$NEW_PID" 2>/dev/null || true
    wait "$NEW_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT
mkdir -p "$TMP/run"
NONODB_DATA="$TMP/data" NONODB_SOCK="$TMP/run/new.sock" RUST_LOG=warn \
  "$NEW" >"$TMP/new.log" 2>&1 &
NEW_PID=$!
for _ in $(seq 1 200); do
  [[ -S "$TMP/run/new.sock" ]] && break
  sleep 0.05
done
[[ -S "$TMP/run/new.sock" ]] || { cat "$TMP/new.log" >&2; exit 1; }

NONODB_SOCK="$TMP/run/new.sock" "$CLI" create-collection events --profile columnar \
  --bucket-max-docs 8 --compression max >/dev/null
NONODB_SOCK="$TMP/run/new.sock" "$CLI" exec \
  '{"op":"insert","coll":"events","doc":{"_id":"e1","v":1}}' >/dev/null
kill "$NEW_PID"
wait "$NEW_PID" 2>/dev/null || true
NEW_PID=""
DATA="$TMP/data/default/data.mdb"
[[ -f "$DATA" ]] || { echo "missing data file: $DATA" >&2; exit 1; }
BEFORE=$(sha256sum "$DATA" | awk '{print $1}')

set +e
NONODB_DATA="$TMP/data" NONODB_SOCK="$TMP/run/old.sock" RUST_LOG=warn \
  timeout 15 "$OLD" >"$TMP/old.log" 2>&1
STATUS=$?
set -e
if [[ $STATUS -eq 0 ]]; then
  echo "old daemon unexpectedly opened a columnar database" >&2
  cat "$TMP/old.log" >&2
  exit 1
fi
if ! grep -q 'unknown collection profile.*columnar' "$TMP/old.log"; then
  echo "old daemon failed without the expected profile error" >&2
  cat "$TMP/old.log" >&2
  exit 1
fi
AFTER=$(sha256sum "$DATA" | awk '{print $1}')
if [[ "$BEFORE" != "$AFTER" ]]; then
  echo "old daemon modified data.mdb" >&2
  exit 1
fi
echo "old binary rejection: ok"
