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
    db.create_collection(
        "py_columnar",
        profile="columnar",
        columnar={"bucket_max_docs": 64, "compression": "max"},
    )
    collection = db.collection("py_columnar")
    collection.insert({"kind": "columnar", "n": 1})
    settings = db.get_collection_columnar("py_columnar")
    assert settings["columnar"]["bucket_max_docs"] == 64
    assert settings["stats"]["rows"] == 1
    db.set_collection_columnar("py_columnar", {"compression": "balanced"})
    assert db.get_collection_columnar("py_columnar")["columnar"]["compression"] == "balanced"
    listed = db.exec({"op": "list_collections"})
    entry = next(item for item in listed["collection_info"] if item["name"] == "py_columnar")
    assert entry["profile"] == "columnar"

    collection.insert({"_id": "rollback", "kind": "old", "n": 2})
    collection.write_batch([
        {"op": "insert", "doc": {"_id": "batch", "kind": "batch", "n": 3}},
        {"op": "update", "filter": {"kind": "columnar"}, "update": {"$inc": {"n": 10}}},
        {"op": "delete", "filter": {"kind": "old"}},
    ])
    assert collection.find_one({"kind": "batch"})["n"] == 3
    assert collection.find_one({"kind": "columnar"})["n"] == 11
    assert collection.find_one({"kind": "old"}) is None
    try:
        collection.write_batch([
            {"op": "insert", "doc": {"_id": "dup", "n": 1}},
            {"op": "insert", "doc": {"_id": "dup", "n": 2}},
        ])
    except Exception:
        pass
    else:
        raise AssertionError("duplicate write_batch unexpectedly succeeded")
    assert collection.find_one({"_id": "dup"}) is None

    for index in range(5):
        collection.insert({"_id": f"cursor-{index}", "kind": "cursor", "n": index})
    cursor = collection.find_cursor({"kind": "cursor"}, batch_size=2)
    assert [doc["_id"] for doc in cursor] == [f"cursor-{index}" for index in range(5)]
    assert cursor.close() is False

    db.create_collection(
        "py_cursor_columnar",
        profile="columnar",
        columnar={"bucket_max_docs": 2, "compression": "max"},
    )
    cursor_column = db.collection("py_cursor_columnar")
    cursor_column.insert_many(
        [
            {"_id": "c3", "n": 3},
            {"_id": "c0", "n": 0},
            {"_id": "c4", "n": 4},
            {"_id": "c1", "n": 1},
            {"_id": "c2", "n": 2},
        ]
    )
    assert [doc["_id"] for doc in cursor_column.find({})] == [
        f"c{index}" for index in range(5)
    ]
    assert [doc["_id"] for doc in cursor_column.find({}, skip=1, limit=2)] == [
        "c1",
        "c2",
    ]
    assert cursor_column.find_one({})["_id"] == "c0"
    assert [
        doc["_id"]
        for doc in cursor_column.aggregate([{"$project": {"_id": 1}}, {"$limit": 2}])
    ] == ["c0", "c1"]
    cursor = cursor_column.find_cursor({}, sort=[("_id", 1)], batch_size=2)
    assert [doc["_id"] for doc in cursor] == [f"c{index}" for index in range(5)]
    assert cursor.close() is False
    aggregate_cursor = cursor_column.aggregate_cursor(
        [{"$match": {"n": {"$gte": 2}}}, {"$project": {"_id": 1, "n": 1}}],
        batch_size=1,
    )
    assert [doc["_id"] for doc in aggregate_cursor] == ["c2", "c3", "c4"]
    assert aggregate_cursor.close() is False
finally:
    db.close()
PY

echo "== CLI columnar settings"
CLI="${NONODB_CLI:-$ROOT/target/release/nonodb-cli}"
if [[ -x "$CLI" ]]; then
  "$CLI" --db app1 create-collection cli_columnar --profile columnar \
    --bucket-max-docs 64 --bucket-max-bytes 65536 --compression max
  "$CLI" --db app1 get-collection-columnar cli_columnar >/dev/null
  "$CLI" --db app1 set-collection-columnar cli_columnar --compression balanced
  "$CLI" --db app1 get-collection-columnar cli_columnar | grep -q '"compression": "balanced"'
  "$CLI" --db app1 collections | grep -q cli_columnar
else
  echo "missing $CLI; skipping CLI columnar check" >&2
fi

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
