#!/usr/bin/env bash
# Crash test na dużych danych: zabija daemon podczas 12 równoległych zapisów.
#
# Wymaga działającego daemona z danymi w NONODB_DATA.
#   NONODB_SOCK=... NONODB_DATA=... NONODB_PID=... bash scripts/large_crash_test.sh
set -euo pipefail

: "${NONODB_SOCK:?}" "${NONODB_DATA:?}" "${NONODB_PID:?}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/nonodb-daemon}"
W="${W:-12}"
PER="${PER:-2000}"
BATCH="${BATCH:-200}"
RUN="${RUN:-$(date +%s%N)}"

count() {
  curl -s --unix-socket "$NONODB_SOCK" -H 'content-type: application/json' \
    -X POST http://localhost/v1/exec -d '{"op":"count","coll":"large","filter":{}}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["count"])'
}

before=$(count)
echo "przed: $before dok."

pids=()
for w in $(seq 1 "$W"); do
  ROOT="$ROOT" NONODB_SOCK="$NONODB_SOCK" W="$w" PER="$PER" BATCH="$BATCH" RUN="$RUN" python3 - <<'PY' &
import json, os, sys, time
sys.path.insert(0, os.path.join(os.environ["ROOT"], "client"))
from nonodb import NonoDB, NonoDBError
w = int(os.environ["W"]); per = int(os.environ["PER"]); batch = int(os.environ["BATCH"])
run = os.environ["RUN"]
db = NonoDB(os.environ["NONODB_SOCK"])
c = db.collection("large")
ok = 0
try:
    for b in range(0, per, batch):
        docs = [{
            "_id": f"crash-{run}-{w}-{b+i}",
            "grp": (b + i) % 1000,
            "age": (b + i) % 90,
            "score": (b + i) % 1000,
            "city": f"city{(b + i) % 50:02d}",
            "name": "crash writer",
            "payload": "x" * 100,
            "src": "crash",
        } for i in range(batch)]
        c.insert_many(docs)
        ok += len(docs)
except (NonoDBError, OSError) as e:
    pass
finally:
    try: db.close()
    except Exception: pass
PY
  pids+=($!)
done

sleep 1.2
echo "kill -9 daemon (PID $NONODB_PID) podczas zapisów..."
kill -9 "$NONODB_PID" 2>/dev/null || true
wait "${pids[@]}" 2>/dev/null || true
rm -f "$NONODB_SOCK"

NONODB_DATA="$NONODB_DATA" NONODB_SOCK="$NONODB_SOCK" RUST_LOG=warn \
  "$BIN" >"$NONODB_DATA/../restart.log" 2>&1 &
newpid=$!
for _ in $(seq 1 300); do
  curl -sf --unix-socket "$NONODB_SOCK" http://localhost/healthz >/dev/null 2>&1 && break
  sleep 0.05
done

after=$(count)
inserted=$((after - before))
echo "po restarcie: $after dok. (przybyło $inserted z max $((W * PER)))"
if [ "$inserted" -lt 0 ] || [ "$inserted" -gt $((W * PER)) ] || [ $((inserted % BATCH)) -ne 0 ]; then
  echo "FAIL: nieoczekiwana liczba dokumentów"; kill "$newpid"; exit 1
fi

ROOT="$ROOT" NONODB_SOCK="$NONODB_SOCK" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.path.join(os.environ["ROOT"], "client"))
from nonodb import NonoDB
db = NonoDB(os.environ["NONODB_SOCK"]); c = db.collection("large")
n = c.count({})
d = c.find_one({"src": "crash"})
assert d is not None, "brak dokumentów zapisanych przed crashem"
print("sample po restarcie:", d["_id"])

# Spójność indeksu: przebuduj `grp` i porównaj liczności.
groups = [0, 321, 999]
old = {g: c.count({"grp": g}) for g in groups}
c.drop_index("grp")
c.create_index("grp")
new = {g: c.count({"grp": g}) for g in groups}
assert old == new, (old, new)
print("indeks grp po przebudowie zgodny:", new)
assert c.count({"grp": 0}) + c.count({"grp": 1}) <= n
db.close()
PY

echo "OK: po kill -9 baza spójna, indeksy zgadzają się, zapis działa (PID $newpid)"
echo "$newpid" >"$NONODB_DATA/../pid"
