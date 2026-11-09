"""Test współbieżności: 12 niezależnych źródeł zapisu + odczyty po `_id` i po polu.

Weryfikuje nie tylko przepustowość, ale poprawność:
  1. 12 równoległych pisarzy (osobne procesy, własne połączenie UDS),
  2. odczyt po `_id` (bezpośredni klucz),
  3. odczyt po polu bez indeksu i z indeksem (wyniki identyczne),
  4. zakres po indeksie + sort + skip/limit,
  5. równoległe `$inc` na tym samym dokumencie,
  6. równoległe wstawianie unikalnego klucza (dokładnie jeden sukces),
  7. mieszane czytanie podczas zapisu (brak błędów, spójny końcowy stan).

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/concurrency_test.py
"""

from __future__ import annotations

import os
import random
import sys
from concurrent.futures import ProcessPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
WRITERS = int(os.environ.get("WRITERS", "12"))
PER_WRITER = int(os.environ.get("PER_WRITER", "300"))
READERS = int(os.environ.get("READERS", "12"))
READS_PER = int(os.environ.get("READS_PER", "500"))
COLL = "concurrency"


def _client():
    from nonodb import NonoDB

    return NonoDB(SOCK)


def writer(src: int, n: int) -> list[str]:
    db = _client()
    c = db.collection(COLL)
    ids = [
        c.insert({"src": src, "seq": i, "grp": i % 10, "payload": "x" * 50})
        for i in range(n)
    ]
    db.close()
    return ids


def reader_ids(ids: list[str], reps: int) -> tuple[int, int]:
    db = _client()
    c = db.collection(COLL)
    ok = bad = 0
    for _ in range(reps):
        want = random.choice(ids)
        doc = c.find_one({"_id": want})
        if doc is not None and doc.get("_id") == want:
            ok += 1
        else:
            bad += 1
    db.close()
    return ok, bad


def reader_field(src: int, n: int) -> tuple[int, int]:
    db = _client()
    c = db.collection(COLL)
    docs = c.find({"src": src})
    ok = len(docs) == n and all(d.get("src") == src for d in docs)
    db.close()
    return len(docs), 0 if ok else 1


def inc_worker(_: int) -> None:
    db = _client()
    c = db.collection(COLL)
    for _ in range(100):
        c.update({"_id": "counter"}, {"$inc": {"n": 1}}, multi=False)
    db.close()


def unique_worker(keys: list[str]) -> tuple[int, int]:
    from nonodb import NonoDBError

    db = _client()
    c = db.collection(COLL)
    ok = dup = 0
    for k in keys:
        try:
            c.insert({"uniq": k})
            ok += 1
        except NonoDBError as e:
            if e.code == "duplicate_key":
                dup += 1
            else:
                raise
    db.close()
    return ok, dup


def mixed_writer(src: int, n: int) -> int:
    db = _client()
    c = db.collection(COLL)
    for i in range(n):
        c.insert({"src": f"mixed-{src}", "seq": i, "grp": i % 10})
    db.close()
    return n


def mixed_reader(ids: list[str], reps: int) -> tuple[int, int]:
    db = _client()
    c = db.collection(COLL)
    ok = bad = 0
    for _ in range(reps):
        doc = c.find_one({"_id": random.choice(ids)})
        if doc is not None:
            ok += 1
        else:
            bad += 1
    db.close()
    return ok, bad


def main() -> int:
    db = _client()
    c = db.collection(COLL)
    c.delete({}, multi=True)
    db.close()

    failures: list[str] = []

    # --- 1. 12 równoległych pisarzy ---------------------------------
    with ProcessPoolExecutor(max_workers=WRITERS) as ex:
        results = list(ex.map(writer, range(WRITERS), [PER_WRITER] * WRITERS))
    ids = [i for r in results for i in r]
    db = _client()
    c = db.collection(COLL)
    expected = WRITERS * PER_WRITER
    check_count(db, COLL, {}, expected, "1a. łączna liczba po 12 pisarzach", failures)
    check_count(
        db, COLL, {"src": 0}, PER_WRITER, "1b. liczba per źródło (bez indeksu)", failures
    )
    check_ids_unique(ids, failures)

    # --- 2b. równoległy odczyt po polu BEZ indeksu ------------------
    with ProcessPoolExecutor(max_workers=READERS) as ex:
        res = list(ex.map(reader_field, range(READERS), [PER_WRITER] * READERS))
    bad = sum(r[1] for r in res)
    if bad:
        failures.append(f"2b. odczyt po polu bez indeksu: {bad} błędów")
    else:
        print(f"  2b. odczyt po polu bez indeksu: {READERS} czytelników, 0 błędów")

    # --- 3. odczyt po polu: bez indeksu vs z indeksem ----------------
    before = {w: len(c_all(db, COLL, {"src": w})) for w in range(WRITERS)}
    create_index(db, COLL, "src", unique=False)
    after = {w: len(c_all(db, COLL, {"src": w})) for w in range(WRITERS)}
    if before != after:
        failures.append(f"3. indeks zmienił wyniki: {before} != {after}")
    else:
        print(f"  pole `src`: bez indeksu == z indeksem == {PER_WRITER} na źródło")

    # --- 3b. równoległy odczyt po polu Z indeksem -------------------
    with ProcessPoolExecutor(max_workers=READERS) as ex:
        res = list(ex.map(reader_field, range(READERS), [PER_WRITER] * READERS))
    bad = sum(r[1] for r in res)
    if bad:
        failures.append(f"3b. odczyt po polu z indeksem: {bad} błędów")
    else:
        print(f"  3b. odczyt po polu z indeksem: {READERS} czytelników, 0 błędów")

    # --- 2. odczyt po `_id` (12 czytelników) -------------------------
    with ProcessPoolExecutor(max_workers=READERS) as ex:
        res = list(ex.map(reader_ids, [ids] * READERS, [READS_PER] * READERS))
    ok = sum(r[0] for r in res)
    bad = sum(r[1] for r in res)
    print(f"  odczyt po _id:    {ok} OK / {bad} błędów ({READS_PER * READERS} prób)")
    if bad:
        failures.append(f"2. odczyt po _id: {bad} błędów")

    # --- 4. zakres po indeksie + sort + skip/limit -------------------
    create_index(db, COLL, "seq", unique=False)
    got = c_all(
        db,
        COLL,
        {"seq": {"$gte": 100, "$lt": 200}},
        sort=[["seq", 1]],
        skip=10,
        limit=5,
    )
    seqs = [d["seq"] for d in got]
    if len(seqs) != 5 or any(not 100 <= s < 200 for s in seqs) or seqs != sorted(seqs):
        failures.append(f"4a. zakres/sort/skip/limit: {seqs}")
    else:
        print(f"  4a. indeks zakres [100,200) sort+skip10+limit5 -> {seqs}")

    # Jedno źródło = unikalne `seq`, więc skip/limit daje dokładny przedział.
    one = c_all(
        db,
        COLL,
        {"src": 0, "seq": {"$gte": 100, "$lt": 200}},
        sort=[["seq", 1]],
        skip=10,
        limit=5,
    )
    one_seqs = [d["seq"] for d in one]
    if one_seqs != [110, 111, 112, 113, 114]:
        failures.append(f"4b. skip/limit: {one_seqs}")
    else:
        print(f"  4b. src=0, skip10+limit5 -> {one_seqs}")

    # --- 5. równoległe $inc -----------------------------------------
    c.insert({"_id": "counter", "n": 0})
    with ProcessPoolExecutor(max_workers=WRITERS) as ex:
        list(ex.map(inc_worker, range(WRITERS)))
    n = c.find_one({"_id": "counter"})["n"]
    if n != WRITERS * 100:
        failures.append(f"5. $inc: {n} != {WRITERS * 100}")
    else:
        print(f"  $inc: {WRITERS} źródeł × 100 -> n={n}")

    # --- 6. unikalny indeks pod obciążeniem -------------------------
    create_index(db, COLL, "uniq", unique=True)
    keys = [f"k{i}" for i in range(50)]
    with ProcessPoolExecutor(max_workers=WRITERS) as ex:
        res = list(ex.map(unique_worker, [keys] * WRITERS))
    ok = sum(r[0] for r in res)
    dup = sum(r[1] for r in res)
    if ok != len(keys) or dup != WRITERS * len(keys) - len(keys):
        failures.append(f"6. unique: ok={ok} dup={dup}")
    else:
        print(f"  unique: {ok} sukcesów, {dup} konfliktów (oczekiwane 50 / 550)")

    # --- 7. mieszany odczyt podczas zapisu --------------------------
    with ProcessPoolExecutor(max_workers=WRITERS) as ex:
        readers = [
            ex.submit(mixed_reader, ids, 300) for _ in range(WRITERS // 2)
        ]
        writers = [
            ex.submit(mixed_writer, w, 150) for w in range(WRITERS - WRITERS // 2)
        ]
        rres = [f.result() for f in readers]
        wres = sum(f.result() for f in writers)
    rok = sum(r[0] for r in rres)
    rbad = sum(r[1] for r in rres)
    print(f"  mixed: {wres} zapisów, {rok} odczytów OK, {rbad} błędów")
    if rbad:
        failures.append(f"7. mixed: {rbad} błędów odczytu")

    # --- końcowa spójność -------------------------------------------
    total = c_all(db, COLL, {})
    want_total = expected + 1 + len(keys) + wres
    if len(total) != want_total:
        failures.append(f"końcowa liczba: {len(total)} != {want_total}")
    for w in range(WRITERS):
        cnt = len(c_all(db, COLL, {"src": w}))
        if cnt != PER_WRITER:
            failures.append(f"per-src {w}: {cnt} != {PER_WRITER}")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(" -", f)
        return 1
    db.close()
    print(f"\nOK: wszystko spójne; dokumentów={len(total)}, oczekiwano={want_total}")
    return 0


# --- pomocnicze ------------------------------------------------------
def c_all(db, coll, flt, sort=None, skip=0, limit=None):
    q = {"op": "find", "coll": coll, "filter": flt}
    if sort:
        q["sort"] = sort
    if skip:
        q["skip"] = skip
    if limit is not None:
        q["limit"] = limit
    return db.exec(q)


def check_count(db, coll, flt, want, label, failures):
    got = len(c_all(db, coll, flt))
    if got != want:
        failures.append(f"{label}: {got} != {want}")
    else:
        print(f"  {label}: {got}")


def check_ids_unique(ids, failures):
    if len(ids) != len(set(ids)):
        failures.append(f"1c. zduplikowane _id: {len(ids) - len(set(ids))}")
    else:
        print(f"  1c. _id unikalne: {len(ids)}")


def create_index(db, coll, field, unique):
    db.exec(
        {
            "op": "create_index",
            "coll": coll,
            "field": field,
            "unique": unique,
        }
    )


if __name__ == "__main__":
    raise SystemExit(main())
