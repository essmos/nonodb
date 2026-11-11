"""End-to-end SQL test against a real daemon.

    cargo build -p nonodb-daemon
    python3 scripts/sql_test.py

The script starts its own daemon in a temporary directory, exercises the SQL
surface in Python and in a read batch, and verifies error mapping.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2

    tmp = Path(tempfile.mkdtemp(prefix="nonodb-sql-"))
    sock = tmp / "nonodb.sock"
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
        }
    )
    proc = subprocess.Popen(
        [str(daemon)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        users = db.collection("users")
        for doc in [
            {"_id": "a", "name": "Ada", "age": 36, "team": "core", "score": 10},
            {"_id": "b", "name": "Grace", "age": 31, "team": "core", "score": 20},
            {"_id": "c", "name": "Linus", "age": 25, "team": "kernel", "score": 30},
            {"_id": "d", "name": "Zażółć", "age": 28, "team": "kernel"},
        ]:
            users.insert(doc)

        rows = db.sql(
            "SELECT name, age FROM users WHERE age >= $1 ORDER BY age DESC",
            [31],
        )
        assert rows == [
            {"name": "Ada", "age": 36},
            {"name": "Grace", "age": 31},
        ], rows

        rows = db.sql(
            "SELECT u.team AS team, COUNT(*) AS n, SUM(u.score) AS total "
            "FROM users AS u WHERE u.team IN ('core', 'kernel') "
            "GROUP BY u.team ORDER BY team"
        )
        assert rows == [
            {"team": "core", "n": 2, "total": 30},
            {"team": "kernel", "n": 2, "total": 30},
        ], rows

        rows = db.sql(
            "SELECT name FROM users WHERE name LIKE 'Z%' OR age BETWEEN 30 AND 40 "
            "ORDER BY name"
        )
        assert rows == [{"name": "Ada"}, {"name": "Grace"}, {"name": "Zażółć"}], rows

        rows = db.sql("SELECT COUNT(*) AS n, AVG(score) AS avg FROM users")
        assert rows == [{"n": 4, "avg": 20}], rows

        rows = db.sql("SELECT DISTINCT team FROM users ORDER BY team")
        assert rows == [{"team": "core"}, {"team": "kernel"}], rows

        rows = db.sql(
            "SELECT team, COUNT(*) AS n FROM users GROUP BY team "
            "HAVING COUNT(*) >= 2 ORDER BY team"
        )
        assert rows == [
            {"team": "core", "n": 2},
            {"team": "kernel", "n": 2},
        ], rows

        rows = db.sql(
            "SELECT CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS band, "
            "upper(name) AS shout, age + 1 AS next_age FROM users WHERE name = 'Ada'"
        )
        assert rows == [{"band": "senior", "shout": "ADA", "next_age": 37}], rows

        plan = db.explain_sql("SELECT name FROM users WHERE age > $1 LIMIT 1", [30])
        assert plan.get("access_path") in {"collection_scan", "parallel_scan", "scan"}, plan

        plan = db.explain_sql(
            "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1"
        )
        assert plan["access_path"] == "aggregation_group", plan
        assert plan["has_having"] is True, plan

        batch = db.batch(
            [
                {"op": "sql", "query": "SELECT COUNT(*) AS n FROM users", "params": []},
                {"op": "sql", "query": "SELECT name FROM users WHERE age > $1", "params": [30]},
                {
                    "op": "explain_sql",
                    "query": "SELECT DISTINCT team FROM users",
                    "params": [],
                },
            ]
        )
        assert batch[0]["ok"] and batch[0]["result"]["results"] == [{"n": 4}], batch
        assert batch[1]["ok"] and len(batch[1]["result"]["results"]) == 2, batch
        assert batch[2]["ok"] and batch[2]["result"]["access_path"] == "aggregation_distinct", batch

        try:
            db.sql("DELETE FROM users")
        except NonoDBError as exc:
            assert exc.code == "invalid_sql", exc
        else:
            raise AssertionError("write SQL must be rejected")

        missing = db.sql("SELECT * FROM missing_collection")
        assert missing == [], missing

        # Exercise the same paths over a larger real dataset, not just four rows.
        bulk = db.collection("sql_bulk")
        docs = [
            {
                "name": f"user{i:05d}",
                "age": 18 + (i % 60),
                "team": f"t{i % 7}",
                "score": i % 100,
                "city": None if i % 3 == 0 else "city",
            }
            for i in range(10_000)
        ]
        for start in range(0, len(docs), 1_000):
            bulk.insert_many(docs[start : start + 1_000])

        rows = db.sql("SELECT COUNT(*) AS n FROM sql_bulk")
        assert rows == [{"n": 10_000}], rows

        rows = db.sql("SELECT DISTINCT team FROM sql_bulk ORDER BY team")
        assert rows == [{"team": f"t{i}"} for i in range(7)], rows

        expected = {}
        for doc in docs:
            count, total = expected.get(doc["team"], (0, 0))
            expected[doc["team"]] = (count + 1, total + doc["score"])
        rows = db.sql(
            "SELECT team, COUNT(*) AS n, SUM(score) AS total FROM sql_bulk "
            "GROUP BY team HAVING n > 1000 ORDER BY team"
        )
        assert rows == [
            {"team": team, "n": count, "total": total}
            for team, (count, total) in sorted(expected.items())
        ], rows

        ages = Counter(doc["age"] for doc in docs if doc["age"] >= 70)
        rows = db.sql(
            "SELECT age, COUNT(*) AS n FROM sql_bulk "
            "WHERE age >= $1 GROUP BY age ORDER BY age",
            [70],
        )
        assert rows == [{"age": age, "n": n} for age, n in sorted(ages.items())], rows

        bulk.create_index("age")
        plan = db.explain_sql(
            "SELECT name, age FROM sql_bulk WHERE age >= 70 ORDER BY age LIMIT 5"
        )
        assert plan["access_path"] == "index_scan" and plan["index_used"] is True, plan
        rows = db.sql(
            "SELECT name, age FROM sql_bulk WHERE age >= 70 ORDER BY age LIMIT 5"
        )
        assert len(rows) == 5 and all(row["age"] >= 70 for row in rows), rows

        rows = db.sql(
            "SELECT COALESCE(city, 'unknown') AS city, LENGTH(name) AS name_len, "
            "score + 1 AS next_score, LOWER(team) AS lower_team FROM sql_bulk "
            "WHERE name = 'user00001'"
        )
        assert rows == [
            {
                "city": "city",
                "name_len": 9,
                "next_score": 2,
                "lower_team": "t1",
            }
        ], rows

        db.close()
        print("OK: SQL real-daemon 10k-row distinct/having/CASE/functions/index/explain/params/batch/errors")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
