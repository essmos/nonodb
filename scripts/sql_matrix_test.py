"""Comprehensive real-daemon SQL query matrix.

    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/sql_matrix_test.py

Exercises every supported SELECT form, write statements, error paths,
concurrent writes, restart durability, metrics, and the SQL CLI.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

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


def start_daemon(daemon: Path, env: dict[str, str], log: Path) -> subprocess.Popen[bytes]:
    fh = log.open("ab")
    proc = subprocess.Popen(
        [str(daemon)],
        env=env,
        stdout=fh,
        stderr=subprocess.STDOUT,
    )
    fh.close()
    wait_for_socket(Path(env["NONODB_SOCK"]), proc)
    return proc


def stop_daemon(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def expect_sql_error(db: NonoDB, query: str, params: list | None = None) -> None:
    try:
        db.sql(query, params or [])
    except NonoDBError as exc:
        assert exc.code == "invalid_sql", (exc.code, exc, query)
    else:
        raise AssertionError(f"expected invalid_sql: {query}")


def check(db: NonoDB, query: str, expected, params: list | None = None) -> None:
    rows = db.sql(query, params or [])
    assert rows == expected, (query, rows, expected)


def seed(db: NonoDB) -> None:
    users = db.collection("users")
    users.insert_many(
        [
            {"_id": "a", "name": "Ada", "age": 36, "city": "London", "team": "core", "score": 10, "active": True},
            {"_id": "b", "name": "Grace", "age": 31, "city": "New York", "team": "core", "score": 20, "active": True},
            {"_id": "c", "name": "Linus", "age": 25, "city": "Helsinki", "team": "kernel", "score": 30, "active": False},
            {"_id": "d", "name": "Margaret", "age": 28, "city": "New York", "team": "kernel", "score": 40, "active": False},
            {"_id": "e", "name": "Noor", "age": 36, "city": None, "team": "core", "active": True},
            {"_id": "f", "name": "Zażółć", "age": 28, "city": "Zürich", "team": "ops", "score": 50, "active": False},
        ]
    )


def select_matrix(db: NonoDB) -> None:
    check(db, "SELECT * FROM users WHERE _id = 'a'", [
        {"_id": "a", "name": "Ada", "age": 36, "city": "London", "team": "core", "score": 10, "active": True}
    ])
    check(db, "SELECT _id FROM users WHERE _id IN ('a', 'b') ORDER BY _id", [
        {"_id": "a"}, {"_id": "b"}
    ])
    check(db, "SELECT name, age FROM users WHERE age >= 30 ORDER BY age DESC, name", [
        {"name": "Ada", "age": 36},
        {"name": "Noor", "age": 36},
        {"name": "Grace", "age": 31},
    ])
    check(db, "SELECT name AS person, age + 1 AS next_age FROM users WHERE _id = 'a'", [
        {"person": "Ada", "next_age": 37}
    ])
    check(db, "SELECT u.name FROM users AS u WHERE u._id = 'a'", [{"name": "Ada"}])
    check(db, "SELECT name FROM users WHERE age = 36 ORDER BY name", [
        {"name": "Ada"}, {"name": "Noor"}
    ])
    check(db, "SELECT name FROM users WHERE age != 36 ORDER BY name", [
        {"name": "Grace"}, {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE age <> 36 ORDER BY name", [
        {"name": "Grace"}, {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE age < 28", [{"name": "Linus"}])
    check(db, "SELECT name FROM users WHERE age <= 28 ORDER BY name", [
        {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE age > 36", [])
    check(db, "SELECT name FROM users WHERE age >= 36 ORDER BY name", [
        {"name": "Ada"}, {"name": "Noor"}
    ])
    check(db, "SELECT name FROM users WHERE team IN ('ops', 'kernel') ORDER BY name", [
        {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE team NOT IN ('core', 'ops') ORDER BY name", [
        {"name": "Linus"}, {"name": "Margaret"}
    ])
    check(db, "SELECT name FROM users WHERE age BETWEEN 25 AND 28 ORDER BY name", [
        {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE age NOT BETWEEN 25 AND 28 ORDER BY name", [
        {"name": "Ada"}, {"name": "Grace"}, {"name": "Noor"}
    ])
    check(db, "SELECT name FROM users WHERE city IS NULL", [{"name": "Noor"}])
    check(db, "SELECT name FROM users WHERE city IS NOT NULL ORDER BY name", [
        {"name": "Ada"}, {"name": "Grace"}, {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users WHERE name LIKE 'A%'", [{"name": "Ada"}])
    check(db, "SELECT name FROM users WHERE name NOT LIKE '%a%' ORDER BY name", [
        {"name": "Linus"}, {"name": "Noor"}
    ])
    check(db, "SELECT name FROM users WHERE (team = 'core' AND age > 32) OR name = 'Linus' ORDER BY name", [
        {"name": "Ada"}, {"name": "Linus"}, {"name": "Noor"}
    ])
    check(db, "SELECT name FROM users WHERE NOT (team = 'core') ORDER BY name", [
        {"name": "Linus"}, {"name": "Margaret"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT LOWER(name) AS lower_name, UPPER(name) AS upper_name, LENGTH(name) AS len FROM users WHERE _id = 'a'", [
        {"lower_name": "ada", "upper_name": "ADA", "len": 3}
    ])
    check(db, "SELECT SUBSTR(name, 1, 2) AS mid, TRIM('  x  ') AS trimmed, LTRIM('  x  ') AS left_trim, RTRIM('  x  ') AS right_trim FROM users WHERE _id = 'a'", [
        {"mid": "da", "trimmed": "x", "left_trim": "x  ", "right_trim": "  x"}
    ])
    check(db, "SELECT COALESCE(city, 'unknown') AS city, NULLIF(age, 36) AS maybe FROM users WHERE _id = 'e'", [
        {"city": "unknown", "maybe": None}
    ])
    check(db, "SELECT ABS(-5) AS a, CEIL(1.2) AS c, FLOOR(1.8) AS f, ROUND(1.234, 2) AS r FROM users WHERE _id = 'a'", [
        {"a": 5, "c": 2, "f": 1, "r": 1.23}
    ])
    check(db, "SELECT CASE WHEN age >= 35 THEN 'senior' WHEN age >= 28 THEN 'mid' ELSE 'junior' END AS band FROM users WHERE _id = 'a'", [
        {"band": "senior"}
    ])
    check(db, "SELECT CASE WHEN age >= 60 THEN 'old' END AS band FROM users WHERE _id = 'a'", [
        {"band": None}
    ])
    check(db, "SELECT name, age * 2 + 1 AS n, name || '!' AS bang FROM users WHERE _id = 'a'", [
        {"name": "Ada", "n": 73, "bang": "Ada!"}
    ])
    check(db, "SELECT DISTINCT team FROM users ORDER BY team", [
        {"team": "core"}, {"team": "kernel"}, {"team": "ops"}
    ])
    check(db, "SELECT DISTINCT LOWER(team) AS team FROM users ORDER BY team", [
        {"team": "core"}, {"team": "kernel"}, {"team": "ops"}
    ])
    check(db, "SELECT DISTINCT * FROM users WHERE _id = 'a'", [
        {"_id": "a", "name": "Ada", "age": 36, "city": "London", "team": "core", "score": 10, "active": True}
    ])
    check(db, "SELECT COUNT(*) AS n, COUNT(score) AS scored, SUM(score) AS total, AVG(score) AS avg, MIN(score) AS lo, MAX(score) AS hi FROM users", [
        {"n": 6, "scored": 5, "total": 150, "avg": 30, "lo": 10, "hi": 50}
    ])
    check(db, "SELECT COUNT(DISTINCT team) AS teams, COUNT(DISTINCT city) AS cities FROM users", [
        {"teams": 3, "cities": 4}
    ])
    check(db, "SELECT team, COUNT(*) AS n, SUM(score) AS total FROM users GROUP BY team ORDER BY team", [
        {"team": "core", "n": 3, "total": 30},
        {"team": "kernel", "n": 2, "total": 70},
        {"team": "ops", "n": 1, "total": 50},
    ])
    check(db, "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING n >= 2 ORDER BY team", [
        {"team": "core", "n": 3}, {"team": "kernel", "n": 2}
    ])
    check(db, "SELECT team FROM users GROUP BY team HAVING SUM(score) >= 70 ORDER BY team", [
        {"team": "kernel"}
    ])
    check(db, "SELECT DISTINCT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1 ORDER BY team", [
        {"team": "core", "n": 3}, {"team": "kernel", "n": 2}
    ])
    check(db, "SELECT name FROM users WHERE age >= $1 ORDER BY name LIMIT 2", [
        {"name": "Ada"}, {"name": "Noor"}
    ], [35])
    check(db, "SELECT name FROM users ORDER BY name LIMIT 2, 2", [
        {"name": "Linus"}, {"name": "Margaret"}
    ])
    check(db, "SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 2", [
        {"name": "Linus"}, {"name": "Margaret"}
    ])
    check(db, "SELECT name FROM users ORDER BY name OFFSET 4", [
        {"name": "Noor"}, {"name": "Zażółć"}
    ])
    check(db, "SELECT name FROM users -- comment\nWHERE _id = 'a'", [{"name": "Ada"}])
    check(db, "SELECT name /* block */ FROM users WHERE _id = 'a'", [{"name": "Ada"}])
    check(db, 'SELECT "name" FROM "users" WHERE "_id" = \'a\'', [{"name": "Ada"}])
    check(db, "SELECT name FROM users WHERE name = 'Zażółć'", [{"name": "Zażółć"}])


def write_matrix(db: NonoDB) -> None:
    out = db.sql(
        "INSERT INTO write_users (name, age, profile.city) VALUES ('A', 20, 'X'), ('B', $1, 'Y')",
        [30],
    )
    assert out[0]["inserted"] == 2, out
    out = db.sql(
        "INSERT INTO write_users (_id, name, age) VALUES ('c', 'C', 40)"
    )
    assert out[0]["inserted_ids"] == ["c"], out

    out = db.sql("UPDATE write_users SET age = age + $1 WHERE name = $2", [5, "A"])
    assert out[0] == {"matched": 1, "modified": 1}, out
    out = db.sql("UPDATE write_users SET active = TRUE")
    assert out[0] == {"matched": 3, "modified": 3}, out
    out = db.sql("UPDATE write_users AS w SET w.profile.level = $1 WHERE w.name = $2", [9, "B"])
    assert out[0] == {"matched": 1, "modified": 1}, out

    check(db, "SELECT name, age, active, profile.city AS city, profile.level AS level FROM write_users ORDER BY name", [
        {"name": "A", "age": 25, "active": True, "city": "X", "level": None},
        {"name": "B", "age": 30, "active": True, "city": "Y", "level": 9},
        {"name": "C", "age": 40, "active": True, "city": None, "level": None},
    ])
    out = db.sql("DELETE FROM write_users AS w WHERE w.age < $1", [30])
    assert out[0] == {"deleted": 1}, out
    check(db, "SELECT name FROM write_users ORDER BY name", [{"name": "B"}, {"name": "C"}])

    rejected = db.batch([
        {"op": "sql", "query": "INSERT INTO write_users (name) VALUES ('batch')", "params": []}
    ])
    assert rejected[0]["ok"] is False and rejected[0]["error"]["code"] == "invalid_sql", rejected


def error_matrix(db: NonoDB) -> None:
    for query in [
        "SELEC name FROM users",
        "SELECT name FROM",
        "SELECT * FROM users WHERE age =",
        "SELECT SUM(DISTINCT score) FROM users",
        "SELECT * FROM users GROUP BY team",
        "SELECT name, COUNT(*) FROM users GROUP BY team",
        "SELECT team, COUNT(*) AS n FROM users GROUP BY team ORDER BY score",
        "SELECT age + 1 FROM users GROUP BY team",
        "SELECT * FROM users JOIN write_users ON users._id = write_users._id",
        "SELECT * FROM users WHERE age IN (SELECT age FROM write_users)",
        "INSERT INTO users (name, age) VALUES ('x')",
        "INSERT INTO users (name, name) VALUES ('x', 'y')",
        "INSERT INTO users (name) VALUES ('x') RETURNING *",
        "UPDATE users SET age = age + 1 RETURNING *",
        "DELETE FROM users RETURNING *",
        "SELECT * FROM users % 2",
        "CREATE TABLE nope (id INT)",
        "BEGIN",
    ]:
        expect_sql_error(db, query)
    expect_sql_error(db, "SELECT name FROM users WHERE age = $2", [1])
    expect_sql_error(db, "SELECT SUM(DISTINCT score) FROM users")


def concurrent_matrix(sock: str) -> None:
    def insert_batch(worker: int) -> None:
        db = NonoDB(sock, cache_ttl=0.0)
        try:
            for i in range(10):
                out = db.sql(
                    "INSERT INTO sql_concurrent (worker, seq, age) VALUES ($1, $2, 1)",
                    [worker, i],
                )
                assert out[0]["inserted"] == 1, out
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(insert_batch, range(8)))

    db = NonoDB(sock, cache_ttl=0.0)
    try:
        assert db.sql("SELECT COUNT(*) AS n FROM sql_concurrent") == [{"n": 80}]

        def bump(_: int) -> None:
            client = NonoDB(sock, cache_ttl=0.0)
            try:
                for _ in range(5):
                    out = client.sql("UPDATE sql_concurrent SET age = age + 1")
                    assert out[0]["matched"] == 80, out
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(bump, range(8)))
        check(db, "SELECT MIN(age) AS lo, MAX(age) AS hi FROM sql_concurrent", [{"lo": 41, "hi": 41}])
        out = db.sql("DELETE FROM sql_concurrent")
        assert out[0] == {"deleted": 80}, out
    finally:
        db.close()


def metrics_matrix(sock: Path, slow_log: Path) -> None:
    uds = httpx.Client(transport=httpx.HTTPTransport(uds=str(sock)))
    try:
        metrics = uds.get("http://localhost/metrics", timeout=10).json()
    finally:
        uds.close()
    assert metrics["sql_select_total"] > 0, metrics
    assert metrics["sql_insert_total"] > 0, metrics
    assert metrics["sql_update_total"] > 0, metrics
    assert metrics["sql_delete_total"] > 0, metrics
    assert metrics["sql_errors_total"] > 0, metrics
    assert metrics["sql_duration_ns_total"] > 0, metrics
    assert "slow SQL statement" in slow_log.read_text(errors="replace")


def cli_matrix(sock: Path) -> None:
    cli = ROOT / "target/debug/nonodb-cli"
    if not cli.exists():
        return
    env = os.environ.copy()
    env["NONODB_SOCK"] = str(sock)
    res = subprocess.run(
        [str(cli), "sql", "SELECT COUNT(*) AS n FROM users"],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(res.stdout)
    assert payload["results"] == [{"n": 6}], payload
    res = subprocess.run(
        [str(cli), "sql", "SELECT COUNT(*) AS n FROM users WHERE age >= $1", "--param", "30"],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(res.stdout)
    assert payload["results"] == [{"n": 3}], payload
    query_file = sock.parent / "query.sql"
    query_file.write_text("SELECT COUNT(*) AS n FROM users WHERE age >= $1\n")
    res = subprocess.run(
        [str(cli), "sql", "--file", str(query_file), "--param", "30"],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(res.stdout)
    assert payload["results"] == [{"n": 3}], payload
    shell = subprocess.run(
        [str(cli), "shell"],
        env=env,
        input="SELECT COUNT(*) AS n FROM users\n.quit\n",
        text=True,
        capture_output=True,
        check=True,
    )
    assert '"n": 6' in shell.stdout, shell.stdout


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-sql-matrix-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update({
        "NONODB_DATA": str(tmp / "data"),
        "NONODB_SOCK": str(sock),
        "NONODB_LOG": "warn",
        "NONODB_SQL_SLOW_MS": "1",
    })
    proc = start_daemon(daemon, env, log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            seed(db)
            select_matrix(db)
            write_matrix(db)
            error_matrix(db)
        finally:
            db.close()
        concurrent_matrix(str(sock))
        metrics_matrix(sock, log)
        cli_matrix(sock)
        stop_daemon(proc)
        proc = start_daemon(daemon, env, log)
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            check(db, "SELECT COUNT(*) AS n FROM users", [{"n": 6}])
            check(db, "SELECT name FROM write_users ORDER BY name", [{"name": "B"}, {"name": "C"}])
            assert db.collection("sql_concurrent").count({}) == 0
        finally:
            db.close()
        print("OK: SQL matrix (all SELECT forms, INSERT/UPDATE/DELETE, errors, concurrency, restart, metrics, CLI)")
        return 0
    finally:
        stop_daemon(proc)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
