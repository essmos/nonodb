"""End-to-end auth registry, scopes, reload, and audit test.

    python3 scripts/auth_test.py

Optional:
    NONODB_DAEMON=./target/release/nonodb-daemon
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

import httpx  # noqa: E402
from nonodb import NonoDB, NonoDBError  # noqa: E402


def binary(env: str, name: str) -> Path:
    raw = os.environ.get(env)
    if raw:
        path = Path(raw)
    else:
        path = next(
            (p for p in (ROOT / "target/debug" / name, ROOT / "target/release" / name) if p.is_file()),
            ROOT / "target/debug" / name,
        )
    if not path.is_file():
        raise SystemExit(f"missing binary: {path}")
    return path


def token(kid: str, secret: str) -> str:
    return f"nodb_{kid}_{secret}"


def entry(kid: str, secret: str, principal: str) -> dict:
    return {
        "kid": kid,
        "sha256": hashlib.sha256(secret.encode()).hexdigest(),
        "principal": principal,
        "disabled": False,
    }


def auth_doc() -> dict:
    return {
        "version": 1,
        "principals": {
            "admin": {"scopes": ["*:*"]},
            "app": {"scopes": ["read:orders", "write:orders"]},
            "reader": {"scopes": ["read:orders"]},
            "wide": {"scopes": ["read:orders", "read:customers"]},
        },
        "tokens": [
            entry("admin", "admin-secret", "admin"),
            entry("app", "app-secret", "app"),
            entry("reader", "reader-secret", "reader"),
            entry("wide", "wide-secret", "wide"),
        ],
        "certificates": [],
    }


def write_auth(path: Path, doc: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2) + "\n")
    os.replace(tmp, path)


def expect_code(fn, code: str) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, (exc.code, exc)
    else:
        raise AssertionError(f"expected {code}")


def wait_tcp(port: int, proc: subprocess.Popen) -> None:
    for _ in range(200):
        if proc.poll() is not None:
            raise RuntimeError("daemon exited during startup")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("TCP listener did not start")


def main() -> None:
    daemon = binary("NONODB_DAEMON", "nonodb-daemon")
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        data = tmp / "data"
        sock = tmp / "nonodb.sock"
        auth_file = tmp / "auth.json"
        audit = tmp / "audit.jsonl"
        log = tmp / "daemon.log"
        write_auth(auth_file, auth_doc())

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()

        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(sock),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_AUDIT_LOG=str(audit),
            NONODB_AUTH_MAX_FAILURES="6",
            NONODB_AUTH_FAILURE_WINDOW="60",
            NONODB_AUTH_BLOCK_SECS="2",
            NONODB_MAP_SIZE="64MiB",
            RUST_LOG="warn",
        )
        fh = log.open("wb")
        proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
        url = f"http://127.0.0.1:{port}"
        try:
            wait_tcp(port, proc)
            admin = NonoDB(base_url=url, token=token("admin", "admin-secret"), timeout=10)
            app = NonoDB(base_url=url, token=token("app", "app-secret"), timeout=10)
            reader = NonoDB(base_url=url, token=token("reader", "reader-secret"), timeout=10)
            wide = NonoDB(base_url=url, token=token("wide", "wide-secret"), timeout=10)
            anon = NonoDB(base_url=url, timeout=10)

            # Missing/bad credentials.
            expect_code(lambda: anon.collections(), "unauthorized")
            bad = NonoDB(base_url=url, token="nodb_reader_nope", timeout=10)
            expect_code(lambda: bad.collections(), "unauthorized")

            # Admin bootstraps two collections.
            admin.create_collection("orders")
            admin.create_collection("customers")
            admin.collection("orders").insert({"_id": "o1", "total": 10})
            admin.collection("customers").insert({"_id": "c1", "name": "Ada"})

            # Reader: read one collection only.
            assert reader.collection("orders").count({}) == 1
            assert reader.collections() == ["orders"]
            expect_code(
                lambda: reader.collection("orders").insert({"total": 11}),
                "forbidden",
            )
            expect_code(lambda: reader.collection("customers").find({}), "forbidden")
            expect_code(lambda: reader.map_info(), "forbidden")
            assert reader.sql("SELECT COUNT(*) AS n FROM orders")[0]["n"] == 1
            expect_code(
                lambda: reader.sql("UPDATE orders SET total = 99 WHERE _id = 'o1'"),
                "forbidden",
            )
            expect_code(
                lambda: reader.sql("INSERT INTO orders (total) VALUES (99)"),
                "forbidden",
            )

            # App: read/write orders, but not admin or another collection.
            app.collection("orders").insert({"_id": "o2", "total": 20})
            updated = app.sql(
                "UPDATE orders SET total = $1 WHERE _id = $2",
                [11, "o1"],
            )
            assert updated[0]["matched"] == 1, updated
            assert app.collection("orders").find_one({"_id": "o1"})["total"] == 11
            expect_code(
                lambda: app.sql("UPDATE customers SET name = 'X' WHERE _id = 'c1'"),
                "forbidden",
            )
            expect_code(
                lambda: app.collection("customers").insert({"name": "Grace"}),
                "forbidden",
            )
            expect_code(
                lambda: app.write_batch(
                    [
                        {
                            "op": "insert",
                            "coll": "orders",
                            "doc": {"_id": "o3", "total": 30},
                        },
                        {
                            "op": "insert",
                            "coll": "customers",
                            "doc": {"_id": "c2", "name": "Linus"},
                        },
                    ]
                ),
                "forbidden",
            )
            assert app.collection("orders").find_one({"_id": "o3"}) is None
            expect_code(lambda: app.collection("orders").create_index("total"), "forbidden")

            # Admin-only operations.
            admin.collection("orders").create_index("total")
            assert admin.map_info()["map_size"] > 0
            assert admin.collections() == ["customers", "orders"]
            assert reader.collections() == ["orders"]
            assert app.collections() == ["orders"]

            # Metrics require admin:db.
            assert httpx.get(url + "/metrics", timeout=10).status_code == 401
            assert (
                httpx.get(
                    url + "/metrics",
                    headers={"authorization": f"Bearer {token('app', 'app-secret')}"},
                    timeout=10,
                ).status_code
                == 403
            )
            metrics_response = httpx.get(
                url + "/metrics",
                headers={"authorization": f"Bearer {token('admin', 'admin-secret')}"},
                timeout=10,
            )
            assert metrics_response.status_code == 200
            metrics = metrics_response.json()
            assert metrics["auth_failures_total"] > 0
            assert metrics["auth_blocks_total"] == 0
            assert metrics["auth_blocked_clients"] == 0

            # UDS remains a trusted local administrative channel.
            local = NonoDB(socket_path=str(sock), timeout=10)
            assert "orders" in local.collections()
            assert "customers" in local.collections()
            local.close()

            # A cached list_collections result must not survive a scope change.
            assert "customers" in wide.collections()
            assert wide.collection("customers").count({}) >= 1
            doc = auth_doc()
            doc["principals"]["wide"]["scopes"] = ["read:orders"]
            write_auth(auth_file, doc)
            time.sleep(2.0)
            visible = wide.collections()
            assert "orders" in visible, visible
            assert "customers" not in visible, visible
            expect_code(lambda: wide.collection("customers").count({}), "forbidden")

            # File watcher revokes a token without a restart.
            doc = auth_doc()
            doc["tokens"][1]["disabled"] = True
            write_auth(auth_file, doc)
            time.sleep(2.0)
            expect_code(lambda: app.collection("orders").find({}), "unauthorized")
            assert reader.collection("orders").count({}) == 2

            # Token secrets and vault keys never reach the audit log.
            audit_text = audit.read_text()
            assert "app-secret" not in audit_text
            assert "admin-secret" not in audit_text
            assert "reader-secret" not in audit_text
            records = [json.loads(line) for line in audit_text.splitlines() if line.strip()]
            assert any(
                r["principal"] == "app" and r["op"] == "insert" and r["result"] == "ok"
                for r in records
            ), records
            assert any(
                r["principal"] == "app" and r["result"] == "error" for r in records
            ), records

            # No successful read audit entries by default; denied reads are still audited.
            assert not any(
                r["op"] == "find" and r["result"] == "ok" for r in records
            ), records

            # Repeated bad credentials lock the peer IP and are audited with a
            # failure count plus the block duration.
            codes = []
            for _ in range(8):
                try:
                    bad.collection("orders").find({})
                except NonoDBError as exc:
                    codes.append(exc.code)
                else:
                    raise AssertionError("bad credentials unexpectedly succeeded")
            assert codes[-1] == "rate_limited", codes
            assert "rate_limited" in codes, codes
            records = [json.loads(line) for line in audit.read_text().splitlines() if line.strip()]
            blocked = [r for r in records if r.get("blocked_for_secs")]
            assert blocked, records[-5:]
            assert blocked[-1]["auth_failures"] >= 6, blocked[-1]
            assert blocked[-1]["result"] == "error"

            uds_http = httpx.Client(transport=httpx.HTTPTransport(uds=str(sock)))
            metrics = uds_http.get("http://localhost/metrics", timeout=10).json()
            uds_http.close()
            assert metrics["auth_blocks_total"] == 1, metrics
            assert metrics["auth_blocked_clients"] == 1, metrics
            assert metrics["auth_blocked_requests_total"] >= 1, metrics
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            fh.close()

        # TCP startup without an auth file must fail.
        env.pop("NONODB_AUTH_FILE", None)
        bad = subprocess.run(
            [str(daemon)],
            env={**env, "NONODB_DATA": str(tmp / "data2"), "NONODB_SOCK": str(tmp / "s2")},
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert bad.returncode != 0
        assert "NONODB_AUTH_FILE" in (bad.stdout + bad.stderr)

    print("auth OK")


if __name__ == "__main__":
    main()
