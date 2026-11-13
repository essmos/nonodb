#!/usr/bin/env python3
"""Smoke test for multiple databases in one daemon (Unix socket)."""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402

BIN = Path(os.environ.get("NONODB_BIN", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def request(sock: Path, path: str, payload: dict) -> dict:
    body = json.dumps(payload).encode()
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(10)
        s.connect(str(sock))
        s.sendall(
            f"POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        raw = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            raw += chunk
    head, _, raw_body = raw.partition(b"\r\n\r\n")
    if b" 200 " not in head.split(b"\r\n", 1)[0]:
        raise AssertionError(f"{path}: {head!r} {raw_body!r}")
    data = json.loads(raw_body)
    if not data.get("ok"):
        raise AssertionError(f"{path}: {data}")
    return data["result"]


def wait_socket(path: Path, proc: subprocess.Popen) -> None:
    for _ in range(100):
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def main() -> int:
    if not BIN.is_file():
        raise SystemExit(f"missing binary: {BIN}; run cargo build")
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-multi-"))
    data = tmp / "data"
    sock = tmp / "nonodb.sock"
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(sock),
        NONODB_MAINTENANCE_INTERVAL_MS="100",
        NONODB_MAX_OPEN_DATABASES="2",
        NONODB_LOG_FORMAT="json",
    )
    proc = subprocess.Popen([str(BIN)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_socket(sock, proc)
        assert request(sock, "/v1/exec", {"op": "list_databases"})["databases"] == ["default"]
        for name in ("app1", "app2"):
            request(sock, "/v1/exec", {"op": "create_database", "name": name})

        for name in ("app1", "app2", "default"):
            for _ in range(2):
                request(sock, f"/{name}/v1/exec", {"op": "insert", "coll": "orders", "doc": {"db": name}})
        for name in ("app1", "app2", "default"):
            docs = request(sock, f"/{name}/v1/exec", {"op": "find", "coll": "orders", "filter": {}})
            assert [doc["db"] for doc in docs] == [name, name], (name, docs)

        # Official Python client database selection over UDS.
        py = NonoDB(str(sock), database="app1")
        assert [doc["db"] for doc in py.collection("orders").find({})] == ["app1", "app1"]
        py.close()

        # Cursors must remember the database that created them.
        cursor = request(
            sock,
            "/app1/v1/exec",
            {"op": "cursor_find", "coll": "orders", "filter": {}, "batch_size": 1},
        )
        assert [doc["db"] for doc in cursor["docs"]] == ["app1"], cursor
        more = request(sock, "/v1/exec", {"op": "get_more", "cursor_id": cursor["cursor_id"]})
        assert [doc["db"] for doc in more["docs"]] == ["app1"], more

        # Live subscriptions are connection-scoped and must not leak across
        # databases with the same collection name.
        transport = httpx.HTTPTransport(uds=str(sock))
        client = httpx.Client(
            transport=transport,
            base_url="http://localhost",
            timeout=httpx.Timeout(2.0, read=0.6),
        )
        request(
            sock,
            "/app1/v1/exec",
            {"op": "create_collection", "coll": "live_orders"},
        )
        stream_ctx = client.stream(
            "POST",
            "/app1/v1/watch",
            content=json.dumps({
                "op": "watch",
                "coll": "live_orders",
                "mode": "live",
                "filter": {"db": "app1"},
                "heartbeat_ms": 100,
            }),
            headers={
                "content-type": "application/json",
                "accept": "application/x-ndjson",
            },
        )
        stream = stream_ctx.__enter__()
        assert stream.status_code == 200, stream.read()
        lines = stream.iter_lines()
        ready = json.loads(next(lines))
        assert ready["stream"]["mode"] == "live", ready
        request(
            sock,
            "/app1/v1/exec",
            {"op": "insert", "coll": "live_orders", "doc": {"db": "app1"}},
        )
        event = None
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            item = json.loads(next(lines))
            if "heartbeat" not in item and "stream" not in item:
                event = item
                break
        assert event and event["payload"]["after"]["db"] == "app1", event
        request(
            sock,
            "/v1/exec",
            {"op": "insert", "coll": "live_orders", "doc": {"db": "default"}},
        )
        try:
            deadline = time.monotonic() + 0.9
            while time.monotonic() < deadline:
                item = json.loads(next(lines))
                assert "heartbeat" in item or "stream" in item, item
        except httpx.ReadTimeout:
            pass
        stream_ctx.__exit__(None, None, None)
        client.close()

        # Unknown databases are 404, not implicitly created.
        try:
            request(sock, "/typo/v1/exec", {"op": "list_collections"})
        except AssertionError:
            pass
        else:
            raise AssertionError("unknown database was created implicitly")
        names = request(sock, "/v1/exec", {"op": "list_databases"})["databases"]
        assert "typo" not in names, names

        # Management is instance-wide.
        assert request(sock, "/v1/exec", {"op": "list_databases"})["databases"] == [
            "app1",
            "app2",
            "default",
        ]

        # Persistence after a clean restart.
        proc.terminate()
        assert proc.wait(timeout=10) == 0
        proc = subprocess.Popen(
            [str(BIN)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        wait_socket(sock, proc)
        docs = request(sock, "/app2/v1/exec", {"op": "find", "coll": "orders", "filter": {}})
        assert [doc["db"] for doc in docs] == ["app2", "app2"], docs

        assert request(sock, "/v1/exec", {"op": "drop_database", "name": "app1"})["dropped"]
        names = request(sock, "/v1/exec", {"op": "list_databases"})["databases"]
        assert names == ["app2", "default"], names

        # Whole-instance backup and restore.
        if not CLI.is_file():
            raise RuntimeError(f"missing CLI binary: {CLI}")
        proc.terminate()
        assert proc.wait(timeout=10) == 0
        backup = tmp / "backup"
        subprocess.run(
            [str(CLI), "backup-all", str(data), str(backup)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        shutil.rmtree(data)
        subprocess.run(
            [str(CLI), "restore-all", str(backup), str(data), "--force"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        proc = subprocess.Popen(
            [str(BIN)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        wait_socket(sock, proc)
        names = request(sock, "/v1/exec", {"op": "list_databases"})["databases"]
        assert names == ["app2", "default"], names
        docs = request(sock, "/app2/v1/exec", {"op": "find", "coll": "orders", "filter": {}})
        assert [doc["db"] for doc in docs] == ["app2", "app2"], docs
        default_docs = request(sock, "/v1/exec", {"op": "find", "coll": "orders", "filter": {}})
        assert [doc["db"] for doc in default_docs] == ["default", "default"], default_docs

        print("multi-database smoke test: ok")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
