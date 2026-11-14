#!/usr/bin/env python3
"""Replication of collection metadata, indexes, TTL, history, events, and live watch."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_health(client: httpx.Client, proc: subprocess.Popen[bytes], name: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{name} exited with {proc.returncode}")
        try:
            if client.get("/healthz", timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"{name} did not become healthy")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-metadata-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: list[subprocess.Popen[bytes]] = []
    clients: list[httpx.Client] = []
    try:
        env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
        subprocess.run([str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True)
        token = subprocess.run(
            [str(CLI), "auth", "token", "create", "admin"],
            env=env_auth,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        headers = {"Authorization": f"Bearer {token}"}
        ports = {"primary": free_port(), "replica": free_port()}

        def start(name: str, role: str, extra: dict[str, str]) -> tuple[httpx.Client, subprocess.Popen]:
            data = temp / f"{name}-data"
            data.mkdir()
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(temp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{ports[name]}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_LOG="warn",
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=f"{name}-origin",
                NONODB_REPLICATION_ROLE=role,
                NONODB_REPLICATION_NODE_ID=name,
                **extra,
            )
            log = open(temp / f"{name}.log", "wb")
            proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
            procs.append(proc)
            log.close()
            client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=60)
            clients.append(client)
            wait_health(client, proc, name)
            return client, proc

        primary, _ = start("primary", "primary", {})
        replica, _ = start(
            "replica",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_BOOTSTRAP": "auto",
            },
        )

        def call(client: httpx.Client, body: dict) -> dict:
            response = client.post("/v1/exec", headers=headers, json=body)
            payload = response.json()
            if response.status_code != 200 or payload.get("ok") is False:
                raise RuntimeError(f"{body.get('op')}: {response.status_code} {payload}")
            return payload.get("result")

        call(primary, {"op": "create_collection", "coll": "repl_meta"})
        call(primary, {"op": "create_index", "coll": "repl_meta", "field": "grp"})
        call(primary, {"op": "create_text_index", "coll": "repl_meta", "field": "body"})
        call(primary, {"op": "create_fuzzy_index", "coll": "repl_meta", "field": "title"})
        call(primary, {"op": "create_vector_index", "coll": "repl_meta", "field": "embedding", "dims": 3})
        call(primary, {"op": "set_collection_history", "coll": "repl_meta", "max_versions": 5})
        call(primary, {"op": "set_collection_events", "coll": "repl_meta", "enabled": True})
        call(
            primary,
            {
                "op": "create_subscription",
                "coll": "repl_meta",
                "name": "all",
                "filter": {"grp": {"$gte": 0}},
                "lease_seconds": 600,
            },
        )
        call(primary, {"op": "create_collection", "coll": "repl_ttl"})
        call(primary, {"op": "set_collection_ttl", "coll": "repl_ttl", "ttl_seconds": 2})
        call(
            primary,
            {
                "op": "insert",
                "coll": "repl_meta",
                "doc": {
                    "_id": "m1",
                    "grp": 1,
                    "title": "rust database",
                    "body": "hello replication metadata",
                    "embedding": [1, 0, 0],
                },
            },
        )
        call(primary, {"op": "update", "coll": "repl_meta", "filter": {"_id": "m1"}, "update": {"$set": {"grp": 2}}})
        call(primary, {"op": "insert", "coll": "repl_ttl", "doc": {"_id": "t1"}})

        def value(client: httpx.Client, op: str, coll: str = "repl_meta") -> dict:
            return call(client, {"op": op, "coll": coll})

        def count(client: httpx.Client, coll: str) -> int:
            return call(client, {"op": "count", "coll": coll, "filter": {}})["count"]

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if count(replica, "repl_meta") == 1 and count(replica, "repl_ttl") == 1:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("documents did not replicate")

        checks = {
            "indexes": value(replica, "list_indexes")["indexes"],
            "text": value(replica, "list_text_indexes")["text_indexes"],
            "fuzzy": value(replica, "list_fuzzy_indexes")["fuzzy_indexes"],
            "vector": value(replica, "list_vector_indexes")["vector_indexes"],
            "history": value(replica, "get_collection_history")["max_versions"],
            "events": value(replica, "get_collection_events")["enabled"],
            "subscriptions": value(replica, "list_subscriptions")["subscriptions"],
            "ttl": value(replica, "get_collection_ttl", "repl_ttl")["ttl_seconds"],
            "outbox": count(replica, "_events"),
        }
        assert len(checks["indexes"]) == 1, checks
        assert checks["text"] == ["body"], checks
        assert checks["fuzzy"] == ["title"], checks
        assert len(checks["vector"]) == 1, checks
        assert checks["history"] == 5, checks
        assert checks["events"] is True, checks
        assert [item["name"] for item in checks["subscriptions"]] == ["all"], checks
        assert checks["ttl"] == 2, checks
        assert checks["outbox"] > 0, checks

        history = call(replica, {"op": "history", "coll": "repl_meta", "id": "m1"})["revisions"]
        assert len(history) >= 2, history

        live_lines: list[str] = []

        def listen() -> None:
            try:
                with replica.stream(
                    "POST",
                    "/v1/watch",
                    headers=headers,
                    json={"op": "watch", "coll": "repl_meta", "mode": "live", "filter": {}, "timeout_ms": 5000},
                    timeout=8,
                ) as response:
                    for line in response.iter_lines():
                        if not line:
                            continue
                        live_lines.append(line)
                        if "heartbeat" not in line and "stream" not in line:
                            return
            except httpx.HTTPError:
                pass

        thread = threading.Thread(target=listen)
        thread.start()
        time.sleep(0.5)
        call(
            primary,
            {
                "op": "insert",
                "coll": "repl_meta",
                "doc": {
                    "_id": "m2",
                    "grp": 3,
                    "title": "live stream",
                    "body": "live replication",
                    "embedding": [0, 1, 0],
                },
            },
        )
        thread.join(timeout=8)
        assert any("m2" in line for line in live_lines), live_lines

        watch = call(
            replica,
            {"op": "watch", "coll": "_events", "limit": 100, "timeout_ms": 100},
        )
        assert watch["events"], watch

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if count(primary, "repl_ttl") == 0 and count(replica, "repl_ttl") == 0:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("TTL delete did not replicate")

        print(
            json.dumps(
                {
                    "ok": True,
                    "metadata": {key: (len(value) if isinstance(value, list) else value) for key, value in checks.items()},
                    "history_revisions": len(history),
                    "outbox_events": len(watch["events"]),
                    "live_watch_matched": True,
                    "ttl_replicated": True,
                }
            )
        )
        return 0
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for client in clients:
            client.close()
        import shutil

        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
