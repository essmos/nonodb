#!/usr/bin/env python3
"""Replica hides an expired TTL document before any local/primary sweep."""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
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
    temp = Path(tempfile.mkdtemp(prefix="nonodb-ttl-replica-", dir="/var/tmp"))
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

        def start(name: str, role: str, extra: dict[str, str]) -> httpx.Client:
            data = temp / f"{name}-data"
            data.mkdir()
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(temp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{ports[name]}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_LOG="warn",
                NONODB_TTL_SWEEP_INTERVAL="3600",
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
            return client

        primary = start("primary", "primary", {})
        replica = start(
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

        def wait_for(predicate, timeout: float = 20.0, label: str = "condition") -> None:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if predicate():
                    return
                time.sleep(0.1)
            raise RuntimeError(f"timed out waiting for {label}")

        coll = "repl_ttl_strict"
        call(primary, {"op": "create_collection", "coll": coll})
        call(primary, {"op": "insert", "coll": coll, "doc": {"_id": "d1", "v": 1, "_ttl": 60}})
        wait_for(
            lambda: call(replica, {"op": "find_one", "coll": coll, "filter": {"_id": "d1"}})
            is not None,
            label="initial document replication",
        )

        call(
            primary,
            {
                "op": "update",
                "coll": coll,
                "filter": {"_id": "d1"},
                "update": {"$set": {"v": 2, "_ttl": 2}},
            },
        )
        wait_for(
            lambda: (
                call(replica, {"op": "find_one", "coll": coll, "filter": {"_id": "d1"}})
                or {}
            ).get("v")
            == 2,
            timeout=4.0,
            label="TTL update replication",
        )
        time.sleep(2.2)
        assert call(replica, {"op": "find_one", "coll": coll, "filter": {"_id": "d1"}}) is None
        assert call(replica, {"op": "count", "coll": coll, "filter": {}})["count"] == 0
        assert call(primary, {"op": "find_one", "coll": coll, "filter": {"_id": "d1"}}) is None
        print("OK: replica strict TTL reads before sweep")
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
