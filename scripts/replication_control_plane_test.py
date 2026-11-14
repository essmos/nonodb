#!/usr/bin/env python3
"""Cluster control-plane replication and pairing test.

Covers separate auth registries, one-time pairing, replicated auth/backup
configuration, database catalog replication, and client-token continuity after
promotion.
"""

from __future__ import annotations

import json
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


def auth_token(path: Path) -> str:
    env = {**os.environ, "NONODB_AUTH_FILE": str(path)}
    subprocess.run([str(CLI), "auth", "init"], env=env, check=True, capture_output=True)
    return subprocess.run(
        [str(CLI), "auth", "token", "create", "admin"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


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


def wait_for(fn, name: str, timeout: float = 30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
            last = value
        except Exception as exc:  # noqa: BLE001 - test diagnostics
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"timeout waiting for {name}: {last}")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-control-", dir="/var/tmp"))
    primary_auth = temp / "primary-auth.json"
    replica_auth = temp / "replica-auth.json"
    primary_token = auth_token(primary_auth)
    replica_token = auth_token(replica_auth)
    headers_p = {"Authorization": f"Bearer {primary_token}"}
    headers_r = {"Authorization": f"Bearer {replica_token}"}
    ports = {"primary": free_port(), "replica": free_port()}
    procs: list[subprocess.Popen[bytes]] = []
    clients: list[httpx.Client] = []

    def start(name: str, role: str, auth: Path, extra: dict[str, str]) -> httpx.Client:
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
            NONODB_REPLICATION_ORIGIN=name,
            NONODB_REPLICATION_NODE_ID=name,
            NONODB_REPLICATION_ROLE=role,
            NONODB_REPLICATION_ADVERTISE_URL=f"http://127.0.0.1:{ports[name]}",
            NONODB_BACKUP_SCHEDULER_INTERVAL="1",
            **extra,
        )
        log = open(temp / f"{name}.log", "wb")
        proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
        procs.append(proc)
        client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=10)
        clients.append(client)
        wait_health(client, proc, name)
        return client

    try:
        primary = start("primary", "primary", primary_auth, {})
        replica = start("replica", "replica", replica_auth, {})

        # Pairing: create a one-time code, then join the replica.
        created = primary.post(
            "/v1/replication/pairings",
            headers=headers_p,
            json={"name": "replica-one", "databases": ["default"]},
        )
        assert created.status_code == 200, created.text
        code = created.json()["result"]["code"]

        bad = primary.post(
            "/v1/replication/pair",
            json={"code": "AAAA-AAAA", "node_id": "replica"},
        )
        assert bad.status_code == 401, bad.text

        joined = replica.post(
            "/v1/replication/join",
            headers=headers_r,
            json={
                "url": f"http://127.0.0.1:{ports['primary']}",
                "code": code,
                "node_id": "replica-one",
            },
        )
        assert joined.status_code == 200, joined.text
        reused = primary.post(
            "/v1/replication/pair",
            json={"code": code, "node_id": "replica-one"},
        )
        assert reused.status_code == 401, reused.text

        def synced_auth() -> bool:
            return json.loads(primary_auth.read_text()) == json.loads(replica_auth.read_text())

        wait_for(synced_auth, "auth registry replication")
        # The old local token is gone; the primary token now works on the replica.
        assert replica.post(
            "/v1/exec",
            headers=headers_r,
            json={"op": "count", "coll": "missing", "filter": {}},
        ).status_code == 401
        assert replica.post(
            "/v1/exec",
            headers=headers_p,
            json={"op": "count", "coll": "missing", "filter": {}},
        ).status_code in (200, 404)

        job = primary.post(
            "/v1/backups/jobs",
            headers=headers_p,
            json={
                "id": "nightly",
                "databases": ["default"],
                "every_minutes": 60,
                "destination": {"kind": "local", "path": str(temp / "backups")},
            },
        )
        assert job.status_code == 200, job.text
        wait_for(
            lambda: (temp / "replica-data" / "backups.json").is_file(),
            "backup job replication",
        )

        # Database catalog and data for a non-default database.
        created_db = primary.post(
            "/v1/exec", headers=headers_p, json={"op": "create_database", "name": "app1"}
        )
        assert created_db.status_code == 200, created_db.text
        inserted = primary.post(
            "/v1/exec",
            headers=headers_p,
            json={"op": "insert", "coll": "orders", "doc": {"tenant": "a", "n": 1}},
        )
        assert inserted.status_code == 200, inserted.text

        # Data inserted through the default route after selecting app1 by URI.
        app_client = httpx.Client(
            base_url=f"http://127.0.0.1:{ports['primary']}/app1", timeout=10
        )
        clients.append(app_client)
        inserted = app_client.post(
            "/v1/exec",
            headers=headers_p,
            json={"op": "insert", "coll": "orders", "doc": {"tenant": "a", "n": 1}},
        )
        assert inserted.status_code == 200, inserted.text

        replica_app = httpx.Client(
            base_url=f"http://127.0.0.1:{ports['replica']}/app1", timeout=10
        )
        clients.append(replica_app)
        wait_for(
            lambda: replica_app.post(
                "/v1/exec",
                headers=headers_p,
                json={"op": "count", "coll": "orders", "filter": {}},
            ).json()
            .get("result", {})
            .get("count")
            == 1,
            "catalog database data replication",
        )

        status = replica.get("/v1/replication/status", headers=headers_p).json()["result"]
        epoch = status["epoch"]
        promoted = replica.post(
            "/v1/replication/promote",
            headers=headers_p,
            json={"expected_epoch": epoch, "force": False},
        )
        assert promoted.status_code == 200, promoted.text
        time.sleep(1.5)
        assert replica.post(
            "/v1/exec",
            headers=headers_p,
            json={"op": "insert", "coll": "orders", "doc": {"n": 2}},
        ).status_code == 200
        promoted_status = replica.get(
            "/v1/replication/status", headers=headers_p
        ).json()["result"]
        assert promoted_status["role"] == "primary"
        assert promoted_status["read_only"] is False

        print(
            json.dumps(
                {
                    "ok": True,
                    "pairing": True,
                    "auth_replicated": True,
                    "backup_config_replicated": True,
                    "catalog_database": "app1",
                    "client_token_after_promotion": True,
                    "promoted_epoch": promoted_status["epoch"],
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        for client in clients:
            client.close()
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print(f"logs: {temp}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
