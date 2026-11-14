#!/usr/bin/env python3
"""A primary with configured voters must not write before acquiring a quorum lease."""

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


def main() -> int:
    temp = Path(tempfile.mkdtemp(prefix="nonodb-lease-gate-", dir="/var/tmp"))
    auth = temp / "auth.json"
    env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
    subprocess.run([str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True)
    token = subprocess.run(
        [str(CLI), "auth", "token", "create", "admin"],
        env=env_auth,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    port = free_port()
    data = temp / "data"
    data.mkdir()
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(temp / "daemon.sock"),
        NONODB_TCP=f"127.0.0.1:{port}",
        NONODB_AUTH_FILE=str(auth),
        NONODB_LOG="warn",
        NONODB_REPLICATION_ENABLED="1",
        NONODB_REPLICATION_ORIGIN="isolated-primary",
        NONODB_REPLICATION_ROLE="primary",
        NONODB_REPLICATION_NODE_ID="isolated-primary",
        NONODB_REPLICATION_ADVERTISE_URL=f"http://127.0.0.1:{port}",
        NONODB_REPLICATION_VOTERS="http://127.0.0.1:1,http://127.0.0.1:2,http://127.0.0.1:3",
        NONODB_REPLICATION_LEASE_TTL="2",
        NONODB_REPLICATION_LEASE_INTERVAL="1",
    )
    log = open(temp / "daemon.log", "wb")
    proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10)
    headers = {"Authorization": f"Bearer {token}"}
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"daemon exited with {proc.returncode}")
            try:
                if client.get("/healthz", timeout=0.5).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        else:
            raise RuntimeError("daemon did not start")
        status = client.get("/v1/replication/status", headers=headers).json()["result"]
        assert status["role"] == "primary", status
        assert status["lease_valid"] is False, status
        assert status["read_only"] is True, status
        write = client.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"n": 1}},
        )
        assert write.status_code == 403, write.text
        assert write.json()["error"]["code"] == "read_only", write.text
        print(json.dumps({"ok": True, "lease_gate": True, "role": status["role"]}))
        return 0
    finally:
        client.close()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        print(f"logs: {temp}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
