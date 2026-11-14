#!/usr/bin/env python3
"""Vault envelope replication test.

Vault payloads are replicated as encrypted envelopes. The replica never needs
the tenant key to apply them; a client with the same key can read and update the
replicated collection.
"""

from __future__ import annotations

import base64
import json
import os
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
    temp = Path(tempfile.mkdtemp(prefix="nonodb-vault-repl-", dir="/var/tmp"))
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
    ports = {"primary": free_port(), "replica": free_port()}
    procs: list[subprocess.Popen[bytes]] = []
    clients: list[httpx.Client] = []

    def start(name: str, role: str, extra: dict[str, str]) -> None:
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
            **extra,
        )
        log = open(temp / f"{name}.log", "wb")
        proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
        procs.append(proc)
        client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=20)
        clients.append(client)
        wait_health(client, proc, name)

    try:
        start("primary", "primary", {})
        start(
            "replica",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
            },
        )
        primary = NonoDB(
            base_url=f"http://127.0.0.1:{ports['primary']}", token=token, timeout=30
        )
        replica = NonoDB(
            base_url=f"http://127.0.0.1:{ports['replica']}", token=token, timeout=30
        )
        key = base64.b64encode(os.urandom(32)).decode()
        wrong = base64.b64encode(os.urandom(32)).decode()
        primary.create_collection("secrets", "vault")
        vault = primary.vault("acme", "acme:v1", key)
        docs = vault.collection("secrets")
        first = docs.insert(
            {"tenant_id": "acme", "secret": "alpha", "payload": {"n": 1}}
        )
        second = docs.insert(
            {"tenant_id": "acme", "secret": "beta", "payload": {"n": 2}}
        )
        assert first and second

        replica_vault = replica.vault("acme", "acme:v1", key)
        replica_docs = replica_vault.collection("secrets")

        def replicated_count() -> bool:
            return replica_docs.count({"tenant_id": "acme"}) == 2

        wait_for(replicated_count, "vault inserts")
        got = replica_docs.find_one({"_id": first})
        assert got["secret"] == "alpha", got

        primary_docs = primary.vault("acme", "acme:v1", key).collection("secrets")
        assert primary_docs.update({"_id": first}, {"$set": {"secret": "changed"}}) == 1
        wait_for(
            lambda: replica_docs.find_one({"_id": first})["secret"] == "changed",
            "vault update",
        )
        assert primary_docs.delete({"_id": second}) == 1
        wait_for(
            lambda: replica_docs.find_one({"_id": second}) is None,
            "vault delete",
        )

        try:
            replica.vault("acme", "acme:v1", wrong).collection("secrets").find_one(
                {"_id": first}
            )
            raise AssertionError("replica accepted a wrong vault key")
        except Exception as exc:  # noqa: BLE001 - client error expected
            assert getattr(exc, "code", "") == "decryption_failed", exc

        print(
            json.dumps(
                {
                    "ok": True,
                    "vault_inserts": 2,
                    "vault_update": True,
                    "vault_delete": True,
                    "wrong_key_rejected": True,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        for client in clients:
            client.close()
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print(f"logs: {temp}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
