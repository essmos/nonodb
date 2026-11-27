#!/usr/bin/env python3
"""Verify promotion fences and demotes the previous replication source."""

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
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-failover-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: list[subprocess.Popen] = []
    logs: list = []
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

        primary_port = free_port()
        replica_port = free_port()
        for name, port, role, extra in [
            (
                "primary",
                primary_port,
                "primary",
                {"NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{primary_port}"},
            ),
            (
                "replica",
                replica_port,
                "replica",
                {
                    "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{primary_port}",
                    "NONODB_REPLICATION_SOURCE_TOKEN": token,
                    "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{replica_port}",
                    "NONODB_REPLICATION_NODE_ID": "replica-1",
                },
            ),
        ]:
            data = temp / f"{name}-data"
            data.mkdir()
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(temp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{port}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=f"{name}-origin",
                NONODB_REPLICATION_ROLE=role,
                NONODB_REPLICATION_VOTERS=(
                    f"http://127.0.0.1:{primary_port},http://127.0.0.1:{replica_port}"
                ),
                NONODB_REPLICATION_VOTER_TOKEN=token,
                NONODB_REPLICATION_POLL_INTERVAL="1",
                # The primary needs a live lease to accept writes; the streams
                # scenario below writes before the promotion.
                NONODB_REPLICATION_LEASE_INTERVAL="1",
                NONODB_REPLICATION_LEASE_TTL="3",
                # Stream janitor runs every second so the dead-letter replication
                # below completes inside the test window.
                NONODB_STREAM_PRUNE_INTERVAL="1",
                NONODB_LOG="warn",
                **extra,
            )
            log = open(temp / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            procs.append(
                subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
            )

        headers = {"Authorization": f"Bearer {token}"}
        clients = []
        for name, port in [("primary", primary_port), ("replica", replica_port)]:
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
            clients.append(client)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if client.get("/healthz").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError(f"{name} did not become healthy")

        primary, replica = clients

        def exec_on(client, payload):
            response = client.post("/v1/exec", headers=headers, json=payload)
            assert response.status_code == 200, response.text
            return response.json()["result"]

        def stats_on(client, coll):
            streams = exec_on(client, {"op": "stream_stats", "coll": coll})["streams"]
            return streams[0] if streams else None

        # --- streams: leaked claims, janitor replication, and failover ---
        # The primary only accepts writes once its replication lease is live.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if primary.get("/readyz/write").status_code == 200:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("primary never became writable")
        # No `create_collection` first: `create_stream` bootstraps the
        # collection, and that has to replicate (CollectionCreate + StreamSet).
        exec_on(primary, {"op": "create_stream", "coll": "ledger"})
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                stream = exec_on(replica, {"op": "get_stream", "coll": "ledger"})
                if stream and stream.get("head_seq") is not None:
                    break
            except AssertionError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("replica never applied the bootstrapped collection/stream")
        exec_on(
            primary,
            {
                "op": "create_stream_group",
                "coll": "ledger",
                "group": "workers",
                "start": "earliest",
            },
        )
        for n in range(3):
            exec_on(primary, {"op": "insert", "coll": "ledger", "doc": {"n": n}})

        # A claim that is never committed. The group row replicates, the lease
        # does not: the promoted replica must deliver this range again.
        claim = exec_on(
            primary,
            {
                "op": "watch",
                "coll": "ledger",
                "mode": "group",
                "group": "workers",
                "member": "w1",
                "limit": 2,
                "lease_ms": 60_000,
                "timeout_ms": 100,
            },
        )
        assert (claim["from_seq"], claim["to_seq"]) == (1, 2), claim

        # A range whose lease expires into a dead letter must reach replicas.
        exec_on(
            primary,
            {
                "op": "create_stream_group",
                "coll": "ledger",
                "group": "poison",
                "start": "earliest",
                "max_attempts": 1,
            },
        )
        poison = exec_on(
            primary,
            {
                "op": "watch",
                "coll": "ledger",
                "mode": "group",
                "group": "poison",
                "member": "p1",
                "limit": 10,
                "lease_ms": 1_000,
                "timeout_ms": 100,
            },
        )
        assert (poison["from_seq"], poison["to_seq"]) == (1, 3), poison
        deadline = time.monotonic() + 20
        replicated = None
        while time.monotonic() < deadline:
            dead = exec_on(
                replica, {"op": "stream_dead", "coll": "ledger", "group": "poison"}
            )
            if dead["dead"]:
                replicated = dead
                break
            time.sleep(0.2)
        assert replicated, "expired lease was dead-lettered on the primary only"
        assert replicated["dead"][0]["from_seq"] == 1, replicated
        replica_poison = [
            group
            for group in stats_on(replica, "ledger")["groups"]
            if group["group"] == "poison"
        ][0]
        assert replica_poison["committed_seq"] == 3, replica_poison
        assert replica_poison["dead"] == 3, replica_poison
        assert replica_poison["dead_ranges"] == 1, replica_poison

        promoted = replica.post(
            "/v1/replication/promote",
            headers=headers,
            json={"expected_epoch": 0},
        )
        assert promoted.status_code == 200, promoted.text
        role = promoted.json()["result"]["role"]
        assert role["role"] == "primary", role
        assert role["epoch"] == 1, role
        assert (temp / "primary-data" / ".node" / "data.mdb").is_file()
        assert (temp / "replica-data" / ".node" / "data.mdb").is_file()

        old = primary.get("/v1/replication/status", headers=headers).json()["result"]
        assert old["role"] == "replica", old
        assert old["epoch"] == 1, old
        assert old["successor"] == f"http://127.0.0.1:{replica_port}", old

        assert old["write_url"] == f"http://127.0.0.1:{replica_port}", old
        new_primary = replica.get("/v1/replication/primary", headers=headers).json()["result"]
        assert new_primary["write_url"] == f"http://127.0.0.1:{replica_port}", new_primary
        old_write = primary.get("/readyz/write")
        assert old_write.status_code == 503, old_write.text
        assert old_write.headers.get("location") == f"http://127.0.0.1:{replica_port}"
        new_write = replica.get("/readyz/write")
        assert new_write.status_code == 200, new_write.text
        assert new_write.json()["write_url"] == f"http://127.0.0.1:{replica_port}"
        insert = replica.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "after-failover"}},
        )
        assert insert.status_code == 200, insert.text
        blocked = primary.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "old-primary"}},
        )
        assert blocked.status_code == 403, blocked.text
        assert blocked.json()["error"]["code"] == "read_only", blocked.text
        assert blocked.headers.get("location") == f"http://127.0.0.1:{replica_port}"

        # The promoted node never saw the lease for `1..=2`, so its next claim
        # resumes at the committed offset and redelivers the range.
        resumed = exec_on(
            replica,
            {
                "op": "watch",
                "coll": "ledger",
                "mode": "group",
                "group": "workers",
                "member": "w2",
                "limit": 2,
                "lease_ms": 60_000,
                "timeout_ms": 100,
            },
        )
        assert (resumed["from_seq"], resumed["to_seq"]) == (1, 2), (
            "a promoted replica must redeliver claimed-but-uncommitted ranges: "
            f"{resumed}"
        )
        # Dead letters replicated above are still visible on the new primary.
        still_dead = exec_on(
            replica, {"op": "stream_dead", "coll": "ledger", "group": "poison"}
        )
        assert still_dead["dead"], still_dead

        print(json.dumps({"ok": True, "new_epoch": role["epoch"], "old_role": old["role"]}))
        return 0
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for log in logs:
            log.close()
        import shutil

        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
