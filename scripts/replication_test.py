#!/usr/bin/env python3
"""End-to-end check for durable writer replication-log capture."""

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


def cli_env(auth_file: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth_file)
    return env


def run_cli(auth_file: Path, *args: str) -> str:
    proc = subprocess.run(
        [str(CLI), *args],
        env=cli_env(auth_file),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nonodb-cli {' '.join(args)}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-replication-", dir="/var/tmp"))
    data = temp / "data"
    data.mkdir()
    auth = temp / "auth.json"
    proc: subprocess.Popen | None = None
    replica_proc: subprocess.Popen | None = None
    log = None
    replica_log = None
    try:
        run_cli(auth, "auth", "init")
        admin = run_cli(auth, "auth", "token", "create", "admin")
        port = free_port()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_REPLICATION_ENABLED="1",
            NONODB_REPLICATION_ORIGIN="test-node",
            NONODB_LOG="warn",
        )
        log = open(temp / "daemon.log", "w", encoding="utf-8")
        proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
        headers = {"Authorization": f"Bearer {admin}"}
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0) as client:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited with {proc.returncode}")
                try:
                    if client.get("/healthz").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
            else:
                raise RuntimeError("daemon did not become healthy")

            def exec(payload: dict) -> dict:
                response = client.post("/v1/exec", headers=headers, json=payload)
                assert response.status_code == 200, response.text
                return response.json()["result"]

            exec({"op": "create_collection", "coll": "orders"})
            inserted = exec({"op": "insert", "coll": "orders", "doc": {"_id": "o1", "n": 1}})
            assert inserted["inserted_id"] == "o1"
            exec(
                {
                    "op": "update",
                    "coll": "orders",
                    "filter": {"_id": "o1"},
                    "update": {"$set": {"n": 2}},
                }
            )
            exec(
                {
                    "op": "write_batch",
                    "ops": [
                        {"op": "insert", "coll": "orders", "doc": {"_id": "o2", "n": 1}},
                        {"op": "insert", "coll": "orders", "doc": {"_id": "o3", "n": 1}},
                    ],
                }
            )
            exec({"op": "delete", "coll": "orders", "filter": {"_id": "o2"}})
            exec({"op": "sql", "query": "INSERT INTO orders (_id, n) VALUES ('o4', 4)"})
            exec({"op": "sql", "query": "UPDATE orders SET n = 5 WHERE _id = 'o4'"})
            exec({"op": "sql", "query": "DELETE FROM orders WHERE _id = 'o4'"})

            replica_data = temp / "replica-data"
            replica_data.mkdir()
            replica_port = free_port()
            replica_env = os.environ.copy()
            replica_env.update(
                NONODB_DATA=str(replica_data),
                NONODB_SOCK=str(temp / "replica.sock"),
                NONODB_TCP=f"127.0.0.1:{replica_port}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_REPLICATION_ROLE="replica",
                NONODB_REPLICATION_SOURCE=f"http://127.0.0.1:{port}",
                NONODB_REPLICATION_SOURCE_TOKEN=admin,
                NONODB_REPLICATION_DATABASES="default",
                NONODB_REPLICATION_POLL_INTERVAL="1",
                NONODB_LOG="warn",
            )
            replica_log = open(temp / "replica.log", "w", encoding="utf-8")
            replica_proc = subprocess.Popen(
                [str(DAEMON)], env=replica_env, stdout=replica_log, stderr=subprocess.STDOUT
            )
            replica = httpx.Client(
                base_url=f"http://127.0.0.1:{replica_port}", timeout=30.0
            )
            try:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if replica_proc.poll() is not None:
                        raise RuntimeError(
                            f"replica daemon exited with {replica_proc.returncode}"
                        )
                    try:
                        if replica.get("/healthz").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.05)
                else:
                    raise RuntimeError("replica daemon did not become healthy")

                def replica_exec(payload: dict) -> dict:
                    response = replica.post("/v1/exec", headers=headers, json=payload)
                    assert response.status_code == 200, response.text
                    return response.json()["result"]

                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    doc = replica_exec(
                        {"op": "find_one", "coll": "orders", "filter": {"_id": "o1"}}
                    )
                    if doc and doc.get("n") == 2:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError("replica did not apply the initial snapshot")

                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o2"}}
                ) is None
                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o3"}}
                ) is not None
                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o4"}}
                ) is None

                exec({"op": "insert", "coll": "orders", "doc": {"_id": "o5", "n": 5}})
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    doc = replica_exec(
                        {"op": "find_one", "coll": "orders", "filter": {"_id": "o5"}}
                    )
                    if doc and doc.get("n") == 5:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError("replica did not apply the live change")

                blocked = replica.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "blocked"}},
                )
                assert blocked.status_code == 403, blocked.text
                assert blocked.json()["error"]["code"] == "read_only", blocked.text

                replica_status = replica.get(
                    "/v1/replication/status", headers=headers
                ).json()["result"]
                assert replica_status["role"] == "replica", replica_status
                assert replica_status["applied"], replica_status
                assert replica_status["applied"][0]["origin"] == "test-node", replica_status
                assert replica_status["applied"][0]["lag"] == 0, replica_status
                replica_metrics = replica.get(
                    "/metrics/prometheus", headers=headers
                ).text
                assert 'nonodb_replication_applied{database="default",origin="test-node"}' in replica_metrics, replica_metrics[-1000:]
                assert 'nonodb_replication_lag{database="default",origin="test-node"} 0' in replica_metrics, replica_metrics[-1000:]
            finally:
                replica.close()

            metrics = client.get("/metrics/prometheus", headers=headers).text
            assert 'nonodb_replication_enabled 1' in metrics, metrics[-1000:]
            assert 'nonodb_replication_role{role="primary"} 1' in metrics, metrics[-1000:]
            assert 'nonodb_replication_origin_info{origin="test-node"} 1' in metrics, metrics[-1000:]
            assert 'nonodb_replication_head{database="default"}' in metrics, metrics[-1000:]

            status = client.get("/v1/replication/status", headers=headers)
            assert status.status_code == 200, status.text
            status = status.json()["result"]
            assert status["enabled"] is True, status
            assert status["origin"] == "test-node", status
            assert status["head"] >= 4, status

            changes = client.get(
                "/v1/replication/changes?after=0&limit=100", headers=headers
            )
            assert changes.status_code == 200, changes.text
            txns = changes.json()["result"]["changes"]
            assert txns, changes.text
            assert all(txn["origin"] == "test-node" for txn in txns)
            ops = [change["op"] for txn in txns for change in txn["changes"]]
            assert "insert" in ops and "update" in ops and "delete" in ops, ops
            assert any(
                change["id"] == "o4"
                for txn in txns
                for change in txn["changes"]
            ), txns
            batch = next(txn for txn in txns if len(txn["changes"]) == 2)
            assert {change["id"] for change in batch["changes"]} == {"o2", "o3"}

            print("PASS  replication log, pull apply, read-only replica")
            return 0
    finally:
        if replica_proc is not None:
            replica_proc.terminate()
            try:
                replica_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                replica_proc.kill()
        if replica_log is not None:
            replica_log.close()
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if log is not None:
            log.close()
        import shutil

        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
