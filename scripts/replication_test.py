#!/usr/bin/env python3
"""End-to-end check for durable writer replication-log capture."""

from __future__ import annotations

import json
import os
import re
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

            initial_changes = client.get(
                "/v1/replication/changes?after=0&limit=100", headers=headers
            )
            assert initial_changes.status_code == 200, initial_changes.text
            initial_txns = initial_changes.json()["result"]["changes"]
            assert all(txn["epoch"] == 0 for txn in initial_txns), initial_txns
            initial_ops = [
                change["op"] for txn in initial_txns for change in txn["changes"]
            ]
            assert "insert" in initial_ops and "update" in initial_ops, initial_ops
            assert "delete" in initial_ops, initial_ops
            assert any(
                change["id"] == "o4"
                for txn in initial_txns
                for change in txn["changes"]
            ), initial_txns
            initial_batch = next(txn for txn in initial_txns if len(txn["changes"]) == 2)
            assert {change["id"] for change in initial_batch["changes"]} == {"o2", "o3"}

            cli_status = subprocess.run(
                [str(CLI), "replication", "status"],
                env={**os.environ, "NONODB_SOCK": str(temp / "nonodb.sock")},
                capture_output=True,
                text=True,
                check=False,
            )
            assert cli_status.returncode == 0, cli_status.stderr
            assert json.loads(cli_status.stdout)["result"]["origin"] == "test-node"

            snapshot = client.get("/v1/replication/snapshot", headers=headers)
            assert snapshot.status_code == 200, snapshot.text
            assert len(snapshot.content) > 4096, len(snapshot.content)
            assert snapshot.headers["x-nonodb-replication-origin"] == "test-node"
            assert int(snapshot.headers["x-nonodb-replication-head"]) >= 4

            source_head = client.get(
                "/v1/replication/status", headers=headers
            ).json()["result"]["head"]
            pruned = client.post(
                "/v1/replication/prune",
                headers=headers,
                json={"through": source_head},
            )
            assert pruned.status_code == 200, pruned.text
            assert pruned.json()["result"]["deleted"] == source_head, pruned.text

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
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_SOURCE=f"http://127.0.0.1:{port}",
                NONODB_REPLICATION_SOURCE_TOKEN=admin,
                NONODB_REPLICATION_DATABASES="default",
                NONODB_REPLICATION_POLL_INTERVAL="1",
                NONODB_REPLICATION_BOOTSTRAP="auto",
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
                doc = None
                while time.monotonic() < deadline:
                    doc = replica_exec(
                        {"op": "find_one", "coll": "orders", "filter": {"_id": "o1"}}
                    )
                    if doc and doc.get("n") == 2:
                        break
                    time.sleep(0.1)
                else:
                    replica_log.flush()
                    lines = (temp / "replica.log").read_text(encoding="utf-8").splitlines()
                    tail = "\n".join(
                        line for line in lines if "replication" in line.lower() or "ERROR" in line or "WARN" in line
                    )[-8000:]
                    collections = replica_exec({"op": "list_collections"})
                    all_docs = replica_exec({"op": "find", "coll": "orders", "filter": {}})
                    count = replica_exec({"op": "count", "coll": "orders", "filter": {}})
                    raise RuntimeError(
                        f"replica did not apply the initial snapshot: doc={doc} count={count} all={all_docs} collections={collections}\n{tail}"
                    )

                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o2"}}
                ) is None
                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o3"}}
                ) is not None
                assert replica_exec(
                    {"op": "find_one", "coll": "orders", "filter": {"_id": "o4"}}
                ) is None

                exec(
                    {
                        "op": "update",
                        "coll": "orders",
                        "filter": {"_id": "o1"},
                        "update": {"$set": {"n": 3}},
                    }
                )
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    doc = replica_exec(
                        {"op": "find_one", "coll": "orders", "filter": {"_id": "o1"}}
                    )
                    if doc and doc.get("n") == 3:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError("replica served a stale cached read after apply")

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

                source_demote = client.post(
                    "/v1/replication/demote",
                    headers=headers,
                    json={"expected_epoch": 0, "role": "replica"},
                )
                assert source_demote.status_code == 200, source_demote.text
                assert source_demote.json()["result"]["role"]["epoch"] == 1
                source_promote = client.post(
                    "/v1/replication/promote",
                    headers=headers,
                    json={"expected_epoch": 1},
                )
                assert source_promote.status_code == 200, source_promote.text
                assert source_promote.json()["result"]["role"]["epoch"] == 2
                exec({"op": "insert", "coll": "orders", "doc": {"_id": "o6", "n": 6}})
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    doc = replica_exec(
                        {"op": "find_one", "coll": "orders", "filter": {"_id": "o6"}}
                    )
                    if doc and doc.get("n") == 6:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError("replica did not apply a new-epoch change")

                blocked = replica.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "blocked"}},
                )
                assert blocked.status_code == 403, blocked.text
                assert blocked.json()["error"]["code"] == "read_only", blocked.text

                promoted = replica.post(
                    "/v1/replication/promote",
                    headers=headers,
                    json={"expected_epoch": 0},
                )
                assert promoted.status_code == 200, promoted.text
                promoted_role = promoted.json()["result"]["role"]
                assert promoted_role["role"] == "primary", promoted_role
                assert promoted_role["epoch"] == 1, promoted_role
                assert promoted_role["read_only"] is False, promoted_role
                writable = replica.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "promoted", "n": 1}},
                )
                assert writable.status_code == 200, writable.text

                demote = subprocess.run(
                    [str(CLI), "replication", "demote", "1", "replica"],
                    env={**os.environ, "NONODB_SOCK": str(temp / "replica.sock")},
                    capture_output=True,
                    text=True,
                    check=False,
                )
                assert demote.returncode == 0, demote.stderr
                demoted = json.loads(demote.stdout)["role"]
                assert demoted["role"] == "replica", demoted
                assert demoted["epoch"] == 2, demoted
                blocked_again = replica.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "blocked-2"}},
                )
                assert blocked_again.status_code == 403, blocked_again.text

                replica_status = replica.get(
                    "/v1/replication/status", headers=headers
                ).json()["result"]
                assert replica_status["role"] == "replica", replica_status
                assert replica_status["epoch"] == 2, replica_status
                assert replica_status["read_only"] is True, replica_status
                assert replica_status["applied"], replica_status
                assert replica_status["applied"][0]["origin"] == "test-node", replica_status
                assert replica_status["applied"][0]["lag"] == 0, replica_status
                assert replica_status["applied"][0]["epoch"] == 2, replica_status
                source_head_now = client.get(
                    "/v1/replication/status", headers=headers
                ).json()["result"]["head"]
                assert replica_status["head"] >= source_head_now, replica_status
                replica_txns = replica.get(
                    "/v1/replication/changes?after=0&limit=100", headers=headers
                ).json()["result"]["changes"]
                assert replica_txns, "replica should keep a local replication log"
                assert any(
                    change["id"] == "promoted"
                    for txn in replica_txns
                    for change in txn["changes"]
                ), replica_txns
                assert replica_status["replication"]["source"].startswith(
                    "http://127.0.0.1:"
                ), replica_status
                assert replica_status["replication"]["bootstrap"] == "auto", replica_status
                source_update = replica.put(
                    "/v1/replication/source",
                    headers=headers,
                    json={
                        "source": f"http://127.0.0.1:{port}",
                        "databases": ["default"],
                        "interval_secs": 1,
                        "bootstrap": "auto",
                    },
                )
                assert source_update.status_code == 200, source_update.text
                assert (replica_data / "replication-source.json").is_file()
                updated_status = replica.get(
                    "/v1/replication/status", headers=headers
                ).json()["result"]
                assert updated_status["replication"]["source"] == (
                    f"http://127.0.0.1:{port}"
                ), updated_status
                assert replica_status["replication"]["last_success_unix"] > 0, replica_status
                assert replica_status["replication"]["last_apply_error"] is None, replica_status
                ready = replica.get("/readyz").json()
                assert ready["checks"]["writer_required"] is False, ready
                assert ready["replication"]["role"] == "replica", ready
                replica_metrics = replica.get(
                    "/metrics/prometheus", headers=headers
                ).text
                assert 'nonodb_replication_applied{database="default",origin="test-node"}' in replica_metrics, replica_metrics[-1000:]
                assert 'nonodb_replication_lag{database="default",origin="test-node"} 0' in replica_metrics, replica_metrics[-1000:]
                assert 'nonodb_replication_applied_epoch{database="default",origin="test-node"} 2' in replica_metrics, replica_metrics[-1000:]
                assert "nonodb_replication_epoch 2" in replica_metrics, replica_metrics[-1000:]
                applied_total = re.search(
                    r"nonodb_replication_applied_total (\d+)", replica_metrics
                )
                assert applied_total and int(applied_total.group(1)) >= 2, replica_metrics[-1000:]
                assert "nonodb_replication_apply_errors_total 0" in replica_metrics, replica_metrics[-1000:]
            finally:
                replica.close()

            replica_proc.terminate()
            replica_proc.wait(timeout=10)
            replica_log.close()

            exec({"op": "insert", "coll": "orders", "doc": {"_id": "o7", "n": 7}})
            new_head = client.get(
                "/v1/replication/status", headers=headers
            ).json()["result"]["head"]
            pruned = client.post(
                "/v1/replication/prune",
                headers=headers,
                json={"through": new_head},
            )
            assert pruned.status_code == 200, pruned.text

            replica_log = open(temp / "replica.log", "a", encoding="utf-8")
            replica_proc = subprocess.Popen(
                [str(DAEMON)], env=replica_env, stdout=replica_log, stderr=subprocess.STDOUT
            )
            replica = httpx.Client(base_url=f"http://127.0.0.1:{replica_port}", timeout=30.0)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if replica_proc.poll() is not None:
                    raise RuntimeError(
                        f"resynced replica exited with {replica_proc.returncode}"
                    )
                try:
                    if replica.get("/healthz").status_code == 200:
                        doc = replica.post(
                            "/v1/exec",
                            headers=headers,
                            json={"op": "find_one", "coll": "orders", "filter": {"_id": "o7"}},
                        ).json()["result"]
                        if doc and doc.get("n") == 7:
                            break
                except (httpx.HTTPError, KeyError):
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("replica did not auto-resync after a log gap")
            resynced = replica.get("/v1/replication/status", headers=headers).json()["result"]
            assert resynced["applied"][0]["applied"] >= new_head, resynced
            assert resynced["applied"][0]["epoch"] == 2, resynced
            assert resynced["head"] == new_head, resynced
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

            assert status["head"] >= new_head, status

            print("PASS  replication snapshot, pull, epoch fence, auto-resync")
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

        if os.environ.get("NONODB_KEEP_REPLICATION_TEMP"):
            print(f"kept replication test directory: {temp}")
        else:
            shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
