#!/usr/bin/env python3
"""End-to-end test for the GUI cluster view and multi-node probing."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_url(url: str, proc: subprocess.Popen[bytes], timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"process exited with {proc.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {url}")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-gui-cluster-", dir="/var/tmp"))
    auth = tmp / "auth.json"
    procs: list[subprocess.Popen[bytes]] = []
    try:
        env = os.environ.copy()
        env["NONODB_AUTH_FILE"] = str(auth)
        subprocess.run([str(CLI), "auth", "init"], env=env, check=True, capture_output=True)
        token = subprocess.run(
            [str(CLI), "auth", "token", "create", "admin"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        ports = [free_port(), free_port()]
        for index, port in enumerate(ports):
            data = tmp / f"node{index}"
            data.mkdir()
            node_env = os.environ.copy()
            node_env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(tmp / f"node{index}.sock"),
                NONODB_TCP=f"127.0.0.1:{port}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_LOG="warn",
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=f"node{index}",
            )
            procs.append(
                subprocess.Popen(
                    [str(DAEMON)],
                    env=node_env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
            wait_url(f"http://127.0.0.1:{port}/healthz", procs[-1])
        gui_port = free_port()
        dead_port = free_port()
        cluster = ",".join(
            [
                f"one=http://127.0.0.1:{ports[0]}",
                f"two=http://127.0.0.1:{ports[1]}",
                f"dead=http://127.0.0.1:{dead_port}",
            ]
        )
        progress_file = tmp / "progress.json"
        progress_file.write_text(
            json.dumps(
                {
                    "active": True,
                    "phase": "bulk_load",
                    "loaded": 5,
                    "total": 10,
                    "docs_per_second": 42.0,
                }
            )
        )
        gui_env = os.environ.copy()
        gui_env.update(
            NONODB_AUTH_FILE=str(auth),
            NONODB_TOKEN=token,
            NONODB_SOCK=str(tmp / "node0.sock"),
        )
        procs.append(
            subprocess.Popen(
                [
                    str(CLI),
                    "gui",
                    "--port",
                    str(gui_port),
                    "--no-open",
                    "--url",
                    f"http://127.0.0.1:{ports[0]}",
                    "--cluster",
                    cluster,
                    "--progress-file",
                    str(progress_file),
                ],
                env=gui_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        base = f"http://127.0.0.1:{gui_port}"
        wait_url(f"{base}/", procs[-1])
        with urllib.request.urlopen(f"{base}/gui/cluster/status", timeout=10) as response:
            payload = json.load(response)
        assert payload["ok"] is True, payload
        nodes = {node["name"]: node for node in payload["nodes"]}
        assert set(nodes) == {"one", "two", "dead"}, nodes
        for name in ("one", "two"):
            assert nodes[name]["ok"] is True, nodes[name]
            assert nodes[name]["status"]["role"] == "primary", nodes[name]
            assert nodes[name]["metrics"]["requests_total"] >= 0, nodes[name]
        assert nodes["dead"]["ok"] is False, nodes["dead"]
        assert "error" in nodes["dead"], nodes["dead"]
        with urllib.request.urlopen(f"{base}/gui/test/progress", timeout=10) as response:
            progress = json.load(response)["progress"]
        assert progress["phase"] == "bulk_load", progress
        assert progress["loaded"] == 5 and progress["total"] == 10, progress
        with urllib.request.urlopen(f"{base}/") as response:
            index = response.read().decode()
        marker = "src=\""
        asset = index.split(marker, 1)[1].split('"', 1)[0]
        with urllib.request.urlopen(f"{base}{asset}") as response:
            javascript = response.read()
        assert b"One row per node" in javascript
        assert b"Max lag" in javascript
        assert b"Live load test" in javascript
        print(json.dumps({"ok": True, "online": ["one", "two"], "offline": ["dead"]}))
        return 0
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
