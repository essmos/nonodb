#!/usr/bin/env python3
"""kill -9 during an online graph rebuild, then resume and verify the swap."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def start(daemon: Path, env: dict[str, str], sock: Path) -> subprocess.Popen[bytes]:
    sock.unlink(missing_ok=True)
    proc = subprocess.Popen(
        [str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    deadline = time.monotonic() + 10
    while not sock.exists() and time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("daemon exited during startup")
        time.sleep(0.02)
    return proc


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.is_file():
        raise SystemExit("missing debug daemon binary")
    total = 120
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-rebuild-crash-") as td:
        tmp = Path(td)
        sock = tmp / "n.sock"
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(tmp / "data"),
            NONODB_SOCK=str(sock),
            NONODB_LOG="warn",
            NONODB_MAINTENANCE_INTERVAL_MS="50",
            NONODB_GRAPH_REBUILD_CHUNK="1",
        )
        proc = start(daemon, env, sock)
        try:
            db = NonoDB(str(sock), cache_ttl=0.0)
            nodes = db.collection("nodes")
            db.create_collection("edges")
            edges = db.collection("edges")
            nodes.insert({"_id": "root"})
            nodes.insert_many([{"_id": f"n{i}"} for i in range(total)])
            nodes.set_graph("edges")
            edges.insert_many(
                [
                    {"_id": f"e{i:03}", "from": "root", "to": f"n{i}"}
                    for i in range(total)
                ]
            )
            generation = nodes.get_graph()["generation"]
            status = nodes.reindex_graph(online=True)
            assert status["done"] is False, status
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                current = nodes.graph_rebuild_status()
                if not current["done"] and current["scanned"] > 0:
                    break
                time.sleep(0.02)
            else:
                raise AssertionError("rebuild never started scanning")
            # A write after the rebuild started must be mirrored into both the
            # active and target generations.
            edges.insert({"_id": "late", "from": "root", "to": "late-node"})
            nodes.insert({"_id": "late-node"})
            proc.kill()
            proc.wait(timeout=5)

            proc = start(daemon, env, sock)
            db = NonoDB(str(sock), cache_ttl=0.0)
            nodes = db.collection("nodes")
            assert nodes.get_graph()["generation"] == generation
            while True:
                current = nodes.graph_rebuild_status()
                if current["done"]:
                    break
                if time.monotonic() > deadline:
                    raise AssertionError(f"rebuild did not resume: {current}")
                time.sleep(0.05)
            assert nodes.get_graph()["generation"] == generation + 1
            children = {
                child["node"]["_id"]
                for child in nodes.traverse("root", max_depth=1)["roots"][0]["children"]
            }
            assert len(children) == total + 1, len(children)
            assert "late-node" in children
            # Rebuild must not duplicate adjacency entries.
            assert nodes.traverse("root", max_depth=1, limit_edges=10_000)["edges"] == total + 1
            print("graph rebuild crash test passed")
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
