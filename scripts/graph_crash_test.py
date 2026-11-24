#!/usr/bin/env python3
"""kill -9 during graph writes; adjacency must match edge documents after restart."""

from __future__ import annotations

import os
import random
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def start(daemon: Path, env: dict[str, str], sock: Path) -> subprocess.Popen[bytes]:
    proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while not sock.exists() and time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("daemon exited during startup")
        time.sleep(0.02)
    return proc


def expected(edges: list[dict], start: str, max_depth: int = 3) -> dict[str, int]:
    graph: dict[str, list[str]] = {}
    for edge in edges:
        graph.setdefault(str(edge["from"]), []).append(str(edge["to"]))
    depth = {start: 0}
    frontier = [start]
    for at in range(max_depth):
        nxt = []
        for node in frontier:
            for target in graph.get(node, []):
                if target not in depth:
                    depth[target] = at + 1
                    nxt.append(target)
        frontier = nxt
    return depth


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.is_file():
        raise SystemExit("missing debug daemon binary")
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-crash-") as td:
        tmp = Path(td)
        sock = tmp / "n.sock"
        env = os.environ.copy()
        env.update(NONODB_DATA=str(tmp / "data"), NONODB_SOCK=str(sock), NONODB_LOG="warn")
        proc = start(daemon, env, sock)
        db = NonoDB(str(sock), cache_ttl=0.0)
        nodes = db.collection("nodes")
        db.create_collection("edges")
        edges = db.collection("edges")
        nodes.set_graph("edges")
        nodes.insert_many([{"_id": f"n{i}"} for i in range(20)])
        stop = threading.Event()
        error: list[BaseException] = []

        def writer(worker: int) -> None:
            rng = random.Random(worker)
            i = 0
            try:
                while not stop.is_set():
                    edge_id = f"w{worker}_{i}"
                    edge = {
                        "_id": edge_id,
                        "from": f"n{rng.randrange(20)}",
                        "to": f"n{rng.randrange(20)}",
                    }
                    edges.insert(edge)
                    if rng.random() < 0.4:
                        edges.update(
                            {"_id": edge_id},
                            {"$set": {"to": f"n{rng.randrange(20)}"}},
                        )
                    if rng.random() < 0.2:
                        edges.delete({"_id": edge_id}, multi=False)
                    i += 1
            except BaseException as exc:  # expected when the daemon is killed
                error.append(exc)

        threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(4)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        proc.kill()
        proc.wait(timeout=5)
        stop.set()
        for thread in threads:
            thread.join(timeout=3)
        proc = start(daemon, env, sock)
        try:
            db = NonoDB(str(sock), cache_ttl=0.0)
            nodes = db.collection("nodes")
            edges = db.collection("edges")
            live = edges.find({}, limit=10_000)
            for start_id in ("n0", "n1", "n2"):
                result = nodes.traverse(
                    start_id,
                    max_depth=3,
                    tree=False,
                    limit_nodes=10_000,
                    limit_edges=100_000,
                )
                got = {
                    str(item["node"]["_id"]): item["depth"]
                    for item in result["items"]
                    if not item["cycle"]
                }
                want = expected(live, start_id)
                assert got == want, (start_id, got, want)
            print("graph crash test passed")
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
