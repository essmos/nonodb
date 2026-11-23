"""End-to-end graph traversal, adjacency, and vector-seed tests.

    cargo build -p nonodb-daemon
    python3 scripts/graph_test.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


def child_ids(tree: dict) -> list[str]:
    ids: list[str] = []
    for item in tree.get("children", []):
        ids.append(item["node"]["_id"])
        ids.extend(child_ids(item))
    return ids


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-graph-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
            "NONODB_TTL_SWEEP_INTERVAL": "1",
        }
    )
    fh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        nodes = db.collection("nodes")
        edges = db.collection("edges")

        for node in [
            {"_id": "a", "name": "A", "embedding": [1.0, 0.0, 0.0]},
            {"_id": "b", "name": "B", "embedding": [0.0, 1.0, 0.0]},
            {"_id": "c", "name": "C", "embedding": [0.0, 0.0, 1.0]},
            {"_id": "d", "name": "D", "embedding": [0.5, 0.5, 0.0]},
            {"_id": "z", "name": "Z", "embedding": [0.0, 0.0, 0.0]},
        ]:
            nodes.insert(node)
        for edge in [
            {"_id": "e1", "from": "a", "to": "b", "label": "KNOWS", "weight": 1},
            {"_id": "e2", "from": "b", "to": "c", "label": "KNOWS", "weight": 2},
            {"_id": "e3", "from": "a", "to": "c", "label": "KNOWS", "weight": 10},
            {"_id": "e4", "from": "c", "to": "a", "label": "BACK"},
            {"_id": "e5", "from": "d", "to": "a", "label": "KNOWS", "weight": 1},
        ]:
            edges.insert(edge)

        graph = nodes.set_graph("edges")
        assert graph["edge_collection"] == "edges", graph
        assert nodes.get_graph()["from_field"] == "from"
        assert nodes.get_graph()["directed"] is True

        # HTTP set_graph must not let `def.node_collection` diverge from `coll`.
        try:
            db.exec(
                {
                    "op": "set_graph",
                    "coll": "nodes",
                    "def": {
                        "node_collection": "other",
                        "edge_collection": "edges",
                        "from_field": "from",
                        "to_field": "to",
                        "label_field": "label",
                        "weight_field": "weight",
                        "directed": True,
                    },
                }
            )
        except NonoDBError:
            pass
        else:
            raise AssertionError("set_graph accepted a mismatched node_collection")

        # A successful edge insert after configuration updates adjacency.
        edges.insert({"_id": "e6", "from": "a", "to": "d", "label": "KNOWS", "weight": 1})
        assert "d" in child_ids(nodes.traverse("a", max_depth=1)["roots"][0])
        edges.delete({"_id": "e6"}, multi=False)
        assert "d" not in child_ids(nodes.traverse("a", max_depth=1)["roots"][0])

        tree = nodes.traverse("a", max_depth=2)
        assert tree["nodes"] == 5, tree
        assert tree["edges"] == 4, tree
        assert tree["truncated"] is False, tree
        assert tree["roots"][0]["node"]["_id"] == "a"
        assert set(child_ids(tree["roots"][0])) >= {"b", "c"}, tree
        assert tree["missing"] == []
        assert '"cycle": true' in json.dumps(tree)

        flat = nodes.traverse("a", max_depth=2, tree=False)
        assert len(flat["items"]) == 5, flat
        assert flat["items"][0]["node"]["_id"] == "a"

        dfs = nodes.traverse("a", max_depth=2, algorithm="dfs")
        assert dfs["nodes"] == 5 and dfs["edges"] == 4, dfs

        node_limited = nodes.traverse("a", max_depth=2, limit_nodes=3)
        assert node_limited["nodes"] == 3, node_limited
        assert node_limited["truncated"] is True, node_limited

        byte_limited = nodes.traverse("a", max_depth=2, max_bytes=1)
        assert byte_limited["nodes"] == 0, byte_limited
        assert byte_limited["truncated"] is True, byte_limited

        depth_one = nodes.traverse("a", max_depth=1)
        assert depth_one["nodes"] == 3 and depth_one["edges"] == 2, depth_one

        limited = nodes.traverse("a", max_depth=2, limit_edges=1)
        assert limited["truncated"] is True and limited["edges"] == 1, limited

        incoming = nodes.traverse("c", direction="in", max_depth=1)
        assert set(child_ids(incoming["roots"][0])) == {"a", "b"}, incoming

        both = nodes.traverse("c", direction="both", max_depth=1)
        assert {"a", "b"} <= set(child_ids(both["roots"][0])), both

        labelled = nodes.traverse("c", labels=["BACK"], direction="out", max_depth=1)
        assert child_ids(labelled["roots"][0]) == ["a"], labelled

        missing = nodes.traverse(["a", "nope"], max_depth=1)
        assert missing["missing"] == ["nope"], missing

        hops = nodes.shortest_path("a", "c")
        assert hops["found"] is True and hops["hops"] == 1, hops
        weighted = nodes.shortest_path("a", "c", weighted=True)
        assert weighted["found"] is True and weighted["weight"] == 3, weighted
        assert weighted["hops"] == 2, weighted
        assert nodes.shortest_path("a", "z")["found"] is False

        # Adjacency follows updates and deletes in the same transaction.
        edges.update({"_id": "e2"}, {"$set": {"to": "d"}})
        updated = nodes.traverse("a", max_depth=2)
        assert "c" in child_ids(updated["roots"][0]), updated
        assert set(child_ids(nodes.traverse("d", max_depth=1)["roots"][0])) == {"a"}, updated

        edges.delete({"_id": "e1"}, multi=False)
        after_delete = nodes.traverse("a", max_depth=1)
        assert child_ids(after_delete["roots"][0]) == ["c"], after_delete

        before = edges.count({})
        try:
            edges.insert({"_id": "bad", "from": "a", "label": "BROKEN"})
        except NonoDBError:
            pass
        else:
            raise AssertionError("malformed edge insert should fail")
        assert edges.count({}) == before
        assert child_ids(nodes.traverse("a", max_depth=1)["roots"][0]) == ["c"]

        report = nodes.reindex_graph()
        assert report["edges"] == before, report
        assert child_ids(nodes.traverse("a", max_depth=1)["roots"][0]) == ["c"]

        # Vector search can seed a traversal.
        nodes.create_vector_index("embedding", 3)
        seeded = nodes.traverse(
            seed={"vector": {"field": "embedding", "vector": [0.0, 1.0, 0.0], "k": 1}},
            max_depth=1,
        )
        assert seeded["roots"][0]["node"]["_id"] == "b", seeded

        # TTL expiry removes edge adjacency through the same hook.
        db.create_collection("tnodes")
        db.create_collection("tedges")
        tnodes = db.collection("tnodes")
        tedges = db.collection("tedges")
        tnodes.insert({"_id": "n1"})
        tnodes.insert({"_id": "n2"})
        tnodes.set_graph("tedges")
        tedges.insert({"_id": "te1", "from": "n1", "to": "n2", "_ttl": 1})
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if not tnodes.traverse("n1", max_depth=1)["edges"]:
                break
            time.sleep(0.2)
        else:
            raise AssertionError("TTL-expired edge still appears in adjacency")
        assert tedges.count({}) == 0

        assert nodes.drop_graph() is True
        assert nodes.get_graph() is None
        assert nodes.drop_graph() is False

        print("graph tests passed")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        if proc.returncode not in (0, -15):
            print(log.read_text(errors="replace"), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
