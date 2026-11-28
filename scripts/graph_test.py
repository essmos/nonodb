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
            "NONODB_MAINTENANCE_INTERVAL_MS": "100",
            "NONODB_GRAPH_REBUILD_CHUNK": "1",
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

        db.create_collection("vaulted_edges", profile="vault")
        try:
            nodes.set_graph("vaulted_edges")
        except NonoDBError as err:
            assert "vault" in str(err).lower(), err
        else:
            raise AssertionError("vault edge collection must be rejected")
        assert nodes.get_graph()["edge_collection"] == "edges"

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

        projected = nodes.traverse("a", max_depth=1, projection={"name": 1})
        node = projected["roots"][0]["node"]
        assert node["_id"] == "a" and node["name"] == "A", projected
        assert "embedding" not in node, projected
        assert len(json.dumps(projected)) < len(
            json.dumps(nodes.traverse("a", max_depth=1))
        ), "projection must shrink the response"

        full_edges = nodes.traverse("a", max_depth=1, include_edges=True)
        via = full_edges["roots"][0]["children"][0]["via"]
        assert "from" in via and "to" in via, full_edges

        filtered_edges = nodes.traverse(
            "a", max_depth=1, edge_filter={"weight": {"$gte": 5}}
        )
        assert child_ids(filtered_edges["roots"][0]) == ["c"], filtered_edges

        filtered_nodes = nodes.traverse("a", max_depth=2, node_filter={"name": {"$in": ["A", "B"]}})
        assert child_ids(filtered_nodes["roots"][0]) == ["b"], filtered_nodes

        near = nodes.neighbors("a")
        assert near["nodes"] == 2, near
        assert {item["via"]["_id"] for item in near["edges"]} == {"e1", "e3"}, near
        near_filtered = nodes.neighbors(
            "a", edge_filter={"weight": {"$gte": 5}}, projection={"name": 1}
        )
        assert near_filtered["edges"][0]["node"]["_id"] == "c", near_filtered
        assert "embedding" not in near_filtered["edges"][0]["node"], near_filtered

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
        projected_path = nodes.shortest_path("a", "c", projection={"name": 1})
        assert "embedding" not in projected_path["path"][0]["node"], projected_path
        no_path = nodes.shortest_path("a", "c", node_filter={"name": "A"})
        assert no_path["found"] is False, no_path

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

        hybrid = nodes.hybrid_search(
            "embedding", [1.0, 0.0, 0.0], k=2, max_depth=1, limit=10,
            vector_weight=0.6, graph_weight=0.4, projection={"name": 1},
        )
        assert hybrid["nodes"] >= 3, hybrid
        scores = [item["score"] for item in hybrid["items"]]
        assert scores == sorted(scores, reverse=True), hybrid
        assert hybrid["items"][0]["node"]["_id"] == "a", hybrid
        assert "embedding" not in hybrid["items"][0]["node"], hybrid
        again = nodes.hybrid_search(
            "embedding", [1.0, 0.0, 0.0], k=2, max_depth=1, limit=10,
            vector_weight=0.6, graph_weight=0.4, projection={"name": 1},
        )
        assert [i["id"] for i in hybrid["items"]] == [i["id"] for i in again["items"]]

        db.create_collection("l2nodes")
        db.create_collection("l2edges")
        l2n = db.collection("l2nodes")
        l2e = db.collection("l2edges")
        l2n.insert_many(
            [
                {"_id": "a", "v": [1.0, 0.0]},
                {"_id": "b", "v": [0.0, 1.0]},
                {"_id": "c", "v": [0.9, 0.1]},
            ]
        )
        l2n.set_graph("l2edges")
        l2e.insert({"_id": "l2e1", "from": "a", "to": "b"})
        l2n.create_vector_index("v", 2, metric="l2")
        l2 = l2n.hybrid_search(
            "v", [1.0, 0.0], k=2, max_depth=1, vector_weight=1.0, graph_weight=0.0
        )
        assert l2["items"][0]["id"] == "a", l2
        assert all(0.0 <= item["vector_score"] <= 1.0 for item in l2["items"]), l2

        # TTL expiry removes edge adjacency through the same hook.
        db.create_collection(
            "tnodes", profile="columnar", columnar={"bucket_max_docs": 8}
        )
        db.create_collection(
            "tedges", profile="columnar", columnar={"bucket_max_docs": 8}
        )
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

        # $graphLookup uses the adjacency fast path when fields match the graph.
        db.create_collection("gnodes")
        db.create_collection("gedges")
        gnodes = db.collection("gnodes")
        gedges = db.collection("gedges")
        for node in ({"_id": "n1"}, {"_id": "n2"}, {"_id": "n3"}):
            gnodes.insert(node)
        gnodes.set_graph("gedges")
        gedges.insert({"_id": "g1", "from": "n1", "to": "n2"})
        gedges.insert({"_id": "g2", "from": "n2", "to": "n3"})
        gedges.insert({"_id": "gz", "from": "n4", "to": "n3", "label": "A"})
        gedges.insert({"_id": "ga", "from": "n5", "to": "n3", "label": "Z"})
        lookup = gnodes.aggregate(
            [
                {
                    "$graphLookup": {
                        "from": "gedges",
                        "startWith": "n3",
                        "connectFromField": "from",
                        "connectToField": "to",
                        "as": "chain",
                        "depthField": "level",
                        "maxDepth": 2,
                    }
                }
            ]
        )[0]["chain"]
        assert {item["_id"] for item in lookup} == {"g1", "g2", "ga", "gz"}, lookup
        assert {item["level"] for item in lookup} == {0, 1}, lookup
        fallback = gnodes.aggregate(
            [
                {
                    "$graphLookup": {
                        "from": "gedges",
                        "startWith": "n3",
                        "connectFromField": "from",
                        "connectToField": "to",
                        "as": "chain",
                        "depthField": "level",
                        "maxDepth": 2,
                        "restrictSearchWithMatch": {},
                    }
                }
            ]
        )[0]["chain"]
        assert [item["_id"] for item in lookup] == [
            item["_id"] for item in fallback
        ], (lookup, fallback)
        restricted = gnodes.aggregate(
            [
                {
                    "$graphLookup": {
                        "from": "gedges",
                        "startWith": "n3",
                        "connectFromField": "from",
                        "connectToField": "to",
                        "as": "chain",
                        "restrictSearchWithMatch": {"missing": True},
                    }
                }
            ]
        )[0]["chain"]
        assert restricted == [], restricted

        # Online rebuild mirrors writes into the target generation and swaps
        # only after a complete scan; reads use the old generation meanwhile.
        db.create_collection(
            "onodes",
            profile="columnar",
            columnar={"bucket_max_docs": 4, "compression": "max"},
        )
        db.create_collection(
            "oedges",
            profile="columnar",
            columnar={"bucket_max_docs": 4, "compression": "max"},
        )
        onodes = db.collection("onodes")
        oedges = db.collection("oedges")
        for i in range(5):
            onodes.insert({"_id": f"n{i}"})
        for i in range(4):
            oedges.insert({"_id": f"o{i}", "from": f"n{i}", "to": f"n{i + 1}"})
        onodes.set_graph("oedges")
        generation = onodes.get_graph()["generation"]
        status = onodes.reindex_graph(online=True)
        assert status["done"] is False, status
        assert status["target_generation"] == generation + 1, status
        # Writes during the rebuild are visible immediately and mirrored.
        oedges.insert({"_id": "online", "from": "n0", "to": "n4"})
        assert "n4" in child_ids(onodes.traverse("n0", max_depth=1)["roots"][0])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = onodes.graph_rebuild_status()
            if current["done"]:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("online graph rebuild did not finish")
        assert onodes.get_graph()["generation"] == generation + 1
        assert "n4" in child_ids(onodes.traverse("n0", max_depth=1)["roots"][0])
        assert onodes.reindex_graph(online=True)["done"] is False
        assert onodes.cancel_graph_rebuild() is True
        assert onodes.graph_rebuild_status()["done"] is True
        assert onodes.get_graph()["generation"] == generation + 1
        assert "n4" in child_ids(onodes.traverse("n0", max_depth=1)["roots"][0])

        # Temporal validity windows use adjacency summaries (no edge JSON read).
        db.create_collection("tn")
        db.create_collection("te")
        tn = db.collection("tn")
        te = db.collection("te")
        for node in ({"_id": "n1"}, {"_id": "n2"}, {"_id": "n3"}):
            tn.insert(node)
        tn.set_graph("te", valid_from_field="vf", valid_to_field="vt")
        te.insert({"_id": "t1", "from": "n1", "to": "n2", "vf": 1000, "vt": 2000})
        te.insert({"_id": "t2", "from": "n1", "to": "n3", "vf": 2000, "vt": 3000})
        at_1500 = tn.traverse("n1", max_depth=1, as_of=1500)
        assert child_ids(at_1500["roots"][0]) == ["n2"], at_1500
        at_2500 = tn.traverse("n1", max_depth=1, as_of=2500)
        assert child_ids(at_2500["roots"][0]) == ["n3"], at_2500
        assert tn.shortest_path("n1", "n2", as_of=2500)["found"] is False
        assert tn.shortest_path("n1", "n2", as_of=1500)["found"] is True

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
