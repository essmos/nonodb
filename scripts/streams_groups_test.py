"""End-to-end stream offsets and consumer groups over the HTTP/UDS API.

    cargo build -p nonodb-daemon
    python3 scripts/streams_groups_test.py

Covers: writer-assigned `_seq`, group creation at earliest/latest, two members
sharing one group without duplicating delivered events, stale-lease rejection,
resume from `committed_seq` after a daemon restart, and `stream_stats` matching a
manual drain.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


class Api:
    def __init__(self, sock: Path) -> None:
        transport = httpx.HTTPTransport(uds=str(sock))
        self._client = httpx.Client(transport=transport, base_url="http://localhost")

    def close(self) -> None:
        self._client.close()

    def call(self, payload: dict[str, Any]) -> Any:
        response = self._client.post("/v1/exec", json=payload)
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"{payload.get('op')} failed: {body.get('error')}")
        return body.get("result")

    def error(self, payload: dict[str, Any]) -> str:
        return self.error_with_status(payload)[1]

    def error_with_status(self, payload: dict[str, Any]) -> tuple[int, str]:
        response = self._client.post("/v1/exec", json=payload)
        body = response.json()
        assert not body.get("ok"), body
        return response.status_code, body["error"]["code"]


def start(tmp: Path, env: dict[str, str]) -> tuple[subprocess.Popen[bytes], Path]:
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    full_env = env.copy()
    full_env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
        }
    )
    fh = log.open("ab")
    proc = subprocess.Popen(
        [str(DAEMON)], env=full_env, stdout=fh, stderr=subprocess.STDOUT
    )
    fh.close()
    wait_for_socket(sock, proc)
    return proc, sock


def stop(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:  # pragma: no cover - defensive
        proc.kill()
        proc.wait(timeout=10)


def main() -> int:
    if not DAEMON.exists():
        print(f"missing daemon binary: {DAEMON}", file=sys.stderr)
        return 2

    env = os.environ.copy()
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-streams-groups-"))
    proc, sock = start(tmp, env)
    try:
        api = Api(sock)
        api.call({"op": "create_collection", "coll": "_events"})

        # Offsets exist only after the stream is defined.
        api.call({"op": "insert", "coll": "_events", "doc": {"type": "pre"}})
        api.call({"op": "create_stream", "coll": "_events", "retention": {"max_events": 100}})
        api.call(
            {
                "op": "insert_many",
                "coll": "_events",
                "docs": [{"type": "a"}, {"type": "b"}, {"type": "c"}],
            }
        )
        docs = api.call({"op": "find", "coll": "_events", "sort": [["_seq", 1]]})
        offsets = [doc.get("_seq") for doc in docs]
        assert offsets == [None, 1, 2, 3], offsets
        head = api.call({"op": "get_stream", "coll": "_events"})["head_seq"]
        assert head == 3, head

        # A user-supplied offset would break ordering.
        assert api.error(
            {"op": "insert", "coll": "_events", "doc": {"_seq": 99}}
        ) == "invalid_query", "reserved _seq must be rejected"

        # Group created at latest sees only new events.
        group = api.call(
            {"op": "create_stream_group", "coll": "_events", "group": "workers"}
        )
        assert group["committed_seq"] == 3, group

        api.call({"op": "insert", "coll": "_events", "doc": {"type": "d"}})
        api.call({"op": "insert", "coll": "_events", "doc": {"type": "e"}})

        seen: list[int] = []
        members = ["m1", "m2"]
        for _ in range(4):
            for member in members:
                batch = api.call(
                    {
                        "op": "watch",
                        "coll": "_events",
                        "mode": "group",
                        "group": "workers",
                        "member": member,
                        "limit": 1,
                        "timeout_ms": 100,
                    }
                )
                events = batch["events"]
                if not events:
                    # The other member holds the lease.
                    assert batch["timed_out"] is True, batch
                    continue
                seq = events[0]["_seq"]
                assert seq not in seen, f"duplicate delivery of offset {seq}"
                assert seq == batch["committed_seq"] + 1, batch
                # A commit from a lease that does not own the batch is rejected.
                if seen:
                    # A client conflict, not a server fault.
                    status, code = api.error_with_status(
                        {
                            "op": "commit_stream_group",
                            "coll": "_events",
                            "group": "workers",
                            "seq": seq,
                            "lease_id": "stale-lease",
                        }
                    )
                    assert code == "stale_lease", code
                    assert status == 409, f"stale_lease must be 409, got {status}"
                api.call(
                    {
                        "op": "commit_stream_group",
                        "coll": "_events",
                        "group": "workers",
                        "seq": seq,
                        "lease_id": batch["lease_id"],
                    }
                )
                seen.append(seq)
                if len(seen) == 2:
                    break
            if len(seen) == 2:
                break
        assert seen == [4, 5], seen

        # Offsets never move backwards.
        assert api.error(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "workers",
                "seq": 3,
            }
        ) == "commit_out_of_order", "backwards commit must be rejected"

        # stats agree with a manual drain
        manual = api.call(
            {
                "op": "find",
                "coll": "_events",
                "filter": {"_seq": {"$gt": 3}},
                "sort": [["_seq", 1]],
            }
        )
        stats = api.call({"op": "stream_stats", "coll": "_events"})["streams"][0]
        group_stats = stats["groups"][0]
        assert group_stats["committed_seq"] == seen[-1], group_stats
        assert group_stats["head_seq"] == 5, group_stats
        assert group_stats["lag"] == 0, group_stats
        assert len(manual) == 2, manual

        # Restart: the group resumes from the committed offset.
        stop(proc)
        proc, sock = start(tmp, env)
        api = Api(sock)
        api.call({"op": "insert", "coll": "_events", "doc": {"type": "f"}})
        resumed = api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "workers",
                "member": "m1",
                "limit": 10,
                "timeout_ms": 200,
            }
        )
        assert [event["_seq"] for event in resumed["events"]] == [6], resumed
        assert resumed["committed_seq"] == 5, resumed

        # Reset moves the group without touching the data.
        reset = api.call(
            {
                "op": "reset_stream_group",
                "coll": "_events",
                "group": "workers",
                "to": "latest",
            }
        )
        assert reset["committed_seq"] == 6, reset
        assert api.call({"op": "count", "coll": "_events"}) == api.call(
            {"op": "count", "coll": "_events"}
        )

        # Deleting the group removes only its offset.
        assert api.call(
            {"op": "delete_stream_group", "coll": "_events", "group": "workers"}
        )["deleted"] is True
        assert api.call({"op": "stream_stats", "coll": "_events"})["streams"][0][
            "groups"
        ] == []

        # `create_stream` bootstraps the collection instead of requiring a
        # separate `create_collection`.
        api.call({"op": "create_stream", "coll": "boot_events"})
        api.call({"op": "insert", "coll": "boot_events", "doc": {"n": 1}})
        assert (
            api.call({"op": "get_stream", "coll": "boot_events"})["head_seq"] == 1
        ), "create_stream must create the collection"

        # Time-series collections cannot back a stream.
        api.call(
            {
                "op": "create_collection",
                "coll": "ts_events",
                "profile": "timeseries",
                "timeseries": {"timeField": "ts"},
            }
        )
        assert (
            api.error({"op": "create_stream", "coll": "ts_events"}) == "invalid_query"
        ), "a time-series collection must not accept a stream"

        api.close()
        print("streams/groups: ok")
        return 0
    finally:
        stop(proc)


if __name__ == "__main__":
    sys.exit(main())
