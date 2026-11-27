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
        # Two members claim distinct contiguous ranges of the same group.
        claims = []
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
            assert batch["events"], batch
            claims.append((member, batch))
        ranges = sorted((claim["from_seq"], claim["to_seq"]) for _m, claim in claims)
        assert ranges == [(4, 4), (5, 5)], ranges

        # A stale lease cannot commit, and the failure is a client conflict
        # (409), not a server fault.
        first_member, first_batch = claims[0]
        status, code = api.error_with_status(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "workers",
                "seq": first_batch["to_seq"],
                "lease_id": "stale-lease",
            }
        )
        assert code == "stale_lease", code
        assert status == 409, f"stale_lease must be 409, got {status}"

        # Committing out of range is rejected too.
        assert (
            api.error(
                {
                    "op": "commit_stream_group",
                    "coll": "_events",
                    "group": "workers",
                    "seq": first_batch["to_seq"] + 10,
                    "lease_id": first_batch["lease_id"],
                }
            )
            == "commit_out_of_order"
        ), "out-of-range commit must be rejected"


        # The later range commits first; the committed offset waits for the hole.
        second_member, second_batch = claims[1]
        api.call(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "workers",
                "seq": second_batch["to_seq"],
                "lease_id": second_batch["lease_id"],
            }
        )
        assert (
            api.call({"op": "stream_stats", "coll": "_events"})["streams"][0]["groups"][0][
                "committed_seq"
            ]
            == 3
        ), "committed offset must not skip the hole"
        api.call(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "workers",
                "seq": first_batch["to_seq"],
                "lease_id": first_batch["lease_id"],
            }
        )
        seen = [first_batch["from_seq"], second_batch["from_seq"]]
        assert seen == [4, 5], seen
        assert (
            api.call({"op": "stream_stats", "coll": "_events"})["streams"][0]["groups"][0][
                "committed_seq"
            ]
            == 5
        )

        # A caught-up group must not be written to by an idle long-poll: no new
        # lease, no replication entry, no `updated_at` bump.
        def workers_group() -> dict[str, Any]:
            streams = api.call({"op": "stream_stats", "coll": "_events"})["streams"]
            return [g for g in streams[0]["groups"] if g["group"] == "workers"][0]

        idle_before = workers_group()
        for _ in range(3):
            idle = api.call(
                {
                    "op": "watch",
                    "coll": "_events",
                    "mode": "group",
                    "group": "workers",
                    "member": "idle",
                    "timeout_ms": 20,
                }
            )
            assert idle["events"] == [], idle
            assert idle.get("lease_id") is None, idle
        idle_after = workers_group()
        assert idle_after["committed_seq"] == idle_before["committed_seq"]
        assert (
            idle_after["updated_at"] == idle_before["updated_at"]
        ), "an idle claim must not write the group row"

        # Offsets never move backwards.
        assert (
            api.error(
                {
                    "op": "commit_stream_group",
                    "coll": "_events",
                    "group": "workers",
                    "seq": 3,
                }
            )
            == "stale_lease"
        ), "committing an unknown batch must be rejected"

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
        assert group_stats["committed_seq"] == 5, group_stats
        assert group_stats["head_seq"] == 5, group_stats
        assert group_stats["lag"] == 0, group_stats
        assert group_stats["inflight"] == 0, group_stats
        assert len(manual) == 2, manual

        # Redelivery: a member claims a batch, dies, and another member gets it
        # with a higher attempt counter.
        api.call({"op": "insert", "coll": "_events", "doc": {"type": "g"}})
        dying = api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "workers",
                "member": "m1",
                "limit": 1,
                "lease_ms": 1000,
                "timeout_ms": 100,
            }
        )
        assert dying["attempts"] == 1, dying
        time.sleep(1.3)
        redelivered = api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "workers",
                "member": "m2",
                "limit": 1,
                "lease_ms": 1000,
                "timeout_ms": 100,
            }
        )
        assert redelivered["from_seq"] == dying["from_seq"], redelivered
        assert redelivered["attempts"] == 2, redelivered
        workers = [
            group
            for group in api.call({"op": "stream_stats", "coll": "_events"})["streams"][0][
                "groups"
            ]
            if group["group"] == "workers"
        ][0]
        assert workers["redeliveries"] == 1, workers
        api.call(
            {
                "op": "nack_stream_group",
                "coll": "_events",
                "group": "workers",
                "lease_id": redelivered["lease_id"],
                "retry_in_ms": 0,
            }
        )

        # A partial ack inside a multi-event range is rejected and advances
        # nothing; the batch stays leased so the same member can finish it.
        api.call(
            {
                "op": "create_stream_group",
                "coll": "_events",
                "group": "partial",
                "start": "earliest",
            }
        )
        multi = api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "partial",
                "member": "p1",
                "limit": 3,
                "timeout_ms": 100,
            }
        )
        assert multi["to_seq"] > multi["from_seq"], multi
        assert (
            api.error(
                {
                    "op": "commit_stream_group",
                    "coll": "_events",
                    "group": "partial",
                    "seq": multi["from_seq"],
                    "lease_id": multi["lease_id"],
                }
            )
            == "commit_out_of_order"
        ), "a partial commit must be rejected"
        partial = [
            group
            for group in api.call({"op": "stream_stats", "coll": "_events"})["streams"][0][
                "groups"
            ]
            if group["group"] == "partial"
        ][0]
        assert partial["committed_seq"] == 0, partial
        assert partial["inflight"] == multi["to_seq"] - multi["from_seq"] + 1, partial
        api.call(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "partial",
                "seq": multi["to_seq"],
                "lease_id": multi["lease_id"],
            }
        )
        api.call({"op": "delete_stream_group", "coll": "_events", "group": "partial"})

        # Dead letters: a group with a single attempt allowance dead-letters a
        # batch that keeps failing, and the group is not blocked by it.
        api.call(
            {
                "op": "create_stream_group",
                "coll": "_events",
                "group": "poison",
                "start": "earliest",
                "max_attempts": 1,
            }
        )
        claim = api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "poison",
                "member": "p1",
                "limit": 100,
                "lease_ms": 1000,
                "timeout_ms": 100,
            }
        )
        assert claim["attempts"] == 1, claim
        time.sleep(1.3)
        # The expired lease is dead-lettered on the next claim attempt.
        api.call(
            {
                "op": "watch",
                "coll": "_events",
                "mode": "group",
                "group": "poison",
                "member": "p2",
                "limit": 100,
                "timeout_ms": 100,
            }
        )
        dead = api.call({"op": "stream_dead", "coll": "_events", "group": "poison"})
        assert len(dead["dead"]) == 1, dead
        poison_stats = [
            stream
            for stream in api.call({"op": "stream_stats", "coll": "_events"})["streams"]
            if stream["stream"] == "_events"
        ][0]
        poison_group = [g for g in poison_stats["groups"] if g["group"] == "poison"][0]
        assert poison_group["dead"] == 1, poison_group
        assert poison_group["lag"] == 0, poison_group

        # Retrying a dead letter puts it back in the queue.
        assert api.call(
            {
                "op": "retry_stream_dead",
                "coll": "_events",
                "group": "poison",
                "from_seq": dead["dead"][0]["from_seq"],
            }
        )["retried"] is True
        assert api.call({"op": "stream_dead", "coll": "_events", "group": "poison"})[
            "dead"
        ] == []
        api.call({"op": "delete_stream_group", "coll": "_events", "group": "poison"})

        # Metrics expose per-group lag and counters.
        metrics = api._client.get("/metrics/prometheus").text
        assert "nonodb_stream_lag_events" in metrics, metrics[:400]
        assert 'group="workers"' in metrics, "stream metrics must be labelled"
        assert "nonodb_stream_claims_total" in metrics
        for name in (
            "nonodb_stream_head_seq",
            "nonodb_stream_inflight_events",
            "nonodb_stream_dead_events",
            "nonodb_stream_redeliveries_total",
            "nonodb_stream_truncated_total",
            "nonodb_stream_commit_failures_total",
            "nonodb_stream_pruned_events_total",
        ):
            assert name in metrics, name
        for name in (
            "nonodb_stream_redeliveries_total",
            "nonodb_stream_commit_failures_total",
        ):
            series = [
                row
                for row in metrics.splitlines()
                if row.startswith(name + "{") and 'group="workers"' in row
            ]
            assert series, f"{name} must carry {{database,stream,group}} labels"
            assert any(float(row.split()[-1]) >= 1 for row in series), series
        truncated = [
            row
            for row in metrics.splitlines()
            if row.startswith("nonodb_stream_truncated_total{")
        ]
        assert truncated, "retention crossings must be labelled per group"
        assert "nonodb_stream_head_seq" in metrics
        # The global claim counter must actually move, not just be present.
        claims_line = next(
            (
                row
                for row in metrics.splitlines()
                if row.startswith("nonodb_stream_claims_total ")
            ),
            None,
        )
        assert claims_line is not None, "missing nonodb_stream_claims_total"
        assert int(claims_line.split()[-1]) >= 1, claims_line

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
        assert resumed["from_seq"] >= 6, resumed
        assert resumed["events"], resumed
        api.call(
            {
                "op": "commit_stream_group",
                "coll": "_events",
                "group": "workers",
                "seq": resumed["to_seq"],
                "lease_id": resumed["lease_id"],
            }
        )

        # Reset moves the group without touching the data.
        reset = api.call(
            {
                "op": "reset_stream_group",
                "coll": "_events",
                "group": "workers",
                "to": "latest",
            }
        )
        assert reset["committed_seq"] == reset["next_seq"], reset

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
        # Offset retention: a group without commits pins the window, and a group
        # behind the window gets `stream_truncated` instead of silent skips.
        api.call({"op": "create_collection", "coll": "ret_events"})
        api.call(
            {
                "op": "create_stream",
                "coll": "ret_events",
                "retention": {"max_events": 5},
            }
        )
        for n in range(20):
            api.call({"op": "insert", "coll": "ret_events", "doc": {"n": n}})
        api.call(
            {
                "op": "create_stream_group",
                "coll": "ret_events",
                "group": "pin",
                "start": "earliest",
            }
        )
        assert api.call({"op": "prune_stream", "coll": "ret_events"})["deleted"] == 0, (
            "a group with no commits must pin retention"
        )
        pinned = api.call({"op": "stream_stats", "coll": "ret_events"})["streams"][0]
        assert pinned["groups"][0]["committed_seq"] == 0, pinned
        drained = api.call(
            {
                "op": "watch",
                "coll": "ret_events",
                "mode": "group",
                "group": "pin",
                "member": "p1",
                "limit": 100,
                "timeout_ms": 100,
            }
        )
        api.call(
            {
                "op": "commit_stream_group",
                "coll": "ret_events",
                "group": "pin",
                "seq": drained["to_seq"],
                "lease_id": drained["lease_id"],
            }
        )
        pruned = api.call({"op": "prune_stream", "coll": "ret_events"})
        assert pruned["deleted"] == 15, pruned
        remaining = api.call(
            {"op": "find", "coll": "ret_events", "sort": [["_seq", 1]]}
        )
        assert [doc["_seq"] for doc in remaining] == list(range(16, 21)), remaining

        api.call({"op": "insert", "coll": "ret_events", "doc": {"n": 20}})
        api.call(
            {
                "op": "create_stream_group",
                "coll": "ret_events",
                "group": "late",
                "start": "earliest",
            }
        )
        status, code = api.error_with_status(
            {
                "op": "watch",
                "coll": "ret_events",
                "mode": "group",
                "group": "late",
                "timeout_ms": 100,
            }
        )
        assert code == "stream_truncated", code
        assert status == 410, f"stream_truncated must be 410, got {status}"
        late = [
            group
            for group in api.call({"op": "stream_stats", "coll": "ret_events"})["streams"][0][
                "groups"
            ]
            if group["group"] == "late"
        ][0]
        assert late["truncated_below"] == 16, late
        assert late["committed_seq"] == 0, late

        # Reset resumes at the oldest surviving event instead of failing again.
        reset = api.call(
            {
                "op": "reset_stream_group",
                "coll": "ret_events",
                "group": "late",
                "to": "earliest",
            }
        )
        assert reset["committed_seq"] == 15, reset
        assert reset.get("truncated_below") is None, reset
        resumedret_events = api.call(
            {
                "op": "watch",
                "coll": "ret_events",
                "mode": "group",
                "group": "late",
                "timeout_ms": 100,
            }
        )
        assert resumedret_events["from_seq"] == 16, resumedret_events

        # Age retention that removes the whole window still truncates a group
        # left behind instead of handing it an empty lease.
        api.call({"op": "create_collection", "coll": "age_events"})
        api.call(
            {
                "op": "create_stream",
                "coll": "age_events",
                "retention": {"max_age_ms": 1},
            }
        )
        for n in range(3):
            api.call({"op": "insert", "coll": "age_events", "doc": {"n": n}})
        time.sleep(0.03)
        assert api.call({"op": "prune_stream", "coll": "age_events"})["deleted"] == 3
        api.call(
            {
                "op": "create_stream_group",
                "coll": "age_events",
                "group": "behind",
                "start": "earliest",
            }
        )
        assert (
            api.error(
                {
                    "op": "watch",
                    "coll": "age_events",
                    "mode": "group",
                    "group": "behind",
                    "timeout_ms": 100,
                }
            )
            == "stream_truncated"
        ), "an empty retained window must fail with stream_truncated"
        behind = api.call({"op": "stream_stats", "coll": "age_events"})["streams"][0][
            "groups"
        ][0]
        assert behind["truncated_below"] == 4, behind
        assert behind["truncations"] == 1, behind
        assert behind["redeliveries"] == 0, behind

        api.close()
        print("streams/groups: ok")
        return 0
    finally:
        stop(proc)


if __name__ == "__main__":
    sys.exit(main())
