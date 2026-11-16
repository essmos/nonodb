#!/usr/bin/env python3
"""Test MongoDB wire resource limits without external driver dependencies.

The test starts a temporary daemon and verifies:

* the concurrent-connection limit rejects excess sockets,
* the first connection remains usable,
* the read timeout closes an idle connection,
* the permit is released and a later connection is accepted,
* invalid limit configuration fails loudly.

Run it from the repository root:

    python3 scripts/mongo_wire_limits_test.py
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def daemon_binary() -> Path:
    configured = os.environ.get("NONODB_DAEMON")
    if configured:
        return Path(configured)
    return ROOT / "target" / "debug" / "nonodb-daemon"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wire_env(tmp: Path, port: int, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(tmp / "nonodb.sock"),
            "NONODB_MONGO_ADDR": f"127.0.0.1:{port}",
        }
    )
    if extra:
        env.update(extra)
    return env


def start_daemon(env: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [str(daemon_binary())],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_listener(proc: subprocess.Popen[bytes], port: int, timeout: float = 10.0) -> socket.socket:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"daemon exited with {proc.returncode}")
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=1)
        except OSError:
            time.sleep(0.05)
    raise AssertionError("Mongo wire listener did not start")


def ping_message() -> bytes:
    # BSON document {"ping": 1}, wrapped in an OP_MSG body section.
    document = (
        (15).to_bytes(4, "little")
        + b"\x10ping\x00"
        + (1).to_bytes(4, "little")
        + b"\x00"
    )
    payload = (0).to_bytes(4, "little") + b"\x00" + document
    return (
        (16 + len(payload)).to_bytes(4, "little")
        + (1).to_bytes(4, "little")
        + (0).to_bytes(4, "little")
        + (2013).to_bytes(4, "little")
        + payload
    )


def read_message(sock: socket.socket) -> bytes:
    header = sock.recv(16)
    if len(header) != 16:
        raise AssertionError("Mongo wire response header is truncated")
    length = int.from_bytes(header[:4], "little")
    payload = bytearray()
    while len(payload) < length - 16:
        chunk = sock.recv(length - 16 - len(payload))
        if not chunk:
            raise AssertionError("Mongo wire response is truncated")
        payload.extend(chunk)
    return header + payload


def expect_closed(sock: socket.socket, timeout: float = 5.0) -> None:
    sock.settimeout(timeout)
    try:
        data = sock.recv(1)
    except ConnectionResetError:
        return
    if data:
        raise AssertionError(f"expected a closed connection, received {data!r}")


def stop_daemon(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def test_limits(tmp: Path) -> None:
    port = free_port()
    env = wire_env(
        tmp,
        port,
        {
            "NONODB_MONGO_MAX_CONNECTIONS": "1",
            "NONODB_MONGO_READ_TIMEOUT_SECS": "1",
            "NONODB_MONGO_WRITE_TIMEOUT_SECS": "0",
        },
    )
    proc = start_daemon(env)
    first = None
    try:
        first = wait_listener(proc, port)
        time.sleep(0.2)

        second = socket.create_connection(("127.0.0.1", port), timeout=2)
        expect_closed(second)
        second.close()

        first.sendall(ping_message())
        response = read_message(first)
        if len(response) < 16 or int.from_bytes(response[12:16], "little") != 2013:
            raise AssertionError("first connection did not receive an OP_MSG reply")

        expect_closed(first, timeout=4)
        first.close()
        first = None

        later = wait_listener(proc, port)
        try:
            later.sendall(ping_message())
            if len(read_message(later)) < 16:
                raise AssertionError("connection limit permit was not released")
        finally:
            later.close()
    finally:
        if first is not None:
            first.close()
        stop_daemon(proc)


def test_invalid_config(tmp: Path) -> None:
    port = free_port()
    env = wire_env(tmp, port, {"NONODB_MONGO_MAX_CONNECTIONS": "0"})
    proc = subprocess.Popen(
        [str(daemon_binary())],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        output, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate(timeout=5)
        raise AssertionError("invalid MongoDB wire limit was accepted") from None
    if proc.returncode == 0:
        raise AssertionError("invalid MongoDB wire limit did not fail")
    if "NONODB_MONGO_MAX_CONNECTIONS" not in output:
        raise AssertionError(f"missing configuration error in daemon output: {output}")


def main() -> int:
    binary = daemon_binary()
    if not binary.is_file():
        print(f"missing daemon binary: {binary}", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="nonodb-mongo-limits-") as tmp:
        test_limits(Path(tmp))
        test_invalid_config(Path(tmp))
    print("mongo wire limits test: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
