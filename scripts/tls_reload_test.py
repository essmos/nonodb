#!/usr/bin/env python3
"""Verify that the daemon reloads rotated TLS server certificates."""

from __future__ import annotations

import json
import os
import socket
import ssl
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run(*args: str, cwd: Path) -> None:
    proc = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {proc.stderr.strip()}")


def make_cert(cwd: Path, serial: str, out: Path) -> None:
    run(
        "openssl",
        "req",
        "-new",
        "-key",
        "server.key",
        "-subj",
        "/CN=127.0.0.1",
        "-out",
        "server.csr",
        cwd=cwd,
    )
    (cwd / "ext.cnf").write_text(
        "subjectAltName=IP:127.0.0.1\n"
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    run(
        "openssl",
        "x509",
        "-req",
        "-in",
        "server.csr",
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-set_serial",
        serial,
        "-days",
        "1",
        "-extfile",
        "ext.cnf",
        "-out",
        str(out),
        cwd=cwd,
    )


def peer_serial(port: int, cafile: Path) -> int:
    context = ssl.create_default_context(cafile=str(cafile))
    with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
        with context.wrap_socket(raw, server_hostname="127.0.0.1") as tls:
            der = tls.getpeercert(binary_form=True)
            cert_text = ssl.DER_cert_to_PEM_cert(der)
            proc = subprocess.run(
                ["openssl", "x509", "-noout", "-serial"],
                input=cert_text,
                capture_output=True,
                text=True,
                check=True,
            )
            serial = proc.stdout.strip().split("=", 1)[1]
            return int(serial, 16)


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-tls-reload-", dir="/var/tmp"))
    data = temp / "data"
    data.mkdir()
    auth = temp / "auth.json"
    proc: subprocess.Popen | None = None
    log = None
    try:
        run("openssl", "genrsa", "-out", "ca.key", "2048", cwd=temp)
        run(
            "openssl",
            "req",
            "-new",
            "-x509",
            "-key",
            "ca.key",
            "-subj",
            "/CN=NonoDB Test CA",
            "-days",
            "1",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-out",
            "ca.crt",
            cwd=temp,
        )
        run("openssl", "genrsa", "-out", "server.key", "2048", cwd=temp)
        make_cert(temp, "0x1001", temp / "server1.crt")
        make_cert(temp, "0x1002", temp / "server2.crt")
        cert = temp / "server.crt"
        cert.write_bytes((temp / "server1.crt").read_bytes())

        env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
        subprocess.run([str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True)

        port = free_port()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_TLS_CERT=str(cert),
            NONODB_TLS_KEY=str(temp / "server.key"),
            NONODB_TLS_RELOAD_INTERVAL="1",
            NONODB_LOG="warn",
        )
        log = open(temp / "daemon.log", "w", encoding="utf-8")
        proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)

        deadline = time.monotonic() + 15
        first = None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"daemon exited with {proc.returncode}")
            try:
                first = peer_serial(port, temp / "ca.crt")
                break
            except (OSError, ssl.SSLError, subprocess.CalledProcessError):
                time.sleep(0.1)
        if first is None:
            raise RuntimeError("daemon did not start TLS")
        assert first == 0x1001, hex(first)

        temp_cert = temp / "server.new.crt"
        temp_cert.write_bytes((temp / "server2.crt").read_bytes())
        os.replace(temp_cert, cert)

        deadline = time.monotonic() + 15
        second = first
        while time.monotonic() < deadline:
            try:
                second = peer_serial(port, temp / "ca.crt")
            except (OSError, ssl.SSLError, subprocess.CalledProcessError):
                time.sleep(0.1)
                continue
            if second != first:
                break
            time.sleep(0.2)
        if second == first:
            log.flush()
            tail = (temp / "daemon.log").read_text(encoding="utf-8")[-4000:]
            raise RuntimeError(f"TLS certificate was not reloaded\n{tail}")
        assert second == 0x1002, hex(second)
        print(json.dumps({"ok": True, "before": first, "after": second}))
        return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if log is not None:
            log.close()
        import shutil

        if os.environ.get("NONODB_KEEP_TLS_TEMP"):
            print(f"kept TLS test directory: {temp}")
        else:
            shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
