"""End-to-end CRL/OCSP client-certificate revocation test.

    cargo build -p nonodb-daemon
    python3 scripts/revocation_test.py
"""

from __future__ import annotations

import hashlib
import http.server
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def run(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout + result.stderr


def make_fixtures(tmp: Path) -> tuple[Path, Path, Path, Path, Path, str]:
    ca_key = tmp / "ca.key"
    ca_crt = tmp / "ca.crt"
    run(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_crt),
        "-days",
        "2",
        "-subj",
        "/CN=nonodb revocation test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )

    def leaf(name: str, extension: str) -> tuple[Path, Path]:
        key = tmp / f"{name}.key"
        csr = tmp / f"{name}.csr"
        crt = tmp / f"{name}.crt"
        run("openssl", "genrsa", "-out", str(key), "2048")
        run("openssl", "req", "-new", "-key", str(key), "-subj", f"/CN={name}", "-out", str(csr))
        ext = tmp / f"{name}.ext"
        ext.write_text(extension)
        run(
            "openssl",
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(ca_crt),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(crt),
            "-days",
            "2",
            "-sha256",
            "-extfile",
            str(ext),
        )
        return key, crt

    server_key, server_crt = leaf(
        "server",
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )
    good_key, good_crt = leaf(
        "good-client",
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=clientAuth\n",
    )
    bad_key, bad_crt = leaf(
        "revoked-client",
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=clientAuth\n",
    )

    # A tiny CA database lets openssl emit a real, signed CRL.
    ca_dir = tmp / "ca-db"
    (ca_dir / "newcerts").mkdir(parents=True)
    (ca_dir / "index.txt").write_text("")
    (ca_dir / "serial").write_text("1000\n")
    (ca_dir / "crlnumber").write_text("1000\n")
    config = tmp / "openssl.cnf"
    config.write_text(
        f"""
[ ca ]
default_ca = CA_default
[ CA_default ]
dir = {ca_dir}
database = $dir/index.txt
new_certs_dir = $dir/newcerts
serial = $dir/serial
crlnumber = $dir/crlnumber
certificate = {ca_crt}
private_key = {ca_key}
default_md = sha256
default_days = 2
default_crl_days = 2
policy = any
unique_subject = no
[ any ]
commonName = supplied
"""
    )
    empty_crl = tmp / "empty.crl.pem"
    run("openssl", "ca", "-config", str(config), "-gencrl", "-out", str(empty_crl), "-batch")
    # Add the revoked certificate to the CA database, then generate the CRL.
    serial = run("openssl", "x509", "-in", str(bad_crt), "-noout", "-serial")
    serial_hex = serial.strip().split("=", 1)[1].upper()
    run(
        "openssl",
        "ca",
        "-config",
        str(config),
        "-revoke",
        str(bad_crt),
        "-crl_reason",
        "keyCompromise",
        "-batch",
    )
    crl = tmp / "revoked.crl.pem"
    run("openssl", "ca", "-config", str(config), "-gencrl", "-out", str(crl), "-batch")
    assert serial_hex
    return ca_crt, server_crt, server_key, crl, empty_crl, serial_hex


def write_auth(path: Path, certs: list[tuple[str, str]], token_hash: str) -> None:
    doc = {
        "version": 1,
        "principals": {name: {"scopes": ["*:*"]} for name, _ in certs},
        "tokens": [
            {
                "kid": "revocation",
                "sha256": token_hash,
                "principal": certs[0][0],
                "disabled": False,
            }
        ],
        "certificates": [
            {"sha256": fp, "principal": name, "disabled": False} for name, fp in certs
        ],
    }
    path.write_text(json.dumps(doc, indent=2) + "\n")


def fingerprint(crt: Path) -> str:
    result = subprocess.run(
        ["openssl", "x509", "-in", str(crt), "-outform", "DER"], capture_output=True, check=True
    )
    result = subprocess.run(
        ["openssl", "dgst", "-sha256"], input=result.stdout, capture_output=True, check=True
    )
    return result.stdout.decode().strip().split("=")[-1].strip()


class Daemon:
    def __init__(self, tmp: Path, name: str, extra: dict[str, str]) -> None:
        self.port = free_port()
        self.sock = tmp / f"{name}.sock"
        self.log = tmp / f"{name}.log"
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(tmp / f"{name}-data"),
            NONODB_SOCK=str(self.sock),
            NONODB_TCP=f"127.0.0.1:{self.port}",
            NONODB_TLS_CERT=str(tmp / "server.crt"),
            NONODB_TLS_KEY=str(tmp / "server.key"),
            NONODB_TLS_CLIENT_CA=str(tmp / "ca.crt"),
            NONODB_TLS_CLIENT_AUTH="required",
            RUST_LOG="warn",
        )
        env.update(extra)
        self.handle = self.log.open("wb")
        self.proc = subprocess.Popen(
            [str(ROOT / "target/debug/nonodb-daemon")],
            env=env,
            stdout=self.handle,
            stderr=subprocess.STDOUT,
        )
        for _ in range(300):
            if self.sock.exists():
                break
            if self.proc.poll() is not None:
                self.handle.close()
                raise RuntimeError(self.log.read_text(errors="replace")[-3000:])
            time.sleep(0.05)
        else:
            self.stop()
            raise RuntimeError("daemon did not start")

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.handle.close()

    def client(self, name: str, token: str | None) -> NonoDB:
        return NonoDB(
            base_url=f"https://localhost:{self.port}",
            token=token,
            ca_cert=str(Path(self.sock).parent / "ca.crt"),
            client_cert=str(Path(self.sock).parent / f"{name}.crt"),
            client_key=str(Path(self.sock).parent / f"{name}.key"),
            timeout=5,
        )


def accepted(daemon: Daemon, name: str, token: str | None) -> None:
    db = daemon.client(name, token)
    try:
        db.exec({"op": "list_collections"})
    finally:
        db.close()


def rejected(daemon: Daemon, name: str, token: str, marker: str) -> None:
    db = daemon.client(name, token)
    try:
        try:
            db.exec({"op": "list_collections"})
        except Exception:
            pass
        else:
            raise AssertionError(f"{name} unexpectedly succeeded")
    finally:
        db.close()
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        text = daemon.log.read_text(errors="replace")
        if marker in text:
            return
        time.sleep(0.05)
    raise AssertionError(f"missing `{marker}` in daemon log:\n{daemon.log.read_text(errors='replace')[-4000:]}")


def der_tlv(tag: int, value: bytes) -> bytes:
    length = len(value)
    if length < 0x80:
        encoded = bytes([length])
    else:
        raw = length.to_bytes((length.bit_length() + 7) // 8, "big")
        encoded = bytes([0x80 | len(raw)]) + raw
    return bytes([tag]) + encoded + value


def der_seq(*items: bytes) -> bytes:
    return der_tlv(0x30, b"".join(items))


def der_oid(value: bytes) -> bytes:
    return der_tlv(0x06, value)


def der_int(value: bytes) -> bytes:
    value = value.lstrip(b"\x00") or b"\x00"
    if value[0] & 0x80:
        value = b"\x00" + value
    return der_tlv(0x02, value)


def der_time(ts: int) -> bytes:
    return der_tlv(0x18, time.strftime("%Y%m%d%H%M%SZ", time.gmtime(ts)).encode())


def read_tlv(data: bytes, offset: int = 0) -> tuple[int, bytes, int]:
    tag = data[offset]
    first = data[offset + 1]
    offset += 2
    if first & 0x80:
        count = first & 0x7F
        length = int.from_bytes(data[offset : offset + count], "big")
        offset += count
    else:
        length = first
    value = data[offset : offset + length]
    return tag, value, offset + length


def request_serial(body: bytes) -> bytes:
    tag, seq, _ = read_tlv(body)
    assert tag == 0x30
    tag, tbs, _ = read_tlv(seq)
    assert tag == 0x30
    tag, requests, _ = read_tlv(tbs)
    assert tag == 0x30
    tag, request, _ = read_tlv(requests)
    assert tag == 0x30
    tag, cert_id, _ = read_tlv(request)
    assert tag == 0x30
    offset = 0
    for _ in range(3):
        _, _, offset = read_tlv(cert_id, offset)
    tag, serial, _ = read_tlv(cert_id, offset)
    assert tag == 0x02
    return serial.lstrip(b"\x00") or b"\x00"


def ocsp_response(serial: bytes, revoked: bool) -> bytes:
    sha1 = der_seq(der_oid(b"\x2b\x0e\x03\x02\x1a"), b"\x05\x00")
    cert_id = der_seq(sha1, der_tlv(0x04, b"x" * 20), der_tlv(0x04, b"y" * 20), der_int(serial))
    status = der_tlv(0x81, der_time(int(time.time()))) if revoked else b"\x80\x00"
    single = der_seq(cert_id, status, der_time(int(time.time())))
    response_data = der_seq(der_tlv(0xA1, b""), der_time(int(time.time())), der_seq(single))
    basic = der_seq(response_data)
    response_bytes = der_seq(der_oid(b"\x2b\x06\x01\x05\x05\x07\x30\x01\x01"), der_tlv(0x04, basic))
    return der_seq(der_tlv(0x0A, b"\x00"), der_tlv(0xA0, response_bytes))


class OcspHandler(http.server.BaseHTTPRequestHandler):
    revoked_serial = b""

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        serial = request_serial(body)
        payload = ocsp_response(serial, serial == self.revoked_serial)
        self.send_response(200)
        self.send_header("content-type", "application/ocsp-response")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args) -> None:
        pass


def main() -> int:
    if not (ROOT / "target/debug/nonodb-daemon").exists():
        print("missing target/debug/nonodb-daemon; cargo build first", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="nonodb-revocation-") as td:
        tmp = Path(td)
        ca, server_crt, server_key, crl, empty_crl, bad_serial = make_fixtures(tmp)
        # Daemon helper expects these conventional names.
        (tmp / "ca.crt").write_bytes(ca.read_bytes())
        (tmp / "server.crt").write_bytes(server_crt.read_bytes())
        (tmp / "server.key").write_bytes(server_key.read_bytes())
        # Client helper names.
        (tmp / "good.crt").write_bytes((tmp / "good-client.crt").read_bytes())
        (tmp / "good.key").write_bytes((tmp / "good-client.key").read_bytes())
        (tmp / "revoked.crt").write_bytes((tmp / "revoked-client.crt").read_bytes())
        (tmp / "revoked.key").write_bytes((tmp / "revoked-client.key").read_bytes())

        good_fp = fingerprint(tmp / "good.crt").lower()
        bad_fp = fingerprint(tmp / "revoked.crt").lower()
        token = "revocation-secret"
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        auth = tmp / "auth.json"
        write_auth(auth, [("good", good_fp), ("revoked", bad_fp)], token_hash)
        export_token = f"nodb_revocation_{token}"

        crl_daemon = Daemon(
            tmp,
            "crl",
            {
                "NONODB_AUTH_FILE": str(auth),
                "NONODB_TLS_CRL_FILE": str(crl),
                "NONODB_TLS_REVOCATION_FAIL_MODE": "closed",
            },
        )
        try:
            accepted(crl_daemon, "good", export_token)
            rejected(crl_daemon, "revoked", export_token, "revoked by CRL")
            print("crl: good accepted, revoked rejected")
        finally:
            crl_daemon.stop()


        # A CRL file is refreshed on its interval; a newly revoked serial takes
        # effect without restarting the daemon.
        refresh_path = tmp / "refresh.crl.pem"
        refresh_path.write_bytes(empty_crl.read_bytes())
        refresh_daemon = Daemon(
            tmp,
            "refresh",
            {
                "NONODB_AUTH_FILE": str(auth),
                "NONODB_TLS_CRL_FILE": str(refresh_path),
                "NONODB_TLS_REVOCATION_REFRESH_SECS": "1",
                "NONODB_TLS_REVOCATION_FAIL_MODE": "closed",
            },
        )
        try:
            accepted(refresh_daemon, "revoked", None)
            refresh_path.write_bytes(crl.read_bytes())
            time.sleep(1.2)
            rejected(refresh_daemon, "revoked", export_token, "revoked by CRL")
            print("crl refresh: newly revoked serial rejected")
        finally:
            refresh_daemon.stop()

        # Fail-closed refuses to start when the configured source is unusable.
        failed = subprocess.run(
            [
                str(ROOT / "target/debug/nonodb-daemon"),
            ],
            env={
                **os.environ,
                "NONODB_DATA": str(tmp / "closed-data"),
                "NONODB_SOCK": str(tmp / "closed.sock"),
                "NONODB_TCP": f"127.0.0.1:{free_port()}",
                "NONODB_TLS_CERT": str(tmp / "server.crt"),
                "NONODB_TLS_KEY": str(tmp / "server.key"),
                "NONODB_TLS_CLIENT_CA": str(tmp / "ca.crt"),
                "NONODB_TLS_CRL_FILE": str(tmp / "missing.crl"),
                "NONODB_TLS_REVOCATION_FAIL_MODE": "closed",
                "NONODB_AUTH_FILE": str(auth),
            },
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert failed.returncode != 0, failed.stderr
        print("crl fail-closed: startup rejected")

        open_daemon = Daemon(
            tmp,
            "fail-open",
            {
                "NONODB_AUTH_FILE": str(auth),
                "NONODB_TLS_CRL_FILE": str(tmp / "missing.crl"),
                "NONODB_TLS_REVOCATION_FAIL_MODE": "open",
            },
        )
        try:
            accepted(open_daemon, "good", export_token)
            print("crl fail-open: connection accepted")
        finally:
            open_daemon.stop()

        # Local OCSP responder: revoked serial is rejected, good serial passes.
        handler = OcspHandler
        handler.revoked_serial = bytes.fromhex(bad_serial)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        ocsp_daemon = Daemon(
            tmp,
            "ocsp",
            {
                "NONODB_AUTH_FILE": str(auth),
                "NONODB_TLS_OCSP_URL": f"http://127.0.0.1:{server.server_address[1]}/",
                "NONODB_TLS_REVOCATION_FAIL_MODE": "closed",
                "NONODB_TLS_OCSP_CACHE_SECS": "60",
            },
        )
        try:
            accepted(ocsp_daemon, "good", export_token)
            rejected(ocsp_daemon, "revoked", export_token, "revoked by OCSP")
            print("ocsp: good accepted, revoked rejected")
        finally:
            ocsp_daemon.stop()
            server.shutdown()
            server.server_close()
    print("revocation: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
