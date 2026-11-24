"""Build the Python client package and test it in a clean virtualenv.

    python3 scripts/python_package_test.py
    python3 scripts/python_package_test.py --dist dist

The build step uses the local environment (`--no-isolation`); the workflow
installs `build`, `setuptools`, and `wheel` first. The publication workflow is
separate and is never triggered by this test.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "client"


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(command))
    subprocess.run(command, check=True, cwd=ROOT, env=env)


def project_version() -> str:
    content = (CLIENT / "pyproject.toml").read_text()
    try:
        import tomllib

        return str(tomllib.loads(content)["project"]["version"])
    except ModuleNotFoundError:  # pragma: no cover - Python 3.9/3.10
        match = re.search(r'^version = "([^"]+)"', content, re.MULTILINE)
        assert match, "pyproject.toml has no project version"
        return match.group(1)


def verify_dist(dist: Path, version: str) -> tuple[Path, Path]:
    wheels = sorted(dist.glob("nonodb_client-*.whl"))
    sdists = sorted(dist.glob("nonodb_client-*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(sdists) == 1, sdists
    wheel, sdist = wheels[0], sdists[0]
    assert version in wheel.name and version in sdist.name, (wheel, sdist)

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert "nonodb.py" in names, names
        metadata = archive.read(
            f"nonodb_client-{version}.dist-info/METADATA"
        ).decode()
        assert f"Version: {version}" in metadata, metadata[:400]
        assert "Requires-Dist: httpx>=0.24" in metadata, metadata[:800]
        assert "License-Expression: MIT" in metadata, metadata[:800]

    with tarfile.open(sdist) as archive:
        names = archive.getnames()
        root = f"nonodb_client-{version}"
        assert f"{root}/nonodb.py" in names, names
        assert f"{root}/pyproject.toml" in names, names
        assert any(name.endswith("python-readme.md") for name in names), names
        assert not any("__pycache__" in name or name.endswith(".pyc") for name in names), names
    return wheel, sdist


def test_clean_venv(wheel: Path, version: str) -> None:
    with tempfile.TemporaryDirectory(prefix="nonodb-python-venv-") as td:
        venv = Path(td) / "venv"
        run([sys.executable, "-m", "venv", str(venv)])
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        # This is a clean environment: dependencies come from the wheel metadata.
        run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--quiet",
                str(wheel),
            ]
        )
        code = f"""
import importlib.metadata
import nonodb
from nonodb import AsyncCollection, AsyncNonoDB, Collection, NonoDB, VaultDB

assert nonodb.__version__ == {version!r}, nonodb.__version__
assert importlib.metadata.version("nonodb-client") == {version!r}
with NonoDB("/tmp/nonodb-package-test-does-not-exist.sock") as db:
    assert db.database == "default"
assert AsyncNonoDB and AsyncCollection and Collection and VaultDB
print("clean venv import: ok", nonodb.__version__, nonodb.__file__)
"""
        run([str(python), "-c", code])
        run([str(python), "-m", "pip", "show", "nonodb-client"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dist",
        type=Path,
        help="Use an existing dist directory instead of rebuilding the package",
    )
    args = parser.parse_args()
    version = project_version()
    generated = args.dist is None
    temp = None
    dist = args.dist
    if dist is None:
        temp = tempfile.TemporaryDirectory(prefix="nonodb-python-dist-")
        dist = Path(temp.name) / "dist"
        try:
            run(
                [
                    sys.executable,
                    "-m",
                    "build",
                    "--no-isolation",
                    str(CLIENT),
                    "--outdir",
                    str(dist),
                ]
            )
        finally:
            # setuptools writes these inside the source tree.
            shutil.rmtree(CLIENT / "build", ignore_errors=True)
            shutil.rmtree(CLIENT / "nonodb_client.egg-info", ignore_errors=True)
    assert dist is not None
    wheel, sdist = verify_dist(dist, version)
    test_clean_venv(wheel, version)
    test_clean_venv(sdist, version)
    if temp is not None:
        temp.cleanup()
    print("python package: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
