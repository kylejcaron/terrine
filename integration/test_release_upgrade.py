"""Published writers reopen in place with the weekly source or release wheel."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.request import urlopen

import pytest
from packaging.version import Version

from scripts import release_upgrade
from scripts.release_upgrade import (
    PublishedWheel,
    candidate_version,
    proof_route,
    published_predecessor,
    sha256,
)
from tests._timing import HANG_GUARD_SECONDS
from tests.integration import release_upgrade_probe as probe

RELEASES_PATH = probe.CHECKOUT / "tests" / "artifacts" / "RELEASES"
RELEASES = [
    line.strip()
    for line in RELEASES_PATH.read_text("utf-8").splitlines()
    if line.strip()
]
CANDIDATE = os.environ.get("TERRINE_UPGRADE_CANDIDATE_WHEEL")
RUNTIME_SETTINGS = ("TERRINE_RUNTIME_NAMESPACE", "TERRINE_RUNTIME_SNAPSHOT_ROOT")
PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"
PROBE = str(Path(probe.__file__).resolve())


def _run(command: list[str], *, cwd: Path) -> None:
    assert not cwd.resolve().is_relative_to(probe.CHECKOUT), cwd
    environment = {
        name: value
        for name, value in os.environ.items()
        if name not in ("VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME", *RUNTIME_SETTINGS)
    }
    result = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
        check=False,
    )
    print(result.stdout, end="")
    assert result.returncode == 0, result.stderr


def _install(wheel: Path, digest: str, directory: Path) -> tuple[str, str, str]:
    version = str(candidate_version(wheel))
    assert sha256(wheel) == digest, wheel
    source = f"{wheel.as_uri()}#sha256={digest}"
    environment = directory / "venv"
    executable = str(environment / "bin" / "python")
    _run(["uv", "venv", "--python", PYTHON, str(environment)], cwd=directory)
    _run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            executable,
            "--link-mode",
            "copy",
            "--reinstall-package",
            "terrine",
            f"terrine[duckdb] @ {source}",
        ],
        cwd=directory,
    )
    assert sha256(wheel) == digest, wheel
    return executable, version, source


def _wheel_probe(installation: tuple[str, str, str], operation: str, output: Path) -> None:
    executable, version, source = installation
    _run([executable, "-I", PROBE, operation, version, source, str(output)], cwd=output)


def test_candidate_wheel_proof_exempts_only_proven_bookkeeping(tmp_path, monkeypatch):
    monkeypatch.setattr(release_upgrade, "CHECKOUT", tmp_path)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")

    def git(*args):
        subprocess.run(
            ["git", "-c", "user.name=Terrine", "-c", "user.email=test@example.com", *args],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

    predecessor = PublishedWheel(Version("0.1.0a2"), "prior.whl", "", "0" * 64)
    git("init")
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.0a2"\n')
    git("add", "pyproject.toml")
    git("commit", "-m", "Published writer")
    assert proof_route(predecessor)[0] is True

    git("tag", "v0.1.0a2")
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.0a3"\n')
    (tmp_path / "README.md").write_text("Release notes\n")
    git("add", "pyproject.toml", "README.md")
    git("commit", "-m", "Bookkeeping")
    assert proof_route(predecessor)[0] is False

    source = tmp_path / "terrine" / "runtime"
    source.mkdir(parents=True)
    (source / "saved_plan.py").write_text("FORMAT_VERSION = 2\n")
    git("add", "terrine/runtime/saved_plan.py")
    git("commit", "-m", "Saved format change")
    assert proof_route(predecessor)[0] is True


if CANDIDATE is None and RELEASES:

    @pytest.mark.scale
    @pytest.mark.parametrize("entry", RELEASES)
    def test_dataset_staged_by_release_reopens_in_place(entry, tmp_path, monkeypatch):
        for name in RUNTIME_SETTINGS:
            monkeypatch.delenv(name, raising=False)
        _run(
            [
                "uv",
                "run",
                "--no-project",
                "--isolated",
                "--python",
                PYTHON,
                "--with",
                f"terrine[duckdb]=={entry}",
                "python",
                "-I",
                PROBE,
                "produce",
                entry,
                "",
                str(tmp_path),
            ],
            cwd=tmp_path,
        )
        probe.reopen(tmp_path)


if CANDIDATE is not None:

    @pytest.fixture(scope="module")
    def candidate_installation(tmp_path_factory):
        assert CANDIDATE is not None
        wheel = Path(CANDIDATE)
        version = candidate_version(wheel)
        digest = sha256(wheel)
        predecessor = published_predecessor(version)
        print(f"Published predecessor: {predecessor}")
        print(f"Published predecessor SHA-256: {predecessor.sha256}")
        directory = tmp_path_factory.mktemp("candidate-wheel")
        installation = _install(wheel, digest, directory)
        print(f"Candidate wheel SHA-256: {digest}", flush=True)
        return installation, predecessor

    @pytest.mark.scale
    @pytest.mark.parametrize("writer", ("predecessor", "candidate"))
    def test_candidate_wheel_reopens_real_owner_data(writer, candidate_installation, tmp_path):
        candidate, predecessor = candidate_installation
        producer = candidate
        if writer == "predecessor":
            wheel = tmp_path / predecessor.filename
            with urlopen(predecessor.url, timeout=30) as response, wheel.open("wb") as output:
                shutil.copyfileobj(response, output)
            producer = _install(wheel, predecessor.sha256, tmp_path)
        _wheel_probe(producer, "produce", tmp_path)
        _wheel_probe(candidate, "reopen", tmp_path)
