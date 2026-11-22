"""Route candidate wheel proofs from the published Terrine release history."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import file_digest
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from packaging.utils import parse_wheel_filename
from packaging.version import Version

CHECKOUT = Path(__file__).resolve().parents[1]
PYPI = "https://pypi.org/pypi/terrine/json"
FIRST_WRITER = Version("0.1.0a1")


@dataclass(frozen=True)
class PublishedWheel:
    version: Version
    filename: str
    url: str
    sha256: str

    def __str__(self) -> str:
        return f"{self.version} from {PYPI}: {self.url} (SHA-256 {self.sha256})"


def candidate_version(path: Path) -> Version:
    if not path.is_absolute() or not path.is_file():
        raise ValueError(f"An absolute path to the built candidate wheel is required: {path}")
    name, version, _, _ = parse_wheel_filename(path.name)
    if name != "terrine":
        raise ValueError(f"Not a Terrine candidate wheel: {path}")
    return version


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def select_predecessor(
    candidate: Version, releases: Mapping[str, list[dict[str, Any]]]
) -> PublishedWheel:
    # Include alphas, betas and release candidates, but not dev snapshots (which
    # predate the saved-format promise). Tags aren't evidence of publication.
    preceding = [
        (version, files)
        for label, files in releases.items()
        if files
        and FIRST_WRITER <= (version := Version(label)) < candidate
        and not version.is_devrelease
    ]
    if not preceding:
        raise ValueError(f"No published Terrine writer precedes {candidate} in {PYPI}")
    version, files = max(preceding, key=lambda release: release[0])
    filename = f"terrine-{version}-py3-none-any.whl"
    wheels = [file for file in files if file["filename"] == filename]
    if len(wheels) != 1:
        raise ValueError(f"Published predecessor {version} has no unambiguous wheel in {PYPI}")
    wheel = wheels[0]
    digest = wheel["digests"]["sha256"]
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError(f"Published predecessor {version} has no valid SHA-256 in {PYPI}")
    return PublishedWheel(version, filename, wheel["url"], digest)


def published_predecessor(candidate: Version) -> PublishedWheel:
    with urlopen(PYPI, timeout=30) as response:
        return select_predecessor(candidate, json.load(response)["releases"])


def _git(*args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args], cwd=CHECKOUT, capture_output=True, check=False, timeout=30
    )


def _version_only(base: str, filename: str) -> bool:
    def settings(ref: str) -> dict[str, Any]:
        blob = _git("show", f"{ref}:{filename}")
        blob.check_returncode()
        data = tomllib.loads(blob.stdout.decode())
        if filename == "pyproject.toml":
            project = data["project"]
        else:
            (project,) = [
                entry
                for entry in data["package"]
                if entry["name"] == "terrine" and entry["source"] == {"editable": "."}
            ]
        del project["version"]
        return data

    try:
        return settings(base) == settings("HEAD")
    except (KeyError, TypeError, ValueError, subprocess.CalledProcessError):
        return False


def proof_route(predecessor: PublishedWheel) -> tuple[bool, str]:
    shallow = _git("rev-parse", "--is-shallow-repository")
    tag = f"refs/tags/v{predecessor.version}"
    ref = _git("rev-parse", "--verify", f"{tag}^{{commit}}")
    if shallow.returncode or shallow.stdout.strip() != b"false" or ref.returncode:
        return True, f"Cannot establish complete history from {tag}"
    base = ref.stdout.decode().strip()
    if _git("merge-base", "--is-ancestor", base, "HEAD").returncode:
        return True, f"{tag} ({base}) is not a proven ancestor of the candidate"
    changed = _git("diff", "--name-only", "-z", "--no-renames", base, "HEAD", "--")
    if changed.returncode:
        return True, f"Cannot classify changes since {tag} ({base})"
    for name in changed.stdout.split(b"\0")[:-1]:
        if name in {
            b"README.md",
            b"CHANGELOG.md",
            b"CONTRIBUTING.md",
            b"tests/artifacts/RELEASES",
        }:
            continue
        if name.startswith(b"docs/") and name.endswith(b".md"):
            continue
        if name.startswith(b"tests/artifacts/surface/") and name.endswith(b".txt"):
            continue
        if name in {b"pyproject.toml", b"uv.lock"} and _version_only(base, name.decode()):
            continue
        return True, f"Changes since {tag} ({base}) include code, formats or unknown inputs"
    return False, f"Only docs or release bookkeeping changed since {tag} ({base})"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args()
    version = candidate_version(args.wheel)
    predecessor = published_predecessor(version)
    required, reason = proof_route(predecessor)
    print(f"Candidate: {version}")
    print(f"Published predecessor: {predecessor}")
    print(f"Saved-format proof {'required' if required else 'not required'}: {reason}")
    with args.github_output.open("a", encoding="utf-8") as output:
        print(f"proof_required={str(required).lower()}", file=output)
        print(f"predecessor_sha256={predecessor.sha256}", file=output)


if __name__ == "__main__":
    main()
