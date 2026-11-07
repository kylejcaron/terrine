import os
import subprocess
from pathlib import Path

import pytest

from scripts.api_check import main
from tests.test_api_check import _RELEASE, _changed


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=terrine", "-c", "user.email=terrine@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def _write_version(repo: Path, version: str) -> None:
    (repo / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n')


def _write_package(repo: Path, modules: dict[str, str]) -> None:
    for name, text in modules.items():
        path = repo / "terrine" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

def _release(repo: Path, tag: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--allow-empty", "-m", tag)
    _git(repo, "tag", tag)


@pytest.fixture
def repo(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write_version(repo, "0.1.0")
    monkeypatch.chdir(repo)
    return repo


def test_check_passes_without_a_current_line_release_tag(repo, capsys):
    _write_package(repo, _RELEASE)
    _release(repo, "v1.0.0")

    assert main([]) == 0
    assert "no release tag" in capsys.readouterr().out


def test_check_ignores_0x_development_tag(repo, capsys):
    _write_package(repo, _RELEASE)
    _release(repo, "v0.1.0.dev2")

    assert main([]) == 0
    assert "no release tag" in capsys.readouterr().out

def test_check_requires_every_current_stable_export_to_resolve(repo, capsys):
    _write_package(
        repo, _changed("spec/__init__.py", '__all__ = ["agg", ', '__all__ = ["lazy", "agg", ')
    )

    assert main([]) == 1
    assert "terrine.spec.lazy" in capsys.readouterr().out

def test_check_compares_with_the_newest_1x_release_tag(repo, capsys):
    _write_version(repo, "1.0.0")
    _write_package(
        repo, _changed("spec/expressions.py", "agg(metric, window)", "agg(metric, window, extra)")
    )
    _release(repo, "v1.0.0rc1")
    _write_package(repo, _RELEASE)
    _release(repo, "v1.0.0")

    assert main([]) == 0
    assert main(["--against", "v1.0.0rc1"]) == 1
    assert "terrine.agg" in capsys.readouterr().out



def test_check_allows_breaking_change_in_new_0x_minor(repo, capsys):
    _write_package(repo, _RELEASE)
    _write_version(repo, "0.1.0")
    _release(repo, "v0.1.0")
    _write_package(repo, _changed("spec/expressions.py", "agg(metric, window)", "agg(metric)"))
    _write_version(repo, "0.2.0")

    assert main([]) == 0
    output = capsys.readouterr().out
    assert "stable API breakages against v0.1.0" in output
    assert "breaking changes are permitted across 0.x minor releases" in output


def test_check_enforces_1x_release_candidate_baseline(repo, capsys):
    _write_package(repo, _RELEASE)
    _write_version(repo, "1.0.0rc1")
    _release(repo, "v1.0.0rc1")
    _write_package(repo, _changed("spec/expressions.py", "agg(metric, window)", "agg(metric)"))
    _write_version(repo, "1.0.0")

    assert main([]) == 1
    assert "stable API breakages against v1.0.0rc1" in capsys.readouterr().out

def test_check_rejects_breaking_change_in_0x_patch(repo, capsys):
    _write_package(repo, _RELEASE)
    _write_version(repo, "0.1.0")
    _release(repo, "v0.1.0")
    _release(repo, "v0.2.0")
    _write_package(repo, _changed("spec/expressions.py", "agg(metric, window)", "agg(metric)"))
    _write_version(repo, "0.1.1")

    assert main([]) == 1
    output = capsys.readouterr().out
    assert "terrine.agg" in output
    assert "informational comparison against v0.2.0" in output

@pytest.mark.parametrize("history", ["release-without-package", "shallow-clone"])
def test_check_fails_when_release_history_is_unavailable(repo, history, monkeypatch, capsys):
    _write_version(repo, "1.0.0")
    if history == "release-without-package":
        _release(repo, "v1.0.0")
        _write_package(repo, _RELEASE)
    else:
        _write_package(repo, _RELEASE)
        _release(repo, "v1.0.0")
        _git(repo.parent, "clone", "-q", "--depth", "1", repo.as_uri(), "clone")
        monkeypatch.chdir(repo.parent / "clone")

    assert main([]) == 2
    assert "comparison history unavailable" in capsys.readouterr().out
