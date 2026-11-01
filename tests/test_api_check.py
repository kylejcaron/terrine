import os
import subprocess
from pathlib import Path

import griffe
import pytest

from scripts.api_check import main, stable_breakages

_RELEASE = {
    "__init__.py": (
        "from terrine.model import Model\n"
        "from terrine.spec import agg, approx_distinct\n"
        '__all__ = ["Model", "agg", "approx_distinct"]\n'
    ),
    "spec/__init__.py": (
        "from terrine.spec.expressions import agg, approx_distinct\n"
        '__all__ = ["agg", "approx_distinct"]\n'
    ),
    "spec/expressions.py": (
        "def agg(metric, window):\n    pass\n\n\n"
        "def approx_distinct(metric, window):\n    pass\n\n\n"
        "def unexported(metric, window):\n    pass\n"
    ),
    "model.py": (
        '__all__ = ["Model"]\n\n\n'
        "class Model:\n"
        "    def from_tables(self, units, events):\n        pass\n\n"
        "    @classmethod\n"
        "    def from_increment(cls, path, con):\n        pass\n"
    ),
}


@pytest.fixture(autouse=True)
def _outside_repository(tmp_path, monkeypatch):
    # Griffe resolves a package name against the working directory first; load
    # the temporary `terrine` packages, never this checkout's.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def _changed(path: str, old: str, new: str) -> dict[str, str]:
    assert old in _RELEASE[path]
    return {**_RELEASE, path: _RELEASE[path].replace(old, new)}


@pytest.mark.parametrize(
    ("path", "old", "new", "reported"),
    [
        pytest.param(
            "spec/expressions.py",
            "agg(metric, window)",
            "agg(metric)",
            ["terrine.agg"],
            id="re-exported-internal-function",
        ),
        pytest.param(
            "spec/expressions.py",
            "unexported(metric, window)",
            "unexported(metric)",
            [],
            id="unexported-function",
        ),
        pytest.param(
            "spec/expressions.py",
            "approx_distinct(metric, window)",
            "approx_distinct(metric)",
            [],
            id="provisional-function-through-aliases",
        ),
        pytest.param(
            "model.py",
            "from_increment(cls, path, con)",
            "from_increment(cls, path)",
            [],
            id="provisional-method-of-stable-class",
        ),
        pytest.param(
            "model.py",
            "from_tables(self, units, events)",
            "from_tables(self, units)",
            ["terrine.Model"],
            id="stable-method",
        ),
        pytest.param(
            "spec/__init__.py",
            '__all__ = ["agg", ',
            "__all__ = [",
            ["terrine.spec.agg"],
            id="name-removed-from-facade",
        ),
    ],
)
def test_stable_breakages_report_only_the_stable_exported_surface(path, old, new, reported):
    with (
        griffe.temporary_visited_package("terrine", _RELEASE) as release,
        griffe.temporary_visited_package("terrine", _changed(path, old, new)) as current,
    ):
        breakages = stable_breakages(release, current)

    assert [line.split(":", 1)[0] for line in breakages] == reported


def test_formerly_shared_exports_are_compared_with_their_own_current_targets():
    current = {
        **_RELEASE,
        "__init__.py": _RELEASE["__init__.py"].replace(
            "from terrine.spec import agg, approx_distinct",
            "from terrine.compat import agg\nfrom terrine.spec import approx_distinct",
        ),
        "compat.py": "def agg(metric, window):\n    pass\n",
        "spec/expressions.py": _RELEASE["spec/expressions.py"].replace(
            "agg(metric, window)", "agg(metric)"
        ),
    }
    with (
        griffe.temporary_visited_package("terrine", _RELEASE) as release,
        griffe.temporary_visited_package("terrine", current) as changed,
    ):
        breakages = stable_breakages(release, changed)

    assert [line.split(":", 1)[0] for line in breakages] == ["terrine.spec.agg"]


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=terrine", "-c", "user.email=terrine@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


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
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    monkeypatch.chdir(repo)
    return repo


def test_check_passes_without_a_1x_release_tag(repo, capsys):
    _write_package(repo, _RELEASE)
    _release(repo, "v0.9.0")

    assert main([]) == 0
    assert "no 1.x release tag" in capsys.readouterr().out


def test_check_requires_every_current_stable_export_to_resolve(repo, capsys):
    _write_package(
        repo, _changed("spec/__init__.py", '__all__ = ["agg", ', '__all__ = ["lazy", "agg", ')
    )

    assert main([]) == 1
    assert "terrine.spec.lazy" in capsys.readouterr().out


def test_check_compares_with_the_newest_1x_release_tag(repo, capsys):
    _write_package(
        repo, _changed("spec/expressions.py", "agg(metric, window)", "agg(metric, window, extra)")
    )
    _release(repo, "v1.0.0rc1")
    _write_package(repo, _RELEASE)
    _release(repo, "v1.0.0")

    assert main([]) == 0
    assert main(["--against", "v1.0.0rc1"]) == 1
    assert "terrine.agg" in capsys.readouterr().out


@pytest.mark.parametrize("history", ["release-without-package", "shallow-clone"])
def test_check_fails_when_release_history_is_unavailable(repo, history, monkeypatch, capsys):
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
