import sys

import pytest

from scripts import ci_scope

SHA = "a" * 40


def _run(*, name="CI", event="pull_request", head_sha=SHA, conclusion=None):
    return {"name": name, "event": event, "head_sha": head_sha, "conclusion": conclusion}


@pytest.mark.parametrize("conclusion", [None, "success", "failure"])
def test_should_defer_to_pull_request_run_of_same_commit(conclusion):
    assert ci_scope.should_defer([_run(conclusion=conclusion)], sha=SHA)


@pytest.mark.parametrize(
    "runs",
    [
        pytest.param([], id="no-runs"),
        pytest.param([_run(name="cla")], id="other-workflow"),
        pytest.param([_run(head_sha="b" * 40)], id="other-commit"),
        pytest.param([_run(conclusion="cancelled")], id="cancelled"),
        pytest.param([_run(event="push")], id="push-run"),
    ],
)
def test_should_not_defer_without_live_pull_request_run_of_same_commit(runs):
    assert not ci_scope.should_defer(runs, sha=SHA)


@pytest.mark.parametrize(
    ("ref", "default_branch", "defer"),
    [
        pytest.param("refs/heads/stable/trunk", "stable/trunk", False, id="default"),
        pytest.param("refs/heads/stable/Trunk", "stable/trunk", True, id="non-default"),
        pytest.param("refs/heads/stable/trunk", "", True, id="unknown-default"),
    ],
)
def test_branch_push_scope_only_looks_up_pr_runs_for_nondefault_or_unknown_branch(
    ref, default_branch, defer, monkeypatch, capsys
):
    repo = "owner/repo"
    lookups = []

    def pull_request_runs(repo, sha):
        lookups.append((repo, sha))
        return [_run(head_sha=SHA)]

    def sleep(seconds):
        raise AssertionError(f"unexpected polling delay: {seconds}s")

    monkeypatch.setattr(sys, "argv", ["ci_scope.py", "--sha", SHA, "--repo", repo])
    monkeypatch.setattr(ci_scope, "_pull_request_runs", pull_request_runs)
    monkeypatch.setattr(ci_scope.time, "sleep", sleep)
    monkeypatch.setenv("GITHUB_REF", ref)
    monkeypatch.setenv("DEFAULT_BRANCH", default_branch)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)

    assert ci_scope.main() == 0
    assert capsys.readouterr().out == ("defer=true\n" if defer else "defer=false\n")
    assert lookups == ([(repo, SHA)] if defer else [])
