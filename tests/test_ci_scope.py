import pytest

from scripts.ci_scope import should_defer

SHA = "a" * 40


def _run(*, name="CI", event="pull_request", head_sha=SHA, conclusion=None):
    return {"name": name, "event": event, "head_sha": head_sha, "conclusion": conclusion}


@pytest.mark.parametrize("conclusion", [None, "success", "failure"])
def test_should_defer_to_pull_request_run_of_same_commit(conclusion):
    assert should_defer([_run(conclusion=conclusion)], sha=SHA)


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
    assert not should_defer(runs, sha=SHA)
