import pytest

from scripts.require_green_ci import ci_succeeded

DEFERRED_GATE = "ci-ok (checked by the pull request run)"


def _run(
    *,
    name="CI",
    event="push",
    status="completed",
    conclusion="success",
    gate="ci-ok",
    gate_conclusion="success",
):
    return {
        "name": name,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "jobs": [
            {"name": "scope", "conclusion": "success"},
            {"name": gate, "conclusion": gate_conclusion},
        ],
    }


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_ci_succeeded_accepts_full_commit_run_whose_gate_succeeded(event):
    assert ci_succeeded([_run(event=event)])


@pytest.mark.parametrize(
    "run",
    [
        pytest.param(_run(gate=DEFERRED_GATE), id="deferred"),
        pytest.param(_run(conclusion="failure", gate_conclusion="failure"), id="failed"),
        pytest.param(
            _run(status="in_progress", conclusion=None, gate_conclusion=None), id="in-progress"
        ),
        pytest.param(_run(event="pull_request"), id="pull-request"),
        pytest.param(_run(name="Release"), id="other-workflow"),
    ],
)
def test_ci_succeeded_rejects_run_that_did_not_validate_the_commit(run):
    assert not ci_succeeded([run])


def test_ci_succeeded_rejects_no_runs():
    assert not ci_succeeded([])


def test_ci_succeeded_accepts_any_qualifying_run_for_commit():
    runs = [_run(conclusion="failure", gate_conclusion="failure"), _run()]

    assert ci_succeeded(runs)
