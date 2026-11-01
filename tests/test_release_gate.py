from scripts.require_green_ci import ci_succeeded


def test_ci_succeeded_for_completed_successful_ci_run():
    assert ci_succeeded(
        [{"name": "CI", "event": "push", "status": "completed", "conclusion": "success"}]
    )


def test_ci_succeeded_rejects_failure():
    assert not ci_succeeded(
        [{"name": "CI", "event": "push", "status": "completed", "conclusion": "failure"}]
    )


def test_ci_succeeded_rejects_in_progress_run():
    assert not ci_succeeded(
        [{"name": "CI", "event": "push", "status": "in_progress", "conclusion": None}]
    )


def test_ci_succeeded_ignores_other_workflows():
    assert not ci_succeeded([{"name": "Release", "status": "completed", "conclusion": "success"}])


def test_ci_succeeded_rejects_no_runs():
    assert not ci_succeeded([])


def test_ci_succeeded_accepts_any_successful_ci_run_for_commit():
    runs = [
        {"name": "CI", "event": "push", "status": "completed", "conclusion": "failure"},
        {"name": "CI", "event": "push", "status": "completed", "conclusion": "success"},
    ]

    assert ci_succeeded(runs)


def test_ci_succeeded_rejects_successful_pull_request_run():
    runs = [
        {
            "name": "CI",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "success",
        }
    ]

    assert not ci_succeeded(runs)


def test_ci_succeeded_accepts_workflow_dispatch_for_commit():
    runs = [
        {
            "name": "CI",
            "event": "workflow_dispatch",
            "status": "completed",
            "conclusion": "success",
        }
    ]

    assert ci_succeeded(runs)
