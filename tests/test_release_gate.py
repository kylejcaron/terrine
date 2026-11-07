from scripts.require_green_ci import gate, required_jobs


def _run(
    workflow,
    run_id,
    *,
    event="push",
    status="completed",
    created_at="2026-01-01T00:00:00Z",
    conclusion="success",
):
    return {
        "id": run_id,
        "name": workflow,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "created_at": created_at,
    }


def test_required_jobs_for_0x_tag():
    assert required_jobs("v0.1.0") == {"CI": ("ci-ok",)}


def test_required_jobs_for_rc_tag():
    assert required_jobs("v1.0.0rc1") == {
        "CI": ("ci-ok",),
        "warehouse-backends": ("postgres",),
        "scale": ("scale",),
    }


def test_rc_admitted_when_postgres_green_and_bigquery_red():
    runs = [
        _run("CI", 1),
        _run("warehouse-backends", 2, conclusion="failure"),
        _run("scale", 3),
    ]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [
            {"name": "postgres", "conclusion": "success"},
            {"name": "bigquery", "conclusion": "failure"},
        ],
        3: [{"name": "scale", "conclusion": "success"}],
    }

    assert gate(runs, jobs.__getitem__, "v1.0.0rc1")


def test_rc_refused_when_scale_job_red():
    runs = [_run("CI", 1), _run("warehouse-backends", 2), _run("scale", 3)]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "postgres", "conclusion": "success"}],
        3: [{"name": "scale", "conclusion": "failure"}],
    }

    assert not gate(runs, jobs.__getitem__, "v1.0.0rc1")


def test_rc_refused_when_no_scale_run_on_sha():
    runs = [_run("CI", 1), _run("warehouse-backends", 2)]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "postgres", "conclusion": "success"}],
    }

    assert not gate(runs, jobs.__getitem__, "v1.0.0rc1")


def test_gate_rejects_pull_request_runs():
    runs = [_run("CI", 1, event="pull_request")]
    jobs = {1: [{"name": "ci-ok", "conclusion": "success"}]}

    assert not gate(runs, jobs.__getitem__, "v0.1.0")


def test_gate_ignores_incomplete_runs():
    runs = [_run("CI", 1, status="in_progress")]
    jobs = {1: [{"name": "ci-ok", "conclusion": "success"}]}

    assert not gate(runs, jobs.__getitem__, "v0.1.0")


def test_gate_uses_newest_completed_run_of_each_workflow():
    runs = [
        _run("CI", 1),
        _run("CI", 2, created_at="2026-01-02T00:00:00Z"),
    ]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "ci-ok", "conclusion": "failure"}],
    }

    assert not gate(runs, jobs.__getitem__, "v0.1.0")


def test_rc_accepts_scheduled_postgres_and_scale_runs():
    runs = [
        _run("CI", 1),
        _run("warehouse-backends", 2, event="schedule"),
        _run("scale", 3, event="schedule"),
    ]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "postgres", "conclusion": "success"}],
        3: [{"name": "scale", "conclusion": "success"}],
    }

    assert gate(runs, jobs.__getitem__, "v1.0.0rc1")


def test_advisory_warehouse_dispatch_does_not_replace_postgres_check():
    runs = [
        _run("CI", 1),
        _run("warehouse-backends", 2),
        _run("warehouse-backends", 3, created_at="2026-01-02T00:00:00Z"),
        _run("scale", 4),
    ]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "postgres", "conclusion": "success"}],
        3: [
            {"name": "postgres", "conclusion": "skipped"},
            {"name": "bigquery", "conclusion": "failure"},
        ],
        4: [{"name": "scale", "conclusion": "success"}],
    }

    assert gate(runs, jobs.__getitem__, "v1.0.0rc1")


def test_newer_scheduled_failure_blocks_release():
    runs = [
        _run("CI", 1),
        _run("warehouse-backends", 2),
        _run("scale", 3),
        _run(
            "scale",
            4,
            event="schedule",
            created_at="2026-01-02T00:00:00Z",
        ),
    ]
    jobs = {
        1: [{"name": "ci-ok", "conclusion": "success"}],
        2: [{"name": "postgres", "conclusion": "success"}],
        3: [{"name": "scale", "conclusion": "success"}],
        4: [{"name": "scale", "conclusion": "failure"}],
    }

    assert not gate(runs, jobs.__getitem__, "v1.0.0rc1")
