from datetime import UTC, datetime

import pytest

from scripts.require_green_ci import (
    ReleaseRun,
    approval_matches,
    gate,
    required_jobs,
    tag_cutoff,
    validate_waiver_notes,
)

SHA = "a" * 40
OTHER_SHA = "b" * 40
TAG = "v0.1.0"
RC = "v1.0.0rc1"
CUTOFF = datetime(2026, 1, 8, tzinfo=UTC)
RELEASE = ReleaseRun("kylejcaron/terrine", 123, 2, TAG, SHA)
CLOUD = ("bigquery", "snowflake")


def _run(name, run_id, *, event="push", status="completed", head_sha=SHA):
    return {
        "id": run_id,
        "name": name,
        "path": f".github/workflows/{name}.yml",
        "head_sha": head_sha,
        "event": event,
        "status": status,
        "created_at": "2026-01-01T00:00:00Z",
    }


def _job(name, conclusion="success", *, completed_at="2026-01-07T00:00:00Z", head_sha=SHA):
    return {
        "name": name,
        "status": "completed",
        "conclusion": conclusion,
        "completed_at": completed_at,
        "head_sha": head_sha,
        "id": 1,
    }


def _check(*rows, tag=TAG):
    runs, jobs = zip(*rows, strict=True)
    by_run = dict(zip((r["id"] for r in runs), jobs, strict=True))
    return gate(runs, by_run.__getitem__, tag, sha=SHA, cutoff=CUTOFF)


def _ci(run_id=1, conclusion="success", **run_fields):
    return _run("CI", run_id, **run_fields), [_job("ci-ok", conclusion)]


def _warehouse(run_id=2, jobs=None, **run_fields):
    return (
        _run("warehouse-backends", run_id, **run_fields),
        [_job("postgres"), _job("bigquery"), _job("snowflake")] if jobs is None else jobs,
    )


def _scale(run_id=3, conclusion="success", **run_fields):
    return _run("scale", run_id, **run_fields), [_job("scale", conclusion)]


def test_required_jobs_preserve_tag_tiers_and_require_both_clouds():
    assert required_jobs(TAG) == {"CI": ("ci-ok",), "warehouse-backends": CLOUD}
    assert required_jobs(RC) == {
        "CI": ("ci-ok",),
        "warehouse-backends": ("postgres", *CLOUD),
        "scale": ("scale",),
    }


def test_0x_tag_refused_with_only_ci_green():
    result = _check(_ci())
    assert result.failure is None
    assert result.unverified_cloud == CLOUD


def test_rc_refused_when_postgres_green_and_bigquery_red():
    result = _check(
        _ci(),
        _warehouse(jobs=[_job("postgres"), _job("bigquery", "failure"), _job("snowflake")]),
        _scale(),
        tag=RC,
    )
    assert result.failure is None
    assert result.unverified_cloud == ("bigquery",)


@pytest.mark.parametrize("has_scale", [False, True], ids=["missing", "red"])
def test_rc_requires_successful_scale(has_scale):
    scale = [_scale(conclusion="failure")] if has_scale else []
    assert _check(_ci(), _warehouse(), *scale, tag=RC).failure == "scale"


@pytest.mark.parametrize(
    "run_fields",
    [{"event": "pull_request"}, {"status": "in_progress"}, {"head_sha": OTHER_SHA}],
    ids=["pull-request", "incomplete", "wrong-sha"],
)
def test_gate_refuses_inapplicable_ci_runs(run_fields):
    assert _check(_ci(**run_fields), _warehouse()).failure == "CI"


def test_gate_rejects_newer_full_ci_failure_even_when_older_run_succeeded():
    deferred = _run("CI", 4), [_job("ci-ok (checked by the pull request run)")]
    assert _check(_ci(), deferred, _ci(5, "failure"), _warehouse()).failure == "CI"


def test_gate_accepts_older_full_ci_success_when_newer_run_is_deferred():
    deferred = _run("CI", 4), [_job("ci-ok (checked by the pull request run)")]
    assert _check(_ci(), deferred, _warehouse()).passed


def test_rc_accepts_scheduled_postgres_and_scale_runs():
    assert _check(_ci(), _warehouse(event="schedule"), _scale(event="schedule"), tag=RC).passed


def test_cloud_only_dispatch_does_not_replace_postgres_check():
    prior_postgres = _warehouse(jobs=[_job("postgres")])
    cloud_only = _warehouse(
        4,
        [_job("postgres", "skipped"), _job("bigquery"), _job("snowflake")],
        event="workflow_dispatch",
    )
    assert _check(_ci(), prior_postgres, _scale(), cloud_only, tag=RC).passed


def test_newer_scheduled_scale_failure_blocks_release():
    result = _check(_ci(), _warehouse(), _scale(), _scale(4, "failure", event="schedule"), tag=RC)
    assert result.failure == "scale"


def test_0x_accepts_separate_cloud_jobs_and_ignores_postgres_only_dispatch():
    bigquery = _warehouse(jobs=[_job("bigquery")], status="in_progress", event="schedule")
    snowflake = _warehouse(3, [_job("snowflake")], event="workflow_dispatch")
    postgres = _warehouse(
        4,
        [_job("postgres", "failure"), _job("bigquery", "skipped"), _job("snowflake", "skipped")],
        event="workflow_dispatch",
    )
    assert _check(_ci(), bigquery, snowflake, postgres).passed


@pytest.mark.parametrize(
    ("snowflake", "unverified"),
    [
        (_job("snowflake", completed_at="2026-01-01T00:00:00Z"), ()),
        (_job("snowflake", completed_at="2025-12-31T23:59:59Z"), ("snowflake",)),
        (_job("snowflake", completed_at="2026-01-08T00:00:01Z"), ("snowflake",)),
        (_job("snowflake", "skipped"), ("snowflake",)),
        (_job("snowflake", head_sha=OTHER_SHA), ("snowflake",)),
    ],
    ids=["inclusive-seven-days", "stale", "future", "skip-only", "wrong-sha"],
)
def test_snowflake_requires_its_own_sha_matched_recent_execution(snowflake, unverified):
    result = _check(_ci(), _warehouse(jobs=[_job("bigquery"), snowflake]))
    assert result.unverified_cloud == unverified


def test_newer_cloud_failure_cannot_be_replaced_by_green_after_tag():
    later_attempt = _warehouse(
        4,
        [
            _job("snowflake", completed_at="2026-01-09T00:00:00Z") | {"id": 3},
            _job("snowflake", "failure") | {"id": 2},
        ],
    )
    earlier = _warehouse(
        jobs=[_job("bigquery"), _job("snowflake", completed_at="2026-01-06T00:00:00Z")],
    )
    assert _check(_ci(), earlier, later_attempt).unverified_cloud == ("snowflake",)


def test_tag_cutoff_is_original_matching_push_not_retry_or_dispatch():
    original = _run("release", 10) | {
        "head_branch": TAG,
        "created_at": "2026-01-08T00:00:00Z",
        "run_started_at": "2026-01-20T00:00:00Z",
        "run_attempt": 3,
    }
    dispatch = original | {
        "id": 12,
        "event": "workflow_dispatch",
        "created_at": "2026-01-16T00:00:00Z",
    }
    other = original | {"id": 9, "head_sha": OTHER_SHA}

    assert tag_cutoff([dispatch, other, original], TAG, SHA) == CUTOFF
    with pytest.raises(ValueError):
        tag_cutoff([dispatch, other], TAG, SHA)


NOTES = b"""# Terrine v0.1.0

## Snowflake waiver

- Date: 2026-01-08
- Reason: Latest exact boundary job failed.
- Operations: Snowflake exact window sums
- Evidence: https://github.com/kylejcaron/terrine/actions/runs/123/job/456
- Qualification: Snowflake exact window sums are not verified for this release.
"""


def test_candidate_notes_cover_the_unverified_backend_without_editing_note_bytes():
    assert validate_waiver_notes(NOTES, ("snowflake",), CUTOFF) == NOTES


@pytest.mark.parametrize(
    "notes",
    [
        NOTES.replace(b"## Snowflake waiver", b"## BigQuery waiver"),
        NOTES.replace(b"- Operations: Snowflake exact window sums\n", b""),
        NOTES.replace(b"2026-01-08", b"2026-01-09"),
        NOTES.replace(b"https://github.com", b"not-a-link"),
    ],
    ids=["wrong-backend", "missing-operations", "future-date", "missing-evidence-link"],
)
def test_candidate_notes_reject_incomplete_or_inapplicable_waiver(notes):
    with pytest.raises(ValueError):
        validate_waiver_notes(notes, ("snowflake",), CUTOFF)


# Literal SHA-256 checked with shasum, not the gate's comment builder.
APPROVAL = (
    f"terrine-release-notes-v1 repo=kylejcaron/terrine run=123 attempt=2 tag=v0.1.0 sha={SHA}"
    " sha256=4f9d4236ef075a53edc4468e5786ef36eff3b9db4ba5e1ef39bce9aabe41903c"
)


def _approval(*, state="approved", actor=44980552, environment="pypi", comment=APPROVAL):
    return {
        "state": state,
        "user": {"id": actor, "login": "kylejcaron"},
        "environments": [{"id": 25514366871, "name": environment}],
        "comment": comment,
    }


def test_notes_accept_only_exact_owner_approval():
    assert approval_matches([_approval()], RELEASE, NOTES)


@pytest.mark.parametrize(
    ("history", "notes"),
    [
        ([], NOTES),
        ([_approval(comment="")], NOTES),
        ([_approval(state="rejected")], NOTES),
        ([_approval(actor=42)], NOTES),
        ([_approval(environment="not-pypi")], NOTES),
        ([_approval(comment=APPROVAL.replace("attempt=2", "attempt=1"))], NOTES),
        ([_approval(comment=APPROVAL.replace(SHA, OTHER_SHA))], NOTES),
        ([_approval()], NOTES + b"\n"),
        ([{"state": "approved", "comment": APPROVAL, "user": None}], NOTES),
    ],
    ids=[
        "absent",
        "blank",
        "denied",
        "actor",
        "environment",
        "old-attempt",
        "sha",
        "mutated",
        "malformed",
    ],
)
def test_notes_refuse_unbound_or_unauthorized_review(history, notes):
    assert not approval_matches(history, RELEASE, notes)
