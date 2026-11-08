"""Require the release checks for a tag to pass on its commit."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable
from typing import Any

CI_JOB = "ci-ok"
DEFERRED_CI_JOB = "ci-ok (checked by the pull request run)"
RELEASE_JOBS = {
    "CI": (CI_JOB,),
    "warehouse-backends": ("postgres",),
    "scale": ("scale",),
}


def required_jobs(tag: str) -> dict[str, tuple[str, ...]]:
    """Return the workflow jobs required to release ``tag``."""
    if tag.startswith("v0.") or ".dev" in tag:
        return {"CI": (CI_JOB,)}
    if "rc" in tag or (
        tag.startswith("v") and tag[1:2].isdigit() and tag[1] != "0"
    ):
        return RELEASE_JOBS.copy()
    return {"CI": (CI_JOB,)}


def _completed_commit_run(run: dict[str, Any], workflow: str) -> bool:
    allowed_events = {"push", "workflow_dispatch"}
    if workflow in {"warehouse-backends", "scale"}:
        allowed_events.update({"schedule", "workflow_call"})
    return (
        run.get("name") == workflow
        and run.get("event") in allowed_events
        and run.get("status") == "completed"
    )


def _run_order(run: dict[str, Any]) -> tuple[str, int]:
    sequence = int(run.get("run_number", run.get("id", 0)))
    return str(run.get("created_at", "")), sequence


def gate(
    runs: list[dict[str, Any]],
    jobs_by_run: Callable[[int], list[dict[str, Any]]],
    tag: str,
) -> bool:
    """Check required jobs in the newest completed run that executed each job."""
    for workflow, job_names in required_jobs(tag).items():
        candidates = sorted(
            (run for run in runs if _completed_commit_run(run, workflow)),
            key=_run_order,
            reverse=True,
        )
        for run in candidates:
            jobs = jobs_by_run(int(run["id"]))
            by_name = {job.get("name"): job.get("conclusion") for job in jobs}
            if workflow == "CI" and DEFERRED_CI_JOB in by_name:
                continue
            if workflow == "warehouse-backends" and all(
                by_name.get(job_name) in {None, "skipped"} for job_name in job_names
            ):
                continue
            if not all(by_name.get(job_name) == "success" for job_name in job_names):
                return False
            break
        else:
            return False
    return True


def _api_lines(endpoint: str, jq: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["gh", "api", "--paginate", endpoint, "--jq", jq],
        check=True,
        capture_output=True,
        text=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="commit SHA to check")
    parser.add_argument("--tag", required=True, help="release tag to check")
    args = parser.parse_args()
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        print("::error::GITHUB_REPOSITORY is required", file=sys.stderr)
        return 1

    try:
        runs = _api_lines(
            f"repos/{repository}/actions/runs?head_sha={args.sha}",
            ".workflow_runs[] | {id,name,event,status,conclusion,created_at,run_number} | @json",
        )
        jobs_cache: dict[int, list[dict[str, Any]]] = {}

        def jobs_by_run(run_id: int) -> list[dict[str, Any]]:
            if run_id not in jobs_cache:
                jobs_cache[run_id] = _api_lines(
                    f"repos/{repository}/actions/runs/{run_id}/jobs",
                    ".jobs[] | {name,conclusion} | @json",
                )
            return jobs_cache[run_id]

        passed = gate(runs, jobs_by_run, args.tag)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"::error::unable to inspect release checks for {args.sha}: {exc}", file=sys.stderr)
        return 1

    if not passed:
        print(
            f"::error::required release checks were not successful for {args.sha} ({args.tag})",
            file=sys.stderr,
        )
        return 1
    print(f"Found all required release checks for {args.sha} ({args.tag})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


