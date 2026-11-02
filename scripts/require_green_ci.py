"""Require a full successful CI workflow run for a commit before release builds.

A push run that deferred to a pull-request run succeeds without running the jobs
and names its gate differently, so only a run whose job named exactly ``ci-ok``
succeeded qualifies.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any

GATE_JOB = "ci-ok"


def _completed_commit_run(run: dict[str, Any], workflow: str) -> bool:
    return (
        run.get("name") == workflow
        and run.get("event") in {"push", "workflow_dispatch"}
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
    )


def ci_succeeded(runs: list[dict[str, Any]], *, workflow: str = "CI") -> bool:
    """Return whether a completed successful commit-level run of the workflow passed its gate.

    Each run carries its ``jobs``; pull-request runs never qualify.
    """
    return any(
        _completed_commit_run(run, workflow)
        and any(
            job.get("name") == GATE_JOB and job.get("conclusion") == "success"
            for job in run.get("jobs", ())
        )
        for run in runs
    )


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
    args = parser.parse_args()
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        print("::error::GITHUB_REPOSITORY is required", file=sys.stderr)
        return 1

    try:
        runs = _api_lines(
            f"repos/{repository}/actions/runs?head_sha={args.sha}",
            ".workflow_runs[] | {id,name,event,status,conclusion} | @json",
        )
        for run in runs:
            if _completed_commit_run(run, "CI"):
                run["jobs"] = _api_lines(
                    f"repos/{repository}/actions/runs/{run['id']}/jobs",
                    ".jobs[] | {name,conclusion} | @json",
                )
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"::error::unable to inspect CI runs for {args.sha}: {exc}", file=sys.stderr)
        return 1

    if not ci_succeeded(runs):
        print(
            f"::error::no completed CI run with a successful {GATE_JOB} job found for {args.sha}",
            file=sys.stderr,
        )
        return 1
    print(f"Found a completed CI run with a successful {GATE_JOB} job for {args.sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
