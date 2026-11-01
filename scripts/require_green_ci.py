"""Require a successful CI workflow run for a commit before release builds."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any


def ci_succeeded(runs: list[dict[str, Any]], *, workflow: str = "CI") -> bool:
    """Return whether any completed run of the named workflow succeeded."""
    return any(
        run.get("name") == workflow
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        for run in runs
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="commit SHA to check")
    args = parser.parse_args()
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        print("::error::GITHUB_REPOSITORY is required", file=sys.stderr)
        return 1

    try:
        result = subprocess.run(
            [
                "gh",
                "api",
                "--paginate",
                f"repos/{repository}/actions/runs?head_sha={args.sha}",
                "--jq",
                ".workflow_runs[] | {name,status,conclusion} | @json",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        runs = [json.loads(line) for line in result.stdout.splitlines() if line]
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"::error::unable to inspect CI runs for {args.sha}: {exc}", file=sys.stderr)
        return 1

    if not ci_succeeded(runs):
        print(f"::error::no completed successful CI run found for {args.sha}", file=sys.stderr)
        return 1
    print(f"Found a completed successful CI run for {args.sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
