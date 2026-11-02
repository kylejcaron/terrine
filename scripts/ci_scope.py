"""Decide whether a branch push run of CI defers to a pull-request run of the same commit.

GitHub runs no pull-request workflow for a pull request with merge conflicts, so an
open pull request alone never defers: only an existing pull-request run does. Runs
with the system ``python3`` and the ``gh`` CLI; standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any

# Push and pull-request runs for one commit start at about the same time, so the
# lookup waits briefly for the pull-request run to appear.
_WAIT_SECONDS = 60
_RETRY_SECONDS = 10


def should_defer(runs: list[dict[str, Any]], *, sha: str, workflow: str = "CI") -> bool:
    """Return whether a pull-request run of the workflow for this commit replaces the push run."""
    return any(
        run.get("event") == "pull_request"
        and run.get("name") == workflow
        and run.get("head_sha") == sha
        and run.get("conclusion") != "cancelled"
        for run in runs
    )


def _pull_request_runs(repo: str, sha: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{repo}/actions/runs?head_sha={sha}&event=pull_request",
            "--jq",
            ".workflow_runs[] | {name,event,head_sha,conclusion} | @json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line]


def _defer(repo: str, sha: str) -> bool:
    deadline = time.monotonic() + _WAIT_SECONDS
    while True:
        try:
            runs = _pull_request_runs(repo, sha)
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            # Running every job is the safe answer when the lookup fails.
            print(
                f"::warning::unable to look up pull-request runs for {sha}: {exc}", file=sys.stderr
            )
            return False
        if should_defer(runs, sha=sha):
            return True
        if time.monotonic() + _RETRY_SECONDS > deadline:
            return False
        time.sleep(_RETRY_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="pushed commit SHA")
    parser.add_argument("--repo", required=True, help="owner/name of the repository")
    args = parser.parse_args()

    line = f"defer={'true' if _defer(args.repo, args.sha) else 'false'}\n"
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as stream:
            stream.write(line)
    print(line, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
