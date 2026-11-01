from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any

_PYTEST_CPU_MARKER = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z )?"
    r"terrine-pytest-cpu ([0-9]+(?:\.[0-9]+)?) ([0-9]+(?:\.[0-9]+)?)$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class CostSummary:
    job_seconds: float
    pytest_cpu_seconds: float
    jobs: int


def summarize(jobs: list[dict[str, Any]], logs: Mapping[int, str]) -> CostSummary:
    job_seconds = 0.0
    pytest_cpu_seconds = 0.0
    counted_jobs = 0

    for job in jobs:
        if job.get("conclusion") == "skipped":
            continue
        started_at = job.get("started_at")
        completed_at = job.get("completed_at")
        if not started_at or not completed_at:
            continue
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
        job_seconds += (completed - started).total_seconds()
        counted_jobs += 1

        log = logs.get(job["id"], "")
        for user_seconds, system_seconds in _PYTEST_CPU_MARKER.findall(log):
            pytest_cpu_seconds += float(user_seconds) + float(system_seconds)

    return CostSummary(
        job_seconds=job_seconds,
        pytest_cpu_seconds=pytest_cpu_seconds,
        jobs=counted_jobs,
    )


def _api_bytes(endpoint: str) -> bytes:
    return subprocess.run(["gh", "api", endpoint], check=True, capture_output=True).stdout


def _job_logs(content: bytes) -> str:
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            return "\n".join(
                archive.read(name).decode("utf-8", errors="replace") for name in archive.namelist()
            )
    except zipfile.BadZipFile:
        return content.decode("utf-8", errors="replace")


def _run(run_id: str, repo: str) -> CostSummary:
    endpoint = f"repos/{repo}/actions/runs/{run_id}/jobs"
    result = subprocess.run(
        ["gh", "api", "--paginate", "--jq", ".jobs[]", endpoint],
        check=True,
        capture_output=True,
        text=True,
    )
    jobs = [json.loads(line) for line in result.stdout.splitlines() if line]
    logs = {
        job["id"]: _job_logs(_api_bytes(f"repos/{repo}/actions/jobs/{job['id']}/logs"))
        for job in jobs
        if job.get("conclusion") != "skipped"
    }
    return summarize(jobs, logs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize CI job and pytest CPU time.")
    parser.add_argument("run_id")
    parser.add_argument("--repo", default="kylejcaron/terrine")
    args = parser.parse_args()

    summary = _run(args.run_id, args.repo)
    print(f"Job elapsed time: {summary.job_seconds:.1f} seconds across {summary.jobs} jobs")
    print(f"Pytest CPU time: {summary.pytest_cpu_seconds:.1f} seconds")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr or error, file=sys.stderr)
        raise SystemExit(error.returncode) from error
