"""Check release evidence and verify approval of exact waiver notes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

CI_JOB = "ci-ok"
DEFERRED_CI_JOB = "ci-ok (checked by the pull request run)"
WAREHOUSE = "warehouse-backends"
WAREHOUSE_PATH = f".github/workflows/{WAREHOUSE}.yml"
RELEASE_PATH = ".github/workflows/release.yml"
CLOUD_JOBS = ("bigquery", "snowflake")
RELEASE_OWNER_ID = 44980552  # Required reviewer of the pypi environment.
WAREHOUSE_EVENTS = {"push", "workflow_dispatch", "schedule", "workflow_call"}
WAIVER_HEADING = re.compile(r"^## (BigQuery|Snowflake) waiver$", re.MULTILINE)
WAIVER_FIELDS = ("Date", "Reason", "Operations", "Evidence", "Qualification")

Run = dict[str, Any]
JobLookup = Callable[[int], list[dict[str, Any]]]


@dataclass(frozen=True)
class ReleaseChecks:
    failure: str | None = None
    unverified_cloud: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.failure is None and not self.unverified_cloud


@dataclass(frozen=True)
class ReleaseRun:
    repo: str
    run_id: int
    attempt: int
    tag: str
    sha: str

    def approval_comment(self, notes: bytes) -> str:
        digest = hashlib.sha256(notes).hexdigest()
        return (
            f"terrine-release-notes-v1 repo={self.repo} run={self.run_id} attempt={self.attempt} "
            f"tag={self.tag} sha={self.sha} sha256={digest}"
        )


def required_jobs(tag: str) -> dict[str, tuple[str, ...]]:
    """Return the workflow jobs required to release ``tag``."""
    jobs = {"CI": (CI_JOB,), WAREHOUSE: CLOUD_JOBS}
    full = "rc" in tag or (tag.startswith("v") and tag[1:2].isdigit() and tag[1] != "0")
    if full and not (tag.startswith("v0.") or ".dev" in tag):
        jobs[WAREHOUSE] = ("postgres", *CLOUD_JOBS)
        jobs["scale"] = ("scale",)
    return jobs


def _completed_commit_run(run: dict[str, Any], workflow: str) -> bool:
    events = WAREHOUSE_EVENTS if workflow in {WAREHOUSE, "scale"} else {"push", "workflow_dispatch"}
    return (
        run.get("name") == workflow
        and run.get("event") in events
        and run.get("status") == "completed"
    )


def _run_order(run: dict[str, Any]) -> tuple[str, int]:
    sequence = int(run.get("run_number", run.get("id", 0)))
    return str(run.get("created_at", "")), sequence


def _utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("missing UTC timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("expected a UTC timestamp")
    return parsed.astimezone(UTC)


def tag_cutoff(runs: Sequence[Run], tag: str, sha: str) -> datetime:
    """Read the GitHub-recorded creation instant of the original tag push run."""
    matching = [
        _utc(run.get("created_at"))
        for run in runs
        if run.get("path") == RELEASE_PATH
        and run.get("event") == "push"
        and run.get("head_branch") == tag
        and run.get("head_sha") == sha
    ]
    if not matching:
        raise ValueError("no original release workflow tag push found for this tag and SHA")
    return min(matching)


def _unverified_cloud(
    runs: Sequence[Run],
    jobs_by_run: JobLookup,
    sha: str,
    cutoff: datetime,
) -> tuple[str, ...]:
    latest: dict[str, tuple[tuple[datetime, int], bool]] = {}
    invalid: set[str] = set()
    for run in runs:
        if (
            run.get("path") != WAREHOUSE_PATH
            or run.get("event") not in WAREHOUSE_EVENTS
            or _utc(run.get("created_at")) > cutoff
        ):
            continue
        for job in jobs_by_run(int(run["id"])):
            name = job.get("name")
            conclusion = job.get("conclusion")
            if (
                name not in CLOUD_JOBS
                or job.get("head_sha") != sha
                or job.get("status") != "completed"
                or conclusion == "skipped"
            ):
                continue
            try:
                finished = _utc(job.get("completed_at"))
            except ValueError:
                invalid.add(name)
                continue
            order = (finished, int(job["id"]))
            if finished <= cutoff and (name not in latest or order > latest[name][0]):
                latest[name] = order, conclusion == "success"
    earliest = cutoff - timedelta(days=7)
    return tuple(
        name
        for name in CLOUD_JOBS
        if name in invalid
        or name not in latest
        or latest[name][0][0] < earliest
        or not latest[name][1]
    )


def gate(
    runs: Sequence[Run],
    jobs_by_run: JobLookup,
    tag: str,
    *,
    sha: str,
    cutoff: datetime,
    warehouse_jobs_by_run: JobLookup | None = None,
) -> ReleaseChecks:
    """Separate non-waivable checks from cloud backends lacking release evidence."""
    runs = [run for run in runs if run.get("head_sha") == sha]
    for workflow, job_names in required_jobs(tag).items():
        job_names = tuple(name for name in job_names if name not in CLOUD_JOBS)
        if not job_names:
            continue
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
            if workflow == WAREHOUSE and all(
                by_name.get(job_name) in {None, "skipped"} for job_name in job_names
            ):
                continue
            if not all(by_name.get(job_name) == "success" for job_name in job_names):
                return ReleaseChecks(failure=workflow)
            break
        else:
            return ReleaseChecks(failure=workflow)
    return ReleaseChecks(
        unverified_cloud=_unverified_cloud(runs, warehouse_jobs_by_run or jobs_by_run, sha, cutoff),
    )


def validate_waiver_notes(notes: bytes, backends: tuple[str, ...], cutoff: datetime) -> bytes:
    """Validate the required disclosures without normalizing the published bytes."""
    text = notes.decode("utf-8")
    headings = list(WAIVER_HEADING.finditer(text))
    if sorted(h[1].lower() for h in headings) != sorted(backends):
        raise ValueError(
            "candidate notes must have one waiver section per unverified cloud backend"
        )
    for heading in headings:
        lines = text[heading.end() :].lstrip("\n").splitlines()[: len(WAIVER_FIELDS)]
        values = []
        for field, line in zip(WAIVER_FIELDS, lines, strict=True):
            prefix = f"- {field}: "
            if not line.startswith(prefix) or not line[len(prefix) :].strip():
                raise ValueError(f"{heading[1]} waiver requires {field}")
            values.append(line[len(prefix) :].strip())
        waiver_day, _, _, evidence, _ = values
        if (
            not re.fullmatch(r"\d{4}-\d{2}-\d{2}", waiver_day)
            or date.fromisoformat(waiver_day) > cutoff.date()
        ):
            raise ValueError(f"{heading[1]} waiver requires a date no later than the tag")
        link = urlsplit(evidence)
        if link.scheme != "https" or not link.hostname or any(c.isspace() for c in evidence):
            raise ValueError(f"{heading[1]} waiver requires an HTTPS evidence link")
    return notes


def approval_matches(history: Sequence[dict[str, Any]], run: ReleaseRun, notes: bytes) -> bool:
    """Require an authenticated pypi review binding the exact bytes to this attempt."""
    comment = run.approval_comment(notes)
    states = []
    for review in history:
        if not isinstance(review, dict) or review.get("comment") != comment:
            continue
        user = review.get("user")
        environments = review.get("environments")
        if (
            not isinstance(user, dict)
            or type(user.get("id")) is not int
            or user.get("id") != RELEASE_OWNER_ID
            or not isinstance(environments, list)
            or not any(isinstance(env, dict) and env.get("name") == "pypi" for env in environments)
        ):
            continue
        states.append(review.get("state"))
    return bool(states) and all(state == "approved" for state in states)


def _api_lines(endpoint: str, jq: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["gh", "api", "--paginate", endpoint, "--jq", jq],
        check=True,
        capture_output=True,
        text=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line]


def _release_run(repo: str, tag: str, sha: str) -> ReleaseRun:
    if not re.fullmatch(r"v[^/\s]+", tag) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("expected a version tag and full commit SHA")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    [run] = _api_lines(f"repos/{repo}/actions/runs/{run_id}", ". | @json")
    if (
        run.get("id") != run_id
        or run.get("run_attempt") != attempt
        or run.get("path") != RELEASE_PATH
        or run.get("head_branch") != tag
        or run.get("head_sha") != sha
    ):
        raise ValueError("this GitHub release workflow run does not match the tag and SHA")
    return ReleaseRun(repo, run_id, attempt, tag, sha)


def _prepare_notes(run: ReleaseRun, target: Path) -> None:
    api = f"repos/{run.repo}/actions"
    query = urlencode({"event": "push", "branch": run.tag, "head_sha": run.sha})
    cutoff = tag_cutoff(
        _api_lines(f"{api}/workflows/release.yml/runs?{query}", ".workflow_runs[] | @json"),
        run.tag,
        run.sha,
    )
    runs = _api_lines(f"{api}/runs?head_sha={run.sha}", ".workflow_runs[] | @json")

    def jobs(run_id: int) -> list[dict[str, Any]]:
        return _api_lines(f"{api}/runs/{run_id}/jobs", ".jobs[] | @json")

    def all_jobs(run_id: int) -> list[dict[str, Any]]:
        return _api_lines(f"{api}/runs/{run_id}/jobs?filter=all", ".jobs[] | @json")

    checks = gate(runs, jobs, run.tag, sha=run.sha, cutoff=cutoff, warehouse_jobs_by_run=all_jobs)
    if checks.failure:
        raise ValueError(f"required {checks.failure} checks were not successful on {run.sha}")
    print(f"Cloud evidence cutoff: {cutoff.isoformat()} for {run.tag} on {run.sha}")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        if checks.passed:
            print("All required checks passed; no cloud waiver needed")
            print("cloud_evidence=green", file=output)
            return
        print(f"Owner approval required for: {', '.join(checks.unverified_cloud)}")
        _stage_waiver(run, target, checks.unverified_cloud, cutoff)
        print("cloud_evidence=waiver", f"notes_attempt={run.attempt}", sep="\n", file=output)


def _stage_waiver(
    run: ReleaseRun, target: Path, backends: tuple[str, ...], cutoff: datetime
) -> None:
    source = Path(".github/release-notes") / f"{run.tag}.md"
    notes = validate_waiver_notes(source.read_bytes(), backends, cutoff)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(notes)
    text = notes.decode("utf-8")
    fence = "`" * max(3, max((len(m[0]) + 1 for m in re.finditer(r"`+", text)), default=0))
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as summary:
        print("## Exact candidate release notes (cloud waiver)", file=summary)
        print(fence, text, sep="\n", end="" if text.endswith("\n") else "\n", file=summary)
        print(fence, file=summary)
        print(
            "Approve `pypi` with this exact comment:",
            f"\n```text\n{run.approval_comment(notes)}\n```",
            sep="\n",
            file=summary,
        )


def _verify_notes(run: ReleaseRun, notes: Path) -> None:
    [history] = _api_lines(f"repos/{run.repo}/actions/runs/{run.run_id}/approvals", ". | @json")
    if not isinstance(history, list) or not approval_matches(history, run, notes.read_bytes()):
        raise ValueError("pypi approval by the release owner does not bind these exact notes")
    print("Release owner approved exact waiver notes; cloud evidence remains unverified")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="commit SHA to check")
    parser.add_argument("--tag", required=True, help="release tag to check")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--prepare-notes", type=Path)
    mode.add_argument("--verify-notes", type=Path)
    args = parser.parse_args()
    try:
        run = _release_run(os.environ["GITHUB_REPOSITORY"], args.tag, args.sha)
        if args.prepare_notes is not None:
            _prepare_notes(run, args.prepare_notes)
        else:
            _verify_notes(run, args.verify_notes)
    except (KeyError, OSError, subprocess.CalledProcessError, TypeError, ValueError) as exc:
        print(f"::error::unable to clear release checks for {args.sha}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
