"""Run one module's mutations with a fresh interpreter for every test invocation."""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import random
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from mutmut.configuration import config


def collect_stats() -> int:
    from mutmut.mutation.trampoline import set_mutant_under_test
    from mutmut.runners.harness import PytestRunner
    from mutmut.stats import save_stats
    from mutmut.utils.file_utils import setup_source_paths

    config().pytest_add_cli_args += [
        "-o",
        "junit_family=xunit1",
        f"--junitxml={Path('mutants/baseline.xml').resolve()}",
    ]
    setup_source_paths()
    set_mutant_under_test("stats")
    result = PytestRunner().run_stats(tests=[])
    if result == 0:
        save_stats()
    return result


def read_report(path: Path) -> tuple[set[str], set[str]]:
    """Read started test identities and named failures from pytest's xunit1 output."""
    seen, failed = set(), set()
    for case in ET.parse(path).iter("testcase"):
        skipped = case.find("skipped")
        # Only collection skips lack pytest's runtime skip or xfail type.
        if skipped is not None and skipped.get("type") is None:
            continue
        if not case.get("file") or not case.get("name"):
            raise ValueError("A test report has no attributable test identity")
        identity = f"{case.get('file')}::{case.get('name')}"
        seen.add(identity)
        if case.find("failure") is not None or case.find("error") is not None:
            failed.add(identity)
    return seen, failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", required=True)
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--sample", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=20261025)
    parser.add_argument("--collect-stats", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("tests", nargs="+")
    args = parser.parse_args()
    module = Path(args.module)
    if not module.is_file() or module.suffix != ".py" or not module.is_relative_to("terrine"):
        parser.error("--module must name a Python source file under terrine/")
    if args.cores < 1 or not 0 < args.sample <= 1:
        parser.error("cores must be positive and sample must be in (0, 1]")
    settings = config()
    settings.only_mutate = [str(module)]
    settings.pytest_add_cli_args_test_selection = args.tests
    if args.collect_stats:
        return collect_stats()

    cores = min(args.cores, int(os.environ["AGENT_SLOTS_HELD"]))
    started = time.monotonic()
    workspace = Path("mutants")
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir()
    # Generation forks before any tests or native numerical libraries are loaded.
    multiprocessing.set_start_method("fork")
    from mutmut.__main__ import cli, create_mutants
    from mutmut.mutation.trampoline import set_mutant_under_test
    from mutmut.utils.file_utils import copy_also_copy_files, copy_src_dir

    set_mutant_under_test("mutant_generation")
    copy_src_dir()
    copy_also_copy_files()
    # Assertion-rewrite caches retain the original test files' source locations.
    for cache in workspace.rglob("__pycache__"):
        shutil.rmtree(cache)
    create_mutants(cores)
    metadata_path = workspace / f"{module}.meta"
    metadata = json.loads(metadata_path.read_text())
    population = sorted(metadata["exit_code_by_key"])
    selected = sorted(
        random.Random(args.seed).sample(population, math.ceil(len(population) * args.sample))
    )
    campaign = {
        "module": str(module),
        "tests": args.tests,
        "cores": cores,
        "population_size": len(population),
        "sample_size": len(selected),
        "seed": args.seed,
        "sample": selected,
        "results": {},
    }
    campaign_path = workspace / "campaign.json"
    campaign_path.write_text(json.dumps(campaign, indent=2))
    stats_started = time.monotonic()
    stats_result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--module",
            str(module),
            "--collect-stats",
            *args.tests,
        ],
        env={**os.environ, "MUTANT_UNDER_TEST": "stats"},
    )
    if stats_result.returncode:
        return stats_result.returncode
    baseline_seconds = time.monotonic() - stats_started
    stats = json.loads((workspace / "mutmut-stats.json").read_text())
    associations = stats["tests_by_mangled_function_name"]
    if not associations or not stats["duration_by_test"]:
        raise RuntimeError("No test-to-function associations were collected")
    campaign["baseline_seconds"] = baseline_seconds
    campaign["associated_functions"] = len(associations)
    campaign["test_count"] = len(stats["duration_by_test"])
    expected, baseline_failures = read_report(workspace / "baseline.xml")
    if baseline_failures or len(expected) != campaign["test_count"]:
        raise RuntimeError("The baseline report does not match the collected test identities")
    timeout = max(60.0, 15 * baseline_seconds)
    reports = workspace / "reports"
    reports.mkdir()

    def execute(index: int, mutant: str) -> tuple[str, dict]:
        tests = args.tests
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            *settings.pytest_add_cli_args,
            "-o",
            "junit_family=xunit1",
            f"--junitxml={(reports / f'{index}.xml').resolve()}",
            *tests,
        ]
        before = time.monotonic()
        raw_code = None
        timed_out = False
        with (reports / f"{index}.log").open("w") as log:
            try:
                result = subprocess.run(
                    command,
                    cwd=workspace,
                    env={**os.environ, "MUTANT_UNDER_TEST": mutant},
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                )
                raw_code = result.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
        report_error = None
        seen, failed = set(), set()
        try:
            seen, failed = read_report(reports / f"{index}.xml")
        except (OSError, ValueError, ET.ParseError) as error:
            report_error = str(error)
        if timed_out:
            code = 36
        elif seen == expected and not report_error and raw_code == 0 and not failed:
            code = 0
        elif seen == expected and not report_error and raw_code == 1 and failed:
            code = 1
        else:
            code = 35
        return mutant, {
            "exit_code": code,
            "raw_exit_code": raw_code,
            "timed_out": timed_out,
            "report_error": report_error,
            "failures": sorted(failed),
            "test_identities_match": seen == expected,
            "seconds": time.monotonic() - before,
            "tests": tests,
            "report": f"reports/{index}.xml",
        }

    with ThreadPoolExecutor(max_workers=cores) as executor:
        futures = [executor.submit(execute, index, mutant) for index, mutant in enumerate(selected)]
        for future in as_completed(futures):
            mutant, result = future.result()
            metadata["exit_code_by_key"][mutant] = result["exit_code"]
            metadata["durations_by_key"][mutant] = result["seconds"]
            campaign["results"][mutant] = result
            metadata_path.write_text(json.dumps(metadata, indent=2))
            campaign["seconds"] = time.monotonic() - started
            campaign_path.write_text(json.dumps(campaign, indent=2))
            print(
                f"[{len(campaign['results'])}/{len(selected)}] {mutant}: {result['exit_code']}",
                flush=True,
            )
    cli(["results", "--all", "true"], standalone_mode=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
