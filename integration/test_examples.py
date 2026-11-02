from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_TESTED: frozenset[str] = frozenset(
    {"ml_staging.py", "batch_scoring.py", "dataset_agent_tools.py", "materialization_runtime.py"}
)
EXAMPLES = sorted(p for p in (ROOT / "examples").glob("*.py") if p.name not in CONTRACT_TESTED)
# The tutorial runs on every push; the other examples run in the weekly scale tier.
EVERY_PUSH = "first_dataset.py"


# distinct scripts, not values on one path
@pytest.mark.parametrize(
    "script",
    [
        pytest.param(p, id=p.stem, marks=() if p.name == EVERY_PUSH else pytest.mark.scale)
        for p in EXAMPLES
    ],
)
def test_example_script_exits_cleanly(script):
    result = subprocess.run([sys.executable, str(script)], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_study_api_smoke_exits_cleanly():
    result = subprocess.run(
        [sys.executable, "-m", "tests.integration.study_api_smoke"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_contract_tested_examples_exist():
    assert all((ROOT / "examples" / name).is_file() for name in CONTRACT_TESTED)
