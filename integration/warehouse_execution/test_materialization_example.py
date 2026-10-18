from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_model_design_example_publishes_exact_generation_and_declared_correction():
    script = Path(__file__).parents[2] / "examples" / "materialization_runtime.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    first, corrected = (json.loads(line) for line in result.stdout.splitlines())
    assert first == {
        "generation": "generation-1",
        "rows": [{"user_id": 1, "total": 3}, {"user_id": 2, "total": 4}],
    }
    assert corrected == {
        "generation": "generation-2",
        "rows": [{"user_id": 1, "total": 8}, {"user_id": 2, "total": 4}],
    }
