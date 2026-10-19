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
    first["rows"].sort(key=lambda row: row["user_id"])
    corrected["rows"].sort(key=lambda row: row["user_id"])
    assert first["generation"]
    assert corrected["generation"] != first["generation"]
    assert first["rows"] == [
        {"user_id": 1, "total": 3, "tier": "gold"},
        {"user_id": 2, "total": 4, "tier": "gold"},
    ]
    assert corrected["rows"] == [
        {"user_id": 1, "total": 8, "tier": "silver"},
        {"user_id": 2, "total": 4, "tier": "silver"},
    ]
