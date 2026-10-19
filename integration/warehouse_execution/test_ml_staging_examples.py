from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("workflow", ["ml_staging", "batch_scoring"])
def test_dataset_examples_complete_public_workflow(workflow):
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "examples" / f"{workflow}.py")],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["rows"] == report["export_rows"] == 2
    assert report["refresh_changed"] is True
    assert report["schema_metadata"] is True
    if workflow == "ml_staging":
        assert report["corrected_amount"] == "1000000000000000000000000000008"
    else:
        assert report["refreshed_rows"] == 1
        assert report["as_of"] == "2025-01-02T00:00:00+00:00"
