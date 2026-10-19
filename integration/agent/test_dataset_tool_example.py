"""Run the local owner-authorized example without exposing source values."""

import json
import subprocess
import sys
from pathlib import Path


def test_dataset_agent_tools_example():
    root = Path(__file__).parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "examples/dataset_agent_tools.py")],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    summary = json.loads(result.stdout)
    assert summary == {
        "workflow": "restricted_dataset_tools",
        "rows": 2,
        "export_rows": 2,
        "refreshed": True,
        "redacted": True,
        "scan_unknown": True,
    }
    assert "gold" not in result.stdout and "silver" not in result.stdout
