from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests._timing import HANG_GUARD_SECONDS


@pytest.mark.parametrize("workflow", ["ml_staging", "batch_scoring"])
def test_dataset_examples_complete_public_workflow(workflow):
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "examples" / f"{workflow}.py")],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
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


def test_readme_builds_and_reopens_first_dataset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TERRINE_RUNTIME_NAMESPACE", raising=False)
    monkeypatch.delenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", raising=False)
    source = Path(__file__).resolve().parents[2] / "README.md"
    section = source.read_text().split("## Build a study dataset\n", 1)[1]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    tutorial = ast.parse(code)
    # Run the optional guide steps before the tutorial closes its connection.
    connection_scope = next(node for node in ast.walk(tutorial) if isinstance(node, ast.Try))
    guide = source.parent / "docs/guides/first-dataset.md"
    for snippet in re.findall(r"```python\n(.*?)\n```", guide.read_text(), re.S):
        connection_scope.body.extend(ast.parse(snippet).body)
    namespace = {}
    exec(compile(ast.fix_missing_locations(tutorial), str(source), "exec"), namespace)
    data = namespace["data"].sort_values("unit")
    columns = ["unit", "before", "before__complete", "after", "after__complete"]
    assert data[columns].to_dict("records") == [
        {"unit": "a", "before": 1, "before__complete": True, "after": 2, "after__complete": True},
        {"unit": "b", "before": 0, "before__complete": True, "after": 0, "after__complete": True},
    ]
    assert data[["before__reason", "after__reason"]].isna().all().all()
    assert namespace["result"].roles.features == ("before",)
    assert namespace["result"].roles.outcomes == ("after",)
    assert namespace["reopened"].manifest == namespace["dataset"].manifest
