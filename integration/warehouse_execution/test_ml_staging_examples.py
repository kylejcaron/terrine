from __future__ import annotations

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
    section = source.read_text().split("[start:first-dataset] -->\n", 1)[1]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    # The guide's optional steps continue the README session.
    guide = source.parent / "docs/guides/first-dataset.md"
    snippets = re.findall(r"```python\n(.*?)\n```", guide.read_text(), re.S)
    namespace = {}
    exec(compile("\n".join([code, *snippets]), str(source), "exec"), namespace)
    data = namespace["data"].sort_values(["t0", "unit"])
    assert data["t0"].dt.strftime("%Y-%m-%d").tolist() == ["2025-02-01"] * 3 + ["2025-03-01"] * 3
    assert data["unit"].tolist() == ["alice", "bob", "carol"] * 2
    assert data["orders_30d"].tolist() == [2, 1, 0, 1, 0, 0]
    assert data["spend_30d"].tolist() == [75.0, 80.0, 0.0, 25.0, 0.0, 0.0]
    assert data["orders_next_30d"].iloc[:3].tolist() == [True, False, False]
    assert data["orders_next_30d"].iloc[3:].isna().all()
    assert data["orders_next_30d__reason"].iloc[:3].isna().all()
    assert data["orders_next_30d__reason"].iloc[3:].eq("incomplete_coverage").all()
    assert namespace["result"].roles.features == ("orders_30d", "spend_30d")
    assert namespace["result"].roles.outcomes == ("orders_next_30d",)
    assert namespace["reopened"].manifest == namespace["dataset"].manifest
