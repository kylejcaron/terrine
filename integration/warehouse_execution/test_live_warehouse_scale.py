"""Run only with current owner approval, an actual remote adapter and enforceable caps."""

import json
import os
from pathlib import Path

import pytest

from benchmarks.run_live import run_approved

pytestmark = pytest.mark.slow


def test_owner_approved_live_compiler_workload():
    reference = os.environ.get("TERRINE_LIVE_APPROVAL")
    if reference is None:
        pytest.skip("UNVERIFIED: no owner-approved backend/account/workload/spend configuration")
    approval = json.loads(Path(reference).read_text(encoding="utf-8"))
    record = run_approved(approval)
    assert record["status"] == "verified" and record["oracle_match"]
    assert all(case["oracle_match"] for case in record["cases"])
    output = os.environ.get("TERRINE_LIVE_REPORT")
    if output is not None:
        from benchmarks.workloads import _json

        with Path(output).open("x", encoding="utf-8") as stream:
            stream.write(
                json.dumps(record, sort_keys=True, separators=(",", ":"), default=_json) + "\n"
            )
