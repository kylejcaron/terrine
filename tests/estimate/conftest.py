"""Small immutable observed-data inputs for native estimator contracts."""

from datetime import UTC, datetime

import numpy as np
import pyarrow as pa
import pytest

from terrine.data import (
    ColumnMeta,
    ExecutionSnapshot,
    FeatureCutoff,
    MaterializedFrame,
    Provenance,
    Roles,
)
from terrine.results import DiagnosticPolicy
from terrine.windows import Window


@pytest.fixture
def frame():
    n = 120
    i = np.arange(n)
    x = np.sin(i * 1.7)
    a = (i % 4 < 2).astype(int)
    y = 3 + x + a * (2 + 0.5 * x) + np.cos(i * 2.3)
    table = pa.table(
        {
            "id": i,
            "anchor": i,
            "t0": [datetime(2025, 1, 1, tzinfo=UTC)] * n,
            "a": a,
            "x": x,
            "y": y,
            "complete": [True] * n,
            "reason": pa.array([None] * n, type=pa.string()),
            "q": 1 + 0.3 * x,
        }
    )
    cutoff = FeatureCutoff(0, True, "recorded_at")
    return MaterializedFrame(
        table,
        Roles("id", "anchor", "t0", "a", ("x",), ("y",)),
        {
            "x": ColumnMeta(None, "feature", None, None, None, None),
            "a": ColumnMeta(None, "treatment", None, None, None, None),
            "y": ColumnMeta(Window(0, 28), "outcome", None, None, "complete", "reason"),
        },
        Provenance(
            {"x": cutoff},
            {},
            "manual",
            "manual",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "cross_section",
            "baseline_binary",
            treatment_knowledge="recorded_at",
            treatment_known_at_t0=True,
        ),
    )


@pytest.fixture
def permissive():
    return DiagnosticPolicy(min_arm_ess=1, max_fail_share=1, smd_warn=10, smd_fail=20)
