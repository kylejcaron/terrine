from datetime import UTC, datetime

import pyarrow as pa
import pytest

from terrine.data import (
    ColumnMeta,
    ExecutionSnapshot,
    ExpressionTiming,
    FeatureCutoff,
    MaterializedFrame,
    Provenance,
    Roles,
)
from terrine.windows import Window


@pytest.fixture
def frame():
    table = pa.table(
        {
            "id": [1, 2],
            "anchor": [1, 2],
            "t0": pa.array(
                [datetime(2025, 1, 1, tzinfo=UTC)] * 2, type=pa.timestamp("us", tz="UTC")
            ),
            "a": [0, 1],
            "x": [1.0, 2.0],
            "y": [1.0, 2.0],
            "complete": [True, True],
            "reason": pa.array([None, None], type=pa.string()),
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
            engine_name="arrow",
            dialect="generic",
            engine_status="unverified",
            expression_timings={
                "y": ExpressionTiming(
                    "fixed_t0", None, None, (0, 28 * 86400), (), {}, "t0", None, None
                )
            },
            treatment_knowledge="recorded_at",
            treatment_known_at_t0=True,
        ),
    )
