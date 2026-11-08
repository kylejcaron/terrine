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
    cutoff = FeatureCutoff(latest_seconds=0, strict=True, knowledge="recorded_at")
    return MaterializedFrame(
        table=table,
        roles=Roles(
            unit="id", anchor="anchor", t0="t0", treatment="a", features=("x",), outcomes=("y",)
        ),
        columns={
            "x": ColumnMeta(
                window=None,
                role="feature",
                observed_from_column=None,
                observed_until_column=None,
                complete_column=None,
                reason_column=None,
            ),
            "a": ColumnMeta(
                window=None,
                role="treatment",
                observed_from_column=None,
                observed_until_column=None,
                complete_column=None,
                reason_column=None,
            ),
            "y": ColumnMeta(
                window=Window(0, "28s"),
                role="outcome",
                observed_from_column=None,
                observed_until_column=None,
                complete_column="complete",
                reason_column="reason",
            ),
        },
        provenance=Provenance(
            feature_cutoffs={"x": cutoff},
            eligibility_cutoffs={},
            design_hash="manual",
            model_hash="manual",
            source_generations={},
            execution_snapshot=ExecutionSnapshot(
                snapshot_id=None,
                backend="arrow",
                source_generations={},
                input_binding_id=None,
                replay_equivalence="unverified",
            ),
            shape="cross_section",
            treatment_design="baseline_binary",
            engine_name="arrow",
            dialect="generic",
            engine_status="unverified",
            expression_timings={
                "y": ExpressionTiming(
                    kind="fixed_t0",
                    selector_ref=None,
                    relative_offsets=None,
                    support_seconds=(0, 28 * 86400),
                    selector_dependencies=(),
                    selector_support_seconds={},
                    point_time_column="t0",
                    start_bound_column=None,
                    end_bound_column=None,
                )
            },
            treatment_knowledge="recorded_at",
            treatment_known_at_t0=True,
        ),
    )
