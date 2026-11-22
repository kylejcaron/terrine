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


@pytest.fixture
def calendar_frame(frame):
    """One treated and one eligible control per cell, sharing outcome completeness."""
    from dataclasses import replace
    from datetime import timedelta

    from terrine.data import CalendarComparisonMeta, CalendarSupportCell

    cohort = datetime(2025, 1, 3, tzinfo=UTC)
    table = pa.table(
        {
            "id": ["C", "C", "T", "T"],
            "anchor": [1, 2, 1, 2],
            "t0": [cohort - timedelta(days=1), cohort] * 2,
            "cohort": [cohort] * 4,
            "event_period": [-1, 0, -1, 0],
            "treatment": [False, False, True, True],
            "control_eligible": [True, True, False, False],
            "x": [2.0] * 4,
            "y": [5.0, 7.0, 3.0, None],
            "complete": [True] * 4,
            "reason": [None, None, None, "all_values_null"],
        }
    )
    comparison = CalendarComparisonMeta(
        control="never_treated",
        cohort_source="events",
        cohort_property="adoption",
        history_start=cohort - timedelta(days=2),
        knowledge="retrospective_effective_time",
        reference_period=-1,
        comparison_periods=(0,),
        anticipation_periods=(),
        period_seconds=86_400,
        support=(
            CalendarSupportCell(cohort=cohort, period=-1, kind="reference", treated=1, control=1),
            CalendarSupportCell(cohort=cohort, period=0, kind="comparison", treated=1, control=1),
        ),
    )
    return replace(
        frame,
        table=table,
        roles=replace(
            frame.roles,
            treatment="treatment",
            cluster_unit="id",
            trial="cohort",
            features=(),
            outcomes=("x", "y"),
        ),
        columns={"x": frame.columns["y"], "y": frame.columns["y"]},
        provenance=replace(
            frame.provenance,
            shape="calendar_comparison",
            calendar_comparison=comparison,
            feature_cutoffs={},
            expression_timings={},
        ),
    )
