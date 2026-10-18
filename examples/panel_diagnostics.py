"""Run panel diagnostics without treating null results as identification proof."""

from datetime import UTC, datetime

import pyarrow as pa

from terrine.data import ExecutionSnapshot, MaterializedFrame, Provenance, Roles
from terrine.diagnostics import negative_control_test, placebo_test, pretrend_test
from terrine.estimate.stacked import StackedEventStudy

origin = datetime(2025, 1, 1, tzinfo=UTC)
stacked = StackedEventStudy(
    cohorts=(origin, datetime(2025, 2, 1, tzinfo=UTC)),
    periods=(-2, -1, 0),
    effects=(0.0, 0.0, 1.0, 0.0, 0.0, 1.2),
    covariance=tuple(tuple(0.04 if i == j else 0.0 for j in range(6)) for i in range(6)),
    standard_errors=(0.2,) * 6,
    aggregate_effects={-2: 0.0, -1: 0.0, 0: 1.1},
    aggregate_standard_errors={-2: 0.2, -1: 0.2, 0: 0.2},
    cohort_weights={origin.isoformat(): 0.5, datetime(2025, 2, 1, tzinfo=UTC).isoformat(): 0.5},
    n=80,
    n_clusters=40,
    alpha=0.05,
    provenance={"reference_period": -3},
)

rows = [
    {
        "unit": unit,
        "anchor_id": (unit, period),
        "t0": origin,
        "treatment": unit < 20,
        "event_period": period,
        "outcome": float((unit < 20) * (period == -2)),
        "negative_control": float((unit % 7) / 7),
    }
    for unit in range(40)
    for period in (-2, -1, 0)
]
frame = MaterializedFrame(
    pa.Table.from_pylist(rows),
    Roles("unit", "anchor_id", "t0", "treatment", (), ("outcome",), cluster_unit="unit"),
    {},
    Provenance(
        {},
        {},
        "example-design",
        "example-model",
        {},
        ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
        "calendar_comparison",
        None,
    ),
)

for diagnostic in (
    pretrend_test(stacked),
    placebo_test(frame, outcome="outcome", placebo_periods=(-2, -1)),
    negative_control_test(frame, outcome="outcome", negative_control="negative_control"),
):
    print(diagnostic.to_dict())
    print("A nonsignificant diagnostic does not establish identification or parallel trends.")
