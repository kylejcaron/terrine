from datetime import UTC, datetime

from terrine.data import Budget, MaterializedFrame
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory
from tests.compile.test_ltv import _trajectory_model


def test_local_design_to_same_frame_cohort_arrow_under_budget():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                "purchase", Trajectory("net_revenue", "7d", periods=3, cumulative=True)
            ),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        ).build(model)
        arrow = frame.cohorts(
            grouping=CohortGrouping("7d", datetime(2025, 1, 1, tzinfo=UTC)),
            value_column="cumulative_value",
        ).to_pyarrow(budget=Budget(max_rows=100, max_output_bytes=1_000_000))
        rows = arrow.to_pylist()
        assert rows
        assert "at_risk" in arrow.column_names
        restored = MaterializedFrame.from_arrow(arrow)
        assert restored.provenance.cohort_operation is not None
        assert "observable" in arrow.column_names
        assert arrow.schema.metadata is not None
    finally:
        con.disconnect()
