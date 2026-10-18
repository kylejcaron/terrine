from __future__ import annotations

from datetime import UTC, datetime

from terrine.data import Budget
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory
from tests.compile.test_ltv import _trajectory_model


def test_design_cutoff_updates_ltv_and_cohort_with_readiness_fixed():
    con, model = _trajectory_model()
    try:
        grouping = CohortGrouping(
            bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
        )

        def design(cutoff):
            return Design(
                "user",
                EventAnchor("purchase", which="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
                ),
                administrative_cutoff=cutoff,
            )

        readiness = model.readiness
        early_design = design(datetime(2025, 1, 22, tzinfo=UTC))
        later_design = design(datetime(2025, 1, 23, tzinfo=UTC))
        early_design.validate(model)
        later_design.validate(model)
        assert model.readiness == readiness
        early = early_design.build(model)
        later = later_design.build(model)
        early_rows = (
            early.cohorts(grouping=grouping, value_column="cumulative_value")
            .to_pyarrow(budget=Budget(max_rows=20, max_output_bytes=100_000))
            .to_pylist()
        )
        later_rows = (
            later.cohorts(grouping=grouping, value_column="cumulative_value")
            .to_pyarrow(budget=Budget(max_rows=20, max_output_bytes=100_000))
            .to_pylist()
        )
        early_young = next(
            row for row in early_rows if row["bucket_index"] == 2 and row["age"] == 1
        )
        later_young = next(
            row for row in later_rows if row["bucket_index"] == 2 and row["age"] == 1
        )
        assert (early_young["at_risk"], early_young["observable"]) == (None, None)
        assert (later_young["at_risk"], later_young["observable"]) == (1, 0)
    finally:
        con.disconnect()
