from __future__ import annotations

from datetime import UTC, datetime

from terrine.data import Budget, MaterializedFrame
from terrine.spec import CohortSpec, Design, EventAnchor
from terrine.spec.trajectory import CohortGrouping, Trajectory
from tests.compile.test_ltv import _trajectory_model


def test_cohort_grouping_bucket_statistic_and_denominators_match_hand_calculation():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
            administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
            ),
            value_column="cumulative_value",
        )
        rows = sorted(
            cohorts.to_pyarrow(budget=Budget(max_rows=20, max_output_bytes=100_000)).to_pylist(),
            key=lambda row: (row["bucket_index"], row["age"]),
        )
        assert [
            (row["bucket_index"], row["age"], row["value"], row["at_risk"], row["observable"])
            for row in rows
        ] == [
            (0, 0, 5.0, 2, 2),
            (0, 1, 7.5, 2, 2),
            (0, 2, 7.5, 2, 2),
            (2, 0, 0.0, 1, 1),
            (2, 1, None, None, None),
            (2, 2, None, None, None),
        ]
    finally:
        con.disconnect()


def test_cohort_collection_observes_fresh_snapshot_and_budget():
    import pytest

    from terrine.errors import TerrineError

    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
            ),
            value_column="cumulative_value",
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 14)"
        )
        result = cohorts.materialize(budget=Budget(max_rows=20, max_output_bytes=100_000))
        row = next(
            row for row in result.table.to_pylist() if row["bucket_index"] == 0 and row["age"] == 0
        )
        assert row["value"] == 12.0
        operation = result.provenance.cohort_operation
        assert operation is not None
        assert operation.parent_design_hash == frame._design.spec_hash
        assert operation.value_column == "cumulative_value"
        restored = MaterializedFrame.from_arrow(result.to_arrow())
        assert restored.provenance.cohort_operation == operation
        with pytest.raises(TerrineError) as error:
            cohorts.materialize(budget=Budget(max_rows=1, max_output_bytes=100_000))
        assert error.value.code == "terrine.budget.rows_exceeded"
    finally:
        con.disconnect()


def test_cohort_table_reuses_parent_ltv_relation_and_provenance():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC)),
            value_column="period_value",
        )
        assert cohorts.parent_frame is frame
        result = cohorts.materialize()
        operation = result.provenance.cohort_operation
        assert operation is not None
        assert operation.parent_design_hash == frame._design.spec_hash
        assert operation.derived_operation_hash
    finally:
        con.disconnect()


def test_bucket_index_respects_subsecond_origin_boundaries():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="7d",
                    bucket_origin=datetime(2025, 1, 1, 0, 0, 0, 500_000, tzinfo=UTC),
                ),
                value_column="period_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        young = next(row for row in rows if row["age"] == 0 and row["at_risk"] == 1)
        assert young["bucket_index"] == 1
    finally:
        con.disconnect()


def test_repeated_first_entry_events_count_one_cohort_member():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
            "('u1', TIMESTAMPTZ '2025-01-02 00:00:00+00', 'purchase', 0)"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC)
                ),
                value_column="period_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        cohort = next(row for row in rows if row["bucket_index"] == 0)
        assert cohort["at_risk"] == 2
    finally:
        con.disconnect()


def test_bucket_index_normalizes_timestamp_remainders_across_unix_epoch():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET ts = TIMESTAMPTZ '1970-01-01 00:00:00.9+00' "
            "WHERE user_id = 'u1' AND kind = 'purchase'"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "1s", periods=1, cumulative=False),
            ),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="1s",
                    bucket_origin=datetime(1969, 12, 31, 23, 59, 59, 100_000, tzinfo=UTC),
                ),
                value_column="period_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        assert any(row["bucket_index"] == 1 for row in rows)
    finally:
        con.disconnect()


def test_zero_partial_and_unavailable_cohort_denominators():
    con, model = _trajectory_model()
    try:
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
            ),
            administrative_cutoff=datetime(2025, 1, 23, tzinfo=UTC),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC)
                ),
                value_column="cumulative_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        young_age_0 = next(row for row in rows if row["bucket_index"] == 2 and row["age"] == 0)
        young_age_1 = next(row for row in rows if row["bucket_index"] == 2 and row["age"] == 1)
        young_age_2 = next(row for row in rows if row["bucket_index"] == 2 and row["age"] == 2)
        assert (young_age_0["value"], young_age_0["at_risk"], young_age_0["observable"]) == (
            0.0,
            1,
            1,
        )
        assert (young_age_1["value"], young_age_1["at_risk"], young_age_1["observable"]) == (
            None,
            1,
            0,
        )
        assert (young_age_2["at_risk"], young_age_2["observable"]) == (None, None)
    finally:
        con.disconnect()


def test_exact_integer_values_are_summed_before_cohort_mean_conversion():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET amount = 9007199254740993 "
            "WHERE user_id = 'u1' AND kind = 'revenue' AND ts = "
            "TIMESTAMPTZ '2025-01-03 00:00:00+00'"
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', -9007199254740992)"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("net_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        rows = (
            frame.cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
                ),
                value_column="period_value",
            )
            .to_pyarrow()
            .to_pylist()
        )
        group = next(row for row in rows if row["bucket_index"] == 0 and row["age"] == 0)
        assert group["value"] == 0.5
    finally:
        con.disconnect()


def test_cohort_mean_preserves_fractional_decimal_and_float_values():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET decimal_amount = 0.25, float_amount = 0.25 WHERE kind = 'revenue'"
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 0, 0.25, 0.25)"
        )
        grouping = CohortGrouping(
            bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
        )
        for metric in ("decimal_revenue", "float_revenue"):
            frame = Design(
                "user",
                EventAnchor("purchase", which="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory(metric, "7d", periods=1, cumulative=False),
                ),
            ).build(model)
            rows = (
                frame.cohorts(grouping=grouping, value_column="period_value")
                .to_pyarrow()
                .to_pylist()
            )
            group = next(row for row in rows if row["bucket_index"] == 0 and row["age"] == 0)
            assert group["value"] == 0.25
    finally:
        con.disconnect()


def test_cohort_float_total_enforces_final_tolerance():
    import pytest

    from terrine.errors import TerrineError

    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET float_amount = 1e12 WHERE user_id = 'u1' AND kind = 'revenue'"
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 0, 0, -1e12)"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("float_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        cohorts = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="total"
            ),
            value_column="period_value",
        )
        with pytest.raises(TerrineError) as error:
            cohorts.materialize()
        assert error.value.code == "terrine.temporal.float64_precision"
    finally:
        con.disconnect()


def test_cohort_summary_surrogate_export_retains_age_and_denominators():
    from terrine.estimate.survival import to_surrogate_arrow

    con, model = _trajectory_model()
    try:
        cohort = (
            Design(
                "user",
                EventAnchor("purchase", which="first"),
                shape=CohortSpec(
                    origin="purchase",
                    trajectory=Trajectory("net_revenue", "7d", periods=2, cumulative=False),
                ),
            )
            .build(model)
            .cohorts(
                grouping=CohortGrouping(
                    bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
                ),
                value_column="period_value",
            )
        )
        materialized = cohort.materialize()
        exported = to_surrogate_arrow(materialized, features=(), outcomes=("value",))
        assert {"age", "at_risk", "observable"} <= set(exported.data.column_names)
        assert sorted(set(exported.data["age"].to_pylist())) == [0, 1]
    finally:
        con.disconnect()


def test_float_cohort_mean_avoids_overflow_and_retains_certificate():
    con, model = _trajectory_model()
    try:
        con.raw_sql(
            "UPDATE events SET float_amount = 1e308 WHERE user_id = 'u1' AND kind = 'revenue'"
        )
        con.raw_sql(
            "UPDATE events SET float_amount = 0 WHERE user_id = 'u1' AND ts = "
            "TIMESTAMPTZ '2025-01-10 00:00:00+00'"
        )
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount) VALUES "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 0, 0, 1e308)"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("float_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        result = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
            ),
            value_column="period_value",
        ).materialize()
        row = next(row for row in result.table.to_pylist() if row["bucket_index"] == 0)
        assert row["value"] == 1e308
        error_column = result.columns["value"].absolute_error_column
        assert result.columns["value"].numeric_policy == "checked_float64_v1"
        assert error_column is not None and row[error_column] > 0
    finally:
        con.disconnect()


def test_float_mean_certificate_covers_many_member_rounding():
    from decimal import Decimal

    con, model = _trajectory_model()
    try:
        count = 256
        con.raw_sql(
            "UPDATE events SET float_amount = 1.0 "
            "WHERE user_id = 'u1' AND kind = 'revenue' AND ts = "
            "TIMESTAMPTZ '2025-01-03 00:00:00+00'"
        )
        con.raw_sql(
            "UPDATE events SET float_amount = 0.0 "
            "WHERE user_id = 'u1' AND kind = 'revenue' AND ts = "
            "TIMESTAMPTZ '2025-01-10 00:00:00+00'"
        )
        users = ", ".join(f"('small-{index}')" for index in range(count))
        events = ", ".join(
            f"('small-{index}', TIMESTAMPTZ '2025-01-01 00:00:00+00', 'purchase', 0, 0, 0), "
            f"('small-{index}', TIMESTAMPTZ '2025-01-03 00:00:00+00', 'revenue', 0, 0, "
            "5.551115123125783e-17)"
            for index in range(count)
        )
        con.raw_sql(f"INSERT INTO users VALUES {users}")
        con.raw_sql(
            "INSERT INTO events (user_id, ts, kind, amount, decimal_amount, float_amount) "
            f"VALUES {events}"
        )
        frame = Design(
            "user",
            EventAnchor("purchase", which="first"),
            shape=CohortSpec(
                origin="purchase",
                trajectory=Trajectory("float_revenue", "7d", periods=1, cumulative=False),
            ),
        ).build(model)
        result = frame.cohorts(
            grouping=CohortGrouping(
                bucket="7d", bucket_origin=datetime(2025, 1, 1, tzinfo=UTC), statistic="mean"
            ),
            value_column="period_value",
        ).materialize()
        row = next(row for row in result.table.to_pylist() if row["bucket_index"] == 0)
        expected = (Decimal(1) + count * Decimal.from_float(2**-54)) / Decimal(count + 2)
        error_column = result.columns["value"].absolute_error_column
        assert error_column is not None
        assert abs(Decimal.from_float(row["value"]) - expected) <= Decimal.from_float(
            row[error_column]
        )
    finally:
        con.disconnect()
