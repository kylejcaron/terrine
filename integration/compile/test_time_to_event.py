from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from terrine.data import MaterializedFrame
from terrine.errors import TerrineError
from terrine.spec import Design, EventAnchor, FixedDate, event_time
from terrine.spec.design import Treatment
from terrine.spec.trajectory import TimeToEvent
from tests.compile.test_time_to_event import _collect, _model


def test_life_table_grouping_features_retain_conservative_surrogate_timing():
    from examples.trajectories_survival import _model as trajectory_model
    from terrine import Budget, asof, eq, recipes
    from terrine.export import ArrowSurrogate, to_surrogate_arrow

    con, model = trajectory_model()
    try:
        life = (
            recipes.life_table(
                start_event="purchase",
                end_event="churn",
                bin="7d",
                group_by={
                    "region": asof("region", before="-1d"),
                    "north": eq(asof("region", before="-1d"), "north"),
                },
                administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
            )
            .build(model)
            .materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
        )
        for feature in ("region", "north"):
            surrogate = to_surrogate_arrow(life, features=(feature,), outcomes=("events",))
            restored = ArrowSurrogate.from_arrow(surrogate.to_arrow())
            timing = life.provenance.expression_timings[feature]
            assert timing.point_time_column is None
            assert timing.start_bound_column is not None
            assert timing.end_bound_column is not None
            assert timing.start_bound_column in restored.data.column_names
            assert timing.end_bound_column in restored.data.column_names
            for row in life.table.to_pylist():
                assert row[timing.start_bound_column] == datetime(2024, 12, 31, tzinfo=UTC)
                assert row[timing.end_bound_column] == (
                    datetime(2025, 1, 14, tzinfo=UTC)
                    if row["north"]
                    else datetime(2024, 12, 31, tzinfo=UTC)
                )
    finally:
        con.disconnect()


def test_prevalent_events_are_excluded_and_cutoff_equality_is_censored():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            treatment=Treatment("treated", known_at_t0=True),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
        )
        frame = design.build(model).materialize()
        restored = MaterializedFrame.from_arrow(frame.to_arrow())
        rows = restored.table.to_pylist()
        assert sorted(
            (row["unit"], row["event_time"], row["event"], row["treatment"]) for row in rows
        ) == [
            ("u1", datetime(2025, 1, 1, tzinfo=UTC), True, True),
            ("u2", None, False, False),
        ]
        assert all(row["duration"] is not None for row in rows)
        assert pa.types.is_duration(restored.table.schema.field("duration").type)
        assert restored.roles.treatment == "treatment"
        assert "treatment" in restored.columns
        treatment_meta = restored.columns["treatment"]
        assert all(
            column is None or column in restored.table.column_names
            for column in (
                treatment_meta.observed_from_column,
                treatment_meta.observed_until_column,
                treatment_meta.complete_column,
                treatment_meta.reason_column,
            )
        )
    finally:
        con.disconnect()


@pytest.mark.parametrize("shape", ["cross_section", TimeToEvent("churn")])
def test_landmark_features_and_survival_followup_use_shifted_entry(shape):
    con, model = _model()
    try:
        con.raw_sql(
            "INSERT INTO entries VALUES "
            "('u2', TIMESTAMPTZ '2025-01-02 12:00:00+00'), "
            "('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00'), "
            "('u2', TIMESTAMPTZ '2025-01-03 01:00:00+00')"
        )
        design = Design(
            "user",
            EventAnchor("signup", offset="1d"),
            features={
                "latest_entry": event_time("enrolled", window=("-1d", "0s"), occurrence="last")
            },
            shape=shape,
        )
        frame = design.build(model).materialize()
        restored = MaterializedFrame.from_arrow(frame.to_arrow())
        rows = {row["unit"]: row for row in restored.table.to_pylist()}
        assert restored.provenance.design_hash == design.spec_hash
        assert rows["u2"]["t0"] == datetime(2025, 1, 3, tzinfo=UTC)
        assert rows["u2"]["latest_entry"] == datetime(2025, 1, 2, 12, tzinfo=UTC)
        timing = restored.provenance.expression_timings["latest_entry"]
        assert timing.end_bound_column is not None
        assert rows["u2"][timing.end_bound_column] == rows["u2"]["t0"]
        if shape == "cross_section":
            assert set(rows) == {"u1", "u2", "u3"}
            assert rows["u1"]["latest_entry"] is None
            assert rows["u3"]["latest_entry"] == datetime(2025, 1, 3, tzinfo=UTC)
        else:
            assert set(rows) == {"u2"}
            assert rows["u2"]["event"] is True
            assert rows["u2"]["event_time"] == datetime(2025, 1, 4, tzinfo=UTC)
            assert rows["u2"]["duration"] == timedelta(days=1)
    finally:
        con.disconnect()


def test_unobserved_initial_history_is_explained_and_retained():
    con, model = _model(outcome_start="2025-01-02")
    try:
        design = Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
        summary = design.describe(model)
        result = _collect(design, model)
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["duration__complete"] is False
        assert rows["u1"]["duration__reason"] == "incomplete_initial_history"
        assert rows["u1"]["duration__reason"] in summary
    finally:
        con.disconnect()


def test_non_delayed_entry_advance_without_prior_event_is_incomplete():
    con, model = _model()
    try:
        con.raw_sql("DELETE FROM outcomes WHERE user_id = 'u1'")
        con.raw_sql("INSERT INTO outcomes VALUES ('u1', TIMESTAMPTZ '2025-01-04 00:00:00+00')")
        con.raw_sql(
            "UPDATE entries SET ts = TIMESTAMPTZ '2025-01-03 00:00:00+00' WHERE user_id = 'u1'"
        )
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn", entry="enrolled"),
        )
        rows = {row["unit"]: row for row in _collect(design, model).to_pylist()}
        assert rows["u1"]["duration__complete"] is False
        assert rows["u1"]["duration__reason"] == "incomplete_initial_history"
    finally:
        con.disconnect()


def test_event_before_declared_delayed_entry_is_prevalent():
    con, model = _model(outcome_start="2025-01-02")
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
        )
        rows = {row["unit"]: row for row in _collect(design, model).to_pylist()}
        assert "u1" not in rows
        assert rows["u2"]["entry_time"] == datetime(2025, 1, 2, tzinfo=UTC)
        assert rows["u2"]["event"] is True
        assert "u3" not in rows
    finally:
        con.disconnect()


def test_entry_and_exit_together_bound_followup_on_both_sides():
    con, model = _model(unit="u2")
    try:
        con.raw_sql(
            "UPDATE exits SET ts = TIMESTAMPTZ '2025-01-03 00:00:00+00' WHERE user_id = 'u2'"
        )
        design = Design(
            "user",
            FixedDate("2025-01-01"),
            shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True, exit="closed"),
        )
        rows = {row["unit"]: row for row in _collect(design, model).to_pylist()}
        assert rows["u2"]["entry_time"] == datetime(2025, 1, 2, tzinfo=UTC)
        assert rows["u2"]["censor_time"] == datetime(2025, 1, 3, tzinfo=UTC)
        assert rows["u2"]["event"] is False
    finally:
        con.disconnect()


def test_administrative_cutoff_changes_followup_without_changing_readiness():
    con, model = _model()
    try:
        base = Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
        early = base.__class__(
            "user",
            base.anchor,
            shape=base.shape,
            administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
        )
        late = base.__class__(
            "user",
            base.anchor,
            shape=base.shape,
            administrative_cutoff=datetime(2025, 1, 5, tzinfo=UTC),
        )
        early_rows = {r["unit"]: r for r in _collect(early, model).to_pylist()}
        late_rows = {r["unit"]: r for r in _collect(late, model).to_pylist()}
        assert early_rows["u2"]["event"] is False
        assert late_rows["u2"]["event"] is True
        assert early_rows["u2"]["duration"] == timedelta(days=2)
        assert late_rows["u2"]["duration"] == timedelta(days=2)
    finally:
        con.disconnect()


def test_cutoff_before_anchor_is_not_emitted_as_complete_followup():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2024, 12, 31, tzinfo=UTC),
        )
        result = _collect(design, model)
        assert all(row["duration__complete"] is False for row in result.to_pylist())
    finally:
        con.disconnect()


def test_nanosecond_events_support_coverage_outside_nanosecond_range():
    con, model = _model(outcome_start="1600-01-01", nanosecond_outcomes=True)
    try:
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_nanosecond_event_after_python_microsecond_clamp_is_retained():
    con, model = _model(
        nanosecond_outcomes=True,
        outcome_end="2262-04-12",
    )
    try:
        con.raw_sql("DELETE FROM outcomes WHERE user_id = 'u1'")
        con.raw_sql(
            "INSERT INTO outcomes VALUES ('u1', TIMESTAMP_NS '2262-04-11 23:47:16.854775500')"
        )
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["event"] is True
    finally:
        con.disconnect()


def test_nanosecond_delayed_entry_keeps_entry_before_outcome_coverage():
    con, model = _model(
        outcome_start="2025-01-03",
        nanosecond_outcomes=True,
    )
    try:
        con.raw_sql("DELETE FROM outcomes WHERE user_id = 'u1' AND ts < TIMESTAMP_NS '2025-01-02'")
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            ),
            model,
        )
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["duration__complete"] is True
    finally:
        con.disconnect()


def test_nanosecond_exit_before_outcome_coverage_is_not_ignored(monkeypatch):
    con, model = _model(
        outcome_start="2025-01-03",
        nanosecond_outcomes=True,
        unit="u1",
    )
    try:
        con.raw_sql("DELETE FROM outcomes WHERE user_id = 'u1' AND ts < TIMESTAMP_NS '2025-01-02'")
        result = _collect(
            Design(
                "user",
                FixedDate("2025-01-01"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
            columns=("unit", "duration__complete", "event", "censor_time"),
            monkeypatch=monkeypatch,
        )
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["duration__complete"] is False
        assert rows["u1"]["event"] is False
        assert rows["u1"]["censor_time"] == datetime(2025, 1, 2, tzinfo=UTC)
    finally:
        con.disconnect()


def test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range(monkeypatch):
    con, model = _model(
        nanosecond_outcomes=True,
        outcome_end="2501-01-01",
        exit_end="2502-01-01",
        unit="u1",
    )
    try:
        result = _collect(
            Design(
                "user",
                FixedDate("2025-01-01"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
            columns=("unit", "censor_time"),
            monkeypatch=monkeypatch,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds(monkeypatch):
    con, model = _model(
        nanosecond_outcomes=True,
        exit_end="2501-01-01",
        unit="u1",
    )
    try:
        con.raw_sql("UPDATE exits SET ts = TIMESTAMPTZ '2500-01-01 00:00:00+00'")
        result = _collect(
            Design(
                "user",
                FixedDate("2025-01-01"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
            columns=("unit", "censor_time"),
            monkeypatch=monkeypatch,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_wide_entry_source_timestamp_is_not_cast_to_nanoseconds():
    con, model = _model(
        nanosecond_outcomes=True,
        entry_end="2501-01-01",
    )
    try:
        con.raw_sql("UPDATE entries SET ts = TIMESTAMPTZ '2500-01-01 00:00:00+00'")
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            ),
            model,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_nanosecond_event_source_accepts_utc_timestamptz_entry_source():
    con, model = _model(nanosecond_outcomes=True)
    try:
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            ),
            model,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_nanosecond_events_survive_time_to_event_compilation_in_any_session_timezone():

    con, model = _model(nanosecond_outcomes=True)
    try:
        con.raw_sql("SET TimeZone='America/Los_Angeles'")
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
    finally:
        con.disconnect()

    con, model = _model(naive_outcomes=True)
    try:
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
        assert result.schema.field("duration").type.unit == "us"
        assert result.schema.field("entry_time").type.tz == "UTC"
    finally:
        con.disconnect()


def test_millisecond_events_promote_losslessly_to_the_microsecond_axis():
    con, model = _model(millisecond_outcomes=True)
    try:
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert result.schema.field("event_time").type.unit == "us"
        assert rows["u1"]["event_time"] == datetime(2025, 1, 1, 0, 0, 0, 1000, tzinfo=UTC)
        assert rows["u2"]["event_time"] == datetime(2025, 1, 2, 0, 0, 0, 2000, tzinfo=UTC)
    finally:
        con.disconnect()


def test_failure_before_non_delayed_entry_is_not_discarded():
    con, model = _model()
    try:
        con.raw_sql("""
            INSERT INTO outcomes VALUES
              ('u2', TIMESTAMPTZ '2025-01-03 00:00:00+00')
        """)
        con.raw_sql("""
            UPDATE entries SET ts = TIMESTAMPTZ '2025-01-04 00:00:00+00'
            WHERE user_id = 'u2'
        """)
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn", entry="enrolled", delayed_entry=False),
        )
        rows = {row["unit"]: row for row in _collect(design, model).to_pylist()}
        assert "u2" not in rows
    finally:
        con.disconnect()


def test_survival_preserves_event_feature_timing_through_arrow_round_trip():
    con, model = _model()
    try:
        frame = Design(
            "user",
            EventAnchor("signup", which="first"),
            features={"signup_time": event_time("signup", window=("-1d", "0s"))},
            shape=TimeToEvent("churn"),
        ).build(model)

        arrow = frame.to_pyarrow()
        restored = MaterializedFrame.from_arrow(arrow)
        timing = restored.provenance.expression_timings["signup_time"]
        assert timing.point_time_column in arrow.column_names
        assert timing.start_bound_column in restored.table.column_names
        assert timing.end_bound_column in restored.table.column_names
    finally:
        con.disconnect()


def test_life_table_matches_independent_pandas_risk_set_oracle():
    import pandas as pd

    from terrine.spec.trajectory import LifeTable

    con, model = _model()
    try:
        con.raw_sql(
            "DELETE FROM outcomes WHERE user_id = 'u1' "
            "AND ts = TIMESTAMPTZ '2025-01-01 00:00:00+00'"
        )
        frame = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=LifeTable(TimeToEvent("churn", entry="enrolled", delayed_entry=True), "1d"),
            administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
        ).build(model)
        materialized = frame.materialize()
        rows = materialized.table.to_pylist()
        expected = pd.DataFrame(
            [
                {"time_bin": 0, "at_risk": 1, "events": 0, "censored": 0, "delayed_entries": 0},
                {"time_bin": 1, "at_risk": 2, "events": 0, "censored": 0, "delayed_entries": 1},
                {"time_bin": 2, "at_risk": 2, "events": 1, "censored": 1, "delayed_entries": 0},
            ]
        )
        actual = pd.DataFrame(rows).sort_values("time_bin").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            actual[expected.columns], expected, check_dtype=False, check_exact=True
        )
        assert rows[-1]["censored_by_reason"] == [("administrative_cutoff", 1)]
        from terrine.export import to_surrogate_arrow

        surrogate = to_surrogate_arrow(materialized, features=(), outcomes=("events",))
        assert (
            materialized.columns["censored_by_reason"].complete_column
            == "censored_by_reason__complete"
        )
        for name in ("censored_by_reason__complete", "censored_by_reason__reason"):
            assert surrogate.data[name].to_pylist() == materialized.table[name].to_pylist()
    finally:
        con.disconnect()


def test_nanosecond_duration_outside_int64_range_uses_timestamp_range_refusal():
    con, model = _model(
        nanosecond_outcomes=True,
        outcome_start="1600-01-01",
        outcome_end="2301-01-01",
        unit="u1",
    )
    try:
        con.raw_sql("DELETE FROM outcomes")
        con.raw_sql("INSERT INTO outcomes VALUES ('u1', TIMESTAMP_NS '2200-01-01 00:00:00')")
        design = Design(
            "user",
            FixedDate("1700-01-01"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2201, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(TerrineError) as error:
            design.build(model).materialize()
        assert error.value.code == "terrine.execution.arrow_timestamp_range"
    finally:
        con.disconnect()
