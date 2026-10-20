from datetime import UTC, datetime, timedelta

import ibis
import pytest

from terrine.data import Coverage, MaterializedFrame
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, FixedDate, agg, event_time
from terrine.spec.trajectory import TimeToEvent


def _model(
    *,
    outcome_start="2024-12-30",
    outcome_end="2025-01-11",
    anchor_end="2025-01-11",
    entry_start="2025-01-01",
    entry_end="2025-01-11",
    naive_outcomes=False,
    exit_start="2025-01-01",
    exit_end="2025-01-11",
    nanosecond_outcomes=False,
):

    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE anchors AS SELECT * FROM (VALUES
          ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00'),
          ('u2', TIMESTAMPTZ '2025-01-02 00:00:00+00'),
          ('u3', TIMESTAMPTZ '2025-01-03 00:00:00+00')
        ) AS a(user_id, ts)
    """)
    if nanosecond_outcomes:
        con.raw_sql("""
            CREATE TABLE outcomes AS SELECT * FROM (VALUES
              ('u1', TIMESTAMP_NS '2025-01-01 00:00:00.000000001'),
              ('u1', TIMESTAMP_NS '2025-01-03 00:00:00'),
              ('u2', TIMESTAMP_NS '2025-01-02 00:00:00.000000002'),
              ('u3', TIMESTAMP_NS '2025-01-01 00:00:00')
            ) AS e(user_id, ts)
        """)
    elif naive_outcomes:
        con.raw_sql("""
            CREATE TABLE outcomes AS SELECT * FROM (VALUES
              ('u1', TIMESTAMP '2025-01-01 00:00:00'),
              ('u1', TIMESTAMP '2025-01-03 00:00:00'),
              ('u2', TIMESTAMP '2025-01-04 00:00:00'),
              ('u3', TIMESTAMP '2025-01-01 00:00:00')
            ) AS e(user_id, ts)
        """)
    else:
        con.raw_sql("""
            CREATE TABLE outcomes AS SELECT * FROM (VALUES
              ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00'),
              ('u1', TIMESTAMPTZ '2025-01-03 00:00:00+00'),
              ('u2', TIMESTAMPTZ '2025-01-04 00:00:00+00'),
              ('u3', TIMESTAMPTZ '2025-01-01 00:00:00+00')
            ) AS e(user_id, ts)
        """)
    con.raw_sql("""
        CREATE TABLE entries AS SELECT * FROM (VALUES
          ('u1', TIMESTAMPTZ '2025-01-02 00:00:00+00'),
          ('u2', TIMESTAMPTZ '2025-01-02 00:00:00+00'),
          ('u3', TIMESTAMPTZ '2025-01-03 00:00:00+00')
        ) AS x(user_id, ts)
    """)
    con.raw_sql("CREATE TABLE exits AS SELECT * FROM entries")
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "anchors": {
                    "relation": {"kind": "table", "name": "anchors"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["signup"],
                    "properties": [],
                    "dimensions": [],
                },
                "outcomes": {
                    "relation": {"kind": "table", "name": "outcomes"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["churn"],
                    "properties": [],
                    "dimensions": [],
                },
                "entries": {
                    "relation": {"kind": "table", "name": "entries"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["enrolled"],
                    "properties": [],
                    "dimensions": [],
                },
                "exits": {
                    "relation": {"kind": "table", "name": "exits"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["closed"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "facts": {
                "signup": {"source": "anchors", "column": None},
                "churn": {"source": "outcomes", "column": None},
                "enrolled": {"source": "entries", "column": None},
                "closed": {"source": "exits", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "anchors": Coverage("2025-01-01", anchor_end),
            "outcomes": Coverage(outcome_start, outcome_end),
            "entries": Coverage(entry_start, entry_end),
            "exits": Coverage(exit_start, exit_end),
        },
    )
    return con, model


def _collect(design, model):
    frame = design.build(model)
    compiled = frame._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    return frame._normalize_output(model.con.to_pyarrow(compiled.table))


def test_prevalent_events_are_excluded_and_cutoff_equality_is_censored():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
        )
        rows = _collect(design, model).to_pylist()
        assert sorted((row["unit"], row["event_time"], row["event"]) for row in rows) == [
            ("u1", datetime(2025, 1, 1, tzinfo=UTC), True),
            ("u2", None, False),
        ]
        assert all(row["duration"] is not None for row in rows)
    finally:
        con.disconnect()


def test_unobserved_initial_history_is_explained_and_excluded():
    con, model = _model(outcome_start="2025-01-02")
    try:
        design = Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
        result = _collect(design, model)
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["complete"] is False
        assert rows["u1"]["reason"] == "incomplete_initial_history"
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
        assert rows["u1"]["complete"] is False
        assert rows["u1"]["reason"] == "incomplete_initial_history"
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
        assert early_rows["u2"]["duration"] == datetime(2025, 1, 4, tzinfo=UTC)
        assert late_rows["u2"]["duration"] == datetime(2025, 1, 4, tzinfo=UTC)
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
        assert all(row["complete"] is False for row in result.to_pylist())
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
        assert rows["u1"]["complete"] is True
    finally:
        con.disconnect()


def test_nanosecond_exit_before_outcome_coverage_is_not_ignored():
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
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
        )
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["complete"] is False
        assert rows["u1"]["event"] is False
        assert rows["u1"]["censor_time"] == datetime(2025, 1, 2, tzinfo=UTC)
    finally:
        con.disconnect()


def test_nanosecond_wide_outcome_and_exit_coverages_do_not_narrow_out_of_range():
    con, model = _model(
        nanosecond_outcomes=True,
        outcome_end="2501-01-01",
        exit_end="2502-01-01",
    )
    try:
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_wide_entry_coverage_start_is_intersected_before_narrowing():
    con, model = _model(
        nanosecond_outcomes=True,
        entry_start="2500-01-01",
        entry_end="2501-01-01",
    )
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


def test_wide_exit_coverage_start_is_intersected_before_narrowing():
    con, model = _model(
        nanosecond_outcomes=True,
        exit_start="2500-01-01",
        exit_end="2501-01-01",
    )
    try:
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
        )
        assert result.num_rows > 0
    finally:
        con.disconnect()


def test_wide_exit_source_timestamp_is_not_cast_to_nanoseconds():
    con, model = _model(
        nanosecond_outcomes=True,
        exit_end="2501-01-01",
    )
    try:
        con.raw_sql("UPDATE exits SET ts = TIMESTAMPTZ '2500-01-01 00:00:00+00'")
        result = _collect(
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            ),
            model,
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


def test_nanosecond_axis_refuses_wide_fixed_anchor_and_cutoff_before_execution():
    from terrine.compile.shapes.time_to_event import compile_time_to_event
    from terrine.data import UnitSource

    con, model = _model(nanosecond_outcomes=True)
    model = Model.from_catalog(
        model.catalog,
        con=con,
        readiness=model.readiness.sources,
        units={"user": UnitSource("anchors", "user_id")},
    )
    try:
        designs = (
            Design("user", FixedDate("1600-01-01"), shape=TimeToEvent("churn")),
            Design(
                "user",
                FixedDate("2025-01-01"),
                shape=TimeToEvent("churn"),
                administrative_cutoff=datetime(2400, 1, 1, tzinfo=UTC),
            ),
        )
        for design in designs:
            with pytest.raises(TerrineError) as error:
                compile_time_to_event(design, model)
            assert error.value.code == "terrine.temporal.timestamp_order_unsupported"
    finally:
        con.disconnect()


def test_wide_roster_observation_end_is_intersected_before_narrowing():
    from terrine.data import UnitSource

    con, model = _model(nanosecond_outcomes=True)
    try:
        con.raw_sql("""
            CREATE TABLE roster AS SELECT * FROM (VALUES
              ('u1', TIMESTAMPTZ '2500-01-01 00:00:00+00'),
              ('u2', TIMESTAMPTZ '2500-01-01 00:00:00+00'),
              ('u3', TIMESTAMPTZ '2500-01-01 00:00:00+00')
            ) AS r(user_id, observed_until)
        """)
        con.raw_sql("DELETE FROM outcomes")
        model = Model.from_catalog(
            model.catalog,
            con=con,
            readiness=model.readiness.sources,
            units={"user": UnitSource("roster", "user_id", observed_until="observed_until")},
        )
        result = _collect(
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn")), model
        )
        rows = result.to_pylist()
        assert rows
        assert all(row["event"] is False for row in rows)
        assert all(row["censor_time"] == datetime(2025, 1, 12, tzinfo=UTC) for row in rows)
    finally:
        con.disconnect()


def test_nanosecond_source_refuses_administrative_cutoff_beyond_ns_range():
    con, model = _model(nanosecond_outcomes=True)
    try:
        with pytest.raises(TerrineError) as error:
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn"),
                administrative_cutoff=datetime(2500, 1, 1, tzinfo=UTC),
            ).build(model)
        assert error.value.code == "terrine.temporal.timestamp_order_unsupported"
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


def test_internal_timestamp_aliases_are_reserved_feature_names():
    con, model = _model()
    try:
        with pytest.raises(TerrineError, match="generated_column_collision"):
            Design(
                "user",
                EventAnchor("signup", which="first"),
                features={"_t0_naive": agg("signup", window=("-1d", "0d"))},
                shape=TimeToEvent("churn"),
            ).build(model)
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
        assert result.schema.field("duration").type.tz == "UTC"
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


def test_internal_survival_columns_cannot_overwrite_features():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            features={"_followup_start": agg("missing", window=("-1d", "0d"))},
            shape=TimeToEvent("churn"),
        )
        with pytest.raises(TerrineError) as error:
            design.build(model)
        assert error.value.code == "terrine.survival.invalid_compilation"
    finally:
        con.disconnect()


def test_survival_generated_columns_cannot_overwrite_features():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            features={"duration": agg("missing", window=("-1d", "0d"))},
            shape=TimeToEvent("churn"),
        )
        with pytest.raises(TerrineError) as error:
            design.build(model)
        assert error.value.code == "terrine.survival.invalid_compilation"
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
        assert timing.start_bound_column in arrow.column_names
        assert timing.end_bound_column in arrow.column_names
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
        rows = frame.materialize().table.to_pylist()
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
    finally:
        con.disconnect()


def test_life_table_delayed_entries_and_incomplete_records_preserve_reason():
    import pyarrow as pa

    from terrine.compile.shapes.life_table import aggregate_life_table

    timestamp = pa.timestamp("us", tz="UTC")
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": pa.array([origin], type=timestamp),
            "duration": pa.array([origin + timedelta(hours=18)], type=timestamp),
            "entry_time": pa.array([origin + timedelta(hours=12)], type=timestamp),
            "event": pa.array([True]),
            "complete": pa.array([False]),
            "reason": pa.array(["incomplete_initial_history"]),
            "censor_reason": pa.array([None], type=pa.string()),
        }
    )
    rows = aggregate_life_table(records, interval_seconds=86_400, group_by=[]).to_pylist()
    assert len(rows) == 1
    assert rows[0]["at_risk"] == 1
    assert rows[0]["delayed_entries"] == 1
    assert rows[0]["events"] == 0
    assert rows[0]["censored"] == 1
    assert rows[0]["censored_by_reason"] == [("incomplete_initial_history", 1)]


def test_life_table_row_budget_applies_to_expanded_output():
    import pyarrow as pa

    from terrine.compile.shapes.life_table import aggregate_life_table

    timestamp = pa.timestamp("us", tz="UTC")
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": pa.array([origin], type=timestamp),
            "duration": pa.array([origin + timedelta(days=2)], type=timestamp),
            "entry_time": pa.array([origin], type=timestamp),
            "event": pa.array([True]),
            "censor_reason": pa.array([None], type=pa.string()),
        }
    )
    with pytest.raises(TerrineError) as error:
        aggregate_life_table(records, interval_seconds=86_400, group_by=[], max_rows=1)
    assert error.value.code == "terrine.budget.rows_exceeded"


def test_life_table_keeps_boundary_censor_at_risk_and_group_support():
    import pyarrow as pa

    from terrine.compile.shapes.life_table import aggregate_life_table

    timestamp = pa.timestamp("us", tz="UTC")
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": pa.array([origin, origin, origin], type=timestamp),
            "duration": pa.array(
                [
                    origin + timedelta(days=1),
                    origin + timedelta(days=1),
                    origin - timedelta(days=1),
                ],
                type=timestamp,
            ),
            "entry_time": pa.array([origin, origin, origin], type=timestamp),
            "event": pa.array([False, False, False]),
            "censor_reason": pa.array(
                ["administrative_cutoff", "administrative_cutoff", "invalid_record"]
            ),
            "region": pa.array([None, None, None], type=pa.string()),
            "region_complete": pa.array([False, False, True]),
            "region_reason": pa.array(["coverage_gap", "source_missing", "invalid_reason"]),
            "region_observed_until": pa.array(
                [
                    origin + timedelta(days=1),
                    origin + timedelta(days=2),
                    origin + timedelta(days=3),
                ],
                type=timestamp,
            ),
        }
    )
    result = aggregate_life_table(
        records,
        interval_seconds=86_400,
        group_by=["region"],
        group_supports={
            "region_complete": "all",
            "region_reason": "reasons",
            "region_observed_until": "max",
        },
    )
    assert result.num_rows == 2
    row = result.to_pylist()[-1]
    assert row["region_complete"] is False
    assert row["region_reason"] == "coverage_gap;source_missing"
    assert row["region_observed_until"] == origin + timedelta(days=2)
    assert row["at_risk"] == 2
    assert row["censored"] == 2
    assert row["censor_rate"] == 1
    without_support_metadata = aggregate_life_table(
        records, interval_seconds=86_400, group_by=["region"]
    )
    assert without_support_metadata.num_rows == 2
