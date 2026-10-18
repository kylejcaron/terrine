from datetime import UTC, datetime

import ibis
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, agg
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


def test_prevalent_events_are_excluded_and_cutoff_equality_is_censored():
    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2025, 1, 4, tzinfo=UTC),
        )
        rows = design.build(model).materialize().table.to_pylist()
        assert sorted((row["unit"], row["event_time"], row["event"]) for row in rows) == [
            ("u1", datetime(2025, 1, 1, tzinfo=UTC), True),
            ("u2", None, False),
        ]
        assert all(row["duration"] is not None for row in rows)
    finally:
        con.disconnect()


def test_unobserved_initial_history_is_explained_and_excluded_from_km():
    from terrine.estimate.survival import kaplan_meier

    con, model = _model(outcome_start="2025-01-02")
    try:
        design = Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
        result = design.build(model).materialize()
        rows = {row["unit"]: row for row in result.table.to_pylist()}
        assert rows["u1"]["complete"] is False
        assert rows["u1"]["reason"] == "incomplete_initial_history"
        curve = kaplan_meier(result, duration="duration", event="event", entry="entry_time")
        assert curve.at_risk == (1,)
        assert curve.events == (1,)
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
        rows = {row["unit"]: row for row in design.build(model).materialize().table.to_pylist()}
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
        rows = {row["unit"]: row for row in design.build(model).materialize().table.to_pylist()}
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
        early_rows = {r["unit"]: r for r in early.build(model).materialize().table.to_pylist()}
        late_rows = {r["unit"]: r for r in late.build(model).materialize().table.to_pylist()}
        assert early_rows["u2"]["event"] is False
        assert late_rows["u2"]["event"] is True
        assert early_rows["u2"]["duration"] == datetime(2025, 1, 4, tzinfo=UTC)
        assert late_rows["u2"]["duration"] == datetime(2025, 1, 4, tzinfo=UTC)
    finally:
        con.disconnect()


def test_cutoff_before_anchor_is_not_emitted_as_complete_followup():
    from terrine.estimate.survival import kaplan_meier

    con, model = _model()
    try:
        design = Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2024, 12, 31, tzinfo=UTC),
        )
        result = design.build(model).materialize()
        assert all(row["complete"] is False for row in result.table.to_pylist())
        curve = kaplan_meier(result, duration="duration", event="event", entry="entry_time")
        assert curve.time == ()
    finally:
        con.disconnect()


def test_nanosecond_events_support_coverage_outside_nanosecond_range():
    con, model = _model(outcome_start="1600-01-01", nanosecond_outcomes=True)
    try:
        result = (
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
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
        result = (
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
            .build(model)
            .materialize()
        )
        rows = {row["unit"]: row for row in result.table.to_pylist()}
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
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            )
            .build(model)
            .materialize()
        )
        rows = {row["unit"]: row for row in result.table.to_pylist()}
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
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            )
            .build(model)
            .materialize()
        )
        rows = {row["unit"]: row for row in result.table.to_pylist()}
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
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            )
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
    finally:
        con.disconnect()


def test_wide_entry_coverage_start_is_intersected_before_narrowing():
    con, model = _model(
        nanosecond_outcomes=True,
        entry_start="2500-01-01",
        entry_end="2501-01-01",
    )
    try:
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            )
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
    finally:
        con.disconnect()


def test_wide_exit_coverage_start_is_intersected_before_narrowing():
    con, model = _model(
        nanosecond_outcomes=True,
        exit_start="2500-01-01",
        exit_end="2501-01-01",
    )
    try:
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", exit="closed"),
            )
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
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
        result = (
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
            .build(model)
            .materialize()
        )
        rows = result.table.to_pylist()
        assert rows
        assert all(row["event"] is False for row in rows)
        assert all(row["censor_time"] == datetime(2025, 1, 12, tzinfo=UTC) for row in rows)
    finally:
        con.disconnect()


def test_nanosecond_source_accepts_administrative_cutoff_beyond_ns_range():
    con, model = _model(nanosecond_outcomes=True)
    try:
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn"),
                administrative_cutoff=datetime(2500, 1, 1, tzinfo=UTC),
            )
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
    finally:
        con.disconnect()


def test_nanosecond_event_source_accepts_utc_timestamptz_entry_source():
    con, model = _model(nanosecond_outcomes=True)
    try:
        result = (
            Design(
                "user",
                EventAnchor("signup", which="first"),
                shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
            )
            .build(model)
            .materialize()
        )
        assert result.table.num_rows > 0
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


def test_nanosecond_events_survive_compile_and_estimation_in_any_session_timezone():
    from terrine.estimate.survival import ElapsedTime, kaplan_meier

    con, model = _model(nanosecond_outcomes=True)
    try:
        con.raw_sql("SET TimeZone='America/Los_Angeles'")
        result = (
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
            .build(model)
            .materialize()
        )
        curve = kaplan_meier(result, duration="duration", event="event")
        assert curve.time == (ElapsedTime(0, 1), ElapsedTime(0, 2))
    finally:
        con.disconnect()
    from terrine.estimate.survival import kaplan_meier

    con, model = _model(naive_outcomes=True)
    try:
        result = (
            Design("user", EventAnchor("signup", which="first"), shape=TimeToEvent("churn"))
            .build(model)
            .materialize()
        )
        assert result.table.schema.field("duration").type.tz == "UTC"
        assert kaplan_meier(result, duration="duration", event="event").events
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
        rows = {row["unit"]: row for row in design.build(model).materialize().table.to_pylist()}
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
