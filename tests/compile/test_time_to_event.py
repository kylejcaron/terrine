from datetime import UTC, datetime, timedelta

import ibis
import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, FixedDate, agg
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
    millisecond_outcomes=False,
    unit=None,
):

    con = ibis.duckdb.connect()
    con.raw_sql("""
        CREATE TABLE anchors AS SELECT * FROM (VALUES
          ('u1', TIMESTAMPTZ '2025-01-01 00:00:00+00', TRUE),
          ('u2', TIMESTAMPTZ '2025-01-02 00:00:00+00', FALSE),
          ('u3', TIMESTAMPTZ '2025-01-03 00:00:00+00', FALSE)
        ) AS a(user_id, ts, treated)
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
    elif millisecond_outcomes:
        con.raw_sql("""
            CREATE TABLE outcomes AS SELECT * FROM (VALUES
              ('u1', TIMESTAMP_MS '2025-01-01 00:00:00.001'),
              ('u1', TIMESTAMP_MS '2025-01-03 00:00:00'),
              ('u2', TIMESTAMP_MS '2025-01-02 00:00:00.002'),
              ('u3', TIMESTAMP_MS '2025-01-01 00:00:00')
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
    if unit is not None:
        anchors = con.table("anchors")
        con.create_table("users", anchors.filter(anchors.user_id == unit).select(id="user_id"))
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
                    "properties": ["treated"],
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
            "properties": {
                "treated": {
                    "source": "anchors",
                    "column": "treated",
                    "dtype": "bool",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {},
            "metrics": {},
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")} if unit is not None else {},
        readiness={
            "anchors": Coverage("2025-01-01", anchor_end),
            "outcomes": Coverage(outcome_start, outcome_end),
            "entries": Coverage(entry_start, entry_end),
            "exits": Coverage(exit_start, exit_end),
        },
    )
    return con, model


def _collect(design, model, *, columns=None, monkeypatch=None):
    if monkeypatch is not None:
        from dataclasses import replace

        from terrine.compile.columns import compile_columns

        base = compile_columns(Design(design.unit, design.anchor), model)
        candidates = model.con.create_table(
            "survival_candidates", base.table.select("unit", "anchor_id", "t0")
        )
        prepared = replace(base, table=candidates)
        monkeypatch.setattr(
            "terrine.compile.shapes.time_to_event.compile_columns", lambda *_args: prepared
        )
    frame = design.build(model)
    compiled = frame._compiled
    compiled.validate(execute=model.con.to_pyarrow)
    output = compiled.table if columns is None else compiled.table.select(*columns)
    return frame._normalize_output(model.con.to_pyarrow(output))


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


def test_internal_timestamp_aliases_are_reserved_feature_names():
    con, model = _model()
    try:
        with pytest.raises(TerrineError) as error:
            Design(
                "user",
                EventAnchor("signup", which="first"),
                features={"_t0_naive": agg("signup", window=("-1d", "0d"))},
                shape=TimeToEvent("churn"),
            ).build(model)
        assert error.value.code == "terrine.survival.invalid_compilation"
        assert error.value.context["reason"].startswith("generated_column_collision_")
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
