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
            "schema_version": 3,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "anchors": {
                    "relation": {"kind": "table", "name": "anchors"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["signup"],
                    "properties": ["treated"],
                    "dimensions": [],
                },
                "outcomes": {
                    "relation": {"kind": "table", "name": "outcomes"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["churn"],
                    "properties": [],
                    "dimensions": [],
                },
                "entries": {
                    "relation": {"kind": "table", "name": "entries"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["enrolled"],
                    "properties": [],
                    "dimensions": [],
                },
                "exits": {
                    "relation": {"kind": "table", "name": "exits"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["closed"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "events": {
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
                EventAnchor("signup", occurrence="first"),
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
                EventAnchor("signup", occurrence="first"),
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
            EventAnchor("signup", occurrence="first"),
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
            EventAnchor("signup", occurrence="first"),
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
    duration = pa.duration("us")
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": pa.array([origin], type=timestamp),
            "duration": pa.array([timedelta(hours=18)], type=duration),
            "entry_time": pa.array([origin + timedelta(hours=12)], type=timestamp),
            "event": pa.array([True]),
            "duration__complete": pa.array([False]),
            "duration__reason": pa.array(["incomplete_initial_history"]),
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


def test_life_table_row_budget_sums_distinct_group_bins():
    import pyarrow as pa

    from terrine.compile.shapes.life_table import aggregate_life_table

    timestamp = pa.timestamp("us", tz="UTC")
    duration = pa.duration("us")
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    records = pa.table(
        {
            "t0": pa.array([origin] * 3, type=timestamp),
            "duration": pa.array([timedelta(days=days) for days in (1, 0, 2)], type=duration),
            "entry_time": pa.array([origin] * 3, type=timestamp),
            "event": pa.array([True] * 3),
            "censor_reason": pa.array([None] * 3, type=pa.string()),
            "region": ["north", "north", "south"],
        }
    )
    with pytest.raises(TerrineError) as error:
        aggregate_life_table(records, interval_seconds=86_400, group_by=["region"], max_rows=4)
    assert error.value.code == "terrine.budget.rows_exceeded"
    assert error.value.context["limit"] == 4
    at_limit = aggregate_life_table(
        records, interval_seconds=86_400, group_by=["region"], max_rows=5
    )
    assert at_limit.num_rows == 5


def test_life_table_matches_grouped_interval_intersection_oracle():
    import pyarrow as pa

    from terrine.compile.shapes.life_table import _LifeTableAccumulator, aggregate_life_table

    second = 1_000_000_000
    timestamp = pa.timestamp("ns", tz="UTC")
    bound_type = pa.timestamp("s", tz="UTC")
    entry = [2 * second, second - 1, second, 2 * second, 0, 0, 5 * second, 0, 0]
    terminal = [
        2 * second, second - 1, second, 3 * second, 3 * second, None, 3 * second, second, -1
    ]
    regions = [None, "north", "north", None, "north", None, None, "north", "north"]
    duration_reasons = [
        None,
        None,
        None,
        "incomplete_initial_history",
        None,
        "missing_history",
        "no_eligible_followup",
        None,
        None,
    ]
    records = pa.table(
        {
            "t0": pa.array([0] * len(regions), type=timestamp),
            "duration": pa.array(terminal, type=pa.duration("ns")),
            "entry_time": pa.array(entry, type=timestamp),
            "event": [False, True, False, True, True, False, False, False, False],
            "duration__complete": [reason is None for reason in duration_reasons],
            "duration__reason": duration_reasons,
            "censor_reason": [
                "exit",
                None,
                "administrative_cutoff",
                None,
                None,
                None,
                None,
                "administrative_cutoff",
                "invalid_record",
            ],
            "region": regions,
            "region__complete": [False, True, True, False, True, False, True, True, False],
            "region__reason": [
                "source_missing",
                None,
                None,
                "coverage_gap",
                None,
                "invalid_reason",
                "coverage_gap",
                None,
                "invalid_reason",
            ],
            "bound_start": pa.array([21, 11, 12, 20, 13, 0, None, 12, 0], bound_type),
            "bound_end": pa.array([None, 30, None, None, 31, 99, None, 31, 99], bound_type),
        }
    )
    group_supports = {
        "region__complete": "all",
        "region__reason": "reasons",
        "bound_start": "min",
        "bound_end": "max",
    }
    result = aggregate_life_table(
        records, interval_seconds=1, group_by=["region"], group_supports=group_supports
    )
    empty = _LifeTableAccumulator(
        records.schema, interval_seconds=1, group_by=["region"], group_supports=group_supports
    ).finish()
    assert empty.num_rows == 0
    assert empty.schema.equals(result.schema, check_metadata=True)
    accumulator = _LifeTableAccumulator(
        records.schema, interval_seconds=1, group_by=["region"], group_supports=group_supports
    )
    reversed_records = records.take(list(reversed(range(records.num_rows))))
    batches = reversed_records.to_batches(max_chunksize=3)
    accumulator.consume(batches[0].slice(0, 0))
    for batch in batches:
        accumulator.consume(batch)
    batched = accumulator.finish()
    assert batched.schema.equals(result.schema, check_metadata=True)
    assert batched.equals(result)
    groups = {
        "north": (True, None, 11, 31, []),
        None: (
            False,
            "coverage_gap;source_missing",
            20,
            None,
            ["incomplete_initial_history", "missing_history", "no_eligible_followup"],
        ),
    }
    terminations = {
        ("north", 0): (1, []),
        ("north", 1): (0, [("administrative_cutoff", 2)]),
        ("north", 3): (1, []),
        (None, 2): (0, [("exit", 1)]),
        (None, 3): (0, [("incomplete_initial_history", 1), ("no_eligible_followup", 1)]),
    }
    delayed_entries = {("north", 0): 1, ("north", 1): 1, (None, 2): 2}
    order = [(unit, region, b) for unit, region in enumerate(groups) for b in range(4)]
    rows = result.to_pylist()
    assert [(row["unit"], row["region"], row["time_bin"]) for row in rows] == order
    for row, (_, region, time_bin) in zip(rows, order, strict=True):
        lower = time_bin * second
        upper = (time_bin + 1) * second - 1
        # Intersect closed integer timestamp intervals, without using bin deltas.
        risk = sum(
            max(lower, entered) <= min(upper, stopped)
            for group, entered, stopped in zip(regions, entry, terminal, strict=True)
            if group == region and stopped is not None
        )
        events, censor_reasons = terminations.get((region, time_bin), (0, []))
        censored = sum(count for _, count in censor_reasons)
        complete, reason, start, end, reasons = groups[region]
        assert row["anchor_id"] == row["t0"] == time_bin
        assert row["region__complete"] is complete
        assert row["region__reason"] == reason
        assert row["bound_start"] == datetime.fromtimestamp(start, tz=UTC)
        assert row["bound_end"] == (None if end is None else datetime.fromtimestamp(end, tz=UTC))
        for name, value in {
            "at_risk": risk,
            "events": events,
            "censored": censored,
            "censored_by_reason": censor_reasons,
            "delayed_entries": delayed_entries.get((region, time_bin), 0),
            "event_rate": pytest.approx(events / risk) if risk else None,
            "censor_rate": pytest.approx(censored / risk) if risk else None,
        }.items():
            assert row[name] == value
            assert row[f"{name}__complete"] is (not reasons)
            assert row[f"{name}__reason"] == reasons


def test_duration_is_elapsed_from_t0_for_event_and_censored_rows():
    import pyarrow as pa

    con, model = _model(outcome_end="2025-01-23")
    try:
        con.raw_sql("DELETE FROM outcomes")
        con.raw_sql("INSERT INTO outcomes VALUES ('u1', TIMESTAMPTZ '2025-01-11 00:00:00+00')")
        design = Design(
            "user",
            EventAnchor("signup", occurrence="first"),
            shape=TimeToEvent("churn"),
            administrative_cutoff=datetime(2025, 1, 23, tzinfo=UTC),
        )
        result = _collect(design, model)
        rows = {row["unit"]: row for row in result.to_pylist()}
        assert rows["u1"]["duration"] == timedelta(days=10)
        assert rows["u2"]["duration"] == timedelta(days=21)
        assert result.schema.field("duration").type == pa.duration("us")
    finally:
        con.disconnect()


def test_duration_with_delayed_entry_is_measured_from_t0():
    import pyarrow as pa

    con, model = _model(outcome_end="2025-01-22")
    try:
        con.raw_sql("DELETE FROM outcomes")
        con.raw_sql("INSERT INTO outcomes VALUES ('u1', TIMESTAMPTZ '2025-01-11 00:00:00+00')")
        con.raw_sql(
            "UPDATE entries SET ts = TIMESTAMPTZ '2025-01-03 00:00:00+00' WHERE user_id = 'u1'"
        )
        design = Design(
            "user",
            EventAnchor("signup", occurrence="first"),
            shape=TimeToEvent("churn", entry_event="enrolled", delayed_entry=True),
        )
        frame = design.build(model).materialize()
        result = frame.table
        row = next(row for row in result.to_pylist() if row["unit"] == "u1")
        assert row["duration"] == timedelta(days=10)
        assert row["entry_time"] == datetime(2025, 1, 3, tzinfo=UTC)
        from terrine.export import to_lifelines_km

        exported = to_lifelines_km(frame)
        index = result["unit"].to_pylist().index("u1")
        assert exported.entry is not None
        assert exported.entry[index] == 2 * 86_400
        assert pa.types.is_duration(result.schema.field("duration").type)
    finally:
        con.disconnect()


def test_nanosecond_duration_keeps_submicrosecond_elapsed():
    import pyarrow as pa

    con, model = _model(nanosecond_outcomes=True)
    try:
        con.raw_sql("DELETE FROM outcomes")
        con.raw_sql(
            "INSERT INTO outcomes VALUES ('u1', TIMESTAMP_NS '2025-01-02 00:00:00.000000001')"
        )
        design = Design(
            "user", EventAnchor("signup", occurrence="first"), shape=TimeToEvent("churn")
        )
        frame = design.build(model).materialize()
        result = frame.table
        row_index = result["unit"].to_pylist().index("u1")
        row = result.to_pylist()[row_index]
        assert result.schema.field("duration").type == pa.duration("ns")
        assert row["duration"].value == 86_400_000_000_001
        from terrine.export import to_lifelines_km

        exported = to_lifelines_km(frame)
        legacy_duration = (
            result["event_time"]
            .cast(pa.timestamp("ns", tz="UTC"))
            .cast(pa.int64())[row_index]
            .as_py()
            - result["t0"].cast(pa.timestamp("ns", tz="UTC")).cast(pa.int64())[row_index].as_py()
        ) / 1e9
        assert exported.durations[row_index] == legacy_duration
    finally:
        con.disconnect()
