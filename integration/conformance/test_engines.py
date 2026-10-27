"""Known answers for historical cross-engine regressions, independent of DuckDB output."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.backends.extrema import greatest, least
from terrine.backends.timestamps import utc_instant, utc_wall_clock
from terrine.semantic.models import BoundDimension, BoundValidity, PropertyBinding, TableRef
from terrine.temporal.attributes import attribute_asof, validate_history

_ENGINES = ("duckdb", pytest.param("postgres", marks=pytest.mark.postgres))
_ORIGIN = datetime(2025, 1, 1, tzinfo=UTC)
_BUDGET = tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)


@pytest.fixture(params=_ENGINES)
def engine(request):
    """Keep engine selection separate from each engine-independent known answer."""
    if request.param == "duckdb":
        con = ibis.duckdb.connect()
        try:
            yield con
        finally:
            con.disconnect()
    else:
        yield request.getfixturevalue("postgres_scratch").con


def _model(con, events):
    con.create_table(
        "users",
        pa.table(
            {
                "id": ["u"],
                "observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            }
        ),
    )
    con.create_table("events", events)
    return tr.Model.from_tables(
        con,
        units={"user": tr.UnitSource("users", "id", observed_until="observed_until")},
        events={
            "events": tr.Events(
                "events",
                keys={"user": "user_id"},
                time="ts",
                values=tuple(name for name in ("amount", "weight") if name in events.column_names),
                complete=("2024-12-01", "2025-01-31"),
            )
        },
        event_ids={"events": "event_id"},
    )


def _rows(model, design):
    return design.build(model).materialize(_BUDGET).table.to_pylist()


def _events(times, ids):
    return pa.table(
        {"user_id": ["u"] * len(ids), "ts": times, "event_id": ids},
        schema=pa.schema(
            [
                ("user_id", pa.string()),
                ("ts", pa.timestamp("us", tz="UTC")),
                ("event_id", pa.string()),
            ]
        ),
    )


def test_schedule_whole_days_advance_by_every(engine):
    """Catch whole-day intervals zeroed by EXTRACT(SECOND) during interval multiplication."""
    model = _model(engine, _events([_ORIGIN], ["event"]))
    rows = _rows(model, tr.Design("user", tr.Schedule("2d", "2025-01-01", "2025-01-07")))
    assert sorted(row["t0"] for row in rows) == [_ORIGIN + timedelta(days=day) for day in (0, 2, 4)]


def test_extrema_ignore_null_arguments(engine):
    """Catch BigQuery-style NULL propagation instead of the non-NULL extreme."""
    table = engine.create_table(
        "extremes",
        pa.table(
            {
                "case": [0, 1, 2, 3],
                "a": [None, 2, 2, None],
                "b": [2, None, 5, None],
                "c": [5, 5, None, None],
            }
        ),
    )
    rows = engine.to_pyarrow(
        table.select(
            "case", low=least(table.a, table.b, table.c), high=greatest(table.a, table.b, table.c)
        ).order_by("case")
    ).to_pylist()
    assert [(row["low"], row["high"]) for row in rows] == [(2, 5)] * 3 + [(None, None)]


def test_window_sums_preserve_small_values_after_large_history(engine):
    """Catch prefix cancellation and decimal sums rounded through binary64."""
    events = _events(
        [datetime(2024, 12, day, tzinfo=UTC) for day in (20, 21)]
        + [_ORIGIN + timedelta(hours=hour) for hour in (12, 13, 14)],
        ["history-a", "history-b", "a", "b", "c"],
    )
    events = events.append_column("weight", pa.array([1e17, -3e16, 0.1, 0.2, 0.0]))
    events = events.append_column(
        "amount",
        pa.array(
            [
                Decimal(value)
                for value in ("0.00", "0.00", "9007199254740993.01", "-9007199254740993.00", "0.02")
            ],
            type=pa.decimal128(20, 2),
        ),
    )
    model = _model(engine, events)
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={
            "weight": tr.agg("events_weight_sum", window=("-1d", "0s")),
            "amount": tr.agg("events_amount_sum", window=("-1d", "0s")),
        },
    )
    (row,) = _rows(model, design)
    # Independent window oracles: 0.1 + 0.2 and exact decimal coefficient addition.
    assert row["weight"] == pytest.approx(0.3, rel=1e-15, abs=0.0)
    assert row["amount"] == Decimal("0.03")


def test_utc_conversions_ignore_session_time_zone(engine):
    """Catch naive source timestamps interpreted in the session zone instead of UTC."""
    engine.raw_sql("SET TimeZone = 'America/New_York'")
    instant = datetime(2025, 3, 9, 6, 30, tzinfo=UTC)
    table = engine.create_table(
        "instants", pa.table({"aware": [instant], "naive": [instant.replace(tzinfo=None)]})
    )
    (row,) = engine.to_pyarrow(
        table.select(wall=utc_wall_clock(table.aware), instant=utc_instant(table.naive))
    ).to_pylist()
    assert row == {"wall": instant.replace(tzinfo=None), "instant": instant}


def test_sequence_uses_configured_event_id_for_timestamp_ties(engine):
    """Catch nondeterministic or reversed ordering of equal-timestamp events."""
    model = _model(
        engine, _events([_ORIGIN, _ORIGIN, _ORIGIN + timedelta(hours=1)], ["z", "a", "end"])
    )
    rows = _rows(
        model,
        tr.Design(
            "user",
            tr.EventAnchor("events"),
            shape=tr.EventSequence("events", tr.Window("0s", "2h"), max_events=5),
        ),
    )
    children = sorted(
        (row for row in rows if row["is_event"]), key=lambda row: row["sequence_position"]
    )
    assert [row["event_id"] for row in children] == ["a", "z", "end"]


def test_censor_time_survives_null_observed_until(engine):
    """Catch a NULL roster observation bound erasing the source-coverage censor time."""
    model = _model(engine, _events([], []))
    (row,) = _rows(
        model, tr.Design("user", tr.FixedDate("2025-01-01"), shape=tr.TimeToEvent("events"))
    )
    assert row["event_time"] is None
    assert row["censor_time"] == datetime(2025, 2, 1, tzinfo=UTC)
    assert row["duration"] == timedelta(days=31)
    assert row["duration__complete"] is True
    assert row["censor_reason"] == "source_coverage"


def test_asof_at_t0_uses_previous_change(engine):
    """Catch an inclusive as-of cutoff leaking a change made exactly at t0."""
    points = engine.create_table(
        "points", pa.table({"point_id": [7], "account": ["a"], "cutoff": [_ORIGIN]})
    )
    history = engine.create_table(
        "tier_history",
        pa.table(
            {
                "account_id": ["a", "a"],
                "changed": [_ORIGIN - timedelta(days=1), _ORIGIN],
                "tier_value": ["silver", "gold"],
            }
        ),
    )
    dimension = BoundDimension(
        name="account_tier",
        relation=TableRef(kind="table", name="tier_history"),
        source_entity="account",
        entity_column="account_id",
        validity=BoundValidity(None, None, "changed"),
        properties={
            "tier": PropertyBinding("tier", "tier_value", "string", "pre_exposure", "account_tier")
        },
    )
    validate_history(history, dimension, execute=engine.to_pyarrow)
    (row,) = engine.to_pyarrow(
        attribute_asof(
            points, history, dimension, "tier", unit_column="account", cutoff_column="cutoff"
        )
    ).to_pylist()
    assert row["point_id"] == 7
    assert row["__terrine_attribute_value"] == "silver"
    assert row["__terrine_attribute_found"] is True
