"""Known answers for historical cross-engine regressions, independent of DuckDB output."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

import terrine as tr
from integration.conftest import force_generic_dialect
from integration.warehouse_execution.bigquery_fixtures import (
    bigquery_scratch as bigquery_scratch,  # noqa: F401
)
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from integration.warehouse_execution.snowflake_fixtures import (
    snowflake_scratch as snowflake_scratch,  # noqa: F401
)
from terrine.backends.dialect import current_dialect, dialect_scope
from terrine.backends.extrema import greatest, least
from terrine.backends.numeric import quantized_float
from terrine.backends.registry import ENGINES
from terrine.backends.timestamps import day_start, utc_instant, utc_wall_clock
from terrine.errors import TerrineError
from terrine.semantic.models import BoundDimension, BoundValidity, PropertyBinding, TableRef
from terrine.spec import EventSequence, TimeToEvent
from terrine.temporal.attributes import attribute_asof, validate_history
from terrine.windows import Window
from tests._float_bound import window_sum_bound

_ENGINE_FIXTURES = {
    "duckdb": "duckdb_connection",
    "postgres": "postgres_scratch",
    "bigquery": "bigquery_scratch",
    "snowflake": "snowflake_scratch",
}
_LIVE_MARKERS = {
    "postgres": pytest.mark.postgres,
    "bigquery": pytest.mark.bigquery,
    "snowflake": pytest.mark.snowflake,
}
_ENGINES = tuple(pytest.param(name, marks=_LIVE_MARKERS.get(name, ()), id=name) for name in ENGINES)
_TIME_ZONE_SETUP = {
    "duckdb": "SET TimeZone = 'America/New_York'",
    "postgres": "SET TimeZone = 'America/New_York'",
    # BigQuery has no session time zone: TIMESTAMP is an instant and DATETIME is naive.
    "bigquery": None,
    # Non-UTC: terrine.execution.snapshot_unavailable; test_non_utc_session_refuses.
    "snowflake": "ALTER SESSION SET TIMEZONE = 'UTC'",
}
_ORIGIN = datetime(2025, 1, 1, tzinfo=UTC)
_BUDGET = tr.Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)


@pytest.fixture
def duckdb_connection():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


@pytest.fixture(params=_ENGINES)
def engine(request, monkeypatch):
    """Keep engine selection separate from each engine-independent known answer.

    ``--generic-dialect`` forces the portable base expression policy for every
    engine while the real connection, provider and SQL dialect stay native.
    """
    scratch = request.getfixturevalue(_ENGINE_FIXTURES[request.param])
    if request.config.getoption("--generic-dialect"):
        force_generic_dialect(monkeypatch)
    return scratch if request.param == "duckdb" else scratch.con


def _name(con, name):
    return normalize_identifiers(exp.to_identifier(name), dialect=con.name).name


def _model(con, events, *, exact=True):
    """A one-unit model whose float sums use the requested arithmetic.

    ``events`` is an Arrow table or an Ibis expression over ``con`` (built
    server side when the history is large).
    """
    columns = events.columns if isinstance(events, ibis.Table) else events.column_names
    con.create_table(
        _name(con, "users"),
        pa.table(
            {
                "id": ["u"],
                "observed_until": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            }
        ),
    )
    con.create_table(_name(con, "events"), events)
    return tr.Model.from_tables(
        con,
        units={"user": tr.UnitSource(_name(con, "users"), "id", observed_until="observed_until")},
        events={
            "events": tr.Events(
                _name(con, "events"),
                time="ts",
                key="user_id",
                metrics={
                    "events": tr.count(),
                    # The float known answers state each arithmetic mode's contract.
                    **{
                        name: tr.sum(name, exact=exact)
                        for name in ("amount", "weight")
                        if name in columns
                    },
                },
                complete=("2024-12-01", "2025-01-31"),
            )
        },
        event_ids={"events": "event_id"},
    )


_MODES = pytest.mark.parametrize("exact", [False, True], ids=["native", "exact"])


def _rows(model, design):
    return design.build(model).materialize(budget=_BUDGET).table.to_pylist()


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


def test_every_registered_engine_has_a_conformance_connection():
    assert set(_ENGINE_FIXTURES) == set(ENGINES)


def test_forced_generic_selection_reaches_nested_literals(engine, request):
    """Under ``--generic-dialect`` even nested literal-only scopes quantize to scale 9."""
    request.config.getoption("--generic-dialect")
    outer = current_dialect().name
    with dialect_scope(engine.name), dialect_scope(engine.name.upper()):
        # Text through DOUBLE: Snowflake types a bare decimal literal as an exact
        # NUMBER, and Ibis elides a float64 cast of a float64 literal.
        value = ibis.literal("0.1234567899").cast(dt.float64)
        values = ibis.literal(0).as_table().select(value=value)
        exact, nonfinite, unrepresentable = quantized_float(values.value)
        (row,) = engine.to_pyarrow(
            values.select(
                state=exact.cast("string"), nonfinite=nonfinite, unrepresentable=unrepresentable
            )
        ).to_pylist()
    assert current_dialect().name == outer
    assert (row["nonfinite"], row["unrepresentable"]) == (False, False)
    assert Decimal(row["state"]) == Decimal("0.123456790")


def test_schedule_whole_days_advance_by_every(engine):
    """Catch whole-day intervals zeroed by EXTRACT(SECOND) during interval multiplication."""
    model = _model(engine, _events([_ORIGIN], ["event"]))
    rows = _rows(model, tr.Design("user", tr.Schedule("2d", "2025-01-01", "2025-01-06")))
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


@_MODES
def test_window_sums_preserve_small_values_after_large_history(engine, exact):
    """Selected day/intraday inputs ignore large excluded history in either mode."""
    history, window = [1e17, -3e16], [0.1, 0.2, 0.0, 0.3]
    events = _events(
        [datetime(2024, 12, day, tzinfo=UTC) for day in (20, 21)]
        + [_ORIGIN + timedelta(hours=hour) for hour in (12, 13, 14)]
        + [_ORIGIN - timedelta(hours=4)],
        ["history-a", "history-b", "a", "b", "c", "intraday-only"],
    )
    events = events.append_column("weight", pa.array(history + window))
    events = events.append_column(
        "amount",
        pa.array(
            [
                Decimal(value)
                for value in (
                    "0.00",
                    "0.00",
                    "9007199254740993.01",
                    "-9007199254740993.00",
                    "0.02",
                    "0.00",
                )
            ],
            type=pa.decimal128(20, 2),
        ),
    )
    model = _model(engine, events, exact=exact)
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={
            "weight": tr.agg("weight", window=("-1d", "0s")),
            "intraday": tr.agg("weight", window=("-36h", "0s")),
            "amount": tr.agg("amount", window=("-1d", "0s")),
        },
    )
    (row,) = _rows(model, design)
    # Independent answers: day 0.1 + 0.2; intraday also includes 0.3.
    for name, selected in (("weight", window[:3]), ("intraday", window)):
        if exact:
            assert abs(Fraction(row[name]) - sum(map(Fraction, selected))) <= window_sum_bound(
                selected
            )
        else:
            expected = 0.3 if name == "weight" else 0.6
            assert row[name] == pytest.approx(expected, rel=1e-15, abs=0)
        assert (row[f"{name}__complete"], row[f"{name}__reason"]) == (True, None), name
    assert row["amount"] == Decimal("0.03")
    assert (row["amount__complete"], row["amount__reason"]) == (True, None)


@_MODES
def test_bounded_accumulation_refuses_exact_overflow(engine, exact):
    """Withhold scale-9 coefficients before a bounded accumulator can overflow; the
    same magnitudes are far inside the native binary64 range and publish."""
    with dialect_scope(engine.name):
        if exact and not current_dialect().bounds_accumulations:
            pytest.skip("The native coefficient domain admits this sum.")  # ty: ignore[too-many-positional-arguments]
    values = [9e28, 9e28]
    events = _events(
        [_ORIGIN + timedelta(hours=hour) for hour in (11, 12)], ["first", "second"]
    ).append_column("weight", pa.array(values, pa.float64()))
    model = _model(engine, events, exact=exact)
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={"weight": tr.agg("weight", window=("-1d", "0s"))},
    )
    if not exact:
        (row,) = _rows(model, design)
        assert row["weight"] == pytest.approx(1.8e29, rel=1e-15, abs=0)
        return
    materialized = []
    with pytest.raises(TerrineError) as caught:
        materialized.append(design.build(model).materialize(budget=_BUDGET))
    assert caught.value.code == "terrine.temporal.exact_overflow"
    assert materialized == []


@_MODES
def test_sub_resolution_values_sum_natively_and_the_exact_path_notes_their_loss(engine, exact):
    """Native inputs retain sub-nano values; exact inputs note per-input loss
    without withdrawing completeness."""
    values = [1e-10, 2e-10, 4e-10]
    events = _events(
        [_ORIGIN + timedelta(hours=hour) for hour in (12, 13, 14)], ["a", "b", "c"]
    ).append_column("weight", pa.array(values))
    model = _model(engine, events, exact=exact)
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={"weight": tr.agg("weight", window=("-1d", "0s"))},
    )
    (row,) = _rows(model, design)
    assert row["weight__complete"] is True
    if exact:
        assert (row["weight"], row["weight__reason"]) == (0.0, "below_resolution")
        return
    assert row["weight"] == pytest.approx(7e-10, rel=1e-15)
    assert row["weight__reason"] is None


def test_utc_conversions_ignore_session_time_zone(engine):
    """Catch naive source timestamps interpreted in the session zone instead of UTC."""
    statement = _TIME_ZONE_SETUP[engine.name]
    if statement is not None:
        with engine._safe_raw_sql(statement):
            pass
    instant = datetime(2025, 3, 9, 6, 30, tzinfo=UTC)
    table = engine.create_table(
        "instants", pa.table({"aware": [instant], "naive": [instant.replace(tzinfo=None)]})
    )
    (row,) = engine.to_pyarrow(
        table.select(wall=utc_wall_clock(table.aware), instant=utc_instant(table.naive))
    ).to_pylist()
    assert row == {"wall": instant.replace(tzinfo=None), "instant": instant}


def test_naive_day_start_ignores_session_time_zone(engine):
    """Catch a naive UTC wall clock re-read in the session zone: the start of its
    day, and a day-aligned feature over naive source events, must not shift."""
    statement = _TIME_ZONE_SETUP[engine.name]
    if statement is not None:
        with engine._safe_raw_sql(statement):
            pass
    midnight = datetime(2025, 1, 1)
    instants = [midnight - timedelta(minutes=30), midnight, midnight + timedelta(hours=12)]
    table = engine.create_table(
        "naive_instants", pa.table({"naive": pa.array(instants, pa.timestamp("us"))})
    )
    rows = engine.to_pyarrow(
        table.select(
            "naive", wall=utc_wall_clock(table.naive), start=day_start(table.naive, 0)
        ).order_by("naive")
    ).to_pylist()
    assert [(row["wall"], row["start"]) for row in rows] == [
        (instant, datetime(instant.year, instant.month, instant.day, tzinfo=UTC))
        for instant in instants
    ]
    events = pa.table(
        {
            "user_id": ["u"] * 3,
            "ts": pa.array(instants, pa.timestamp("us")),
            "event_id": ["a", "b", "c"],
        }
    )
    model = _model(engine, events)
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-01"),
        features={"before": tr.agg("events", window=("-1d", "0s"))},
        outcomes={"after": tr.agg("events", window=("0s", "1d"))},
    )
    (row,) = _rows(model, design)
    assert (row["before"], row["after"]) == (1, 2)


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
            shape=EventSequence("events", Window("0s", "2h"), max_events=5),
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
        model, tr.Design("user", tr.FixedDate("2025-01-01"), shape=TimeToEvent("events"))
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
        unit="account",
        key="account_id",
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


def test_null_safe_keys_preserve_nulls_and_duplicate_matches(engine):
    from terrine.backends.joins import null_safe_equal

    left = engine.create_table(
        "left_keys",
        pa.table(
            {
                "left_id": list(range(6)),
                "unit": [None, 0, 1, 1, 0, 1],
                "anchor": [None, "", "a", "a", None, ""],
            }
        ),
    )
    right = engine.create_table(
        "right_keys",
        pa.table(
            {
                "right_id": list(range(6)),
                "right_unit": [None, 0, 1, 1, 0, 1],
                "right_anchor": [None, "", "a", "a", None, ""],
            }
        ),
    )
    joined = left.left_join(
        right,
        [
            null_safe_equal(left.unit, right.right_unit),
            null_safe_equal(left.anchor, right.right_anchor),
        ],
    )
    rows = engine.to_pyarrow(joined.select("left_id", "right_id")).to_pylist()
    assert sorted((row["left_id"], row["right_id"]) for row in rows) == [
        (0, 0),
        (1, 1),
        (2, 2),
        (2, 3),
        (3, 2),
        (3, 3),
        (4, 4),
        (5, 5),
    ]
