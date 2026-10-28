from __future__ import annotations

import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import duckdb
import ibis
import pyarrow as pa
import pytest

from terrine.backends.numeric import exact_literal
from terrine.data import BoundCoverage, RowCheck
from terrine.errors import RefusalSpec, TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import (
    BoundFilter,
    BoundMeasure,
    BoundMetric,
    BoundSource,
    PropertyBinding,
    TableRef,
)
from terrine.temporal.daily import _sparse_group, resolve_measure, sparse_daily
from terrine.temporal.prefix import sparse_prefix
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

_DEFAULT_WINDOW = Window(0, 86400)
_REFUSAL = RefusalSpec("terrine.test.failed_rows", "{failed_count} rows failed")


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _state(
    con,
    values,
    *,
    aggregation="sum",
    dtype="float64",
    times=None,
    units=None,
    name="postings",
    resolution="event",
    boundary="UTC",
    active=None,
):
    n = len(values)
    fields = {"user_id": "string", "occurred_at": "timestamp('UTC')", "amount": dtype}
    data = {
        "user_id": units or ["a"] * n,
        "occurred_at": times or [datetime(2025, 1, 2, 12, tzinfo=UTC)] * n,
        "amount": values,
    }
    properties, filters = {}, ()
    if active is not None:
        fields["active"] = "string"
        data["active"] = active
        properties["active"] = PropertyBinding("active", "active", "string", "event_time", None)
        filters = (BoundFilter("active", "equals", ("yes",)),)
    raw = con.create_table(
        name, pa.Table.from_pydict(data, schema=ibis.schema(fields).to_pyarrow()), overwrite=True
    )
    source = BoundSource(
        name,
        TableRef(kind="table", name=name),
        "occurred_at",
        {"user": "user_id"},
        {"revenue": "amount"},
        properties,
        (),
        "duckdb",
        boundary,
    )
    events = normalize_events(raw, source, "user_id")
    measure = resolve_measure(
        events, BoundMeasure("revenue", name, aggregation, filters, None, None), source=source
    )
    grouped = (
        sparse_daily(events, measure, boundary, "user_id", source=source)
        if resolution == "day"
        else _sparse_group(
            events, measure, {"_timestamp": events._timestamp}, "user_id", source=source
        )
    )
    prefix = sparse_prefix(grouped, measure, resolution=resolution)
    return measure, con.create_table(f"{name}_prefix", prefix, overwrite=True)


def _anchors(con, *, time="2025-01-02 00:00:00+00", units=("a",), extra=""):
    values = ",".join(f"('{unit}', '{index}')" for index, unit in enumerate(units))
    return con.sql(
        f"SELECT unit, anchor_id, TIMESTAMPTZ '{time}' AS t0 {extra} "
        f"FROM (VALUES {values}) AS anchors(unit, anchor_id)"
    )


def _result(
    anchors,
    pairs,
    *,
    window=_DEFAULT_WINDOW,
    kind="mean",
    readiness=None,
    boundary="UTC",
    resolution="event",
    strategy="native",
    output="requested",
):
    from terrine.temporal.aggregate import aggregate_window

    measures = tuple(pair[0] for pair in pairs)
    metric = BoundMetric(
        "revenue",
        "user",
        {m.source_name: "user_id" for m in measures},
        kind,
        measures,
        tuple(m.source_name for m in measures),
        (),
    )
    if readiness is None:
        readiness = Readiness(
            {
                m.source_name: BoundCoverage(
                    datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
                )
                for m in measures
            }
        )
    return aggregate_window(
        anchors,
        metric,
        dict(pairs),
        window,
        readiness,
        boundary=boundary,
        output=output,
        role="outcome",
        resolution=resolution,
        strategy=strategy,
    )


def _rows(con, result):
    con.raw_sql("BEGIN TRANSACTION")
    try:
        result.validate(con.to_pyarrow)
        table = con.to_pyarrow(result.table)
        table.validate(full=True)
        return table.to_pylist()
    finally:
        con.raw_sql("ROLLBACK")


def _shortest(values):
    """The exact sum of the values: a float counts as its shortest round-trip decimal."""
    return sum(
        (Fraction(repr(x)) if isinstance(x, float) else Fraction(x) for x in values),
        Fraction(),
    )


def _assert_quotient(returned, numerator: Fraction, denominator: Fraction):
    """``|r − p/q| ≤ 6·2⁻⁵³·|p/q| + 2⁻¹⁰⁷⁵`` with the sign of ``p/q``."""
    exact = numerator / denominator
    error = abs(Fraction(returned) - exact)
    assert error <= 6 * Fraction(1, 2**53) * abs(exact) + Fraction(1, 2**1075)
    assert (returned < 0) == (exact < 0)


def test_long_history_small_window_is_exact(con):
    generator = random.Random(2026)
    history = [generator.uniform(0, 10000) for _ in range(1000)]
    window = [1.25, 2.5, 3.16]
    first = datetime(2025, 1, 1, tzinfo=UTC)
    pair = _state(
        con,
        history + window,
        times=[first + timedelta(seconds=index) for index in range(len(history))]
        + [datetime(2025, 1, 2, 12, tzinfo=UTC)] * len(window),
    )
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    assert row["requested"] == float(_shortest(window)) == 6.91
    meta = result.columns["requested"]
    assert meta.numeric_policy is None


def test_float_window_cancels_history_beyond_binary64_precision(con):
    pair = _state(con, [1e16, 1.0], times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)])
    assert _rows(con, _result(_anchors(con), [pair]))[0]["requested"] == 1.0


def test_tiny_value_does_not_change_other_units(con):
    pair = _state(
        con,
        [1.0, 5e-324, -1.0, 0.1, 0.2],
        units=["tiny", "tiny", "tiny", "other", "other"],
    )
    result = _result(_anchors(con, units=("tiny", "other")), [pair])
    rows = sorted(_rows(con, result), key=lambda row: row["unit"])
    assert [row["requested"] for row in rows] == [0.3, 5e-324]


def test_nonfinite_history_refuses(con):
    pair = _state(
        con,
        [float("nan"), 2.0],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)],
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [pair]))
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "nonfinite_input"
    assert set(error.value.context) == {"failure", "failed_count", "source", "measure", "output"}


def _recording(con):
    executed = []

    def execute(query):
        executed.append(query)
        return con.to_pyarrow(query)

    return executed, execute


def test_validate_runs_one_aggregate_for_every_row_check(con):
    pair = _state(
        con,
        [float("nan"), 2.0],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)],
    )
    result = _result(_anchors(con), [pair])
    assert len(result.row_checks) >= 2
    assert all(check.column in result.table.columns for check in result.row_checks)
    executed, execute = _recording(con)
    with pytest.raises(TerrineError) as error:
        result.validate(execute)
    assert len(executed) == 1
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "nonfinite_input"
    assert error.value.context["failed_count"] == 1


def test_row_check_columns_are_true_only_on_failing_rows(con):
    pair = _state(con, [float("nan"), 2.0], units=["bad", "good"])
    result = _result(_anchors(con, units=("bad", "good")), [pair])
    nonfinite = next(
        check for check in result.row_checks if check.context.get("failure") == "nonfinite_input"
    )
    table = result.table
    rows = con.to_pyarrow(
        table.select("unit", flag=table[nonfinite.column]).order_by("unit")
    ).to_pylist()
    assert rows == [{"unit": "bad", "flag": True}, {"unit": "good", "flag": False}]


@pytest.mark.parametrize(
    "aggregation,kind,expected,reasons",
    [
        ("sum", "mean", [0, None], [None, "all_values_null"]),
        ("count", "mean", [0, 1], [None, None]),
        ("sum", "conversion", [False, True], [None, None]),
        ("avg_event", "mean", [None, None], ["no_observations", "all_values_null"]),
        ("avg_calendar_day", "mean", [0.0, None], [None, "all_values_null"]),
    ],
)
def test_complete_empty_and_all_null_remain_distinct(con, aggregation, kind, expected, reasons):
    pair = _state(con, [None], dtype="float64", aggregation=aggregation, units=["null"])
    result = _result(_anchors(con, units=("empty", "null")), [pair], kind=kind)
    rows = sorted(_rows(con, result), key=lambda r: r["unit"])
    meta = result.columns["requested"]
    assert [r["requested"] for r in rows] == expected
    assert [r[meta.reason_column] for r in rows] == reasons


def test_unit_observation_end_preserves_value_or_exact_unavailable_reason(con):
    pair = _state(con, [2, 2], dtype="int64", units=["complete", "incomplete"])
    anchors = _anchors(con, units=("complete", "incomplete"))
    anchors = anchors.mutate(
        _observed_until=(anchors.unit == "complete").ifelse(
            ibis.timestamp(datetime(2025, 1, 3, tzinfo=UTC)),
            ibis.timestamp(datetime(2025, 1, 2, 12, tzinfo=UTC)),
        )
    )
    result = _result(anchors, [pair])
    meta = result.columns["requested"]
    rows = sorted(_rows(con, result), key=lambda row: row["unit"])
    assert [
        (row["unit"], row["requested"], row[meta.complete_column], row[meta.reason_column])
        for row in rows
    ] == [
        ("complete", Decimal(2), True, None),
        ("incomplete", None, False, "incomplete_observation"),
    ]


@pytest.mark.parametrize(
    "time,window",
    [
        ("2025-01-02 00:00:00+00", Window(0, 3600)),
        ("2025-01-02 12:00:00+00", Window(0, 86400)),
        ("2025-01-02 00:00:00.000001+00", Window(0, 86400)),
        ("2025-01-02 00:00:00+00", Window(1, 86401)),
    ],
)
def test_calendar_domain_precedes_empty_and_unavailable(con, time, window):
    pair = _state(con, [None], aggregation="avg_calendar_day", units=["other"])
    readiness = Readiness(
        {
            "postings": BoundCoverage(
                datetime(2025, 1, 4, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
            )
        }
    )
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con, time=time), [pair], window=window, readiness=readiness))
    assert error.value.code == "terrine.metric.unaligned_calendar_average"


def test_exact_wide_history_retracts_and_retains_decimal_scale(con):
    maximum = Decimal("99999999999999999999999999999999.999999")
    pair = _state(
        con,
        [maximum, maximum, maximum.copy_negate(), Decimal("0.000001")],
        dtype="decimal(38,6)",
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 1, 2, 2)],
    )
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    expected = Decimal("-99999999999999999999999999999999.999998")
    assert row["requested"] == expected
    assert row["requested"].as_tuple().exponent == -6
    assert result.columns["requested"].numeric_policy == "exact"
    overflow = _result(_anchors(con, time="2025-01-01 00:00:00+00"), [pair])
    with pytest.raises(TerrineError) as error:
        _rows(con, overflow)
    assert error.value.code == "terrine.temporal.exact_overflow"


def test_unknown_filter_history_cancels_and_unavailable_suppresses_refusal(con):
    pair = _state(
        con,
        [1, 2],
        dtype="int64",
        active=[None, "yes"],
        times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)],
    )
    assert _rows(con, _result(_anchors(con), [pair]))[0]["requested"] == 2
    anchors = _anchors(con, time="2025-01-01 00:00:00+00")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(anchors, [pair]))
    assert error.value.code == "terrine.filter.unknown"
    unavailable = _result(
        anchors,
        [pair],
        readiness=Readiness(
            {
                "postings": BoundCoverage(
                    datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 1, 5, tzinfo=UTC)
                )
            }
        ),
    )
    assert (
        _rows(con, unavailable)[0][unavailable.columns["requested"].reason_column]
        == "incomplete_coverage"
    )


def test_count_and_conversion_ignore_nonfinite_float_inputs(con):
    pair = _state(con, [float("inf")])
    conversion = _result(_anchors(con), [pair], kind="conversion")
    assert _rows(con, conversion)[0]["requested"] is True
    assert conversion.columns["requested"].numeric_policy is None
    measure, state = pair
    count = replace(measure, aggregation="count")
    state = state.mutate(event_count=exact_literal(2**63))
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [(count, state)]))
    assert error.value.code == "terrine.temporal.exact_overflow"


def test_count_metric_matches_sum_of_ones_and_has_no_unknown_check(con):
    from terrine.semantic.rules import UNKNOWN_FILTER

    measure, state = _state(con, [1.0, 2.0, 3.0], aggregation="count")
    # A count's sufficient state is the event count alone.
    pair = (measure, state.select("_unit", "_timestamp", "event_count"))
    result = _result(_anchors(con), [pair])
    assert _rows(con, result)[0]["requested"] == 3
    assert all(check.refusal is not UNKNOWN_FILTER for check in result.row_checks)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        pa.array([None], type=pa.int64()),
        pa.array([-1]),
        pa.array(["0"]),
        pa.array([0.0]),
        pa.array([0, 0]),
    ],
)
def test_deferred_scalar_checks_fail_closed(con, bad):
    pair = _state(con, [1.0])
    result = _result(_anchors(con), [pair])

    def malformed(query):
        if bad is None:
            return None
        columns = {name: pa.array([0] * len(bad), type=pa.int64()) for name in query.columns}
        columns[query.columns[0]] = bad
        return pa.table(columns)

    with pytest.raises((TypeError, ValueError)):
        result.validate(malformed)


@pytest.mark.parametrize(
    "bad",
    [
        None,
        pa.array([None], type=pa.string()),
        pa.array(["-1"]),
        pa.array(["00"]),
        pa.array([0], type=pa.int64()),
        pa.array(["0", "0"]),
    ],
)
def test_aggregate_string_counts_fail_closed(con, bad):
    from terrine.temporal.aggregate import _Check

    anchors = _anchors(con)
    check = _Check(
        anchors.aggregate(failed_0=anchors.count().cast("string")),
        (RowCheck("failed_0", _REFUSAL, {}),),
    )

    def malformed(query):
        if bad is None:
            return None
        return pa.table({query.columns[0]: bad})

    with pytest.raises((TypeError, ValueError)):
        check.validate(malformed)


def test_integer_sum_widens_without_losing_units_above_binary64_precision(con):
    pair = _state(con, [2**63 - 1, 2**63 - 1], dtype="int64")
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    assert row["requested"] == Decimal(2**64 - 2)
    assert result.columns["requested"].numeric_policy == "exact"


def _count_groups(con, counts):
    """Grouped count states twelve hours apart from 2025-01-01 12:00, as stored HUGEINT."""
    values = ",".join(
        f"(TIMESTAMPTZ '2025-01-01 12:00:00+00' + INTERVAL {index * 12} HOUR, {count})"
        for index, count in enumerate(counts)
    )
    return con.sql(
        "SELECT 'a' AS _unit, ts AS _timestamp, n::HUGEINT AS event_count,"
        " n::HUGEINT AS nonnull_count, n::HUGEINT AS sum_value"
        f" FROM (VALUES {values}) AS s(ts, n)"
    )


def test_count_prefixes_beyond_int64_refuse_instead_of_wrapping(con):
    # Three groups of 2**62 merge to the prefixes 2**62, 2**63 and 3 * 2**62: the
    # window [t0, t0 + 12h) reads the first two and publishes their difference
    # exactly, the windows ending at t0 + 1d read a difference of 2**63 or more.
    measure, _ = _state(con, [1], aggregation="count")
    prefix = sparse_prefix(_count_groups(con, [2**62] * 3), measure, resolution="event")
    pair = (measure, prefix)
    fits = _result(_anchors(con), [pair], window=Window(0, 12 * 3600))
    assert _rows(con, fits)[0]["requested"] == 2**62
    for window in (Window(0, 86400), Window(-86400, 86400)):
        with pytest.raises(TerrineError) as error:
            _rows(con, _result(_anchors(con), [pair], window=window))
        assert error.value.code == "terrine.temporal.exact_overflow"
    # A prefix at the HUGEINT limit still narrows by refusal, never by wrapping,
    # and accumulating past the limit raises.
    prefix = sparse_prefix(_count_groups(con, [0, 2**127 - 1]), measure, resolution="event")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [(measure, prefix)]))
    assert error.value.code == "terrine.temporal.exact_overflow"
    prefix = sparse_prefix(_count_groups(con, [2**127 - 1, 1]), measure, resolution="event")
    with pytest.raises(duckdb.OutOfRangeException):
        _rows(con, _result(_anchors(con), [(measure, prefix)]))


def test_day_prefixes_cannot_answer_an_intraday_request(con):
    pair = _state(con, [2], dtype="int64", resolution="day")
    result = _result(_anchors(con, time="2025-01-02 12:00:00+00"), [pair], resolution="day")
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.unaligned_day_resolution"
