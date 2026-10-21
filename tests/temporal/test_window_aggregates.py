from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import pyarrow as pa
import pytest

from terrine.data import BoundCoverage
from terrine.errors import TerrineError
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
from terrine.temporal.prefix import exact_literal, sparse_prefix
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

_DEFAULT_WINDOW = Window(0, 86400)


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


def _certificate(result, row, exact):
    meta = result.columns["requested"]
    error = Fraction.from_float(row[meta.absolute_error_column])
    returned = Fraction.from_float(row["requested"])
    assert abs(returned - exact) <= error
    assert 0 <= error <= Fraction(1, 2**40) + abs(returned) / 2**30


def test_float_window_certifies_ordinary_history_but_refuses_cancellation(con):
    ordinary = _state(con, [0.1, -0.2, 0.4], name="ordinary")
    accepted = _result(_anchors(con), [ordinary])
    _certificate(accepted, _rows(con, accepted)[0], sum(map(Fraction.from_float, [0.1, -0.2, 0.4])))
    pair = _state(con, [1e16, 1.0], times=[datetime(2025, 1, d, 12, tzinfo=UTC) for d in (1, 2)])
    result = _result(_anchors(con), [pair])
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "tolerance"


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_exact_instants_half_open_boundaries_and_named_results(con, strategy):
    t0 = datetime(2025, 1, 2, 12, tzinfo=UTC)
    pair = _state(
        con,
        [1, 2, 4, 8, 16],
        dtype="int64",
        times=[
            t0 - timedelta(microseconds=1),
            t0,
            t0 + timedelta(microseconds=1),
            t0 + timedelta(hours=1),
            t0 + timedelta(hours=1),
        ],
    )
    anchors = _anchors(con, time=t0.isoformat(), units=("a", "a"))
    pre = _result(anchors, [pair], window=Window(-3600, 0), strategy=strategy, output="pre")
    post = _result(pre.table, [pair], window=Window(0, 3600), strategy=strategy, output="post")
    rows = _rows(con, post)
    assert len(rows) == 2
    assert [(r["pre"], r["post"], r["t0"]) for r in rows] == [(Decimal(1), Decimal(6), t0)] * 2
    assert post.columns["post"].window == Window(0, 3600)
    assert post.columns["post"].role == "outcome"


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
    if meta.absolute_error_column:
        assert rows[1][meta.absolute_error_column] is None


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


def test_count_and_conversion_ignore_unused_float_certificates(con):
    pair = _state(con, [float("inf")])
    conversion = _result(_anchors(con), [pair], kind="conversion")
    assert _rows(con, conversion)[0]["requested"] is True
    assert conversion.columns["requested"].numeric_policy is None
    assert conversion.columns["requested"].absolute_error_column is None
    measure, state = pair
    count = replace(measure, aggregation="count")
    state = state.mutate(event_count=exact_literal(2**63))
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [(count, state)]))
    assert error.value.code == "terrine.temporal.exact_overflow"


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
def test_deferred_scalar_checks_fail_closed(con, bad):
    pair = _state(con, [1.0])
    result = _result(_anchors(con), [pair])

    def malformed(query):
        if bad is None:
            return None
        columns = {name: pa.array(["0"] * len(bad)) for name in query.columns}
        columns[query.columns[0]] = bad
        return pa.table(columns)

    with pytest.raises((TypeError, ValueError)):
        result.validate(malformed)


def test_integer_sum_widens_without_losing_units_above_binary64_precision(con):
    pair = _state(con, [2**63 - 1, 2**63 - 1], dtype="int64")
    result = _result(_anchors(con), [pair])
    row = _rows(con, result)[0]
    assert row["requested"] == Decimal(2**64 - 2)
    assert result.columns["requested"].numeric_policy == "exact"


def test_all_null_ratio_numerator_is_not_a_zero_denominator(con):
    numerator = _state(con, [None], dtype="int64", name="numerator")
    denominator = _state(con, [0], dtype="int64", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "all_values_null"
    assert row[result.columns["requested"].absolute_error_column] is None


def test_day_prefixes_cannot_answer_an_intraday_request(con):
    pair = _state(con, [2], dtype="int64", resolution="day")
    result = _result(_anchors(con, time="2025-01-02 12:00:00+00"), [pair], resolution="day")
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.unaligned_day_resolution"


@pytest.mark.parametrize("kind", ["average"])
def test_fractional_outputs_apply_tolerance_in_final_result_units(con, kind):
    numerator = _state(
        con,
        [1.0, -1.0] * 100,
        aggregation="avg_event" if kind == "average" else "sum",
        name="numerator",
    )
    pairs = [numerator]
    if kind == "ratio":
        pairs.append(_state(con, [200], dtype="int64", name="denominator"))
    result = _result(_anchors(con), pairs, kind="ratio" if kind == "ratio" else "mean")
    _certificate(result, _rows(con, result)[0], Fraction(0))


@pytest.mark.parametrize("sign", [1, -1])
def test_quotient_refuses_overflowing_input_interval_even_when_output_is_finite(con, sign):
    numerator = _state(con, [1.0], name="numerator")
    denominator = _state(con, [sign * float.fromhex("0x1.fffffffffffffp+1023")], name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "bound_overflow"


@pytest.mark.parametrize("strategy", ["native", "union_window"])
def test_public_window_preserves_ns_events_around_utc_microsecond_anchor(con, strategy):
    events = con.sql("""
        SELECT 'a' AS user_id, ts::TIMESTAMP_NS AS occurred_at, amount::BIGINT AS amount
        FROM (VALUES
            ('2025-01-02 11:59:59.999999999', 1),
            ('2025-01-02 12:00:00.000000000', 2),
            ('2025-01-02 12:00:00.000000001', 4)
        ) AS e(ts, amount)
    """)
    source = BoundSource(
        "ns_events",
        TableRef(kind="table", name="ns_events"),
        "occurred_at",
        {"user": "user_id"},
        {"revenue": "amount"},
        {},
        (),
        "duckdb",
        "UTC",
    )
    events = normalize_events(events, source, "user_id")
    measure = resolve_measure(
        events, BoundMeasure("revenue", "ns_events", "sum", (), None, None), source=source
    )
    state = sparse_prefix(
        _sparse_group(events, measure, {"_timestamp": events._timestamp}, "user_id", source=source),
        measure,
        resolution="event",
    )
    anchors = _anchors(con, time="2025-01-02 12:00:00+00")
    before = _result(
        anchors, [(measure, state)], kind="mean", window=Window(-1, 0), strategy=strategy
    )
    after = _result(
        anchors, [(measure, state)], kind="mean", window=Window(0, 1), strategy=strategy
    )
    assert _rows(con, before)[0]["requested"] == 1
    assert _rows(con, after)[0]["requested"] == 6


def test_historical_invalid_float_state_is_not_erased_by_a_later_group(con):
    measure, state = _state(
        con,
        [1.0, 2.0],
        times=[
            datetime(2025, 1, 1, 12, tzinfo=UTC),
            datetime(2025, 1, 2, 12, tzinfo=UTC),
        ],
    )
    state = state.mutate(float64_nonfinite_state=ibis.literal(True))
    result = _result(_anchors(con), [(measure, state)])
    with pytest.raises(TerrineError) as error:
        _rows(con, result)
    assert error.value.context["failure"] == "nonfinite_state"
