from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import pytest

from terrine.backends.numeric import exact_literal
from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import BoundMeasure, BoundSource, TableRef
from terrine.temporal.daily import _sparse_group, resolve_measure
from terrine.temporal.prefix import sparse_prefix
from terrine.temporal.readiness import Readiness
from terrine.windows import Window
from tests.temporal.test_window_aggregates import (
    _anchors,
    _assert_quotient,
    _result,
    _rows,
    _shortest,
    _state,
)


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


# These exercise native quotient/calendar compilation through DuckDB execution.
def test_ratio_coverage_and_observation_intersect_before_values(con):
    numerator = _state(con, [2], dtype="int64", name="numerator")
    denominator = _state(con, [0], dtype="int64", name="denominator")
    start, end = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)
    readiness = Readiness(
        {
            "numerator": BoundCoverage(start, end),
            "denominator": BoundCoverage(start, end - timedelta(hours=12)),
        }
    )
    anchors = _anchors(con, extra=", TIMESTAMPTZ '2025-01-02 06:00:00+00' AS _observed_from")
    result = _result(anchors, [numerator, denominator], kind="ratio", readiness=readiness)
    row = _rows(con, result)[0]
    meta = result.columns["requested"]
    assert row["requested"] is None
    assert row[meta.reason_column] == "incomplete_coverage"
    assert row[meta.observed_from_column] == datetime(2025, 1, 2, 6, tzinfo=UTC)
    assert row[meta.observed_until_column] == datetime(2025, 1, 2, 12, tzinfo=UTC)
    assert row[meta.complete_column] is False
    result = _result(anchors, [numerator, denominator], kind="ratio")
    assert (
        _rows(con, result)[0][result.columns["requested"].reason_column] == "incomplete_observation"
    )


def test_calendar_nonzero_offset_day_and_event_agree(con):
    time = "2025-01-01 18:30:00+00"
    for resolution in ("day", "event"):
        pair = _state(
            con,
            [2.0, 4.0],
            aggregation="avg_calendar_day",
            resolution=resolution,
            boundary="+05:30",
            times=[datetime(2025, 1, 1, 19, tzinfo=UTC), datetime(2025, 1, 2, 18, tzinfo=UTC)],
        )
        result = _result(_anchors(con, time=time), [pair], boundary="+05:30", resolution=resolution)
        assert _rows(con, result)[0]["requested"] == 6.0


def test_decimal_average_and_ratio_meet_the_quotient_bound(con):
    numerator = _state(
        con,
        [Decimal("0.10"), Decimal("0.20")],
        aggregation="avg_event",
        dtype="decimal(38,2)",
        name="numerator",
    )
    result = _result(_anchors(con), [numerator])
    _assert_quotient(_rows(con, result)[0]["requested"], Fraction(3, 10), Fraction(2))
    denominator = _state(con, [Decimal("0.70")], dtype="decimal(38,2)", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(_rows(con, result)[0]["requested"], Fraction(3, 20), Fraction(7, 10))


def test_float_average_and_its_ratio_divide_the_exact_operands(con):
    numerator = _state(con, [0.1, 0.2, -0.05], aggregation="avg_event", name="numerator")
    average = _rows(con, _result(_anchors(con), [numerator]))[0]["requested"]
    _assert_quotient(average, Fraction(1, 4), Fraction(3))
    denominator = _state(con, [3.0, 4.0], name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(_rows(con, ratio)[0]["requested"], Fraction(1, 4) / 3, Fraction(7))


def test_zero_numerator_ratio_is_exactly_zero(con):
    numerator = _state(con, [1.0, -1.0] * 100, name="numerator")
    denominator = _state(con, [200], dtype="int64", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    assert _rows(con, result)[0]["requested"] == 0.0


def test_exact_zero_denominators_are_a_reason_not_a_refusal(con):
    numerator = _state(con, [2.0], name="numerator")
    for name, values, dtype in (("zero", [0], "int64"), ("cancelled", [1.0, -1.0], "float64")):
        denominator = _state(con, values, dtype=dtype, name=name)
        result = _result(_anchors(con), [numerator, denominator], kind="ratio")
        row = _rows(con, result)[0]
        assert row["requested"] is None
        assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_negative_denominator_ratio_meets_the_bound_and_subnormal_sum_is_exact(con):
    numerator = _state(con, [-0.1, 0.025], name="numerator")
    denominator = _state(con, [-0.7], name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _assert_quotient(_rows(con, result)[0]["requested"], Fraction("-0.075"), Fraction("-0.7"))
    tiny = _state(con, [float.fromhex("0x0.0000000000001p-1022")], name="tiny")
    result = _result(_anchors(con), [tiny])
    assert _rows(con, result)[0]["requested"] == 5e-324


def test_fractional_results_do_not_narrow_internal_exact_states(con):
    maximum = Decimal("99999999999999999999999999999999999999")
    numerator = _state(con, [maximum, maximum], dtype="decimal(38,0)", name="numerator")
    denominator = _state(con, [maximum, maximum], dtype="decimal(38,0)", name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    assert _rows(con, ratio)[0]["requested"] == 1.0
    average_pair = _state(
        con, [maximum, maximum], aggregation="avg_event", dtype="decimal(38,0)", name="average"
    )
    average = _result(_anchors(con), [average_pair])
    _assert_quotient(_rows(con, average)[0]["requested"], Fraction(2 * (10**38 - 1)), Fraction(2))
    count_measure, count_state = _state(con, [1], aggregation="count", dtype="int64", name="counts")
    count_state = count_state.mutate(event_count=exact_literal(2**63))
    count_pair = (count_measure, count_state)
    count_ratio = _result(_anchors(con), [count_pair, count_pair], kind="ratio")
    assert _rows(con, count_ratio)[0]["requested"] == 1.0


@pytest.mark.parametrize(
    "numerator,denominator",
    [
        ([1.25, -3.5, 0.75], None),
        ([1.0, -1.0] * 100, None),
        ([1.25, -3.5], [3]),
        ([1.25, -3.5], [3e-7, 2.5]),
        ([-1.25, -3.5], [-0.7]),
        ([1.0], [float.fromhex("0x1.fffffffffffffp+1023")]),
        ([Decimal("0.10"), Decimal("0.20")], None),
        ([Decimal("0.10"), Decimal("0.20")], [7]),
    ],
)
def test_mixed_scale_ratio_and_average_meet_bound(con, numerator, denominator):
    dtype = "decimal(38,2)" if isinstance(numerator[0], Decimal) else "float64"
    if denominator is None:
        pair = _state(con, numerator, aggregation="avg_event", dtype=dtype, name="numerator")
        result = _result(_anchors(con), [pair])
        p, q = _shortest(numerator), Fraction(len(numerator))
    else:
        pairs = [
            _state(con, numerator, dtype=dtype, name="numerator"),
            _state(
                con,
                denominator,
                dtype="int64" if isinstance(denominator[0], int) else "float64",
                name="denominator",
            ),
        ]
        result = _result(_anchors(con), pairs, kind="ratio")
        p, q = _shortest(numerator), _shortest(denominator)
    row = _rows(con, result)[0]
    _assert_quotient(row["requested"], p, q)
    meta = result.columns["requested"]
    assert meta.numeric_policy is None


def test_ratio_of_averages_divides_the_exact_averages(con):
    first, second = [0.1, 0.2, -0.05], [3.0, 4.0, 0.3]
    pairs = [
        _state(con, first, aggregation="avg_event", name="first"),
        _state(con, second, aggregation="avg_event", name="second"),
    ]
    ratio = _rows(con, _result(_anchors(con), pairs, kind="ratio"))[0]["requested"]
    _assert_quotient(ratio, _shortest(first) / 3, _shortest(second) / 3)


def test_ratio_composes_an_average_before_it_is_rounded(con):
    # 5e-324 / 3 rounds to 0.0 on its own; a ratio must divide the unrounded average.
    tiny = Fraction(repr(5e-324))
    average = _state(con, [5e-324, 0.0, 0.0], aggregation="avg_event", name="average")
    total = _state(con, [5e-324], name="total")
    assert _rows(con, _result(_anchors(con), [average]))[0]["requested"] == 0.0
    forward = _rows(con, _result(_anchors(con), [average, total], kind="ratio"))[0]
    _assert_quotient(forward["requested"], tiny / 3, tiny)
    reverse = _rows(con, _result(_anchors(con), [total, average], kind="ratio"))[0]
    _assert_quotient(reverse["requested"], tiny, tiny / 3)
    cancelled = _state(con, [1.0, -1.0], aggregation="avg_event", name="cancelled")
    result = _result(_anchors(con), [total, cancelled], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "zero_denominator"


def test_quotient_overflow_refuses(con):
    numerator = _state(con, [1e308], name="numerator")
    denominator = _state(con, [1e-10], name="denominator")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [numerator, denominator], kind="ratio"))
    assert error.value.code == "terrine.temporal.float64_precision"
    assert error.value.context["failure"] == "bound_overflow"


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


def test_all_null_ratio_numerator_is_not_a_zero_denominator(con):
    numerator = _state(con, [None], dtype="int64", name="numerator")
    denominator = _state(con, [0], dtype="int64", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "all_values_null"


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
