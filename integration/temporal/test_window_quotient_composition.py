from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import ibis
import pytest

from terrine.backends.numeric import exact_literal
from terrine.data import BoundCoverage
from terrine.errors import TerrineError
from terrine.temporal.readiness import Readiness
from tests.temporal.test_window_aggregates import (
    _anchors,
    _certificate,
    _result,
    _rows,
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
    assert row[meta.absolute_error_column] is None
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
        row = _rows(con, result)[0]
        assert row["requested"] == 6.0
        _certificate(result, row, Fraction(6))


def test_decimal_average_and_ratio_have_independent_fraction_certificates(con):
    numerator = _state(
        con,
        [Decimal("0.10"), Decimal("0.20")],
        aggregation="avg_event",
        dtype="decimal(38,2)",
        name="numerator",
    )
    result = _result(_anchors(con), [numerator])
    _certificate(result, _rows(con, result)[0], Fraction(3, 20))
    denominator = _state(con, [Decimal("0.70")], dtype="decimal(38,2)", name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _certificate(result, _rows(con, result)[0], Fraction(3, 14))


def test_float_average_and_ratio_return_final_not_input_errors(con):
    numerator = _state(con, [0.1, 0.2, -0.05], aggregation="avg_event", name="numerator")
    exact = sum(map(Fraction.from_float, [0.1, 0.2, -0.05])) / 3
    average = _result(_anchors(con), [numerator])
    _certificate(average, _rows(con, average)[0], exact)
    denominator = _state(con, [3.0, 4.0], name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _certificate(ratio, _rows(con, ratio)[0], exact / 7)


@pytest.mark.parametrize("kind", ["ratio"])
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


def test_known_zero_and_uncertain_zero_denominators_are_distinct(con):
    numerator = _state(con, [2.0], name="numerator")
    zero = _state(con, [0], dtype="int64", name="zero")
    result = _result(_anchors(con), [numerator, zero], kind="ratio")
    row = _rows(con, result)[0]
    assert row["requested"] is None
    assert row[result.columns["requested"].reason_column] == "zero_denominator"
    uncertain = _state(con, [1.0, -1.0], name="uncertain")
    with pytest.raises(TerrineError) as error:
        _rows(con, _result(_anchors(con), [numerator, uncertain], kind="ratio"))
    assert error.value.context["failure"] == "denominator_interval_zero"


def test_negative_denominator_and_subnormal_sum_have_fraction_enclosures(con):
    numerator = _state(con, [-0.1, 0.025], name="numerator")
    denominator = _state(con, [-0.7], name="denominator")
    result = _result(_anchors(con), [numerator, denominator], kind="ratio")
    exact = (Fraction.from_float(-0.1) + Fraction.from_float(0.025)) / Fraction.from_float(-0.7)
    _certificate(result, _rows(con, result)[0], exact)
    tiny = _state(con, [float.fromhex("0x0.0000000000001p-1022")], name="tiny")
    result = _result(_anchors(con), [tiny])
    _certificate(result, _rows(con, result)[0], Fraction(1, 2**1074))


def test_fractional_results_do_not_narrow_internal_exact_states(con):
    maximum = Decimal("99999999999999999999999999999999999999")
    numerator = _state(con, [maximum, maximum], dtype="decimal(38,0)", name="numerator")
    denominator = _state(con, [maximum, maximum], dtype="decimal(38,0)", name="denominator")
    ratio = _result(_anchors(con), [numerator, denominator], kind="ratio")
    _certificate(ratio, _rows(con, ratio)[0], Fraction(1))
    average_pair = _state(
        con, [maximum, maximum], aggregation="avg_event", dtype="decimal(38,0)", name="average"
    )
    average = _result(_anchors(con), [average_pair])
    _certificate(average, _rows(con, average)[0], Fraction(10**38 - 1))
    count_measure, count_state = _state(con, [1], aggregation="count", dtype="int64", name="counts")
    count_state = count_state.mutate(event_count=exact_literal(2**63))
    count_pair = (count_measure, count_state)
    count_ratio = _result(_anchors(con), [count_pair, count_pair], kind="ratio")
    _certificate(count_ratio, _rows(con, count_ratio)[0], Fraction(1))
