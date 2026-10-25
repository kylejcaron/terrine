import math
from decimal import Decimal
from fractions import Fraction
from types import MappingProxyType

import ibis
import pytest

from terrine.backends.numeric import (
    exact_float,
    exact_sum,
    exact_to_float,
    exact_type,
    float_scale,
    normalized,
    normalized_quotient,
    rounded_normalized,
)
from tests.backends.test_numeric import _MAX, _floats, _rows
from tests.backends.test_numeric import connection as connection  # noqa: F401

# |r - p/q| <= 6 * 2**-53 * |p/q| + 2**-1075 for a quotient r of exact p and q.
_RELATIVE = 6 * Fraction(1, 2**53)
_ABSOLUTE = Fraction(1, 2**1075)


def _decimal_sum(values):
    return float(sum(Fraction(repr(value)) for value in values if math.isfinite(value)))


def _coefficient(text):
    """``(coefficient, scale)`` with ``Decimal(text) == coefficient * 10**-scale``."""
    whole, _, fraction = format(Decimal(text), "f").partition(".")
    return str(int(whole + fraction)), len(fraction)


def _assert_quotient_within_bound(row, reference):
    assert row["overflow"] is False
    result = Fraction(row["value"])
    assert (result < 0) == (reference < 0)
    assert abs(result - reference) <= _RELATIVE * abs(reference) + _ABSOLUTE


_GROUPS = {
    "cancel": [0.1, 0.2, -0.3],
    "extreme": [5e-324, _MAX, -0.0],
    "large": [1e16, 1.0, -1e16],
    "all": [0.1, 0.2, -0.3, 5e-324, _MAX, -0.0, 1e16, 1.0, -1e16],
    "nonfinite": [1.5, math.inf, math.nan],
}


def test_exact_float_sums_match_shortest_decimal_sums(connection):
    table = _floats(
        connection,
        "floats",
        grp=[name for name, values in _GROUPS.items() for _ in values],
        value=[value for values in _GROUPS.values() for value in values],
    )
    scaled = table.mutate(scale=float_scale(table.value, [table.grp]))
    exact, nonfinite, unrepresentable = exact_float(scaled.value, scaled.scale)
    state = scaled.mutate(exact=exact, nonfinite=nonfinite, unrepresentable=unrepresentable)
    totals = state.group_by("grp").aggregate(
        total=exact_sum(state.exact),
        scale=state.scale.max(),
        nonfinite=state.nonfinite.sum(),
        unrepresentable=state.unrepresentable.sum(),
    )
    value, nonfinite, out_of_range = exact_to_float(totals.total, totals.scale)
    rows = _rows(
        connection,
        totals.select(
            "grp",
            "nonfinite",
            "unrepresentable",
            value=value,
            value_nonfinite=nonfinite,
            out_of_range=out_of_range,
        ),
    )
    assert len(rows) == len(_GROUPS)
    for row in rows:
        values = _GROUPS[row["grp"]]
        assert row["value"] == _decimal_sum(values)
        assert row["nonfinite"] == sum(not math.isfinite(value) for value in values)
        assert row["unrepresentable"] == 0
        assert row["value_nonfinite"] is False
        assert row["out_of_range"] is False


_QUOTIENTS = [
    ("4.4e-323", "5e-323"),
    ("8.957174075507802e-308", "4.44329165384767"),
    ("-7.5", "2.5"),
    ("7.5", "-2.5"),
    ("-1e-320", "3.0"),
    ("3.5953862697246314e308", "2"),
    # Quotients at or just past the largest finite double stay finite.
    ("5.93238734504564181e308", "3.3"),
    ("-1.7976931348623158e308", "1"),
]


def _quotient(table, numerator, denominator, *columns):
    significand, exponent = normalized_quotient(*numerator, *denominator)
    value, overflow = rounded_normalized(significand, exponent)
    return table.select(*columns, value=value, overflow=overflow)


@pytest.fixture(scope="module")
def exact_quotients():
    connection = ibis.duckdb.connect()
    try:
        operands = [
            (*_coefficient(numerator), *_coefficient(denominator))
            for numerator, denominator in _QUOTIENTS
        ]
        columns = dict(
            zip(("n", "n_scale", "d", "d_scale"), zip(*operands, strict=True), strict=True)
        )
        columns["case"] = tuple(range(len(operands)))
        table = connection.create_table(
            "operands",
            ibis.memtable(columns),
        )
        exact = exact_type(table.n)
        quotient = _quotient(
            table,
            normalized(table.n.cast(exact), table.n_scale),
            normalized(table.d.cast(exact), table.d_scale),
            "case",
        )
        rows = _rows(connection, quotient)
        assert len(rows) == len(_QUOTIENTS)
        return MappingProxyType({_QUOTIENTS[row["case"]]: MappingProxyType(row) for row in rows})
    finally:
        connection.disconnect()


@pytest.fixture(scope="module")
def rounded_quotients():
    cases = [case for case in _QUOTIENTS if all(math.isfinite(float(text)) for text in case)]
    connection = ibis.duckdb.connect()
    try:
        table = _floats(
            connection,
            "operands",
            n=[float(numerator) for numerator, _ in cases],
            d=[float(denominator) for _, denominator in cases],
            case=list(range(len(cases))),
        )
        quotient = _quotient(table, normalized(table.n), normalized(table.d), "case")
        rows = _rows(connection, quotient)
        assert len(rows) == len(cases)
        return MappingProxyType({cases[row["case"]]: MappingProxyType(row) for row in rows})
    finally:
        connection.disconnect()


@pytest.mark.parametrize(("numerator", "denominator"), _QUOTIENTS)
def test_quotient_of_exact_operands_meets_bound(exact_quotients, numerator, denominator):
    row = exact_quotients[numerator, denominator]
    _assert_quotient_within_bound(row, Fraction(numerator) / Fraction(denominator))


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    [case for case in _QUOTIENTS if all(math.isfinite(float(text)) for text in case)],
)
def test_quotient_of_rounded_operands_meets_bound(rounded_quotients, numerator, denominator):
    row = rounded_quotients[numerator, denominator]
    # A rounded operand stands for its shortest decimal, not its binary expansion.
    reference = Fraction(repr(float(numerator))) / Fraction(repr(float(denominator)))
    _assert_quotient_within_bound(row, reference)


def test_nested_quotients_keep_their_precision(connection):
    # (5e-324 / 3) / 5e-324 ≈ 1/3 although 5e-324 / 3 alone rounds to 0.0.
    table = _floats(connection, "operands", n=[5e-324], d=[5e-324], members=["3"])
    members = normalized(table.members.cast(exact_type(table.members)))
    average = normalized_quotient(*normalized(table.n), *members)
    row = _rows(connection, _quotient(table, average, normalized(table.d)))[0]
    _assert_quotient_within_bound(row, Fraction(1, 3))
    row = _rows(connection, table.select(value=rounded_normalized(*average)[0]))[0]
    assert row["value"] == 0.0


def test_quotient_overflow_is_flagged(connection):
    table = _floats(connection, "operands", n=[1e308], d=[1e-10])
    row = _rows(connection, _quotient(table, normalized(table.n), normalized(table.d)))[0]
    assert row == {"value": None, "overflow": True}


@pytest.mark.parametrize(("numerator", "denominator"), [(1.0, 0.0), (math.nan, 2.0)])
def test_quotient_without_a_finite_ratio_is_null(connection, numerator, denominator):
    table = _floats(connection, "operands", n=[numerator], d=[denominator])
    row = _rows(connection, _quotient(table, normalized(table.n), normalized(table.d)))[0]
    assert row == {"value": None, "overflow": False}
