import math
from decimal import Decimal
from fractions import Fraction

import ibis
import pyarrow as pa
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

# |r - p/q| <= 6 * 2**-53 * |p/q| + 2**-1075 for a quotient r of exact p and q.
_RELATIVE = 6 * Fraction(1, 2**53)
_ABSOLUTE = Fraction(1, 2**1075)
_MAX = 1.7976931348623157e308


@pytest.fixture
def connection():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


def _rows(connection, table):
    # DuckDB BIGNUM is an internal coefficient, not a valid Arrow numeric column.
    projected = table.select(
        *(
            table[name].cast("string").name(name) if dtype.is_unknown() else table[name]
            for name, dtype in table.schema().items()
        )
    )
    return connection.to_pyarrow(projected).to_pylist()


def _floats(connection, name, **columns):
    # pandas-backed memtables turn NaN into NULL; Arrow keeps it.
    arrays = {
        key: pa.array(values, pa.float64() if key in {"value", "n", "d"} else None)
        for key, values in columns.items()
    }
    return connection.create_table(name, pa.table(arrays))


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


def test_float_scale_is_per_partition(connection):
    table = _floats(
        connection,
        "floats",
        grp=["a", "a", "a", "b", "b", "c"],
        value=[1.5, 2.25e-3, None, 1e20, math.nan, None],
    )
    rows = _rows(connection, table.select("grp", scale=float_scale(table.value, [table.grp])))
    assert {(row["grp"], row["scale"]) for row in rows} == {("a", 5), ("b", 0), ("c", 0)}


def test_normalized_truncates_to_nineteen_significant_digits(connection):
    table = connection.create_table(
        "coefficients",
        ibis.memtable(
            {"c": ["99999999999999999999", "-123456789012345678901", "5", "0"], "f": [4.4e-323] * 4}
        ),
    )
    significand, exponent = normalized(table.c.cast(exact_type(table.c)), 3)
    float_significand, float_exponent = normalized(table.f)
    rows = _rows(
        connection,
        table.select(
            significand=significand.cast("string"),
            exponent=exponent,
            float_significand=float_significand.cast("string"),
            float_exponent=float_exponent,
        ),
    )
    assert [(row["significand"], row["exponent"]) for row in rows] == [
        ("9999999999999999999", 16),
        ("-1234567890123456789", 17),
        ("5000000000000000000", -3),
        ("0", 0),
    ]
    assert {(row["float_significand"], row["float_exponent"]) for row in rows} == {
        ("4400000000000000000", -323)
    }


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


def _quotient(table, numerator, denominator):
    significand, exponent = normalized_quotient(*numerator, *denominator)
    value, overflow = rounded_normalized(significand, exponent)
    return table.select(value=value, overflow=overflow)


@pytest.mark.parametrize(("numerator", "denominator"), _QUOTIENTS)
def test_quotient_of_exact_operands_meets_bound(connection, numerator, denominator):
    n_coefficient, n_scale = _coefficient(numerator)
    d_coefficient, d_scale = _coefficient(denominator)
    table = connection.create_table(
        "operands",
        ibis.memtable(
            {
                "n": [n_coefficient],
                "n_scale": [n_scale],
                "d": [d_coefficient],
                "d_scale": [d_scale],
            }
        ),
    )
    exact = exact_type(table.n)
    quotient = _quotient(
        table,
        normalized(table.n.cast(exact), table.n_scale),
        normalized(table.d.cast(exact), table.d_scale),
    )
    row = _rows(connection, quotient)[0]
    _assert_quotient_within_bound(row, Fraction(numerator) / Fraction(denominator))


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    [case for case in _QUOTIENTS if all(math.isfinite(float(text)) for text in case)],
)
def test_quotient_of_rounded_operands_meets_bound(connection, numerator, denominator):
    table = _floats(connection, "operands", n=[float(numerator)], d=[float(denominator)])
    row = _rows(connection, _quotient(table, normalized(table.n), normalized(table.d)))[0]
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


def test_exact_to_float_flags_overflow(connection):
    groups = {"positive": [_MAX, _MAX], "negative": [-_MAX, -_MAX], "tiny": [5e-324, 5e-324]}
    table = _floats(
        connection,
        "floats",
        grp=[name for name, values in groups.items() for _ in values],
        value=[value for values in groups.values() for value in values],
    )
    scaled = table.mutate(scale=float_scale(table.value, [table.grp]))
    exact, _, _ = exact_float(scaled.value, scaled.scale)
    state = scaled.mutate(exact=exact)
    totals = state.group_by("grp").aggregate(total=exact_sum(state.exact), scale=state.scale.max())
    value, nonfinite, out_of_range = exact_to_float(totals.total, totals.scale)
    rows = _rows(
        connection,
        totals.select("grp", value=value, nonfinite=nonfinite, out_of_range=out_of_range),
    )
    assert {row["grp"]: (row["value"], row["nonfinite"], row["out_of_range"]) for row in rows} == {
        "positive": (None, False, True),
        "negative": (None, False, True),
        "tiny": (1e-323, False, False),
    }
