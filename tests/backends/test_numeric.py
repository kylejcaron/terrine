import math
from decimal import Decimal
from fractions import Fraction
from types import MappingProxyType

import ibis
import pyarrow as pa
import pytest

from terrine.backends.numeric import (
    _DUCKDB_HUGE,
    exact_float,
    exact_sum,
    exact_to_float,
    exact_type,
    float_scale,
    normalized,
    normalized_quotient,
    rounded_normalized,
)

_MAX = 1.7976931348623157e308
_RELATIVE = 6 * Fraction(1, 2**53)
_ABSOLUTE = Fraction(1, 2**1075)


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


def test_coefficient_bounds_follow_the_physical_type(connection):
    from terrine.backends.dialect import dialect_scope
    from terrine.backends.numeric import coefficient_bounds

    table = connection.create_table(
        "exact_columns",
        pa.table(
            {
                "n": pa.array([1], pa.int64()),
                "wide": pa.array([1], pa.decimal128(38, 0)),
                "money": pa.array([1], pa.decimal128(12, 2)),
            }
        ),
    )
    assert coefficient_bounds(table.n) == (-(1 << 63), (1 << 63) - 1)
    # DuckDB reports HUGEINT as decimal(38, 0), with a wider signed range.
    assert coefficient_bounds(table.wide) == (-(1 << 127), (1 << 127) - 1)
    assert coefficient_bounds(table.money) == (-(10**12 - 1), 10**12 - 1)
    # Snowflake's NUMBER(38, 0) holds exactly 38 digits.
    with dialect_scope("snowflake"):
        assert coefficient_bounds(table.wide) == (-(10**38 - 1), 10**38 - 1)


def _coefficient(text):
    """``(coefficient, scale)`` with ``Decimal(text) == coefficient * 10**-scale``."""
    whole, _, fraction = format(Decimal(text), "f").partition(".")
    return str(int(whole + fraction)), len(fraction)


def _assert_quotient_within_bound(row, reference):
    assert row["overflow"] is False
    result = Fraction(row["value"])
    assert (result < 0) == (reference < 0)
    assert abs(result - reference) <= _RELATIVE * abs(reference) + _ABSOLUTE


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


def _normalized_operands(connection, table, numerator, denominator, *columns):
    # Preserve the backend's wide integers between arithmetic operations.
    operands = connection.create_table(
        "normalized_operands",
        table.select(
            *columns,
            n_significand=numerator[0],
            n_exponent=numerator[1],
            d_significand=denominator[0],
            d_exponent=denominator[1],
        ),
    )
    # Ibis reflects HUGEINT as decimal(38, 0); retain the native UDF signature.
    return operands.mutate(
        n_significand=operands.n_significand.cast(_DUCKDB_HUGE),
        d_significand=operands.d_significand.cast(_DUCKDB_HUGE),
    )


def _quotient(connection, table, *columns):
    significand, exponent = normalized_quotient(
        table.n_significand.cast(_DUCKDB_HUGE),
        table.n_exponent,
        table.d_significand.cast(_DUCKDB_HUGE),
        table.d_exponent,
    )
    quotient = connection.create_table(
        "quotient", table.select(*columns, significand=significand, exponent=exponent)
    )
    value, overflow = rounded_normalized(quotient.significand, quotient.exponent)
    return quotient.select(*columns, value=value, overflow=overflow)


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
        operands = _normalized_operands(
            connection,
            table,
            normalized(table.n.cast(exact), table.n_scale),
            normalized(table.d.cast(exact), table.d_scale),
            "case",
        )
        quotient = _quotient(connection, operands, "case")
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
        operands = _normalized_operands(
            connection, table, normalized(table.n), normalized(table.d), "case"
        )
        quotient = _quotient(connection, operands, "case")
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
    operands = _normalized_operands(
        connection,
        table,
        normalized(table.n),
        normalized(table.members.cast(exact_type(table.members))),
        "d",
    )
    significand, exponent = normalized_quotient(
        operands.n_significand, operands.n_exponent, operands.d_significand, operands.d_exponent
    )
    denominator = normalized(operands.d)
    average = connection.create_table(
        "average",
        operands.select(
            n_significand=significand,
            n_exponent=exponent,
            d_significand=denominator[0],
            d_exponent=denominator[1],
        ),
    )
    row = _rows(connection, _quotient(connection, average))[0]
    _assert_quotient_within_bound(row, Fraction(1, 3))
    row = _rows(
        connection,
        average.select(value=rounded_normalized(average.n_significand, average.n_exponent)[0]),
    )[0]
    assert row["value"] == 0.0


def test_quotient_overflow_is_flagged(connection):
    table = _floats(connection, "operands", n=[1e308], d=[1e-10])
    operands = _normalized_operands(connection, table, normalized(table.n), normalized(table.d))
    row = _rows(connection, _quotient(connection, operands))[0]
    assert row == {"value": None, "overflow": True}


@pytest.mark.parametrize(("numerator", "denominator"), [(1.0, 0.0), (math.nan, 2.0)])
def test_quotient_without_a_finite_ratio_is_null(connection, numerator, denominator):
    table = _floats(connection, "operands", n=[numerator], d=[denominator])
    operands = _normalized_operands(connection, table, normalized(table.n), normalized(table.d))
    row = _rows(connection, _quotient(connection, operands))[0]
    assert row == {"value": None, "overflow": False}
