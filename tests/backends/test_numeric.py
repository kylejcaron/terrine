import math

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
)

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
