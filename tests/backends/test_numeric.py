import math
from decimal import Decimal

import duckdb
import ibis
import pyarrow as pa
import pytest

from terrine.backends.dialect import dialect_for_name
from terrine.backends.numeric import (
    exact_sum,
    exact_to_float,
    exact_type,
)


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


def test_exact_to_float_flags_overflow(connection):
    table = connection.create_table(
        "coefficients",
        ibis.memtable({"value": ["1" + "0" * 309, "-" + "1" + "0" * 309, "10"]}),
    )
    value, nonfinite, out_of_range = exact_to_float(table.value.cast(exact_type(table.value)))
    rows = _rows(
        connection, table.select(value=value, nonfinite=nonfinite, out_of_range=out_of_range)
    )
    assert rows == [
        {"value": None, "nonfinite": False, "out_of_range": True},
        {"value": None, "nonfinite": False, "out_of_range": True},
        {"value": 10.0, "nonfinite": False, "out_of_range": False},
    ]


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


def test_duckdb_counts_are_native_integers_that_narrow_without_text(connection):
    import ibis.expr.datatypes as dt

    from terrine.backends.dialect import dialect_scope
    from terrine.backends.engines.duckdb import _EXACT, _HUGE
    from terrine.backends.numeric import (
        count_literal,
        exact_literal,
        exact_subtract,
        is_exact_type,
    )
    from terrine.temporal.aggregate import narrow_exact

    stored = dt.Decimal(38, 0)
    with dialect_scope("duckdb"):
        assert count_literal(1).type() == dt.int64
        table = ibis.table({"term": "int64", "huge": _HUGE, "stored": stored, "exact": _EXACT})
        assert exact_sum(table.term).type() == exact_sum(table.stored).type() == _HUGE
        assert exact_sum(table.exact).type() == _EXACT
        assert exact_subtract(table.huge, table.stored).type() == _HUGE
        assert [exact_literal(0, column).type() for column in (table.huge, table.stored)] == [
            _HUGE,
            stored,
        ]
        assert exact_literal(0, table.exact).type() == _EXACT
    assert is_exact_type(stored)
    with dialect_scope("postgres"):
        assert count_literal(1).type() == dialect_for_name("postgres").coefficient_type()
    # A stored HUGEINT count and its window difference narrow to int64 exactly at the
    # boundary and to NULL beyond it, up to the HUGEINT limit.
    counts = connection.sql(
        "SELECT n::HUGEINT AS n FROM (VALUES (9223372036854775807), (9223372036854775808),"
        " (170141183460469231731687303715884105727), (-1)) AS s(n)"
    )
    difference = exact_subtract(counts.n, exact_literal(0, counts.n))
    rows = _rows(
        connection,
        counts.select(
            stored=narrow_exact(counts.n, 0, "int64"), huge=narrow_exact(difference, 0, "int64")
        ),
    )
    assert [(row["stored"], row["huge"]) for row in rows] == [
        (2**63 - 1, 2**63 - 1),
        (None, None),
        (None, None),
        (-1, -1),
    ]
    # Accumulating past the HUGEINT limit raises rather than wrapping.
    with pytest.raises(duckdb.OutOfRangeException):
        _rows(connection, counts.aggregate(total=exact_sum(counts.n)))


def test_quantized_float_rounds_to_nine_places(connection):
    from terrine.backends.numeric import quantized_float

    values = [
        0.1,
        6e-10,
        -6e-10,
        1 / 1024,
        -1 / 1024,
        math.nan,
        1e29,
        -1e29,
        math.nextafter(1e29, math.inf),
        -math.nextafter(1e29, math.inf),
    ]
    table = _floats(connection, "quantization", value=values)
    value, nonfinite, out_of_range = quantized_float(table.value)
    rows = _rows(
        connection, table.select(value=value, nonfinite=nonfinite, out_of_range=out_of_range)
    )
    assert [row["value"] for row in rows[:5]] == [
        Decimal("0.100000000"),
        Decimal("0.000000001"),
        Decimal("-0.000000001"),
        Decimal("0.000976563"),
        Decimal("-0.000976563"),
    ]
    assert rows[5] == {"value": None, "nonfinite": True, "out_of_range": False}
    assert all(not row["out_of_range"] and row["value"] is not None for row in rows[6:8])
    assert all(row == {"value": None, "nonfinite": False, "out_of_range": True} for row in rows[8:])
