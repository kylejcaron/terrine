import math
from decimal import Decimal
from fractions import Fraction

import pyarrow as pa
import pytest

from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.backends.numeric import decimal_coefficient, exact_sum, exact_to_float, quantized_float
from tests._float_bound import window_sum_bound
from tests.backends.test_numeric import _floats, _rows
from tests.backends.test_numeric import connection as connection  # noqa: F401

_GROUPS = {
    "cancel": [0.1, 0.2, -0.3],
    "tiny": [5e-324, -0.0],
    "large": [1e16, 1.0, -1e16],
    "wide": [9e28, 9e28],
    "nonfinite": [1.5, math.inf, math.nan],
}


def test_quantized_float_sums_meet_window_bound(connection):
    table = _floats(
        connection,
        "floats",
        grp=[name for name, values in _GROUPS.items() for _ in values],
        value=[value for values in _GROUPS.values() for value in values],
    )
    quantized, nonfinite, out_of_range = quantized_float(table.value)
    state = table.mutate(
        exact=decimal_coefficient(quantized, 9), nonfinite=nonfinite, invalid=out_of_range
    )
    totals = state.group_by("grp").aggregate(
        total=exact_sum(state.exact), nonfinite=state.nonfinite.sum(), invalid=state.invalid.sum()
    )
    value, nonfinite, out_of_range = exact_to_float(totals.total, 9)
    rows = _rows(
        connection,
        totals.select(
            "grp",
            "nonfinite",
            "invalid",
            value=value,
            value_nonfinite=nonfinite,
            out_of_range=out_of_range,
        ),
    )
    assert len(rows) == len(_GROUPS)
    for row in rows:
        values = _GROUPS[row["grp"]]
        finite = [value for value in values if math.isfinite(value)]
        assert abs(Fraction(row["value"]) - sum(map(Fraction, finite))) <= window_sum_bound(finite)
        assert row["nonfinite"] == sum(not math.isfinite(value) for value in values)
        assert row["invalid"] == 0
        assert row["value_nonfinite"] is False
        assert row["out_of_range"] is False


@pytest.mark.postgres
def test_postgres_float_quantization_keeps_boundary_coefficients_unbounded(postgres_scratch):
    con = postgres_scratch.con
    values = [
        0.1,
        4e-10,
        -4e-10,
        6e-10,
        -6e-10,
        1 / 1024,
        -1 / 1024,
        1e29,
        -1e29,
        math.nextafter(1e29, math.inf),
        -math.nextafter(1e29, math.inf),
    ]
    table = con.create_table(
        "float_quantization",
        pa.table({"case": list(range(len(values))), "value": pa.array(values, pa.float64())}),
    )
    quantized, nonfinite, out_of_range = quantized_float(table.value)
    rows = con.to_pyarrow(
        table.select(
            "case", text=quantized.cast("string"), nonfinite=nonfinite, out_of_range=out_of_range
        ).order_by("case")
    ).to_pylist()
    assert [Decimal(row["text"]) for row in rows[:7]] == [
        Decimal("0.100000000"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0.000000001"),
        Decimal("-0.000000001"),
        Decimal("0.000976563"),
        Decimal("-0.000976563"),
    ]
    for value, row in zip(values[:9], rows[:9], strict=True):
        assert row["nonfinite"] is False and row["out_of_range"] is False
        assert abs(Fraction(Decimal(row["text"])) - Fraction(value)) <= window_sum_bound([value])
    for row in rows[9:]:
        assert row["text"] is None
        assert row["nonfinite"] is False and row["out_of_range"] is True

    boundary = table.filter(table["case"] == 7)
    quantized, _, _ = quantized_float(boundary.value)
    state = boundary.mutate(coefficient=decimal_coefficient(quantized, 9))
    total = state.aggregate(coefficient=exact_sum(state.coefficient))
    value, nonfinite, out_of_range = exact_to_float(total.coefficient, 9)
    row = con.to_pyarrow(
        total.select(
            text=total.coefficient.cast("string"),
            value=value,
            nonfinite=nonfinite,
            out_of_range=out_of_range,
        )
    ).to_pylist()[0]
    assert abs(Fraction(row["value"]) - Fraction(1e29)) <= window_sum_bound([1e29])
    assert row["nonfinite"] is False and row["out_of_range"] is False
    assert int(Decimal(row["text"])) == int(Fraction(Decimal(rows[7]["text"])) * 10**9)
