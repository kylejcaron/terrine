import math
from fractions import Fraction

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
