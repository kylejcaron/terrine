import math
from fractions import Fraction

from terrine.backends.numeric import (
    exact_float,
    exact_sum,
    exact_to_float,
    float_scale,
)
from tests.backends.test_numeric import _MAX, _floats, _rows
from tests.backends.test_numeric import connection as connection  # noqa: F401


def _decimal_sum(values):
    return float(sum(Fraction(repr(value)) for value in values if math.isfinite(value)))


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
