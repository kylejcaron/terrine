from terrine.spec import Design, FixedDate, agg
from tests.parity.test_increment_frames import (
    _T0,
    _UNITS,
    _fixture,
    _increment_rows,
)


def test_increment_panel_per_unit_matches_terrine_on_day_aligned_support():
    con, model = _fixture()
    try:
        frame = (
            Design(
                "user",
                FixedDate(_T0),
                features={"pre_revenue": agg("pre_revenue", window=("-1d", "0d"))},
                outcomes={
                    "post_revenue": agg("post_revenue", window=("0d", "1d")),
                    "post_orders": agg("post_orders", window=("0d", "1d")),
                    "post_items": agg("post_items", window=("0d", "1d")),
                    "long_revenue": agg("long_revenue", window=("0d", "3d")),
                },
            )
            .build(model)
            .materialize()
        )
        terrine_rows = {row["unit"]: row for row in frame.table.to_pylist()}
        _source, increment_output, _metrics = _increment_rows()
        increment_rows = {
            name: {row["unit_id"]: row for row in rows} for name, rows in increment_output.items()
        }
        expected = {
            "u1": {"pre": 5, "revenue": 23, "orders": 3, "items": 6},
            "u2": {"pre": 13, "revenue": 17, "orders": 1, "items": 4},
            "u3": {"pre": 0, "revenue": 19, "orders": 1, "items": 5},
            "u4": {"pre": 0, "revenue": 0, "orders": 0, "items": 0},
        }
        assert tuple(sorted(terrine_rows)) == _UNITS
        assert tuple(sorted(increment_rows["revenue"])) == _UNITS
        for unit, values in expected.items():
            row = terrine_rows[unit]
            revenue = increment_rows["revenue"][unit]
            orders = increment_rows["orders"][unit]
            ratio = increment_rows["ratio"][unit]
            assert revenue["y"] == values["revenue"]
            assert orders["y"] == values["orders"]
            assert (ratio["y"], ratio["y_den"]) == (values["revenue"], values["items"])
            assert row["pre_revenue"] == revenue["pre_revenue"] == values["pre"]
            assert row["post_revenue"] == revenue["y"] == values["revenue"]
            assert row["post_orders"] == orders["y"] == values["orders"]
            assert row["post_revenue"] == ratio["y"]
            assert row["post_items"] == ratio["y_den"] == values["items"]
            assert row["long_revenue"] is None
    finally:
        con.disconnect()
