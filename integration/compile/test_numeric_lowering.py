"""Model-backed native and exact arithmetic workflows through the compiler."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from fractions import Fraction

import ibis
import pyarrow as pa
import pytest
import terrine as tr
from terrine.spec import Design, FixedDate, agg
from tests._float_bound import native_window_bound

_T0 = datetime(2025, 1, 25, tzinfo=UTC)


@pytest.fixture
def con():
    connection = ibis.duckdb.connect()
    try:
        yield connection
    finally:
        connection.disconnect()


def _model(con, units, times, amounts, *, exact=False):
    con.create_table("customers", pa.table({"customer_id": sorted(set(units))}), overwrite=True)
    con.create_table(
        "orders",
        pa.table(
            {
                "customer_id": units,
                "ordered_at": pa.array(times, pa.timestamp("us", tz="UTC")),
                "amount": pa.array(amounts, pa.float64()),
            }
        ),
        overwrite=True,
    )
    return tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={
                    "spend": tr.sum("amount", exact=exact),
                    "mean_spend": tr.mean("amount", exact=exact),
                    "largest": tr.max("amount", exact=exact),
                },
                complete=("2025-01-01", "2025-01-31"),
            )
        },
    )


def _hand_windows(con, start: timedelta):
    """Each unit's window and history-prefix values, from a hand-written query."""
    lower = (_T0 + start).isoformat(sep=" ")
    upper = _T0.isoformat(sep=" ")
    rows = con.sql(
        "SELECT customer_id,"
        f" list(amount) FILTER (WHERE ordered_at >= TIMESTAMPTZ '{lower}'"
        f" AND ordered_at < TIMESTAMPTZ '{upper}') AS window_values,"
        f" list(amount) FILTER (WHERE ordered_at < TIMESTAMPTZ '{upper}') AS prefix_values"
        " FROM orders GROUP BY customer_id"
    ).to_pyarrow()
    return {
        row["customer_id"]: (row["window_values"] or [], row["prefix_values"] or [])
        for row in rows.to_pylist()
    }


def test_native_sum_matches_hand_query_within_bound(con):
    generator = random.Random(20261107)
    units = [f"u{generator.randrange(10)}" for _ in range(1000)]
    times = [
        datetime(2025, 1, 1, tzinfo=UTC) + timedelta(seconds=generator.randrange(30 * 86400))
        for _ in units
    ]
    # Thousandths: rounding each input once at 1e-9 would exceed the native bound.
    amounts = [generator.uniform(-1e-3, 1e-3) for _ in units]
    model = _model(con, units, times, amounts)
    windows = {"week": timedelta(days=-7), "hours": timedelta(hours=-84)}
    design = Design(
        "customer",
        FixedDate(_T0),
        features={
            "week": agg("spend", window=("-7d", "0s")),
            "hours": agg("spend", window=("-84h", "0s")),
            "mean_week": agg("mean_spend", window=("-7d", "0s")),
        },
    )
    frame = design.build(model)
    rows = {row["unit"]: row for row in frame.to_pyarrow().to_pylist()}
    assert len(rows) == 10
    for name, start in (*windows.items(), ("mean_week", windows["week"])):
        assert frame.columns[name].arithmetic == "native"
        for unit, (window, prefix) in _hand_windows(con, start).items():
            exact = sum((Fraction(value) for value in window), Fraction())
            value = rows[unit][name]
            if name == "mean_week":
                expected = exact / len(window)
                bound = native_window_bound(prefix) / len(window) + 2**-52 * abs(float(expected))
            else:
                expected, bound = exact, native_window_bound(prefix)
            assert abs(Fraction(value) - expected) <= bound
    assert tr.preview(design, model).strategy == {
        "week": "sparse_day_prefix/native_float",
        "hours": "sparse_event_prefix/native_float",
        "mean_week": "sparse_day_prefix/native_float",
    }


def test_exact_opt_in_keeps_quantized_path(con):
    # Below half a nano-unit, the exact path's one-time quantization drops the value.
    amounts = [4e-10, 4e-10]
    times = [_T0 - timedelta(days=1), _T0 - timedelta(days=2)]
    design = Design(
        "customer",
        FixedDate(_T0),
        features={name: agg(name, window=("-7d", "0s")) for name in ("spend", "largest")},
    )
    results, reasons = {}, {}
    for exact in (False, True):
        model = _model(con, ["a", "a"], times, amounts, exact=exact)
        frame = design.build(model)
        table = frame.to_pyarrow()
        results[exact] = table["spend"][0].as_py()
        reasons[exact] = (table["spend__reason"][0].as_py(), table["spend__complete"][0].as_py())
        # Every metric column records the arithmetic its declaration chose.
        mode = "exact" if exact else "native"
        assert {name: frame.columns[name].arithmetic for name in ("spend", "largest")} == {
            "spend": mode,
            "largest": mode,
        }
        assert tr.preview(design, model).strategy["spend"] == (
            "sparse_day_prefix/native" if exact else "sparse_day_prefix/native_float"
        )
    # The exact path says where it discarded nonzero contributions; the native
    # path keeps them and has nothing to note. Neither makes the window incomplete.
    assert results[True] == 0.0
    assert reasons == {True: ("below_resolution", True), False: (None, True)}
    total = sum(map(Fraction, amounts), Fraction())
    assert abs(Fraction(results[False]) - total) <= native_window_bound(amounts)
