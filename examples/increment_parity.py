"""Compare increment's public day-panel unit export with a DuckDB Terrine frame.

Run with ``uv run python examples/increment_parity.py`` after installing the
``increment`` and ``duckdb`` extras. The per-unit panel constructor accepts
caller-bucketed event days; this example deliberately demonstrates the loss of
intraday boundary precision alongside a day-aligned comparison.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import ibis
import pyarrow as pa
from increment.frame import MetricSpec, from_unit_panel

from terrine import Coverage, Model, UnitSource
from terrine.spec import Design, FixedDate, agg

T0 = datetime(2025, 1, 3, tzinfo=UTC)
UNITS = ("u1", "u2", "u3", "u4")
DAYS = (date(2025, 1, 2), date(2025, 1, 3))
EVENTS = (
    ("u1", datetime(2025, 1, 2, 8, tzinfo=UTC), 5),
    ("u1", datetime(2025, 1, 3, 0, tzinfo=UTC), 2),
    ("u1", datetime(2025, 1, 3, 8, tzinfo=UTC), 3),
    ("u1", datetime(2025, 1, 3, 12, tzinfo=UTC), 7),
    ("u1", datetime(2025, 1, 3, 13, tzinfo=UTC), 11),
    ("u2", datetime(2025, 1, 2, 8, tzinfo=UTC), 13),
    ("u2", datetime(2025, 1, 3, 15, tzinfo=UTC), 17),
    ("u3", datetime(2025, 1, 3, 16, tzinfo=UTC), 19),
)
ORDERS = (
    ("u1", datetime(2025, 1, 3, 8, tzinfo=UTC), 1),
    ("u1", datetime(2025, 1, 3, 12, tzinfo=UTC), 2),
    ("u1", datetime(2025, 1, 3, 14, tzinfo=UTC), 3),
    ("u2", datetime(2025, 1, 3, 15, tzinfo=UTC), 4),
    ("u3", datetime(2025, 1, 3, 16, tzinfo=UTC), 5),
)


def _catalog() -> dict:
    sources = {
        "events": {
            "relation": {"kind": "table", "name": "events"},
            "timestamp": "event_ts",
            "entities": {"user": "unit_id"},
            "facts": ["amount"],
            "properties": [],
            "dimensions": [],
        },
        "orders": {
            "relation": {"kind": "table", "name": "orders"},
            "timestamp": "order_ts",
            "entities": {"user": "unit_id"},
            "facts": ["items"],
            "properties": [],
            "dimensions": [],
        },
    }
    measures = {
        "pre_revenue": {"fact": "amount", "aggregation": "sum"},
        "post_revenue": {"fact": "amount", "aggregation": "sum"},
        "long_revenue": {"fact": "amount", "aggregation": "sum"},
        "post_items": {"fact": "items", "aggregation": "sum"},
        "post_orders": {"fact": "items", "aggregation": "count"},
    }
    metrics = {name: {"kind": "mean", "entity": "user", "measure": name} for name in measures}
    return {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": sources,
            "facts": {
                "amount": {"source": "events", "column": "amount"},
                "items": {"source": "orders", "column": "items"},
            },
            "properties": {},
            "dimensions": {},
            "measures": measures,
            "metrics": metrics,
        }


def _increment_source():
    revenue = {(unit, day): 0 for unit in UNITS for day in DAYS}
    order_count = {(unit, day): 0 for unit in UNITS for day in DAYS}
    items = {(unit, day): 0 for unit in UNITS for day in DAYS}
    for unit, timestamp, amount in EVENTS:
        revenue[(unit, timestamp.date())] += amount
    for unit, timestamp, value in ORDERS:
        key = (unit, timestamp.date())
        order_count[key] += 1
        items[key] += value
    pre_revenue = {
        unit: sum(
            amount
            for row_unit, timestamp, amount in EVENTS
            if row_unit == unit and timestamp.date() == DAYS[0]
        )
        for unit in UNITS
    }
    panel = pa.table(
        {
            "unit_id": [unit for unit in UNITS for _ in DAYS],
            "group": ["all"] * (len(UNITS) * len(DAYS)),
            "date": [day for _unit in UNITS for day in DAYS],
            "exposure_date": [date(2025, 1, 3)] * (len(UNITS) * len(DAYS)),
            "revenue": [revenue[(unit, day)] for unit in UNITS for day in DAYS],
            "order_count": [order_count[(unit, day)] for unit in UNITS for day in DAYS],
            "items": [items[(unit, day)] for unit in UNITS for day in DAYS],
            "pre_revenue": [pre_revenue[unit] for unit in UNITS for _ in DAYS],
        }
    )
    source = from_unit_panel(
        panel,
        unit="unit_id",
        group="group",
        date="date",
        control="all",
        exposure_date="exposure_date",
        observation_end=date(2025, 1, 4),
        metrics=[
            MetricSpec(name="revenue", value_column="revenue"),
            MetricSpec(name="orders", value_column="order_count"),
            MetricSpec(name="rate", type="ratio", numerator="revenue", denominator="items"),
            MetricSpec(name="long_revenue", value_column="revenue", window_days=3),
        ],
    )
    metrics_by_name = {metric.name: metric for metric in source.context.metrics}
    return source, metrics_by_name


def main() -> None:
    con = ibis.duckdb.connect()
    try:
        con.create_table("units", pa.table({"unit_id": list(UNITS)}))
        con.create_table(
            "events",
            pa.table(
                {
                    "unit_id": [row[0] for row in EVENTS],
                    "event_ts": [row[1] for row in EVENTS],
                    "amount": [row[2] for row in EVENTS],
                }
            ),
        )
        con.create_table(
            "orders",
            pa.table(
                {
                    "unit_id": [row[0] for row in ORDERS],
                    "order_ts": [row[1] for row in ORDERS],
                    "items": [row[2] for row in ORDERS],
                }
            ),
        )
        model = Model.from_catalog(
            _catalog(),
            con=con,
            readiness={
                "events": Coverage("2025-01-01", "2025-01-04"),
                "orders": Coverage("2025-01-02", "2025-01-03"),
            },
            units={"user": UnitSource("units", "unit_id")},
        )
        terrine_frame = (
            Design(
                "user",
                FixedDate(T0),
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
        terrine_rows = {row["unit"]: row for row in terrine_frame.table.to_pylist()}
        increment_source, metrics = _increment_source()
        increment_outputs = {
            "revenue": increment_source.unit_frame(
                metrics["revenue"], covariates=("pre_revenue",)
            ).to_pylist(),
            "orders": increment_source.unit_frame(metrics["orders"]).to_pylist(),
            "ratio": increment_source.unit_frame(metrics["rate"]).to_pylist(),
        }
        incomplete_moments = increment_source.moments(metrics["long_revenue"], grain="total")
        assert incomplete_moments == []
        print(
            "increment 3-day total moments: [] (all units censored at the supplied observation_end)"
        )
        by_metric = {
            name: {row["unit_id"]: row for row in rows} for name, rows in increment_outputs.items()
        }
        for unit in UNITS:
            output = terrine_rows[unit]
            revenue = by_metric["revenue"][unit]
            orders = by_metric["orders"][unit]
            ratio = by_metric["ratio"][unit]
            assert output["pre_revenue"] == revenue["pre_revenue"]
            assert output["post_revenue"] == revenue["y"] == ratio["y"]
            assert output["post_orders"] == orders["y"]
            assert output["post_items"] == ratio["y_den"]
            print(
                f"{unit}: pre={output['pre_revenue']!r}; "
                f"revenue={output['post_revenue']!r}; orders={output['post_orders']!r}; "
                f"ratio=({ratio['y']!r}, {ratio['y_den']!r}); "
                f"long_outcome={output['long_revenue']!r}"
            )
        # Same daily panel, later exact instant: the 08:00 amount is in
        # increment's day-0 bucket but precedes Terrine's noon t0.
        noon_frame = (
            Design(
                "user",
                FixedDate(datetime(2025, 1, 3, 12, tzinfo=UTC)),
                outcomes={"from_noon": agg("post_revenue", window=("0d", "1d"))},
            )
            .build(model)
            .materialize()
        )
        noon_u1 = next(row for row in noon_frame.table.to_pylist() if row["unit"] == "u1")
        increment_u1 = by_metric["revenue"]["u1"]["y"]
        print(
            "day-grain boundary: increment u1 day-0 revenue="
            f"{increment_u1!r}; Terrine noon-to-next-day revenue="
            f"{noon_u1['from_noon']!r}; "
            f"difference={Decimal(str(increment_u1)) - noon_u1['from_noon']!r}"
        )
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
