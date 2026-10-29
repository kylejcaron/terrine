from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import ibis
import pyarrow as pa
import pytest

pytest.importorskip(
    "increment.frame",
    reason="Task 4.5 requires increment==0.1.0a2 (install with `uv sync --extra increment`)",
)

from increment.errors import CapabilityError
from increment.frame import MetricSpec, from_unit_panel

from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg

_T0 = datetime(2025, 1, 3, tzinfo=UTC)
_UNITS = ("u1", "u2", "u3", "u4")
_DAYS = (date(2025, 1, 2), date(2025, 1, 3))
_EVENT_ROWS = (
    ("u1", datetime(2025, 1, 2, 8, tzinfo=UTC), 5),
    ("u1", datetime(2025, 1, 3, 0, tzinfo=UTC), 2),
    ("u1", datetime(2025, 1, 3, 8, tzinfo=UTC), 3),  # before noon boundary
    ("u1", datetime(2025, 1, 3, 12, tzinfo=UTC), 7),  # exact noon exposure
    ("u1", datetime(2025, 1, 3, 13, tzinfo=UTC), 11),
    ("u2", datetime(2025, 1, 2, 8, tzinfo=UTC), 13),
    ("u2", datetime(2025, 1, 3, 15, tzinfo=UTC), 17),
    ("u3", datetime(2025, 1, 3, 16, tzinfo=UTC), 19),
)
_ORDER_ROWS = (
    ("u1", datetime(2025, 1, 3, 8, tzinfo=UTC), 1),
    ("u1", datetime(2025, 1, 3, 12, tzinfo=UTC), 2),  # exact noon exposure
    ("u1", datetime(2025, 1, 3, 14, tzinfo=UTC), 3),
    ("u2", datetime(2025, 1, 3, 15, tzinfo=UTC), 4),
    ("u3", datetime(2025, 1, 3, 16, tzinfo=UTC), 5),
)


def _fixture():
    con = ibis.duckdb.connect()
    con.create_table("units", pa.table({"unit_id": list(_UNITS)}))
    con.create_table(
        "events",
        pa.table(
            {
                "unit_id": [row[0] for row in _EVENT_ROWS],
                "event_ts": [row[1] for row in _EVENT_ROWS],
                "amount": [row[2] for row in _EVENT_ROWS],
            }
        ),
    )
    con.create_table(
        "orders",
        pa.table(
            {
                "unit_id": [row[0] for row in _ORDER_ROWS],
                "order_ts": [row[1] for row in _ORDER_ROWS],
                "items": [row[2] for row in _ORDER_ROWS],
            }
        ),
    )
    catalog = Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "event_ts",
                    "units": {"user": "unit_id"},
                    "events": ["amount"],
                    "properties": [],
                    "dimensions": [],
                },
                "orders": {
                    "relation": {"kind": "table", "name": "orders"},
                    "time": "order_ts",
                    "units": {"user": "unit_id"},
                    "events": ["items"],
                    "properties": [],
                    "dimensions": [],
                },
            },
            "events": {
                "amount": {"source": "events", "column": "amount"},
                "items": {"source": "orders", "column": "items"},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "pre_revenue": {"event": "amount", "aggregation": "sum"},
                "post_revenue": {"event": "amount", "aggregation": "sum"},
                "long_revenue": {"event": "amount", "aggregation": "sum"},
                "post_items": {"event": "items", "aggregation": "sum"},
                "post_orders": {"event": "items", "aggregation": "count"},
            },
            "metrics": {
                name: {"kind": "mean", "unit": "user", "measure": name}
                for name in (
                    "pre_revenue",
                    "post_revenue",
                    "long_revenue",
                    "post_items",
                    "post_orders",
                )
            },
        }
    )
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "events": Coverage("2025-01-01", "2025-01-04"),
            "orders": Coverage("2025-01-02", "2025-01-03"),
        },
        units={"user": UnitSource("units", "unit_id")},
    )
    return con, model


def _increment_panel():
    revenue_by_day = {(unit, day): 0 for unit in _UNITS for day in _DAYS}
    order_count_by_day = {(unit, day): 0 for unit in _UNITS for day in _DAYS}
    items_by_day = {(unit, day): 0 for unit in _UNITS for day in _DAYS}
    for unit, timestamp, amount in _EVENT_ROWS:
        revenue_by_day[(unit, timestamp.date())] += amount
    for unit, timestamp, items in _ORDER_ROWS:
        key = (unit, timestamp.date())
        order_count_by_day[key] += 1
        items_by_day[key] += items
    pre_revenue = {
        unit: sum(
            amount
            for row_unit, timestamp, amount in _EVENT_ROWS
            if row_unit == unit and timestamp.date() == _DAYS[0]
        )
        for unit in _UNITS
    }
    return pa.table(
        {
            "unit_id": [unit for unit in _UNITS for _ in _DAYS],
            "group": ["all"] * (len(_UNITS) * len(_DAYS)),
            "date": [day for _ in _UNITS for day in _DAYS],
            "exposure_date": [date(2025, 1, 3)] * (len(_UNITS) * len(_DAYS)),
            "revenue": [revenue_by_day[(unit, day)] for unit in _UNITS for day in _DAYS],
            "order_count": [order_count_by_day[(unit, day)] for unit in _UNITS for day in _DAYS],
            "items": [items_by_day[(unit, day)] for unit in _UNITS for day in _DAYS],
            "pre_revenue": [pre_revenue[unit] for unit in _UNITS for _ in _DAYS],
        }
    )


def _increment_rows():
    source = from_unit_panel(
        _increment_panel(),
        unit="unit_id",
        group="group",
        date="date",
        control="all",
        exposure_date="exposure_date",
        observation_end=date(2025, 1, 4),
        metrics=[
            MetricSpec(name="revenue", value_column="revenue"),
            MetricSpec(name="post_orders", value_column="order_count"),
            MetricSpec(name="rate", type="ratio", numerator="revenue", denominator="items"),
            MetricSpec(name="long_revenue", value_column="revenue", window_days=3),
        ],
    )
    metrics = {metric.name: metric for metric in source.context.metrics}

    def rows(metric, **kwargs):
        # Arrow input yields Arrow output through increment's narwhals boundary.
        return cast(pa.Table, source.unit_frame(metrics[metric], **kwargs)).to_pylist()

    return (
        source,
        {
            "revenue": rows("revenue", covariates=("pre_revenue",)),
            "orders": rows("post_orders"),
            "ratio": rows("rate"),
        },
        metrics,
    )


def test_exposure_date_bucket_is_not_an_instant_equality_boundary():
    con, model = _fixture()
    try:
        frame = (
            Design(
                "user",
                FixedDate(datetime(2025, 1, 3, 12, tzinfo=UTC)),
                outcomes={"from_noon": agg("post_revenue", window=("0d", "1d"))},
            )
            .build(model)
            .materialize()
        )
        terrine_rows = {row["unit"]: row for row in frame.table.to_pylist()}
        _source, increment_output, _metrics = _increment_rows()
        increment_rows = {row["unit_id"]: row for row in increment_output["revenue"]}
        # Increment's public panel constructor sees a day label, so its day-0
        # sum includes u1's 00:00 and 08:00 amounts (5), both before noon t0.
        # Terrine's exact instant window excludes those rows but includes the
        # exact-noon amount (7). The increment day bucket includes equality too;
        # this measures loss of timestamp resolution, not the query path's strict
        # raw-event boundary.
        assert terrine_rows["u1"]["from_noon"] == Decimal(18)
        assert increment_rows["u1"]["y"] == 23.0
        assert Decimal(str(increment_rows["u1"]["y"])) - terrine_rows["u1"]["from_noon"] == 5
    finally:
        con.disconnect()


def test_increment_panel_censors_incomplete_windows_but_cannot_export_them_per_unit():
    source, _outputs, metrics = _increment_rows()
    assert source.moments(metrics["long_revenue"], grain="total") == []
    with pytest.raises(CapabilityError) as error:
        source.unit_frame(metrics["long_revenue"])
    assert error.value.code == "source.frame.unit_frame_panel"
