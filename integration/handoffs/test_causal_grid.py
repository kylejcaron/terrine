"""Catalog-backed trial panels preserve each event/metric pair through export."""

from datetime import UTC, date, datetime
from types import MappingProxyType

import ibis
import pyarrow as pa
import pytest

import terrine as tr
from terrine.export import preflight_export
from terrine.spec import Initiation

pytestmark = pytest.mark.handoffs

GRID = [
    ("upgrade", "net_revenue"),
    ("upgrade", "session_count"),
    ("referral", "net_revenue"),
    ("referral", "session_count"),
]


def _times(*days):
    return [datetime.fromisoformat(day).replace(tzinfo=UTC) for day in days]


def _grid_model(con):
    con.create_table("customers", pa.table({"customer_id": ["upgraded", "referred", "control"]}))
    con.create_table(
        "orders",
        pa.table(
            {
                "customer_id": ["upgraded"] * 3 + ["referred"] * 2,
                "ordered_at": _times(
                    "2024-12-20", "2025-01-10", "2025-02-01", "2024-12-21", "2025-01-15"
                ),
                "amount": [5, 10, 20, 7, 30],
            }
        ),
    )
    con.create_table(
        "sessions",
        pa.table(
            {
                "customer_id": ["upgraded"] * 3 + ["referred"] * 2,
                "started_at": _times(
                    "2024-12-22", "2025-01-08", "2025-02-05", "2024-12-23", "2025-01-15"
                ),
            }
        ),
    )
    con.create_table(
        "account_events",
        pa.table(
            {
                "customer_id": ["upgraded", "referred"],
                "occurred_at": _times("2025-01-02", "2025-01-03"),
                "upgrade": [True, None],
                "referral": [None, True],
            }
        ),
    )
    catalog = {
        "schema_version": 2,
        "day_boundary": "UTC",
        "dialect": "duckdb",
        "sources": {
            "orders": {
                "relation": {"kind": "table", "name": "orders"},
                "time": "ordered_at",
                "units": {"customer": "customer_id"},
                "events": ["order", "order_amount"],
                "properties": [],
                "dimensions": [],
            },
            "sessions": {
                "relation": {"kind": "table", "name": "sessions"},
                "time": "started_at",
                "units": {"customer": "customer_id"},
                "events": ["session"],
                "properties": [],
                "dimensions": [],
            },
            "account_events": {
                "relation": {"kind": "table", "name": "account_events"},
                "time": "occurred_at",
                "units": {"customer": "customer_id"},
                "events": [],
                "properties": ["upgrade", "referral"],
                "dimensions": [],
            },
        },
        "events": {
            "order": {"source": "orders", "column": None},
            "order_amount": {"source": "orders", "column": "amount"},
            "session": {"source": "sessions", "column": None},
        },
        "properties": {
            name: {
                "source": "account_events",
                "column": name,
                "dtype": "bool",
                "as_of": "event_time",
            }
            for name in ("upgrade", "referral")
        },
        "dimensions": {},
        "measures": {
            "order_count": {"event": "order", "aggregation": "count"},
            "net_revenue": {"event": "order_amount", "aggregation": "sum"},
            "session_count": {"event": "session", "aggregation": "count"},
        },
        "metrics": {
            name: {"kind": "mean", "unit": "customer", "measure": name}
            for name in ("order_count", "net_revenue", "session_count")
        },
    }
    return tr.Model.from_catalog(
        catalog,
        con=con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        readiness={
            name: tr.Coverage("2024-01-01", "2025-06-30")
            for name in ("orders", "sessions", "account_events")
        },
    )


@pytest.fixture(scope="module")
def grid_frames():
    con = ibis.duckdb.connect()
    try:
        model = _grid_model(con)
        covariates = {
            "orders_90d": tr.agg("order_count", window=("-90d", "0s")),
            "sessions_90d": tr.agg("session_count", window=("-90d", "0s")),
        }
        frames = {}
        for event, metric in GRID:
            design = tr.recipes.trial_panel(
                every="7d",
                start="2025-01-06",
                end="2025-01-27",
                treatment=Initiation(event, history_start=datetime(2024, 1, 1, tzinfo=UTC)),
                features=covariates,
                outcomes={"y": tr.agg(metric, window=("0s", "28d"))},
            )
            frames[event, metric] = design.build(model).materialize(
                tr.Budget(max_rows=10_000, max_output_bytes=10_000_000)
            )
        return MappingProxyType(frames)
    finally:
        con.disconnect()


@pytest.mark.parametrize("event,metric", GRID)
def test_grid_pair_builds_a_trial_panel_with_treated_and_control_rows(grid_frames, event, metric):
    materialized = grid_frames[event, metric]
    roles = materialized.roles
    assert roles.treatment == "treatment"
    assert roles.cluster_unit == "unit"
    assert roles.features == ("orders_90d", "sessions_90d")
    assert roles.outcomes == ("y",)
    checked = preflight_export(materialized)
    rows = checked.table.to_pylist()
    assert len(rows) == 12
    assert {row["trial_id"] for row in rows} == {f"schedule:{k}" for k in range(4)}
    expected_treated = "upgraded" if event == "upgrade" else "referred"
    for day in (6, 13, 20, 27):
        trial = [row for row in rows if row["t0"].date() == date(2025, 1, day)]
        assert {row["unit"] for row in trial} == {"upgraded", "referred", "control"}
        assert {row["unit"] for row in trial if row["treatment"]} == {expected_treated}
        assert sum(not row["treatment"] for row in trial) == 2
    first = {row["unit"]: row for row in rows if row["trial_id"] == "schedule:0"}
    assert {unit: row["y"] for unit, row in first.items()} == {
        "upgraded": 30 if metric == "net_revenue" else 1,
        "referred": 30 if metric == "net_revenue" else 1,
        "control": 0,
    }
    assert {unit: row["orders_90d"] for unit, row in first.items()} == {
        "upgraded": 1,
        "referred": 1,
        "control": 0,
    }
    assert {unit: row["sessions_90d"] for unit, row in first.items()} == {
        "upgraded": 1,
        "referred": 1,
        "control": 0,
    }


def test_grid_pair_hands_off_to_doubleml(grid_frames, library):
    doubleml = library("doubleml")
    materialized = grid_frames["upgrade", "net_revenue"]
    checked = preflight_export(materialized)
    roles = materialized.roles
    data = doubleml.DoubleMLData(
        checked.table.to_pandas(),
        y_col="y",
        d_cols=roles.treatment,
        x_cols=list(roles.features),
        cluster_cols=roles.cluster_unit,
    )
    assert data.n_obs == 12
    assert data.cluster_cols == ["unit"]
    assert data.cluster_vars[:, 0].tolist() == checked.table.column("unit").to_pylist()
    assert data.x.shape == (12, 2)
