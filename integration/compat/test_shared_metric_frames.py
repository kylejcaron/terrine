"""Shared shop definitions through an ordinary Terrine frame."""

import sys
from datetime import datetime
from pathlib import Path

import ibis
import pytest

import terrine as tr
from terrine.data import BoundCoverage

pytest.importorskip("metric_definitions")

_SHOP = Path(sys.prefix) / "share/metric-definitions/examples/shop"
_UNITS = {"customer": tr.UnitSource("customers", "customer_id")}
_START = "2026-01-01T12:00:00Z"
_END = "2026-01-02T12:00:00Z"


@pytest.fixture
def shop():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql((_SHOP / "data.sql").read_text(encoding="utf-8"))
        yield con
    finally:
        con.disconnect()


def _coverage(end=_END):
    return {"orders": BoundCoverage(datetime.fromisoformat(_START), datetime.fromisoformat(end))}


def _frame(model, **metrics):
    return tr.Design(
        "customer",
        tr.FixedDate(_START),
        outcomes={name: tr.agg(metric, window=("0s", "1d")) for name, metric in metrics.items()},
    ).build(model)


def test_calculates_formulas_after_window_aggregation(shop):
    model = tr.Model.from_metric_definitions(
        _SHOP / "metrics.yaml",
        con=shop,
        units=_UNITS,
        readiness=_coverage(),
        metrics=["net_revenue", "average_order_value", "web_revenue_per_order"],
    )
    frame = _frame(model, net="net_revenue", aov="average_order_value", web="web_revenue_per_order")
    rows = {row["unit"]: row for row in frame.to_pyarrow().to_pylist()}
    assert {unit: (row["net"], row["aov"], row["web"]) for unit, row in rows.items()} == {
        "A": (55, 30, 10),
        "B": (50, 60, 60),
        "C": (0, None, None),
    }
    assert rows["C"]["aov__reason"] == "zero_denominator"


@pytest.mark.parametrize("complete", [True, False], ids=["complete", "incomplete"])
def test_round_trip_preserves_population_values_and_reasons(shop, complete):
    readiness = _coverage(_END if complete else "2026-01-01T18:00:00Z")
    model = tr.Model.from_metric_definitions(
        _SHOP / "metrics.yaml",
        con=shop,
        units=_UNITS,
        readiness=readiness,
        metrics=[
            "scaled_net_revenue",
            "revenue_per_customer",
            "purchase_conversion",
            "average_order_value",
        ],
    )
    restored = tr.Model.from_catalog(
        model.to_catalog(), con=shop, units=_UNITS, readiness=readiness
    )
    assert restored.model_hash == model.model_hash
    assert restored.catalog.schema_version == 4
    assert restored.catalog.metrics["revenue_per_customer"].population == "eligible_customers"
    roster = restored.catalog.populations["eligible_customers"]
    assert (roster.relation.name, roster.key) == ("customers", "customer_id")
    rows = (
        _frame(
            restored,
            net="scaled_net_revenue",
            revenue="revenue_per_customer",
            converted="purchase_conversion",
            aov="average_order_value",
        )
        .to_pyarrow()
        .to_pylist()
    )
    by_unit = {row["unit"]: row for row in rows}
    assert set(by_unit) == {"A", "B", "C"}
    if complete:
        assert {
            unit: (row["net"], row["revenue"], row["converted"]) for unit, row in by_unit.items()
        } == {"A": (110, 60, True), "B": (100, 60, True), "C": (0, 0, False)}
        return
    for row in rows:
        for name in ("net", "revenue", "converted", "aov"):
            assert row[name] is None
            assert row[f"{name}__complete"] is False
            assert row[f"{name}__reason"] == "incomplete_coverage"


def test_duplicate_dimension_key_refuses_before_publishing(shop):
    shop.raw_sql("INSERT INTO channels VALUES ('web', 'duplicate')")
    model = tr.Model.from_metric_definitions(
        _SHOP / "metrics.yaml",
        con=shop,
        units=_UNITS,
        readiness=_coverage(),
        metrics=["net_revenue"],
    )
    with pytest.raises(tr.TerrineError) as caught:
        _frame(model, net="net_revenue").to_pyarrow()
    assert caught.value.code == "terrine.temporal.static_key_multiplicity"
    assert caught.value.context["relation"] == "channels"
    assert caught.value.context["key"] == "channel"


def test_native_v4_conversion_does_not_require_amounts_for_presence(shop):
    shop.raw_sql("ALTER TABLE orders ALTER COLUMN gross_amount DROP NOT NULL")
    shop.raw_sql("UPDATE orders SET gross_amount = NULL")
    model = tr.Model.from_metric_definitions(
        _SHOP / "metrics.yaml",
        con=shop,
        units=_UNITS,
        readiness=_coverage(),
        metrics=["purchase_conversion", "revenue_per_customer"],
    )
    catalog = model.catalog.model_dump(mode="python")
    conversion = catalog["metrics"]["purchase_conversion"]
    conversion["measure"] = "revenue"
    catalog["metrics"]["presence_formula"] = {
        "kind": "formula",
        "unit": conversion["unit"],
        "population": conversion["population"],
        "formula": {"kind": "metric", "name": "purchase_conversion"},
    }
    native = tr.Model.from_catalog(catalog, con=shop, units=_UNITS, readiness=_coverage())
    rows = (
        _frame(native, converted="purchase_conversion", presence="presence_formula")
        .to_pyarrow()
        .to_pylist()
    )
    assert {row["unit"]: (row["converted"], row["presence"]) for row in rows} == {
        "A": (True, 1),
        "B": (True, 1),
        "C": (False, 0),
    }
    with pytest.raises(tr.TerrineError) as caught:
        _frame(native, revenue="revenue_per_customer").to_pyarrow()
    assert caught.value.code == "terrine.temporal.null_measure_value"
