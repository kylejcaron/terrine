"""Published Increment definitions executed in a native Terrine frame."""

import ibis
import pytest

import terrine as tr
from terrine.data import Coverage
from terrine.model import Model

increment_semantics = pytest.importorskip("increment.semantics")


def test_unfiltered_legacy_additives_keep_null_rows_and_completeness():
    from increment.semantics.models import ConversionMetric, Fact, FactSource, MeanMetric

    definitions = increment_semantics.Definitions(
        fact_sources=(
            FactSource(
                name="orders",
                sql="SELECT * FROM orders",
                timestamp_column="occurred_at",
                entities=("customer",),
                facts=(Fact(name="spend", column="amount"), Fact(name="visit")),
            ),
        ),
        metrics=(
            MeanMetric(name="sum", fact="spend", entity="customer", aggregation="sum"),
            MeanMetric(name="count", fact="visit", entity="customer", aggregation="count"),
            ConversionMetric(name="conversion", fact="visit", entity="customer"),
        ),
    )
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE customers AS SELECT * FROM (VALUES (1), (2)) t(id)")
        con.raw_sql(
            "CREATE TABLE orders AS SELECT * FROM "
            "(VALUES (1, TIMESTAMP '2025-01-01 12:00:00', 4), "
            "        (1, TIMESTAMP '2025-01-01 13:00:00', NULL)) "
            "t(customer, occurred_at, amount)"
        )
        model = Model.from_increment(
            definitions,
            con=con,
            units={"customer": tr.UnitSource("customers", "id")},
            readiness={"orders": Coverage("2025-01-01", "2025-01-01")},
        )
        frame = tr.Design(
            "customer",
            tr.FixedDate("2025-01-01"),
            outcomes={
                "spend": tr.agg("sum", window=("0s", "1d")),
                "visits": tr.agg("count", window=("0s", "1d")),
                "converted": tr.agg("conversion", window=("0s", "1d")),
                "incomplete": tr.agg("sum", window=("0s", "2d")),
            },
        ).build(model)
        rows = {row["unit"]: row for row in frame.to_pyarrow().to_pylist()}
    finally:
        con.disconnect()
    assert {
        unit: (row["spend"], row["visits"], row["converted"]) for unit, row in rows.items()
    } == {1: (4, 2, True), 2: (0, 0, False)}
    for row in rows.values():
        assert row["incomplete"] is None
        assert row["incomplete__reason"] == "incomplete_coverage"
