"""Bind simple event tables and materialize scheduled and cumulative datasets."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Budget, Events, Model, UnitSource, agg, recipes


def main() -> None:
    con = ibis.duckdb.connect()
    try:
        con.create_table("customers", pa.table({"id": ["active", "quiet"]}))
        con.create_table(
            "orders",
            pa.table(
                {
                    "customer_id": ["active", "active"],
                    "occurred_at": [
                        datetime(2025, 1, 2, tzinfo=UTC),
                        datetime(2025, 1, 10, tzinfo=UTC),
                    ],
                    "amount": [7, 13],
                }
            ),
        )
        model = Model.from_tables(
            con,
            units={"customer": UnitSource("customers", "id")},
            events={
                "orders": Events(
                    "orders",
                    "occurred_at",
                    {"customer": "customer_id"},
                    values=("amount",),
                    complete=("2025-01-01", "2025-01-31"),
                )
            },
        )
        # Use Model.from_catalog for filters, ratios, static/as-of or SCD2
        # attributes, SQL relations, explicit names, and multi-unit sources.
        assert set(model.catalog.events) == {"orders", "orders_amount"}
        assert set(model.catalog.metrics) == {
            "orders_count",
            "orders_amount_sum",
            "orders_amount_mean",
        }
        budget = Budget(max_rows=100, max_output_bytes=1_000_000)
        outcomes = {
            "count": agg("orders_count", window=("0d", "7d")),
            "amount": agg("orders_amount_sum", window=("0d", "7d")),
            "mean": agg("orders_amount_mean", window=("0d", "7d")),
        }
        scheduled = (
            recipes.time_series(
                "customer",
                every="7d",
                start="2025-01-01",
                end="2025-01-08",
                outcomes=outcomes,
            )
            .build(model)
            .materialize(budget)
        )
        by_unit = {row["unit"]: row for row in scheduled.table.to_pylist()}
        assert set(by_unit) == {"active", "quiet"}
        assert by_unit["active"]["count"] == 1
        assert by_unit["active"]["amount"] == 7
        assert by_unit["active"]["mean"] == 7
        assert by_unit["quiet"]["count"] == 0
        assert by_unit["quiet"]["amount"] == 0
        assert by_unit["quiet"]["mean"] is None
        mean_reason = scheduled.columns["mean"].reason_column
        assert mean_reason is not None
        assert by_unit["quiet"][mean_reason] == "no_observations"

        incomplete = (
            recipes.time_series(
                "customer",
                every="7d",
                start="2025-01-29",
                end="2025-01-30",
                outcomes=outcomes,
            )
            .build(model)
            .materialize(budget)
        )
        for row in incomplete.table.to_pylist():
            for name in outcomes:
                assert row[name] is None
                reason = incomplete.columns[name].reason_column
                assert reason is not None
                assert row[reason] == "incomplete_coverage"

        # Age a sums [t0, t0 + (a + 1) * period); integer amounts are exact.
        ltv = (
            recipes.ltv(
                "customer",
                start_event="orders",
                metric="orders_amount_sum",
                period="7d",
                periods=2,
            )
            .build(model)
            .materialize(budget)
        )
        rows = sorted(ltv.table.to_pylist(), key=lambda row: row["age"])
        assert [row["unit"] for row in rows] == ["active", "active"]
        assert [row["cumulative_value"] for row in rows] == [7, 20]
        assert all(row["cumulative_value__complete"] for row in rows)
        print(
            "Scheduled roster values:",
            {
                unit: (row["count"], row["amount"], row["mean"], row[mean_reason])
                for unit, row in by_unit.items()
            },
        )
        print("Incomplete windows retain nulls and incomplete_coverage reasons.")
        print("LTV cumulative values (origin included):", [row["cumulative_value"] for row in rows])
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
