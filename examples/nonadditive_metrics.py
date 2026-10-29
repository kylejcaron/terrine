"""Execute exact nonadditive windows through the native Catalog/Design/Frame API."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Coverage, Model, UnitSource
from terrine.spec import Design, FixedDate, agg, quantile


def main() -> None:
    con = ibis.duckdb.connect()
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["u-1", "u-1", "u-1", "u-2"],
                "occurred_at": [
                    datetime(2025, 3, 1, 9, tzinfo=UTC),
                    datetime(2025, 3, 1, 10, tzinfo=UTC),
                    datetime(2025, 3, 1, 11, tzinfo=UTC),
                    datetime(2025, 3, 1, 10, tzinfo=UTC),
                ],
                "amount": [1, 2, 8, None],
            }
        ),
    )
    con.create_table("users", pa.table({"id": ["u-1", "u-2", "u-3"]}))
    catalog = {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "occurred_at",
                    "units": {"user": "user_id"},
                    "events": ["amount"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "events": {"amount": {"source": "events", "column": "amount"}},
            "properties": {},
            "dimensions": {},
            "measures": {
                "amount_sum": {"event": "amount", "aggregation": "sum"},
                "amount_min": {"event": "amount", "aggregation": "min"},
                "amount_max": {"event": "amount", "aggregation": "max"},
                "amount_distinct": {"event": "amount", "aggregation": "count_distinct"},
            },
            "metrics": {
                "amount": {"kind": "mean", "unit": "user", "measure": "amount_sum"},
                "minimum": {"kind": "mean", "unit": "user", "measure": "amount_min"},
                "maximum": {"kind": "mean", "unit": "user", "measure": "amount_max"},
                "distinct": {
                    "kind": "mean",
                    "unit": "user",
                    "measure": "amount_distinct",
                },
            },
        }
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-03-01", "2025-03-01")},
    )
    try:
        design = Design(
            "user",
            FixedDate(datetime(2025, 3, 2, tzinfo=UTC)),
            features={
                "p25": quantile("amount", "0.25", window=("-1d", "0d")),
                "minimum": agg("minimum", window=("-1d", "0d")),
                "maximum": agg("maximum", window=("-1d", "0d")),
                "distinct": agg("distinct", window=("-1d", "0d")),
            },
        )
        print(design.build(model).materialize().to_arrow().to_pylist())
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
