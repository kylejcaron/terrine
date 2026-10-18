"""Execute exact nonadditive windows through the native Catalog/Design/Frame API."""

from datetime import UTC, datetime

import ibis
import pyarrow as pa

from terrine import Coverage, Model, UnitSource
from terrine.semantic.models import Catalog
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
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["amount"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"amount": {"source": "events", "column": "amount"}},
            "properties": {},
            "dimensions": {},
            "measures": {
                "amount_sum": {"fact": "amount", "aggregation": "sum"},
                "amount_min": {"fact": "amount", "aggregation": "min"},
                "amount_max": {"fact": "amount", "aggregation": "max"},
                "amount_distinct": {"fact": "amount", "aggregation": "count_distinct"},
            },
            "metrics": {
                "amount": {"kind": "mean", "entity": "user", "measure": "amount_sum"},
                "minimum": {"kind": "mean", "entity": "user", "measure": "amount_min"},
                "maximum": {"kind": "mean", "entity": "user", "measure": "amount_max"},
                "distinct": {
                    "kind": "mean",
                    "entity": "user",
                    "measure": "amount_distinct",
                },
            },
        }
    )
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
