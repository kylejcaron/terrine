"""The README's customer spend tables, model and monthly design."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pyarrow as pa

from terrine import recipes
from terrine.data import Events, UnitSource
from terrine.metrics import count, sum
from terrine.model import Model
from terrine.spec import Design, agg


def readme_tables() -> dict[str, pa.Table]:
    """The README rows: three customers, five orders, naive timestamps."""
    return {
        "customers": pa.table({"customer_id": ["alice", "bob", "carol"]}),
        "orders": pa.table(
            {
                "customer_id": ["alice", "alice", "alice", "alice", "bob"],
                "ordered_at": pa.array(
                    [
                        datetime(2025, 1, 3),
                        datetime(2025, 1, 20),
                        datetime(2025, 2, 10),
                        datetime(2025, 3, 5),
                        datetime(2025, 1, 5),
                    ],
                    pa.timestamp("us"),
                ),
                "amount": [30.0, 45.0, 25.0, 60.0, 80.0],
            }
        ),
    }


def readme_model(con: Any) -> Model:
    """The README binding on a connection holding ``customers`` and ``orders``."""
    return Model.from_tables(
        con,
        units={"customer": UnitSource("customers", "customer_id")},
        events={
            "orders": Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={"orders": count(), "spend": sum("amount")},
                complete=("2025-01-01", "2025-03-20"),
            )
        },
    )


def readme_design(unit: str = "customer") -> Design:
    """The README's monthly snapshots: trailing 30-day features, next 30 days of spend."""
    return recipes.time_series(
        unit,
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={
            "orders_30d": agg("orders", window=("-30d", "0s")),
            "spend_30d": agg("spend", window=("-30d", "0s")),
        },
        outcomes={"spend_next_30d": agg("spend", window=("0s", "30d"))},
    )
