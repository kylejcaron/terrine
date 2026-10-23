"""Live BigQuery execution probes against the DuckDB reference backend."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from integration.warehouse_execution.bigquery_fixtures import (
    bigquery_scratch as bigquery_scratch,  # noqa: F401
)

pytestmark = pytest.mark.bigquery


def test_scratch_dataset_is_isolated_and_round_trips_exact_values(bigquery_scratch):  # noqa: F811
    scratch = bigquery_scratch
    scratch.sql("CREATE TABLE events(user_id STRING, occurred_at TIMESTAMP, amount NUMERIC(12, 2))")
    scratch.sql("INSERT events VALUES ('a', TIMESTAMP '2025-01-01 12:00:00.000001+00', 1.5)")
    table = scratch.con.table("events", database=scratch.dataset)
    row = scratch.con.to_pyarrow(table).to_pylist()[0]
    assert row == {
        "user_id": "a",
        "occurred_at": datetime(2025, 1, 1, 12, 0, 0, 1, tzinfo=UTC),
        "amount": Decimal("1.50"),
    }
    assert scratch.dataset.startswith("terrine_ci_")
