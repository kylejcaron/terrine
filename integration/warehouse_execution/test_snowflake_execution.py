"""Live Snowflake execution probes against the DuckDB reference backend."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from integration.warehouse_execution.snowflake_fixtures import (
    snowflake_scratch as snowflake_scratch,  # noqa: F401
)

pytestmark = pytest.mark.snowflake


def test_scratch_schema_is_isolated_and_round_trips_exact_values(snowflake_scratch):  # noqa: F811
    scratch = snowflake_scratch
    scratch.sql(
        'CREATE TABLE "events"("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ, '
        '"amount" NUMBER(12, 2))'
    )
    scratch.sql(
        "INSERT INTO \"events\" VALUES ('a', '2025-01-01 12:00:00.000001 +0000'::TIMESTAMP_TZ, 1.5)"
    )
    table = scratch.con.table("events", database=(scratch.database, scratch.schema))
    row = scratch.con.to_pyarrow(table).to_pylist()[0]
    assert row == {
        "user_id": "a",
        "occurred_at": datetime(2025, 1, 1, 12, 0, 0, 1, tzinfo=UTC),
        "amount": Decimal("1.50"),
    }
    assert scratch.schema.startswith("TERRINE_CI_")
    assert scratch.con.current_database == scratch.schema
