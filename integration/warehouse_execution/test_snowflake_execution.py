"""Live Snowflake execution probes against the DuckDB reference backend."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from integration.warehouse_execution.snowflake_fixtures import (
    snowflake_scratch as snowflake_scratch,  # noqa: F401
)

pytestmark = pytest.mark.snowflake


def test_unread_and_partial_readers_release_snapshot_cursors(
    snowflake_scratch, tmp_path, monkeypatch
):
    from integration.warehouse_execution.test_snowflake_admission import _events, _model
    from terrine.execution.inputs import open_execution

    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql("INSERT INTO events SELECT * FROM events")
    model = _model(scratch, tmp_path)
    with open_execution(model.catalog, scratch.con) as context:
        query = context.resolve(model.catalog.sources["sessions"].relation).select("user_id")
        raw = scratch.con.con
        open_cursor = raw.cursor
        cursors = []

        def track(*args, **kwargs):
            cursor = open_cursor(*args, **kwargs)
            cursors.append(cursor)
            return cursor

        monkeypatch.setattr(raw, "cursor", track)
        for partial in (False, True):
            reader = context.to_arrow_batches(query, chunk_size=1)
            assert any(not cursor.is_closed() for cursor in cursors)
            if partial:
                assert next(reader).num_rows == 1
            reader.close()
            assert all(cursor.is_closed() for cursor in cursors)
            assert context.execute(query.count()).as_py() == 2


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
