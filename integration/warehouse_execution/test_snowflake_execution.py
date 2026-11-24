"""Live Snowflake execution probes against the DuckDB reference backend."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from integration.warehouse_execution.snowflake_fixtures import connect_snowflake
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


def test_training_quantile_uses_pinned_snapshot(snowflake_scratch, monkeypatch):
    import terrine as tr
    from terrine.data import UnitSource
    from terrine.spec import approx_quantile

    scratch = snowflake_scratch
    scratch.sql('CREATE TABLE USERS ("id" VARCHAR)')
    scratch.sql("INSERT INTO USERS VALUES ('a')")
    scratch.sql('CREATE TABLE EVENTS ("id" VARCHAR, "ts" TIMESTAMP_TZ(6), "amount" INTEGER)')
    scratch.sql("INSERT INTO EVENTS VALUES ('a', '2025-01-02 12:00:00 +0000', 7)")
    model = tr.Model.from_tables(
        scratch.con,
        units={"user": UnitSource("USERS", "id")},
        events={
            "visit": tr.Events(
                "EVENTS", time="ts", key="id", metrics={"spend": tr.sum("amount")},
                complete=("2025-01-01", "2025-01-04"),
            ),
        },
    )
    training = tr.Design(
        "user", tr.FixedDate("2025-01-02"),
        outcomes={"q": approx_quantile("spend", "0.5", window=("0s", "1d"))},
    ).build(model).training_set(label="q")
    open_study = tr.Model._study_execution

    @contextmanager
    def change_source_after_pin(self, *args, **kwargs):
        with open_study(self, *args, **kwargs) as study:
            writer = connect_snowflake(scratch.schema)
            try:
                with writer.con.cursor() as cursor:
                    cursor.execute('UPDATE EVENTS SET "amount" = 70')
                writer.con.commit()
            finally:
                writer.disconnect()
            yield study

    def read_label():
        table = training.materialize(budget=tr.Budget(strict_scan=False)).table
        return table.select(["q", "q__complete", "q__reason"]).to_pylist()

    with monkeypatch.context() as patch:
        patch.setattr(tr.Model, "_study_execution", change_source_after_pin)
        assert read_label() == [{"q": 7, "q__complete": True, "q__reason": None}]
    assert read_label() == [{"q": 70, "q__complete": True, "q__reason": None}]
