"""BigQuery execution probes at the owned client's submission boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import ibis
import pyarrow as pa
import pytest
import sqlglot
from sqlglot import exp

from integration.warehouse_execution.bigquery_fixtures import (
    bigquery_scratch as bigquery_scratch,  # noqa: F401
)
from terrine import Budget, Design, FixedDate, agg, preview
from tests.execution.test_bigquery_session import offline_bigquery
from tests.fixtures.readme import readme_model


@pytest.mark.bigquery
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


def test_native_event_partition_uses_utc_literals_in_the_submitted_study(monkeypatch):
    """A partition claim needs a direct timestamp predicate at real submission."""
    bq = pytest.importorskip("google.cloud.bigquery")
    pytest.importorskip("ibis.backends.bigquery")
    schemas = {
        "customers": ibis.schema({"customer_id": "string"}),
        "orders": ibis.schema(
            {
                "customer_id": "string",
                "ordered_at": "timestamp('UTC')",
                "month_start": "date",
                "amount": "float64",
            }
        ),
    }
    design = Design(
        "customer",
        FixedDate("2025-03-01T00:30:00+01:00"),
        features={"past": agg("spend", window=("-30d", "0s"))},
        outcomes={"future": agg("spend", window=("0s", "30d"))},
    )
    submitted = []

    class GuardedOutputSubmitted(Exception):
        pass

    def submit(sql):
        submitted.append(sql.encode())
        if sql.endswith("LIMIT 2"):
            raise GuardedOutputSubmitted
        query = sqlglot.parse_one(sql, read="bigquery")
        assert isinstance(query, exp.Select)
        projection = query.named_selects
        assert set(projection) <= {"invalid_count", "duplicate_count"}
        return SimpleNamespace(to_arrow=lambda **_: pa.table({name: [0] for name in projection}))

    with offline_bigquery(monkeypatch, schemas, submit) as (owner, admitted, controls):
        partition = bq.TimePartitioning(type_="MONTH", field="ordered_at")
        get_table = bq.Client.get_table

        def table(client, key):
            metadata = get_table(client, key)
            if metadata.table_id == "orders":
                metadata.time_partitioning = partition
            return metadata

        monkeypatch.setattr(bq.Client, "get_table", table)
        model = readme_model(owner)
        frame = design.build(model)
        controls.clear()
        report = preview(design, model)
        assert not submitted and not controls
        with pytest.raises(GuardedOutputSubmitted):
            frame.materialize(budget=Budget(max_rows=1))
        assert submitted == admitted
        assert _literal_filters(submitted[-1], "ordered_at") == {
            (exp.GTE, datetime(2025, 1, 29, 23, 30, tzinfo=UTC)),
            (exp.LT, datetime(2025, 3, 30, 23, 30, tzinfo=UTC)),
        }
        assert not _literal_filters(submitted[-1], "month_start")
        assert [(use.physical_key, use.reason) for use in report.source_reads] == [
            ("ordered_at", None)
        ]

        # A separate look-alike date column does not prove UTC event-month lineage.
        partition = bq.TimePartitioning(type_="MONTH", field="month_start")
        changed = preview(design, model)
        assert changed.source_reads[0].reason == "layout_changed"
        assert changed.source_reads[0].physical_key is None
        assert changed.source_reads[0].event_span == report.source_reads[0].event_span
        changed = preview(design, readme_model(owner))
        assert [(use.physical_key, use.reason) for use in changed.source_reads] == [
            (None, "event_lineage_unverified")
        ]

        partition = None
        assert preview(design, readme_model(owner)).source_reads[0].reason == "layout_unverified"

        # An ingestion partition may contain older events; never restrict it by event bounds.
        partition = bq.TimePartitioning(type_="MONTH")
        model = readme_model(owner)
        assert preview(design, model).source_reads[0].reason == "ingestion_time_partition"
        with pytest.raises(GuardedOutputSubmitted):
            design.build(model).materialize(budget=Budget(max_rows=1))
        assert _literal_filters(submitted[-1], "ordered_at")
        assert not _literal_filters(submitted[-1], "_PARTITIONTIME")
        assert not _literal_filters(submitted[-1], "PARTITIONTIME")


def _literal_filters(sql, column):
    tree = sqlglot.parse_one(sql.decode(), read="bigquery")
    return {
        (type(predicate), datetime.fromisoformat(literal.this))
        for where in tree.find_all(exp.Where)
        for predicate in where.this.find_all(exp.GTE, exp.LT)
        if isinstance(predicate.this, exp.Column) and predicate.this.name == column
        if (literal := predicate.expression.find(exp.Literal)) is not None and literal.is_string
    }
