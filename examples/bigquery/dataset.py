"""Build a point-in-time dataset directly from BigQuery tables.

Install the extras, authenticate (application-default credentials or
``GOOGLE_APPLICATION_CREDENTIALS``) and name a project you can create a dataset in:

    uv pip install 'terrine[bigquery,duckdb]'
    TERRINE_BIGQUERY_PROJECT=my-project python examples/bigquery/dataset.py

The example creates its own dataset, reads it through one snapshot transaction
in a private session and drops the dataset afterwards. ``Frame.materialize()``
runs on BigQuery; staging and publishing a Dataset generation stay on DuckDB.
Queries are billed to the project (this one scans a few kilobytes).
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
from google.cloud import bigquery

import terrine as tr

CATALOG = """schema_version: 1
day_boundary: UTC
dialect: bigquery
sources:
  orders:
    relation: {kind: table, name: orders}
    timestamp: ordered_at
    entities: {customer: customer_id}
    facts: [order, amount]
    properties: []
    dimensions: []
facts:
  order: {source: orders, column: null}
  amount: {source: orders, column: amount}
properties: {}
dimensions: {}
measures:
  order_count: {fact: order, aggregation: count}
  amount_total: {fact: amount, aggregation: sum}
metrics:
  orders: {kind: mean, entity: customer, measure: order_count}
  spend: {kind: mean, entity: customer, measure: amount_total}
"""


def main() -> None:
    project = os.environ.get("TERRINE_BIGQUERY_PROJECT") or os.environ.get(
        "TERRINE_TEST_BIGQUERY_PROJECT"
    )
    if not project:
        raise SystemExit("Set TERRINE_BIGQUERY_PROJECT to a project you can create a dataset in")
    dataset = f"terrine_example_{uuid.uuid4().hex[:8]}"
    con = ibis.bigquery.connect(project_id=project, dataset_id=dataset, location="US")
    client = con.client
    client.query(f"CREATE SCHEMA `{project}.{dataset}` OPTIONS(location='US')").result()
    in_dataset = bigquery.QueryJobConfig(default_dataset=f"{project}.{dataset}")
    try:
        for statement in (
            "CREATE TABLE customers(customer_id STRING)",
            "INSERT customers VALUES ('c1'), ('c2')",
            # NUMERIC holds exact decimal amounts.
            "CREATE TABLE orders(customer_id STRING, ordered_at TIMESTAMP, amount NUMERIC(12, 2))",
            "INSERT orders VALUES "
            "('c1', TIMESTAMP '2025-01-01 09:00:00+00', 20.00), "
            "('c1', TIMESTAMP '2025-01-02 10:00:00+00', 5.50), "
            "('c2', TIMESTAMP '2025-01-02 18:00:00+00', 12.25)",
        ):
            client.query(statement, job_config=in_dataset).result()
        with TemporaryDirectory(prefix="terrine-bigquery-") as directory:
            catalog = Path(directory) / "catalog.yaml"
            catalog.write_text(CATALOG, encoding="utf-8")
            model = tr.Model.from_catalog(
                catalog,
                con=con,
                units={"customer": tr.UnitSource("customers", "customer_id")},
                readiness={"orders": tr.Coverage("2025-01-01", "2025-01-04")},
            )
            design = tr.Design(
                "customer",
                tr.FixedDate("2025-01-02"),
                features={"orders_before": tr.agg("orders", window=("-1d", "0s"))},
                outcomes={"spend_after": tr.agg("spend", window=("0s", "1d"))},
            )
            budget = tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
            result = design.build(model).materialize(budget)
        rows = sorted(
            result.table.select(["unit", "orders_before", "spend_after"]).to_pylist(),
            key=lambda row: row["unit"],
        )
        summary = [(row["unit"], row["orders_before"], row["spend_after"]) for row in rows]
        assert summary == [("c1", 1, Decimal("5.50")), ("c2", 0, Decimal("12.25"))]
        print(rows)
    finally:
        con.disconnect()
        client.query(f"DROP SCHEMA IF EXISTS `{project}.{dataset}` CASCADE").result()


if __name__ == "__main__":
    main()
