"""Build a point-in-time dataset directly from PostgreSQL tables.

Install the extras and point the example at a database you can create a schema in:

    uv pip install 'terrine[postgres]'
    TERRINE_POSTGRES_URL=postgres://user:password@localhost:5432/analytics \
        python examples/postgres/dataset.py

The example creates its own schema, reads it through one consistent read-only
snapshot and drops the schema afterwards. ``Frame.materialize()`` runs on
PostgreSQL; staging and publishing a Dataset generation stay on DuckDB.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis

import terrine as tr

CATALOG = """schema_version: 2
day_boundary: UTC
dialect: postgres
sources:
  orders:
    relation: {kind: table, name: orders}
    time: ordered_at
    units: {customer: customer_id}
    events: [order, amount]
    properties: []
    dimensions: []
events:
  order: {source: orders, column: null}
  amount: {source: orders, column: amount}
properties: {}
dimensions: {}
measures:
  order_count: {event: order, aggregation: count}
  amount_total: {event: amount, aggregation: sum}
metrics:
  orders: {kind: mean, unit: customer, measure: order_count}
  spend: {kind: mean, unit: customer, measure: amount_total}
"""


def main() -> None:
    url = os.environ.get("TERRINE_POSTGRES_URL")
    if not url:
        raise SystemExit("Set TERRINE_POSTGRES_URL=postgres://user:password@host:port/database")
    schema = f"terrine_example_{uuid.uuid4().hex[:8]}"
    admin = ibis.connect(url)
    admin.raw_sql(f'CREATE SCHEMA "{schema}"').close()
    try:
        con = ibis.connect(url, schema=schema)
        try:
            con.raw_sql("CREATE TABLE customers(customer_id text)").close()
            con.raw_sql("INSERT INTO customers VALUES ('c1'), ('c2')").close()
            # Exact decimal amounts need a declared precision and scale.
            con.raw_sql(
                "CREATE TABLE orders(customer_id text, ordered_at timestamptz, "
                "amount numeric(12, 2))"
            ).close()
            con.raw_sql(
                "INSERT INTO orders VALUES "
                "('c1', '2025-01-01 09:00:00+00', 20.00), "
                "('c1', '2025-01-02 10:00:00+00', 5.50), "
                "('c2', '2025-01-02 18:00:00+00', 12.25)"
            ).close()
            with TemporaryDirectory(prefix="terrine-postgres-") as directory:
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
            summary = [(row["unit"], row["orders_before"], str(row["spend_after"])) for row in rows]
            assert summary == [("c1", 1, "5.50"), ("c2", 0, "12.25")]
            print(rows)
        finally:
            con.disconnect()
    finally:
        admin.raw_sql(f'DROP SCHEMA "{schema}" CASCADE').close()
        admin.disconnect()


if __name__ == "__main__":
    main()
