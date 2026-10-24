"""Build a point-in-time dataset directly from Snowflake tables.

Install the extras and point the example at a database your key-pair service
user can create a schema in:

    uv pip install 'terrine[snowflake,duckdb]'
    TERRINE_SNOWFLAKE_ACCOUNT=... TERRINE_SNOWFLAKE_USER=... \
    TERRINE_SNOWFLAKE_PRIVATE_KEY="$(cat rsa_key.p8)" \
    TERRINE_SNOWFLAKE_WAREHOUSE=... TERRINE_SNOWFLAKE_DATABASE=... \
        python examples/snowflake/dataset.py

``TERRINE_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`` is optional; the ``TERRINE_TEST_SNOWFLAKE_*``
settings of the live probes are accepted too. The example creates its own schema,
reads it pinned to one Time Travel instant on the connection and drops the
schema afterwards. ``Frame.materialize()`` runs on Snowflake; staging and
publishing a Dataset generation stay on DuckDB. Warehouse time is billed.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
from cryptography.hazmat.primitives import serialization

import terrine as tr

CATALOG = """schema_version: 1
day_boundary: UTC
dialect: snowflake
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
_SETTINGS = ("ACCOUNT", "USER", "PRIVATE_KEY", "WAREHOUSE", "DATABASE")


def _setting(name: str, required: bool = True) -> str | None:
    for prefix in ("TERRINE_SNOWFLAKE_", "TERRINE_TEST_SNOWFLAKE_"):
        value = os.environ.get(prefix + name)
        if value:
            return value
    if required:
        raise SystemExit(f"Set TERRINE_SNOWFLAKE_{name}")
    return None


def _private_key() -> bytes:
    passphrase = _setting("PRIVATE_KEY_PASSPHRASE", required=False)
    key = _setting("PRIVATE_KEY")
    assert key is not None
    parsed = serialization.load_pem_private_key(
        key.encode(), password=passphrase.encode() if passphrase else None
    )
    return parsed.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect(schema: str | None):
    settings = {name.lower(): _setting(name) for name in _SETTINGS if name != "PRIVATE_KEY"}
    return ibis.snowflake.connect(
        private_key=_private_key(), schema=schema, create_object_udfs=False, **settings
    )


def _run(con, statement: str) -> None:
    with con.con.cursor() as cursor:
        cursor.execute(statement)


def main() -> None:
    schema = f"TERRINE_EXAMPLE_{uuid.uuid4().hex[:8].upper()}"
    admin = _connect(None)
    database = admin.current_catalog
    try:
        _run(admin, f'CREATE SCHEMA "{database}"."{schema}"')
        con = _connect(schema)
        try:
            # Unquoted names fold to upper case; quoted columns keep the catalog's spelling.
            _run(con, 'CREATE TABLE customers("customer_id" VARCHAR)')
            _run(con, "INSERT INTO customers VALUES ('c1'), ('c2')")
            _run(
                con,
                'CREATE TABLE orders("customer_id" VARCHAR, "ordered_at" TIMESTAMP_TZ, '
                '"amount" NUMBER(12, 2))',
            )
            _run(
                con,
                "INSERT INTO orders VALUES "
                "('c1', '2025-01-01 09:00:00 +0000', 20.00), "
                "('c1', '2025-01-02 10:00:00 +0000', 5.50), "
                "('c2', '2025-01-02 18:00:00 +0000', 12.25)",
            )
            with TemporaryDirectory(prefix="terrine-snowflake-") as directory:
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
    finally:
        _run(admin, f'DROP SCHEMA IF EXISTS "{database}"."{schema}" CASCADE')
        admin.disconnect()


if __name__ == "__main__":
    main()
