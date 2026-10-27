"""Local DuckDB, Arrow, Parquet and optional bounded dataframe hand-offs."""

from __future__ import annotations

import json
from decimal import Decimal
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
import pyarrow as pa
import pyarrow.parquet as pq

from terrine import Budget, Coverage, Design, FixedDate, Model, UnitSource, agg
from terrine.data import MaterializedFrame


def _assert_batches_match(batches, expected: pa.Table) -> None:
    rows_seen = 0
    for batch in batches:
        assert batch.schema.names == expected.schema.names
        for row in batch.to_pylist():
            assert rows_seen < expected.num_rows
            assert row == {
                name: expected[name][rows_seen].as_py() for name in expected.column_names
            }
            rows_seen += 1
    assert rows_seen == expected.num_rows


def main() -> None:
    with TemporaryDirectory(prefix="terrine-interop-") as temporary:
        root = Path(temporary)
        con = ibis.duckdb.connect(str(root / "datasets.duckdb"))
        try:
            con.raw_sql("CREATE TABLE users AS SELECT 1 AS user_id")
            con.raw_sql("""
                CREATE TABLE events AS SELECT 1 AS user_id,
                    TIMESTAMPTZ '2025-01-01 12:00:00+00' AS ts,
                    CAST(9007199254740993.01 AS DECIMAL(20, 2)) AS amount
            """)
            catalog = {
                    "schema_version": 1,
                    "day_boundary": "UTC",
                    "dialect": "duckdb",
                    "sources": {
                        "events": {
                            "relation": {"kind": "table", "name": "events"},
                            "timestamp": "ts",
                            "entities": {"user": "user_id"},
                            "facts": ["amount"],
                            "properties": [],
                            "dimensions": [],
                        }
                    },
                    "facts": {"amount": {"source": "events", "column": "amount"}},
                    "properties": {},
                    "dimensions": {},
                    "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
                    "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
                }
            model = Model.from_catalog(
                catalog,
                con=con,
                units={"user": UnitSource("users", "user_id")},
                readiness={"events": Coverage("2025-01-01", "2025-01-03")},
            )
            frame = Design(
                "user",
                FixedDate("2025-01-02"),
                features={"amount": agg("total", window=("-1d", "0s"))},
            ).build(model)
            budget = Budget(max_rows=10, max_output_bytes=1_000_000)
            arrow = frame.to_pyarrow(budget=budget)
            metadata = MaterializedFrame.from_arrow(arrow)
            assert metadata.roles.features == ("amount",)
            for extra in ("pandas", "polars"):
                if find_spec(extra) is None:
                    print(f"Optional {extra}: install terrine[{extra}] to run conversion")
                else:
                    converted = getattr(frame, f"to_{extra}")(budget=budget)
                    assert len(converted) == arrow.num_rows
                    print(f"Bounded {extra} conversion: verified")
            dataset = frame.stage("common_stack", budget=budget)
            assert arrow["amount"][0].as_py() == Decimal("9007199254740993.01")
            _assert_batches_match(
                dataset.to_arrow_batches(batch_size=1),
                arrow,
            )
            frame_metadata = json.loads(dataset.manifest.to_json())["frame_metadata"]
            assert frame_metadata["roles"] == metadata.to_metadata()["roles"]
            destination = root / "parquet"
            manifest = dataset.export_parquet(destination=str(destination), budget=budget)

            def parquet_batches():
                for shard in manifest.shards:
                    parquet = pq.ParquetFile(destination / shard.path)
                    schema = parquet.schema_arrow
                    assert b"terrine.materialized_frame.v2" in schema.metadata
                    restored = MaterializedFrame.from_arrow(
                        pa.Table.from_batches([], schema=schema)
                    )
                    assert restored.roles == metadata.roles
                    yield from parquet.iter_batches(batch_size=1)

            _assert_batches_match(parquet_batches(), arrow)
            print("Incremental Arrow stream and Parquet hand-off: verified")
            print("Provenance: retained in Arrow schema and pinned Dataset manifest")
        finally:
            con.disconnect()


if __name__ == "__main__":
    main()
