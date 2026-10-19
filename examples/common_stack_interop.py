"""Local DuckDB, Arrow, Parquet and optional bounded dataframe hand-offs."""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

import ibis
import pyarrow as pa
import pyarrow.parquet as pq

from terrine import Budget, Coverage, Design, FixedDate, Model, UnitSource, agg
from terrine.data import MaterializedFrame
from terrine.execution.backend import backend_capabilities
from terrine.semantic.models import Catalog


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
            catalog = Catalog.model_validate(
                {
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
            )
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
            batches = list(dataset.to_arrow_batches(batch_size=1))
            streamed = pa.Table.from_batches(batches)
            assert streamed.select(arrow.column_names).equals(arrow)
            destination = root / "parquet"
            manifest = dataset.export_parquet(destination=str(destination), budget=budget)
            readback = pa.concat_tables(
                [pq.ParquetFile(destination / shard.path).read() for shard in manifest.shards]
            )
            restored = MaterializedFrame.from_arrow(readback)
            assert restored.roles == metadata.roles
            assert readback.select(arrow.column_names).equals(arrow)
            assert backend_capabilities(con).native_parquet_unload is None
            print("Local Arrow stream and Parquet hand-off: verified")
            print("Provenance: retained in Arrow schema and pinned Dataset manifest")
        finally:
            con.disconnect()


if __name__ == "__main__":
    main()
