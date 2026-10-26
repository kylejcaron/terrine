"""Local Model/Design training data, pinned batches, Parquet, and corrected refresh."""

from __future__ import annotations

import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import ibis
import pyarrow.parquet as pq

from terrine import Budget, Coverage, Design, FixedDate, Model, UnitSource, agg, asof
from terrine.dataset.manifest import DatasetManifest
from terrine import Dataset
from terrine.dataset import GroupSplit
from terrine.runtime.changes import Change, ChangeSet


def catalog() -> dict[str, Any]:
    return {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                name: {
                    "relation": {"kind": "table", "name": name},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": [fact],
                    "properties": ["segment"] if name == "events" else [],
                    "dimensions": [],
                }
                for name, fact in (("events", "event_amount"), ("labels", "label_amount"))
            },
            "facts": {
                "event_amount": {"source": "events", "column": "amount"},
                "label_amount": {"source": "labels", "column": "amount"},
            },
            "properties": {
                "segment": {
                    "source": "events",
                    "column": "segment",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {
                "event_total": {"fact": "event_amount", "aggregation": "sum"},
                "label_total": {"fact": "label_amount", "aggregation": "sum"},
            },
            "metrics": {
                name: {"kind": "mean", "entity": "user", "measure": name}
                for name in ("event_total", "label_total")
            },
    }

def bind_model(con: Any) -> Model:
    return Model.from_catalog(
        catalog(),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-31"),
            "labels": Coverage("2025-01-02", "2025-01-02"),
        },
    )


def create_model(path: Path) -> Model:
    con = ibis.duckdb.connect(str(path))
    con.raw_sql("CREATE TABLE users(user_id BIGINT PRIMARY KEY)")
    con.raw_sql("INSERT INTO users VALUES (1), (2)")
    con.raw_sql(
        "CREATE TABLE events(ts TIMESTAMP, user_id BIGINT, amount DECIMAL(38,0), segment VARCHAR)"
    )
    con.raw_sql(
        "INSERT INTO events VALUES "
        "('2025-01-01 09:00:00', 1, 1000000000000000000000000000003, 'gold'), "
        "('2025-01-01 10:00:00', 2, 1000000000000000000000000000004, 'silver')"
    )
    con.raw_sql("CREATE TABLE labels(ts TIMESTAMP, user_id BIGINT, amount DECIMAL(38,0))")
    con.raw_sql("INSERT INTO labels VALUES ('2025-01-02 12:00:00', 1, 7)")
    return bind_model(con)


def training_design() -> Design:
    return Design(
        "user",
        FixedDate("2025-01-02"),
        features={
            "past_amount": agg("event_total", window=("-1d", "0s")),
            "segment": asof("segment", before="0s"),
        },
        outcomes={"following_amount": agg("label_total", window=("0s", "1d"))},
    )


def amounts(dataset: Dataset) -> dict[int, Decimal]:
    """Consume each bounded batch; this example retains only two scalar summaries."""
    values = {}
    for batch in dataset.to_arrow_batches(batch_size=1):
        assert batch.num_rows <= 1
        for row in batch.to_pylist():
            values[row["unit"]] = row["past_amount"]
    return values


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="terrine-training-") as directory:
        root = Path(directory)
        os.environ["TERRINE_RUNTIME_NAMESPACE"] = "ml_example"
        os.environ["TERRINE_RUNTIME_SNAPSHOT_ROOT"] = str(root / "snapshots")
        model = create_model(root / "owner.duckdb")
        try:
            budget = Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
            training = (
                training_design()
                .build(model)
                .training_set(
                    label="following_amount",
                    incomplete_labels="keep",
                    split=GroupSplit(seed=17),
                )
            )
            dataset = training.stage("training", budget=budget)
            original = amounts(dataset)
            manifest_path = root / "dataset.json"
            manifest_path.write_bytes(dataset.manifest.to_json())
            reopened = Dataset.from_manifest(
                DatasetManifest.from_json(manifest_path.read_bytes()),
                con=model.con,
            )
            export = reopened.export_parquet(
                destination=str(root / "parquet"),
                partition_by=("__terrine_split",),
                target_file_bytes=64_000,
                budget=budget,
            )
            for shard in export.shards:
                parquet = pq.ParquetFile(root / "parquet" / shard.path)
                assert parquet.schema_arrow.metadata
                assert parquet.schema_arrow.field("past_amount").type.precision == 38
                for batch in parquet.iter_batches(batch_size=1):
                    assert batch.num_rows == 1
            model.con.raw_sql("UPDATE events SET amount=amount+5 WHERE user_id=1")
            changes = ChangeSet((Change("update", "events"),), "operator")
            assert reopened.plan_refresh(model, changes=changes, budget=budget).tasks
            refreshed = reopened.refresh(model, changes=changes, budget=budget)
            corrected = amounts(refreshed)
            assert amounts(reopened) == original
            assert int(corrected[1]) == int(original[1]) + 5
            print(
                json.dumps(
                    {
                        "workflow": "training",
                        "rows": dataset.manifest.generation.row_count,
                        "export_rows": export.row_count,
                        "refresh_changed": refreshed.manifest.generation
                        != dataset.manifest.generation,
                        "schema_metadata": bool(dataset.manifest.frame_metadata),
                        "corrected_amount": str(corrected[1]),
                    },
                    sort_keys=True,
                )
            )
        finally:
            model.con.disconnect()


if __name__ == "__main__":
    main()
