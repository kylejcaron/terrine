from __future__ import annotations

import ibis
import pyarrow.parquet as pq
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, FixedDate, agg


def _staged_dataset(native_inputs, tmp_path):
    from terrine.dataset.reference import stage_frame

    catalog, source_con = native_inputs
    con = ibis.duckdb.connect(str(tmp_path / "study.duckdb"))
    for name in ("events", "labels", "users"):
        con.create_table(name, source_con.table(name).execute())
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-31"),
            "labels": Coverage("2025-01-01", "2025-01-31"),
        },
    )
    frame = Design(
        "user",
        FixedDate("2025-01-05"),
        features={"sessions": agg("sessions", window=("-7d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "7d"))},
    ).build(model)
    return stage_frame(frame, "training", budget=Budget(strict_scan=False)), con


def test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite(
    native_inputs, tmp_path
):
    dataset, con = _staged_dataset(native_inputs, tmp_path)
    destination = tmp_path / "export"
    export = dataset.export_parquet(destination=str(destination), target_file_bytes=128)

    assert export.row_count == dataset.manifest.generation.row_count
    shards = sorted(destination.glob("*.parquet"))
    assert len(shards) == len(export.shards)
    parquet = pq.ParquetFile(shards[0])
    assert parquet.schema_arrow.names == [column.name for column in dataset.manifest.columns]
    assert dataset.export_parquet(destination=str(destination), target_file_bytes=128) == export
    conflict = tmp_path / "conflict"
    conflict.mkdir()
    with pytest.raises(TerrineError) as error:
        dataset.export_parquet(destination=str(conflict), target_file_bytes=128)
    assert error.value.code == "terrine.dataset.export_conflict"
    assert list(conflict.iterdir()) == []
    con.disconnect()
