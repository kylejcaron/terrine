from __future__ import annotations

import ibis
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.dataset.reference import Dataset
from terrine.model import Model
from terrine.spec import Design, FixedDate, agg


def _durable_frame(native_inputs, tmp_path):
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
    return frame, con


def test_frame_stage_publishes_and_reads_its_pinned_generation(
    native_inputs, tmp_path, monkeypatch
):
    frame, con = _durable_frame(native_inputs, tmp_path)

    def collect_forbidden(*args, **kwargs):
        raise AssertionError("staging must stream the compiled output")

    monkeypatch.setattr(type(frame), "materialize", collect_forbidden)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    pinned = Dataset.from_manifest(dataset.manifest, con=con)
    original_generation = dataset.manifest.generation.generation_id

    assert dataset.manifest.generation.dataset_id == "training"
    assert sum(batch.num_rows for batch in dataset.to_arrow_batches(batch_size=1)) == (
        dataset.manifest.generation.row_count
    )
    assert all(batch.num_rows <= 1 for batch in pinned.to_arrow_batches(batch_size=1))

    newer = frame.stage("training", budget=Budget(strict_scan=False))
    assert newer.manifest.generation.generation_id != original_generation
    assert sum(batch.num_rows for batch in pinned.to_arrow_batches(batch_size=2)) == (
        dataset.manifest.generation.row_count
    )
    con.disconnect()


def test_invalid_arrow_batch_size_refuses_before_read(native_inputs, tmp_path):
    from terrine.errors import TerrineError

    frame, con = _durable_frame(native_inputs, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    with pytest.raises(TerrineError) as error:
        dataset.to_arrow_batches(batch_size=0)
    assert error.value.code == "terrine.dataset.invalid_batch_size"
    con.disconnect()
