from __future__ import annotations

import ibis
import pyarrow.parquet as pq
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, FixedDate, agg


def _staged_dataset(native_inputs, tmp_path, *, empty_training=False):
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
            "labels": Coverage("2025-01-01", "2025-01-01" if empty_training else "2025-01-31"),
        },
    )
    frame = Design(
        "user",
        FixedDate("2025-01-05"),
        features={"sessions": agg("sessions", window=("-7d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "7d"))},
    ).build(model)
    if empty_training:
        frame = frame.training_set(label="churn")
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
    with pytest.raises(TerrineError) as layout_error:
        dataset.export_parquet(
            destination=str(destination),
            partition_by=("sessions",),
            target_file_bytes=128,
        )
    assert layout_error.value.code == "terrine.dataset.export_conflict"
    with pytest.raises(TerrineError) as limit_error:
        dataset.export_parquet(
            destination=str(destination),
            target_file_bytes=128,
            budget=Budget(max_rows=0, strict_scan=False),
        )
    assert limit_error.value.code == "terrine.budget.rows_exceeded"
    con.disconnect()


def test_export_discards_unfinished_shard_and_rewrites_on_retry(
    native_inputs, tmp_path, monkeypatch
):
    dataset, con = _staged_dataset(native_inputs, tmp_path)
    destination = tmp_path / "resumable"
    partial = tmp_path / ".resumable.partial"
    original = dataset.to_arrow_batches
    from terrine.dataset import export as export_module

    original_partition_rows = export_module._partition_rows

    def partition_first_batch(batch, partition_by):
        if batch.num_rows < 2:
            yield from original_partition_rows(batch, partition_by)
            return
        yield from original_partition_rows(batch.slice(0, 1), partition_by)
        yield from original_partition_rows(batch.slice(1), partition_by)

    monkeypatch.setattr(export_module, "_partition_rows", partition_first_batch)

    def interrupted(*, batch_size=65_536):
        batch = next(iter(original(batch_size=batch_size)))
        yield batch
        raise OSError("simulated interruption in an unfinished shard")

    monkeypatch.setattr(
        type(dataset), "to_arrow_batches", lambda self, **kwargs: interrupted(**kwargs)
    )
    with pytest.raises(OSError, match="unfinished shard"):
        dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert len(list(partial.glob("**/*.parquet"))) == 1

    monkeypatch.setattr(
        type(dataset), "to_arrow_batches", lambda self, **kwargs: original(**kwargs)
    )

    writers = 0
    original_writer = export_module.pq.ParquetWriter

    def count_writers(*args, **kwargs):
        nonlocal writers
        writers += 1
        return original_writer(*args, **kwargs)

    monkeypatch.setattr(export_module.pq, "ParquetWriter", count_writers)
    export = dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert writers > 0
    assert export.row_count == dataset.manifest.generation.row_count
    assert sum(shard.row_count for shard in export.shards) == export.row_count
    assert all((destination / shard.path).is_file() for shard in export.shards)
    con.disconnect()


def test_export_syncs_shards_and_directories_before_checkpoint_and_publication(
    native_inputs, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(native_inputs, tmp_path)
    destination = tmp_path / "durable"
    events = []
    synced = {}
    original_fsync = os.fsync
    original_state = export_module._write_state
    original_rename = os.rename

    def identity(path):
        stat = path.stat()
        return stat.st_dev, stat.st_ino

    def sync(descriptor):
        stat = os.fstat(descriptor)
        original_fsync(descriptor)
        synced[stat.st_dev, stat.st_ino] = len(events)
        events.append("sync")

    def state(root, manifest):
        for shard in manifest.shards:
            path = root / shard.path
            position = synced.get(identity(path), -1)
            assert position >= 0, "shard data must be durable before checkpoint"
            for directory in path.parents:
                if not directory.is_relative_to(root):
                    break
                assert synced.get(identity(directory), -1) > position
        return original_state(root, manifest)

    def rename(source, target):
        manifest_position = synced[identity(source / "export-manifest.json")]
        assert synced.get(identity(source), -1) > manifest_position
        original_rename(source, target)
        events.append("rename")

    monkeypatch.setattr(export_module.os, "fsync", sync)
    monkeypatch.setattr(export_module, "_write_state", state)
    monkeypatch.setattr(export_module.os, "rename", rename)
    result = dataset.export_parquet(
        destination=str(destination), partition_by=("sessions", "churn"), target_file_bytes=1
    )
    assert result.row_count == 2
    assert synced[identity(tmp_path)] > events.index("rename")
    con.disconnect()


def test_export_does_not_checkpoint_a_shard_when_fsync_fails(native_inputs, tmp_path, monkeypatch):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(native_inputs, tmp_path)
    destination = tmp_path / "sync-retry"
    partial = tmp_path / ".sync-retry.partial"
    original_fsync = os.fsync
    failed = False

    def sync(descriptor):
        nonlocal failed
        stat = os.fstat(descriptor)
        for path in partial.glob("**/*.parquet"):
            target = path.stat()
            if not failed and (stat.st_dev, stat.st_ino) == (target.st_dev, target.st_ino):
                failed = True
                raise OSError("shard persistence interrupted")
        original_fsync(descriptor)

    monkeypatch.setattr(export_module.os, "fsync", sync)
    with pytest.raises(OSError, match="shard persistence"):
        dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    state = export_module.ExportManifest.from_json((partial / "export-state.json").read_bytes())
    assert state.shards == ()
    assert list(partial.glob("**/*.parquet")) == []
    result = dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert result.row_count == 2
    con.disconnect()


def test_export_retry_resyncs_parent_after_interrupted_publication(
    native_inputs, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(native_inputs, tmp_path)
    destination = tmp_path / "rename-retry"
    original_fsync, original_rename = os.fsync, os.rename
    renamed = False
    parent_syncs = 0
    parent = tmp_path.stat()

    def rename(source, target):
        nonlocal renamed
        original_rename(source, target)
        renamed = True

    def sync(descriptor):
        nonlocal parent_syncs
        stat = os.fstat(descriptor)
        if renamed and (stat.st_dev, stat.st_ino) == (parent.st_dev, parent.st_ino):
            parent_syncs += 1
            if parent_syncs == 1:
                raise OSError("publication directory persistence interrupted")
        original_fsync(descriptor)

    monkeypatch.setattr(export_module.os, "rename", rename)
    monkeypatch.setattr(export_module.os, "fsync", sync)
    with pytest.raises(OSError, match="publication directory"):
        dataset.export_parquet(destination=str(destination))
    assert destination.is_dir()
    result = dataset.export_parquet(destination=str(destination))
    assert result.row_count == 2
    assert parent_syncs == 2
    con.disconnect()


@pytest.mark.parametrize("partition_by", [(), ("sessions",)])
def test_empty_training_export_preserves_complete_arrow_metadata(
    native_inputs, tmp_path, partition_by
):
    from terrine.data import MaterializedFrame

    dataset, con = _staged_dataset(native_inputs, tmp_path, empty_training=True)
    assert dataset.manifest.generation.row_count == 0
    schema = dataset._arrow_schema()
    assert schema.metadata and b"terrine.materialized_frame.v1" in schema.metadata
    destination = tmp_path / "empty"
    export = dataset.export_parquet(
        destination=str(destination),
        partition_by=partition_by,
        budget=Budget(max_rows=0, strict_scan=False),
    )
    assert export.row_count == 0
    assert len(export.shards) == 1
    shard = destination / export.shards[0].path
    restored = pq.read_table(shard)
    assert restored.num_rows == 0
    assert restored.schema.equals(schema, check_metadata=True)
    frame = MaterializedFrame.from_arrow(restored)
    assert frame.roles.features == ("sessions",)
    assert frame.roles.outcomes == ("churn",)
    assert frame.provenance.dataset_view is not None
    assert frame.provenance.dataset_view.counts_by_reason == {"incomplete_coverage": 2}
    assert (
        dataset.export_parquet(
            destination=str(destination),
            partition_by=partition_by,
            budget=Budget(max_rows=0, strict_scan=False),
        )
        == export
    )
    con.disconnect()
