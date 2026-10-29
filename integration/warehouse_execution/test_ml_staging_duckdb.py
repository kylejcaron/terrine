from __future__ import annotations

import gc
import json
import subprocess
import sys
import weakref
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files
from pathlib import Path

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from examples.ml_staging import create_model, training_design
from terrine import Budget, agg
from terrine.data import Coverage, MaterializedFrame, UnitSource
from terrine.dataset.export import ExportManifest
from terrine.dataset.reference import Dataset, _definition_for
from terrine.dataset.splits import GroupSplit, TimeSplit, group_bucket
from terrine.errors import TerrineError, thaw
from terrine.model import Model
from terrine.runtime.api import run_serialized_plan
from terrine.runtime.arrow import ArrowOutput
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.saved_plan import SavedPlan
from terrine.spec import BinnedMetricState, Design, EventAnchor, FixedDate, Schedule, StatePanel, gt
from terrine.spec.dataset import DatasetDefinition
from tests import _worlds
from tests._timing import HANG_GUARD_SECONDS
from tests.dataset.test_staging import _durable_frame
from tests.dataset.test_training_view import _frame
from tests.fixtures.interop import interop_design
from tests.fixtures.winsorization import bind_model, design


@pytest.fixture(scope="session")
def events_world():
    return _worlds.events_world()


@pytest.fixture(scope="session")
def winsorization_world():
    return _worlds.winsorization_world()


@pytest.fixture(scope="session")
def interop_world():
    return _worlds.interop_world()


@pytest.fixture
def native_inputs(events_world):
    con = ibis.duckdb.connect()
    try:
        for name, table in events_world.tables.items():
            con.create_table(name, table)
        yield events_world.catalog, con
    finally:
        con.disconnect()


def _training(model):
    design = replace(
        training_design(),
        outcomes={"following_amount": agg("label_total", window=("0s", "2d"))},
    )
    return design.build(model).training_set(
        label="following_amount", incomplete_labels="keep", split=GroupSplit(seed=17)
    )


def _run(script, *args):
    result = subprocess.run(
        [sys.executable, "-c", script, *(str(arg) for arg in args)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=HANG_GUARD_SECONDS,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_fresh_process_manifest_refresh_matches_independent_build_and_parquet(tmp_path):
    model = create_model(tmp_path / "owner.duckdb")
    dataset = _training(model).stage("training", budget=Budget(strict_scan=False))
    manifest_path = tmp_path / "dataset.json"
    manifest_path.write_bytes(dataset.manifest.to_json())
    model.con.disconnect()
    report = _run(
        r"""
import json, sys
from pathlib import Path
import ibis
import pyarrow as pa
import pyarrow.parquet as pq
from examples.ml_staging import bind_model
from integration.warehouse_execution.test_ml_staging_duckdb import _training
from terrine import Budget
from terrine.data import MaterializedFrame
from terrine.dataset.manifest import DatasetManifest
from terrine.dataset.reference import Dataset
from terrine.runtime import run_serialized_plan
from terrine.runtime.changes import Change, ChangeSet
root = Path(sys.argv[1])
con = ibis.duckdb.connect(str(root / "owner.duckdb"))
try:
    model = bind_model(con)
    manifest = DatasetManifest.from_json((root / "dataset.json").read_bytes())
    original = Dataset.from_manifest(manifest, con=con)
    old_reader = original.to_arrow_batches(batch_size=1)
    first = next(old_reader)
    con.raw_sql("UPDATE events SET amount=amount+5 WHERE user_id=1")
    budget = Budget(strict_scan=False)
    changes = ChangeSet((Change("update", "events"),), "operator")
    before = con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0]
    assert original.plan_refresh(model, changes=changes, budget=budget).tasks
    after = con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0]
    assert after == before
    refreshed = original.refresh(model, changes=changes, budget=budget)
    old = pa.Table.from_batches([first, *old_reader]).sort_by("unit")
    streamed = pa.Table.from_batches(list(refreshed.to_arrow_batches(batch_size=1)))
    actual = MaterializedFrame.from_arrow(streamed)
    clean = _training(model).materialize(budget)
    assert actual.table.sort_by("unit").equals(clean.table.sort_by("unit"))
    assert actual.roles == clean.roles and actual.columns == clean.columns
    assert actual.provenance.dataset_view == clean.provenance.dataset_view
    assert actual.provenance.feature_cutoffs == clean.provenance.feature_cutoffs
    corrected = actual.table.sort_by("unit")["past_amount"][0].as_py()
    assert str(corrected) == "1000000000000000000000000000008"
    assert str(old["past_amount"][0].as_py()) == "1000000000000000000000000000003"
    assert actual.table["following_amount"].null_count == 2
    reason = actual.columns["following_amount"].reason_column
    assert actual.table[reason].to_pylist() == ["incomplete_coverage"] * 2
    assert manifest.definition.digest == refreshed.manifest.definition.digest
    exported = refreshed.export_parquet(destination=str(root / "parquet"), budget=budget)
    tables = []
    for shard in exported.shards:
        parquet = pq.ParquetFile(root / "parquet" / shard.path)
        tables.extend(
            pa.Table.from_batches([batch]) for batch in parquet.iter_batches(batch_size=1)
        )
    restored = MaterializedFrame.from_arrow(pa.concat_tables(tables))
    assert restored.table.sort_by("unit").equals(actual.table.sort_by("unit"))
    assert restored.to_metadata() == actual.to_metadata()
    saved, key = con.con.execute(
        "SELECT recipe_json,request_key FROM terrine_runtime.runtime_dataset_recipes "
        "WHERE dataset='training' AND generation=?", [refreshed.manifest.generation.generation_id]
    ).fetchone()
    replay = run_serialized_plan(saved, con=con.con, namespace="terrine_runtime", request_key=key)
    assert replay.published_generation == refreshed.manifest.generation.generation_id
    print(json.dumps({"old": manifest.generation.generation_id,
                      "new": refreshed.manifest.generation.generation_id,
                      "rows": exported.row_count, "replayed": replay.state}))
finally:
    con.disconnect()
""",
        tmp_path,
    )
    assert report["old"] != report["new"]
    assert report["rows"] == 2 and report["replayed"] == "succeeded"


def test_export_recovers_durable_shard_and_discards_interrupted_shard(tmp_path, monkeypatch):
    import terrine.dataset.export as exports

    model = create_model(tmp_path / "owner.duckdb")
    dataset = _training(model).stage("training", budget=Budget(strict_scan=False))
    original_batches = Dataset.to_arrow_batches
    original_writer = pq.ParquetWriter
    writers = []

    def one_row_batches(self, *, batch_size=65_536):
        return original_batches(self, batch_size=1)

    failure = OSError("interrupted shard")

    class Writer:
        def __init__(self, path, *args, **kwargs):
            self.path = Path(path)
            self.raw = original_writer(path, *args, **kwargs)
            writers.append(self.path.name)

        def write_table(self, table):
            self.raw.write_table(table)
            if len(writers) == 2:
                raise failure

        def close(self):
            self.raw.close()

    destination = tmp_path / "export"
    monkeypatch.setattr(Dataset, "to_arrow_batches", one_row_batches)
    with monkeypatch.context() as patch:
        patch.setattr(exports.pq, "ParquetWriter", Writer)
        with pytest.raises(OSError) as error:
            dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert error.value is failure
    assert not destination.exists()
    partial = tmp_path / ".export.partial"
    checkpoint = ExportManifest.from_json((partial / "export-state.json").read_bytes())
    assert checkpoint.row_count == 1 and len(checkpoint.shards) == 1
    durable = checkpoint.shards[0]
    durable_bytes = (partial / durable.path).read_bytes()
    assert not (partial / "part-00000001.parquet").exists()
    created = []

    def writer(path, *args, **kwargs):
        created.append(Path(path).name)
        return original_writer(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(exports.pq, "ParquetWriter", writer)
        exported = dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert exported.row_count == 2
    assert Path(durable.path).name not in created
    assert (destination / durable.path).read_bytes() == durable_bytes
    assert dataset.export_parquet(destination=str(destination), target_file_bytes=1) == exported
    model.con.disconnect()


@pytest.mark.parametrize(
    ("budget", "code"),
    [
        (Budget(max_rows=0, strict_scan=False), "terrine.budget.rows_exceeded"),
        (
            Budget(max_output_bytes=0, strict_scan=False),
            "terrine.budget.output_bytes_exceeded",
        ),
    ],
)
def test_export_budget_cancellation_closes_pinned_batch_reader(tmp_path, monkeypatch, budget, code):
    model = create_model(tmp_path / "owner.duckdb")
    dataset = _training(model).stage("training", budget=Budget(strict_scan=False))
    original = Dataset.to_arrow_batches
    streams, closed = [], []

    def tracked(self, *, batch_size=65_536):
        source = original(self, batch_size=1)

        def batches():
            try:
                yield from source
            finally:
                source.close()
                closed.append(True)

        stream = batches()
        streams.append(stream)
        return stream

    monkeypatch.setattr(Dataset, "to_arrow_batches", tracked)
    try:
        with pytest.raises(TerrineError) as error:
            dataset.export_parquet(destination=str(tmp_path / "cancelled"), budget=budget)
        assert error.value.code == code
        assert closed == [True]
        assert not (tmp_path / "cancelled").exists()
        assert sum(batch.num_rows for batch in original(dataset, batch_size=1)) == 2
    finally:
        for stream in streams:
            stream.close()
        model.con.disconnect()


def _compressible_dataset(tmp_path):
    model = create_model(tmp_path / "owner.duckdb")
    model.con.raw_sql("UPDATE events SET segment=repeat('x', 100000)")
    dataset = (
        training_design()
        .build(model)
        .training_set(label="following_amount", incomplete_labels="keep")
        .stage("training", budget=Budget(strict_scan=False))
    )
    return dataset, model


def test_parquet_export_budget_counts_arrow_bytes_before_publication(tmp_path):
    dataset, model = _compressible_dataset(tmp_path)
    destination = tmp_path / "bounded"
    try:
        with pytest.raises(TerrineError) as error:
            dataset.export_parquet(
                destination=str(destination),
                budget=Budget(max_output_bytes=50_000, strict_scan=False),
            )
        assert error.value.code == "terrine.budget.output_bytes_exceeded"
        assert list((tmp_path / ".bounded.partial").glob("**/*.parquet")) == []
        assert not destination.exists()
    finally:
        model.con.disconnect()


def test_completed_export_replay_enforces_arrow_byte_budget(tmp_path):
    dataset, model = _compressible_dataset(tmp_path)
    destination = tmp_path / "replay"
    try:
        exported = dataset.export_parquet(
            destination=str(destination),
            budget=Budget(max_output_bytes=1_000_000, strict_scan=False),
        )
        manifest_bytes = (destination / "export-manifest.json").read_bytes()
        with pytest.raises(TerrineError) as error:
            dataset.export_parquet(
                destination=str(destination),
                budget=Budget(max_output_bytes=50_000, strict_scan=False),
            )
        assert error.value.code == "terrine.budget.output_bytes_exceeded"
        assert (destination / "export-manifest.json").read_bytes() == manifest_bytes
        assert dataset.export_parquet(destination=str(destination)) == exported
    finally:
        model.con.disconnect()


def _measure_bounded_workflow(root: Path, rows: int) -> dict[str, int]:
    model = create_model(root / "owner.duckdb")
    width = 64
    model.con.raw_sql(f"INSERT INTO users SELECT i FROM range(3, {rows + 1}) t(i)")
    model.con.raw_sql(
        "INSERT INTO events SELECT TIMESTAMP '2025-01-01 09:00:00', i, "
        f"1000000000000000000000000000000+i, repeat('x', {width}) "
        f"FROM range(3, {rows + 1}) t(i)"
    )
    original_write = ArrowOutput.write
    live = {}
    stats = {"live_bytes": 0, "batch_bytes": 0, "batch_rows": 0, "stage_batches": 0}
    base_pool = pa.default_memory_pool()
    pool = pa.proxy_memory_pool(base_pool)

    def observe(batch):
        identity = id(batch)
        live[identity] = batch.nbytes
        weakref.finalize(batch, live.pop, identity, None)
        stats["live_bytes"] = max(stats["live_bytes"], sum(live.values()))
        stats["batch_bytes"] = max(stats["batch_bytes"], batch.nbytes)
        stats["batch_rows"] = max(stats["batch_rows"], batch.num_rows)

    @classmethod
    def measured(cls, reader, *, root, budget=None):
        def batches():
            for batch in reader:
                observe(batch)
                stats["stage_batches"] += 1
                yield batch

        stream = batches()
        try:
            return original_write(
                pa.RecordBatchReader.from_batches(reader.schema, stream), root=root, budget=budget
            )
        finally:
            stream.close()
            reader.close()

    pa.set_memory_pool(pool)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(ArrowOutput, "write", measured)
            dataset = (
                training_design()
                .build(model)
                .training_set(label="following_amount", incomplete_labels="keep")
                .stage("training", budget=Budget(strict_scan=False))
            )
        read_rows = 0
        reader = dataset.to_arrow_batches(batch_size=1024)
        try:
            for batch in reader:
                observe(batch)
                assert batch.num_rows <= 1024
                read_rows += batch.num_rows
            del batch
        finally:
            reader.close()
        exported = dataset.export_parquet(destination=str(root / "parquet"), target_file_bytes=1)
        stats["rows"] = read_rows
        stats["export_rows"] = exported.row_count
        stats["arrow_peak"] = pool.max_memory()
        assert read_rows == exported.row_count == rows
        assert stats["batch_rows"] <= 65_536
        assert stats["live_bytes"] <= 3 * stats["batch_bytes"]
        assert stats["arrow_peak"] <= 16 * stats["batch_bytes"]
        return stats
    finally:
        model.con.disconnect()
        gc.collect()
        pa.set_memory_pool(base_pool)


@pytest.mark.scale
def test_growing_dataset_bounds_client_batches_and_arrow_buffers(tmp_path):
    measurements = []
    for rows in (65_537, 327_681):
        root = tmp_path / str(rows)
        root.mkdir()
        measurements.append(
            _run(
                "import json,sys; from pathlib import Path; "
                "import integration.warehouse_execution.test_ml_staging_duckdb as checks; "
                "print(json.dumps(checks._measure_bounded_workflow("
                "Path(sys.argv[1]), int(sys.argv[2]))))",
                root,
                rows,
            )
        )
    small, large = measurements
    assert large["rows"] > 4 * small["rows"]
    assert large["stage_batches"] > small["stage_batches"]
    assert large["batch_rows"] == small["batch_rows"] == 65_536
    assert large["live_bytes"] <= 3 * small["batch_bytes"]
    assert large["arrow_peak"] <= 16 * small["batch_bytes"]


def _staged_dataset(events_world, tmp_path, *, empty_training=False):
    from terrine.dataset.reference import stage_frame

    if empty_training:
        events_world = replace(
            events_world,
            readiness={
                **events_world.readiness,
                "labels": Coverage("2025-01-01", "2025-01-01"),
            },
        )
    model = _worlds.bind_model(events_world, tmp_path)
    con = model.con
    frame = Design(
        "user",
        FixedDate("2025-01-05"),
        features={"sessions": agg("sessions", window=("-7d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "7d"))},
    ).build(model)
    if empty_training:
        frame = frame.training_set(label="churn")
    return stage_frame(frame, "training", budget=Budget(strict_scan=False)), con


def test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite(events_world, tmp_path):
    dataset, con = _staged_dataset(events_world, tmp_path)
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
    events_world, tmp_path, monkeypatch
):
    dataset, con = _staged_dataset(events_world, tmp_path)
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

    failure = OSError("simulated interruption in an unfinished shard")

    def interrupted(*, batch_size=65_536):
        batch = next(iter(original(batch_size=batch_size)))
        yield batch
        raise failure

    monkeypatch.setattr(
        type(dataset), "to_arrow_batches", lambda self, **kwargs: interrupted(**kwargs)
    )
    with pytest.raises(OSError) as error:
        dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert error.value is failure
    assert len(list(partial.glob("**/*.parquet"))) == 1

    monkeypatch.setattr(
        type(dataset), "to_arrow_batches", lambda self, **kwargs: original(**kwargs)
    )
    with pytest.raises(TerrineError) as error:
        dataset.export_parquet(
            destination=str(destination),
            target_file_bytes=1,
            budget=Budget(max_output_bytes=1, strict_scan=False),
        )
    assert error.value.code == "terrine.budget.output_bytes_exceeded"
    assert len(list(partial.glob("**/*.parquet"))) == 1

    writers = 0
    original_writer = export_module.pq.ParquetWriter

    def count_writers(*args, **kwargs):
        nonlocal writers
        writers += 1
        return original_writer(*args, **kwargs)

    monkeypatch.setattr(export_module.pq, "ParquetWriter", count_writers)
    export = dataset.export_parquet(
        destination=str(destination),
        target_file_bytes=1,
        budget=Budget(max_output_bytes=1_000_000, strict_scan=False),
    )
    assert writers > 0
    assert export.row_count == dataset.manifest.generation.row_count
    assert sum(shard.row_count for shard in export.shards) == export.row_count
    assert all((destination / shard.path).is_file() for shard in export.shards)
    con.disconnect()


def test_export_syncs_shards_and_directories_before_checkpoint_and_publication(
    events_world, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(events_world, tmp_path)
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


def test_export_does_not_checkpoint_a_shard_when_fsync_fails(events_world, tmp_path, monkeypatch):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(events_world, tmp_path)
    destination = tmp_path / "sync-retry"
    partial = tmp_path / ".sync-retry.partial"
    original_fsync = os.fsync
    failed = False

    failure = OSError("shard persistence interrupted")

    def sync(descriptor):
        nonlocal failed
        stat = os.fstat(descriptor)
        for path in partial.glob("**/*.parquet"):
            target = path.stat()
            if not failed and (stat.st_dev, stat.st_ino) == (target.st_dev, target.st_ino):
                failed = True
                raise failure
        original_fsync(descriptor)

    monkeypatch.setattr(export_module.os, "fsync", sync)
    with pytest.raises(OSError) as error:
        dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert error.value is failure
    state = export_module.ExportManifest.from_json((partial / "export-state.json").read_bytes())
    assert state.shards == ()
    assert list(partial.glob("**/*.parquet")) == []
    result = dataset.export_parquet(destination=str(destination), target_file_bytes=1)
    assert result.row_count == 2
    con.disconnect()


def test_export_retry_resyncs_parent_after_interrupted_publication(
    events_world, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset, con = _staged_dataset(events_world, tmp_path)
    destination = tmp_path / "rename-retry"
    original_fsync, original_rename = os.fsync, os.rename
    renamed = False
    parent_syncs = 0
    parent = tmp_path.stat()

    def rename(source, target):
        nonlocal renamed
        original_rename(source, target)
        renamed = True

    failure = OSError("publication directory persistence interrupted")

    def sync(descriptor):
        nonlocal parent_syncs
        stat = os.fstat(descriptor)
        if renamed and (stat.st_dev, stat.st_ino) == (parent.st_dev, parent.st_ino):
            parent_syncs += 1
            if parent_syncs == 1:
                raise failure
        original_fsync(descriptor)

    monkeypatch.setattr(export_module.os, "rename", rename)
    monkeypatch.setattr(export_module.os, "fsync", sync)
    with pytest.raises(OSError) as error:
        dataset.export_parquet(destination=str(destination))
    assert error.value is failure
    assert destination.is_dir()
    result = dataset.export_parquet(destination=str(destination))
    assert result.row_count == 2
    assert parent_syncs == 2
    con.disconnect()


@pytest.mark.parametrize("partition_by", [(), ("sessions",)])
def test_empty_training_export_preserves_complete_arrow_metadata(
    events_world, tmp_path, partition_by
):
    from terrine.data import MaterializedFrame

    dataset, con = _staged_dataset(events_world, tmp_path, empty_training=True)
    assert dataset.manifest.generation.row_count == 0
    schema = dataset._arrow_schema()
    assert schema.metadata and b"terrine.materialized_frame.v2" in schema.metadata
    destination = tmp_path / "empty"
    export = dataset.export_parquet(
        destination=str(destination),
        partition_by=partition_by,
        budget=Budget(max_rows=0, max_output_bytes=0, strict_scan=False),
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
            budget=Budget(max_rows=0, max_output_bytes=0, strict_scan=False),
        )
        == export
    )
    con.disconnect()


def test_refresh_reconstructs_definition_without_mutating_pinned_reader(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    model = initial._model
    frame = (
        Design(
            "user",
            FixedDate("2025-01-04"),
            features={"sessions": agg("sessions", window=("-1d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "1d"))},
        )
        .build(model)
        .training_set(label="churn", incomplete_labels="keep")
    )
    budget = Budget(strict_scan=False)
    original = frame.stage("training", budget=budget)
    reopened = Dataset.from_manifest(original.manifest, con=con)
    old_reader = reopened.to_arrow_batches(batch_size=1)
    first = next(old_reader)
    con.raw_sql("DELETE FROM events WHERE user_id='a'")
    changes = ChangeSet((Change("delete", "events"),), "operator")
    before = con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0]
    plan = reopened.plan_refresh(model, changes=changes, budget=budget)
    assert plan.tasks
    assert (
        con.con.execute("SELECT count(*) FROM terrine_runtime.runtime_runs").fetchone()[0] == before
    )
    refreshed = reopened.refresh(model, changes=changes, budget=budget)
    assert refreshed.manifest.generation.generation_id != original.manifest.generation.generation_id
    old = pa.Table.from_batches([first, *old_reader])
    new = pa.Table.from_batches(list(refreshed.to_arrow_batches()))
    assert sorted(old["sessions"].to_pylist()) == [1, 1]
    assert sorted(new["sessions"].to_pylist()) == [0, 1]
    clean = refreshed.manifest.definition.build(model).materialize(budget)
    order = [("unit", "ascending"), ("anchor_id", "ascending")]
    assert (
        new.select(clean.table.column_names)
        .replace_schema_metadata(None)
        .sort_by(order)
        .equals(clean.table.replace_schema_metadata(None).sort_by(order))
    )
    assert original.manifest.definition.digest == refreshed.manifest.definition.digest
    con.disconnect()


def test_scoring_refresh_preserves_as_of_and_never_reads_unused_labels(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    model = initial._model
    design = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "1d"))},
    )
    scoring = design.snapshot(
        model,
        as_of="2025-01-04",
        eligibility={"active": gt(agg("sessions", window=("-1d", "0s")), 0)},
    )
    con.drop_table("labels")
    dataset = scoring.stage("scoring", budget=Budget(strict_scan=False))
    refreshed = dataset.refresh(model, budget=Budget(strict_scan=False))
    assert refreshed.manifest.definition.view["as_of"] == dataset.manifest.definition.view["as_of"]
    assert all("churn" not in batch.schema.names for batch in refreshed.to_arrow_batches())
    assert set(refreshed.manifest.source_coverage) == {"events"}
    con.disconnect()


def test_refresh_nochange_reuses_only_same_definition_and_rejects_stale_base(
    events_world, tmp_path
):
    import pytest

    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    budget = Budget(strict_scan=False)
    dataset = frame.stage("training", budget=budget)
    empty = ChangeSet((), "operator")
    assert not dataset.plan_refresh(frame._model, changes=empty, budget=budget).tasks
    assert dataset.refresh(frame._model, changes=empty, budget=budget).manifest == dataset.manifest
    newer = dataset.refresh(frame._model, budget=budget)
    assert newer.manifest.generation != dataset.manifest.generation
    with pytest.raises(TerrineError) as error:
        dataset.refresh(frame._model, changes=empty, budget=budget)
    assert error.value.code == "terrine.dataset.stale_generation"
    con.disconnect()


def test_training_split_assigns_support_without_reassigning_overlap(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    base = Design(
        "user",
        EventAnchor("session"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "1d"))},
    ).build(initial._model)
    training = base.training_set(
        label="churn",
        incomplete_labels="keep",
        split=TimeSplit(train_until="2025-01-03T12:00:00Z"),
    )
    ordinary = training.materialize(Budget(strict_scan=False))
    assert ordinary.table["__terrine_split"].to_pylist() == ["unassigned"] * 2
    assert ordinary.table["__terrine_split_reason"].to_pylist() == ["label_overlap"] * 2
    assert ordinary.provenance.dataset_view is not None
    assert ordinary.provenance.dataset_view.counts_by_reason == {"label_overlap": 2}
    staged = training.stage("training", budget=Budget(strict_scan=False))
    actual = MaterializedFrame.from_arrow(
        pa.Table.from_batches(list(staged.to_arrow_batches(batch_size=1)))
    )
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.table.sort_by("unit").equals(ordinary.table.sort_by("unit"))
    assert staged.manifest.frame_metadata["provenance"]["dataset_view"]["counts_by_reason"] == {
        "label_overlap": 2
    }
    con.disconnect()


def test_group_split_view_matches_typed_reference_and_reconstruction(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    policy = GroupSplit(seed=17)
    training = initial.training_set(label="churn", incomplete_labels="keep", split=policy)
    result = training.materialize(Budget(strict_scan=False))
    assert result.table["__terrine_split"].to_pylist() == [
        group_bucket(key, policy) for key in result.table["unit"].to_pylist()
    ]
    staged = training.stage("groups", budget=Budget(strict_scan=False))
    rebuilt = staged.manifest.definition.build(initial._model).materialize(
        Budget(strict_scan=False)
    )
    assert rebuilt.table.sort_by("unit").equals(result.table.sort_by("unit"))
    con.disconnect()


def test_state_panel_stage_matches_ordinary_normalization_and_support(events_world, tmp_path):
    base, con = _durable_frame(events_world, tmp_path)
    frame = (
        Design(
            "user",
            Schedule("1d", "2025-01-03", "2025-01-04"),
            shape=StatePanel(
                BinnedMetricState("sessions", (0,), ("none", "some")),
                max_periods=2,
                baseline_features={"baseline": agg("sessions", window=("-1d", "0s"))},
            ),
        )
        .build(base._model)
        .training_set(
            label="to_state", features=("baseline", "from_state"), incomplete_labels="keep"
        )
    )
    budget = Budget(max_rows=100, max_output_bytes=100_000, strict_scan=False)
    ordinary = frame.materialize(budget)
    staged = frame.stage("periods", budget=budget)
    batches = list(staged.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.table.schema.field("gap").type == pa.duration("us")
    order = [("unit", "ascending"), ("period_index", "ascending")]
    assert actual.table.sort_by(order).equals(ordinary.table.sort_by(order))
    assert actual.provenance.state_panel == ordinary.provenance.state_panel
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.provenance.state_panel is not None
    assert actual.provenance.state_panel.period_column in actual.table.column_names
    con.disconnect()


def test_stage_never_collects_full_output_through_snapshot_execute(
    events_world, tmp_path, monkeypatch
):
    from terrine.execution.inputs import ExecutionContext

    original = ExecutionContext.execute

    def execute(context, relation):
        if {"unit", "t0", "sessions", "churn"}.issubset(relation.columns):
            raise AssertionError("full output must cross the bounded Arrow boundary")
        return original(context, relation)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    frame, con = _durable_frame(events_world, tmp_path)
    staged = frame.stage("bounded", budget=Budget(strict_scan=False))
    iterator = staged.to_arrow_batches(batch_size=1)
    assert next(iterator).num_rows == 1
    iterator.close()
    assert con.con.execute("SELECT 1").fetchone() == (1,)
    con.disconnect()


def test_staging_runs_the_check_schedule_once(events_world, tmp_path, monkeypatch):
    """Composition validates the frame; staging then reads only its eligibility
    summary, so no gate or anchor statement runs twice in one task."""
    import ibis
    from ibis.backends.duckdb import Backend

    from terrine.execution.inputs import ExecutionContext

    frame, con = _durable_frame(events_world, tmp_path)
    executed = []
    context_execute, backend_to_pyarrow = ExecutionContext.execute, Backend.to_pyarrow

    def execute(self, query):
        executed.append(ibis.to_sql(query, dialect="duckdb"))
        return context_execute(self, query)

    def to_pyarrow(self, expr, **kwargs):
        executed.append(ibis.to_sql(expr, dialect="duckdb"))
        return backend_to_pyarrow(self, expr, **kwargs)

    monkeypatch.setattr(ExecutionContext, "execute", execute)
    monkeypatch.setattr(Backend, "to_pyarrow", to_pyarrow)
    try:
        frame.stage("dataset")
    finally:
        con.disconnect()
    gates = [sql for sql in executed if "__terrine_gate_check_" in sql]
    anchors = [sql for sql in executed if "invalid_count" in sql]
    assert len(gates) == 1
    assert anchors and len(anchors) == len(set(anchors))


def test_scoring_today_is_resolved_once_at_catalog_day_boundary(
    native_inputs, tmp_path, monkeypatch
):
    catalog, source_con = native_inputs
    con = ibis.duckdb.connect(str(tmp_path / "scoring.duckdb"))
    for name in ("events", "labels", "users"):
        con.create_table(name, source_con.table(name).execute())
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-1d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    )
    from terrine.errors import TerrineError

    with pytest.raises(TerrineError) as error:
        design.snapshot(
            model,
            as_of="2025-01-05T00:00:00.000000001Z",
            eligibility={"active": gt(agg("sessions", window=("-1d", "0d")), 0)},
            features=("past_sessions",),
        )
    assert error.value.code == "terrine.dataset.invalid_view"
    from datetime import UTC
    from datetime import datetime as OriginalDateTime

    class FrozenDateTime(OriginalDateTime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return OriginalDateTime(2025, 1, 5, 23, 45, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr("terrine.spec.design.datetime", FrozenDateTime)
    eligibility = {"active": gt(agg("sessions", window=("-1d", "0d")), 0)}

    scoring = design.snapshot(
        model,
        as_of="today",
        eligibility=eligibility,
        features=("past_sessions",),
    )
    from terrine.frame import _feature_definition_hash

    assert _feature_definition_hash(design, ("past_sessions",), model) == _feature_definition_hash(
        scoring._design, ("past_sessions",), model
    )
    assert FrozenDateTime.calls == 1

    assert scoring.roles.outcomes == ()
    assert scoring.roles.features == ("past_sessions",)
    result = scoring.materialize()
    view = result.provenance.dataset_view
    assert view is not None
    assert view.kind == "scoring"
    assert view.selected_features == ("past_sessions",)
    assert view.selected_labels == ()
    assert view.resolved_as_of is not None
    assert view.resolved_as_of.isoformat() == "2025-01-05T00:00:00+00:00"
    assert result.roles.outcomes == ()
    assert isinstance(design.anchor, EventAnchor)
    assert "churn" in design.outcomes
    staged = scoring.stage("scoring", budget=Budget(strict_scan=False))
    assert staged.manifest.definition.view["kind"] == "scoring"
    assert staged.manifest.definition.view["features"] == ("past_sessions",)
    assert staged.manifest.definition.view["as_of"] == view.resolved_as_of.isoformat()
    assert staged.manifest.definition.view["eligibility"]["active"]
    assert staged.manifest.definition.view["parent_design_hash"] == design.spec_hash
    con.disconnect()


def test_owner_prepares_scoring_view_before_preflight_without_label_inputs(
    native_inputs, tmp_path, monkeypatch
):
    import json

    import terrine.compile.dispatch as dispatch
    from terrine.dataset.reference import _definition_for
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec.dataset import DatasetDefinition

    catalog, source = native_inputs
    database = tmp_path / "scoring-owner.duckdb"
    con = ibis.duckdb.connect(str(database))
    try:
        for name in ("events", "labels", "users"):
            con.create_table(name, source.table(name).execute())
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-01-31")},
            units={"user": UnitSource("users", "user_id")},
        )
        parent = Design(
            "user",
            EventAnchor("session"),
            features={"past_sessions": agg("sessions", window=("-1d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "1d"))},
        )
        definition = _definition_for(parent.snapshot(model, as_of="2025-01-05", eligibility={}))
        owner = RuntimeOwner.from_connection(con)
        reference = ModelReference.from_model(model, owner_key=owner.reference)
        con.drop_table("labels")
        expected = reference

        class Resolver:
            def resolve(self, reference):
                assert reference == expected
                return model

        resolved = []
        build, compile_design = DatasetDefinition.build, dispatch.compile_design

        def resolve_view(self, model):
            resolved.append(self.digest)
            return build(self, model)

        def compile_resolved(design, model):
            assert resolved, "resolve the definition view before preflight compilation"
            return compile_design(design, model)

        monkeypatch.setattr(DatasetDefinition, "build", resolve_view)
        monkeypatch.setattr(dispatch, "compile_design", compile_resolved)
        authority = SnapshotAuthority(owner)
        invocation = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority).plan(
            PlanRequest(
                "scoring",
                reference,
                definition.base_design,
                DestinationRef(str(database), "owner", "scoring"),
                budget=Budget(strict_scan=False),
                dataset_definition=definition,
            ),
            con=con.con,
            namespace="owner",
        )
        retained = authority.find_retained(invocation.snapshot_id, namespace="owner")
        assert "labels" not in json.dumps(retained.roots)
        saved = SavedPlan.from_json(invocation.saved_plan_json)
        assert saved.restore_dataset_definition() == definition
        assert definition.view["parent_design_hash"] == parent.spec_hash
        assert definition.view["as_of"] == "2025-01-05T00:00:00+00:00"
    finally:
        con.disconnect()


@pytest.fixture
def model(winsorization_world, tmp_path):
    model = _worlds.bind_model(winsorization_world, tmp_path)
    try:
        yield model
    finally:
        model.con.disconnect()


def training(model, *, split=True, keep=False):
    return (
        design()
        .build(model)
        .training_set(
            label="label",
            features=("value", "category"),
            incomplete_labels="keep" if keep else "drop",
            split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07")
            if split
            else None,
        )
    )


def test_percentile_threshold_fits_training_rows_only_and_freezes_definition(model):
    from terrine.spec.transforms import Winsorization

    frame = training(model)
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    assert fitted.lower == Decimal(5) and fitted.upper == Decimal(15)
    assert fitted.fit.row_count == fitted.fit.non_null_count == 3
    assert fitted.fit.definition_hash == _definition_for(frame).digest
    raw = frame.materialize()
    assert fitted.fit.view_hash == raw.provenance.dataset_view.view_hash
    reference = raw.table.to_pandas()
    thresholds = (
        reference.loc[reference["__terrine_split"] == "train", "value"]
        .astype("int64")
        .quantile([0.25, 0.75])
    )
    assert tuple(Decimal(str(value)) for value in thresholds) == (fitted.lower, fitted.upper)
    assert fitted.fit.split_policy["kind"] == "time"
    assert fitted.fit.snapshot["backend"] == "duckdb"
    with pytest.raises((FrozenInstanceError, AttributeError)):
        fitted.lower = Decimal(0)
    with pytest.raises(TypeError):
        fitted.fit.split_policy["kind"] = "other"
    assert Winsorization.from_json(fitted.to_json()) == fitted
    transformed = frame.winsorize(fitted)
    result = transformed.materialize()
    assert result.table.sort_by("t0")["value"].to_pylist() == [
        Decimal(5),
        Decimal(10),
        Decimal(15),
        Decimal(15),
        Decimal(15),
    ]
    definition = _definition_for(transformed)
    assert definition.digest != _definition_for(frame).digest
    restored = DatasetDefinition.from_json(definition.canonical_json())
    actual = restored.build(model).materialize()
    assert actual.table.equals(result.table)
    assert actual.columns == result.columns
    metadata = actual.columns["value"].transformation
    assert metadata["stage"] == "post_aggregation"
    assert metadata["fit"]["definition_hash"] == fitted.fit.definition_hash
    assert actual.columns["label"] == result.columns["label"]


def test_validation_and_test_rows_cannot_change_fit_or_population_identity(model):
    frame = training(model)
    before = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    model.con.raw_sql("UPDATE events SET amount=-999999 WHERE ts >= '2025-01-05'")
    after = training(model).fit_winsorization("value", lower="0.25", upper="0.75")
    assert (after.lower, after.upper) == (Decimal(5), Decimal(15))
    assert after.fit.population_hash == before.fit.population_hash
    model.con.raw_sql("CREATE OR REPLACE TABLE events AS SELECT * FROM events ORDER BY ts DESC")
    reordered = training(bind_model(model.con)).fit_winsorization(
        "value", lower="0.25", upper="0.75"
    )
    assert reordered.fit.population_hash == before.fit.population_hash
    model.con.raw_sql("UPDATE events SET amount=30 WHERE ts::DATE='2025-01-04'")
    changed = training(bind_model(model.con)).fit_winsorization("value", lower="0.25", upper="0.75")
    assert changed.upper == Decimal(20)
    assert changed.fit.population_hash != before.fit.population_hash


def test_fitting_requires_declared_training_split_and_exact_numeric_feature(model):
    with pytest.raises(TerrineError) as missing:
        training(model, split=False).fit_winsorization("value", lower="0", upper="1")
    assert missing.value.code == "terrine.transform.training_split_required"
    assert missing.value.context["route"]
    with pytest.raises(TerrineError) as dtype:
        training(model).fit_winsorization("category", lower="0.25", upper="0.75")
    assert dtype.value.code == "terrine.transform.winsorization_type_unsupported"
    assert dtype.value.context["route"]
    with pytest.raises(TerrineError) as labels:
        training(bind_model(model.con, label_end="2025-01-03"), keep=True).fit_winsorization(
            "value", lower="0.25", upper="0.75"
        )
    assert labels.value.code == "terrine.export.incomplete_outcome"


def test_fitted_transform_refuses_different_feature_definition(model):
    from dataclasses import replace

    from terrine.spec import agg

    fitted = training(model).fit_winsorization("value", lower="0.25", upper="0.75")
    changed = replace(design(), features={"value": agg("total", window=("-2d", "0s"))})
    with pytest.raises(TerrineError) as error:
        changed.snapshot(model, as_of="2025-01-08", features=("value",), eligibility={}).winsorize(
            fitted
        )
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"


def test_independently_fitted_features_compose_and_reopen(model):
    from dataclasses import replace

    from terrine.spec import agg

    study = replace(
        design(),
        features={
            "value": agg("total", window=("-1d", "0d")),
            "other": agg("total", window=("-1d", "0d")),
        },
    )
    frame = study.build(model).training_set(
        label="label",
        features=("value", "other"),
        split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
    )
    first = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    second = frame.fit_winsorization("other", lower="0.25", upper="0.75")
    transformed = frame.winsorize(first).winsorize(second)
    definition = DatasetDefinition.from_json(_definition_for(transformed).canonical_json())
    result = definition.build(model).materialize()
    assert definition.transforms == (first, second)
    assert result.table.sort_by("t0")["value"].to_pylist() == [
        Decimal(value) for value in (5, 10, 15, 15, 15)
    ]
    assert result.table["other"].equals(result.table["value"])
    assert first.fit.definition_hash == second.fit.definition_hash == _definition_for(frame).digest


@pytest.mark.parametrize("kind", ("quantile", "approximate_distinct"))
def test_fitted_transform_refuses_same_typed_rebound_metric(model, kind):
    from dataclasses import replace

    from terrine import Coverage, Model, UnitSource
    from terrine.semantic.models import Catalog
    from terrine.spec import approx_distinct, quantile

    model.con.raw_sql("ALTER TABLE events ADD COLUMN other_amount BIGINT DEFAULT 7")
    definitions = model.catalog.model_dump(mode="python")
    definitions["sources"]["events"]["events"] = (
        *definitions["sources"]["events"]["events"],
        "other_amount",
    )
    definitions["events"]["other_amount"] = {"source": "events", "column": "other_amount"}

    def bind():
        return Model.from_catalog(
            Catalog.model_validate(definitions),
            con=model.con,
            units={"user": UnitSource("users", "user_id")},
            readiness={name: Coverage("2025-01-01", "2025-01-12") for name in ("events", "labels")},
        )

    expression = (
        quantile("total", "0.5", window=("-1d", "0d"))
        if kind == "quantile"
        else approx_distinct("total", window=("-1d", "0d"))
    )
    study = replace(design(), features={"value": expression})
    original = bind()
    frame = study.build(original).training_set(
        label="label",
        split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
    )
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    definitions["measures"]["total"]["event"] = "other_amount"
    rebound = study.snapshot(bind(), as_of="2025-01-08", features=("value",), eligibility={})
    assert rebound.expr.schema()["value"] == frame._active_compiled().table.schema()["value"]
    with pytest.raises(TerrineError) as error:
        rebound.winsorize(fitted)
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"


@pytest.mark.parametrize(
    ("binding", "nested", "feature_kind"),
    (
        ("source", False, "aggregate"),
        ("event", True, "aggregate"),
        ("property", True, "attribute"),
        ("dimension", True, "aggregate"),
    ),
)
def test_frozen_limits_refuse_rebound_selector_dependencies(model, binding, nested, feature_kind):
    from dataclasses import replace

    from terrine import Coverage, Model, UnitSource
    from terrine.frame import _feature_definition_hash
    from terrine.semantic.models import Catalog
    from terrine.spec import Schedule, agg, asof, event_time

    model.con.raw_sql(
        "CREATE TABLE selection_events AS SELECT user_id,ts,1::BIGINT AS selected,"
        "NULL::BIGINT AS alternate,'keep' AS kind,'drop' AS other_kind FROM events; "
        "CREATE TABLE alternate_selection_events AS "
        "SELECT * REPLACE(ts+INTERVAL 6 HOUR AS ts) FROM selection_events"
    )
    definitions = model.catalog.model_dump(mode="python")
    definitions["sources"]["selection"] = {
        "relation": {"kind": "table", "name": "selection_events"},
        "time": "ts",
        "units": {"user": "user_id"},
        "events": ("selection",),
        "properties": ("selection_kind",),
        "dimensions": (),
    }
    definitions["events"]["selection"] = {"source": "selection", "column": "selected"}
    definitions["properties"]["selection_kind"] = {
        "source": "selection",
        "column": "kind",
        "dtype": "string",
        "as_of": "event_time",
    }
    definitions["sources"]["events"]["properties"] = ("category", "amount_value")
    definitions["properties"]["amount_value"] = {
        "source": "events",
        "column": "amount",
        "dtype": "int",
        "as_of": "event_time",
    }
    if binding == "dimension":
        model.con.raw_sql(
            "CREATE TABLE selection_history AS SELECT user_id,'keep' AS kind,"
            "TIMESTAMPTZ '2025-01-01' AS starts,TIMESTAMPTZ '2025-01-12' AS ends FROM users; "
            "CREATE TABLE alternate_selection_history AS "
            "SELECT * REPLACE('drop' AS kind) FROM selection_history"
        )
        definitions["sources"]["selection"]["dimensions"] = ("selection_history",)
        definitions["properties"]["selection_kind"]["dimension"] = "selection_history"
        definitions["dimensions"]["selection_history"] = {
            "relation": {"kind": "table", "name": "selection_history"},
            "unit": "user",
            "key": "user_id",
            "properties": ("selection_kind",),
            "validity": {"valid_from": "starts", "valid_to": "ends"},
        }

    def bind():
        return Model.from_catalog(
            Catalog.model_validate(definitions),
            con=model.con,
            units={"user": UnitSource("users", "user_id")},
            readiness={
                name: Coverage("2025-01-01", "2025-01-12") for name in definitions["sources"]
            },
        )

    selector = event_time(
        "selection", window=("-2d", "-1d"), label_property="selection_kind", equals="keep"
    )
    if nested:
        selector = event_time("amount", window=("0s", "1h"), relative_to=selector)
    expression = (
        agg("total", window=("-1d", "0s"), relative_to=selector)
        if feature_kind == "aggregate"
        else asof("amount_value", before="0s", relative_to=selector)
    )
    study = replace(
        design(), anchor=Schedule("1d", "2025-01-05", "2025-01-07"), features={"value": expression}
    )
    split = TimeSplit(train_until="2025-01-07", validation_until="2025-01-08")
    original = bind()
    frame = study.build(original).training_set(label="label", split=split)
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    assert fitted.fit.non_null_count == 2
    original_score = study.snapshot(
        original, as_of="2025-01-08", features=("value",), eligibility={}
    )
    original_score.winsorize(fitted)
    assert fitted.fit.feature_definition_hash == _feature_definition_hash(
        original_score._design, ("value",), original
    )
    if binding == "source":
        definitions["sources"]["selection"]["relation"]["name"] = "alternate_selection_events"
    elif binding == "event":
        definitions["events"]["selection"]["column"] = "alternate"
    elif binding == "property":
        definitions["properties"]["selection_kind"]["column"] = "other_kind"
    else:
        definitions["dimensions"]["selection_history"]["relation"]["name"] = (
            "alternate_selection_history"
        )
    rebound = bind()
    candidates = (
        study.build(rebound).training_set(label="label", split=split),
        study.snapshot(rebound, as_of="2025-01-08", features=("value",), eligibility={}),
    )
    for candidate in candidates:
        assert candidate.expr.schema()["value"] == frame.expr.schema()["value"]
        with pytest.raises(TerrineError) as error:
            candidate.winsorize(fitted)
        assert error.value.code == "terrine.transform.winsorization_identity_mismatch"
        assert _feature_definition_hash(candidate._design, ("value",), rebound) != (
            fitted.fit.feature_definition_hash
        )
    timestamp_study = replace(study, features={"selected_at": selector})
    assert _feature_definition_hash(timestamp_study, ("selected_at",), original) != (
        _feature_definition_hash(timestamp_study, ("selected_at",), rebound)
    )


def test_saved_stage_replays_exact_generation_and_refuses_changed_view(events_world, tmp_path):
    frame, con = _durable_frame(events_world, tmp_path)
    training = frame.training_set(label="churn", incomplete_labels="keep")
    dataset = training.stage("training", budget=Budget(strict_scan=False))
    saved, request_key = con.con.execute(
        "SELECT recipe_json,request_key FROM terrine_runtime.runtime_dataset_recipes "
        "WHERE dataset=? AND generation=?",
        ["training", dataset.manifest.generation.generation_id],
    ).fetchone()
    definition = SavedPlan.from_json(saved)
    restored_definition = definition.restore_dataset_definition()
    assert restored_definition is not None
    assert restored_definition == dataset.manifest.definition
    assert definition.dataset_definition_json is not None
    con.raw_sql("DELETE FROM events")
    replay = run_serialized_plan(
        saved, con=con.con, namespace="terrine_runtime", request_key=request_key
    )
    assert replay.published_generation == dataset.manifest.generation.generation_id
    altered = json.loads(definition.dataset_definition_json)
    altered["view"]["incomplete_labels"] = "drop"
    changed = SavedPlan.create(
        plan_json=definition.plan_json,
        design=restored_definition.base_design,
        model=definition.model,
        destination=definition.destination,
        namespace=definition.namespace,
        expected_base_manifest=definition.expected_base_manifest,
        dataset_definition=type(dataset.manifest.definition).from_json(json.dumps(altered)),
    )
    with pytest.raises(TerrineError) as error:
        run_serialized_plan(
            changed.to_json(), con=con.con, namespace="terrine_runtime", request_key=request_key
        )
    assert error.value.code == "terrine.runtime.request_key_conflict"
    con.disconnect()


def test_staged_batches_preserve_frame_metadata_and_verify_definition_ownership(
    events_world, tmp_path
):
    frame, con = _durable_frame(events_world, tmp_path)
    training = frame.training_set(label="churn", incomplete_labels="keep")
    expected = training.materialize(Budget(strict_scan=False))
    dataset = training.stage("training", budget=Budget(strict_scan=False))
    batches = list(dataset.to_arrow_batches(batch_size=1))
    actual = MaterializedFrame.from_arrow(pa.Table.from_batches(batches))
    assert actual.roles == expected.roles
    assert actual.columns == expected.columns
    assert actual.provenance.dataset_view == expected.provenance.dataset_view
    assert all(batch.num_rows <= 1 for batch in batches)
    wrong = replace(dataset.manifest, definition=frame_definition(frame))
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(wrong, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def frame_definition(frame):
    from terrine.spec.dataset import DatasetDefinition

    return DatasetDefinition(schema_version=2, base_design=frame._design)


def test_stage_renews_lease_during_snapshot_tasks(events_world, tmp_path, monkeypatch):
    import threading

    from terrine.runtime.api import DesignExecutor
    from terrine.runtime.leases import LeaseStore

    renewed = threading.Event()
    original_renew = LeaseStore.renew
    original_task = DesignExecutor.run_snapshot_task

    def renew(store, lease, *, ttl, now):
        result = original_renew(store, lease, ttl=ttl, now=now)
        renewed.set()
        return result

    def run_task(executor, context, task, dependencies):
        assert renewed.wait(timeout=HANG_GUARD_SECONDS)
        return original_task(executor, context, task, dependencies)

    monkeypatch.setattr(LeaseStore, "renew", renew)
    monkeypatch.setattr(DesignExecutor, "run_snapshot_task", run_task)
    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("long_stage", budget=Budget(strict_scan=False))
    assert renewed.is_set()
    assert dataset.manifest.generation.row_count > 0
    con.disconnect()


def test_frame_stage_publishes_and_reads_its_pinned_generation(events_world, tmp_path, monkeypatch):
    frame, con = _durable_frame(events_world, tmp_path)

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


def test_manifest_reopen_rejects_tampered_generation_row_count(events_world, tmp_path):
    from dataclasses import replace

    from terrine.errors import TerrineError

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    altered_generation = replace(
        dataset.manifest.generation,
        row_count=dataset.manifest.generation.row_count + 1,
    )
    altered_manifest = replace(dataset.manifest, generation=altered_generation)
    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(altered_manifest, con=con)
    assert error.value.code == "terrine.dataset.invalid_generation"
    con.disconnect()


def test_roster_only_stage_uses_executor_default_durable_outputs(
    events_world, tmp_path, monkeypatch
):
    from terrine.runtime.api import DesignExecutor

    frame, con = _durable_frame(events_world, tmp_path)
    original = DesignExecutor.__init__

    def initialize(self, *args, **kwargs):
        kwargs.pop("output_root", None)
        original(self, *args, **kwargs)

    monkeypatch.setattr(DesignExecutor, "__init__", initialize)
    con.drop_table("labels")
    roster = Design("user", FixedDate("2025-01-05")).build(frame._model)
    dataset = roster.stage("roster", budget=Budget(strict_scan=False))
    assert dataset.manifest.generation.row_count == 2
    assert set(dataset.manifest.source_coverage) == set()
    assert sorted(
        key for batch in dataset.to_arrow_batches() for key in batch["unit"].to_pylist()
    ) == ["a", "b"]
    con.disconnect()


def test_reopen_accepts_resolved_owner_scoped_definition_reference(events_world, tmp_path):
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.spec.dataset import DatasetDefinitionStore

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    definitions = DatasetDefinitionStore(tmp_path / "definitions", owner="customer")
    reference = definitions.put(dataset.manifest.definition)
    manifest = DatasetManifest.from_json(
        replace(dataset.manifest, definition_reference=reference).to_json(),
        definition_store=definitions,
    )
    reopened = Dataset.from_manifest(manifest, con=con)
    assert reopened.manifest.definition_reference == reference
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()


def test_reopen_preserves_version_one_manifest_without_frame_metadata(events_world, tmp_path):
    import json
    from dataclasses import replace

    from terrine.dataset.manifest import DatasetManifest
    from terrine.runtime.backends.duckdb import _digest_relation

    frame, con = _durable_frame(events_world, tmp_path)
    dataset = frame.stage("training", budget=Budget(strict_scan=False))
    checksums = []
    for partition in dataset.manifest.generation.output_refs:
        physical = con.con.execute(
            "SELECT table_name FROM terrine_runtime.runtime_partitions WHERE partition=?",
            [partition],
        ).fetchone()
        assert physical is not None
        checksum = _digest_relation(con.con, "terrine_runtime", physical[0], version=1)
        con.con.execute(
            "UPDATE terrine_runtime.runtime_partitions SET digest=? WHERE partition=?",
            [checksum, partition],
        )
        checksums.append(checksum)
    old = replace(
        dataset.manifest,
        frame_metadata=None,
        generation=replace(dataset.manifest.generation, checksums=tuple(checksums)),
    ).to_json()
    assert "frame_metadata" not in json.loads(old)
    con.con.execute(
        "UPDATE terrine_runtime.runtime_dataset_recipes SET manifest_json=? "
        "WHERE dataset=? AND generation=?",
        [old.decode(), "training", dataset.manifest.generation.generation_id],
    )
    restored = DatasetManifest.from_json(old)
    assert restored.to_json() == old
    reopened = Dataset.from_manifest(restored, con=con)
    assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == 2
    con.disconnect()


def test_incomplete_labels_drop_rows_and_keep_null_reasons(native_inputs):
    frame = _frame(native_inputs, label_coverage_end="2025-01-01", missing_label=True)
    dropped = frame.training_set(label="churn", incomplete_labels="drop").materialize()
    kept = frame.training_set(label="churn", incomplete_labels="keep").materialize()
    assert dropped.table.num_rows == 0, (kept.columns["churn"], kept.table.to_pylist())
    assert kept.table.num_rows == 2
    assert kept.table["churn"].null_count == 2
    reason_column = kept.columns["churn"].reason_column
    assert reason_column is not None
    assert kept.table[reason_column].to_pylist() == ["incomplete_coverage", "incomplete_coverage"]
    assert dropped.provenance.dataset_view.counts_by_reason == {"incomplete_coverage": 2}


def test_training_projection_preserves_baseline_treatment_role(native_inputs):
    from terrine.semantic.models import Catalog
    from terrine.spec import FixedDate, Treatment, asof

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN enabled BOOLEAN")
    con.raw_sql("UPDATE events SET enabled = (user_id = 'a')")
    definitions = catalog.model_dump(mode="json")
    definitions["sources"]["events"]["properties"] = ["enabled"]
    definitions["properties"]["enabled"] = {
        "source": "events",
        "column": "enabled",
        "dtype": "bool",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definitions),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    frame = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"enabled": asof("enabled", before="0s")},
        outcomes={"future": agg("sessions", window=("0s", "1d"))},
        treatment=Treatment("enabled", known_at_t0=True),
    ).build(model)

    materialized = frame.training_set(label="future", features=("enabled",)).materialize()
    assert materialized.roles.treatment == "treatment"
    assert "treatment" in materialized.table.column_names
    treatment = materialized.columns["treatment"]
    for support in (
        treatment.observed_from_column,
        treatment.observed_until_column,
        treatment.complete_column,
        treatment.reason_column,
    ):
        if support is not None:
            assert support in materialized.table.column_names


def test_training_view_hashes_selected_feature_definition_and_parent_design(native_inputs):
    from terrine.spec import EventAnchor

    frame = _frame(native_inputs)
    first = frame.training_set(label="churn", features=("past_sessions",)).materialize()
    second = frame.training_set(label="churn", features=("earlier_sessions",)).materialize()
    assert (
        first.provenance.dataset_view.feature_definition_hash
        != second.provenance.dataset_view.feature_definition_hash
    )

    changed_design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-3d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    ).build(frame._model)
    changed = changed_design.training_set(label="churn", features=("past_sessions",)).materialize()
    first_view = first.provenance.dataset_view
    changed_view = changed.provenance.dataset_view
    assert first_view is not None and changed_view is not None
    assert first_view.view_hash != changed_view.view_hash


def test_state_panel_training_view_hashes_baseline_and_prior_state(native_inputs):
    from decimal import Decimal

    from terrine.data import Budget
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    frame = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", (Decimal("0"),), ("none", "some")),
            max_periods=2,
            baseline_features={"baseline": agg("sessions", window=("-1d", "0d"))},
        ),
    ).build(model)
    result = frame.training_set(
        label="to_state",
        features=("baseline", "from_state"),
        incomplete_labels="keep",
    ).materialize(Budget(max_rows=100, max_output_bytes=100_000))
    assert result.roles.features == ("baseline", "from_state")
    assert result.provenance.dataset_view is not None
    assert result.provenance.state_panel is not None
    assert result.provenance.state_panel.period_column in result.table.column_names
    prior = result.columns["from_state"]
    for support in (prior.complete_column, prior.reason_column):
        if support is not None:
            assert support in result.table.column_names


def test_feature_definition_hash_tracks_source_timestamp_semantics(native_inputs):
    from terrine.frame import _feature_definition_hash
    from terrine.semantic.models import Catalog
    from terrine.spec import FixedDate

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN earlier TIMESTAMPTZ")
    con.raw_sql("UPDATE events SET earlier=ts-INTERVAL '1 day'")
    units = {"user": UnitSource("users", "user_id")}
    readiness = {"events": Coverage("2025-01-01", "2025-01-31")}
    original = Model.from_catalog(catalog, con=con, units=units, readiness=readiness)
    changed = catalog.model_dump(mode="json")
    changed["sources"]["events"]["time"] = "earlier"
    corrected = Model.from_catalog(
        Catalog.model_validate(changed), con=con, units=units, readiness=readiness
    )
    design = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
    )
    assert design.build(original).materialize().table["sessions"].to_pylist() == [1, 1]
    assert design.build(corrected).materialize().table["sessions"].to_pylist() == [0, 0]
    assert _feature_definition_hash(design, ("sessions",), original) != _feature_definition_hash(
        design, ("sessions",), corrected
    )


@pytest.fixture(params=[False, True], ids=["typed_rows", "empty"])
def common_frame(request, tmp_path, interop_world):
    model = _worlds.bind_model(interop_world, tmp_path)
    try:
        if request.param:
            model.con.raw_sql("DELETE FROM users")
        yield interop_design(full=False).build(model)
    finally:
        model.con.disconnect()


def test_local_arrow_preserves_exact_temporal_values_and_semantic_metadata(common_frame):
    materialized = common_frame.materialize(Budget(max_rows=3))
    arrow = materialized.to_arrow().sort_by("unit")
    assert arrow.schema.field("amount").type == pa.decimal128(38, 2)
    assert arrow.schema.field("t0").type == pa.timestamp("us", tz="UTC")
    restored = MaterializedFrame.from_arrow(arrow)
    assert restored.roles == materialized.roles
    assert restored.columns == materialized.columns
    assert restored.provenance == materialized.provenance
    if arrow.num_rows:
        rows = arrow.to_pylist()
        assert [row["amount"] for row in rows] == [
            Decimal("999999999999999999999999999999999999.98"),
            None,
            None,
        ]
        assert [row["t0"] for row in rows] == [datetime(2025, 1, 2, 12, tzinfo=UTC)] * 3
        assert arrow["note"].null_count == 3
        amount_meta = restored.columns["amount"]
        assert rows[1][amount_meta.reason_column] == "all_values_null"
    else:
        assert arrow.num_rows == 0
        assert arrow.schema.field("note").type == pa.string()
    decorated = replace(
        materialized,
        table=materialized.table.replace_schema_metadata(
            {b"consumer": b"kept", b"terrine.materialized_frame.v2": b"stale"}
        ),
    )
    transported = decorated.to_arrow()
    assert transported.schema.metadata[b"consumer"] == b"kept"
    assert MaterializedFrame.from_arrow(transported).to_metadata() == decorated.to_metadata()


@pytest.mark.parametrize("extra", ["pandas", "polars"])
def test_optional_dataframe_values_are_lossless_but_metadata_stays_in_arrow(common_frame, extra):
    module = pytest.importorskip(extra)
    expected = common_frame.to_pyarrow(budget=Budget(max_rows=3)).sort_by("unit")
    expected = expected.select(
        [
            "unit",
            "anchor_id",
            "t0",
            "amount",
            "amount__complete",
            "amount__reason",
            "note",
            "note__complete",
            "note__reason",
        ]
    )
    converted = getattr(common_frame, f"to_{extra}")(budget=Budget(max_rows=3))
    if extra == "pandas":
        assert all(isinstance(dtype, module.ArrowDtype) for dtype in converted.dtypes)
        actual = pa.Table.from_pandas(converted, preserve_index=False)
    else:
        actual = converted.to_arrow()
    actual = actual.sort_by("unit")
    assert actual.schema.field("amount").type == expected.schema.field("amount").type
    assert actual.schema.field("t0").type == expected.schema.field("t0").type
    assert actual.to_pylist() == expected.to_pylist()
    assert actual["note"].null_count == expected["note"].null_count


def test_dataset_batches_and_parquet_preserve_typed_metadata(common_frame, tmp_path):
    budget = Budget(max_rows=3, max_output_bytes=1_000_000)
    expected = common_frame.to_pyarrow(budget=budget).sort_by("unit")
    expected_metadata = MaterializedFrame.from_arrow(expected)
    dataset = common_frame.stage("common_stack", budget=budget)
    batches = list(dataset.to_arrow_batches(batch_size=1))
    assert all(batch.num_rows <= 1 for batch in batches)
    if batches:
        streamed = pa.Table.from_batches(batches).sort_by("unit")
        assert streamed.to_pylist() == expected.to_pylist()
        assert MaterializedFrame.from_arrow(streamed).columns == expected_metadata.columns
    destination = tmp_path / "parquet"
    exported = dataset.export_parquet(destination=str(destination), budget=budget)
    assert exported.generation_id == dataset.manifest.generation.generation_id
    assert exported.schema == tuple((field.name, str(field.type)) for field in expected.schema)
    readback = pa.concat_tables(
        [pq.ParquetFile(destination / shard.path).read() for shard in exported.shards]
    ).sort_by("unit")
    assert readback.schema.equals(expected.schema, check_metadata=False)
    assert readback.to_pylist() == expected.to_pylist()
    restored = MaterializedFrame.from_arrow(readback)
    assert restored.roles == expected_metadata.roles
    assert restored.columns == expected_metadata.columns
    assert restored.provenance.design_hash == expected_metadata.provenance.design_hash
    assert restored.provenance.model_hash == expected_metadata.provenance.model_hash
    assert thaw(dataset.manifest.frame_metadata) == restored.to_metadata()


def test_pre_rename_owner_database_refuses_old_generation_and_stages_new(events_world, tmp_path):
    from terrine.runtime.backends.duckdb import DuckDBBackend

    frame, con = _durable_frame(events_world, tmp_path)
    fixtures = files("tests.fixtures")
    serialized = fixtures.joinpath("pre_rename_manifest.json").read_text()
    saved = fixtures.joinpath("pre_rename_saved_plan.json").read_text()
    definition = DatasetDefinition.from_json(fixtures.joinpath("definition_v2.json").read_bytes())
    namespace = "terrine_runtime"
    try:
        raw = con.con
        backend = DuckDBBackend(raw, namespace=namespace)
        raw.execute(
            "CREATE TABLE terrine_runtime.runtime_dataset_recipes "
            "(dataset VARCHAR,generation VARCHAR,recipe_json VARCHAR,request_key VARCHAR,"
            "manifest_json VARCHAR,PRIMARY KEY(dataset,generation))"
        )
        raw.execute(
            "CREATE TABLE terrine_runtime.runtime_dataset_requests "
            "(request_key VARCHAR PRIMARY KEY,recipe_json VARCHAR NOT NULL)"
        )
        raw.execute(
            "INSERT INTO terrine_runtime.runtime_dataset_recipes VALUES (?,?,?,?,?)",
            ["legacy", "pre-rename-generation", saved, "legacy-request", serialized],
        )
        raw.execute(
            "INSERT INTO terrine_runtime.runtime_dataset_requests VALUES (?,?)",
            ["legacy-request", saved],
        )
        raw.execute(
            "INSERT INTO terrine_runtime.runtime_manifests VALUES (?,?,?,?)",
            ["legacy", "pre-rename-generation", 1, "pre-rename-snapshot"],
        )
        assert backend.current_generation("legacy") == "pre-rename-generation"
        with pytest.raises(TerrineError) as error:
            Dataset.open_generation("legacy", "pre-rename-generation", con=con, namespace=namespace)
        assert error.value.code == "terrine.dataset.invalid_manifest"
        new = definition.build(frame._model).stage("legacy", budget=Budget(strict_scan=False))
        assert new.manifest.generation.generation_id != "pre-rename-generation"
        assert new.manifest.definition.digest == definition.digest
        reopened = Dataset.open_generation(
            "legacy", new.manifest.generation.generation_id, con=con, namespace=namespace
        )
        assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == len(
            events_world.tables["users"]
        )
        assert (
            raw.execute(
                "SELECT manifest_json FROM terrine_runtime.runtime_dataset_recipes "
                "WHERE dataset='legacy' AND generation='pre-rename-generation'"
            ).fetchone()[0]
            == serialized
        )
    finally:
        con.disconnect()
