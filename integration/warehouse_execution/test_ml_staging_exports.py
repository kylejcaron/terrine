"""Parquet exports, export recovery and budgets, and refresh and replay of staged datasets."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from examples.ml_staging import create_model, training_design
from integration.warehouse_execution.ml_staging_fixtures import run_script, training_view
from terrine import Budget, agg
from terrine.data import Coverage
from terrine.dataset.export import ExportManifest
from terrine.dataset.reference import Dataset
from terrine.errors import TerrineError
from terrine.runtime.api import run_serialized_plan
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.saved_plan import SavedPlan
from terrine.spec import Design, FixedDate, gt
from tests import _worlds
from tests.conftest import events_world as events_world  # noqa: F401
from tests.dataset.test_staging import _durable_frame


def test_fresh_process_manifest_refresh_matches_independent_build_and_parquet(tmp_path):
    model = create_model(tmp_path / "owner.duckdb")
    dataset = training_view(model).stage("training", budget=Budget(strict_scan=False))
    manifest_path = tmp_path / "dataset.json"
    manifest_path.write_bytes(dataset.manifest.to_json().encode("utf-8"))
    model.con.disconnect()
    report = run_script(
        r"""
import json, sys
from pathlib import Path
import ibis
import pyarrow as pa
import pyarrow.parquet as pq
from examples.ml_staging import bind_model
from integration.warehouse_execution.ml_staging_fixtures import training_view
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
    clean = training_view(model).materialize(budget)
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


@pytest.fixture(scope="module")
def training_dataset(tmp_path_factory):
    model = create_model(tmp_path_factory.mktemp("training-dataset") / "owner.duckdb")
    try:
        yield training_view(model).stage("training", budget=Budget(strict_scan=False))
    finally:
        model.con.disconnect()


def test_export_recovers_durable_shard_and_discards_interrupted_shard(
    training_dataset, tmp_path, monkeypatch
):
    import terrine.dataset.export as exports

    dataset = training_dataset
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
def test_export_budget_cancellation_closes_pinned_batch_reader(
    training_dataset, tmp_path, monkeypatch, budget, code
):
    dataset = training_dataset
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


@pytest.mark.scale
def test_growing_dataset_bounds_client_batches_and_arrow_buffers(tmp_path):
    measurements = []
    for rows in (65_537, 327_681):
        root = tmp_path / str(rows)
        root.mkdir()
        measurements.append(
            run_script(
                "import json,sys; from pathlib import Path; "
                "import integration.warehouse_execution.ml_staging_fixtures as checks; "
                "print(json.dumps(checks.measure_bounded_workflow("
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


def _staged_dataset(events_world, root, *, empty_training=False):
    from terrine.dataset.reference import stage_frame

    if empty_training:
        events_world = replace(
            events_world,
            readiness={
                **events_world.readiness,
                "labels": Coverage("2025-01-01", "2025-01-01"),
            },
        )
    model = _worlds.bind_model(events_world, root)
    try:
        frame = Design(
            "user",
            FixedDate("2025-01-05"),
            features={"sessions": agg("sessions", window=("-7d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "7d"))},
        ).build(model)
        if empty_training:
            frame = frame.training_set(label="churn")
        yield stage_frame(frame, "training", budget=Budget(strict_scan=False))
    finally:
        model.con.disconnect()


@pytest.fixture(scope="module")
def events_dataset(events_world, tmp_path_factory):
    yield from _staged_dataset(events_world, tmp_path_factory.mktemp("events-dataset"))


@pytest.fixture(scope="module")
def empty_training_dataset(events_world, tmp_path_factory):
    yield from _staged_dataset(
        events_world, tmp_path_factory.mktemp("empty-training-dataset"), empty_training=True
    )


def test_parquet_export_preserves_pinned_arrow_schema_and_refuses_overwrite(
    events_dataset, tmp_path
):
    dataset = events_dataset
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


def test_export_discards_unfinished_shard_and_rewrites_on_retry(
    events_dataset, tmp_path, monkeypatch
):
    dataset = events_dataset
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


def test_export_syncs_shards_and_directories_before_checkpoint_and_publication(
    events_dataset, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset = events_dataset
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


def test_export_does_not_checkpoint_a_shard_when_fsync_fails(events_dataset, tmp_path, monkeypatch):
    import os

    from terrine.dataset import export as export_module

    dataset = events_dataset
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


def test_export_retry_resyncs_parent_after_interrupted_publication(
    events_dataset, tmp_path, monkeypatch
):
    import os

    from terrine.dataset import export as export_module

    dataset = events_dataset
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


@pytest.mark.parametrize("partition_by", [(), ("sessions",)])
def test_empty_training_export_preserves_complete_arrow_metadata(
    empty_training_dataset, tmp_path, partition_by
):
    from terrine.data import MaterializedFrame

    dataset = empty_training_dataset
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


def test_refresh_after_mode_change_rebuilds_layers_and_matches_fresh_build(tmp_path):
    """Switching a float sum from exact to native arithmetic rebuilds its persisted layers.

    A refresh without source changes replans every task for the changed declaration, and the
    refreshed rows equal a fresh build of the native declaration. This checks invalidation and
    equality with a fresh build, not the native error bound.
    """
    import ibis

    import terrine as tr
    from terrine.compile.layers import plan_layers

    con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
    con.create_table("customers", pa.table({"customer_id": ["alice", "bob"]}))
    ordered_at = [datetime(2025, 1, day, 9, tzinfo=UTC) for day in (3, 12, 20)]
    con.create_table(
        "orders",
        pa.table(
            {
                "customer_id": ["alice", "alice", "bob"],
                "ordered_at": pa.array(ordered_at, pa.timestamp("us", tz="UTC")),
                "amount": [30.25, 45.5, 80.125],
            }
        ),
    )

    def bind(exact: bool):
        return tr.Model.from_tables(
            con,
            units={"customer": tr.UnitSource("customers", "customer_id")},
            events={
                "orders": tr.Events(
                    "orders",
                    time="ordered_at",
                    key="customer_id",
                    metrics={"spend": tr.sum("amount", exact=exact)},
                    complete=("2025-01-01", "2025-01-31"),
                )
            },
        )

    models = {"exact": bind(True), "native": bind(False)}
    design = Design(
        "customer", FixedDate("2025-01-31"), features={"spend": agg("spend", window=("-30d", "0s"))}
    )
    prefixes = {
        mode: {key for key in plan_layers(design, model).keys if key.layer == "prefix"}
        for mode, model in models.items()
    }
    assert prefixes["exact"] and prefixes["native"]
    assert prefixes["exact"].isdisjoint(prefixes["native"])
    assert {key.measure for key in prefixes["exact"]}.isdisjoint(
        {key.measure for key in prefixes["native"]}
    )
    budget = Budget(strict_scan=False)
    dataset = design.build(models["exact"]).stage("spend", budget=budget)
    unchanged = ChangeSet((), "operator")
    assert not dataset.plan_refresh(models["exact"], changes=unchanged, budget=budget).tasks
    assert dataset.plan_refresh(models["native"], changes=unchanged, budget=budget).tasks
    refreshed = dataset.refresh(models["native"], changes=unchanged, budget=budget)
    assert refreshed.manifest.model_hash == models["native"].model_hash
    assert refreshed.manifest.model_hash != dataset.manifest.model_hash
    fresh = design.build(models["native"]).materialize(budget).table.replace_schema_metadata(None)
    rows = pa.Table.from_batches(list(refreshed.to_arrow_batches()))
    order = [("unit", "ascending"), ("anchor_id", "ascending")]
    assert (
        rows.select(fresh.column_names)
        .replace_schema_metadata(None)
        .sort_by(order)
        .equals(fresh.sort_by(order))
    )
    con.disconnect()


def test_native_and_exact_measures_on_same_column_stage_and_refresh(tmp_path):
    """Native and exact sums of one float column keep distinct persisted layers.

    Staging composes the frame from both prefix layers and a refresh after an insert rebuilds
    them; both generations equal fresh builds and keep each column's arithmetic.
    """
    import ibis

    import terrine as tr
    from terrine.compile.layers import plan_layers
    from terrine.data import MaterializedFrame

    con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
    con.create_table("customers", pa.table({"customer_id": ["alice", "bob"]}))
    ordered_at = [datetime(2025, 1, day, 9, tzinfo=UTC) for day in (3, 12, 20)]
    con.create_table(
        "orders",
        pa.table(
            {
                "customer_id": ["alice", "alice", "bob"],
                "ordered_at": pa.array(ordered_at, pa.timestamp("us", tz="UTC")),
                "amount": [30.25, 45.5, 80.125],
            }
        ),
    )
    model = tr.Model.from_tables(
        con,
        units={"customer": tr.UnitSource("customers", "customer_id")},
        events={
            "orders": tr.Events(
                "orders",
                time="ordered_at",
                key="customer_id",
                metrics={"native": tr.sum("amount"), "exact": tr.sum("amount", exact=True)},
                complete=("2025-01-01", "2025-01-31"),
            )
        },
    )
    design = Design(
        "customer",
        FixedDate("2025-01-31"),
        features={name: agg(name, window=("-30d", "0s")) for name in ("native", "exact")},
    )
    prefixes = [key for key in plan_layers(design, model).keys if key.layer == "prefix"]
    assert len({key.measure for key in prefixes}) == 2
    budget = Budget(strict_scan=False)
    order = [("unit", "ascending"), ("anchor_id", "ascending")]

    def staged_rows(dataset):
        staged = MaterializedFrame.from_arrow(
            pa.Table.from_batches(list(dataset.to_arrow_batches()))
        )
        fresh = design.build(model).materialize(budget)
        assert {name: staged.columns[name].arithmetic for name in ("native", "exact")} == {
            "native": "native",
            "exact": "exact",
        }
        assert staged.columns == fresh.columns
        table = staged.table.select(fresh.table.column_names).replace_schema_metadata(None)
        assert table.sort_by(order).equals(fresh.table.replace_schema_metadata(None).sort_by(order))
        return {row["unit"]: row for row in table.to_pylist()}

    dataset = design.build(model).stage("spend", budget=budget)
    staged_rows(dataset)
    # Below the exact path's resolution, only the native sum retains the insert, so the
    # refreshed columns differ only if each kept its own layer.
    con.raw_sql("INSERT INTO orders VALUES ('bob', TIMESTAMPTZ '2025-01-21 09:00:00+00', 4e-10)")
    changes = ChangeSet((Change("insert", "orders"),), "operator")
    refreshed = dataset.refresh(model, changes=changes, budget=budget)
    bob = staged_rows(refreshed)["bob"]
    assert bob["native"] != bob["exact"]
    con.disconnect()


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
