from __future__ import annotations

import gc
import json
import subprocess
import sys
import weakref
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from examples.ml_staging import create_model, training_design
from terrine import Budget, agg
from terrine.dataset.export import ExportManifest
from terrine.dataset.reference import Dataset
from terrine.dataset.splits import GroupSplit
from terrine.errors import TerrineError
from terrine.runtime.arrow import ArrowOutput


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
        timeout=300,
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
    assert manifest.recipe.digest == refreshed.manifest.recipe.digest
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

    class Writer:
        def __init__(self, path, *args, **kwargs):
            self.path = Path(path)
            self.raw = original_writer(path, *args, **kwargs)
            writers.append(self.path.name)

        def write_table(self, table):
            self.raw.write_table(table)
            if len(writers) == 2:
                raise OSError("interrupted shard")

        def close(self):
            self.raw.close()

    destination = tmp_path / "export"
    monkeypatch.setattr(Dataset, "to_arrow_batches", one_row_batches)
    with monkeypatch.context() as patch:
        patch.setattr(exports.pq, "ParquetWriter", Writer)
        with pytest.raises(OSError, match="interrupted shard"):
            dataset.export_parquet(destination=str(destination), target_file_bytes=1)
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


def test_export_budget_cancellation_closes_pinned_batch_reader(tmp_path, monkeypatch):
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
            dataset.export_parquet(
                destination=str(tmp_path / "cancelled"), budget=Budget(max_rows=0)
            )
        assert error.value.code == "terrine.budget.rows_exceeded"
        assert closed == [True]
        assert not (tmp_path / "cancelled").exists()
        assert sum(batch.num_rows for batch in original(dataset, batch_size=1)) == 2
    finally:
        for stream in streams:
            stream.close()
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
