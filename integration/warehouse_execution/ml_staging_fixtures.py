"""Shared DuckDB ML-staging helpers; fresh-process checks import this module by name."""

from __future__ import annotations

import gc
import json
import subprocess
import sys
import weakref
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

from examples.ml_staging import create_model, training_design
from terrine import Budget, agg
from terrine.dataset.splits import GroupSplit
from terrine.runtime.arrow import ArrowOutput
from tests._timing import HANG_GUARD_SECONDS


def training_view(model):
    design = replace(
        training_design(),
        outcomes={"following_amount": agg("label_total", window=("0s", "2d"))},
    )
    return design.build(model).training_set(
        label="following_amount", incomplete_labels="keep", split=GroupSplit(seed=17)
    )


def run_script(script, *args):
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


def measure_bounded_workflow(root: Path, rows: int) -> dict[str, int]:
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
