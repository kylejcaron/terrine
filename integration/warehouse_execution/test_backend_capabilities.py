"""Executed local capability evidence; no external warehouse connections."""

import sys
from _thread import LockType, RLock
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from threading import Event

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from terrine.data import Budget, MaterializedFrame
from terrine.errors import TerrineError, thaw
from terrine.execution.backend import backend_capabilities
from tests._timing import HANG_GUARD_SECONDS
from tests.conftest import events_world as events_world
from tests.conftest import native_inputs as native_inputs
from tests.fixtures.interop import interop_frame
from tests.test_frame_runtime import _design, _model


def test_duckdb_typed_dataset_arrow_parquet_and_generation_parity(tmp_path):
    con = ibis.duckdb.connect(str(tmp_path / "owner.duckdb"))
    try:
        frame = interop_frame(con)
        budget = Budget(max_rows=3, max_output_bytes=1_000_000)
        expected = frame.to_pyarrow(budget=budget).sort_by("unit")
        metadata = MaterializedFrame.from_arrow(expected)
        rows = expected.to_pylist()
        assert [row["amount"] for row in rows] == [
            Decimal("999999999999999999999999999999999999.98"),
            None,
            Decimal("0.00"),
        ]
        assert [row["tier"] for row in rows] == ["old", None, "empty"]
        assert rows[1][metadata.columns["amount"].reason_column] == "all_values_null"
        assert all(
            row[metadata.columns["future"].reason_column] == "incomplete_coverage" for row in rows
        )
        score = metadata.columns["score"]
        assert score.numeric_policy == "checked_float64_v1"
        assert score.absolute_error_column is not None
        assert expected.schema.field("amount").type == pa.decimal128(38, 2)
        assert expected.schema.field("t0").type == pa.timestamp("us", tz="UTC")

        dataset = frame.stage("interop", budget=budget)
        batches = list(dataset.to_arrow_batches(batch_size=1))
        assert all(batch.num_rows <= 1 for batch in batches)
        streamed = pa.Table.from_batches(batches).sort_by("unit")
        assert streamed.equals(expected)
        streamed_metadata = MaterializedFrame.from_arrow(streamed)
        assert streamed_metadata.columns == metadata.columns
        assert streamed_metadata.roles == metadata.roles
        assert streamed_metadata.provenance.design_hash == metadata.provenance.design_hash
        assert streamed_metadata.provenance.model_hash == metadata.provenance.model_hash
        assert dataset.manifest.source_snapshots["snapshot_id"]
        assert dataset.manifest.source_snapshots["receipt_hash"]
        snapshot = streamed_metadata.provenance.execution_snapshot
        assert snapshot.input_binding_id is not None
        assert snapshot.snapshot_id == dataset.manifest.source_snapshots["snapshot_id"]
        assert snapshot.input_binding_id == snapshot.snapshot_id
        assert snapshot.replay_equivalence == "verified"
        assert snapshot.source_generations
        assert all(
            isinstance(value, str) and value for value in snapshot.source_generations.values()
        )
        assert metadata.provenance.execution_snapshot.input_binding_id is None
        assert set(dataset.manifest.source_snapshots["revisions"].values()) == {None}

        destination = tmp_path / "export"
        exported = dataset.export_parquet(destination=str(destination), budget=budget)
        assert backend_capabilities(con).native_parquet_unload is None
        assert exported.generation_id == dataset.manifest.generation.generation_id
        assert exported.schema == tuple((field.name, str(field.type)) for field in streamed.schema)
        readback = pa.concat_tables(
            [pq.ParquetFile(destination / shard.path).read() for shard in exported.shards]
        ).sort_by("unit")
        assert readback.equals(streamed, check_metadata=True)
        assert (
            MaterializedFrame.from_arrow(readback).to_metadata() == streamed_metadata.to_metadata()
        )
        assert thaw(dataset.manifest.frame_metadata) == streamed_metadata.to_metadata()
        for extra in ("pandas", "polars"):
            try:
                module = __import__(extra)
            except ImportError:
                continue
            converted = (
                pa.Table.from_pandas(
                    readback.to_pandas(types_mapper=module.ArrowDtype), preserve_index=False
                )
                if extra == "pandas"
                else module.from_arrow(readback).to_arrow()
            )
            assert converted.to_pylist() == readback.to_pylist()
            assert converted.schema.field("amount").type == pa.decimal128(38, 2)

        con.con.execute("""
            INSERT INTO events VALUES
            ('a', '2025-01-02 11:59:59+00', 0.02, 0.0, NULL)
        """)
        with pytest.raises(TerrineError) as error:
            frame.materialize(budget)
        assert error.value.code == "terrine.temporal.exact_overflow"
        pinned = pa.Table.from_batches(list(dataset.to_arrow_batches(batch_size=2))).sort_by("unit")
        assert pinned.equals(streamed, check_metadata=True)
    finally:
        con.disconnect()


def test_shared_model_concurrent_exports_keep_independent_results(native_inputs):
    frame = _design().build(_model(native_inputs))
    origin = native_inputs[1].con
    execute, fetchall = origin.execute.__func__, origin.fetchall.__func__
    executed, release, fetched = Event(), Event(), Event()

    def export(first):
        paused = False

        def interleave(_frame, event, operation):
            nonlocal paused
            if first:
                if event == "c_return" and operation is execute and not paused:
                    paused = True
                    executed.set()
                    assert release.wait(timeout=HANG_GUARD_SECONDS)
                elif event == "c_return" and operation is fetchall:
                    fetched.set()
            elif not release.is_set() and (
                (event == "c_return" and operation is fetchall)
                or (event == "return" and isinstance(operation, (LockType, RLock)))
            ):
                # Allow serialization, or force the competing result to be consumed
                # before the first export fetches. No replacement database is used.
                release.set()
                assert fetched.wait(timeout=HANG_GUARD_SECONDS)

        sys.setprofile(interleave)
        try:
            return frame.materialize().table.select(["unit", "t0"]).sort_by("unit")
        finally:
            sys.setprofile(None)
            release.set()
            fetched.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(export, True)
        assert executed.wait(timeout=HANG_GUARD_SECONDS)
        second = pool.submit(export, False)
        results = [future.result(timeout=HANG_GUARD_SECONDS) for future in (first, second)]
    expected = [{"unit": unit, "t0": datetime(2025, 1, 3, tzinfo=UTC)} for unit in ("a", "b")]
    assert all(result.to_pylist() == expected for result in results)
