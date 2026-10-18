from __future__ import annotations

import multiprocessing
import os
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.backends.base import PublicationReceipt, StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.leases import LeaseStore


def _crash_writer(path: str, crash_at: str) -> None:
    con = duckdb.connect(path)
    backend = DuckDBBackend(con, namespace="runtime_owner")
    lease_store = LeaseStore(con, namespace="runtime_owner")
    lease = lease_store.acquire(
        "dataset:orders", "crash-writer", ttl=timedelta(minutes=1), now=datetime.now(UTC)
    )

    def crash(phase: str) -> None:
        if phase == crash_at:
            os._exit(91)

    backend.fault_injector = crash
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"all": pa.table({"id": [2, 3]})}),
        expected_manifest="g0",
        lease=lease,
    )
    con.close()


@pytest.mark.parametrize(
    ("boundary", "expected_generation", "expected_values"),
    [
        ("before_commit", "g0", [1]),
        ("after_commit", "g1", [2, 3]),
    ],
)
def test_duckdb_crash_boundary_retry_reconciles_manifest(
    tmp_path, boundary, expected_generation, expected_values
):
    path = str(tmp_path / "owner.duckdb")
    writer = duckdb.connect(path)
    backend = DuckDBBackend(writer, namespace="runtime_owner")
    leases = LeaseStore(writer, namespace="runtime_owner")
    lease = leases.acquire(
        "dataset:orders", "initial-writer", ttl=timedelta(minutes=1), now=datetime.now(UTC)
    )
    backend.publish_generation(
        StagedGeneration("orders", "g0", {"all": pa.table({"id": [1]})}),
        expected_manifest=None,
        lease=lease,
    )
    leases.release(lease)
    writer.close()

    process = multiprocessing.get_context("spawn").Process(
        target=_crash_writer, args=(path, boundary)
    )
    process.start()
    process.join(timeout=30)
    if process.is_alive():
        process.kill()
        process.join()
        raise AssertionError("crash-boundary process did not exit")
    assert process.exitcode == 91

    reader = duckdb.connect(path)
    recovered = DuckDBBackend(reader, namespace="runtime_owner")
    assert recovered.current_generation("orders") == expected_generation
    version_row = reader.execute(
        'SELECT version FROM "runtime_owner".runtime_manifests WHERE dataset=?', ["orders"]
    ).fetchone()
    assert version_row is not None
    publication = PublicationReceipt("orders", expected_generation, version_row[0], ("all",))
    assert recovered.read_partition(publication, "all")["id"].to_pylist() == expected_values
    reader.close()
