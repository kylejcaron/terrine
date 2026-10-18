from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
from terrine.runtime.leases import LeaseStore


def test_duckdb_manifest_switch_is_atomic_with_generation():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    stage = StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1, 2]})})
    receipt = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    assert backend.current_generation("orders") == "g1"
    assert backend.read_partition(receipt, "p1").to_pylist() == [{"id": 1}, {"id": 2}]


def test_partial_stage_never_becomes_visible():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")

    def fail(phase):
        if phase == "before_commit":
            raise RuntimeError("injected staging failure")

    backend.fault_injector = fail
    with pytest.raises(RuntimeError):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
            expected_manifest=None,
            lease=lease,
        )
    assert backend.current_generation("orders") is None


def test_manifest_conflict_refuses_stale_expected_generation():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
        expected_manifest=None,
        lease=lease,
    )
    with pytest.raises(ValueError):
        backend.publish_generation(
            StagedGeneration("orders", "g2", {"p1": pa.table({"id": [2]})}),
            expected_manifest=None,
            lease=lease,
        )
    assert backend.current_generation("orders") == "g1"


def test_retry_reuses_only_matching_generation_partitions():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    stage = StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})})
    receipt = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    retry = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    assert retry == receipt
    with pytest.raises(ValueError):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": pa.table({"id": [2]})}),
            expected_manifest=None,
            lease=lease,
        )


def test_expired_writer_cannot_publish_after_new_fence():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    leases = LeaseStore(con, namespace="owner")
    now = datetime.now(UTC)
    old = leases.acquire("dataset:orders", "old", ttl=timedelta(minutes=1), now=now)
    current = leases.acquire(
        "dataset:orders", "new", ttl=timedelta(minutes=1), now=now + timedelta(minutes=2)
    )
    assert current.fence > old.fence
    with pytest.raises(ValueError):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
            expected_manifest=None,
            lease=old,
        )
    assert backend.current_generation("orders") is None


def test_previously_published_generation_cannot_be_reactivated():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    first = StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})})
    second = StagedGeneration("orders", "g2", {"p1": pa.table({"id": [2]})})
    backend.publish_generation(first, expected_manifest=None, lease=lease)
    backend.publish_generation(second, expected_manifest="g1", lease=lease)

    with pytest.raises(ValueError, match="already published"):
        backend.publish_generation(
            StagedGeneration(
                "orders",
                "g1",
                {"p1": pa.table({"id": [1]}), "p2": pa.table({"id": [3]})},
            ),
            expected_manifest="g2",
            lease=lease,
        )

    assert backend.current_generation("orders") == "g2"


def test_publish_requires_lease_for_staged_dataset():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = LeaseStore(con, namespace="owner").acquire(
        "dataset:customers",
        "customer-writer",
        ttl=timedelta(minutes=1),
        now=datetime.now(UTC),
    )

    with pytest.raises(ValueError, match="lease resource"):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
            expected_manifest=None,
            lease=lease,
        )

    assert backend.current_generation("orders") is None


def _lease(con, namespace):
    store = LeaseStore(con, namespace=namespace)
    store.initialize()
    return store.acquire(
        "dataset:orders", "writer", ttl=timedelta(minutes=1), now=datetime.now(UTC)
    )
