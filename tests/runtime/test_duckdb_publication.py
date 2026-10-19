from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.backends.duckdb import DuckDBBackend, StagedGeneration
from terrine.runtime.leases import LeaseStore
from terrine.runtime.state import RuntimeStore


def test_duckdb_manifest_switch_is_atomic_with_generation():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    stage = StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1, 2]})})
    receipt = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    assert backend.current_generation("orders") == "g1"
    assert backend.read_partition(receipt, "p1").to_pylist() == [{"id": 1}, {"id": 2}]


def test_manifest_publication_atomically_moves_snapshot_reference():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}, snapshot_id="snap-1"),
        expected_manifest=None,
        lease=lease,
    )
    assert store.snapshot_references("snap-1") == (("published_manifest", '["orders","g1"]'),)
    backend.publish_generation(
        StagedGeneration("orders", "g2", {"p1": pa.table({"id": [2]})}, snapshot_id="snap-2"),
        expected_manifest="g1",
        lease=lease,
    )
    assert store.snapshot_references("snap-1") == ()
    assert store.snapshot_references("snap-2") == (("published_manifest", '["orders","g2"]'),)
    manifest = con.execute(
        'SELECT snapshot_id FROM "owner".runtime_manifests WHERE dataset=?', ["orders"]
    ).fetchone()
    assert manifest is not None and manifest[0] == "snap-2"


def test_empty_generation_is_rejected_without_changing_manifest():
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
            StagedGeneration("orders", "g2", {}),
            expected_manifest="g1",
            lease=lease,
        )
    assert backend.current_generation("orders") == "g1"


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


def test_retry_digest_ignores_arrow_chunk_boundaries():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    first = pa.table({"id": pa.chunked_array([[1], [2]])})
    retry = pa.table({"id": [1, 2]})
    receipt = backend.publish_generation(
        StagedGeneration("orders", "g1", {"p1": first}),
        expected_manifest=None,
        lease=lease,
    )
    assert (
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": retry}),
            expected_manifest=None,
            lease=lease,
        )
        == receipt
    )


def test_publication_without_snapshot_releases_previous_tracked_reference():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}, snapshot_id="snap-1"),
        expected_manifest=None,
        lease=lease,
    )
    backend.publish_generation(
        StagedGeneration("orders", "g2", {"p1": pa.table({"id": [2]})}),
        expected_manifest="g1",
        lease=lease,
    )
    assert store.snapshot_references("snap-1") == ()
    assert (
        con.execute(
            'SELECT snapshot_id FROM "owner".runtime_snapshot_manifests WHERE dataset=?',
            ["orders"],
        ).fetchall()
        == []
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


def test_takeover_after_final_fence_check_cannot_publish():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    leases = LeaseStore(con, namespace="owner")
    now = datetime.now(UTC)
    old = leases.acquire("dataset:orders", "old", ttl=timedelta(seconds=1), now=now)
    takeover = LeaseStore(con.cursor(), namespace="owner")

    def take_over(phase):
        if phase != "before_commit":
            return
        takeover.acquire(
            "dataset:orders",
            "new",
            ttl=timedelta(minutes=1),
            now=now + timedelta(seconds=2),
        )

    backend.fault_injector = take_over
    with pytest.raises(duckdb.TransactionException):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
            expected_manifest=None,
            lease=old,
        )
    assert backend.current_generation("orders") is None


def test_reconcile_uses_durable_run_to_publication_mapping():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    stage = StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}, run_id="run-1")
    backend.publish_generation(stage, expected_manifest=None, lease=lease)
    result = backend.reconcile("run-1")
    assert result.run_id == "run-1"
    assert result.published_generation == "g1"
    assert result.manifest_version == 1
    assert backend.reconcile("orders").published_generation is None


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
