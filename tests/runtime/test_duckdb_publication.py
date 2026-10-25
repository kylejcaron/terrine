from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from terrine.errors import TerrineError
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


def test_publication_preserves_arrow_schema_and_values():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    source = pa.table(
        {
            "id": [1, 2, 3],
            "gap": pa.array(
                [timedelta(days=1, microseconds=1), None, timedelta(microseconds=-3)],
                type=pa.duration("us"),
            ),
            "reason": pa.nulls(3),
        }
    ).replace_schema_metadata({b"terrine.frame": b"metadata"})

    receipt = backend.publish_generation(
        StagedGeneration("orders", "g1", {"frame": source}),
        expected_manifest=None,
        lease=lease,
    )

    restored = backend.read_partition(receipt, "frame")
    assert restored.schema.equals(source.schema, check_metadata=True)
    assert restored.to_pylist() == source.to_pylist()


def test_lazy_read_refuses_nanosecond_duration_without_losing_exact_arrow_values():
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    source = pa.table({"gap": pa.array([1, None, -3], type=pa.duration("ns"))})
    receipt = backend.publish_generation(
        StagedGeneration("orders", "g1", {"frame": source}),
        expected_manifest=None,
        lease=_lease(con, "owner"),
    )

    assert backend.read_partition(receipt, "frame").equals(source)
    with pytest.raises(TerrineError) as error:
        backend.read_relation(receipt, "frame", ibis_backend=None)
    assert error.value.code == "terrine.runtime.lazy_duration_unsupported"
    assert error.value.context == {
        "column": "gap",
        "unit": "ns",
        "route": "DuckDBBackend.read_partition or open_published_frame",
    }


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

    with pytest.raises(ValueError):
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

    with pytest.raises(ValueError):
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


@pytest.mark.parametrize("change", ["duration_unit", "metadata"])
def test_publication_retry_verifies_logical_schema_before_physical_digest(change):
    con = duckdb.connect(":memory:")
    backend = DuckDBBackend(con, namespace="owner")
    lease = _lease(con, "owner")
    table = pa.table({"gap": pa.array([1], type=pa.duration("us"))})
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"frame": table}),
        expected_manifest=None,
        lease=lease,
    )
    changed = (
        pa.table({"gap": pa.array([1], type=pa.duration("ns"))})
        if change == "duration_unit"
        else table.replace_schema_metadata({b"contract": b"changed"})
    )
    with pytest.raises(ValueError):
        backend.publish_generation(
            StagedGeneration("orders", "g1", {"frame": changed}),
            expected_manifest="g1",
            lease=lease,
        )


@pytest.mark.parametrize("nested", [False, True])
def test_publication_digest_preserves_timestamp_instants_across_session_timezones(nested):
    from terrine.runtime.backends.duckdb import _digest_relation

    con = duckdb.connect()
    con.execute("SET timezone='UTC'")
    backend = DuckDBBackend(con, namespace="owner")
    dtype = pa.timestamp("us", "Pacific/Auckland")
    values = [datetime(2025, 1, 1, tzinfo=UTC), None]
    column = (
        pa.array([[values[0], None], None], pa.list_(dtype)) if nested else pa.array(values, dtype)
    )
    table = pa.table({"value": column})
    lease = _lease(con, "owner")
    stage = StagedGeneration("orders", "g1", {"frame": table})
    receipt = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    stored = con.execute(
        "SELECT table_name,digest FROM owner.runtime_partitions WHERE partition='frame'"
    ).fetchone()
    assert stored is not None
    physical, digest = stored
    con.execute("SET timezone='America/New_York'")
    assert _digest_relation(con, "owner", physical) == digest
    assert backend.read_partition(receipt, "frame").equals(table)
    assert backend.publish_generation(stage, expected_manifest="g1", lease=lease) == receipt
    con.close()


@pytest.mark.parametrize("reader", [False, True])
def test_publication_retry_verifies_original_checksum_format(reader):
    import hashlib

    con = duckdb.connect()
    backend = DuckDBBackend(con, namespace="owner")
    table = pa.table({"value": [1, None]})
    stage = StagedGeneration("orders", "g1", {"frame": table})
    lease = _lease(con, "owner")
    receipt = backend.publish_generation(stage, expected_manifest=None, lease=lease)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    original = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
    con.execute("UPDATE owner.runtime_partitions SET digest=?", [original])
    retry = StagedGeneration("orders", "g1", {"frame": table.to_reader() if reader else table})
    assert backend.publish_generation(retry, expected_manifest="g1", lease=lease) == receipt
    assert backend.read_partition(receipt, "frame").equals(table)
    physical = con.execute("SELECT table_name FROM owner.runtime_partitions").fetchone()
    assert physical is not None
    con.execute(f'UPDATE owner."{physical[0]}" SET value=2 WHERE value=1')
    with pytest.raises(ValueError):
        backend.publish_generation(stage, expected_manifest="g1", lease=lease)
    con.close()
