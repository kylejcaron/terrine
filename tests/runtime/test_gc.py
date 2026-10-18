from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.gc import collect_superseded
from terrine.runtime.state import DefinitionVersion, GenerationRef, RuntimeStore, SourceSnapshot


def test_gc_only_removes_explicit_unpinned_superseded_generations():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner")
    old = GenerationRef("owner", "orders", "g1")
    current = GenerationRef("owner", "orders", "g2")
    store.write_generation(old, definition=definition, snapshot=snapshot)
    store.write_generation(current, definition=definition, snapshot=snapshot, supersedes=old)
    store.pin_generation(old, "reader-a")
    now = datetime.now(UTC) + timedelta(seconds=1)

    dry = collect_superseded(store, retain=timedelta(0), now=now)
    assert dry.candidates == (old,)
    assert store.read_generation(old) is not None

    store.unpin_generation(old, "reader-a")
    removed = collect_superseded(store, retain=timedelta(0), now=now, dry_run=False)
    assert removed.removed == (old,)
    assert store.read_generation(old) is None
    assert store.read_generation(current) is not None


def test_pin_conflicts_with_in_progress_gc_guard_update():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner")
    old = GenerationRef("owner", "orders", "g1")
    new = GenerationRef("owner", "orders", "g2")
    store.write_generation(old, definition=definition, snapshot=snapshot)
    store.write_generation(new, definition=definition, snapshot=snapshot, supersedes=old)

    gc_con = con.cursor()
    pin_store = RuntimeStore(con.cursor(), namespace="owner")
    gc_con.execute("BEGIN TRANSACTION")
    gc_con.execute(
        'UPDATE "owner".runtime_generations SET guard_version=guard_version+1 '
        "WHERE dataset=? AND generation=?",
        ["orders", "g1"],
    )
    with pytest.raises(duckdb.TransactionException):
        pin_store.pin_generation(old, "reader")
    gc_con.execute("ROLLBACK")
    pin_count = con.execute(
        'SELECT count(*) FROM "owner".runtime_generation_pins WHERE dataset=? AND generation=?',
        ["orders", "g1"],
    ).fetchone()
    assert pin_count is not None and pin_count[0] == 0


def test_gc_never_removes_manifest_referenced_generation():
    con = duckdb.connect(":memory:")
    DuckDBBackend(con, namespace="owner")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner")
    current = GenerationRef("owner", "orders", "g1")
    staged_next = GenerationRef("owner", "orders", "g2")
    store.write_generation(current, definition=definition, snapshot=snapshot)
    store.write_generation(
        staged_next, definition=definition, snapshot=snapshot, supersedes=current
    )
    con.execute(
        'INSERT INTO "owner".runtime_manifests (dataset, generation, version) VALUES (?, ?, ?)',
        ["orders", "g1", 1],
    )

    report = collect_superseded(store, retain=timedelta(0), now=datetime.now(UTC), dry_run=False)

    assert current not in report.removed
    assert store.read_generation(current) is not None


def test_gc_removes_superseded_duckdb_partition_storage():
    import pyarrow as pa

    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend
    from terrine.runtime.leases import LeaseStore

    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    backend = DuckDBBackend(con, namespace="owner")
    leases = LeaseStore(con, namespace="owner")
    now = datetime.now(UTC)
    lease = leases.acquire("dataset:orders", "writer", ttl=timedelta(minutes=1), now=now)
    backend.publish_generation(
        StagedGeneration("orders", "g1", {"p1": pa.table({"id": [1]})}),
        expected_manifest=None,
        lease=lease,
    )
    leases.release(lease)
    lease = leases.acquire(
        "dataset:orders", "writer-2", ttl=timedelta(minutes=1), now=datetime.now(UTC)
    )
    backend.publish_generation(
        StagedGeneration("orders", "g2", {"p1": pa.table({"id": [2]})}),
        expected_manifest="g1",
        lease=lease,
    )
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, now, "owner")
    old, new = GenerationRef("owner", "orders", "g1"), GenerationRef("owner", "orders", "g2")
    store.write_generation(old, definition=definition, snapshot=snapshot)
    store.write_generation(new, definition=definition, snapshot=snapshot, supersedes=old)
    physical_row = con.execute(
        'SELECT table_name FROM "owner".runtime_partitions WHERE generation=?', ["g1"]
    ).fetchone()
    assert physical_row is not None
    physical = physical_row[0]

    report = collect_superseded(store, retain=timedelta(0), now=datetime.now(UTC), dry_run=False)
    assert report.removed == (old,)
    remaining = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
        ["owner", physical],
    ).fetchone()
    assert remaining is None
