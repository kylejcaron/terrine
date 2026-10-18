from datetime import UTC, datetime

import duckdb
import pytest

from terrine.data import BoundCoverage
from terrine.runtime.state import (
    DefinitionVersion,
    GenerationRef,
    RuntimeStore,
    SourceSnapshot,
)


def test_runtime_schema_is_customer_namespace_scoped():
    con = duckdb.connect(":memory:")
    left = RuntimeStore(con, namespace="customer_a")
    right = RuntimeStore(con, namespace="customer_b")
    left.initialize()
    right.initialize()
    left.write_generation(
        GenerationRef("customer_a", "orders", "g1"),
        definition=DefinitionVersion("def", "schema", "1"),
        snapshot=SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner"),
    )
    assert left.read_generation(GenerationRef("customer_a", "orders", "g1")) is not None
    assert right.read_generation(GenerationRef("customer_b", "orders", "g1")) is None


def test_generation_records_definition_and_snapshot_separately():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="customer")
    store.initialize()
    definition = DefinitionVersion("definition-hash", "schema-hash", "codec-1")
    snapshot = SourceSnapshot(
        "snapshot-1",
        {"orders": "revision-4"},
        {
            "orders": BoundCoverage(
                datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)
            )
        },
        datetime(2025, 2, 1, tzinfo=UTC),
        "owner-ref",
    )
    ref = GenerationRef("customer", "orders", "generation-1")
    store.write_generation(ref, definition=definition, snapshot=snapshot)
    record = store.read_generation(ref)
    assert record is not None
    assert record.definition == definition
    assert record.snapshot == snapshot
    assert "password" not in repr(record).lower()


def test_generation_identity_is_immutable_and_supersession_explicit():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="customer")
    store.initialize()
    ref = GenerationRef("customer", "orders", "g1")
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner")
    store.write_generation(ref, definition=definition, snapshot=snapshot)
    try:
        store.write_generation(
            ref, definition=DefinitionVersion("other", "s", "1"), snapshot=snapshot
        )
    except ValueError:
        pass
    else:
        raise AssertionError("generation identity was overwritten")
    next_ref = GenerationRef("customer", "orders", "g2")
    store.write_generation(next_ref, definition=definition, snapshot=snapshot, supersedes=ref)
    prior, current = store.read_generation(ref), store.read_generation(next_ref)
    assert prior is not None and current is not None
    assert prior.superseded_by == next_ref
    assert current.supersedes == ref


def test_failed_supersession_rolls_back_successor_and_predecessor_update():
    con = duckdb.connect(":memory:")
    base = RuntimeStore(con, namespace="customer")
    base.initialize()
    definition = DefinitionVersion("d", "s", "1")
    snapshot = SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner")
    prior = GenerationRef("customer", "orders", "g1")
    successor = GenerationRef("customer", "orders", "g2")
    base.write_generation(prior, definition=definition, snapshot=snapshot)

    class FailAfterSupersession:
        def execute(self, sql, parameters=None):
            result = con.execute(sql, parameters) if parameters is not None else con.execute(sql)
            if sql.startswith("UPDATE") and "superseded_by" in sql:
                raise RuntimeError("simulated interruption")
            return result

    store = RuntimeStore(FailAfterSupersession(), namespace="customer")
    try:
        store.write_generation(
            successor, definition=definition, snapshot=snapshot, supersedes=prior
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("injected supersession failure was ignored")
    old_record = base.read_generation(prior)
    assert old_record is not None and old_record.superseded_by is None
    assert base.read_generation(successor) is None


def test_generation_cannot_supersede_itself():
    con = duckdb.connect(":memory:")
    store = RuntimeStore(con, namespace="customer")
    store.initialize()
    ref = GenerationRef("customer", "orders", "g1")
    with pytest.raises(ValueError):
        store.write_generation(
            ref,
            definition=DefinitionVersion("d", "s", "1"),
            snapshot=SourceSnapshot("snap", {}, {}, datetime(2025, 1, 1, tzinfo=UTC), "owner"),
            supersedes=ref,
        )
    assert store.read_generation(ref) is None


def test_initialize_migrates_existing_generation_timestamp_to_naive_utc():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_generations (dataset VARCHAR NOT NULL, "
        "generation VARCHAR NOT NULL, definition_json VARCHAR NOT NULL, "
        "snapshot_json VARCHAR NOT NULL, created_at VARCHAR NOT NULL, "
        "supersedes VARCHAR, superseded_by VARCHAR, "
        "PRIMARY KEY(dataset, generation))"
    )
    con.execute(
        "INSERT INTO owner.runtime_generations VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "orders",
            "g1",
            '{"definition_hash":"d","schema_hash":"s","codec_version":"1"}',
            '{"snapshot_id":"snap","source_revisions":{},"readiness":{},'
            '"captured_at":"2025-01-01T02:00:00.123456+02:00","owner_reference":"owner"}',
            "2025-01-01T02:00:00.123456+02:00",
            None,
            None,
        ],
    )
    store = RuntimeStore(con, namespace="owner")
    store.initialize()
    record = store.read_generation(GenerationRef("owner", "orders", "g1"))
    assert record is not None
    assert record.created_at == datetime(2025, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    assert con.execute(
        "SELECT typeof(created_at), created_at FROM owner.runtime_generations"
    ).fetchone() == ("TIMESTAMP", datetime(2025, 1, 1, 0, 0, 0, 123456))


def test_generation_initialization_migrates_timestamp_with_case_variant_namespace():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute(
        "CREATE TABLE owner.runtime_generations (dataset VARCHAR NOT NULL, "
        "generation VARCHAR NOT NULL, definition_json VARCHAR NOT NULL, "
        "snapshot_json VARCHAR NOT NULL, created_at VARCHAR NOT NULL, "
        "supersedes VARCHAR, superseded_by VARCHAR, "
        "PRIMARY KEY(dataset, generation))"
    )
    con.execute(
        "INSERT INTO owner.runtime_generations VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "orders",
            "g1",
            '{"definition_hash":"d","schema_hash":"s","codec_version":"1"}',
            '{"snapshot_id":"snap","source_revisions":{},"readiness":{},'
            '"captured_at":"2025-01-01T00:00:00+00:00","owner_reference":"owner"}',
            "2025-01-01T00:00:00+00:00",
            None,
            None,
        ],
    )
    RuntimeStore(con, namespace="OWNER").initialize()
    assert con.execute("SELECT typeof(created_at) FROM owner.runtime_generations").fetchone() == (
        "TIMESTAMP",
    )
