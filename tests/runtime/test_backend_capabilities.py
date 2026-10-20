from __future__ import annotations

from dataclasses import replace

import ibis
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.execution.backend import backend_provider
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.spec import Design, EventAnchor
from tests.runtime.test_snapshot import _catalog


def test_unknown_scan_estimate_refuses_strict_budget_but_not_non_strict(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    frame = Design("user", EventAnchor("session")).build(model)
    assert backend_provider(con).capabilities.scan_estimate is None
    with pytest.raises(TerrineError) as error:
        frame.materialize(Budget(max_scan_bytes=1))
    assert error.value.code == "terrine.budget.scan_unknown"
    with pytest.warns(RuntimeWarning):
        assert frame.materialize(Budget(max_scan_bytes=1, strict_scan=False)).table.num_rows == 2


@pytest.mark.parametrize("capability", ["dialect_admission", "arrow_batches"])
def test_query_gates_refuse_only_the_requested_operation(native_inputs, monkeypatch, capability):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    frame = Design("user", EventAnchor("session")).build(model)
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, **{capability: None})
    )
    with pytest.raises(TerrineError) as error:
        frame.materialize()
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == capability
    assert error.value.context["route"]


@pytest.mark.parametrize("operation", ["table_publication", "atomic_manifest_publish"])
def test_unverified_publication_refuses_before_lease_or_destination_work(
    native_inputs, monkeypatch, operation
):
    import pyarrow as pa

    from terrine.runtime.backends.base import StagedGeneration
    from terrine.runtime.backends.duckdb import DuckDBBackend

    backend = DuckDBBackend(native_inputs[1].con, namespace="runtime")
    monkeypatch.setattr(backend, "capabilities", replace(backend.capabilities, **{operation: None}))
    with pytest.raises(TerrineError) as error:
        backend.publish_generation(
            StagedGeneration("data", "generation", {"p": pa.table({"x": [1]})}),
            expected_manifest=None,
            lease=None,
        )
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == operation
    assert backend.current_generation("data") is None


@pytest.mark.parametrize("operation", ["exact_timestamps", "exact_decimals"])
def test_unverified_scalar_semantics_refuse_declared_source_types(
    native_inputs, monkeypatch, operation
):
    catalog, con = native_inputs
    if operation == "exact_decimals":
        con.con.execute("ALTER TABLE events ADD COLUMN amount DECIMAL(20, 2)")
        definition = catalog.model_dump(mode="python")
        definition["facts"]["session"]["column"] = "amount"
        from terrine.semantic.models import Catalog

        catalog = Catalog.model_validate(definition)
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, **{operation: None})
    )
    with pytest.raises(TerrineError) as error:
        Model.from_catalog(catalog, con=con, readiness={})
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == operation


def test_roster_only_compilation_requires_verified_timestamp_semantics(native_inputs, monkeypatch):
    from terrine.spec import FixedDate

    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog, con=con, readiness={}, units={"user": UnitSource("users", "user_id")}
    )
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, exact_timestamps=None)
    )
    with pytest.raises(TerrineError) as error:
        Design("user", FixedDate("2025-01-02")).build(model)
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == "exact_timestamps"


@pytest.fixture
def publication_frame(tmp_path):
    from terrine.spec import FixedDate

    con = ibis.duckdb.connect(str(tmp_path / "capabilities.duckdb"))
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.con.execute("CREATE TABLE users AS SELECT 1 AS user_id")
        model = Model.from_catalog(
            _catalog(), con=con, readiness={}, units={"user": UnitSource("users", "user_id")}
        )
        yield Design("user", FixedDate("2025-01-02")).build(model), con
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    ("operation", "value", "status"),
    [
        ("table_publication", None, "unverified"),
        ("atomic_manifest_publish", False, "unsupported"),
        ("retained_snapshot", None, "unverified"),
    ],
)
def test_stage_refuses_only_missing_publication_evidence(
    publication_frame, monkeypatch, operation, value, status
):
    frame, con = publication_frame
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, **{operation: value})
    )
    assert frame.materialize().table["unit"].to_pylist() == [1]
    con.con.execute("DROP TABLE users")
    with pytest.raises(TerrineError) as error:
        frame.stage("dataset")
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == operation
    assert error.value.context["status"] == status
    assert con.con.execute(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name='terrine_runtime'"
    ).fetchone() == (0,)


def test_dataset_arrow_gate_precedes_read_and_export_destination_work(
    publication_frame, tmp_path, monkeypatch
):
    frame, con = publication_frame
    dataset = frame.stage("dataset")
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, arrow_batches=None)
    )
    for operation in (
        lambda: dataset.to_arrow_batches(),
        lambda: dataset.export_parquet(destination=str(tmp_path / "refused")),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["operation"] == "arrow_batches"
    assert not (tmp_path / "refused").exists()
    assert not (tmp_path / ".refused.lock").exists()
    assert not (tmp_path / ".refused.partial").exists()


def test_dataset_export_uses_arrow_without_native_unload_and_checks_scan_policy(
    publication_frame, tmp_path, monkeypatch
):
    import pyarrow.parquet as pq

    frame, con = publication_frame
    dataset = frame.stage("dataset")
    provider = backend_provider(con)
    monkeypatch.setattr(
        provider, "capabilities", replace(provider.capabilities, native_parquet_unload=False)
    )
    destination = tmp_path / "parquet"
    with pytest.raises(TerrineError) as error:
        dataset.export_parquet(destination=str(destination), budget=Budget(max_scan_bytes=1))
    assert error.value.code == "terrine.budget.scan_unknown"
    assert not (tmp_path / ".parquet.lock").exists()
    with pytest.warns(RuntimeWarning):
        export = dataset.export_parquet(
            destination=str(destination), budget=Budget(max_scan_bytes=1, strict_scan=False)
        )
    assert export.row_count == 1
    assert pq.ParquetFile(destination / export.shards[0].path).read()["unit"].to_pylist() == [1]


def test_dataset_reopen_refuses_unverified_backend_before_native_attributes(publication_frame):
    from terrine.dataset.reference import Dataset

    frame, _ = publication_frame
    dataset = frame.stage("dataset")

    class Unverified:
        name = "bigquery"

        @property
        def con(self):
            raise AssertionError("unverified backend native resources accessed")

    with pytest.raises(TerrineError) as error:
        Dataset.from_manifest(dataset.manifest, con=Unverified())
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["backend"] == "bigquery"


def test_retained_capture_accepts_raw_owner_connection_with_verified_model(
    publication_frame, tmp_path
):
    frame, con = publication_frame
    authority = SnapshotAuthority(
        RuntimeOwner(tmp_path / "capabilities.duckdb", tmp_path / "snapshots")
    )
    receipt = authority.capture(
        frame._model,
        design=frame._design,
        con=con.con,
        namespace="owner",
        run_id="capture",
        budget=Budget(strict_scan=False),
    )
    assert receipt.snapshot_id
    restored = authority.find_retained(receipt.snapshot_id, namespace="owner")
    assert restored.to_json() == receipt.to_json()
