from __future__ import annotations

import copy
import pickle
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
from tests.runtime.test_snapshot import _catalog, _design


def test_unknown_snapshot_refuses_only_retained_execution_before_source_work(tmp_path, monkeypatch):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        con.con.execute("CREATE TABLE users AS SELECT 1 AS user_id")
        model = Model.from_catalog(
            _catalog(),
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "user_id")},
        )
        provider = backend_provider(con)
        monkeypatch.setattr(
            provider, "capabilities", replace(provider.capabilities, retained_snapshot=None)
        )
        frame = _design().build(model)
        assert frame.materialize().table["total"].to_pylist() == [3]
        con.con.execute("DROP TABLE events")
        owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
        with pytest.raises(TerrineError) as error:
            SnapshotAuthority(owner).capture(
                model, design=_design(), con=con, namespace="runtime", run_id="run", budget=Budget()
            )
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["operation"] == "retained_snapshot"
        assert error.value.context["status"] == "unverified"
        assert not owner.snapshot_root.exists()
        for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
            assert restored.context == error.value.context
            with pytest.raises(TypeError):
                restored.context["status"] = "verified"  # ty: ignore[invalid-assignment]
    finally:
        con.disconnect()


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
