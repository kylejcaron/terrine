from __future__ import annotations

import ibis
import pytest

from terrine.data import Budget, Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.semantic.models import Catalog


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


def test_snapshot_receipt_reopens_same_inputs_after_fresh_owner_connection(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    model = Model.from_catalog(
        _catalog(), con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    receipt = SnapshotAuthority(owner).capture(
        model, con=con, namespace="runtime", run_id="run-1", budget=Budget()
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()

    restored = ReopenableSnapshot.from_json(receipt.to_json())
    reopened_con = ibis.duckdb.connect(str(state_path))
    try:
        execution = SnapshotAuthority(owner).reopen(restored, con=reopened_con, namespace="runtime")
        relation = execution.resolve(model.catalog.sources["events"].relation)
        assert execution.execute(relation).to_pylist()[0]["value"] == 3
        execution.close()
    finally:
        reopened_con.disconnect()


def test_capture_budget_refuses_unknown_scan_before_creating_snapshot(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = Model.from_catalog(
            _catalog(), con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
        )
        with pytest.raises(TerrineError) as error:
            SnapshotAuthority(owner).capture(
                model,
                con=con,
                namespace="runtime",
                run_id="run-1",
                budget=Budget(max_scan_bytes=1),
            )
        assert error.value.code == "terrine.budget.scan_unknown"
        assert not list((tmp_path / "snapshots").glob("**/inputs.duckdb"))
    finally:
        con.disconnect()


def test_snapshot_reopen_refuses_modified_retained_inputs(tmp_path):
    import duckdb

    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    model = Model.from_catalog(
        _catalog(), con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    receipt = SnapshotAuthority(owner).capture(
        model, con=con, namespace="runtime", run_id="run-1", budget=Budget()
    )
    con.disconnect()

    artifact = duckdb.connect(receipt.artifact_path)
    artifact.execute("UPDATE input_0 SET value=7")
    artifact.close()
    reopened_con = ibis.duckdb.connect(str(state_path))
    try:
        with pytest.raises(TerrineError) as error:
            SnapshotAuthority(owner).reopen(
                ReopenableSnapshot.from_json(receipt.to_json()),
                con=reopened_con,
                namespace="runtime",
            )
        assert error.value.code == "terrine.runtime.snapshot_invalid"
    finally:
        reopened_con.disconnect()
