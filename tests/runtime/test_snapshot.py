from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import ibis
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "events": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"event": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
        }
    )


def _design():
    return Design(
        "user",
        FixedDate("2025-01-02T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )


def test_snapshot_root_discovery_includes_approx_distinct_without_unit_roster():
    from terrine.runtime.snapshot import _design_root_names
    from terrine.spec import EventAnchor, approx_distinct

    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = Model.from_catalog(
            _catalog(),
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        )
        design = Design(
            "user",
            EventAnchor("value"),
            features={"distinct": approx_distinct("total", window=("-1d", "0d"))},
        )
        sources, dimensions, units = _design_root_names(model, design)
        assert sources == frozenset({"events"})
        assert dimensions == units == frozenset()
    finally:
        con.disconnect()


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
        model, design=_design(), con=con, namespace="runtime", run_id="run-1", budget=Budget()
    )
    con.raw_sql("UPDATE events SET value=99")
    con.disconnect()

    restored = ReopenableSnapshot.from_json(receipt.to_json())
    reopened_con = ibis.duckdb.connect(str(state_path))
    try:
        execution = SnapshotAuthority(owner).reopen(restored, con=reopened_con, namespace="runtime")
        relation = execution.resolve(model.catalog.sources["events"].relation)
        assert execution.execute(relation).to_pylist()[0]["value"] == 3
        published = execution._published
        assert published is not None
        published.con.raw_sql("CREATE TEMP MACRO sum(x) AS first(x)")
        with pytest.raises(TerrineError) as error:
            execution.execute(relation)
        assert error.value.code == "terrine.execution.invalid_input_binding"
        source_generations = execution.snapshot.source_generations
        execution.close()
        reopened_again = SnapshotAuthority(owner).reopen(
            restored, con=reopened_con, namespace="runtime"
        )
        assert reopened_again.snapshot.source_generations == source_generations
        reopened_again.close()
    finally:
        reopened_con.disconnect()


def test_snapshot_authority_finds_retained_receipt_by_id(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    model = Model.from_catalog(
        _catalog(), con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    authority = SnapshotAuthority(owner)
    receipt = authority.capture(
        model, design=_design(), con=con, namespace="runtime", run_id="run-1", budget=Budget()
    )
    try:
        assert authority.find_retained(receipt.snapshot_id, namespace="runtime").to_json() == (
            receipt.to_json()
        )
    finally:
        con.disconnect()


def test_snapshot_receipt_rejects_null_checksum_with_coded_refusal():
    receipt = {
        "schema_version": 1,
        "snapshot_id": "snapshot",
        "namespace": "runtime",
        "run_id": "run",
        "owner_reference": "owner",
        "artifact_path": "/tmp/inputs.duckdb",
        "receipt_path": "/tmp/receipt.json",
        "catalog_json": "{}",
        "units_json": "{}",
        "event_ids_json": "{}",
        "model_hash": "hash",
        "roots": [],
        "artifact_hash": "hash",
        "receipt_hash": None,
    }
    with pytest.raises(TerrineError) as error:
        ReopenableSnapshot.from_json(json.dumps(receipt))
    assert error.value.code == "terrine.runtime.snapshot_invalid"


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
                design=_design(),
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
        model, design=_design(), con=con, namespace="runtime", run_id="run-1", budget=Budget()
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


def test_snapshot_gc_respects_pins_and_tombstones_collected_inputs(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    model = Model.from_catalog(
        _catalog(), con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )
    authority = SnapshotAuthority(owner)
    receipt = authority.capture(
        model, design=_design(), con=con, namespace="runtime", run_id="collect", budget=Budget()
    )
    import duckdb

    wrong_store = RuntimeStore(duckdb.connect(":memory:"), namespace="runtime")
    wrong_store.initialize()
    with pytest.raises(TerrineError) as owner_error:
        authority.collect_unreferenced(
            wrong_store, namespace="runtime", retain=timedelta(0), now=datetime.now(UTC)
        )
    assert owner_error.value.code == "terrine.runtime.snapshot_invalid"
    assert Path(receipt.artifact_path).exists()
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    store.pin_snapshot(receipt.snapshot_id, kind="saved_plan", reference="saved-plan")
    past = datetime.now(UTC) + timedelta(seconds=1)
    assert (
        authority.collect_unreferenced(store, namespace="runtime", retain=timedelta(0), now=past)
        == ()
    )
    authority.release(receipt, store, kind="saved_plan", reference="saved-plan")
    assert authority.collect_unreferenced(
        store, namespace="runtime", retain=timedelta(0), now=past
    ) == (receipt.snapshot_id,)
    assert not Path(receipt.artifact_path).exists()
    with pytest.raises(ValueError):
        store.pin_snapshot(receipt.snapshot_id, kind="saved_plan", reference="saved-plan")
    con.disconnect()


def test_snapshot_gc_rejects_attached_noncurrent_owner_database(tmp_path):
    owner = RuntimeOwner(state_path=tmp_path / "owner.duckdb", snapshot_root=tmp_path / "snapshots")
    owner_con = ibis.duckdb.connect(str(owner.state_path))
    owner_con.disconnect()
    con = ibis.duckdb.connect(":memory:")
    con.raw_sql(f"ATTACH '{owner.state_path}' AS owner_db")
    store = RuntimeStore(con.con, namespace="runtime")
    store.initialize()
    with pytest.raises(TerrineError) as error:
        SnapshotAuthority(owner).collect_unreferenced(
            store, namespace="runtime", retain=timedelta(0), now=datetime.now(UTC)
        )
    assert error.value.code == "terrine.runtime.snapshot_invalid"
    con.disconnect()


def test_snapshot_captures_only_sources_in_design_dependency_closure(tmp_path):
    state_path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=state_path, snapshot_root=tmp_path / "snapshots")
    con = ibis.duckdb.connect(str(state_path))
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
    con.raw_sql("CREATE TABLE users (user_id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1)")
    con.raw_sql("CREATE TABLE labels (ts TIMESTAMP, user_id INTEGER, label VARCHAR)")
    con.raw_sql("INSERT INTO labels VALUES ('2025-01-01', 1, 'unused')")
    definition = _catalog().model_dump(mode="python")
    definition["sources"]["labels"] = {
        "relation": {"kind": "table", "name": "labels"},
        "time": "ts",
        "units": {"user": "user_id"},
        "events": [],
        "properties": ["label"],
        "dimensions": [],
    }
    definition["properties"]["label"] = {
        "source": "labels",
        "column": "label",
        "dtype": "string",
        "as_of": "event_time",
    }
    catalog = Catalog.model_validate(definition)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={
            "events": Coverage("2025-01-01", "2025-02-01"),
            "labels": Coverage("2025-01-01", "2025-02-01"),
        },
        units={"user": UnitSource("users", "user_id")},
    )
    try:
        receipt = SnapshotAuthority(owner).capture(
            model, design=_design(), con=con, namespace="runtime", run_id="closure", budget=Budget()
        )
        from terrine.compile.dispatch import compile_design
        from terrine.spec.validate import validate_design

        execution = SnapshotAuthority(owner).reopen(receipt, con=con, namespace="runtime")
        try:
            design = _design()
            sources = set(validate_design(design, model).required_sources)
            execution_model = model._for_execution(
                execution, source_names=sources, unit_name=design.unit
            )
            compiled = compile_design(design, execution_model)
            compiled.validate(execute=execution.execute)
            assert execution.execute(compiled.table).num_rows == 1
        finally:
            execution.close()
        assert {entry["relation_key"] for entry in receipt.roots if entry["kind"] == "table"} == {
            "events",
            "users",
        }
    finally:
        con.disconnect()
