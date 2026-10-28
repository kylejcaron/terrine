from __future__ import annotations

import hashlib
import json
from importlib.resources import files

import ibis
import pyarrow as pa
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.saved_plan import (
    DestinationRef,
    ModelReference,
    SavedPlan,
    resolve_plan,
)
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate
from terrine.spec.dataset import DatasetDefinition


def _model(con):
    catalog = Catalog.model_validate(
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
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )


class _Resolver:
    def __init__(self, model):
        self.model = model

    def resolve(self, reference):
        assert reference.owner_key == "owner-model"
        return self.model


def test_saved_plan_reconstructs_design_and_verifies_owner_model_in_fresh_resolver():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"), outcomes={})
        reference = ModelReference.from_model(model, owner_key="owner-model")
        destination = DestinationRef("output.duckdb", "analytics", "study")
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="snapshot-1",
            graph=dependency_graph(
                design=design, sources=("events",), definition_hash=design.spec_hash
            ),
        )
        dataset_definition = DatasetDefinition(
            schema_version=1,
            base_design=design,
            view={
                "kind": "training",
                "features": ["x"],
                "labels": ["y"],
                "incomplete_labels": "drop",
                "split": None,
            },
        )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=design,
            model=reference,
            destination=destination,
            namespace="runtime",
            expected_base_manifest="manifest-1",
            dataset_definition=dataset_definition,
        )
        store = RuntimeStore(con.con, namespace="runtime")
        store.initialize()
        other_store = RuntimeStore(con.con, namespace="other")
        other_store.initialize()
        with pytest.raises(TerrineError) as pin_error:
            saved_plan.pin_snapshot(other_store)
        assert pin_error.value.code == "terrine.runtime.saved_plan_mismatch"
        with pytest.raises(TerrineError) as release_error:
            saved_plan.release_snapshot(other_store)
        assert release_error.value.code == "terrine.runtime.saved_plan_mismatch"
        assert saved_plan.pin_snapshot(store) == "snapshot-1"
        restored = SavedPlan.from_json(saved_plan.to_json())
        restored_dataset_definition = restored.restore_dataset_definition()
        assert restored_dataset_definition is not None
        assert restored_dataset_definition.digest == dataset_definition.digest
        resolved_plan, resolved_model, resolved_design, resolved_destination = resolve_plan(
            restored, resolver=_Resolver(model), namespace="runtime"
        )
        assert resolved_plan.expected_base_manifest == "manifest-1"
        assert resolved_plan.expected_base_manifest_bound
        assert resolved_plan.definition.reconstruction_hash == restored.plan_hash
        assert resolved_plan.definition.schema_version == 2
        assert resolved_plan.tasks == plan.tasks
        assert resolved_plan.scan_bytes == plan.scan_bytes
        restored_plan = type(resolved_plan).deserialize(resolved_plan.serialize())
        assert restored_plan == resolved_plan
        assert restored_plan.expected_base_manifest == "manifest-1"
        assert restored_plan.expected_base_manifest_bound
        assert resolved_model is model
        assert isinstance(resolved_design, DatasetDefinition)
        assert resolved_design.digest == dataset_definition.digest
        con.raw_sql(
            "CREATE TABLE runtime.runtime_manifests "
            "(dataset VARCHAR PRIMARY KEY,generation VARCHAR NOT NULL,version BIGINT NOT NULL,"
            "snapshot_id VARCHAR)"
        )
        con.raw_sql("INSERT INTO runtime.runtime_manifests VALUES ('study','manifest-1',1,NULL)")
        assert resolved_destination == destination
        run = execute_plan(
            resolved_plan,
            con=con.con,
            namespace="runtime",
            run_id="saved_plan-run",
            run_task=lambda task, dependencies: pa.table({"layer": [task.layer]}),
            publish=lambda plan, outputs, lease: "generation-1",
        )
        assert store.snapshot_references("snapshot-1") == (
            ("published_manifest", '["study","generation-1"]'),
            ("saved_plan", saved_plan.plan_hash),
        )
        assert saved_plan.release_snapshot(store) == "snapshot-1"
        assert run.state == "succeeded"
    finally:
        con.disconnect()


def test_saved_plan_creation_refuses_approximate_quantile_design():
    from decimal import Decimal

    from terrine.spec import SketchRequest, approx_quantile

    design = Design(
        "user",
        FixedDate("2025-01-15"),
        features={
            "q": approx_quantile(
                "total",
                "0.5",
                window=("-1d", "0d"),
                sketch=SketchRequest("tdigest", "5.2.0", Decimal("0.01")),
            )
        },
    )
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot-1",
        graph=dependency_graph(
            design=design, sources=("events",), definition_hash=design.spec_hash
        ),
    )
    with pytest.raises(TerrineError) as error:
        SavedPlan.create(
            plan_json=plan.serialize(),
            design=design,
            model=ModelReference(
                "owner-model", "catalog", "model", "units", "readiness", "event_ids"
            ),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
    assert error.value.code == "terrine.runtime.sketch_sidecar_unavailable"


def test_saved_plan_preserves_strict_unknown_scan_refusal():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"), outcomes={})
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="snapshot",
            graph=dependency_graph(
                design=design,
                sources=("events",),
                definition_hash=design.spec_hash,
                partitions=("all",),
            ),
        )
        payload = json.loads(plan.serialize())
        payload["budget"] = {
            "max_output_bytes": None,
            "max_scan_bytes": 1,
            "max_rows": None,
            "strict_scan": True,
        }
        fingerprint_body = {
            key: payload[key]
            for key in (
                "schema_version",
                "namespace",
                "dataset",
                "definition_hash",
                "snapshot_id",
                "tasks",
                "change_evidence",
                "budget",
                "expected_base_manifest",
                "expected_base_manifest_bound",
            )
        }
        payload["fingerprint"] = hashlib.sha256(
            json.dumps(fingerprint_body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        saved_plan = SavedPlan.create(
            plan_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            design=design,
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as error:
            SavedPlan.from_json(saved_plan.to_json())
        assert error.value.code == "terrine.budget.scan_unknown"
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "binding", ("catalog_hash", "units_hash", "event_ids_hash", "readiness_hash", "model_hash")
)
def test_saved_plan_rejects_owner_model_declaration_mismatch(binding):
    from dataclasses import replace

    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"))
        reference = replace(
            ModelReference.from_model(model, owner_key="owner-model"), **{binding: "changed"}
        )
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="snapshot-1",
            graph=dependency_graph(
                design=design, sources=("events",), definition_hash=design.spec_hash
            ),
        )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=design,
            model=reference,
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as error:
            resolve_plan(saved_plan, resolver=_Resolver(model), namespace="runtime")
        assert error.value.code == "terrine.runtime.saved_plan_mismatch"
    finally:
        con.disconnect()


def test_saved_plan_rejects_plan_and_design_hash_mismatch():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        plan_design = Design("user", FixedDate("2025-01-15"))
        saved_design = Design("user", FixedDate("2025-01-16"))
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=plan_design.spec_hash,
            snapshot_id="snapshot-1",
            graph=dependency_graph(
                design=plan_design,
                sources=("events",),
                definition_hash=plan_design.spec_hash,
            ),
        )
        saved_plan = SavedPlan.create(
            plan_json=plan.serialize(),
            design=saved_design,
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as error:
            resolve_plan(saved_plan, resolver=_Resolver(model), namespace="runtime")
        assert error.value.code == "terrine.runtime.saved_plan_mismatch"
    finally:
        con.disconnect()


def test_saved_plan_rejects_null_checksum_with_coded_refusal():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        saved_plan = SavedPlan.create(
            plan_json=b"{}",
            design=Design("user", FixedDate("2025-01-15")),
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        document = json.loads(saved_plan.to_json())
        document["plan_hash"] = None
        with pytest.raises(TerrineError) as error:
            SavedPlan.from_json(json.dumps(document))
        assert error.value.code == "terrine.runtime.saved_plan_mismatch"
    finally:
        con.disconnect()


def test_saved_plan_round_trips_typed_dataset_view():
    from terrine.runtime.changes import ChangeSet
    from terrine.runtime.saved_plan import _bind_saved_plan

    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"))
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="retained",
            graph=dependency_graph(sources=("events",), definition_hash=design.spec_hash),
            changes=ChangeSet((), "conservative_rebuild"),
        )
        for view in (
            {
                "kind": "training",
                "features": ["x"],
                "labels": ["y"],
                "incomplete_labels": "keep",
                "split": {"kind": "time", "train_until": "2025-01-15"},
            },
            {
                "kind": "scoring",
                "features": [],
                "eligibility": {},
                "as_of": "2025-01-15T00:00:00+00:00",
                "parent_design_hash": design.spec_hash,
            },
        ):
            typed = DatasetDefinition(schema_version=1, base_design=design, view=view)
            saved = SavedPlan.create(
                plan_json=plan.serialize(),
                design=design,
                model=ModelReference.from_model(model, owner_key="owner"),
                destination=DestinationRef("owner.duckdb", "runtime", "study"),
                namespace="runtime",
                expected_base_manifest="generation-1",
                dataset_definition=typed,
            )
            restored = SavedPlan.from_json(saved.to_json())
            restored_definition = restored.restore_dataset_definition()
            assert restored_definition is not None and restored_definition.digest == typed.digest
            bound = _bind_saved_plan(restored, namespace="runtime")
            assert bound.expected_base_manifest == "generation-1"
            assert bound.definition.change_evidence == plan.definition.change_evidence
            tampered = json.loads(saved.to_json())
            tampered["dataset_definition_json"] = tampered["dataset_definition_json"].replace(
                "2025-01-15", "2025-01-16"
            )
            with pytest.raises(TerrineError):
                SavedPlan.from_json(json.dumps(tampered))
            tampered = json.loads(saved.to_json())
            raw_plan = json.loads(tampered["plan_json"])
            raw_plan["change_evidence"] = {}
            tampered["plan_json"] = json.dumps(raw_plan)
            with pytest.raises(TerrineError):
                SavedPlan.from_json(json.dumps(tampered))
    finally:
        con.disconnect()


def test_saved_plan_json_with_renamed_fields_refuses():
    serialized = files("tests.fixtures").joinpath("pre_rename_saved_plan.json").read_bytes()
    with pytest.raises(TerrineError) as error:
        SavedPlan.from_json(serialized)
    assert error.value.code == "terrine.runtime.saved_plan_mismatch"


@pytest.mark.parametrize("field", ["dataset_recipe_json", "recipe_hash", "unexpected"])
def test_saved_plan_refuses_unknown_fields_in_current_envelope(field):
    design = Design("user", FixedDate("2025-01-15"))
    plan = plan_refresh(
        namespace="runtime",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot-1",
        graph=dependency_graph(sources=("events",), definition_hash=design.spec_hash),
    )
    saved = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference("owner", "catalog", "model", "units", "readiness", "event_ids"),
        destination=DestinationRef("output.duckdb", "analytics", "study"),
        namespace="runtime",
        expected_base_manifest=None,
    )
    assert SavedPlan.from_json(saved.to_json()) == saved
    payload = json.loads(saved.to_json())
    payload[field] = "obsolete"
    with pytest.raises(TerrineError) as error:
        SavedPlan.from_json(json.dumps(payload))
    assert error.value.code == "terrine.runtime.saved_plan_mismatch"
