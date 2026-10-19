from __future__ import annotations

import hashlib
import json

import ibis
import pyarrow as pa
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import (
    DestinationRef,
    ModelReference,
    SavedPlanRecipe,
    resolve_plan,
)
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate


def _model(con):
    catalog = Catalog.model_validate(
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
    return Model.from_catalog(
        catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
    )


class _Resolver:
    def __init__(self, model):
        self.model = model

    def resolve(self, reference):
        assert reference.owner_key == "owner-model"
        return self.model


def test_saved_plan_recipe_reconstructs_design_and_verifies_owner_model_in_fresh_resolver():
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
        recipe = SavedPlanRecipe.create(
            plan_json=plan.serialize(),
            design=design,
            model=reference,
            destination=destination,
            namespace="runtime",
            expected_base_manifest="manifest-1",
        )
        store = RuntimeStore(con.con, namespace="runtime")
        store.initialize()
        other_store = RuntimeStore(con.con, namespace="other")
        other_store.initialize()
        with pytest.raises(TerrineError) as pin_error:
            recipe.pin_snapshot(other_store)
        assert pin_error.value.code == "terrine.runtime.plan_recipe_mismatch"
        with pytest.raises(TerrineError) as release_error:
            recipe.release_snapshot(other_store)
        assert release_error.value.code == "terrine.runtime.plan_recipe_mismatch"
        assert recipe.pin_snapshot(store) == "snapshot-1"
        restored = SavedPlanRecipe.from_json(recipe.to_json())
        resolved_plan, resolved_model, resolved_design, resolved_destination = resolve_plan(
            restored, resolver=_Resolver(model), namespace="runtime"
        )
        assert resolved_plan.expected_base_manifest == "manifest-1"
        assert resolved_plan.expected_base_manifest_bound
        restored_plan = type(resolved_plan).deserialize(resolved_plan.serialize())
        assert restored_plan.expected_base_manifest == "manifest-1"
        assert restored_plan.expected_base_manifest_bound
        assert resolved_model is model
        assert resolved_design.spec_hash == design.spec_hash
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
            run_id="recipe-run",
            run_task=lambda task, dependencies: pa.table({"layer": [task.layer]}),
            publish=lambda plan, outputs, lease: "generation-1",
        )
        assert store.snapshot_references("snapshot-1") == (
            ("published_manifest", '["study","generation-1"]'),
            ("saved_plan", recipe.recipe_hash),
        )
        assert recipe.release_snapshot(store) == "snapshot-1"
        assert run.state == "succeeded"
    finally:
        con.disconnect()


def test_saved_recipe_preserves_strict_unknown_scan_refusal():
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
        recipe = SavedPlanRecipe.create(
            plan_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            design=design,
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as error:
            SavedPlanRecipe.from_json(recipe.to_json())
        assert error.value.code == "terrine.budget.scan_unknown"
    finally:
        con.disconnect()


def test_saved_plan_rejects_owner_model_declaration_mismatch():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"))
        reference = ModelReference.from_model(model, owner_key="owner-model")
        plan = plan_refresh(
            namespace="runtime",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="snapshot-1",
            graph=dependency_graph(
                design=design, sources=("events",), definition_hash=design.spec_hash
            ),
        )
        recipe = SavedPlanRecipe.create(
            plan_json=plan.serialize(),
            design=design,
            model=reference,
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        changed = ModelReference(
            **{
                **reference.to_dict(),
                "catalog_hash": "changed",
            }
        )

        class ChangedResolver:
            def resolve(self, reference):
                return type("Resolved", (), {"catalog_hash": changed.catalog_hash})()

        with pytest.raises(TerrineError) as error:
            resolve_plan(recipe, resolver=ChangedResolver(), namespace="runtime")
        assert error.value.code == "terrine.runtime.plan_recipe_mismatch"
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
        recipe = SavedPlanRecipe.create(
            plan_json=plan.serialize(),
            design=saved_design,
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        with pytest.raises(TerrineError) as error:
            resolve_plan(recipe, resolver=_Resolver(model), namespace="runtime")
        assert error.value.code == "terrine.runtime.plan_recipe_mismatch"
    finally:
        con.disconnect()


def test_saved_recipe_rejects_null_checksum_with_coded_refusal():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        recipe = SavedPlanRecipe.create(
            plan_json=b"{}",
            design=Design("user", FixedDate("2025-01-15")),
            model=ModelReference.from_model(model, owner_key="owner-model"),
            destination=DestinationRef("output.duckdb", "analytics", "study"),
            namespace="runtime",
            expected_base_manifest=None,
        )
        document = json.loads(recipe.to_json())
        document["recipe_hash"] = None
        with pytest.raises(TerrineError) as error:
            SavedPlanRecipe.from_json(json.dumps(document))
        assert error.value.code == "terrine.runtime.plan_recipe_mismatch"
    finally:
        con.disconnect()
