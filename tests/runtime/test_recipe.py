from __future__ import annotations

import ibis
import pytest

from terrine.data import Coverage
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.recipe import (
    DestinationRef,
    ModelReference,
    SavedPlanRecipe,
    resolve_plan,
)
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
        recipe = SavedPlanRecipe.create(
            plan_json=b'{"schema_version":1,"tasks":[]}',
            design=design,
            model=reference,
            destination=destination,
            namespace="runtime",
            expected_base_manifest="manifest-1",
        )
        restored = SavedPlanRecipe.from_json(recipe.to_json())
        resolved_model, resolved_design, resolved_destination = resolve_plan(
            restored, resolver=_Resolver(model), namespace="runtime"
        )
        assert resolved_model is model
        assert resolved_design.spec_hash == design.spec_hash
        assert resolved_destination == destination
    finally:
        con.disconnect()


def test_saved_plan_rejects_owner_model_declaration_mismatch():
    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        model = _model(con)
        design = Design("user", FixedDate("2025-01-15"))
        reference = ModelReference.from_model(model, owner_key="owner-model")
        recipe = SavedPlanRecipe.create(
            plan_json=b"{}",
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
