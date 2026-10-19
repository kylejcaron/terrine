from __future__ import annotations

import ibis
import pytest

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.spec import Design, EventAnchor, agg, gt


def test_scoring_today_is_resolved_once_at_catalog_day_boundary(
    native_inputs, tmp_path, monkeypatch
):
    catalog, source_con = native_inputs
    con = ibis.duckdb.connect(str(tmp_path / "scoring.duckdb"))
    for name in ("events", "labels", "users"):
        con.create_table(name, source_con.table(name).execute())
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-1d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    )
    from terrine.errors import TerrineError

    with pytest.raises(TerrineError) as error:
        design.snapshot(
            model,
            as_of="2025-01-05T00:00:00.000000001Z",
            eligibility={"active": gt(agg("sessions", window=("-1d", "0d")), 0)},
            features=("past_sessions",),
        )
    assert error.value.code == "terrine.dataset.invalid_view"
    from datetime import UTC
    from datetime import datetime as OriginalDateTime

    class FrozenDateTime(OriginalDateTime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return OriginalDateTime(2025, 1, 5, 23, 45, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr("terrine.spec.design.datetime", FrozenDateTime)
    eligibility = {"active": gt(agg("sessions", window=("-1d", "0d")), 0)}

    scoring = design.snapshot(
        model,
        as_of="today",
        eligibility=eligibility,
        features=("past_sessions",),
    )
    from terrine.frame import _feature_definition_hash

    assert _feature_definition_hash(design, ("past_sessions",), model) == _feature_definition_hash(
        scoring._design, ("past_sessions",), model
    )
    assert FrozenDateTime.calls == 1

    assert scoring.roles.outcomes == ()
    assert scoring.roles.features == ("past_sessions",)
    result = scoring.materialize()
    view = result.provenance.dataset_view
    assert view is not None
    assert view.kind == "scoring"
    assert view.selected_features == ("past_sessions",)
    assert view.selected_labels == ()
    assert view.resolved_as_of is not None
    assert view.resolved_as_of.isoformat() == "2025-01-05T00:00:00+00:00"
    assert result.roles.outcomes == ()
    assert isinstance(design.anchor, EventAnchor)
    assert "churn" in design.outcomes
    staged = scoring.stage("scoring", budget=Budget(strict_scan=False))
    assert staged.manifest.recipe.view["kind"] == "scoring"
    assert staged.manifest.recipe.view["features"] == ("past_sessions",)
    assert staged.manifest.recipe.view["as_of"] == view.resolved_as_of.isoformat()
    assert staged.manifest.recipe.view["eligibility"]["active"]
    assert staged.manifest.recipe.view["parent_design_hash"] == design.spec_hash
    con.disconnect()


def test_owner_prepares_scoring_view_before_preflight_without_label_inputs(
    native_inputs, tmp_path, monkeypatch
):
    import json

    import terrine.compile.dispatch as dispatch
    from terrine.dataset.reference import _recipe_for
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec.dataset import DatasetRecipe

    catalog, source = native_inputs
    database = tmp_path / "scoring-owner.duckdb"
    con = ibis.duckdb.connect(str(database))
    try:
        for name in ("events", "labels", "users"):
            con.create_table(name, source.table(name).execute())
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-01-31")},
            units={"user": UnitSource("users", "user_id")},
        )
        parent = Design(
            "user",
            EventAnchor("session"),
            features={"past_sessions": agg("sessions", window=("-1d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "1d"))},
        )
        recipe = _recipe_for(parent.snapshot(model, as_of="2025-01-05", eligibility={}))
        owner = RuntimeOwner.from_connection(con)
        reference = ModelReference.from_model(model, owner_key=owner.reference)
        con.drop_table("labels")
        expected = reference

        class Resolver:
            def resolve(self, reference):
                assert reference == expected
                return model

        resolved = []
        build, compile_design = DatasetRecipe.build, dispatch.compile_design

        def resolve_view(self, model):
            resolved.append(self.digest)
            return build(self, model)

        def compile_resolved(design, model):
            assert resolved, "resolve the recipe view before preflight compilation"
            return compile_design(design, model)

        monkeypatch.setattr(DatasetRecipe, "build", resolve_view)
        monkeypatch.setattr(dispatch, "compile_design", compile_resolved)
        authority = SnapshotAuthority(owner)
        invocation = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority).plan(
            PlanRequest(
                "scoring",
                reference,
                recipe.base_design,
                DestinationRef(str(database), "owner", "scoring"),
                budget=Budget(strict_scan=False),
                dataset_recipe=recipe,
            ),
            con=con.con,
            namespace="owner",
        )
        retained = authority.find_retained(invocation.snapshot_id, namespace="owner")
        assert "labels" not in json.dumps(retained.roots)
        saved = SavedPlanRecipe.from_json(invocation.recipe_json)
        assert saved.restore_dataset_recipe() == recipe
        assert recipe.view["parent_design_hash"] == parent.spec_hash
        assert recipe.view["as_of"] == "2025-01-05T00:00:00+00:00"
    finally:
        con.disconnect()
