import json
from dataclasses import replace

from terrine.agent import DatasetTools
from terrine.data import Budget
from terrine.runtime.recipe import DestinationRef, SavedPlanRecipe
from terrine.spec.dataset import DatasetRecipe
from terrine.spec.io import from_json


def recipe_for(tools):
    return DatasetRecipe(
        schema_version=1, base_design=from_json(tools.design_example().output.serialized), view=None
    )


def _unexpected_execution(*args, **kwargs):
    raise AssertionError("unexpected source execution")


def test_preview_is_row_free_and_unknown_scan_cost_stays_unknown(owner_tools, monkeypatch):
    import terrine.compile.cost as cost

    recipe = recipe_for(owner_tools)
    calls = []
    real = cost.preview

    def preview(*args, **kwargs):
        calls.append(kwargs.get("bounded_count"))
        return real(*args, **kwargs)

    monkeypatch.setattr(cost, "preview", preview)
    monkeypatch.setattr(
        owner_tools.policy.snapshot_authority,
        "capture",
        _unexpected_execution,
    )
    result = owner_tools.preview_dataset(recipe)
    assert result.status == "ok"
    assert calls == [0]
    assert result.output.planned_rows is None
    assert result.output.scan_bytes is None
    strict = owner_tools.preview_dataset(recipe, budget=Budget(max_scan_bytes=1, strict_scan=True))
    assert strict.code == "terrine.budget.scan_unknown"
    assert (
        owner_tools.preview_dataset(recipe, bounded_count=1).code
        == "terrine.agent.permission_denied"
    )
    owner_tools.policy = replace(owner_tools.policy, preview=True)
    counted = owner_tools.preview_dataset(recipe, bounded_count=1)
    assert counted.output.counted_rows == 1
    assert calls == [0, 1]
    assert (
        owner_tools.preview_dataset(
            recipe, bounded_count=owner_tools.policy.max_bounded_count + 1
        ).status
        == "refused"
    )


def test_prepare_uses_owner_planner_and_refuses_without_reopenable_snapshot(owner_tools):
    recipe = recipe_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    denied = DatasetTools(
        owner_tools.model, replace(owner_tools.policy, snapshot_authority=None), namespace="agent"
    )
    assert (
        denied.prepare_dataset(recipe, destination=destination, invocation_id="missing").code
        == "terrine.agent.owner_configuration"
    )
    prepared = owner_tools.prepare_dataset(recipe, destination=destination, invocation_id="first")
    assert prepared.status == "ok"
    assert (
        owner_tools.prepare_dataset(recipe, destination=destination, invocation_id="first").output
        == prepared.output
    )
    encoded = prepared.model_dump_json()
    assert "snapshot" not in encoded and "database" not in encoded and "recipe_json" not in encoded
    raw = owner_tools.model.con.con
    invocation = json.loads(
        raw.execute("SELECT invocation_json FROM agent.runtime_plan_invocations").fetchone()[0]
    )
    saved = SavedPlanRecipe.from_json(invocation["recipe_json"])
    restored_recipe = saved.restore_dataset_recipe()
    assert restored_recipe is not None and restored_recipe.digest == recipe.digest
    assert owner_tools.policy.snapshot_authority.find_retained(
        invocation["snapshot_id"], namespace="agent"
    )
    changed = replace(recipe, base_design=replace(recipe.base_design, gap=1))
    assert (
        owner_tools.prepare_dataset(changed, destination=destination, invocation_id="first").code
        == "terrine.runtime.plan_invocation_conflict"
    )


def test_execute_replays_before_source_freshness_and_resolves_exact_invocation(
    owner_tools, monkeypatch
):
    recipe = recipe_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    prepared = owner_tools.prepare_dataset(recipe, destination=destination, invocation_id="execute")
    first = owner_tools.execute_plan(prepared.output, request_key="stable")
    assert first.status == "ok" and first.output.durable_state == "succeeded"
    owner_tools.model.con.raw_sql("DROP TABLE users")
    monkeypatch.setattr(
        owner_tools.policy.snapshot_authority,
        "reopen",
        _unexpected_execution,
    )
    replay = owner_tools.execute_plan(prepared.output, request_key="stable")
    assert replay.output == first.output
    owner_tools.policy = replace(owner_tools.policy, write=False)
    assert (
        owner_tools.execute_plan(prepared.output, request_key="stable").code
        == "terrine.agent.permission_denied"
    )


def test_plan_and_run_recheck_destination_source_and_budget_grants(owner_tools):
    recipe = recipe_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    assert (
        owner_tools.prepare_dataset(
            recipe,
            destination=DestinationRef(destination.database, "agent", "private"),
            invocation_id="bad",
        ).code
        == "terrine.agent.scope_denied"
    )
    prepared = owner_tools.prepare_dataset(recipe, destination=destination, invocation_id="revoke")
    owner_tools.policy = replace(owner_tools.policy, budget=Budget(max_rows=1))
    assert (
        owner_tools.execute_plan(prepared.output, request_key="no-write").code
        == "terrine.agent.scope_denied"
    )
    assert (
        owner_tools.model.con.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='agent' AND table_name='runtime_runs'"
        ).fetchone()[0]
        == 0
    )


def test_receipt_binds_dataset_recipe_change_evidence_and_base_generation(owner_tools):
    from terrine.dataset.splits import TimeSplit
    from terrine.spec import agg

    base = recipe_for(owner_tools)
    metric = owner_tools.inspect_catalog().output.metrics[0].name
    design = replace(
        base.base_design,
        features={"history": agg(metric, window=("-1d", "0s"))},
        outcomes={"label": agg(metric, window=("0s", "1d"))},
    )
    recipe = DatasetRecipe(
        schema_version=1,
        base_design=design,
        view={
            "kind": "training",
            "features": ["history"],
            "labels": ["label"],
            "incomplete_labels": "keep",
            "split": TimeSplit(train_until="2025-01-15").to_record(),
        },
    )
    destination = next(iter(owner_tools.policy.allowed_destinations))
    prepared = owner_tools.prepare_dataset(
        recipe, destination=destination, invocation_id="training"
    )
    assert prepared.status == "ok"
    changed = replace(
        recipe, view={**dict(recipe.view), "split": TimeSplit(train_until="2025-01-16").to_record()}
    )
    assert (
        owner_tools.prepare_dataset(changed, destination=destination, invocation_id="training").code
        == "terrine.runtime.plan_invocation_conflict"
    )
    for revoked in (
        replace(owner_tools.policy, allowed_sources=frozenset()),
        replace(owner_tools.policy, allowed_destinations=frozenset()),
    ):
        restricted = DatasetTools(owner_tools.model, revoked, namespace="agent")
        assert (
            restricted.execute_plan(prepared.output, request_key="not-authorized").code
            == "terrine.agent.scope_denied"
        )
    forged = prepared.output.model_copy(update={"recipe_hash": "0" * 64})
    assert (
        owner_tools.execute_plan(forged, request_key="forged").code
        == "terrine.agent.receipt_unavailable"
    )
