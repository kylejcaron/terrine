import json
from dataclasses import replace

from terrine.agent import DatasetTools
from terrine.data import Budget
from terrine.runtime.saved_plan import DestinationRef, SavedPlan
from terrine.spec.dataset import DatasetDefinition
from terrine.spec.io import from_json


def definition_for(tools):
    return DatasetDefinition(
        schema_version=2, base_design=from_json(tools.design_example().output.serialized), view=None
    )


def _unexpected_execution(*args, **kwargs):
    raise AssertionError("unexpected source execution")


def test_preview_is_row_free_and_unknown_scan_cost_stays_unknown(owner_tools, monkeypatch):
    import terrine.compile.cost as cost

    definition = definition_for(owner_tools)
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
    result = owner_tools.preview_dataset(definition)
    assert result.status == "ok"
    assert calls == [0]
    assert result.output.planned_rows is None
    assert result.output.scan_bytes is None
    strict = owner_tools.preview_dataset(
        definition, budget=Budget(max_scan_bytes=1, strict_scan=True)
    )
    assert strict.code == "terrine.budget.scan_unknown"
    assert (
        owner_tools.preview_dataset(definition, bounded_count=1).code
        == "terrine.agent.permission_denied"
    )
    owner_tools.policy = replace(owner_tools.policy, preview=True)
    counted = owner_tools.preview_dataset(definition, bounded_count=1)
    assert counted.output.counted_rows == 1
    assert calls == [0, 1]
    assert (
        owner_tools.preview_dataset(
            definition, bounded_count=owner_tools.policy.max_bounded_count + 1
        ).status
        == "refused"
    )


def test_prepare_uses_owner_planner_and_refuses_without_reopenable_snapshot(owner_tools):
    definition = definition_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    denied = DatasetTools(
        owner_tools.model, replace(owner_tools.policy, snapshot_authority=None), namespace="agent"
    )
    assert (
        denied.prepare_dataset(definition, destination=destination, invocation_id="missing").code
        == "terrine.agent.owner_configuration"
    )
    prepared = owner_tools.prepare_dataset(
        definition, destination=destination, invocation_id="first"
    )
    assert prepared.status == "ok"
    assert (
        owner_tools.prepare_dataset(
            definition, destination=destination, invocation_id="first"
        ).output
        == prepared.output
    )
    encoded = prepared.model_dump_json()
    assert (
        "snapshot" not in encoded and "database" not in encoded and "saved_plan_json" not in encoded
    )
    raw = owner_tools.model.con.con
    invocation = json.loads(
        raw.execute("SELECT invocation_json FROM agent.runtime_plan_invocations").fetchone()[0]
    )
    saved = SavedPlan.from_json(invocation["saved_plan_json"])
    restored_definition = saved.restore_dataset_definition()
    assert restored_definition is not None and restored_definition.digest == definition.digest
    assert owner_tools.policy.snapshot_authority.find_retained(
        invocation["snapshot_id"], namespace="agent"
    )
    changed = replace(definition, base_design=replace(definition.base_design, gap=1))
    assert (
        owner_tools.prepare_dataset(changed, destination=destination, invocation_id="first").code
        == "terrine.runtime.plan_invocation_conflict"
    )


def test_plan_and_run_recheck_destination_source_and_budget_grants(owner_tools):
    definition = definition_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    assert (
        owner_tools.prepare_dataset(
            definition,
            destination=DestinationRef(destination.database, "agent", "private"),
            invocation_id="bad",
        ).code
        == "terrine.agent.scope_denied"
    )
    prepared = owner_tools.prepare_dataset(
        definition, destination=destination, invocation_id="revoke"
    )
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


def test_prepare_refuses_invalid_group_identity_without_leaking_exception(owner_tools):
    from terrine.dataset.splits import GroupSplit
    from terrine.spec import agg

    base = definition_for(owner_tools)
    metric = owner_tools.inspect_catalog().output.metrics[0].name
    design = replace(
        base.base_design,
        features={"history": agg(metric, window=("-1d", "0s"))},
        outcomes={"label": agg(metric, window=("0s", "1d"))},
    )
    definition = DatasetDefinition(
        schema_version=2,
        base_design=design,
        view={
            "kind": "training",
            "features": ["history"],
            "labels": ["label"],
            "incomplete_labels": "keep",
            "split": GroupSplit(group="customer_segment").to_record(),
        },
    )
    result = owner_tools.prepare_dataset(
        definition,
        destination=next(iter(owner_tools.policy.allowed_destinations)),
        invocation_id="bad-group",
    )
    assert result.code == "terrine.dataset.invalid_definition"


def test_prepare_requires_the_connected_dataset_owner_configuration(owner_tools, tmp_path):
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority

    authority = owner_tools.policy.snapshot_authority
    mismatched = SnapshotAuthority(
        RuntimeOwner(authority.owner.state_path, tmp_path / "other-root")
    )
    owner_tools.policy = replace(owner_tools.policy, snapshot_authority=mismatched)
    result = owner_tools.prepare_dataset(
        definition_for(owner_tools),
        destination=next(iter(owner_tools.policy.allowed_destinations)),
        invocation_id="mismatched-owner",
    )
    assert result.code == "terrine.agent.owner_configuration"


def test_preparation_replays_before_source_or_roster_reopening(owner_tools):
    definition = definition_for(owner_tools)
    destination = next(iter(owner_tools.policy.allowed_destinations))
    first = owner_tools.prepare_dataset(
        definition, destination=destination, invocation_id="lost-preparation"
    )
    assert first.status == "ok" and first.output is not None
    owner_tools.model.con.drop_table("events")
    owner_tools.model.con.drop_table("users")
    replay = owner_tools.prepare_dataset(
        definition, destination=destination, invocation_id="lost-preparation"
    )
    assert replay.status == "ok" and replay.output == first.output
