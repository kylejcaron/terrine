import json
from dataclasses import replace

import pytest

from terrine.agent import DatasetTools
from terrine.agent.schemas import AgentDatasetReference, PlanReceipt
from terrine.data import Budget
from terrine.runtime.saved_plan import DestinationRef, SavedPlan
from terrine.runtime.schema import ensure_runtime_schema
from terrine.spec.dataset import DatasetDefinition
from terrine.spec.io import from_json


def definition_for(tools):
    return DatasetDefinition(
        base_design=from_json(tools.design_example().output.serialized), view=None
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
    changed = replace(definition, base_design=replace(definition.base_design, gap="1s"))
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
    runs = owner_tools.model.con.con.execute("SELECT count(*) FROM agent.runtime_runs")
    assert runs.fetchone() == (0,)


@pytest.mark.parametrize("reason", ["unversioned", "newer"])
def test_unsupported_runtime_namespace_refuses_agents_with_its_reason(owner_tools, reason):
    raw = owner_tools.model.con.con
    if reason == "newer":
        ensure_runtime_schema(raw, "agent")
        raw.execute("UPDATE agent.runtime_schema_version SET version=2")
    else:
        raw.execute("CREATE SCHEMA agent")
        raw.execute("CREATE TABLE agent.runtime_runs (run_id VARCHAR)")
    receipt = PlanReceipt(receipt_id="0" * 64, design_hash="0" * 64, plan_hash="0" * 64)
    reference = AgentDatasetReference(
        dataset="study",
        generation="generation",
        definition_hash="0" * 64,
        definition_reference="0" * 64,
        model_hash="0" * 64,
        manifest_hash="0" * 64,
        row_count=0,
        columns=(),
    )
    for response in (
        owner_tools.get_run_status("run"),
        owner_tools.execute_plan(receipt, request_key="key"),
        owner_tools.refresh(reference, receipt, request_key="key"),
    ):
        assert response.status == "refused"
        assert response.code == "terrine.runtime.schema_unsupported"
        assert response.context == {
            "namespace": "agent",
            "reason": reason,
            "found_version": 2 if reason == "newer" else None,
            "supported_version": 1,
        }
        assert response.suggested_route


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
    assert result.code == "terrine.dataset.invalid_split"


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


def test_historical_invocations_authorize_administration_without_blocking_current_runs(owner_tools):
    from terrine.runtime.graph import dependency_graph
    from terrine.runtime.planner import plan_refresh
    from terrine.runtime.planning import PlanInvocation
    from terrine.runtime.saved_plan import _bind_saved_plan
    from terrine.spec import Design, FixedDate, agg
    from tests.runtime.test_saved_plan import _legacy_saved_plan

    tools = owner_tools
    tools.policy = replace(tools.policy, cancel=True)
    design = Design(
        "user", FixedDate("2025-01-02"), features={"history": agg("total", window=("-1d", "0s"))}
    )
    plan = plan_refresh(
        namespace="agent",
        dataset="study",
        definition_hash=design.spec_hash,
        snapshot_id="snapshot",
        graph=dependency_graph(sources=("events",)),
    )
    current = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=tools.policy.model_reference,
        destination=next(iter(tools.policy.allowed_destinations)),
        namespace="agent",
        expected_base_manifest=None,
    )
    old = _legacy_saved_plan(current)
    raw = tools.model.con.con
    ensure_runtime_schema(raw, "agent")
    for name, saved in (("historical", old), ("current", current)):
        invocation = PlanInvocation(
            name, saved.plan_json, name, saved.destination, "snapshot", None, saved.to_json()
        )
        raw.execute(
            "INSERT INTO agent.runtime_plan_invocations VALUES (?,?,?,?)",
            [name, name, "snapshot", json.dumps(invocation.to_dict())],
        )
    for run_id, saved in (("old-run", old), ("current-run", current)):
        plan = _bind_saved_plan(saved, namespace="agent")
        raw.execute(
            "INSERT INTO agent.runtime_runs "
            "(run_id,dataset,fingerprint,state,destination_json) VALUES (?,?,?,'queued',?)",
            [
                run_id,
                saved.destination.dataset,
                plan.definition.fingerprint,
                json.dumps(saved.destination.to_dict()),
            ],
        )
        assert tools.get_run_status(run_id).status == "ok"
        assert tools.get_run_events(run_id).status == "ok"
        assert tools.request_cancel(run_id).output.disposition == "requested"
    tools.policy = replace(tools.policy, allowed_sources=frozenset())
    assert tools.get_run_status("old-run").code == "terrine.agent.scope_denied"
