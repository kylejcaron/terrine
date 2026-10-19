"""Exercise policy and replay boundaries against real local publications."""

from dataclasses import replace
from pathlib import Path

import ibis
import pytest
import yaml
from pydantic import ValidationError

from terrine import Budget
from terrine.agent import AgentPolicy, DatasetTools
from terrine.agent.schemas import AgentDatasetReference
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.runtime.api import DesignExecutor
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.execute import _EXECUTION_FAILED
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import RefreshPlan
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.status import cancel_run
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor, FixedDate, agg
from terrine.spec.dataset import DatasetRecipe
from terrine.spec.trajectory import Trajectory

_ROOT = Path(__file__).parents[2]
_EVAL = yaml.safe_load((_ROOT / "evals/agent_dataset_tools.yaml").read_text())


@pytest.fixture
def owner_tools(tmp_path):
    database = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(database))
    con.raw_sql("CREATE TABLE events(ts TIMESTAMP,user_id INTEGER,value INTEGER)")
    con.raw_sql("CREATE TABLE users(id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1),(2)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01 12:00:00',1,3),('2025-01-02',2,4)")
    model = Model.from_catalog(
        _ROOT / "tests/agent/fixtures/catalog.yaml",
        con=con,
        readiness={"events": Coverage("2024-12-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    owner = RuntimeOwner.from_connection(con)
    reference = ModelReference.from_model(model, owner_key=owner.reference)

    class Resolver:
        def resolve(self, requested):
            assert requested == reference
            return model

    policy = AgentPolicy(
        prepare=True,
        write=True,
        read=True,
        cancel=True,
        allowed_sources=frozenset({"events"}),
        allowed_catalogs=frozenset({reference.catalog_hash}),
        allowed_destinations=frozenset({DestinationRef(str(database), "agent", "study")}),
        model_reference=reference,
        resolver=Resolver(),
        snapshot_authority=SnapshotAuthority(owner),
    )
    try:
        yield DatasetTools(model, policy, namespace="agent")
    finally:
        con.disconnect()


def _recipe(window="-1d"):
    return DatasetRecipe(
        schema_version=1,
        base_design=Design(
            "user",
            FixedDate("2025-01-02"),
            features={"history": agg("total", window=(window, "0s"))},
        ),
        view=None,
    )


def _prepare(tools, *, invocation="stage", recipe=None):
    result = tools.prepare_dataset(
        recipe or _recipe(),
        destination=next(iter(tools.policy.allowed_destinations)),
        invocation_id=invocation,
    )
    assert result.status == "ok", result.code
    assert result.output is not None
    return result.output


@pytest.mark.parametrize("case", _EVAL["runtime_cases"], ids=lambda case: case["name"])
def test_restricted_runtime_evaluations(case, owner_tools):
    tools = owner_tools
    operation = case["operation"]
    if operation == "strict_preview":
        result = tools.preview_dataset(_recipe(), budget=Budget(max_scan_bytes=1, strict_scan=True))
    elif operation == "unbounded_preview":
        result = tools.preview_dataset(_recipe(), bounded_count=tools.policy.max_bounded_count + 1)
    elif operation == "unsupported_shape":
        recipe = DatasetRecipe(
            schema_version=1,
            view=None,
            base_design=Design(
                "user",
                EventAnchor("value"),
                shape=CohortSpec("value", Trajectory("total", "1d", 2)),
            ),
        )
        result = tools.prepare_dataset(
            recipe, destination=next(iter(tools.policy.allowed_destinations)), invocation_id="shape"
        )
    elif operation == "destination_escalation":
        destination = replace(next(iter(tools.policy.allowed_destinations)), dataset="private")
        result = tools.prepare_dataset(_recipe(), destination=destination, invocation_id="private")
    elif operation == "stale_receipt":
        receipt = _prepare(tools).model_copy(update={"recipe_hash": "0" * 64})
        result = tools.execute_plan(
            receipt,
            request_key="stale",
        )
    elif operation == "write_escalation":
        receipt = _prepare(tools)
        tools.policy = replace(tools.policy, write=False)
        result = tools.stage(receipt, request_key="denied")
    else:
        assert operation == "export_escalation"
        reference = AgentDatasetReference(
            dataset="study",
            generation="private",
            definition_hash="0" * 64,
            recipe_reference="0" * 64,
            model_hash="0" * 64,
            manifest_hash="0" * 64,
            row_count=0,
            columns=(),
        )
        result = tools.export_parquet(reference, destination="/private/credentials")
    assert result.status == "refused" and result.code == case["code"]
    assert result.suggested_route and "credentials" not in result.model_dump_json()
    tables = tools.model.con.con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='agent' "
        "AND table_name IN ('runtime_runs','runtime_partitions','runtime_run_publications')"
    ).fetchall()
    assert tables == []


@pytest.mark.parametrize("case", _EVAL["catalog_cases"], ids=lambda case: case["name"])
def test_catalog_description_instructions_are_not_executable_metadata(case, owner_tools):
    raw = owner_tools.model.catalog.model_dump(mode="json")
    raw["sources"]["events"]["description"] = case["description"]
    with pytest.raises(ValidationError) as error:
        Catalog.model_validate(raw)
    assert any(item["type"] == case["error_type"] for item in error.value.errors())
    inspected = owner_tools.inspect_catalog()
    assert inspected.status == "ok" and case["description"] not in inspected.model_dump_json()
    assert (
        owner_tools.model.con.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='agent'"
        ).fetchall()
        == []
    )


def test_publication_replay_conflict_and_refresh_keep_old_generation(owner_tools):
    tools = owner_tools
    receipt = _prepare(tools)
    first = tools.stage(receipt, request_key="stable")
    assert first.status == "ok" and first.output is not None
    assert tools.stage(receipt, request_key="stable").output == first.output
    changed = _prepare(tools, invocation="changed", recipe=_recipe("-2d"))
    conflict = tools.stage(changed, request_key="stable")
    assert conflict.code == _EVAL["flow_cases"]["replay_conflict"]["code"]
    assert conflict.suggested_route
    tools.policy = replace(tools.policy, preview=True, preview_columns=frozenset({"history"}))
    old_rows = tools.preview_rows(first.output, limit=2, columns=("history",)).output
    tools.model.con.raw_sql("UPDATE events SET value=value+10 WHERE user_id=1")
    prepared = tools.prepare_refresh(
        first.output,
        changes=ChangeSet((Change("update", "events"),), "operator"),
        invocation_id="refresh",
    )
    assert prepared.status == "ok" and prepared.output is not None
    second = tools.refresh(first.output, prepared.output, request_key="refresh")
    assert second.status == "ok" and second.output is not None
    assert second.output.generation != first.output.generation
    assert (
        tools.refresh(first.output, prepared.output, request_key="refresh").output == second.output
    )
    assert tools.preview_rows(first.output, limit=2, columns=("history",)).output == old_rows
    assert tools.preview_rows(second.output, limit=2, columns=("history",)).output != old_rows
    run_id = tools.model.con.con.execute(
        "SELECT run_id FROM agent.runtime_runs WHERE request_key='refresh'"
    ).fetchone()[0]
    assert tools.request_cancel(run_id).output.disposition == "already_terminal"


def test_response_loss_recovers_manifest_without_reopening_sources(owner_tools, monkeypatch):
    import terrine.agent.tools as module

    tools = owner_tools
    receipt = _prepare(tools)
    finish = module.dataset_from_run

    def lost_response(*args, **kwargs):
        raise RuntimeError("response lost before dataset receipt")

    monkeypatch.setattr(module, "dataset_from_run", lost_response)
    with pytest.raises(RuntimeError, match="response lost"):
        tools.stage(receipt, request_key="lost")
    monkeypatch.setattr(module, "dataset_from_run", finish)
    monkeypatch.setattr(
        tools.policy.snapshot_authority,
        "reopen",
        lambda *args, **kwargs: pytest.fail("terminal replay reopened source inputs"),
    )
    tools.model.con.raw_sql("DROP TABLE events")
    recovered = tools.stage(receipt, request_key="lost")
    assert recovered.status == "ok" and recovered.output is not None
    assert tools.stage(receipt, request_key="lost").output == recovered.output
    assert (
        tools.model.con.con.execute("SELECT state FROM agent.runtime_runs").fetchone()[0]
        == (_EVAL["flow_cases"]["response_loss"]["state"])
    )
    assert tools.model.con.con.execute("SELECT count(*) FROM agent.runtime_runs").fetchone()[0] == 1
    assert (
        tools.model.con.con.execute(
            "SELECT count(*) FROM agent.runtime_run_publications"
        ).fetchone()[0]
        == 1
    )


def _executor(tools, receipt):
    invocation, saved = tools._resolve_receipt(receipt, "write")
    recipe = saved.restore_dataset_recipe()
    assert recipe is not None
    return DesignExecutor(
        tools.model,
        recipe.base_design,
        RefreshPlan.deserialize(invocation.serialized_plan),
        tools.model.con.con,
        namespace="agent",
        dataset_recipe=recipe,
    )


def test_failed_run_retries_same_identity_with_newer_fence(owner_tools):
    tools = owner_tools
    receipt = _prepare(tools)
    delegate = _executor(tools, receipt)

    class FailOnce:
        fail = True

        def run_snapshot_task(self, context, task, dependencies):
            if self.fail:
                self.fail = False
                raise _EXECUTION_FAILED.error()
            return delegate.run_snapshot_task(context, task, dependencies)

        def publish(self, plan, outputs, lease, run_id):
            return delegate.publish(plan, outputs, lease, run_id)

    tools.policy = replace(tools.policy, executor=FailOnce())
    failed = tools.stage(receipt, request_key="retry")
    assert failed.code == _EVAL["flow_cases"]["failed_retry"]["code"] and failed.suggested_route
    original = tools.model.con.con.execute(
        "SELECT run_id,attempt_fence,state FROM agent.runtime_runs"
    ).fetchone()
    assert original[2] == "failed"
    retried = tools.stage(receipt, request_key="retry")
    assert retried.status == "ok" and retried.output is not None
    renewed = tools.model.con.con.execute(
        "SELECT run_id,attempt_fence,state FROM agent.runtime_runs"
    ).fetchone()
    assert renewed[0] == original[0] and renewed[1] > original[1]
    assert renewed[2] == _EVAL["flow_cases"]["failed_retry"]["state"]
    assert tools.stage(receipt, request_key="retry").output == retried.output


def test_cancellation_during_task_prevents_publication_and_stays_terminal(owner_tools):
    tools = owner_tools
    receipt = _prepare(tools)
    delegate = _executor(tools, receipt)
    calls = []

    class Cancelling:
        def run_snapshot_task(self, context, task, dependencies):
            output = delegate.run_snapshot_task(context, task, dependencies)
            calls.append(task.task_id)
            run_id = tools.model.con.con.execute(
                "SELECT run_id FROM agent.runtime_runs"
            ).fetchone()[0]
            cancel_run(run_id, con=tools.model.con.con, namespace="agent")
            return output

        def publish(self, plan, outputs, lease, run_id):
            pytest.fail("cancelled execution must not publish")

    tools.policy = replace(tools.policy, executor=Cancelling())
    cancelled = tools.execute_plan(receipt, request_key="cancelled")
    assert (
        cancelled.output.durable_state == _EVAL["flow_cases"]["cancellation_publication"]["state"]
    )
    assert len(calls) == 1
    replay = tools.execute_plan(receipt, request_key="cancelled")
    assert replay.output == cancelled.output and len(calls) == 1
    assert (
        tools.model.con.con.execute(
            "SELECT count(*) FROM agent.runtime_run_publications"
        ).fetchone()[0]
        == 0
    )
