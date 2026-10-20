"""Exercise policy and replay boundaries against real local publications."""

import json
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
from terrine.runtime.api import DesignExecutor, run_serialized_plan
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.execute import _EXECUTION_FAILED
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import RefreshPlan
from terrine.runtime.planning import OwnerPlanner, PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.status import cancel_run
from terrine.semantic.models import Catalog
from terrine.spec import CohortSpec, Design, EventAnchor, FixedDate, agg
from terrine.spec.dataset import DatasetRecipe
from terrine.spec.io import from_json
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


def _prepare_materialization(tools, invocation="stage"):
    recipe = DatasetRecipe(
        schema_version=1, base_design=from_json(tools.design_example().output.serialized), view=None
    )
    return tools.prepare_dataset(
        recipe, destination=next(iter(tools.policy.allowed_destinations)), invocation_id=invocation
    ).output


def test_stage_refresh_and_export_require_separate_capabilities(owner_tools, tmp_path):
    receipt = _prepare_materialization(owner_tools)
    denied = DatasetTools(
        owner_tools.model, replace(owner_tools.policy, write=False), namespace="agent"
    )
    assert denied.stage(receipt, request_key="stage").code == "terrine.agent.permission_denied"
    first = owner_tools.stage(receipt, request_key="stage")
    assert first.status == "ok" and isinstance(first.output, AgentDatasetReference)
    assert owner_tools.stage(receipt, request_key="stage").output == first.output
    encoded = first.model_dump_json()
    assert (
        "recipe_json" not in encoded and "snapshot" not in encoded and "owner.duckdb" not in encoded
    )
    assert first.output.row_count == 2
    assert first.output.columns and all(
        column.role and column.data_type for column in first.output.columns
    )
    owner_tools.model.con.raw_sql("INSERT INTO users VALUES (3)")
    prepared = owner_tools.prepare_refresh(
        first.output, changes=ChangeSet((), "conservative_rebuild"), invocation_id="refresh"
    )
    assert prepared.status == "ok"
    second = owner_tools.refresh(first.output, prepared.output, request_key="refresh")
    assert second.status == "ok" and second.output.generation != first.output.generation
    assert first.output.row_count == 2 and second.output.row_count == 3
    assert (
        owner_tools.refresh(first.output, prepared.output, request_key="refresh").output
        == second.output
    )
    assert owner_tools.prepare_refresh(first.output, invocation_id="stale").status == "refused"
    destination = tmp_path / "export"
    assert (
        owner_tools.export_parquet(second.output, destination=str(destination)).code
        == "terrine.agent.permission_denied"
    )
    owner_tools.policy = replace(
        owner_tools.policy, export=True, allowed_exports=frozenset({destination})
    )
    assert (
        owner_tools.export_parquet(second.output, destination=str(tmp_path / "private")).code
        == "terrine.agent.scope_denied"
    )
    exported = owner_tools.export_parquet(second.output, destination=str(destination))
    assert exported.status == "ok" and exported.output.row_count == 3
    assert (
        owner_tools.export_parquet(second.output, destination=str(destination)).output
        == exported.output
    )
    forged = first.output.model_copy(update={"generation": second.output.generation})
    assert owner_tools.preview_rows(forged, limit=1, columns=("unit",)).status == "refused"


@pytest.mark.parametrize("kind", ["training", "scoring"])
def test_typed_dataset_views_survive_preparation_and_staging(owner_tools, kind):
    from terrine.dataset.splits import TimeSplit
    from terrine.spec import Design, FixedDate, agg

    metric = owner_tools.inspect_catalog().output.metrics[0].name
    original = Design(
        "user",
        FixedDate("2025-01-01"),
        features={"history": agg(metric, window=("-1d", "0s"))},
        outcomes={"label": agg(metric, window=("0s", "1d"))},
    )
    if kind == "training":
        design = original
        view = {
            "kind": kind,
            "features": ["history"],
            "labels": ["label"],
            "incomplete_labels": "keep",
            "split": TimeSplit(train_until="2025-01-15").to_record(),
        }
    else:
        design = replace(original, outcomes={})
        view = {
            "kind": kind,
            "features": ["history"],
            "eligibility": {},
            "as_of": "2025-01-01T00:00:00+00:00",
            "parent_design_hash": original.spec_hash,
        }
    recipe = DatasetRecipe(schema_version=1, base_design=design, view=view)
    prepared = owner_tools.prepare_dataset(
        recipe, destination=next(iter(owner_tools.policy.allowed_destinations)), invocation_id=kind
    )
    assert prepared.status == "ok"
    staged = owner_tools.stage(prepared.output, request_key=kind)
    assert staged.status == "ok"
    assert any(
        column.name == "history" and column.role == "feature" for column in staged.output.columns
    )
    assert ("label" in {column.name for column in staged.output.columns}) == (kind == "training")
    assert staged.output.recipe_reference == recipe.digest


def test_refresh_preserves_owner_approved_destination_alias(owner_tools, tmp_path):
    destination = next(iter(owner_tools.policy.allowed_destinations))
    alias = tmp_path / "owner-alias.duckdb"
    alias.symlink_to(destination.database)
    approved = replace(destination, database=str(alias))
    owner_tools.policy = replace(owner_tools.policy, allowed_destinations=frozenset({approved}))
    receipt = _prepare_materialization(owner_tools)
    first = owner_tools.stage(receipt, request_key="alias")
    assert first.status == "ok" and first.output is not None
    refreshed = owner_tools.prepare_refresh(first.output, invocation_id="alias-refresh")
    assert refreshed.status == "ok" and refreshed.output is not None
    _, saved = owner_tools._resolve_receipt(refreshed.output, "prepare")
    assert saved.destination == approved


@pytest.mark.parametrize("preview", [False, True])
def test_partitioned_export_projects_no_partition_values(owner_tools, tmp_path, preview):
    import json

    from terrine.dataset.export import ExportManifest

    reference = owner_tools.stage(
        _prepare_materialization(owner_tools), request_key="partitioned"
    ).output
    destination = tmp_path / "partitioned"
    owner_tools.policy = replace(
        owner_tools.policy,
        export=True,
        allowed_exports=frozenset({destination}),
        preview=preview,
        preview_columns=frozenset({"unit"}),
        redacted_columns=frozenset({"unit"}),
    )
    result = owner_tools.export_parquet(
        reference, destination=str(destination), partition_by=("unit",)
    )
    assert result.status == "ok"
    payload = json.loads(result.model_dump_json())["output"]
    assert "shards" not in payload and "path" not in payload
    assert set(payload) == {
        "schema_version",
        "dataset",
        "generation",
        "row_count",
        "byte_count",
        "shard_count",
    }
    manifest = ExportManifest.from_json((destination / "export-manifest.json").read_bytes())
    assert any("unit=" in shard.path for shard in manifest.shards)
    assert payload["row_count"] == manifest.row_count
    assert payload["byte_count"] == manifest.byte_count
    assert payload["shard_count"] == len(manifest.shards)


@pytest.mark.parametrize("advance", [False, True])
def test_refresh_preparation_replays_before_live_inputs_or_current_base_checks(
    owner_tools, advance
):
    reference = owner_tools.stage(_prepare_materialization(owner_tools), request_key="base").output
    changes = ChangeSet((), "conservative_rebuild")
    prepared = owner_tools.prepare_refresh(reference, invocation_id="lost-refresh", changes=changes)
    assert prepared.status == "ok" and prepared.output is not None
    if advance:
        owner_tools.model.con.raw_sql("INSERT INTO users VALUES (3)")
        published = owner_tools.refresh(reference, prepared.output, request_key="refresh")
        assert published.status == "ok" and published.output.generation != reference.generation
    owner_tools.model.con.drop_table("events")
    owner_tools.model.con.drop_table("users")
    replay = owner_tools.prepare_refresh(reference, invocation_id="lost-refresh", changes=changes)
    assert replay.status == "ok" and replay.output == prepared.output
    changed = owner_tools.prepare_refresh(reference, invocation_id="lost-refresh", changes=None)
    assert changed.code == "terrine.runtime.plan_invocation_conflict"
    if advance:
        stale = owner_tools.prepare_refresh(reference, invocation_id="new-refresh", changes=changes)
        assert stale.code == "terrine.runtime.plan_request_invalid"


def recipe_for(tools):
    return DatasetRecipe(
        schema_version=1, base_design=from_json(tools.design_example().output.serialized), view=None
    )


def _unexpected_execution(*args, **kwargs):
    raise AssertionError("unexpected source execution")


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


def test_runtime_and_concurrency_limits_are_distinct(owner_tools, monkeypatch):
    from datetime import UTC, datetime, timedelta

    import terrine.agent.tools as module
    from terrine.runtime.leases import LeaseStore

    receipt = _prepare_materialization(owner_tools)
    first = owner_tools.execute_plan(receipt, request_key="complete")
    assert first.output.durable_state == "succeeded"
    raw = owner_tools.model.con.con
    receipt = _prepare_materialization(owner_tools, invocation="limits")
    leases = LeaseStore(raw, namespace="agent")
    active = leases.acquire(
        "dataset:study", first.output.run_id, ttl=timedelta(minutes=5), now=datetime.now(UTC)
    )
    raw.execute(
        "UPDATE agent.runtime_runs SET state='running',attempt_fence=? WHERE run_id=?",
        [active.fence, first.output.run_id],
    )
    blocked = owner_tools.execute_plan(receipt, request_key="concurrency")
    assert blocked.code == "terrine.runtime.concurrency_limit"
    leases.release(active)
    raw.execute(
        "UPDATE agent.runtime_runs SET state='succeeded' WHERE run_id=?", [first.output.run_id]
    )
    ticks = iter((0.0, 301.0))
    monkeypatch.setattr(module, "monotonic", lambda: next(ticks, 301.0))
    cancelled = owner_tools.execute_plan(receipt, request_key="deadline")
    assert cancelled.output.durable_state == "cancelled"
    assert cancelled.output.pinned_result_ref is None


def test_preview_reads_only_bounded_batches_without_rehashing_output(owner_tools, monkeypatch):
    import terrine.dataset.reference as module

    reference = owner_tools.stage(
        _prepare_materialization(owner_tools), request_key="bounded"
    ).output
    name = next(column.name for column in reference.columns if column.role == "unit")
    owner_tools.policy = replace(
        owner_tools.policy, preview=True, preview_columns=frozenset({name}), batch_cap=1
    )

    def unexpected(*args, **kwargs):
        raise AssertionError("bounded preview rehashed full output")

    monkeypatch.setattr(module, "_digest_relation", unexpected)
    result = owner_tools.preview_rows(reference, limit=1, columns=(name,))
    assert result.status == "ok"
    assert len(result.output.rows) == 1


def test_status_events_and_cancel_require_run_destination_ownership(owner_tools, tmp_path):
    tools = owner_tools
    import ibis

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.recipe import ModelReference

    con = ibis.duckdb.connect(database=str(tmp_path / "runtime.duckdb"))
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.raw_sql("CREATE TABLE users (id INTEGER)")
        model = Model.from_catalog(
            tools.model.catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "id")},
        )
        reference = ModelReference.from_model(model, owner_key="owner")
        authority = SnapshotAuthority(
            RuntimeOwner(tmp_path / "runtime.duckdb", tmp_path / "snapshots")
        )

        class Resolver:
            def resolve(self, reference):
                return model

        resolver = Resolver()
        planner = OwnerPlanner(resolver=resolver, snapshot_authority=authority)
        design = from_json(tools.design_example().output.serialized)
        destinations = [
            DestinationRef(str(tmp_path / "runtime.duckdb"), "agent", name)
            for name in ("allowed", "private")
        ]
        planner.plan(
            PlanRequest("other-spelling", reference, design, destinations[0]),
            con=con.con,
            namespace="AGENT",
        )
        runs = []
        for index, destination in enumerate(destinations):
            invocation = planner.plan(
                PlanRequest(f"feedback-{index}", reference, design, destination),
                con=con.con,
                namespace="agent",
            )
            runs.append(
                run_serialized_plan(
                    invocation.recipe_json,
                    con=con.con,
                    namespace="agent",
                    request_key=invocation.request_key,
                    resolver=resolver,
                    snapshot_authority=authority,
                )
            )
        policy = replace(
            tools.policy,
            read=True,
            cancel=True,
            model_reference=reference,
            allowed_catalogs=frozenset({reference.catalog_hash}),
            allowed_destinations=frozenset({destinations[0]}),
        )
        feedback = DatasetTools(model, policy, namespace="agent")
        status = feedback.get_run_status(runs[0].run_id)
        assert status.status == "ok" and status.output is not None
        assert status.output.durable_state == "succeeded"
        assert status.output.pinned_result_ref == runs[0].published_generation
        page = feedback.get_run_events(runs[0].run_id, limit=1)
        assert page.output is not None and len(page.output.events) <= 1
        next_page = feedback.get_run_events(runs[0].run_id, after=page.output.next_cursor, limit=1)
        assert next_page.output is not None
        assert next_page.output.events[0].sequence > page.output.events[0].sequence
        cancelled = feedback.request_cancel(runs[0].run_id)
        assert cancelled.output is not None
        assert cancelled.output.disposition == "already_terminal"
        for result in (
            feedback.get_run_status(runs[1].run_id),
            feedback.get_run_events(runs[1].run_id),
            feedback.request_cancel(runs[1].run_id),
        ):
            assert result.code == "terrine.agent.scope_denied"
        assert (
            feedback.get_run_events(runs[0].run_id, limit=0).code == "terrine.agent.invalid_request"
        )
        safe = json.loads(status.model_dump_json())
        assert "recipe_json" not in str(safe) and "database" not in str(safe)
    finally:
        con.disconnect()


def test_dataset_preview_resolves_persisted_destination_before_reading(owner_tools):
    from terrine.spec.dataset import DatasetRecipe

    allowed = owner_tools.policy.allowed_destinations
    destination = next(iter(allowed))
    private = DestinationRef(destination.database, destination.schema, "private")
    owner_tools.policy = replace(owner_tools.policy, allowed_destinations=allowed | {private})
    recipe = DatasetRecipe(
        schema_version=1,
        base_design=from_json(owner_tools.design_example().output.serialized),
        view=None,
    )
    receipt = owner_tools.prepare_dataset(
        recipe, destination=private, invocation_id="private"
    ).output
    reference = owner_tools.stage(receipt, request_key="private").output
    assert reference is not None
    unit = next(column.name for column in reference.columns if column.role == "unit")
    owner_tools.policy = replace(
        owner_tools.policy,
        allowed_destinations=allowed,
        preview=True,
        preview_columns=frozenset({unit}),
    )
    assert (
        owner_tools.preview_rows(reference, limit=1, columns=(unit,)).code
        == "terrine.agent.scope_denied"
    )
