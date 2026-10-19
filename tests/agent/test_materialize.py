from dataclasses import replace

import pytest

from terrine.agent import DatasetTools
from terrine.agent.schemas import AgentDatasetReference
from terrine.runtime.changes import ChangeSet
from terrine.spec.dataset import DatasetRecipe
from terrine.spec.io import from_json


def _prepare(tools, invocation="stage"):
    recipe = DatasetRecipe(
        schema_version=1, base_design=from_json(tools.design_example().output.serialized), view=None
    )
    return tools.prepare_dataset(
        recipe, destination=next(iter(tools.policy.allowed_destinations)), invocation_id=invocation
    ).output


def test_stage_refresh_and_export_require_separate_capabilities(owner_tools, tmp_path):
    receipt = _prepare(owner_tools)
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


def test_preview_is_denied_by_default_and_bounded_redacted_when_granted(owner_tools, monkeypatch):
    from terrine.dataset.reference import Dataset

    reference = owner_tools.stage(_prepare(owner_tools), request_key="preview").output
    name = next(column.name for column in reference.columns if column.role == "unit")
    assert (
        owner_tools.preview_rows(reference, limit=1, columns=(name,)).code
        == "terrine.agent.permission_denied"
    )
    owner_tools.policy = replace(
        owner_tools.policy,
        preview=True,
        preview_columns=frozenset({name}),
        redacted_columns=frozenset({name}),
        batch_cap=1,
        max_preview_rows=2,
    )
    for limit in (0, -1, 3, True):
        assert owner_tools.preview_rows(reference, limit=limit, columns=(name,)).status == "refused"
    assert (
        owner_tools.preview_rows(reference, limit=1, columns=("private",)).code
        == "terrine.agent.scope_denied"
    )
    called = []
    closed = []
    real = Dataset.to_arrow_batches

    def batches(self, *, batch_size):
        called.append(batch_size)
        iterator = real(self, batch_size=batch_size)
        try:
            yield from iterator
        finally:
            iterator.close()
            closed.append(True)

    monkeypatch.setattr(Dataset, "to_arrow_batches", batches)
    result = owner_tools.preview_rows(reference, limit=2, columns=(name,))
    assert result.status == "ok"
    assert result.output.columns == (name,)
    assert result.output.rows == ((None,), (None,))
    assert called == [1] and closed == [True]


def test_runtime_and_concurrency_limits_are_distinct(owner_tools, monkeypatch):
    from datetime import UTC, datetime, timedelta

    import terrine.agent.tools as module
    from terrine.runtime.leases import LeaseStore

    receipt = _prepare(owner_tools)
    first = owner_tools.execute_plan(receipt, request_key="complete")
    assert first.output.durable_state == "succeeded"
    raw = owner_tools.model.con.con
    receipt = _prepare(owner_tools, invocation="limits")
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


def test_preview_reads_only_bounded_batches_without_rehashing_output(owner_tools, monkeypatch):
    import terrine.dataset.reference as module

    reference = owner_tools.stage(_prepare(owner_tools), request_key="bounded").output
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


def test_refresh_preserves_owner_approved_destination_alias(owner_tools, tmp_path):
    destination = next(iter(owner_tools.policy.allowed_destinations))
    alias = tmp_path / "owner-alias.duckdb"
    alias.symlink_to(destination.database)
    approved = replace(destination, database=str(alias))
    owner_tools.policy = replace(owner_tools.policy, allowed_destinations=frozenset({approved}))
    receipt = _prepare(owner_tools)
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

    reference = owner_tools.stage(_prepare(owner_tools), request_key="partitioned").output
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
    reference = owner_tools.stage(_prepare(owner_tools), request_key="base").output
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
