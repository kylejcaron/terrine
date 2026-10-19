"""Owner-authorized training data with safe receipts and pinned refresh."""

import json
import tempfile
from dataclasses import replace
from pathlib import Path

from ml_staging import create_model, training_design

from terrine import Budget
from terrine.agent import AgentPolicy, DatasetTools
from terrine.dataset.splits import GroupSplit
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.spec.dataset import DatasetRecipe


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="terrine-agent-") as directory:
        root = Path(directory)
        model = create_model(root / "owner.duckdb")
        try:
            owner = RuntimeOwner.from_connection(model.con)
            reference = ModelReference.from_model(model, owner_key=owner.reference)

            class Resolver:
                def resolve(self, requested):
                    assert requested == reference
                    return model

            destination = DestinationRef(str(root / "owner.duckdb"), "agent_example", "training")
            policy = AgentPolicy(
                allowed_sources=frozenset({"events", "labels"}),
                allowed_catalogs=frozenset({reference.catalog_hash}),
                allowed_destinations=frozenset({destination}),
                model_reference=reference,
                resolver=Resolver(),
                snapshot_authority=SnapshotAuthority(owner),
                budget=Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False),
            )
            tools = DatasetTools(model, policy, namespace="agent_example")
            recipe = DatasetRecipe(
                schema_version=1,
                base_design=training_design(),
                view={
                    "kind": "training",
                    "features": ["past_amount", "segment"],
                    "labels": ["following_amount"],
                    "incomplete_labels": "keep",
                    "split": GroupSplit(seed=17).to_record(),
                },
            )
            preview = tools.preview_dataset(recipe)
            assert preview.status == "ok" and preview.output is not None
            assert (
                tools.prepare_dataset(recipe, destination=destination, invocation_id="denied").code
                == "terrine.agent.permission_denied"
            )
            policy = replace(policy, prepare=True)
            tools = DatasetTools(model, policy, namespace="agent_example")
            prepared = tools.prepare_dataset(
                recipe, destination=destination, invocation_id="training"
            )
            assert prepared.status == "ok" and prepared.output is not None
            policy = replace(policy, write=True, read=True)
            tools = DatasetTools(model, policy, namespace="agent_example")
            staged = tools.stage(prepared.output, request_key="training")
            assert staged.status == "ok" and staged.output is not None
            original = staged.output
            assert tools.stage(prepared.output, request_key="training").output == original
            assert tools.preview_rows(original, limit=1, columns=("segment",)).code == (
                "terrine.agent.permission_denied"
            )
            policy = replace(
                policy,
                preview=True,
                preview_columns=frozenset({"segment"}),
                redacted_columns=frozenset({"segment"}),
                batch_cap=1,
            )
            tools = DatasetTools(model, policy, namespace="agent_example")
            rows = tools.preview_rows(original, limit=1, columns=("segment",))
            assert rows.status == "ok" and rows.output is not None
            assert rows.output.rows == ((None,),)
            model.con.raw_sql("UPDATE events SET amount=amount+5 WHERE user_id=1")
            refreshed_plan = tools.prepare_refresh(
                original,
                changes=ChangeSet((Change("update", "events"),), "operator"),
                invocation_id="correction",
            )
            assert refreshed_plan.status == "ok" and refreshed_plan.output is not None, (
                refreshed_plan.code
            )
            refreshed = tools.refresh(original, refreshed_plan.output, request_key="correction")
            assert refreshed.status == "ok" and refreshed.output is not None
            assert refreshed.output.generation != original.generation
            assert tools.refresh(
                original, refreshed_plan.output, request_key="correction"
            ).output == (refreshed.output)
            export_path = root / "parquet"
            policy = replace(policy, export=True, allowed_exports=frozenset({export_path}))
            tools = DatasetTools(model, policy, namespace="agent_example")
            exported = tools.export_parquet(refreshed.output, destination=str(export_path))
            assert exported.status == "ok" and exported.output is not None
            print(
                json.dumps(
                    {
                        "workflow": "restricted_dataset_tools",
                        "rows": original.row_count,
                        "export_rows": exported.output.row_count,
                        "refreshed": True,
                        "redacted": rows.output.rows == ((None,),),
                        "scan_unknown": preview.output.scan_bytes is None,
                    },
                    sort_keys=True,
                )
            )
        finally:
            model.con.disconnect()


if __name__ == "__main__":
    main()
