"""Feature-only roster scoring with pinned local export and corrected refresh."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from examples.ml_staging import create_model, training_design
from terrine import Budget, agg, gt
from terrine.dataset.manifest import DatasetManifest
from terrine.dataset.reference import Dataset
from terrine.runtime.changes import Change, ChangeSet


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="terrine-scoring-") as directory:
        root = Path(directory)
        os.environ["TERRINE_RUNTIME_NAMESPACE"] = "scoring_example"
        os.environ["TERRINE_RUNTIME_SNAPSHOT_ROOT"] = str(root / "snapshots")
        model = create_model(root / "owner.duckdb")
        try:
            budget = Budget(max_rows=100, max_output_bytes=1_000_000, strict_scan=False)
            scoring = training_design().snapshot(
                model,
                as_of="2025-01-02",
                eligibility={"active": gt(agg("event_total", window=("-1d", "0s")), 0)},
            )
            model.con.drop_table("labels")
            dataset = scoring.stage("scoring", budget=budget)
            manifest = DatasetManifest.from_json(dataset.manifest.to_json())
            reopened = Dataset.from_manifest(manifest, con=model.con)
            rows = 0
            for batch in reopened.to_arrow_batches(batch_size=1):
                assert "following_amount" not in batch.schema.names
                assert batch.schema.metadata
                rows += batch.num_rows
            export = reopened.export_parquet(
                destination=str(root / "scoring-parquet"), budget=budget
            )
            model.con.raw_sql("DELETE FROM events WHERE user_id=1")
            changes = ChangeSet((Change("delete", "events"),), "operator")
            assert reopened.plan_refresh(model, changes=changes, budget=budget).tasks
            refreshed = reopened.refresh(model, changes=changes, budget=budget)
            assert sum(batch.num_rows for batch in reopened.to_arrow_batches()) == rows
            refreshed_rows = sum(batch.num_rows for batch in refreshed.to_arrow_batches())
            assert refreshed_rows == 1
            assert refreshed.manifest.recipe.view["as_of"] == manifest.recipe.view["as_of"]
            print(
                json.dumps(
                    {
                        "workflow": "scoring",
                        "rows": rows,
                        "export_rows": export.row_count,
                        "refreshed_rows": refreshed_rows,
                        "refresh_changed": refreshed.manifest.generation != manifest.generation,
                        "schema_metadata": bool(manifest.frame_metadata),
                        "as_of": refreshed.manifest.recipe.view["as_of"],
                    },
                    sort_keys=True,
                )
            )
        finally:
            model.con.disconnect()


if __name__ == "__main__":
    main()
