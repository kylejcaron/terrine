# ruff: noqa: F811
"""Local DuckDB runtime/compile cost evidence; no live warehouse connection."""

import duckdb
import pyarrow as pa
import pytest

from terrine import Budget, Design, FixedDate, agg
from terrine.errors import TerrineError
from terrine.runtime import get_run, open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.graph import dependency_graph
from terrine.runtime.materialize import compose_runtime_frame
from terrine.runtime.planner import plan_refresh
from tests.compile.test_columns import column_model  # noqa: F401


def test_duckdb_runtime_reports_unknown_scan_and_observed_compiler_output(column_model):
    with duckdb.connect() as con:
        backend = DuckDBBackend(con, namespace="owner")
        design = Design(
            "user",
            FixedDate("2025-01-02T12:00:00+00:00"),
            features={"total": agg("total", window=("-1s", "0s"))},
        )
        calls = []

        class CompilerExecutor:
            def run_task(self, task, dependencies):
                calls.append(task.task_id)
                if task.layer != "frame":
                    return pa.table({"unit": pa.array([], type=pa.string())})
                compiled = compose_runtime_frame(design, column_model, persisted={}, budget=budget)
                return column_model.con.to_pyarrow(compiled.table)

            def publish(self, plan, outputs, lease, run_id):
                frames = {
                    task.task_id: outputs[task.task_id]
                    for task in plan.tasks
                    if task.layer == "frame"
                }
                return backend.publish_generation(
                    StagedGeneration(plan.dataset, "g1", frames, run_id=run_id),
                    expected_manifest=None,
                    lease=lease,
                )

        graph = dependency_graph(column_model, design, partitions=("all",))
        with pytest.raises(TerrineError) as error:
            plan_refresh(
                namespace="owner",
                dataset="study",
                definition_hash=design.spec_hash,
                snapshot_id="unchanged-local-input",
                graph=graph,
                cost_provider=backend,
                budget=Budget(max_scan_bytes=1),
            )
        assert error.value.code == "terrine.budget.scan_unknown"
        assert calls == []
        budget = Budget(max_scan_bytes=1, strict_scan=False, max_rows=2, max_output_bytes=4096)
        plan = plan_refresh(
            namespace="owner",
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="unchanged-local-input",
            graph=graph,
            cost_provider=backend,
            budget=budget,
        )
        assert plan.scan_bytes is None and plan.budget_warnings
        receipt = run_serialized_plan(
            plan.serialize(), con=con, namespace="owner", executor=CompilerExecutor()
        )
        status = get_run(receipt.run_id, con=con, namespace="owner")
        assert status.cost["output_rows"] == 2
        assert status.cost["scan_bytes"] is None
        published = open_published_generation(receipt, con=con, namespace="owner")
        assert published.to_pyarrow().select(["unit", "total"]).sort_by("unit").to_pylist() == [
            {"unit": "a", "total": 3},
            {"unit": "b", "total": None},
        ]
