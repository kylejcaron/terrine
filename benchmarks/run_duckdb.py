"""Opt-in real compiler/runtime DuckDB workload with independent exact references."""

from __future__ import annotations

import argparse
import json
import platform
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import Any

import duckdb
import ibis
import pyarrow as pa

from benchmarks.workloads import _json, check_rows, content_digest, designs, load_model, run_case
from terrine import Budget
from terrine.errors import TerrineError
from terrine.runtime import open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.changes import Change, ChangeSet
from terrine.runtime.graph import DependencyGraph
from terrine.runtime.materialize import compose_runtime_frame
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import LayerNode, RuntimeStore
from terrine.simulate.scale import ScaleConfig, correction_batches


def _correct_source(con, config: ScaleConfig) -> dict[str, int]:
    counts = {"updates": 0, "deletes": 0}
    for batch in correction_batches(config):
        for row in batch.to_pylist():
            counts["updates" if row["change_kind"] == "update" else "deletes"] += 1
        con.create_table("scale_changes", batch, temp=True, overwrite=True)
        con.raw_sql(
            "DELETE FROM events USING scale_changes c "
            "WHERE events.event_id=c.event_id AND c.change_kind='delete'"
        )
        con.raw_sql(
            "UPDATE events SET value=c.value,revision=c.revision FROM scale_changes c "
            "WHERE events.event_id=c.event_id AND c.change_kind='update'"
        )
        con.drop_table("scale_changes")
    return counts


def _source_digest(con, config: ScaleConfig) -> str:
    return content_digest(
        con.to_pyarrow_batches(
            con.table("events").order_by("unit_id", "event_id"), chunk_size=config.batch_size
        )
    )


def _runtime(con, model, design, config: ScaleConfig, directory: Path) -> dict[str, Any]:
    owner = RuntimeOwner(
        state_path=directory / "owner.duckdb", snapshot_root=directory / "snapshots"
    )
    authority = SnapshotAuthority(owner)
    namespace = "scale_owner"
    backend = DuckDBBackend(con.con, namespace=namespace)
    store = RuntimeStore(con.con, namespace=namespace)
    budget = Budget(max_rows=config.units)
    # One complete compiler task: no artificial daily/prefix work for this full-rebuild owner.
    graph = DependencyGraph((LayerNode("frame", "events", "all", design.spec_hash, "pending"),), ())
    strict_codes = {}
    try:
        authority.capture(
            model,
            design=design,
            con=con,
            namespace=namespace,
            run_id="strict-capture",
            budget=Budget(max_scan_bytes=1),
        )
    except TerrineError as error:
        strict_codes["capture"] = error.code
    else:
        raise AssertionError("native DuckDB capture silently admitted a strict unknown scan")
    try:
        plan_refresh(
            namespace=namespace,
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id="not-captured",
            graph=graph,
            cost_provider=backend,
            budget=Budget(max_scan_bytes=1),
        )
    except TerrineError as error:
        strict_codes["plan"] = error.code
    else:
        raise AssertionError("native DuckDB plan silently admitted a strict unknown scan")

    class Resolver:
        def resolve(self, reference):
            return model

    class CompilerExecutor:
        def run_snapshot_task(self, context, task, dependencies):
            execution_model = model._for_execution(
                context, source_names={"events"}, unit_name="user"
            )
            compiled = compose_runtime_frame(design, execution_model, persisted={}, budget=budget)
            return context.execute(compiled.table)

        def publish(self, plan, outputs, lease, run_id):
            generation = "scale-1" if plan.expected_base_manifest is None else "scale-2"
            return backend.publish_generation(
                StagedGeneration(
                    plan.dataset, generation, outputs, run_id=run_id, snapshot_id=plan.snapshot_id
                ),
                expected_manifest=plan.expected_base_manifest,
                lease=lease,
            )

    def generation(corrected: bool, expected_base):
        snapshot = authority.capture(
            model,
            design=design,
            con=con,
            namespace=namespace,
            run_id="corrected" if corrected else "baseline",
            budget=budget,
        )
        changes = (
            ChangeSet((Change("update", "events", revision="2"),), "conservative_rebuild")
            if corrected
            else None
        )
        plan = plan_refresh(
            namespace=namespace,
            dataset="study",
            definition_hash=design.spec_hash,
            snapshot_id=snapshot.snapshot_id,
            graph=graph,
            cost_provider=backend,
            budget=budget,
            changes=changes,
        )
        recipe = SavedPlanRecipe.create(
            plan_json=plan.serialize(),
            design=design,
            model=ModelReference.from_model(model, owner_key="scale-model"),
            destination=DestinationRef(str(owner.state_path), "published", "study"),
            namespace=namespace,
            expected_base_manifest=expected_base,
        )
        recipe.pin_snapshot(store)
        receipt = run_serialized_plan(
            recipe.to_json(),
            con=con.con,
            namespace=namespace,
            executor=CompilerExecutor(),
            resolver=Resolver(),
            snapshot_authority=authority,
        )
        table = open_published_generation(receipt, con=con.con, namespace=namespace).to_pyarrow()
        checked = check_rows(table, config, "frame", corrected=corrected)
        from terrine.runtime import get_run

        observed = get_run(receipt.run_id, con=con.con, namespace=namespace).cost
        return receipt, {
            **checked,
            "snapshot_id": snapshot.snapshot_id,
            "tasks": len(plan.tasks),
            "generation": receipt.published_generation,
            "estimated_scan_bytes": plan.scan_bytes,
            "budget_warnings": plan.budget_warnings,
            "observed_cost": observed,
        }

    first, baseline = generation(False, None)
    corrections = _correct_source(con, config)
    corrected_events = int(
        con.to_pyarrow(con.table("events").count().as_table()).column(0)[0].as_py()
    )
    if corrected_events != config.units * config.events_per_unit - corrections["deletes"]:
        raise AssertionError("declared correction cardinality differs from actual source")
    _, corrected = generation(True, first.published_generation)
    old = open_published_generation(first, con=con.con, namespace=namespace).to_pyarrow()
    old_check = check_rows(old, config, "frame")
    if old_check["output_digest"] != baseline["output_digest"]:
        raise AssertionError("old generation changed after a source correction")
    return {
        "mode": "owner_single_task_full_rebuild",
        "baseline": baseline,
        "corrected": corrected,
        "corrections": corrections,
        "corrected_input_events": corrected_events,
        "strict_unknown_refusals": strict_codes,
        "old_generation_unchanged": True,
    }


def _process_peak() -> tuple[int | None, str]:
    try:
        import resource
    except ImportError:
        return None, "resource_module_unavailable"
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Darwin":
        return int(raw), "process_lifetime_peak_rss_bytes"
    if platform.system() == "Linux":
        return int(raw) * 1024, "process_lifetime_peak_rss_kib_converted_to_bytes"
    return None, "platform_rss_units_unavailable"


def run(config: ScaleConfig) -> dict[str, Any]:
    """Execute real Frames, input-derived range probes and two retained runtime generations."""
    tracemalloc.start()
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix="terrine-scale-") as temporary:
            directory = Path(temporary)
            con = ibis.duckdb.connect(str(directory / "owner.duckdb"))
            try:
                model, counts = load_model(con, config)
                digest = _source_digest(con, config)
                cases = designs(config)
                results = [run_case(model, config, name, design) for name, design in cases.items()]
                runtime = _runtime(con, model, cases["frame"], config, directory)
                corrected_digest = _source_digest(con, config)
                _, traced_peak = tracemalloc.get_traced_memory()
                process_peak, process_basis = _process_peak()
                return {
                    "schema_version": 1,
                    "status": "verified",
                    "backend": "duckdb",
                    "workload": config.to_dict(),
                    "source_digest": digest,
                    "corrected_source_digest": corrected_digest,
                    **counts,
                    "declared_schedule_anchors": config.units * config.anchor_periods,
                    "anchors": sum(case["anchors"] for case in results),
                    "emitted_rows": sum(case["emitted_rows"] for case in results),
                    "oracle_match": True,
                    "oracle_scope": "exact values/support/nulls; sketch errors are empirical",
                    "cases": results,
                    "runtime": runtime,
                    "client_peak_memory_bytes": traced_peak,
                    "client_memory_scope": "tracemalloc Python allocations",
                    "process_peak_memory_bytes": process_peak,
                    "process_memory_scope": process_basis,
                    "elapsed_seconds": time.perf_counter() - started,
                    "scan_bytes": None,
                    "scan_cost": None,
                    "scan_cost_status": "unknown_local_backend_capability",
                    "environment": {
                        "python": platform.python_version(),
                        "system": platform.system(),
                        "architecture": platform.machine(),
                        "ibis": ibis.__version__,
                        "duckdb": duckdb.__version__,
                        "pyarrow": pa.__version__,
                    },
                }
            finally:
                con.disconnect()
    finally:
        tracemalloc.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    record = run(ScaleConfig(**json.loads(args.config.read_text(encoding="utf-8"))))
    encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), default=_json)
    if args.output is not None:
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(encoded + "\n")
        from benchmarks.report import summarize

        print(json.dumps(summarize([record]), sort_keys=True, default=_json))
    else:
        print(encoded)


if __name__ == "__main__":
    main()
