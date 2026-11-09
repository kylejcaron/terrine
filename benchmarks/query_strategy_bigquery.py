"""Throwaway metadata-only BigQuery comparison; no warehouse execution or billing."""

import json
import sys
import time
from pathlib import Path
from typing import Any

import ibis
import ibis.expr.operations as ops
from ibis.backends.bigquery import Backend

from benchmarks.query_strategy_spike import canonical, direct_query, make_design, make_model
from benchmarks.workloads import load_tables
from terrine import Model
from terrine.backends.registry import ENGINES
from terrine.simulate.scale import ScaleConfig


def run(out):
    out.mkdir(parents=True, exist_ok=False)
    config = ScaleConfig(units=2, events_per_unit=2, batch_size=4, anchor_periods=21)
    local = ibis.duckdb.connect(threads=1)
    try:
        load_tables(local, config)
        events = local.table("events")
        local.create_table("events_native", events.mutate(value=events.value.cast("float64") / 10))
        schemas = {
            name: local.table(name).schema() for name in ("events", "events_native", "users")
        }
        schemas["spike_times_1"] = ibis.schema({"t0": "timestamp('UTC')"})
        rows = []
        for mode in ("integer", "native"):
            source = make_model(local, config, mode, "duckdb")
            backend = Backend()
            namespace = ops.Namespace(catalog="project", database="dataset")

            def bound(name, backend=backend, namespace=namespace):
                table: Any = ops.DatabaseTable
                return table(name, schemas[name], backend, namespace).to_expr()

            def resolver(relation: Any, bound=bound):
                return bound(relation.name)

            model = Model._assemble(
                catalog=source.catalog,
                con=backend,
                readiness=source.readiness,
                units=source.units,
                event_ids=source.event_ids,
                sources=source.sources,
                metrics=source.metrics,
                resolver=resolver,
                input_binding=None,
                engine=ENGINES["bigquery"],
            )
            frame = make_design(1, range(1, 11)).build(model)
            users = bound("users")
            anchors = users.select(
                unit=users.unit_id,
                _observed_from=users.observed_from,
                _observed_until=users.observed_until,
            ).cross_join(bound("spike_times_1"))
            direct, _ = direct_query(
                anchors,
                bound("events_native" if mode == "native" else "events"),
                model.readiness.for_fact("events"),
                tuple(range(1, 11)),
                native=mode == "native",
            )
            for variant, expression in (
                ("current", canonical(frame, range(1, 11))),
                ("direct", direct),
            ):
                start = time.perf_counter()
                compact = str(ibis.to_sql(expression, dialect="bigquery", pretty=False))  # noqa: TID251 - measurement
                elapsed = time.perf_counter() - start
                pretty = str(ibis.to_sql(expression, dialect="bigquery", pretty=True))  # noqa: TID251 - measurement
                (out / f"{mode}-{variant}.sql").write_text(compact)
                rows.append(
                    {
                        "mode": mode,
                        "variant": variant,
                        "compact_sql_chars": len(compact),
                        "formatted_sql_chars": len(pretty),
                        "compile_seconds": elapsed,
                        "executed_on_bigquery": False,
                    }
                )
                print(json.dumps(rows[-1]), flush=True)
        (out / "results.json").write_text(json.dumps(rows, indent=2))
    finally:
        local.disconnect()


if __name__ == "__main__":
    run(Path(sys.argv[1]))
