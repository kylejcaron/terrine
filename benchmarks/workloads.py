"""Bounded synthetic loading and real Model/Design benchmark cases."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa

from terrine import Budget, Coverage, Model, UnitSource
from terrine.compile.dispatch import compile_design
from terrine.data import _encode
from terrine.semantic.models import Catalog
from terrine.simulate.scale import ScaleConfig, event_batches, unit_batches
from terrine.spec import (
    Design,
    EventAnchor,
    EventPanel,
    FixedDate,
    Initiation,
    Schedule,
    Sequential,
    SketchRequest,
    agg,
    approx_distinct,
    approx_quantile,
    quantile,
)
from terrine.temporal.ranges import blocked_range_join
from terrine.windows import Window
from tests.reference.scale_metrics import metric_rows

_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_EXACT = ("total", "minimum", "maximum", "distinct", "q25")


def _catalog(dialect: str) -> Catalog:
    aggregations = {
        "total": "sum",
        "minimum": "min",
        "maximum": "max",
        "distinct": "count_distinct",
    }
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": dialect,
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "event_time",
                    "entities": {"user": "unit_id"},
                    "facts": ["value", "event"],
                    "properties": ["adopted", "event_name"],
                    "dimensions": [],
                }
            },
            "facts": {
                "value": {"source": "events", "column": "value"},
                "event": {"source": "events", "column": None},
            },
            "properties": {
                name: {
                    "source": "events",
                    "column": column,
                    "dtype": "string",
                    "as_of": "event_time",
                }
                for name, column in (("adopted", "adopted"), ("event_name", "event_type"))
            },
            "dimensions": {},
            "measures": {
                name: {"fact": "value", "aggregation": aggregation}
                for name, aggregation in aggregations.items()
            },
            "metrics": {
                name: {"kind": "mean", "entity": "user", "measure": measure}
                for name, measure in (*((name, name) for name in aggregations), ("value", "total"))
            },
        }
    )


def load_model(
    con: Any, config: ScaleConfig, *, dialect: str = "duckdb"
) -> tuple[Model, dict[str, int]]:
    """Create only the workload-owned events/units relations in an empty workspace."""
    counts = {"input_events": 0, "input_arrow_bytes": 0, "max_source_batch_rows": 0}
    for name, stream in (("events", event_batches(config)), ("users", unit_batches(config))):
        for index, batch in enumerate(stream):
            if index == 0:
                con.create_table(name, batch, overwrite=False)
            else:
                con.insert(name, batch)
            if name == "events":
                counts["input_events"] += len(batch)
                counts["input_arrow_bytes"] += batch.nbytes
                counts["max_source_batch_rows"] = max(counts["max_source_batch_rows"], len(batch))
    model = Model.from_catalog(
        _catalog(dialect),
        con=con,
        units={
            "user": UnitSource(
                "users", "unit_id", observed_from="observed_from", observed_until="observed_until"
            )
        },
        readiness={
            "events": Coverage(
                config.coverage_start.date(), (config.coverage_end - timedelta(days=1)).date()
            )
        },
        event_ids={"events": "event_id"},
    )
    return model, counts


def designs(config: ScaleConfig) -> dict[str, Design]:
    window = (-config.window_days * 86400, 0)
    features = {
        "total": agg("total", window=window),
        "minimum": agg("minimum", window=window),
        "maximum": agg("maximum", window=window),
        "distinct": agg("distinct", window=window),
        "q25": quantile("value", "0.25", window=window),
    }
    fixed = Design(
        "user", FixedDate(_EPOCH + timedelta(days=config.window_days)), features=features
    )
    schedule = Design(
        "user",
        Schedule(
            "1d",
            (_EPOCH + timedelta(days=config.window_days)).date(),
            (_EPOCH + timedelta(days=config.window_days + config.anchor_periods)).date(),
        ),
        features=features,
    )
    panel = EventPanel(
        Design("user", EventAnchor("event", "first"), features=features),
        periods=(0, 1, 2),
        period_seconds=86400,
        reference_period=0,
    )
    sketch = Design(
        "user",
        fixed.anchor,
        features={
            **features,
            "hll": approx_distinct("value", window=window),
            "tdigest": approx_quantile(
                "value",
                "0.25",
                window=window,
                sketch=SketchRequest("tdigest", "5.2.0", Decimal("0.01")),
            ),
        },
    )
    return {
        "frame": fixed,
        "schedule": schedule,
        "sequential": Sequential(
            schedule, treatment=Initiation("events", "adopted", config.coverage_start)
        ),
        "event_panel": panel,
        "sketch": sketch,
    }


def _json(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported report scalar {type(value).__name__}")


def content_digest(batches) -> str:
    """Hash canonical ordered rows, independently of Arrow batch boundaries."""
    digest = hashlib.sha256()
    for batch in batches:
        for row in batch.to_pylist():
            digest.update(
                json.dumps(row, sort_keys=True, separators=(",", ":"), default=_json).encode()
            )
            digest.update(b"\n")
    return digest.hexdigest()


def row_bound(config: ScaleConfig, case: str) -> int:
    """Bound emitted rows using only the declared unit and anchor dimensions."""
    periods = {
        "frame": 1,
        "schedule": config.anchor_periods,
        "sequential": config.anchor_periods,
        "event_panel": 3,
        "sketch": 1,
    }
    return config.units * periods[case]


def check_rows(
    table: pa.Table, config: ScaleConfig, case: str, *, corrected: bool = False, columns=None
) -> dict[str, Any]:
    """Compare exact outputs and availability; report, rather than promise, sketch error."""
    actual = table.sort_by([("unit", "ascending"), ("t0", "ascending")])
    expected = iter(metric_rows(config, case, corrected=corrected))
    digest = hashlib.sha256()
    errors: dict[str, float | None] = {
        "hll_max_absolute_error": None,
        "hll_max_relative_error": None,
        "tdigest_max_absolute_error": None,
    }
    for batch in actual.to_batches(max_chunksize=config.batch_size):
        for row in batch.to_pylist():
            reference = next(expected, None)
            if reference is None or (row["unit"], row["t0"]) != (
                reference["unit"],
                reference["t0"],
            ):
                raise AssertionError("compiler row support differs from independent oracle")
            for name in _EXACT:
                if row[name] != reference[name]:
                    raise AssertionError(f"exact {case}/{name} differs from independent oracle")
                if columns is not None and name != "total":
                    meta = columns[name]
                    if (
                        row[meta.reason_column] != reference["reason"]
                        or not row[meta.complete_column]
                    ):
                        raise AssertionError(
                            "compiler completeness differs from independent oracle"
                        )
            if case == "sequential" and row["treatment"] != reference["treatment"]:
                raise AssertionError(
                    "treatment history differs from independent observation contract"
                )
            for name, truth in (("hll", reference["distinct"]), ("tdigest", reference["q25"])):
                if name not in row:
                    continue
                value = row[name]
                if truth is None:
                    if value is not None:
                        raise AssertionError("sketch populated an unavailable value")
                    continue
                if value is None or not math.isfinite(float(value)):
                    raise AssertionError("sketch lost a finite available value")
                error = abs(float(value) - float(truth))
                key = f"{name}_max_absolute_error"
                errors[key] = max(errors[key] or 0, error)
                if name == "hll" and truth:
                    errors["hll_max_relative_error"] = max(
                        errors["hll_max_relative_error"] or 0, error / float(truth)
                    )
                if name == "hll" and not truth and value != 0:
                    raise AssertionError("empty distinct sketch must be zero")
            public = {
                name: row[name] for name in ("unit", "t0", *_EXACT, "hll", "tdigest") if name in row
            }
            digest.update(
                json.dumps(public, sort_keys=True, separators=(",", ":"), default=_json).encode()
            )
            digest.update(b"\n")
    if next(expected, None) is not None:
        raise AssertionError("compiler omitted independently expected rows")
    return {"oracle_match": True, "output_digest": digest.hexdigest(), "approximate_errors": errors}


def run_case(model: Model, config: ScaleConfig, case: str, design: Design) -> dict[str, Any]:
    compiled = compile_design(design, model)
    anchors = int(
        model.con.to_pyarrow(compiled._anchors.table.count().as_table()).column(0)[0].as_py()
    )
    events = model.con.table("events")
    input_events = int(model.con.to_pyarrow(events.count().as_table()).column(0)[0].as_py())
    bounds = {}
    probes = {}
    for name, plan in compiled.range_plans.items():
        window = Window(-config.window_days * 86400, 0)
        if plan.block_seconds not in probes:
            joined = blocked_range_join(
                compiled._anchors.table,
                events,
                plan=plan,
                window=window,
                event_unit="unit_id",
                event_timestamp="event_time",
            )
            requests = int(
                model.con.to_pyarrow(joined.requests.count().as_table()).column(0)[0].as_py()
            )
            candidates = int(
                model.con.to_pyarrow(joined.candidates.count().as_table()).column(0)[0].as_py()
            )
            probes[plan.block_seconds] = requests, candidates
        requests, candidates = probes[plan.block_seconds]
        if requests > 2 * anchors or candidates > plan.max_candidates_per_event * input_events:
            raise AssertionError("range intermediate exceeded its input-derived bound")
        bounds[name] = {
            **asdict(plan),
            "requests": requests,
            "request_bound": 2 * anchors,
            "candidates": candidates,
            "candidate_bound": plan.max_candidates_per_event * input_events,
        }
        bounds[name]["probe_scope"] = "raw_source_bucket_join_upper_relation"
    budget = Budget(max_rows=row_bound(config, case))
    materialized = design.build(model).materialize(budget=budget)
    checked = check_rows(materialized.table, config, case, columns=materialized.columns)
    approximation = {
        name: _encode(meta.approximation)
        for name, meta in materialized.columns.items()
        if meta.approximation is not None
    }
    query = model.con.compile(compiled.table)
    physical = (
        model.con.raw_sql("EXPLAIN " + query).fetchall() if model.con.name == "duckdb" else None
    )
    return {
        "case": case,
        **checked,
        "anchors": anchors,
        "emitted_rows": len(materialized.table),
        "output_bytes": materialized.table.nbytes,
        "range_bounds": bounds,
        "strategy": dict(compiled.strategy),
        "approximation": approximation,
        "physical_plan": [row[1] for row in physical] if physical is not None else None,
        "physical_plan_reason": None
        if physical is not None
        else "backend_plan_inspection_unavailable",
        "budget": {"max_rows": budget.max_rows, "max_scan_bytes": None},
    }
