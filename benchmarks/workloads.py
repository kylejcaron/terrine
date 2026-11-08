"""Bounded synthetic loading and real Model/Design benchmark cases."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa

from terrine import Budget, Coverage, Model, UnitSource, recipes
from terrine.compile.dispatch import compile_design
from terrine.data import _encode
from terrine.semantic.models import Catalog
from terrine.simulate.scale import ScaleConfig, event_batches, unit_batches
from terrine.spec import (
    BinnedMetricState,
    Design,
    EventAnchor,
    EventPanel,
    FixedDate,
    Initiation,
    Schedule,
    Sequential,
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
            "schema_version": 3,
            "day_boundary": "UTC",
            "dialect": dialect,
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "event_time",
                    "units": {"user": "unit_id"},
                    "events": ["value", "event"],
                    "properties": ["adopted", "event_name"],
                    "dimensions": [],
                }
            },
            "events": {
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
                name: {"event": "value", "aggregation": aggregation}
                for name, aggregation in aggregations.items()
            },
            "metrics": {
                name: {"kind": "mean", "unit": "user", "measure": measure}
                for name, measure in (*((name, name) for name in aggregations), ("value", "total"))
            },
        }
    )


_UNITS = {
    "user": UnitSource(
        "users", "unit_id", observed_from="observed_from", observed_until="observed_until"
    )
}


def _coverage(config: ScaleConfig) -> Coverage:
    return Coverage(config.coverage_start.date(), (config.coverage_end - timedelta(days=1)).date())


def load_tables(con: Any, config: ScaleConfig) -> dict[str, int]:
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
    return counts


def load_model(
    con: Any, config: ScaleConfig, *, dialect: str = "duckdb"
) -> tuple[Model, dict[str, int]]:
    """Load the workload relations and bind the benchmark catalog over them."""
    counts = load_tables(con, config)
    model = Model.from_catalog(
        _catalog(dialect),
        con=con,
        units=_UNITS,
        readiness={"events": _coverage(config)},
        event_ids={"events": "event_id"},
    )
    return model, counts


def designs(config: ScaleConfig) -> dict[str, Design]:
    window = (f"-{config.window_days}d", "0s")
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
            (_EPOCH + timedelta(days=config.window_days + config.anchor_periods - 1)).date(),
        ),
        features=features,
    )
    panel = EventPanel(
        Design("user", EventAnchor("event", "first"), features=features),
        periods=(0, 1, 2),
        period="1d",
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
            ),
        },
    )
    return {
        "frame": fixed,
        "schedule": schedule,
        "sequential": Sequential(
            schedule, treatment=Initiation("adopted", history_start=config.coverage_start)
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
                if not isinstance(truth, int | float | Decimal):
                    raise AssertionError("sketch oracle returned a nonnumeric value")
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
        window = Window(f"-{config.window_days}d", "0s")
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


# Cost budget: one ordinary Design per recipe and a hand-written query of the same semantics.


def _cost_catalog(dialect: str) -> Catalog:
    """Additive metrics of every kind over all events, and a filtered end event."""
    return Catalog.model_validate(
        {
            "schema_version": 3,
            "day_boundary": "UTC",
            "dialect": dialect,
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "event_time",
                    "units": {"user": "unit_id"},
                    "events": ["event", "value"],
                    "properties": ["adopted"],
                    "dimensions": [],
                },
                "targets": {
                    "relation": {"kind": "table", "name": "events"},
                    "time": "event_time",
                    "units": {"user": "unit_id"},
                    "events": ["target"],
                    "properties": [],
                    "dimensions": [],
                    "where": [{"property": "event_type", "op": "equals", "values": ["target"]}],
                },
            },
            "events": {
                "event": {"source": "events", "column": None},
                "value": {"source": "events", "column": "value"},
                "target": {"source": "targets", "column": None},
            },
            "properties": {
                "adopted": {
                    "source": "events",
                    "column": "adopted",
                    "dtype": "string",
                    "as_of": "event_time",
                }
            },
            "dimensions": {},
            "measures": {
                "events": {"event": "event", "aggregation": "count"},
                "total": {"event": "value", "aggregation": "sum"},
                "average": {"event": "value", "aggregation": "avg_event"},
            },
            "metrics": {
                **{
                    name: {"kind": "mean", "unit": "user", "measure": name}
                    for name in ("events", "total", "average")
                },
                "per_event": {
                    "kind": "ratio",
                    "unit": "user",
                    "numerator": "total",
                    "denominator": "events",
                },
                "active": {"kind": "conversion", "unit": "user", "measure": "events"},
            },
        }
    )


def cost_model(con: Any, config: ScaleConfig, *, dialect: str = "duckdb") -> Model:
    """Bind the cost-budget catalog over relations created by ``load_tables``."""
    return Model.from_catalog(
        _cost_catalog(dialect),
        con=con,
        units=_UNITS,
        readiness={"events": _coverage(config), "targets": _coverage(config)},
        event_ids={"events": "event_id", "targets": "event_id"},
    )


@dataclass(frozen=True, slots=True)
class RecipeCase:
    """A recipe's Design, its feature count and a hand-written query over the same rows.

    ``reference`` returns ``key`` and ``values`` columns equal to the Design's on the
    synthetic workload, where every window lies inside declared coverage and each
    unit's observation interval.
    """

    design: Design
    features: int
    key: tuple[str, ...]
    values: tuple[str, ...]
    reference: str


def _instant(value: datetime) -> str:
    return f"TIMESTAMPTZ '{value.isoformat(sep=' ')}'"


def _days(count: int) -> str:
    return f"INTERVAL '{count * 86400} seconds'"


def _within(start: int, end: int, origin: str = "a.t0") -> str:
    return f"e.event_time >= {origin} + {_days(start)} AND e.event_time < {origin} + {_days(end)}"


def _count(start: int, end: int) -> str:
    return f"COUNT(e.event_id) FILTER (WHERE {_within(start, end)})"


def _total(start: int, end: int) -> str:
    """Zero for an empty window, NULL when every event in it has a NULL value."""
    window = _within(start, end)
    return (
        f"CASE WHEN COUNT(e.value) FILTER (WHERE {window}) = 0 AND {_count(start, end)} > 0 "
        f"THEN NULL ELSE COALESCE(SUM(e.value) FILTER (WHERE {window}), 0) END"
    )


def _windowed(columns: dict[str, str], start: int, end: int, *, extra: str = "") -> str:
    """Aggregate events joined to the ``anchors`` (unit, t0) relation over their windows."""
    selected = ",\n  ".join(f"{expression} AS {name}" for name, expression in columns.items())
    return (
        f"SELECT a.unit, a.t0,\n  {selected}\n"
        f"FROM anchors AS a{extra}\n"
        f"LEFT JOIN events AS e ON e.unit_id = a.unit AND {_within(start, end)}\n"
        "GROUP BY a.unit, a.t0"
    )


def _follow_up(config: ScaleConfig, features: dict[str, str], window: int) -> str:
    """Time to the first target at or after each unit's first event, censored at coverage end."""
    end = _instant(config.coverage_end)
    feature_columns = "".join(
        f",\n  {expression} AS {name}" for name, expression in features.items()
    )
    feature_join = (
        f"\nLEFT JOIN events AS e ON e.unit_id = a.unit AND {_within(-window, 0)}"
        if features
        else ""
    )
    return (
        "WITH a AS (SELECT unit_id AS unit, MIN(event_time) AS t0 FROM events GROUP BY unit_id),\n"
        "targets AS (\n"
        "  SELECT a.unit, MIN(e.event_time) AS event_time FROM a\n"
        "  JOIN events AS e ON e.unit_id = a.unit AND e.event_type = 'target'\n"
        f"    AND e.event_time >= a.t0 AND e.event_time < {end}\n"
        "  GROUP BY a.unit\n"
        ")\n"
        f"SELECT a.unit, a.t0, t.event_time, {end} AS censor_time,\n"
        f"  COALESCE(t.event_time, {end}) AS duration, t.event_time IS NOT NULL AS event,\n"
        "  a.t0 AS entry_time,\n"
        "  CASE WHEN t.event_time IS NULL THEN 'source_coverage' END AS censor_reason"
        f"{feature_columns}\n"
        f"FROM a LEFT JOIN targets AS t ON t.unit = a.unit{feature_join}\n"
        "WHERE NOT EXISTS (\n"
        "  SELECT 1 FROM events AS p\n"
        "  WHERE p.unit_id = a.unit AND p.event_type = 'target' AND p.event_time < a.t0\n"
        ")\n"
        "GROUP BY a.unit, a.t0, t.event_time"
    )


def recipe_cases(config: ScaleConfig) -> dict[str, RecipeCase]:
    """Every recipe over the cost catalog, with a hand-written reference query each.

    ``features`` counts declared features and outcomes plus one for the
    measurement the recipe's shape adds (treatment, trajectory, follow-up, state
    or sequence).
    """
    days = config.window_days
    first = _EPOCH + timedelta(days=days)
    t0s = [first + timedelta(days=period) for period in range(config.anchor_periods)]
    window = (f"-{days}d", "0s")
    features = {
        "recent_events": agg("events", window=window),
        "recent_total": agg("total", window=window),
    }
    feature_sql = {"recent_events": _count(-days, 0), "recent_total": _total(-days, 0)}

    def scheduled(instants) -> str:
        values = ", ".join(f"({_instant(t0)})" for t0 in instants)
        return (
            "WITH anchors AS (\n  SELECT u.unit_id AS unit, s.t0 FROM users AS u\n"
            f"  CROSS JOIN (VALUES {values}) AS s(t0)\n)\n"
        )

    origins = (
        "origins AS (SELECT unit_id AS unit, MIN(event_time) AS t0 FROM events GROUP BY unit_id)"
    )
    adoptions = (
        "\nLEFT JOIN (\n  SELECT unit_id, MIN(event_time) AS adopted_at FROM events\n"
        f"  WHERE adopted IS NOT NULL AND event_time >= {_instant(config.coverage_start)}\n"
        "  GROUP BY unit_id\n) AS d ON d.unit_id = a.unit"
    )
    periods = (
        f"WITH {origins},\nanchors AS (\n  SELECT o.unit, o.t0 + p.period * {_days(1)} AS t0\n"
        "  FROM origins AS o CROSS JOIN (VALUES (0), (1), (2)) AS p(period)\n)\n"
    )
    ages = ", ".join(f"({age})" for age in range(days))
    trajectory = (
        f"WITH {origins},\n"
        "periods AS (\n"
        "  SELECT o.unit, o.t0 AS cohort_time, g.age,\n"
        "    CASE WHEN COUNT(e.value) = 0 AND COUNT(e.event_id) > 0 THEN NULL\n"
        "      ELSE COALESCE(SUM(e.value), 0) END AS period_value\n"
        f"  FROM origins AS o CROSS JOIN (VALUES {ages}) AS g(age)\n"
        "  LEFT JOIN events AS e ON e.unit_id = o.unit\n"
        f"    AND e.event_time >= o.t0 + g.age * {_days(1)}\n"
        f"    AND e.event_time < o.t0 + (g.age + 1) * {_days(1)}\n"
        "  GROUP BY o.unit, o.t0, g.age\n"
        ")\n"
        "SELECT unit, cohort_time, age, period_value,\n"
        "  CASE WHEN SUM(CASE WHEN period_value IS NULL THEN 1 ELSE 0 END) OVER w > 0 THEN NULL\n"
        "    ELSE SUM(period_value) OVER w END AS cumulative_value\n"
        "FROM periods\n"
        "WINDOW w AS (PARTITION BY unit, cohort_time ORDER BY age\n"
        "  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )

    def level(name: str) -> str:
        return f"CASE WHEN {name} <= 0 THEN 'low' WHEN {name} > 0 THEN 'high' END"

    states = (
        f"SELECT unit, t0, {level('before_total')} AS from_state,\n"
        f"  {level('after_total')} AS to_state\nFROM (\n"
        + scheduled((first, first + timedelta(days=days)))
        + _windowed({"before_total": _total(-days, 0), "after_total": _total(0, days)}, -days, days)
        + "\n) AS totals"
    )
    sequence = (
        f"WITH {origins},\n"
        "anchors AS (SELECT * FROM origins),\n"
        "members AS (\n"
        "  SELECT o.unit, e.event_id, e.event_time,\n"
        "    ROW_NUMBER() OVER (PARTITION BY o.unit ORDER BY e.event_time, e.event_id) - 1\n"
        "      AS sequence_position\n"
        "  FROM origins AS o JOIN events AS e ON e.unit_id = o.unit\n"
        f"    AND {_within(0, days, 'o.t0')}\n"
        "),\n"
        "sequence_features AS (\n"
        + _windowed({**feature_sql, "event_count": _count(0, days)}, -days, days)
        + "\n)\n"
        "SELECT f.unit, f.t0, FALSE AS is_event, CAST(NULL AS BIGINT) AS sequence_position,\n"
        "  CAST(NULL AS TEXT) AS event_id, CAST(NULL AS TIMESTAMPTZ) AS event_time,\n"
        "  f.event_count, f.recent_events, f.recent_total\n"
        "FROM sequence_features AS f\n"
        "UNION ALL\n"
        "SELECT f.unit, f.t0, TRUE, m.sequence_position, m.event_id, m.event_time,\n"
        "  CAST(NULL AS BIGINT), f.recent_events, f.recent_total\n"
        "FROM sequence_features AS f JOIN members AS m ON m.unit = f.unit\n"
        "ORDER BY unit, t0, is_event, sequence_position"
    )
    follow_up = ("event_time", "censor_time", "duration", "event", "entry_time", "censor_reason")
    start, end = t0s[0].date(), t0s[-1].date()
    return {
        "time_series": RecipeCase(
            recipes.time_series(
                every="1d",
                start=start,
                end=end,
                features=features,
                outcomes={"next_total": agg("total", window=("0s", f"{days}d"))},
            ),
            3,
            ("unit", "t0"),
            (*features, "next_total"),
            scheduled(t0s) + _windowed({**feature_sql, "next_total": _total(0, days)}, -days, days),
        ),
        "trial_panel": RecipeCase(
            recipes.trial_panel(
                every="1d",
                start=start,
                end=end,
                treatment=Initiation("adopted", history_start=config.coverage_start),
                features=features,
            ),
            3,
            ("unit", "t0"),
            (*features, "treatment"),
            scheduled(t0s)
            + _windowed(
                {**feature_sql, "treatment": "COALESCE(MIN(d.adopted_at) <= a.t0, FALSE)"},
                -days,
                0,
                extra=adoptions,
            ),
        ),
        "event_panel": RecipeCase(
            recipes.event_panel(
                event="event", periods=(0, 1, 2), period="1d", reference_index=0, features=features
            ),
            2,
            ("unit", "t0"),
            tuple(features),
            periods + _windowed(feature_sql, -days, 0),
        ),
        "ltv": RecipeCase(
            recipes.ltv(start_event="event", metric="total", period="1d", max_periods=days),
            1,
            ("unit", "cohort_time", "age"),
            ("period_value", "cumulative_value"),
            trajectory,
        ),
        "survival": RecipeCase(
            recipes.survival(start_event="event", end_event="target", features=features),
            3,
            ("unit", "t0"),
            (*follow_up, *features),
            _follow_up(config, feature_sql, days),
        ),
        "life_table": RecipeCase(
            recipes.life_table(start_event="event", end_event="target", bin=f"{days}d"),
            1,
            ("unit", "t0"),
            follow_up,
            _follow_up(config, {}, days),
        ),
        "state_panel": RecipeCase(
            recipes.state_panel(
                state_source=BinnedMetricState("total", (0,), ("low", "high")),
                every=f"{days}d",
                start=first.date(),
                end=(first + timedelta(days=2 * days - 1)).date(),
            ),
            1,
            ("unit", "t0"),
            ("from_state", "to_state"),
            states,
        ),
        "event_sequence": RecipeCase(
            recipes.event_sequence(
                event="event",
                horizon=f"{days}d",
                max_events=config.events_per_unit,
                features=features,
            ),
            3,
            ("unit", "t0", "is_event", "sequence_position"),
            ("event_id", "event_time", "event_count", *features),
            sequence,
        ),
    }
