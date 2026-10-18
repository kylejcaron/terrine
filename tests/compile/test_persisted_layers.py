from dataclasses import replace

import ibis
import pytest

from terrine.compile.layers import plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.semantic.bind import normalize_events
from terrine.spec import Design, FixedDate, agg
from terrine.temporal.daily import _sparse_group, resolve_measure
from terrine.temporal.prefix import sparse_prefix
from tests.compile.test_columns import _compile

pytest_plugins = ("tests.compile.test_columns",)


def _persisted_event_prefix(design, model):
    key = plan_layers(design, model).keys[0]
    source = model.sources[key.source]
    events = normalize_events(
        model._relation(source.relation), source, source.entities[design.unit]
    )
    measure = next(m for m in model.metrics["total"].measures if m.fact == "amount")
    measure = resolve_measure(events, measure, source=source)
    grouped = _sparse_group(
        events, measure, {"_timestamp": events._timestamp}, "_unit", source=source
    )
    return key, sparse_prefix(grouped, measure, resolution="event")


def test_persisted_daily_prefix_anchor_reuse_matches_ordinary_frame_compiler(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = _compile(column_model, features={"total": agg("total", window=("-1d", "0s"))})
    key, prefix = _persisted_event_prefix(design, column_model)
    key = replace(key, snapshot_id="same-snapshot")
    reused = compose_materialized_frame(design, column_model, persisted={key: prefix})
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(reused.audit_table.order_by("unit"))
    )
    assert ordinary.columns == reused.columns


def test_layer_substitution_checks_measure_unit_grain_and_resolution_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key = plan_layers(design, column_model).keys[0]
    for bad in (replace(key, grain="event"), replace(key, unit="other")):
        with pytest.raises(ValueError, match="identity"):
            compose_materialized_frame(
                design, column_model, persisted={bad: ibis.memtable({"x": []})}
            )


def test_layer_substitution_rejects_cross_snapshot_relations(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key = plan_layers(design, column_model).keys[0]
    a = replace(key, snapshot_id="snapshot-a")
    b = replace(key, snapshot_id="snapshot-b", source="other")
    empty = ibis.memtable({"_unit": [], "_timestamp": [], "_value": []})
    with pytest.raises(ValueError, match="one non-empty snapshot"):
        compose_materialized_frame(design, column_model, persisted={a: empty, b: empty})


def test_layer_plan_retains_distinct_day_and_event_resolutions(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        features={
            "daily": agg("total", window=("-1d", "0s")),
            "hourly": agg("total", window=("-1h", "0s")),
        },
    )
    keys = plan_layers(design, column_model).keys
    assert {key.resolution for key in keys} == {"day", "event"}


def test_layer_composition_preserves_strict_scan_budget_refusal(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    with pytest.raises(TerrineError) as error:
        compose_materialized_frame(
            design,
            column_model,
            persisted={},
            budget=Budget(max_scan_bytes=1, strict_scan=True),
        )
    assert error.value.code == "terrine.budget.scan_unknown"
