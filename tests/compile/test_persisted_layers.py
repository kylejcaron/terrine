# ruff: noqa: F811
from dataclasses import replace

import ibis
import pytest

from terrine.compile.anchors import compile_anchors
from terrine.compile.columns import compile_columns
from terrine.compile.layers import measure_identity, plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg
from terrine.temporal.daily import _sparse_group, resolve_measure, sparse_daily
from terrine.temporal.prefix import sparse_prefix
from tests.compile.test_columns import column_model  # noqa: F401,F811


def _persisted_prefixes(design, model):
    measure_by_id = {
        measure_identity(measure): measure
        for metric in model.metrics.values()
        for measure in metric.measures
    }
    result = {}
    for key in plan_layers(design, model).keys:
        if key.layer != "prefix":
            continue
        source = model.sources[key.source]
        events = normalize_events(
            model._relation(source.relation), source, source.entities[design.unit]
        )
        if key.measure is None or key.resolution not in {"day", "event"}:
            raise AssertionError("planned temporal layer has an invalid measure or resolution")
        measure = resolve_measure(events, measure_by_id[key.measure], source=source)
        grouped = (
            sparse_daily(events, measure, model.catalog.day_boundary, "_unit", source=source)
            if key.resolution == "day"
            else _sparse_group(
                events, measure, {"_timestamp": events._timestamp}, "_unit", source=source
            )
        )
        relation = sparse_prefix(grouped, measure, resolution=key.resolution)
        result[replace(key, snapshot_id="same-snapshot")] = relation
    return result


@pytest.mark.parametrize(
    ("invalid_anchor", "check_key"),
    [
        (lambda table: table.union(table, distinct=False), "duplicate_event_id"),
        (
            lambda table: table.mutate(anchor_id=ibis.literal(None, type="string")),
            "null_event_id",
        ),
    ],
)
def test_persisted_anchor_validation_checks_candidate_identity(
    column_model, invalid_anchor, check_key
):
    design = Design("user", FixedDate("2025-01-03T00:00:00+00:00"))
    persisted = invalid_anchor(compile_anchors(design, column_model).table)
    compiled = compile_anchors(design, column_model, persisted=persisted)
    with pytest.raises(TerrineError) as error:
        compiled.validate(execute=column_model.con.to_pyarrow)
    assert error.value.code == "terrine.compile.anchor_integrity"
    assert error.value.context["check_key"] == check_key


def test_layer_substitution_checks_measure_unit_grain_and_resolution_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key = next(key for key in plan_layers(design, column_model).keys if key.layer == "prefix")
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
    key = next(key for key in plan_layers(design, column_model).keys if key.layer == "prefix")
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
    keys = tuple(key for key in plan_layers(design, column_model).keys if key.layer == "prefix")
    assert {key.resolution for key in keys} == {"day", "event"}


def test_layer_composition_preserves_strict_scan_budget_refusal(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    ordinary = compose_materialized_frame(
        design,
        column_model,
        persisted={},
        budget=Budget(max_scan_bytes=1, strict_scan=False),
    )
    assert ordinary.roles.features == ("total",)
    with pytest.raises(TerrineError) as error:
        compose_materialized_frame(
            design,
            column_model,
            persisted={},
            budget=Budget(max_scan_bytes=1, strict_scan=True),
        )
    assert error.value.code == "terrine.budget.scan_unknown"


def test_layer_definition_identity_binds_catalog_and_design(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    changed = column_model.catalog.model_dump(mode="json")
    changed["measures"]["total"]["aggregation"] = "count"
    other_model = Model.from_catalog(
        Catalog.model_validate(changed),
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    original = _persisted_prefixes(design, column_model)
    other = _persisted_prefixes(design, other_model)
    original_key, original_prefix = next(iter(original.items()))
    other_key, other_prefix = next(iter(other.items()))
    assert original_key.definition_hash != other_key.definition_hash
    with pytest.raises(ValueError, match="identity"):
        compose_materialized_frame(
            design,
            other_model,
            persisted={replace(original_key, snapshot_id="same"): other_prefix},
        )
    actual = compose_materialized_frame(design, other_model, persisted=other)
    expected = compile_columns(design, other_model)
    assert column_model.con.to_pyarrow(expected.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(actual.audit_table.order_by("unit"))
    )

    from terrine.compile.dispatch import compile_design
    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-03"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    ordinary = compile_design(design, column_model)
    budget = Budget(max_rows=100, max_output_bytes=100_000)
    materialized = compose_runtime_frame(design, column_model, persisted={}, budget=budget)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(materialized.audit_table.order_by("unit"))
    )


def test_runtime_composition_refuses_approximate_quantile_sidecar_before_source_work(
    column_model,
):
    from decimal import Decimal

    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import SketchRequest, approx_quantile

    design = Design(
        "user",
        FixedDate("2025-01-03"),
        features={
            "q": approx_quantile(
                "total",
                "0.5",
                window=("-1d", "0d"),
                sketch=SketchRequest("tdigest", "5.2.0", Decimal("0.01")),
            )
        },
    )
    with pytest.raises(TerrineError) as error:
        compose_runtime_frame(design, column_model, persisted={})
    assert error.value.code == "terrine.runtime.sketch_sidecar_unavailable"


@pytest.mark.parametrize(
    "shape_name",
    [
        "custom_shape",
        "EventSequence",
        "CalendarComparisonSpec",
        "CohortSpec",
        "TimeToEvent",
    ],
)
def test_unsupported_runtime_shape_refusal_names_shape_and_route(column_model, shape_name):
    from datetime import UTC, datetime

    from terrine.spec import (
        CalendarComparisonSpec,
        CohortSpec,
        EventAnchor,
        EventPanelSpec,
        EventSequence,
        Initiation,
        SequentialSpec,
        TimeToEvent,
        Trajectory,
    )
    from terrine.windows import Window

    initiation = Initiation("events", "tier", datetime(2025, 1, 1, tzinfo=UTC))
    shapes = {
        "EventSequence": EventSequence("amount", Window("-1d", "0d"), max_events=3),
        "SequentialSpec": SequentialSpec(initiation),
        "EventPanelSpec": EventPanelSpec((-1, 0), 86_400, -1),
        "CalendarComparisonSpec": CalendarComparisonSpec(initiation, "never_treated", -2, (0,)),
        "CohortSpec": CohortSpec("amount", Trajectory("total", "1d", 2)),
        "TimeToEvent": TimeToEvent("amount"),
    }
    shape = shapes.get(shape_name, shape_name)
    anchor = (
        EventAnchor("amount")
        if shape_name == "CohortSpec"
        else FixedDate("2025-01-03T00:00:00+00:00")
    )
    design = Design("user", anchor, shape=shape)
    with pytest.raises(TerrineError) as error:
        compose_materialized_frame(design, column_model, persisted={})
    assert error.value.code == "terrine.compile.runtime_shape_unsupported"
    assert error.value.context["shape"] == shape_name
    assert error.value.context["route"] == "ordinary Frame materialization or a full rebuild"


def test_runtime_and_frame_share_shape_budget_refusal():
    from typing import cast

    from terrine.frame import Frame
    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    class DerivedStatePanel(StatePanel):
        pass

    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-03"),
        shape=DerivedStatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            max_periods=2,
        ),
    )
    frame = object.__new__(Frame)
    object.__setattr__(frame, "_design", design)
    object.__setattr__(frame, "_model", None)
    refusals = []
    for operation in (
        lambda: frame.materialize(),
        lambda: compose_runtime_frame(design, cast(Model, None), persisted={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        refusals.append(error.value.code)
    assert refusals == ["terrine.state_panel.budget_required"] * 2


def test_same_fact_measures_keep_filter_and_aggregation_identity(column_model):
    changed = column_model.catalog.model_dump(mode="json")
    changed["measures"]["count_amount"] = {"fact": "amount", "aggregation": "count"}
    changed["measures"]["new_amount"] = {
        "fact": "amount",
        "aggregation": "sum",
        "filters": [{"property": "tier", "op": "equals", "values": ["new"]}],
    }
    changed["metrics"]["count_amount"] = {
        "kind": "mean",
        "entity": "user",
        "measure": "count_amount",
    }
    changed["metrics"]["new_amount"] = {
        "kind": "mean",
        "entity": "user",
        "measure": "new_amount",
    }
    model = Model.from_catalog(
        Catalog.model_validate(changed),
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
    )
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={
            "sum": agg("total", window=("-1d", "0s")),
            "count": agg("count_amount", window=("-1d", "0s")),
            "filtered": agg("new_amount", window=("-1d", "0s")),
        },
    )
    layers = tuple(key for key in plan_layers(design, model).keys if key.layer == "prefix")
    assert len(layers) == 3
    persisted = _persisted_prefixes(design, model)
    expected = compile_columns(design, model)
    actual = compose_materialized_frame(design, model, persisted=persisted)
    assert column_model.con.to_pyarrow(expected.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(actual.audit_table.order_by("unit"))
    )


def test_physical_source_type_change_invalidates_persisted_layer_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    old_key = next(key for key in plan_layers(design, column_model).keys if key.layer == "prefix")
    old_relation = ibis.memtable({"_unit": [], "_timestamp": [], "_prefix": []})

    column_model.con.raw_sql("ALTER TABLE events ALTER amount TYPE DECIMAL(12, 4)")
    changed_model = Model.from_catalog(
        column_model.catalog,
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids=column_model.event_ids,
    )

    new_key = next(key for key in plan_layers(design, changed_model).keys if key.layer == "prefix")
    assert new_key.definition_hash != old_key.definition_hash
    with pytest.raises(ValueError, match="identity"):
        compose_materialized_frame(
            design,
            changed_model,
            persisted={replace(old_key, snapshot_id="same-snapshot"): old_relation},
        )


def test_direct_dispatch_refuses_unsupported_substitutions_before_sources(
    column_model, monkeypatch
):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import EventSequence
    from terrine.windows import Window

    design = Design(
        "user",
        FixedDate("2025-01-03"),
        shape=EventSequence("amount", Window("-1d", "0s"), max_events=3),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("unsupported substitution resolved a source")

    monkeypatch.setattr(Model, "_relation", forbidden)
    with pytest.raises(TerrineError) as error:
        compile_design(design, column_model, persisted_anchors=ibis.memtable({"x": [1]}))
    assert error.value.code == "terrine.compile.runtime_shape_unsupported"
    assert error.value.context["shape"] == "EventSequence"


def test_reanchored_layer_plan_uses_event_resolution(column_model):
    from terrine.spec import event_time

    design = Design(
        "user",
        FixedDate("2025-01-03"),
        features={
            "total": agg(
                "total",
                window=("-1d", "0s"),
                relative_to=event_time("amount", window=("-2d", "-1d")),
            )
        },
    )
    keys = plan_layers(design, column_model).keys
    assert {key.resolution for key in keys if key.layer == "prefix"} == {"event"}
