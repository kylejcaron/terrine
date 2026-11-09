# ruff: noqa: F811
from dataclasses import replace

import ibis
import pytest

from terrine.compile.anchors import compile_anchors
from terrine.compile.columns import compile_columns
from terrine.compile.layers import build_layer, plan_layers
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, FixedDate, agg
from tests.compile.test_columns import column_model  # noqa: F401,F811


def _persisted_anchors(design, model):
    return {
        replace(key, snapshot_id="same-snapshot"): build_layer(design, model, key)
        for key in plan_layers(design, model).keys
    }


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


def test_layer_substitution_checks_anchor_unit_grain_and_resolution_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    key, relation = next(iter(_persisted_anchors(design, column_model).items()))
    # A valid relation under a key whose grain or unit is wrong must still refuse.
    for bad in (
        replace(key, grain="event"),
        replace(key, unit="other"),
        replace(key, resolution="event"),
        replace(key, measure="invalid"),
    ):
        with pytest.raises(ValueError):
            compose_materialized_frame(design, column_model, persisted={bad: relation})


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
    with pytest.raises(ValueError):
        compose_materialized_frame(design, column_model, persisted={a: empty, b: empty})


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
    original = _persisted_anchors(design, column_model)
    other = _persisted_anchors(design, other_model)
    original_key, _ = next(iter(original.items()))
    other_key, other_anchor = next(iter(other.items()))
    assert original_key.definition_hash != other_key.definition_hash
    with pytest.raises(ValueError):
        compose_materialized_frame(
            design,
            other_model,
            persisted={replace(original_key, snapshot_id="same"): other_anchor},
        )
    actual = compose_materialized_frame(design, other_model, persisted=other)
    expected = compile_columns(design, other_model)
    assert column_model.con.to_pyarrow(expected.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(actual.audit_table.order_by("unit"))
    )


def test_runtime_layer_composition_matches_direct_state_audit(column_model):
    from terrine.compile.dispatch import compile_design
    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    design = Design(
        "user",
        Schedule("1d", "2025-01-02", "2025-01-02"),
        shape=StatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=1,
        ),
    )
    ordinary = compile_design(design, column_model)
    budget = Budget(max_rows=100, max_output_bytes=100_000)
    anchors = _persisted_anchors(design, column_model)
    materialized = compose_runtime_frame(design, column_model, persisted=anchors, budget=budget)
    assert column_model.con.to_pyarrow(ordinary.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(materialized.audit_table.order_by("unit"))
    )


@pytest.mark.parametrize("role", ["unwrapped", "feature", "outcome", "eligibility"])
def test_runtime_composition_keeps_approximate_value_and_predicate_refusals_distinct(
    column_model, role
):

    from terrine.runtime.materialize import compose_runtime_frame
    from terrine.spec import approx_quantile, gt

    value = approx_quantile("total", "0.5", window=("-1d", "0d"))
    predicate = {"q": gt(value, 0)}
    design = Design(
        "user",
        FixedDate("2025-01-03"),
        features={"ordinary": value, **(predicate if role == "feature" else {})},
        outcomes=predicate if role == "outcome" else None,
        eligibility=predicate if role == "eligibility" else None,
    )
    with pytest.raises(TerrineError) as runtime_error:
        compose_runtime_frame(design, column_model, persisted={})
    if role == "unwrapped":
        assert runtime_error.value.code == "terrine.runtime.sketch_sidecar_unavailable"
    else:
        predicate_only = Design(
            "user",
            design.anchor,
            features=predicate if role == "feature" else None,
            outcomes=predicate if role == "outcome" else None,
            eligibility=predicate if role == "eligibility" else None,
        )
        with pytest.raises(TerrineError) as validation_error:
            predicate_only.validate(column_model)
        assert runtime_error.value.code == validation_error.value.code
        assert runtime_error.value.code == "terrine.metric.approx_eligibility_unavailable"
        assert runtime_error.value.context == validation_error.value.context


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

    initiation = Initiation("tier", history_start=datetime(2025, 1, 1, tzinfo=UTC))
    shapes = {
        "EventSequence": EventSequence("amount", Window("-1d", "0d"), max_events=3),
        "SequentialSpec": SequentialSpec(initiation),
        "EventPanelSpec": EventPanelSpec((-1, 0), "1d", -1),
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
        Schedule("1d", "2025-01-02", "2025-01-02"),
        shape=DerivedStatePanel(
            BinnedMetricState("total", cutpoints=(0,), levels=("none", "some")),
            periods=2,
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
    changed["measures"]["count_amount"] = {"event": "amount", "aggregation": "count"}
    changed["measures"]["new_amount"] = {
        "event": "amount",
        "aggregation": "sum",
        "filters": [{"property": "tier", "op": "equals", "values": ["new"]}],
    }
    changed["metrics"]["count_amount"] = {
        "kind": "mean",
        "unit": "user",
        "measure": "count_amount",
    }
    changed["metrics"]["new_amount"] = {
        "kind": "mean",
        "unit": "user",
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
    persisted = _persisted_anchors(design, model)
    expected = compile_columns(design, model)
    actual = compose_materialized_frame(design, model, persisted=persisted)
    assert column_model.con.to_pyarrow(expected.audit_table.order_by("unit")).equals(
        column_model.con.to_pyarrow(actual.audit_table.order_by("unit"))
    )


def test_measure_mode_change_invalidates_persisted_anchors(column_model):
    column_model.con.raw_sql("ALTER TABLE events ALTER amount TYPE DOUBLE")
    native = column_model.catalog.model_dump(mode="json")
    native["measures"]["total"]["exact"] = False
    models = {
        exact: Model.from_catalog(
            definition,
            con=column_model.con,
            readiness=column_model.readiness,
            units=column_model.units,
        )
        for exact, definition in ((True, column_model.catalog), (False, native))
    }
    design = Design(
        "user",
        FixedDate("2025-01-02T12:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    keys = {exact: plan_layers(design, model).keys[0] for exact, model in models.items()}
    assert keys[True].definition_hash != keys[False].definition_hash
    # A mode change invalidates persisted layers instead of reusing them.
    with pytest.raises(ValueError):
        compose_materialized_frame(
            design, models[False], persisted=_persisted_anchors(design, models[True])
        )


def test_physical_source_type_change_invalidates_persisted_layer_identity(column_model):
    design = Design(
        "user",
        FixedDate("2025-01-03T00:00:00+00:00"),
        features={"total": agg("total", window=("-1d", "0s"))},
    )
    old_key, old_relation = next(iter(_persisted_anchors(design, column_model).items()))

    column_model.con.raw_sql("ALTER TABLE events ALTER amount TYPE DECIMAL(12, 4)")
    changed_model = Model.from_catalog(
        column_model.catalog,
        con=column_model.con,
        readiness=column_model.readiness,
        units=column_model.units,
        event_ids=column_model.event_ids,
    )

    new_key = plan_layers(design, changed_model).keys[0]
    assert new_key.definition_hash != old_key.definition_hash
    with pytest.raises(ValueError):
        compose_materialized_frame(
            design,
            changed_model,
            persisted={replace(old_key, snapshot_id="same-snapshot"): old_relation},
        )


@pytest.mark.parametrize("entry", ["sequence", "offset"])
def test_direct_dispatch_refuses_unsupported_substitutions_before_sources(
    column_model, monkeypatch, entry
):
    from terrine.compile.dispatch import compile_design
    from terrine.spec import EventSequence
    from terrine.windows import Window

    design = (
        Design("user", EventAnchor("amount", offset="1d"))
        if entry == "offset"
        else Design(
            "user",
            FixedDate("2025-01-03"),
            shape=EventSequence("amount", Window("-1d", "0s"), max_events=3),
        )
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("unsupported substitution resolved a source")

    monkeypatch.setattr(Model, "_relation", forbidden)
    persisted = ibis.memtable({"x": [1]})
    entrypoints = [
        lambda: compile_design(design, column_model, persisted_anchors=persisted),
        lambda: compose_materialized_frame(design, column_model, persisted={}),
    ]
    if entry == "offset":
        entrypoints.append(lambda: compile_anchors(design, column_model, persisted=persisted))
    for invoke in entrypoints:
        with pytest.raises(TerrineError) as error:
            invoke()
        assert error.value.code == "terrine.compile.runtime_shape_unsupported"
        assert error.value.context["shape"] == (
            "EventAnchor.offset" if entry == "offset" else "EventSequence"
        )
