import pytest

from terrine.data import Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, EventAnchor, agg


def _frame(native_inputs, *, label_coverage_end="2025-01-31", missing_label=False):
    catalog, con = native_inputs
    if missing_label:
        labels = con.table("labels").filter(con.table("labels").user_id != "b").execute()
        con.drop_table("labels")
        con.create_table("labels", labels)
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}
    readiness["labels"] = Coverage("2025-01-01", label_coverage_end)
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness=readiness,
        units={"user": UnitSource("users", "user_id")},
    )
    return Design(
        "user",
        EventAnchor("session"),
        features={
            "past_sessions": agg("sessions", window=("-1d", "0d")),
            "earlier_sessions": agg("sessions", window=("-2d", "-1d")),
        },
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    ).build(model)


def test_training_set_selects_named_features_and_derives_label_support(native_inputs):
    frame = _frame(native_inputs)
    training = frame.training_set(label="churn", features=("past_sessions",))
    assert training is not frame
    assert training.roles.features == ("past_sessions",)
    assert training.roles.outcomes == ("churn",)
    assert "past_sessions" in training.expr.columns
    assert "earlier_sessions" not in training.expr.columns
    assert training.columns["churn"].complete_column
    assert training.columns["churn"].reason_column


def test_training_set_rejects_unknown_or_duplicate_selections(native_inputs):
    frame = _frame(native_inputs)
    with pytest.raises(TerrineError):
        frame.training_set(label="missing")
    with pytest.raises(ValueError):
        frame.training_set(label=("churn", "churn"))


def test_training_set_incomplete_label_policy_is_explicit(native_inputs):
    frame = _frame(native_inputs)
    kept = frame.training_set(label="churn", incomplete_labels="keep")
    dropped = frame.training_set(label="churn", incomplete_labels="drop")
    assert kept._training_view.incomplete_labels == "keep"
    assert dropped._training_view.incomplete_labels == "drop"
    with pytest.raises(ValueError):
        frame.training_set(label="churn", incomplete_labels="impute")


def test_boolean_feature_hash_includes_each_operand_definition(native_inputs):
    from dataclasses import replace

    from terrine.frame import _feature_definition_hash
    from terrine.spec import FixedDate, all_of, eq, gt

    bound = _frame(native_inputs)._model
    design = Design(
        "user",
        FixedDate("2025-01-03"),
        features={
            "active": all_of(
                gt(agg("sessions", window=("-1d", "0s")), 0),
                eq(agg("churned", window=("-1d", "0s")), 0),
            )
        },
    )
    original = _feature_definition_hash(design, ("active",), bound)
    for metric_name in ("sessions", "churned"):
        altered = Model(
            catalog=bound.catalog,
            con=bound.con,
            readiness=bound.readiness,
            units=bound.units,
            event_ids=bound.event_ids,
            sources=bound.sources,
            metrics={
                **bound.metrics,
                metric_name: replace(
                    bound.metrics[metric_name],
                    kind="mean"
                    if bound.metrics[metric_name].kind == "conversion"
                    else "conversion",
                ),
            },
            resolver=bound._resolver,
            input_binding=bound.input_binding,
            engine=bound._engine,
        )
        assert _feature_definition_hash(design, ("active",), altered) != original


def test_state_panel_feature_hash_includes_state_definition(native_inputs):
    from dataclasses import replace

    from terrine.frame import _feature_definition_hash
    from terrine.model import Model
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    frame = _frame(native_inputs)
    model = frame._model
    state = BinnedMetricState("sessions", (0,), ("none", "some"))
    design = Design(
        "user", Schedule("1d", "2025-01-03", "2025-01-05"), shape=StatePanel(state, max_periods=2)
    )
    altered_metrics = dict(model.metrics)
    altered_metrics["sessions"] = replace(model.metrics["sessions"], kind="conversion")
    altered_model = Model(
        catalog=model.catalog,
        con=model.con,
        readiness=model.readiness,
        units=model.units,
        event_ids=model.event_ids,
        sources=model.sources,
        metrics=altered_metrics,
        resolver=model._resolver,
        input_binding=model.input_binding,
        engine=model._engine,
    )

    assert _feature_definition_hash(design, ("from_state",), model) != _feature_definition_hash(
        design, ("from_state",), altered_model
    )


def test_state_panel_feature_hash_distinguishes_baseline_evaluation(native_inputs):
    from terrine.frame import _feature_definition_hash
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    frame = _frame(native_inputs)
    model = frame._model
    expression = agg("sessions", window=("-1d", "0d"))
    schedule = Schedule("1d", "2025-01-03", "2025-01-05")
    row_design = Design(
        "user",
        schedule,
        features={"sessions_at_start": expression},
        shape=StatePanel(BinnedMetricState("sessions", (0,), ("none", "some")), max_periods=2),
    )
    baseline_design = Design(
        "user",
        schedule,
        shape=StatePanel(
            BinnedMetricState("sessions", (0,), ("none", "some")),
            max_periods=2,
            baseline_features={"sessions_at_start": expression},
        ),
    )

    assert _feature_definition_hash(
        row_design, ("sessions_at_start",), model
    ) != _feature_definition_hash(baseline_design, ("sessions_at_start",), model)


def test_training_view_binds_split_calendar_dates(native_inputs):
    from datetime import UTC, datetime

    from terrine.dataset.splits import TimeSplit

    frame = _frame(native_inputs).training_set(
        label="churn", split=TimeSplit(train_until="2025-01-05")
    )
    assert frame._training_view is not None
    assert frame._training_view.split.train_until == datetime(2025, 1, 5, tzinfo=UTC)


def test_downstream_vocabulary_is_fitted_only_from_training_rows():
    from terrine.dataset.splits import TimeSplit, assign_splits

    rows = [
        {
            "id": "train",
            "origin": "2025-01-01T00:00:00+00:00",
            "label_end": "2025-01-02T00:00:00+00:00",
            "token": "training-only",
        },
        {
            "id": "validation",
            "origin": "2025-01-04T00:00:00+00:00",
            "label_end": "2025-01-05T00:00:00+00:00",
            "token": "validation-only",
        },
    ]
    assignments = assign_splits(
        rows,
        TimeSplit(train_until="2025-01-03T00:00:00+00:00"),
    )
    vocabulary = {row["token"] for row in rows if assignments[row["id"]].split == "train"}
    assert vocabulary == {"training-only"}


@pytest.mark.parametrize("kind", ["life_table", "sequence"])
def test_training_rejects_non_tabular_shapes_before_execution(native_inputs, kind):
    from terrine.spec import EventSequence, LifeTable, TimeToEvent, event_time
    from terrine.windows import Window

    frame = _frame(native_inputs)
    model = frame._model
    shape = (
        LifeTable(TimeToEvent("churn"), "1d")
        if kind == "life_table"
        else EventSequence("session", Window("-1d", "0s"), max_events=2, representation="arrays")
    )
    design = Design(
        "user",
        EventAnchor("session"),
        shape=shape,
        outcomes={}
        if kind == "life_table"
        else {"churn": event_time("churn", window=("0s", "1d"))},
    )
    with pytest.raises(TerrineError) as error:
        design.build(model).training_set(
            label=("duration", "event") if kind == "life_table" else "churn"
        )
    assert error.value.code == "terrine.dataset.unsupported_view"


@pytest.mark.parametrize("field", ["relation", "unit", "key", "validity", "property"])
def test_asof_feature_hash_tracks_resolved_dimension_definition(native_inputs, field):
    from dataclasses import replace

    from terrine.frame import _feature_definition_hash
    from terrine.semantic.models import (
        BoundDimension,
        BoundValidity,
        Property,
        PropertyBinding,
        TableRef,
    )
    from terrine.spec import FixedDate, asof

    model = _frame(native_inputs)._model
    prop = PropertyBinding("tier", "tier", "string", "event_time", "tier_history")
    dimension = BoundDimension(
        "tier_history",
        TableRef(kind="table", name="tiers"),
        "user",
        "user_id",
        BoundValidity("valid_from", "valid_to", None),
        {"tier": prop},
    )
    shadow = replace(dimension, name="other_history")
    declaration = Property(
        source="events",
        column="tier",
        dtype="string",
        as_of="event_time",
        dimension="tier_history",
    )
    catalog = model.catalog.model_copy(update={"properties": {"tier": declaration}})

    def bound(selected, ignored=shadow):
        sources = dict(model.sources)
        sources["events"] = replace(
            sources["events"], dimensions=(ignored, selected), properties={"tier": prop}
        )
        return Model(
            catalog=catalog,
            con=model.con,
            readiness=model.readiness,
            units=model.units,
            event_ids=model.event_ids,
            sources=sources,
            metrics=model.metrics,
            resolver=model._resolver,
            input_binding=model.input_binding,
            engine=model._engine,
        )

    changes = {
        "relation": TableRef(kind="table", name="corrected_tiers"),
        "unit": "account",
        "key": "account_id",
        "validity": BoundValidity(None, None, "changed_at"),
        "property": {"tier": replace(prop, column="corrected_tier")},
    }
    changed = replace(
        dimension, **{("properties" if field == "property" else field): changes[field]}
    )
    design = Design("user", FixedDate("2025-01-04"), features={"tier": asof("tier", before="0s")})
    original = _feature_definition_hash(design, ("tier",), bound(dimension))
    assert original != _feature_definition_hash(design, ("tier",), bound(changed))
    assert original == _feature_definition_hash(
        design, ("tier",), bound(dimension, replace(shadow, relation=changes["relation"]))
    )
