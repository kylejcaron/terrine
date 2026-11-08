import copy
import inspect
import pickle
from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pytest

from terrine import recipes
from terrine.data import BoundCoverage, Budget, Coverage, Roles, UnitSource, output_sort_keys
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import (
    CohortGrouping,
    CohortSpec,
    Design,
    EventAnchor,
    EventSequence,
    Initiation,
    Trajectory,
    agg,
)
from terrine.windows import Window
from tests.fixtures.readme import readme_design, readme_model


def test_materialize_budget_is_keyword_only():
    from terrine.frame import Frame

    parameter = inspect.signature(Frame.materialize).parameters["budget"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    ("names", "roles", "shape", "expected"),
    [
        (
            (
                "age",
                "trial_id",
                "event_period",
                "period_index",
                "sequence_position",
                "t0",
                "anchor_id",
                "unit",
                "value",
            ),
            Roles(
                unit="unit",
                anchor="anchor_id",
                t0="t0",
                treatment=None,
                features=("value",),
                outcomes=(),
            ),
            "cross_section",
            ("unit", "t0", "anchor_id"),
        ),
        (
            ("unit", "cohort_time", "age"),
            Roles(
                unit="unit",
                anchor="anchor_id",
                t0="cohort_time",
                treatment=None,
                features=(),
                outcomes=(),
            ),
            "ltv",
            ("unit", "cohort_time", "age"),
        ),
        (
            ("unit", "anchor_id", "t0", "region", "segment", "time_bin", "events"),
            Roles(
                unit="unit",
                anchor="anchor_id",
                t0="t0",
                treatment=None,
                features=("region", "segment", "time_bin"),
                outcomes=("events",),
            ),
            "life_table",
            ("region", "segment", "time_bin"),
        ),
        (
            ("unit", "anchor_id", "t0", "time_bin"),
            Roles(
                unit="unit",
                anchor="anchor_id",
                t0="t0",
                treatment=None,
                features=("time_bin",),
                outcomes=(),
            ),
            "life_table",
            ("time_bin",),
        ),
        (
            ("unit", "anchor_id", "t0", "time_bin"),
            Roles(
                unit="unit",
                anchor="anchor_id",
                t0="t0",
                treatment=None,
                features=("time_bin",),
                outcomes=(),
            ),
            "cross_section",
            ("unit", "t0", "anchor_id"),
        ),
        (
            ("value",),
            Roles(
                unit="identity",
                anchor="identity",
                t0="origin",
                treatment=None,
                features=(),
                outcomes=("value",),
            ),
            "cross_section",
            (),
        ),
    ],
)
def test_output_sort_keys_use_present_identity_or_shape_columns(names, roles, shape, expected):
    table = pa.table({name: pa.array([], type=pa.int64()) for name in names})
    assert output_sort_keys(table.schema, roles, shape=shape) == expected


def test_unused_label_source_readiness_is_not_a_model_requirement(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    Design(
        "user", EventAnchor("session"), features={"past": agg("sessions", window=("-1d", "0d"))}
    ).build(model)
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), outcomes={"future": agg("churned", window=("0d", "1d"))}
        ).build(model)
    assert error.value.code == "terrine.readiness.missing_source"
    assert error.value.context["source"] == "labels"


def test_model_hash_tracks_observation_and_identity_bindings(native_inputs):
    catalog, con = native_inputs
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}

    def bind(**changes):
        return Model.from_catalog(
            catalog,
            con=con,
            readiness=changes.get("readiness", readiness),
            units=changes.get("units", {"user": UnitSource("users", "user_id")}),
            event_ids=changes.get("event_ids", {"events": "event_id"}),
        )

    base = bind().model_hash
    assert (
        bind(readiness={**readiness, "events": Coverage("2025-01-02", "2025-01-31")}).model_hash
        != base
    )
    assert bind(units={"user": UnitSource("users", "alt_id")}).model_hash != base
    assert bind(event_ids={"events": "alternate_event_id"}).model_hash != base
    offset = timezone(timedelta(hours=-5))
    equivalent = BoundCoverage(
        datetime(2024, 12, 31, 19, tzinfo=offset),
        datetime(2025, 1, 31, 19, tzinfo=offset),
    )
    assert bind(readiness={**readiness, "events": equivalent}).model_hash == base


@pytest.mark.parametrize("budget", [None, Budget(max_rows=1), Budget(max_output_bytes=1024)])
def test_sequence_materialization_requires_positive_row_and_byte_limits(native_inputs, budget):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources},
        units={"user": UnitSource("users", "user_id")},
    )
    frame = Design(
        "user",
        EventAnchor("session"),
        shape=EventSequence("session", Window("0s", "1d"), max_events=2),
    ).build(model)
    with pytest.raises(TerrineError) as error:
        frame.materialize(budget=budget)
    assert error.value.code == "terrine.sequence.budget_required"


def test_inferred_unit_frame_shares_the_explicit_design_identity(native_inputs):
    catalog, con = native_inputs
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}
    features = {"past": agg("sessions", window=("-1d", "0d"))}
    explicit = Design("user", EventAnchor("session"), features=features)
    inferred = Design(None, EventAnchor("session"), features=features)
    one = Model.from_catalog(
        catalog, con=con, readiness=readiness, units={"user": UnitSource("users", "user_id")}
    )
    result = inferred.build(one).materialize()
    assert result.provenance.design_hash == explicit.spec_hash
    expected = explicit.build(one).materialize().table
    assert result.table.sort_by("unit").equals(expected.sort_by("unit"))
    two = Model.from_catalog(
        catalog,
        con=con,
        readiness=readiness,
        units={"user": UnitSource("users", "user_id"), "account": UnitSource("users", "alt_id")},
    )
    with pytest.raises(TerrineError) as error:
        inferred.build(two)
    assert error.value.code == "terrine.model.ambiguous_unit"
    assert error.value.context["choices"] == ("account", "user")
    assert explicit.build(two).materialize().provenance.design_hash == explicit.spec_hash


def test_values_only_keeps_roles_and_values_and_drops_support(readme_con):
    frame = readme_design().build(readme_model(readme_con))
    data = frame.to_pandas(values_only=True)
    assert list(data.columns) == [
        "unit",
        "anchor_id",
        "t0",
        "orders_30d",
        "spend_30d",
        "spend_next_30d",
    ]
    assert len(data) == 6
    march = data[data["t0"].dt.month == 3]
    assert len(march) == 3
    assert march["spend_next_30d"].isna().all()
    assert data[data["t0"].dt.month == 2]["spend_next_30d"].notna().all()
    with pytest.raises(TerrineError) as error:
        frame.to_pandas(values_only=True, include_observation_metadata=True)
    assert error.value.code == "terrine.dataset.invalid_view"
    assert error.value.context["reason"] == "values_only_excludes_observation_metadata"
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.code == error.value.code
        assert restored.context == error.value.context
    assert error.value.context["conflicts"] == ("values_only", "include_observation_metadata")
    with pytest.raises(TerrineError) as error:
        frame.to_polars(values_only=True, include_observation_metadata=True)
    assert error.value.context["conflicts"] == ("values_only", "include_observation_metadata")
    assert error.value.code == "terrine.dataset.invalid_view"
    assert error.value.context["reason"] == "values_only_excludes_observation_metadata"


def test_to_polars_values_only_has_the_pandas_columns(readme_con):
    pytest.importorskip("polars")
    frame = readme_design().build(readme_model(readme_con))
    data = frame.to_polars(values_only=True)
    assert data.columns == list(frame.to_pandas(values_only=True).columns)
    assert data.height == 6
    assert data["spend_next_30d"].null_count() == 3


def test_values_only_on_shapes_keeps_structural_columns(readme_con, native_inputs):
    ltv = (
        recipes.ltv(start_event="orders", metric="spend", period="7d", max_periods=3)
        .build(readme_model(readme_con))
        .to_pandas(values_only=True)
    )
    assert list(ltv.columns) == [
        "unit",
        "anchor_id",
        "cohort_time",
        "age",
        "period_value",
        "period_value_mature",
        "cumulative_value",
    ]
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
        event_ids={"events": "event_id"},
    )
    frame = recipes.trial_panel(
        "user",
        every="1d",
        start="2025-01-03",
        end="2025-01-04",
        treatment=Initiation("session", history_start=datetime(2025, 1, 1, tzinfo=UTC)),
    ).build(model)
    panel = frame.to_pandas(values_only=True)
    roles = frame.roles
    assert list(panel.columns) == ["unit", "anchor_id", "t0", "treatment", "trial_id"]
    assert {roles.unit, roles.cluster_unit, roles.treatment, roles.trial} <= set(panel.columns)


@pytest.mark.parametrize(
    ("build", "value_column", "context"),
    [
        (readme_design, "period_value", {"reason": "requires_trajectory", "field": "frame"}),
        (
            lambda: recipes.ltv(start_event="orders", metric="spend", period="7d", max_periods=2),
            "total",
            {"reason": "unsupported_value", "field": "value_column"},
        ),
        (
            lambda: Design(
                "customer",
                EventAnchor("orders"),
                shape=CohortSpec("orders", Trajectory("spend", "7d", 2)),
            ),
            "cumulative_value",
            {"reason": "not_cumulative", "field": "value_column"},
        ),
    ],
)
def test_cohort_requests_refuse_with_codes(readme_con, build, value_column, context):
    frame = build().build(readme_model(readme_con))
    grouping = CohortGrouping("7d", datetime(2025, 1, 1, tzinfo=UTC))
    with pytest.raises(TerrineError) as error:
        frame.cohorts(grouping=grouping, value_column=value_column)
    assert error.value.code == "terrine.cohort.invalid_operation"
    assert {key: error.value.context[key] for key in context} == context


def test_conflicting_view_flags_refuse_before_frame_materialization(readme_con, monkeypatch):
    frame = readme_design().build(readme_model(readme_con))

    def fail_materialization(*args, **kwargs):
        raise AssertionError("materialization should not run")

    monkeypatch.setattr(type(frame), "materialize", fail_materialization)
    with pytest.raises(TerrineError) as error:
        frame.to_pandas(values_only=True, include_observation_metadata=True)

    assert error.value.code == "terrine.dataset.invalid_view"
    assert error.value.context["reason"] == "values_only_excludes_observation_metadata"
