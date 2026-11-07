"""Training and scoring views, splits and fitted winsorization on DuckDB."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest

from terrine import Budget, agg
from terrine.data import Coverage, MaterializedFrame, UnitSource
from terrine.dataset.reference import _definition_for
from terrine.dataset.splits import GroupSplit, TimeSplit, group_bucket
from terrine.errors import TerrineError, thaw
from terrine.model import Model
from terrine.spec import Design, EventAnchor, gt
from terrine.spec.dataset import DatasetDefinition
from tests import _worlds
from tests.conftest import events_world as events_world  # noqa: F401
from tests.conftest import native_inputs as native_inputs  # noqa: F401
from tests.conftest import winsorization_world as winsorization_world  # noqa: F401
from tests.dataset.test_staging import _durable_frame
from tests.dataset.test_training_view import _frame
from tests.fixtures.winsorization import bind_model, design


def test_training_split_assigns_support_without_reassigning_overlap(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    base = Design(
        "user",
        EventAnchor("session"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
        outcomes={"churn": agg("churned", window=("0s", "1d"))},
    ).build(initial._model)
    training = base.training_set(
        label="churn",
        incomplete_labels="keep",
        split=TimeSplit(train_until="2025-01-03T12:00:00Z"),
    )
    ordinary = training.materialize(budget=Budget(strict_scan=False))
    assert ordinary.table["__terrine_split"].to_pylist() == ["unassigned"] * 2
    assert ordinary.table["__terrine_split_reason"].to_pylist() == ["label_overlap"] * 2
    assert ordinary.provenance.dataset_view is not None
    assert ordinary.provenance.dataset_view.counts_by_reason == {"label_overlap": 2}
    staged = training.stage("training", budget=Budget(strict_scan=False))
    actual = MaterializedFrame.from_arrow(
        pa.Table.from_batches(list(staged.to_arrow_batches(batch_size=1)))
    )
    assert actual.provenance.dataset_view == ordinary.provenance.dataset_view
    assert actual.table.sort_by("unit").equals(ordinary.table.sort_by("unit"))
    assert staged.manifest.frame_metadata["provenance"]["dataset_view"]["counts_by_reason"] == {
        "label_overlap": 2
    }
    con.disconnect()


def test_group_split_view_matches_typed_reference_and_reconstruction(events_world, tmp_path):
    initial, con = _durable_frame(events_world, tmp_path)
    policy = GroupSplit(seed=17)
    training = initial.training_set(label="churn", incomplete_labels="keep", split=policy)
    result = training.materialize(budget=Budget(strict_scan=False))
    assert result.table["__terrine_split"].to_pylist() == [
        group_bucket(key, policy) for key in result.table["unit"].to_pylist()
    ]
    staged = training.stage("groups", budget=Budget(strict_scan=False))
    rebuilt = staged.manifest.definition.build(initial._model).materialize(
        budget=Budget(strict_scan=False)
    )
    assert rebuilt.table.sort_by("unit").equals(result.table.sort_by("unit"))
    con.disconnect()


def test_scoring_today_is_resolved_once_at_catalog_day_boundary(
    native_inputs, tmp_path, monkeypatch
):
    catalog, source_con = native_inputs
    con = ibis.duckdb.connect(str(tmp_path / "scoring.duckdb"))
    for name in ("events", "labels", "users"):
        con.create_table(name, source_con.table(name).execute())
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
        units={"user": UnitSource("users", "user_id")},
    )
    design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-1d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    )
    from terrine.errors import TerrineError

    with pytest.raises(TerrineError) as error:
        design.snapshot(
            model,
            as_of="2025-01-05T00:00:00.000000001Z",
            eligibility={"active": gt(agg("sessions", window=("-1d", "0d")), 0)},
            features=("past_sessions",),
        )
    assert error.value.code == "terrine.dataset.invalid_view"
    from datetime import UTC
    from datetime import datetime as OriginalDateTime

    class FrozenDateTime(OriginalDateTime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return OriginalDateTime(2025, 1, 5, 23, 45, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr("terrine.spec.design.datetime", FrozenDateTime)
    eligibility = {"active": gt(agg("sessions", window=("-1d", "0d")), 0)}

    scoring = design.snapshot(
        model,
        as_of="today",
        eligibility=eligibility,
        features=("past_sessions",),
    )
    from terrine.frame import _feature_definition_hash

    assert _feature_definition_hash(design, ("past_sessions",), model) == _feature_definition_hash(
        scoring._design, ("past_sessions",), model
    )
    assert FrozenDateTime.calls == 1

    assert scoring.roles.outcomes == ()
    assert scoring.roles.features == ("past_sessions",)
    result = scoring.materialize()
    view = result.provenance.dataset_view
    assert view is not None
    assert view.kind == "scoring"
    assert view.selected_features == ("past_sessions",)
    assert view.selected_labels == ()
    assert view.resolved_as_of is not None
    assert view.resolved_as_of.isoformat() == "2025-01-05T00:00:00+00:00"
    assert result.roles.outcomes == ()
    assert isinstance(design.anchor, EventAnchor)
    assert "churn" in design.outcomes
    staged = scoring.stage("scoring", budget=Budget(strict_scan=False))
    assert staged.manifest.definition.view["kind"] == "scoring"
    assert staged.manifest.definition.view["features"] == ("past_sessions",)
    assert staged.manifest.definition.view["as_of"] == view.resolved_as_of.isoformat()
    assert staged.manifest.definition.view["eligibility"]["active"]
    assert staged.manifest.definition.view["parent_design_hash"] == design.spec_hash
    pandas_snapshot = staged.to_pandas()
    polars_snapshot = staged.to_polars()
    assert "unit" in pandas_snapshot.columns
    assert "unit" in polars_snapshot.columns
    assert len(pandas_snapshot) == len(polars_snapshot)
    con.disconnect()


def test_owner_prepares_scoring_view_before_preflight_without_label_inputs(
    native_inputs, tmp_path, monkeypatch
):
    import json

    import terrine.compile.dispatch as dispatch
    from terrine.dataset.reference import _definition_for
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner, PlanRequest
    from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec.dataset import DatasetDefinition

    catalog, source = native_inputs
    database = tmp_path / "scoring-owner.duckdb"
    con = ibis.duckdb.connect(str(database))
    try:
        for name in ("events", "labels", "users"):
            con.create_table(name, source.table(name).execute())
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-01-31")},
            units={"user": UnitSource("users", "user_id")},
        )
        parent = Design(
            "user",
            EventAnchor("session"),
            features={"past_sessions": agg("sessions", window=("-1d", "0s"))},
            outcomes={"churn": agg("churned", window=("0s", "1d"))},
        )
        definition = _definition_for(parent.snapshot(model, as_of="2025-01-05", eligibility={}))
        owner = RuntimeOwner.from_connection(con)
        reference = ModelReference.from_model(model, owner_key=owner.reference)
        con.drop_table("labels")
        expected = reference

        class Resolver:
            def resolve(self, reference):
                assert reference == expected
                return model

        resolved = []
        build, compile_design = DatasetDefinition.build, dispatch.compile_design

        def resolve_view(self, model):
            resolved.append(self.digest)
            return build(self, model)

        def compile_resolved(design, model):
            assert resolved, "resolve the definition view before preflight compilation"
            return compile_design(design, model)

        monkeypatch.setattr(DatasetDefinition, "build", resolve_view)
        monkeypatch.setattr(dispatch, "compile_design", compile_resolved)
        authority = SnapshotAuthority(owner)
        invocation = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority).plan(
            PlanRequest(
                "scoring",
                reference,
                definition.base_design,
                DestinationRef(str(database), "owner", "scoring"),
                budget=Budget(strict_scan=False),
                dataset_definition=definition,
            ),
            con=con.con,
            namespace="owner",
        )
        retained = authority.find_retained(invocation.snapshot_id, namespace="owner")
        assert "labels" not in json.dumps(retained.roots)
        saved = SavedPlan.from_json(invocation.saved_plan_json)
        assert saved.restore_dataset_definition() == definition
        assert definition.view["parent_design_hash"] == parent.spec_hash
        assert definition.view["as_of"] == "2025-01-05T00:00:00+00:00"
    finally:
        con.disconnect()


@pytest.fixture
def model(winsorization_world, tmp_path):
    model = _worlds.bind_model(winsorization_world, tmp_path)
    try:
        yield model
    finally:
        model.con.disconnect()


def training(model, *, split=True, keep=False):
    return (
        design()
        .build(model)
        .training_set(
            label="label",
            features=("value", "category"),
            incomplete_labels="keep" if keep else "drop",
            split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07")
            if split
            else None,
        )
    )


def test_percentile_threshold_fits_training_rows_only_and_freezes_definition(model):
    from terrine.spec.transforms import Winsorization

    frame = training(model)
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    assert fitted.lower == Decimal(5) and fitted.upper == Decimal(15)
    assert fitted.fit.row_count == fitted.fit.non_null_count == 3
    assert fitted.fit.definition_hash == _definition_for(frame).digest
    raw = frame.materialize()
    assert fitted.fit.view_hash == raw.provenance.dataset_view.view_hash
    reference = raw.table.to_pandas()
    thresholds = (
        reference.loc[reference["__terrine_split"] == "train", "value"]
        .astype("int64")
        .quantile([0.25, 0.75])
    )
    assert tuple(Decimal(str(value)) for value in thresholds) == (fitted.lower, fitted.upper)
    assert fitted.fit.split_policy["kind"] == "time"
    assert fitted.fit.snapshot["backend"] == "duckdb"
    with pytest.raises((FrozenInstanceError, AttributeError)):
        fitted.lower = Decimal(0)
    with pytest.raises(TypeError):
        fitted.fit.split_policy["kind"] = "other"
    assert Winsorization.from_json(fitted.to_json()) == fitted
    transformed = frame.winsorize(fitted)
    result = transformed.materialize()
    assert result.table.sort_by("t0")["value"].to_pylist() == [
        Decimal(5),
        Decimal(10),
        Decimal(15),
        Decimal(15),
        Decimal(15),
    ]
    definition = _definition_for(transformed)
    assert definition.digest != _definition_for(frame).digest
    restored = DatasetDefinition.from_json(definition.canonical_json())
    actual = restored.build(model).materialize()
    assert actual.table.equals(result.table)
    assert actual.columns == result.columns
    metadata = actual.columns["value"].transformation
    assert Winsorization.from_record(thaw(metadata)) == fitted
    assert metadata["fit"]["definition_hash"] == fitted.fit.definition_hash
    assert actual.columns["label"] == result.columns["label"]


def test_validation_and_test_rows_cannot_change_fit_or_population_identity(model):
    frame = training(model)
    before = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    model.con.raw_sql("UPDATE events SET amount=-999999 WHERE ts >= '2025-01-05'")
    after = training(model).fit_winsorization("value", lower="0.25", upper="0.75")
    assert (after.lower, after.upper) == (Decimal(5), Decimal(15))
    assert after.fit.population_hash == before.fit.population_hash
    model.con.raw_sql("CREATE OR REPLACE TABLE events AS SELECT * FROM events ORDER BY ts DESC")
    reordered = training(bind_model(model.con)).fit_winsorization(
        "value", lower="0.25", upper="0.75"
    )
    assert reordered.fit.population_hash == before.fit.population_hash
    model.con.raw_sql("UPDATE events SET amount=30 WHERE ts::DATE='2025-01-04'")
    changed = training(bind_model(model.con)).fit_winsorization("value", lower="0.25", upper="0.75")
    assert changed.upper == Decimal(20)
    assert changed.fit.population_hash != before.fit.population_hash


def test_fitting_requires_declared_training_split_and_exact_numeric_feature(model):
    with pytest.raises(TerrineError) as missing:
        training(model, split=False).fit_winsorization("value", lower="0", upper="1")
    assert missing.value.code == "terrine.transform.training_split_required"
    assert missing.value.context["route"]
    with pytest.raises(TerrineError) as dtype:
        training(model).fit_winsorization("category", lower="0.25", upper="0.75")
    assert dtype.value.code == "terrine.transform.winsorization_type_unsupported"
    assert dtype.value.context["route"]
    with pytest.raises(TerrineError) as labels:
        training(bind_model(model.con, label_end="2025-01-03"), keep=True).fit_winsorization(
            "value", lower="0.25", upper="0.75"
        )
    assert labels.value.code == "terrine.export.incomplete_outcome"


def test_fitted_transform_refuses_different_feature_definition(model):
    from dataclasses import replace

    from terrine.spec import agg

    fitted = training(model).fit_winsorization("value", lower="0.25", upper="0.75")
    changed = replace(design(), features={"value": agg("total", window=("-2d", "0s"))})
    with pytest.raises(TerrineError) as error:
        changed.snapshot(model, as_of="2025-01-08", features=("value",), eligibility={}).winsorize(
            fitted
        )
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"


def test_inferred_unit_snapshot_shares_identity_with_the_explicit_design(model):
    fitted = training(model).fit_winsorization("value", lower="0.25", upper="0.75")
    explicit, inferred = design(), replace(design(), unit=None)
    assert inferred.spec_hash != explicit.spec_hash
    kwargs = dict(as_of="2025-01-08", features=("value",), eligibility={})
    explicit_scored = explicit.snapshot(model, **kwargs).winsorize(fitted).materialize()
    inferred_scored = inferred.snapshot(model, **kwargs).winsorize(fitted).materialize()
    explicit_view = explicit_scored.provenance.dataset_view
    inferred_view = inferred_scored.provenance.dataset_view
    assert inferred_view is not None and explicit_view is not None
    assert inferred_view.view_hash == explicit_view.view_hash
    assert inferred_scored.table.equals(explicit_scored.table)
    definition = _definition_for(inferred.snapshot(model, **kwargs))
    assert definition.view["parent_design_hash"] == explicit.spec_hash
    assert definition.digest == _definition_for(explicit.snapshot(model, **kwargs)).digest


def test_staged_definition_records_the_resolved_unit(events_world, tmp_path):
    from terrine import recipes

    bound = _worlds.bind_model(events_world, tmp_path)
    features = {"sessions": agg("sessions", window=("-7d", "0s"))}
    explicit = recipes.time_series(
        "user", every="1d", start="2025-01-02", end="2025-01-03", features=features
    )
    inferred = recipes.time_series(
        every="1d", start="2025-01-02", end="2025-01-03", features=features
    )
    dataset = inferred.build(bound).stage("inferred", budget=Budget(strict_scan=False))
    try:
        definition = dataset.manifest.definition
        assert definition.base_design.unit == "user"
        assert definition.base_design == explicit
        assert definition.digest == _definition_for(explicit.build(bound)).digest
        assert dataset.manifest.design_hash == explicit.spec_hash
    finally:
        bound.con.disconnect()


def test_independently_fitted_features_compose_and_reopen(model):
    from dataclasses import replace

    from terrine.spec import agg

    study = replace(
        design(),
        features={
            "value": agg("total", window=("-1d", "0d")),
            "other": agg("total", window=("-1d", "0d")),
        },
    )
    frame = study.build(model).training_set(
        label="label",
        features=("value", "other"),
        split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
    )
    first = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    second = frame.fit_winsorization("other", lower="0.25", upper="0.75")
    transformed = frame.winsorize(first).winsorize(second)
    definition = DatasetDefinition.from_json(_definition_for(transformed).canonical_json())
    result = definition.build(model).materialize()
    assert definition.transforms == (first, second)
    assert result.table.sort_by("t0")["value"].to_pylist() == [
        Decimal(value) for value in (5, 10, 15, 15, 15)
    ]
    assert result.table["other"].equals(result.table["value"])
    assert first.fit.definition_hash == second.fit.definition_hash == _definition_for(frame).digest


@pytest.mark.parametrize("kind", ("quantile", "approximate_distinct"))
def test_fitted_transform_refuses_same_typed_rebound_metric(model, kind):
    from dataclasses import replace

    from terrine import Coverage, Model, UnitSource
    from terrine.semantic.models import Catalog
    from terrine.spec import approx_distinct, quantile

    model.con.raw_sql("ALTER TABLE events ADD COLUMN other_amount BIGINT DEFAULT 7")
    definitions = model.catalog.model_dump(mode="python")
    definitions["sources"]["events"]["events"] = (
        *definitions["sources"]["events"]["events"],
        "other_amount",
    )
    definitions["events"]["other_amount"] = {"source": "events", "column": "other_amount"}

    def bind():
        return Model.from_catalog(
            Catalog.model_validate(definitions),
            con=model.con,
            units={"user": UnitSource("users", "user_id")},
            readiness={name: Coverage("2025-01-01", "2025-01-12") for name in ("events", "labels")},
        )

    expression = (
        quantile("total", "0.5", window=("-1d", "0d"))
        if kind == "quantile"
        else approx_distinct("total", window=("-1d", "0d"))
    )
    study = replace(design(), features={"value": expression})
    original = bind()
    frame = study.build(original).training_set(
        label="label",
        split=TimeSplit(train_until="2025-01-06", validation_until="2025-01-07"),
    )
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    definitions["measures"]["total"]["event"] = "other_amount"
    rebound = study.snapshot(bind(), as_of="2025-01-08", features=("value",), eligibility={})
    assert rebound.expr.schema()["value"] == frame._active_compiled().table.schema()["value"]
    with pytest.raises(TerrineError) as error:
        rebound.winsorize(fitted)
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"


@pytest.mark.parametrize(
    ("binding", "nested", "feature_kind"),
    (
        ("source", False, "aggregate"),
        ("event", True, "aggregate"),
        ("property", True, "attribute"),
        ("dimension", True, "aggregate"),
    ),
)
def test_frozen_limits_refuse_rebound_selector_dependencies(model, binding, nested, feature_kind):
    from dataclasses import replace

    from terrine import Coverage, Model, UnitSource
    from terrine.frame import _feature_definition_hash
    from terrine.semantic.models import Catalog
    from terrine.spec import Schedule, agg, asof, event_time

    model.con.raw_sql(
        "CREATE TABLE selection_events AS SELECT user_id,ts,1::BIGINT AS selected,"
        "NULL::BIGINT AS alternate,'keep' AS kind,'drop' AS other_kind FROM events; "
        "CREATE TABLE alternate_selection_events AS "
        "SELECT * REPLACE(ts+INTERVAL 6 HOUR AS ts) FROM selection_events"
    )
    definitions = model.catalog.model_dump(mode="python")
    definitions["sources"]["selection"] = {
        "relation": {"kind": "table", "name": "selection_events"},
        "time": "ts",
        "units": {"user": "user_id"},
        "events": ("selection",),
        "properties": ("selection_kind",),
        "dimensions": (),
    }
    definitions["events"]["selection"] = {"source": "selection", "column": "selected"}
    definitions["properties"]["selection_kind"] = {
        "source": "selection",
        "column": "kind",
        "dtype": "string",
        "as_of": "event_time",
    }
    definitions["sources"]["events"]["properties"] = ("category", "amount_value")
    definitions["properties"]["amount_value"] = {
        "source": "events",
        "column": "amount",
        "dtype": "int",
        "as_of": "event_time",
    }
    if binding == "dimension":
        model.con.raw_sql(
            "CREATE TABLE selection_history AS SELECT user_id,'keep' AS kind,"
            "TIMESTAMPTZ '2025-01-01' AS starts,TIMESTAMPTZ '2025-01-12' AS ends FROM users; "
            "CREATE TABLE alternate_selection_history AS "
            "SELECT * REPLACE('drop' AS kind) FROM selection_history"
        )
        definitions["sources"]["selection"]["dimensions"] = ("selection_history",)
        definitions["properties"]["selection_kind"]["dimension"] = "selection_history"
        definitions["dimensions"]["selection_history"] = {
            "relation": {"kind": "table", "name": "selection_history"},
            "unit": "user",
            "key": "user_id",
            "properties": ("selection_kind",),
            "validity": {"valid_from": "starts", "valid_to": "ends"},
        }

    def bind():
        return Model.from_catalog(
            Catalog.model_validate(definitions),
            con=model.con,
            units={"user": UnitSource("users", "user_id")},
            readiness={
                name: Coverage("2025-01-01", "2025-01-12") for name in definitions["sources"]
            },
        )

    selector = event_time(
        "selection", window=("-2d", "-1d"), label_property="selection_kind", equals="keep"
    )
    if nested:
        selector = event_time("amount", window=("0s", "1h"), relative_to=selector)
    expression = (
        agg("total", window=("-1d", "0s"), relative_to=selector)
        if feature_kind == "aggregate"
        else asof("amount_value", before="0s", relative_to=selector)
    )
    study = replace(
        design(), anchor=Schedule("1d", "2025-01-05", "2025-01-07"), features={"value": expression}
    )
    split = TimeSplit(train_until="2025-01-07", validation_until="2025-01-08")
    original = bind()
    frame = study.build(original).training_set(label="label", split=split)
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    assert fitted.fit.non_null_count == 2
    original_score = study.snapshot(
        original, as_of="2025-01-08", features=("value",), eligibility={}
    )
    original_score.winsorize(fitted)
    assert fitted.fit.feature_definition_hash == _feature_definition_hash(
        original_score._design, ("value",), original
    )
    if binding == "source":
        definitions["sources"]["selection"]["relation"]["name"] = "alternate_selection_events"
    elif binding == "event":
        definitions["events"]["selection"]["column"] = "alternate"
    elif binding == "property":
        definitions["properties"]["selection_kind"]["column"] = "other_kind"
    else:
        definitions["dimensions"]["selection_history"]["relation"]["name"] = (
            "alternate_selection_history"
        )
    rebound = bind()
    candidates = (
        study.build(rebound).training_set(label="label", split=split),
        study.snapshot(rebound, as_of="2025-01-08", features=("value",), eligibility={}),
    )
    for candidate in candidates:
        assert candidate.expr.schema()["value"] == frame.expr.schema()["value"]
        with pytest.raises(TerrineError) as error:
            candidate.winsorize(fitted)
        assert error.value.code == "terrine.transform.winsorization_identity_mismatch"
        assert _feature_definition_hash(candidate._design, ("value",), rebound) != (
            fitted.fit.feature_definition_hash
        )
    timestamp_study = replace(study, features={"selected_at": selector})
    assert _feature_definition_hash(timestamp_study, ("selected_at",), original) != (
        _feature_definition_hash(timestamp_study, ("selected_at",), rebound)
    )


def test_incomplete_labels_drop_rows_and_keep_null_reasons(native_inputs):
    frame = _frame(native_inputs, label_coverage_end="2025-01-01", missing_label=True)
    dropped = frame.training_set(label="churn", incomplete_labels="drop").materialize()
    kept = frame.training_set(label="churn", incomplete_labels="keep").materialize()
    assert dropped.table.num_rows == 0, (kept.columns["churn"], kept.table.to_pylist())
    assert kept.table.num_rows == 2
    assert kept.table["churn"].null_count == 2
    reason_column = kept.columns["churn"].reason_column
    assert reason_column is not None
    assert kept.table[reason_column].to_pylist() == ["incomplete_coverage", "incomplete_coverage"]
    assert dropped.provenance.dataset_view.counts_by_reason == {"incomplete_coverage": 2}


def test_training_projection_preserves_baseline_treatment_role(native_inputs):
    from terrine.semantic.models import Catalog
    from terrine.spec import FixedDate, Treatment, asof

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN enabled BOOLEAN")
    con.raw_sql("UPDATE events SET enabled = (user_id = 'a')")
    definitions = catalog.model_dump(mode="json")
    definitions["sources"]["events"]["properties"] = ["enabled"]
    definitions["properties"]["enabled"] = {
        "source": "events",
        "column": "enabled",
        "dtype": "bool",
        "as_of": "event_time",
    }
    model = Model.from_catalog(
        Catalog.model_validate(definitions),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    frame = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"enabled": asof("enabled", before="0s")},
        outcomes={"future": agg("sessions", window=("0s", "1d"))},
        treatment=Treatment("enabled", known_at_t0=True),
    ).build(model)

    materialized = frame.training_set(label="future", features=("enabled",)).materialize()
    assert materialized.roles.treatment == "treatment"
    assert "treatment" in materialized.table.column_names
    treatment = materialized.columns["treatment"]
    for support in (
        treatment.observed_from_column,
        treatment.observed_until_column,
        treatment.complete_column,
        treatment.reason_column,
    ):
        if support is not None:
            assert support in materialized.table.column_names


def test_training_view_hashes_selected_feature_definition_and_parent_design(native_inputs):
    from terrine.spec import EventAnchor

    frame = _frame(native_inputs)
    first = frame.training_set(label="churn", features=("past_sessions",)).materialize()
    second = frame.training_set(label="churn", features=("earlier_sessions",)).materialize()
    assert (
        first.provenance.dataset_view.feature_definition_hash
        != second.provenance.dataset_view.feature_definition_hash
    )

    changed_design = Design(
        "user",
        EventAnchor("session"),
        features={"past_sessions": agg("sessions", window=("-3d", "0d"))},
        outcomes={"churn": agg("churned", window=("0d", "1d"))},
    ).build(frame._model)
    changed = changed_design.training_set(label="churn", features=("past_sessions",)).materialize()
    first_view = first.provenance.dataset_view
    changed_view = changed.provenance.dataset_view
    assert first_view is not None and changed_view is not None
    assert first_view.view_hash != changed_view.view_hash


def test_state_panel_training_view_hashes_baseline_and_prior_state(native_inputs):
    from decimal import Decimal

    from terrine.data import Budget
    from terrine.spec import BinnedMetricState, Schedule, StatePanel

    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    frame = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(
            BinnedMetricState("sessions", (Decimal("0"),), ("none", "some")),
            periods=2,
            baseline_features={"baseline": agg("sessions", window=("-1d", "0d"))},
        ),
    ).build(model)
    result = frame.training_set(
        label="to_state",
        features=("baseline", "from_state"),
        incomplete_labels="keep",
    ).materialize(budget=Budget(max_rows=100, max_output_bytes=100_000))
    assert result.roles.features == ("baseline", "from_state")
    assert result.provenance.dataset_view is not None
    assert result.provenance.state_panel is not None
    assert result.provenance.state_panel.period_column in result.table.column_names
    prior = result.columns["from_state"]
    for support in (prior.complete_column, prior.reason_column):
        if support is not None:
            assert support in result.table.column_names


def test_feature_definition_hash_tracks_source_timestamp_semantics(native_inputs):
    from terrine.frame import _feature_definition_hash
    from terrine.semantic.models import Catalog
    from terrine.spec import FixedDate

    catalog, con = native_inputs
    con.raw_sql("ALTER TABLE events ADD COLUMN earlier TIMESTAMPTZ")
    con.raw_sql("UPDATE events SET earlier=ts-INTERVAL '1 day'")
    units = {"user": UnitSource("users", "user_id")}
    readiness = {"events": Coverage("2025-01-01", "2025-01-31")}
    original = Model.from_catalog(catalog, con=con, units=units, readiness=readiness)
    changed = catalog.model_dump(mode="json")
    changed["sources"]["events"]["time"] = "earlier"
    corrected = Model.from_catalog(
        Catalog.model_validate(changed), con=con, units=units, readiness=readiness
    )
    design = Design(
        "user",
        FixedDate("2025-01-04"),
        features={"sessions": agg("sessions", window=("-1d", "0s"))},
    )
    assert design.build(original).materialize().table["sessions"].to_pylist() == [1, 1]
    assert design.build(corrected).materialize().table["sessions"].to_pylist() == [0, 0]
    assert _feature_definition_hash(design, ("sessions",), original) != _feature_definition_hash(
        design, ("sessions",), corrected
    )
