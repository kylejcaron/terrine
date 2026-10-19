from dataclasses import FrozenInstanceError
from decimal import Decimal, localcontext

import ibis
import pytest

from terrine.dataset.reference import _recipe_for
from terrine.dataset.splits import TimeSplit
from terrine.errors import TerrineError
from terrine.spec.dataset import DatasetRecipe
from tests.fixtures.winsorization import bind_model, create_model, design


@pytest.fixture
def model():
    con = ibis.duckdb.connect()
    try:
        yield create_model(con)
    finally:
        con.disconnect()


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


def test_percentile_threshold_fits_training_rows_only_and_freezes_recipe(model):
    from terrine.spec.transforms import Winsorization

    frame = training(model)
    fitted = frame.fit_winsorization("value", lower="0.25", upper="0.75")
    assert fitted.lower == Decimal(5) and fitted.upper == Decimal(15)
    assert fitted.fit.row_count == fitted.fit.non_null_count == 3
    assert fitted.fit.recipe_hash == _recipe_for(frame).digest
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
    assert result.table["value"].to_pylist() == [
        Decimal(5),
        Decimal(10),
        Decimal(15),
        Decimal(15),
        Decimal(15),
    ]
    recipe = _recipe_for(transformed)
    assert recipe.digest != _recipe_for(frame).digest
    restored = DatasetRecipe.from_json(recipe.canonical_json())
    actual = restored.build(model).materialize()
    assert actual.table.equals(result.table)
    assert actual.columns == result.columns
    metadata = actual.columns["value"].transformation
    assert metadata["stage"] == "post_aggregation"
    assert metadata["fit"]["recipe_hash"] == fitted.fit.recipe_hash
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


def test_fitted_limits_are_exact_under_small_decimal_context(model):
    model.con.raw_sql(
        "ALTER TABLE events ALTER amount TYPE DECIMAL(38,2); "
        "UPDATE events SET amount=CASE ts::DATE "
        "WHEN DATE '2025-01-02' THEN '1000000000000000000000000000000.01'::DECIMAL(38,2) "
        "WHEN DATE '2025-01-03' THEN '1000000000000000000000000000000.03'::DECIMAL(38,2) "
        "ELSE NULL END"
    )
    with localcontext() as context:
        context.prec = 6
        fitted = training(bind_model(model.con)).fit_winsorization(
            "value", lower="0.25", upper="0.75"
        )
    assert fitted.lower == Decimal("1000000000000000000000000000000.015")
    assert fitted.upper == Decimal("1000000000000000000000000000000.025")
    assert fitted.fit.non_null_count == 2


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
    recipe = DatasetRecipe.from_json(_recipe_for(transformed).canonical_json())
    result = recipe.build(model).materialize()
    assert recipe.transforms == (first, second)
    assert result.table["value"].to_pylist() == [Decimal(value) for value in (5, 10, 15, 15, 15)]
    assert result.table["other"].equals(result.table["value"])
    assert first.fit.recipe_hash == second.fit.recipe_hash == _recipe_for(frame).digest


@pytest.mark.parametrize("kind", ("quantile", "approximate_distinct"))
def test_fitted_transform_refuses_same_typed_rebound_metric(model, kind):
    from dataclasses import replace

    from terrine import Coverage, Model, UnitSource
    from terrine.semantic.models import Catalog
    from terrine.spec import approx_distinct, quantile

    model.con.raw_sql("ALTER TABLE events ADD COLUMN other_amount BIGINT DEFAULT 7")
    definitions = model.catalog.model_dump(mode="python")
    definitions["sources"]["events"]["facts"] = (
        *definitions["sources"]["events"]["facts"],
        "other_amount",
    )
    definitions["facts"]["other_amount"] = {"source": "events", "column": "other_amount"}

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
    definitions["measures"]["total"]["fact"] = "other_amount"
    rebound = study.snapshot(bind(), as_of="2025-01-08", features=("value",), eligibility={})
    assert rebound.expr.schema()["value"] == frame._active_compiled().table.schema()["value"]
    with pytest.raises(TerrineError) as error:
        rebound.winsorize(fitted)
    assert error.value.code == "terrine.transform.winsorization_identity_mismatch"
