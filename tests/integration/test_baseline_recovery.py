"""End-to-end parity for independently generated and natively compiled worlds."""

from __future__ import annotations

from fractions import Fraction
from importlib.resources import files
from tempfile import TemporaryDirectory

import ibis
import numpy as np
import pytest

import terrine as tr
from terrine.estimate import aipw, ipw, regression_adjustment
from terrine.simulate import WorldConfig, simulate
from terrine.simulate.truth import ate
from tests.fixtures.baseline import materialized_baseline
from tests.reference.baseline import reference_rows
from tests.reference.estimation import exact_score

_ESTIMATORS = (ipw, aipw, regression_adjustment)


def _compile(world, con, catalog_path):
    con.create_table("world_roster", world.roster)
    con.create_table("world_events", world.events)
    con.create_table("world_attributes", world.attributes)
    model = tr.Model.from_catalog(
        catalog_path,
        con=con,
        readiness=world.readiness,
        units=world.units,
        event_ids=world.event_ids,
    )
    days = world.config.horizon_seconds // 86400
    design = tr.Design(
        "user",
        tr.FixedDate(world.config.t0),
        treatment=tr.Treatment("baseline_enabled", at="0d", known_at_t0=True),
        features={
            "x": tr.asof("engagement_score", before="0d"),
            "plan_tier": tr.asof("plan_tier", before="0d"),
        },
        outcomes={"net_revenue": tr.agg("net_revenue", window=("0d", f"{days}d"))},
    )
    return design.build(model).materialize()


@pytest.fixture
def native_world():
    world = simulate(WorldConfig(n_units=240), seed=231)
    con = ibis.duckdb.connect()
    try:
        with TemporaryDirectory(prefix="terrine-baseline-") as directory:
            catalog_path = f"{directory}/baseline.yaml"
            with open(catalog_path, "w", encoding="utf-8") as stream:
                stream.write(files("terrine.simulate").joinpath("baseline.yaml").read_text())
            yield world, _compile(world, con, catalog_path)
    finally:
        con.disconnect()


def test_compiled_baseline_matches_independent_raw_rows(native_world):
    world, compiled = native_world
    direct = reference_rows(world)
    compiled_units = compiled.table.column(compiled.roles.unit).to_pylist()
    direct_units = direct.column("unit_id").to_pylist()
    assert set(compiled_units) == set(direct_units)
    compiled_order = sorted(range(len(compiled_units)), key=compiled_units.__getitem__)
    direct_by_unit = {unit: index for index, unit in enumerate(direct_units)}
    direct_order = [direct_by_unit[compiled_units[index]] for index in compiled_order]
    for role, reference in (
        (compiled.roles.treatment, "baseline_enabled"),
        ("x", "x"),
        ("plan_tier", "plan_tier"),
        ("net_revenue", "net_revenue"),
    ):
        got = compiled.table.column(role).to_pylist()
        expected = direct.column(reference).to_pylist()
        np.testing.assert_equal(
            [got[index] for index in compiled_order],
            [expected[index] for index in direct_order],
        )
    for name, reference_name in (
        ("x", "x"),
        ("plan_tier", "plan_tier"),
        ("treatment", "baseline_enabled"),
        ("net_revenue", "net_revenue"),
    ):
        metadata = compiled.columns[name]
        for column, reference in (
            (metadata.complete_column, f"{reference_name}__complete"),
            (metadata.reason_column, f"{reference_name}__reason"),
            (metadata.observed_from_column, f"{reference_name}__observed_from"),
            (metadata.observed_until_column, f"{reference_name}__observed_until"),
        ):
            got = compiled.table.column(column).to_pylist()
            expected = direct.column(reference).to_pylist()
            assert [got[index] for index in compiled_order] == [
                expected[index] for index in direct_order
            ]
    assert compiled.provenance.feature_cutoffs["x"].strict
    assert compiled.provenance.feature_cutoffs["plan_tier"].strict


def test_independent_array_nuisance_equation_reference():
    from tests.reference.estimation import logistic_fit, weighted_mean

    x = np.tile(np.asarray([-1.0, 0.0, 1.0]), 200)
    beta = np.asarray([-0.2, 0.7])
    probability = 1 / (1 + np.exp(-(beta[0] + beta[1] * x)))
    treatment = np.random.default_rng(405).binomial(1, probability)
    design = np.column_stack((np.ones(len(x)), x))
    fitted = logistic_fit(x, treatment)
    score = design.T @ (1 / (1 + np.exp(-(design @ fitted))) - treatment)
    np.testing.assert_allclose(score / len(x), np.zeros(2), atol=1e-8, rtol=0)
    assert weighted_mean([1.0, 3.0], [1.0, 3.0]) == pytest.approx(2.5)


def test_exact_fraction_scores_match_target_normalized_formulas():
    arguments = {
        "treatment": [1, 0],
        "outcome": [4, 2],
        "propensity": [Fraction(1, 2), Fraction(1, 4)],
        "mu0": [3, 3],
        "mu1": [4, 4],
        "weight": [2, 1],
    }
    assert exact_score("ipw", **arguments) == Fraction(40, 9)
    assert exact_score("regression_adjustment", **arguments) == Fraction(1, 1)
    assert exact_score("aipw", **arguments) == Fraction(13, 9)


def test_native_catalog_object_and_yaml_bind_same_baseline():
    from terrine.semantic.io import load_catalog

    world = simulate(WorldConfig(n_units=90), seed=235)
    with TemporaryDirectory(prefix="terrine-catalog-parity-") as directory:
        catalog_path = f"{directory}/baseline.yaml"
        with open(catalog_path, "w", encoding="utf-8") as stream:
            stream.write(files("terrine.simulate").joinpath("baseline.yaml").read_text())
        catalog = load_catalog(catalog_path)
        path_connection = ibis.duckdb.connect()
        object_connection = ibis.duckdb.connect()
        try:
            from_yaml = _compile(world, path_connection, catalog_path)
            from_object = _compile(world, object_connection, catalog)
            assert from_yaml.roles == from_object.roles
            assert from_yaml.columns == from_object.columns
            assert from_yaml.provenance.feature_cutoffs == from_object.provenance.feature_cutoffs
            assert from_yaml.provenance.treatment_design == from_object.provenance.treatment_design
            assert from_yaml.table.equals(from_object.table)
            assert world.units["user"].key == "unit_id"
            assert from_yaml.roles.treatment == "treatment"
            assert from_yaml.provenance.feature_cutoffs["x"].strict
            assert from_yaml.table.num_rows == world.config.n_units
        finally:
            path_connection.disconnect()
            object_connection.disconnect()


@pytest.mark.parametrize("estimator", _ESTIMATORS)
def test_direct_and_compiled_estimation_match(native_world, estimator):
    world, compiled = native_world
    direct = materialized_baseline(world)
    got = estimator(compiled, outcome="net_revenue", folds=5, seed=17)
    ref = estimator(direct, outcome="net_revenue", folds=5, seed=17)
    np.testing.assert_allclose(
        [got.value, got.se, *got.ci],
        [ref.value, ref.se, *ref.ci],
        rtol=1e-8,
        atol=1e-10,
    )
    assert got.estimand == ref.estimand == "ATE"
    assert got.weighting == ref.weighting == "unit"
    assert got.n == got.n_clusters == world.config.n_units


def test_noiseless_paired_counterfactual_identity():
    world = simulate(WorldConfig(n_units=40, outcome_noise_sd=0), seed=19)
    oracle = world.oracle
    np.testing.assert_allclose(
        np.asarray(oracle["y1"]) - np.asarray(oracle["y0"]),
        2 + 0.75 * np.asarray(oracle["x"]) + 0.5 * (np.asarray(oracle["plan_tier"]) == "plus"),
        rtol=0,
        atol=1e-14,
    )
    assert ate(world, target="cohort") == pytest.approx(
        np.mean(np.asarray(oracle["y1"]) - np.asarray(oracle["y0"]))
    )
