from dataclasses import replace
from math import fsum

import numpy as np
import pytest

from terrine.simulate import WorldConfig, simulate
from terrine.simulate.truth import ate


def test_observed_outcome_selects_the_assigned_potential_outcome():
    world = simulate(WorldConfig(n_units=64, outcome_noise_sd=0), seed=17)
    oracle = world.oracle
    assigned = np.asarray(world.analysis_table["baseline_enabled"]).astype(bool)
    observed = np.asarray(world.analysis_table["net_revenue"])
    np.testing.assert_array_equal(
        observed, np.where(assigned, np.asarray(oracle["y1"]), np.asarray(oracle["y0"]))
    )


def test_noise_zero_has_analytical_potential_outcomes():
    config = WorldConfig(
        n_units=32,
        outcome_noise_sd=0,
        propensity_quadratic=1.5,
        outcome_quadratic=2,
        target_weight_slope=0.25,
    )
    oracle = simulate(config, seed=9).oracle.to_pydict()
    x = np.asarray(oracle["x"])
    plus = np.asarray(oracle["plan_tier"]) == "plus"
    enterprise = np.asarray(oracle["plan_tier"]) == "enterprise"
    h = x * x - 1 / 3
    eta = -0.2 + 0.8 * x + 0.5 * plus - 0.4 * enterprise + 1.5 * h
    mu0 = 10 + 2 * x + 1.5 * plus - enterprise + 2 * h
    delta = 2 + 0.75 * x + 0.5 * plus
    for name, expected in (
        ("e", np.exp(-np.logaddexp(0, -eta))),
        ("mu0", mu0),
        ("mu1", mu0 + delta),
        ("y0", mu0),
        ("y1", mu0 + delta),
        ("target_weight", 1 + 0.25 * x),
    ):
        np.testing.assert_allclose(oracle[name], expected, rtol=1e-13, atol=1e-13)


def test_population_and_cohort_truth_are_distinct_targets():
    world = simulate(WorldConfig(n_units=7), seed=11)
    rows = world.oracle.to_pylist()
    cohort = fsum(row["y1"] - row["y0"] for row in rows) / len(rows)
    assert ate(world) == pytest.approx(2 + 0.5 * 0.3)
    assert ate(world, target="cohort") == pytest.approx(cohort, rel=1e-13, abs=1e-13)
    assert abs(cohort - ate(world)) > 1e-6
    with pytest.raises(ValueError):
        ate(world, target="sample")  # ty: ignore[invalid-argument-type]


def test_weighted_truth_uses_baseline_target_distribution():
    world = simulate(WorldConfig(n_units=9, target_weight_slope=0.7), seed=23)
    rows = world.oracle.to_pylist()
    weighted = fsum(row["target_weight"] * (row["y1"] - row["y0"]) for row in rows)
    weighted /= fsum(row["target_weight"] for row in rows)
    assert ate(world, weighted=True) == pytest.approx(2 + 0.5 * 0.3 + 0.7 * 0.75 / 3)
    assert ate(world, target="cohort", weighted=True) == pytest.approx(weighted, rel=1e-13)
    assert ate(world, target="cohort") != pytest.approx(weighted)
    altered = replace(
        world, analysis_table=world.analysis_table.slice(0, 0), events=world.events.slice(0, 0)
    )
    assert ate(altered, target="cohort", weighted=True) == ate(
        world, target="cohort", weighted=True
    )
