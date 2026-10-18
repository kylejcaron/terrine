"""Nuisance-omission and sample-size recovery checks."""

from __future__ import annotations

import json
import os
import platform
import time
from pathlib import Path

import numpy as np
import pytest
from scipy.special import ndtri_exp

from terrine.estimate import LinearTerms, NuisanceSpec, aipw, ipw, regression_adjustment
from terrine.simulate import WorldConfig, simulate
from tests.fixtures.baseline import materialized_baseline

SIZES = (500, 2000, 8000)
SEEDS = range(100)
FAMILY_ALPHA = 0.01
METHODS = {"ipw": ipw, "aipw": aipw, "regression_adjustment": regression_adjustment}


def _scenarios():
    for method in METHODS:
        yield (f"correct-{method}", method, 0.0, 0.0)
    yield ("aipw-propensity-correct-outcome-omitted", "aipw", 0.0, 2.0)
    yield ("aipw-outcome-correct-propensity-omitted", "aipw", 1.0, 0.0)


def _estimate(method, n_units, seed, gamma, beta):
    world = simulate(
        WorldConfig(
            n_units=n_units,
            propensity_quadratic=gamma,
            outcome_quadratic=beta,
            outcome_noise_sd=2,
        ),
        seed=seed,
    )
    estimate = METHODS[method](
        materialized_baseline(world), outcome="net_revenue", folds=5, seed=seed
    )
    return estimate, 2.15


@pytest.mark.slow
@pytest.mark.parameter_recovery
@pytest.mark.parametrize(
    "scenario",
    list(_scenarios()),
    ids=[scenario[0] for scenario in _scenarios()],
)
def test_large_sample_mse_bound_resolves_against_small_sample(scenario):
    name, method, gamma, beta = scenario
    started = time.perf_counter()
    records = {}
    for size in SIZES:
        errors, standard_errors, refused = [], [], []
        for seed in SEEDS:
            try:
                estimate, truth = _estimate(method, size, seed, gamma, beta)
            except Exception as exc:
                refused.append({"seed": seed, "code": getattr(exc, "code", type(exc).__name__)})
                continue
            errors.append(estimate.value - truth)
            standard_errors.append(estimate.se)
        squared = np.square(np.asarray(errors, dtype=np.float64))
        mse = float(np.mean(squared)) if len(squared) else float("nan")
        mse_mcse = (
            float(np.std(squared, ddof=1) / np.sqrt(len(squared)))
            if len(squared) > 1
            else float("nan")
        )
        records[size] = {
            "attempted": len(SEEDS),
            "completed": len(errors),
            "refused": refused,
            "bias": float(np.mean(errors)) if errors else None,
            "rmse": float(np.sqrt(mse)) if errors else None,
            "mean_estimated_se": float(np.mean(standard_errors)) if errors else None,
            "mse": mse,
            "mse_mcse": mse_mcse,
        }
    payload = {
        "scenario": name,
        "method": method,
        "configuration": {
            "sizes": SIZES,
            "seeds": [0, 99],
            "seed_count": 100,
            "gamma": gamma,
            "b": beta,
            "target_weight_slope": 0.0,
            "outcome_noise_sd": 2.0,
            "features": ["x", "plan_tier"],
            "propensity_terms": {
                "main_effects": ["x", "plan_tier"],
                "squares": [],
                "interactions": [],
            },
            "outcome_terms": {
                "main_effects": ["x", "plan_tier"],
                "squares": [],
                "interactions": [],
            },
            "folds": 5,
            "family_alpha": FAMILY_ALPHA,
            "per_comparison_alpha": FAMILY_ALPHA / 5,
        },
        "results": records,
        "runtime_seconds": time.perf_counter() - started,
        "environment": {"python": platform.python_version(), "numpy": np.__version__},
    }
    output_dir = Path(
        os.environ.get("TERRINE_CALIBRATION_DIR", "docs/validation/generated/baseline-size")
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{name}.json"
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for size, result in records.items():
        assert result["completed"] == 100, f"{name}, n={size}: refusals {result['refused']}"
    critical = -ndtri_exp(np.log(FAMILY_ALPHA / 5))
    small = records[500]
    large = records[8000]
    lower_small = small["mse"] - critical * small["mse_mcse"]
    upper_large = large["mse"] + critical * large["mse_mcse"]
    assert upper_large < lower_small


@pytest.mark.parameter_recovery
def test_fast_nonlinear_term_recovery_smoke():

    world = simulate(
        WorldConfig(n_units=120, propensity_quadratic=0.4, outcome_quadratic=0.4),
        seed=501,
    )
    spec = NuisanceSpec(
        propensity=LinearTerms(squares=("x",)),
        outcome=LinearTerms(squares=("x",)),
    )
    result = aipw(
        materialized_baseline(world),
        outcome="net_revenue",
        folds=5,
        seed=13,
        nuisance=spec,
    )
    assert np.isfinite(result.value)
    assert np.isfinite(result.se)
