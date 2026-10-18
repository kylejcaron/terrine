"""Predeclared population-target coverage and studentization calibration grid."""

from __future__ import annotations

import json
import os
import platform
import time
from pathlib import Path

import numpy as np
import pytest
import scipy
from scipy.special import ndtri_exp
from scipy.stats import binomtest, chi2

from terrine.estimate import LinearTerms, NuisanceSpec, aipw, ipw, regression_adjustment
from terrine.simulate import WorldConfig, simulate
from tests.fixtures.baseline import materialized_baseline
from tests.reference.estimation import population_truth

METHODS = {"ipw": ipw, "aipw": aipw, "regression_adjustment": regression_adjustment}
ALPHA = 0.01
CELL_ALPHA = ALPHA / (3 * 10)
SEEDS = range(400)
N_UNITS = 2000


def _cells():
    for slope in (0.0, 0.25):
        for method in METHODS:
            yield (f"correct-{method}-weight-{slope:g}", method, 0.0, 0.0, slope)
    yield ("omit-outcome-ipw", "ipw", 0.0, 2.0, 0.0)
    yield ("omit-outcome-aipw", "aipw", 0.0, 2.0, 0.0)
    yield ("omit-propensity-aipw", "aipw", 1.0, 0.0, 0.0)
    yield ("omit-propensity-ra", "regression_adjustment", 1.0, 0.0, 0.0)


def _nuisance(fit_propensity_square=False, fit_outcome_square=False):
    p_square = ("x",) if fit_propensity_square else ()
    y_square = ("x",) if fit_outcome_square else ()
    return NuisanceSpec(
        propensity=LinearTerms(squares=p_square),
        outcome=LinearTerms(squares=y_square),
    )


def _run_cell(cell):
    name, method, gamma, beta, slope = cell
    start = time.perf_counter()
    truth = population_truth(WorldConfig(target_weight_slope=slope))
    errors, standard_errors, widths, statuses, refusals = [], [], [], [], []
    intervals_cover = 0
    studentized_error_sum = 0.0
    for seed in SEEDS:
        world = simulate(
            WorldConfig(
                n_units=N_UNITS,
                propensity_quadratic=gamma,
                outcome_quadratic=beta,
                target_weight_slope=slope,
            ),
            seed=seed,
        )
        frame = materialized_baseline(world, weighted=bool(slope))
        try:
            result = METHODS[method](
                frame,
                outcome="net_revenue",
                folds=5,
                seed=seed,
                nuisance=_nuisance(),
            )
        except Exception as exc:  # Every refusal is recorded and remains a failed replication.
            refusals.append({"seed": seed, "code": getattr(exc, "code", type(exc).__name__)})
            continue
        error_i = result.value - truth
        errors.append(error_i)
        standard_errors.append(result.se)
        widths.append(result.ci[1] - result.ci[0])
        intervals_cover += int(result.ci[0] <= truth <= result.ci[1])
        studentized_error_sum += (error_i / result.se) ** 2
        statuses.append(
            [
                {"name": diagnostic.name, "status": diagnostic.status}
                for diagnostic in result.diagnostics
            ]
        )
    elapsed = time.perf_counter() - start
    error = np.asarray(errors, dtype=np.float64)
    se = np.asarray(standard_errors, dtype=np.float64)

    payload = {
        "cell": name,
        "method": method,
        "config": {
            "n_units": N_UNITS,
            "seeds": [0, 399],
            "seed_count": len(SEEDS),
            "propensity_quadratic": gamma,
            "outcome_quadratic": beta,
            "target_weight_slope": slope,
            "measured_features": ["x", "plan_tier"],
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
            "alpha": 0.05,
            "calibration_family_alpha": ALPHA,
            "per_gate_alpha": CELL_ALPHA,
        },
        "attempted": len(SEEDS),
        "completed": len(errors),
        "refused": refusals,
        "mean_error": float(np.mean(error)) if len(error) else None,
        "empirical_sd": float(np.std(error, ddof=1)) if len(error) > 1 else None,
        "mean_estimated_se": float(np.mean(se)) if len(se) else None,
        "rms_estimated_se": float(np.sqrt(np.mean(se * se))) if len(se) else None,
        "coverage_count": intervals_cover,
        "median_interval_width": float(np.median(widths)) if widths else None,
        "studentized_error_sum": studentized_error_sum,
        "diagnostic_statuses": statuses,
        "runtime_seconds": elapsed,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "numpy": np.__version__,
            "scipy": scipy.__version__,
        },
        "population_truth": truth,
    }
    output_root = Path(
        os.environ.get(
            "TERRINE_CALIBRATION_DIR",
            "docs/validation/generated/baseline-calibration",
        )
    )
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"{name}.json"
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    assert not refusals, f"{name}: refused replications {refusals}"
    assert len(errors) == 400
    return payload


@pytest.mark.slow
@pytest.mark.parameter_recovery
@pytest.mark.parametrize("cell", list(_cells()), ids=[cell[0] for cell in _cells()])
def test_population_bias_coverage_and_studentization_gates(cell):
    payload = _run_cell(cell)
    assert payload["refused"] == []
    assert payload["completed"] == payload["attempted"] == 400
    # The actual coverage count and studentized errors are retained by full evidence runs.
    error = payload["mean_error"]
    empirical_sd = payload["empirical_sd"]
    assert abs(error) <= -ndtri_exp(np.log(CELL_ALPHA / 2)) * empirical_sd / np.sqrt(400)
    coverage_p = binomtest(payload["coverage_count"], 400, 0.95, alternative="two-sided").pvalue
    assert coverage_p >= CELL_ALPHA
    lower = chi2.ppf(CELL_ALPHA / 2, 400)
    upper = chi2.isf(CELL_ALPHA / 2, 400)
    assert lower <= payload["studentized_error_sum"] <= upper
