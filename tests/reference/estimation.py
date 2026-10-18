"""Independent, array-only score references for baseline recovery tests."""

from __future__ import annotations

from fractions import Fraction
from math import fsum

import numpy as np
from scipy.optimize import minimize


def exact_score(method: str, treatment, outcome, propensity, mu0, mu1, weight=None) -> Fraction:
    """Evaluate a small-sample ATE score exactly from rational inputs."""
    a, y, e, m0, m1 = (
        [Fraction(value) for value in values]
        for values in (treatment, outcome, propensity, mu0, mu1)
    )
    q = [Fraction(1)] * len(a) if weight is None else [Fraction(value) for value in weight]
    if not (len(a) == len(y) == len(e) == len(m0) == len(m1) == len(q)) or not a:
        raise ValueError("input arrays must have the same positive length")
    total = sum(q, Fraction())
    if method == "ipw":
        scores = [
            q_i * (a_i * y_i / e_i - (1 - a_i) * y_i / (1 - e_i))
            for a_i, y_i, e_i, q_i in zip(a, y, e, q, strict=True)
        ]
    elif method == "regression_adjustment":
        scores = [q_i * (m1_i - m0_i) for m0_i, m1_i, q_i in zip(m0, m1, q, strict=True)]
    elif method == "aipw":
        scores = [
            q_i * (m1_i - m0_i + a_i * (y_i - m1_i) / e_i - (1 - a_i) * (y_i - m0_i) / (1 - e_i))
            for a_i, y_i, e_i, m0_i, m1_i, q_i in zip(a, y, e, m0, m1, q, strict=True)
        ]
    else:
        raise ValueError("unknown method")
    return sum(scores, Fraction()) / total


def logistic_fit(x, treatment, weight=None):
    """Solve a weighted logistic likelihood independently for test reference use."""
    x = np.asarray(x, dtype=np.float64)
    a = np.asarray(treatment, dtype=np.float64)
    design = np.column_stack((np.ones(len(a)), x))
    q = np.ones(len(a)) if weight is None else np.asarray(weight, dtype=np.float64)

    def objective(beta):
        eta = design @ beta
        return float(np.sum(q * (np.logaddexp(0.0, eta) - a * eta)))

    def gradient(beta):
        eta = design @ beta
        return design.T @ (q * (1 / (1 + np.exp(-eta)) - a))

    result = minimize(
        objective,
        np.zeros(design.shape[1]),
        jac=gradient,
        method="BFGS",
        options={"gtol": 1e-10, "maxiter": 1000},
    )
    if not result.success and np.linalg.norm(gradient(result.x), ord=np.inf) > 1e-7:
        raise ArithmeticError("independent logistic reference did not converge")
    return result.x


def weighted_mean(values, weights=None) -> float:
    """Return a compensated weighted mean using a max-scaled denominator."""
    vals = np.asarray(values, dtype=np.float64)
    q = np.ones(len(vals)) if weights is None else np.asarray(weights, dtype=np.float64)
    scaled = q / np.max(q)
    return fsum(float(w) * float(x) for w, x in zip(scaled, vals, strict=True)) / fsum(
        map(float, scaled)
    )


def population_truth(config) -> float:
    """Evaluate the simulator's analytical weighted or unweighted target."""
    return 2.15 + 0.25 * config.target_weight_slope
