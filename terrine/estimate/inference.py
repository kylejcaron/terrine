"""Analytical cross-fit parametric estimating equations and uncertainty."""

from __future__ import annotations

import math

import numpy as np
from scipy import linalg, special

from terrine.errors import RefusalSpec
from terrine.estimate._data import PreparedData
from terrine.estimate.nuisance import CONDITION_LIMIT, CrossFitPredictions

INVALID_ALPHA = RefusalSpec(
    "terrine.estimate.invalid_alpha", "Use a finite alpha strictly between zero and one."
)
SINGULAR_INFERENCE = RefusalSpec(
    "terrine.estimate.singular_inference",
    "The joint estimating-equation bread is singular or ill-conditioned; "
    "no score-only variance is substituted.",
)
NONFINITE_RESULT = RefusalSpec(
    "terrine.estimate.nonfinite_result",
    "Numerical estimation produced a nonfinite intermediate or result.",
)


def critical_value(alpha: float) -> float:
    if (
        isinstance(alpha, bool)
        or not isinstance(alpha, (int, float))
        or not math.isfinite(alpha)
        or not 0 < alpha < 1
    ):
        raise INVALID_ALPHA.error(reason="alpha_domain")
    return float(-special.ndtri_exp(math.log(alpha) - math.log(2)))


def _checked(array: np.ndarray, stage: str) -> np.ndarray:
    if not np.all(np.isfinite(array)):
        raise NONFINITE_RESULT.error(stage=stage)
    return array


def pseudo_outcome(
    method: str,
    a: np.ndarray,
    y: np.ndarray,
    log_e: np.ndarray,
    log1m_e: np.ndarray,
    mu0: np.ndarray | None,
    mu1: np.ndarray | None,
) -> np.ndarray:
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        if method == "regression_adjustment":
            assert mu0 is not None and mu1 is not None
            result = mu1 - mu0
        else:
            treated = a == 1
            if method == "ipw":
                result = np.empty_like(y)
                result[treated] = y[treated] * np.exp(-log_e[treated])
                result[~treated] = -y[~treated] * np.exp(-log1m_e[~treated])
            else:
                assert mu0 is not None and mu1 is not None
                result = mu1 - mu0
                result[treated] += (y[treated] - mu1[treated]) * np.exp(-log_e[treated])
                result[~treated] -= (y[~treated] - mu0[~treated]) * np.exp(-log1m_e[~treated])
    return _checked(result, "heldout_pseudo_outcome")


def target_derivatives(
    method: str,
    a: np.ndarray,
    y: np.ndarray,
    q: np.ndarray,
    log_e: np.ndarray,
    log1m_e: np.ndarray,
    mu0: np.ndarray | None,
    mu1: np.ndarray | None,
    x: np.ndarray,
    z: np.ndarray | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Row derivatives ordered propensity, control outcome, treated outcome.

    d(1/e)/d beta = -(1-e)x/e; d(1/(1-e))/d beta = ex/(1-e).
    The target equation multiplies these derivatives by q, with d/d tau = -q.
    """
    n = len(a)
    beta = np.zeros_like(x)
    theta0 = np.zeros((n, 0 if z is None else z.shape[1]))
    theta1 = np.zeros_like(theta0)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        if method != "regression_adjustment":
            treated = a == 1
            if method == "ipw":
                residual1 = residual0 = y
            else:
                assert mu0 is not None and mu1 is not None
                residual1 = y - mu1
                residual0 = y - mu0
            coefficient = np.empty(n)
            coefficient[treated] = (
                -q[treated] * residual1[treated] * np.exp(log1m_e[treated] - log_e[treated])
            )
            coefficient[~treated] = (
                -q[~treated] * residual0[~treated] * np.exp(log_e[~treated] - log1m_e[~treated])
            )
            beta = coefficient[:, None] * x
        if method == "regression_adjustment":
            assert z is not None
            theta0 = -q[:, None] * z
            theta1 = q[:, None] * z
        elif method == "aipw":
            assert z is not None
            c0 = np.full(n, -1.0)
            c1 = np.ones(n)
            c0[a == 0] = np.exp(log_e[a == 0] - log1m_e[a == 0])
            c1[a == 1] = -np.exp(log1m_e[a == 1] - log_e[a == 1])
            theta0 = (q * c0)[:, None] * z
            theta1 = (q * c1)[:, None] * z
    return (
        _checked(beta, "target_jacobian"),
        _checked(theta0, "target_jacobian"),
        _checked(theta1, "target_jacobian"),
    )


def _mean(values: np.ndarray, weights: np.ndarray | None = None) -> float:
    """Compensated mean after magnitude scaling, including positive target weights."""
    _checked(values, "mean_input")
    magnitude = float(np.max(np.abs(values)))
    if magnitude == 0:
        return 0.0
    scaled = values / magnitude
    if weights is None:
        average = math.fsum(scaled) / len(values)
    else:
        average = math.fsum((weights / math.fsum(weights)) * scaled)
    result = magnitude * average
    if not math.isfinite(result):
        raise NONFINITE_RESULT.error(stage="scaled_mean")
    return result


def _mean_columns(values: np.ndarray) -> np.ndarray:
    return np.array([_mean(values[:, j]) for j in range(values.shape[1])])


def _nuisance_blocks(data: PreparedData, pred: CrossFitPredictions, method: str):
    """Yield one symmetric bread block, target derivative, design and score multiplier."""
    for fit in pred.fits:
        x, z, train = fit.propensity_design, fit.outcome_design, fit.train
        heldout = ~train
        count = np.count_nonzero(heldout)
        db, d0, d1 = target_derivatives(
            method,
            data.a[heldout],
            data.y[heldout],
            data.q[heldout],
            pred.log_e[heldout],
            pred.log1m_e[heldout],
            None if pred.mu0 is None else pred.mu0[heldout],
            None if pred.mu1 is None else pred.mu1[heldout],
            x[heldout],
            None if z is None else z[heldout],
        )
        with np.errstate(over="ignore", invalid="ignore"):
            if method != "regression_adjustment":
                eta = x @ fit.beta
                log_e, log1m = -np.logaddexp(0, -eta), -np.logaddexp(0, eta)
                residual = np.where(data.a == 1, special.expit(-eta), -special.expit(eta))
                w = train * data.q
                curvature = w * np.exp(log_e + log1m)
                block = -(x.T * (curvature / data.n)) @ x
                derivative = _mean_columns(db) * (count / data.n)
                yield (
                    _checked(block, "nuisance_bread"),
                    derivative,
                    x,
                    _checked(w * residual, "nuisance_score"),
                )
            if method != "ipw":
                assert z is not None and fit.theta0 is not None and fit.theta1 is not None
                for arm, theta, derivative in ((0, fit.theta0, d0), (1, fit.theta1, d1)):
                    w = (train & (data.a == arm)) * data.q
                    block = -(z.T * (w / data.n)) @ z
                    yield (
                        _checked(block, "nuisance_bread"),
                        _mean_columns(derivative) * (count / data.n),
                        z,
                        _checked(w * (data.y - z @ theta), "nuisance_score"),
                    )


def _secular_sign(v: float, d: np.ndarray, t2: np.ndarray, q2: float) -> int:
    """Sign of q²/v - 1 - sum(t²/(d-v)), or zero when arithmetic is unresolved."""
    delta = d - v
    if np.any(delta == 0):
        return 0
    # Log-scale individual terms before summation: even a finite root bracket
    # may have unrepresentably large unscaled secular terms near a pole.
    logs = np.r_[math.log(q2) - math.log(v), 0.0, np.log(t2) - np.log(np.abs(delta))]
    terms = np.exp(logs - np.max(logs))
    terms[1] *= -1
    terms[2:] *= -np.sign(delta)
    value = math.fsum(terms)
    # Covers subtraction, division represented through log/exp, and summation
    # roundoff in binary64; an indeterminate sign never narrows the bracket.
    error = 32 * np.finfo(float).eps * (len(d) + 3) * math.fsum(np.abs(terms))
    return 1 if value > error else -1 if value < -error else 0


def _outer_root(
    d: np.ndarray, t2: np.ndarray, q2: float, lower: float, upper: float, *, small: bool
) -> tuple[float, float]:
    """Enclose an outer arrowhead eigenvalue by monotone log-space bisection."""
    lo, hi = math.log(lower), math.log(upper)
    tolerance = 256 * np.finfo(float).eps * (len(d) + 3)
    if tolerance >= 0.01:
        raise SINGULAR_INFERENCE.error(reason="bread_root_arithmetic")
    if small and _secular_sign(lower, d, t2, q2) != 1:
        raise SINGULAR_INFERENCE.error(reason="bread_root_nonrepresentable")
    if not small and _secular_sign(upper, d, t2, q2) != -1:
        raise SINGULAR_INFERENCE.error(reason="bread_root_bracket")
    # At the active pole the one-sided signs are known analytically. All
    # evaluated interior signs must be separated from their arithmetic bounds.
    for _ in range(128):
        if hi - lo <= tolerance:
            return math.nextafter(math.exp(lo), 0.0), math.nextafter(math.exp(hi), math.inf)
        midpoint = lo / 2 + hi / 2
        v = math.exp(midpoint)
        sign = _secular_sign(v, d, t2, q2)
        if sign > 0:
            lo = midpoint
        elif sign < 0:
            hi = midpoint
        else:
            left, right = max(lo, midpoint - tolerance), min(hi, midpoint + tolerance)
            if (
                _secular_sign(math.exp(left), d, t2, q2) == 1
                and _secular_sign(math.exp(right), d, t2, q2) == -1
            ):
                return (
                    math.nextafter(math.exp(left), 0.0),
                    math.nextafter(math.exp(right), math.inf),
                )
            raise SINGULAR_INFERENCE.error(reason="bread_root_uncertainty")
    raise SINGULAR_INFERENCE.error(reason="bread_root_iterations")


def _bread_condition(eigenvalues: np.ndarray, couplings: np.ndarray, qbar: float) -> float:
    """Full spectral condition via the block-eigenbasis arrowhead of A A'.

    For A=[[diag(B_j),0],[c',-qbar]], B_j=U_j diag(lambda_j) U_j',
    put t_j=U_j'c_j. The arrowhead has diagonal lambda² and qbar²+||t||²,
    and off-diagonal lambda*t. Its outer roots satisfy
    qbar²/v - 1 - sum(t²/(lambda²-v))=0. Include inherited eigenvalues
    at zero couplings/repeated poles; only block eigensolves are needed.
    """
    scale = max(float(np.max(np.abs(eigenvalues))), float(np.max(np.abs(couplings))), qbar)
    with np.errstate(over="ignore", under="ignore", invalid="ignore"):
        d = (eigenvalues / scale) ** 2
        t2 = (couplings / scale) ** 2
        q2 = (qbar / scale) ** 2
    if (
        not math.isfinite(scale)
        or q2 < np.finfo(float).tiny
        or np.any(d < np.finfo(float).tiny)
        or np.any((couplings != 0) & (t2 < np.finfo(float).tiny))
    ):
        raise SINGULAR_INFERENCE.error(reason="bread_spectral_scale")
    active = t2 > 0
    smallest, largest = min(float(np.min(d)), q2), max(float(np.max(d)), q2)
    if np.any(active):
        active_d, active_t2 = d[active], t2[active]
        low, _ = _outer_root(
            active_d, active_t2, q2, np.finfo(float).tiny, float(np.min(active_d)), small=True
        )
        trace = math.fsum(d) + math.fsum(t2) + q2
        upper = trace * (1 + 64 * np.finfo(float).eps * (len(d) + 3))
        _, high = _outer_root(active_d, active_t2, q2, float(np.max(active_d)), upper, small=False)
        smallest, largest = min(smallest, low), max(largest, high)
    log_condition = (math.log(largest) - math.log(smallest)) / 2
    if log_condition > math.log(CONDITION_LIMIT):
        raise SINGULAR_INFERENCE.error(
            reason="bread_condition", limit=CONDITION_LIMIT, log_condition_upper=log_condition
        )
    return math.exp(log_condition)


def _unit_influence(
    data: PreparedData, pred: CrossFitPredictions, method: str, tau: float
) -> np.ndarray:
    psi = pseudo_outcome(method, data.a, data.y, pred.log_e, pred.log1m_e, pred.mu0, pred.mu1)
    with np.errstate(over="ignore", invalid="ignore"):
        influence = _checked(data.q * (psi - tau), "target_score")
    eigenvalues, couplings = [], []
    for block, derivative, design, residual in _nuisance_blocks(data, pred, method):
        try:
            eigen, basis = linalg.eigh(block, check_finite=False)
            if np.any(eigen >= 0):
                raise SINGULAR_INFERENCE.error(reason="nuisance_bread_rank")
            coupling = _checked(basis.T @ derivative, "bread_coupling")
            # Solve B_j.T v_j=c_j in its orthonormal eigenbasis; no joint
            # score matrix, dense joint bread, or explicit inverse is formed.
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                projection = basis @ (coupling / eigen)
            if not np.all(np.isfinite(projection)):
                raise SINGULAR_INFERENCE.error(reason="nuisance_bread_linear_solve")
        except linalg.LinAlgError as exc:
            raise SINGULAR_INFERENCE.error(reason="nuisance_bread_eigensolve") from exc
        eigenvalues.append(eigen)
        couplings.append(coupling)
        with np.errstate(over="ignore", invalid="ignore"):
            influence -= residual * (design @ projection)
        _checked(influence, "projected_unit_score")
    qbar = _mean(data.q)
    _bread_condition(np.concatenate(eigenvalues), np.concatenate(couplings), qbar)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        influence /= qbar
    return _checked(influence, "unit_influence")


def _standard_error(influence: np.ndarray) -> float:
    """Center around a safe midpoint, then scale before compensated moments."""
    _checked(influence, "unit_influence")
    reference = float(np.min(influence)) / 2 + float(np.max(influence)) / 2
    delta = _checked(influence - reference, "centered_unit_influence")
    magnitude = float(np.max(np.abs(delta)))
    if magnitude == 0:
        return 0.0
    scaled = delta / magnitude
    centered = scaled - _mean(scaled)
    n = len(influence)
    variance_scaled = math.fsum(float(v) ** 2 for v in centered) / (n * (n - 1))
    se = magnitude * math.sqrt(variance_scaled)
    if not math.isfinite(se):
        raise NONFINITE_RESULT.error(stage="sandwich_standard_error")
    return se


def sandwich(data: PreparedData, pred: CrossFitPredictions, method: str, tau: float) -> float:
    if data.n < 2:
        raise SINGULAR_INFERENCE.error(reason="at_least_two_independent_units")
    return _standard_error(_unit_influence(data, pred, method, tau))
