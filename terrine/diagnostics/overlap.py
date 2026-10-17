"""Target-weighted propensity support and actual inverse-weight arm ESS."""

from __future__ import annotations

import math

import numpy as np
from scipy.special import logsumexp

from terrine.data import MaterializedFrame
from terrine.diagnostics._data import InvalidDiagnosticData, arrays, unavailable
from terrine.results import Diagnostic, DiagnosticPolicy


def _overlap_arrays(
    a: np.ndarray,
    q: np.ndarray,
    log_propensity: np.ndarray,
    log_complement: np.ndarray,
    policy: DiagnosticPolicy,
) -> Diagnostic:
    le = np.asarray(log_propensity, dtype=float)
    lc = np.asarray(log_complement, dtype=float)
    if (
        le.shape != a.shape
        or lc.shape != a.shape
        or np.any(np.isnan(le))
        or np.any(np.isnan(lc))
        or np.any(le > 0)
        or np.any(lc > 0)
        or not np.allclose(np.logaddexp(le, lc), 0, atol=1e-12, rtol=0)
    ):
        return unavailable("overlap", "invalid_log_probability_pair")
    thresholds = {
        "overlap_warn": policy.overlap_warn,
        "overlap_fail": policy.overlap_fail,
        "max_fail_share": policy.max_fail_share,
        "min_arm_ess": policy.min_arm_ess,
    }
    numerical_support = bool(
        np.all(np.isfinite(le))
        and np.all(np.isfinite(lc))
        and np.all(-le <= math.log(np.finfo(float).max))
        and np.all(-lc <= math.log(np.finfo(float).max))
    )
    fail_tail = (le < math.log(policy.overlap_fail)) | (lc < math.log(policy.overlap_fail))
    warn_tail = (le < math.log(policy.overlap_warn)) | (lc < math.log(policy.overlap_warn))
    total = math.fsum(q)
    fail_share = math.fsum(q[fail_tail]) / total
    warn_share = math.fsum(q[warn_tail]) / total
    metrics: dict[str, float | int | str | bool | None] = {
        "fail_tail_share": fail_share,
        "warn_tail_share": warn_share,
        "numerical_support": numerical_support,
    }
    arm_ess: list[float | None] = []
    bins = np.array([0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0])
    histograms = {}
    log_q = np.log(q)
    for arm in (0, 1):
        mask = a == arm
        if not np.any(mask):
            return unavailable("overlap", "missing_treatment_arm")
        arm_total = math.fsum(q[mask])
        log_weights = log_q[mask] - (le[mask] if arm else lc[mask])
        if np.all(np.isfinite(log_weights)):
            normalized = np.exp(log_weights - logsumexp(log_weights))
            ess = 1 / math.fsum(float(w) ** 2 for w in normalized)
        else:
            ess = None
            numerical_support = False
        metrics[f"arm{arm}_ess"] = ess
        arm_ess.append(ess)
        metrics[f"arm{arm}_min_propensity"] = float(np.exp(np.min(le[mask])))
        metrics[f"arm{arm}_max_propensity"] = float(np.exp(np.max(le[mask])))
        metrics[f"arm{arm}_min_log_propensity"] = (
            float(np.min(le[mask])) if np.all(np.isfinite(le[mask])) else None
        )
        metrics[f"arm{arm}_min_log_complement"] = (
            float(np.min(lc[mask])) if np.all(np.isfinite(lc[mask])) else None
        )
        metrics[f"arm{arm}_fail_tail_share"] = math.fsum(q[mask & fail_tail]) / arm_total
        # Normalized before binning, so histogram accumulation cannot overflow.
        histogram, _ = np.histogram(np.exp(le[mask]), bins=bins, weights=q[mask] / arm_total)
        histograms[f"arm{arm}"] = [float(v) for v in histogram]
    metrics["numerical_support"] = numerical_support
    fail = (
        not numerical_support
        or fail_share > policy.max_fail_share
        or any(value is None or value < policy.min_arm_ess for value in arm_ess)
    )
    status = "fail" if fail else "warn" if warn_share > 0 else "pass"
    reasons = {key: "numerical_non_support" for key, value in metrics.items() if value is None}
    return Diagnostic(
        "overlap",
        status,
        metrics,
        thresholds,
        {
            "histogram_edges": bins.tolist(),
            "histograms": histograms,
            "histogram_weighting": "target_within_arm",
            "ess_weighting": "complete_target_inverse_probability",
            "metric_reasons": reasons,
            "assumptions": (
                "Screening evidence does not establish exchangeability or population positivity."
            ),
        },
    )


def overlap(
    frame: MaterializedFrame,
    *,
    log_propensity: np.ndarray,
    log_complement: np.ndarray,
    policy: DiagnosticPolicy | None = None,
) -> Diagnostic:
    """Screen held-out log probabilities without recovering complements by subtraction."""
    try:
        a, q, _, _ = arrays(frame, include_features=False)
        return _overlap_arrays(a, q, log_propensity, log_complement, policy or DiagnosticPolicy())
    except InvalidDiagnosticData as exc:
        return unavailable("overlap", str(exc))
    except (ValueError, TypeError, OverflowError):
        return unavailable("overlap", "invalid_measured_input_conversion")
