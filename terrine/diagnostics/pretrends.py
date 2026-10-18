"""Joint event-time pretrend evidence for stacked event studies."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np
from scipy.stats import chi2

from terrine.results import Diagnostic

if TYPE_CHECKING:
    from terrine.estimate.stacked import StackedEventStudy

__all__ = ["pretrend_test"]


def pretrend_test(result: StackedEventStudy, *, alpha: float = 0.05) -> Diagnostic:
    """Jointly test every declared pre-treatment event-time contrast.

    A non-rejection is not evidence for parallel trends. The diagnostic is
    conservatively marked ``warn`` when the study has fewer than five cohorts
    or twenty independent units, even if the joint test rejects.
    """
    if isinstance(alpha, bool) or not isinstance(alpha, (int, float)) or not math.isfinite(alpha):
        raise ValueError("alpha must be finite and strictly between zero and one")
    if not 0 < alpha < 1:
        raise ValueError("alpha must be finite and strictly between zero and one")
    periods = tuple(result.periods)
    leads = tuple(index for index, period in enumerate(periods) if period < 0)
    cohorts = len(result.cohorts)
    if not leads:
        return Diagnostic(
            "joint_pretrend",
            "unavailable",
            {
                "n_leads": 0,
                "n_cohorts": cohorts,
                "n_units": result.n_clusters,
                "statistic": None,
                "p_value": None,
                "degrees_of_freedom": 0,
            },
            {"alpha": float(alpha)},
            {
                "reason": "no_declared_pre_treatment_leads",
                "interpretation": "failure_to_reject_is_not_evidence_of_parallel_trends",
            },
        )
    indices = [
        cohort_index * len(periods) + lead for cohort_index in range(cohorts) for lead in leads
    ]
    estimate = np.asarray([result.effects[index] for index in indices], dtype=np.float64)
    covariance = np.asarray(result.covariance, dtype=np.float64)[np.ix_(indices, indices)]
    if not np.isfinite(estimate).all() or not np.isfinite(covariance).all():
        return Diagnostic(
            "joint_pretrend",
            "unavailable",
            {
                "n_leads": len(leads),
                "n_cohorts": cohorts,
                "n_units": result.n_clusters,
                "statistic": None,
                "p_value": None,
                "degrees_of_freedom": 0,
            },
            {"alpha": float(alpha)},
            {
                "reason": "nonfinite_joint_covariance",
                "interpretation": "failure_to_reject_is_not_evidence_of_parallel_trends",
            },
        )
    rank = int(np.linalg.matrix_rank(covariance))
    if rank == 0:
        return Diagnostic(
            "joint_pretrend",
            "unavailable",
            {
                "n_leads": len(leads),
                "n_cohorts": cohorts,
                "n_units": result.n_clusters,
                "statistic": None,
                "p_value": None,
                "degrees_of_freedom": 0,
            },
            {"alpha": float(alpha)},
            {
                "reason": "singular_joint_covariance",
                "interpretation": "failure_to_reject_is_not_evidence_of_parallel_trends",
            },
        )
    statistic = float(estimate @ np.linalg.pinv(covariance, hermitian=True) @ estimate)
    statistic = max(0.0, statistic)
    p_value = float(chi2.sf(statistic, rank))
    low_power = cohorts < 5 or result.n_clusters < 20
    status = "warn" if low_power else ("fail" if p_value < alpha else "pass")
    return Diagnostic(
        "joint_pretrend",
        status,
        {
            "n_leads": len(leads),
            "n_cohorts": cohorts,
            "n_units": result.n_clusters,
            "statistic": statistic,
            "p_value": p_value,
            "degrees_of_freedom": rank,
            "reject": p_value < alpha,
        },
        {"alpha": float(alpha)},
        {
            "reason": "low_power"
            if low_power
            else ("pretrend_detected" if p_value < alpha else "not_rejected"),
            "tested_event_periods": tuple(periods[index] for index in leads),
            "interpretation": "failure_to_reject_is_not_evidence_of_parallel_trends",
            "power_context": {
                "minimum_cohorts_for_unwarned_status": 5,
                "minimum_independent_units_for_unwarned_status": 20,
            },
        },
    )
