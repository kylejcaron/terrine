"""Weighted stacked DiD contrasts with unit-cluster joint covariance."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import NormalDist

import numpy as np

from terrine.data import MaterializedFrame
from terrine.errors import RefusalSpec
from terrine.results import Diagnostic

__all__ = ["StackedEventStudy", "stacked_event_study", "naive_staggered_twfe_warning"]

_INPUT = RefusalSpec(
    "terrine.estimate.stacked_input",
    "Stacked event-study input is incomplete or inconsistent: {reason}.",
)
_SAMPLED = RefusalSpec(
    "terrine.estimate.sampled_analysis_population",
    "Stacked event studies refuse sampled populations until sampling-aware inference is specified.",
)


@dataclass(frozen=True, slots=True)
class StackedEventStudy:
    cohorts: tuple[datetime, ...]
    periods: tuple[int, ...]
    effects: tuple[float, ...]
    covariance: tuple[tuple[float, ...], ...]
    standard_errors: tuple[float, ...]
    aggregate_effects: Mapping[int, float]
    aggregate_standard_errors: Mapping[int, float]
    cohort_weights: Mapping[str, float]
    n: int
    n_clusters: int
    alpha: float
    provenance: Mapping


def _column(frame: MaterializedFrame, name: str):
    if name not in frame.table.column_names:
        raise _INPUT.error(reason=f"missing_column:{name}")
    column = frame.table[name]
    if column.null_count:
        raise _INPUT.error(reason=f"null_column:{name}")
    return column.to_pylist()


def _normalized_cohort_weights(cohorts: tuple[datetime, ...], raw: Mapping[str, float] | None):
    keys = tuple(cohort.isoformat() for cohort in cohorts)
    if raw is None:
        return dict.fromkeys(keys, 1.0 / len(keys))
    if not isinstance(raw, Mapping) or set(raw) != set(keys):
        raise _INPUT.error(reason="cohort_weight_domain")
    values = [float(raw[key]) for key in keys]
    if any(not math.isfinite(value) or value <= 0 for value in values):
        raise _INPUT.error(reason="cohort_weights_must_be_positive_finite")
    scale = max(values)
    scaled = [value / scale for value in values]
    total = math.fsum(scaled)
    return {key: value / total for key, value in zip(keys, scaled, strict=True)}


def stacked_event_study(
    frame: MaterializedFrame,
    *,
    outcome: str,
    treatment: str,
    cohort: str,
    period: str,
    cohort_weights: Mapping[str, float] | None = None,
    unit_weight: str | None = None,
    alpha: float = 0.05,
) -> StackedEventStudy:
    """Estimate weighted CATTs and their joint unit-cluster covariance."""
    if frame.provenance.sampling is not None:
        raise _SAMPLED.error()
    meta = frame.provenance.calendar_comparison
    if frame.provenance.shape != "calendar_comparison" or meta is None:
        raise _INPUT.error(reason="not_calendar_comparison")
    if not 0 < alpha < 1 or not math.isfinite(alpha):
        raise _INPUT.error(reason="invalid_alpha")
    roles = frame.roles
    if roles.unit == "" or treatment != roles.treatment:
        raise _INPUT.error(reason="treatment_role_mismatch")
    if (
        treatment not in frame.table.column_names
        or cohort not in frame.table.column_names
        or period not in frame.table.column_names
    ):
        raise _INPUT.error(reason="missing_role_column")
    values = {
        "unit": _column(frame, roles.unit),
        "outcome": _column(frame, outcome),
        "treatment": _column(frame, treatment),
        "cohort": _column(frame, cohort),
        "period": _column(frame, period),
        "control": _column(frame, "control_eligible"),
    }
    weights = [1.0] * frame.table.num_rows if unit_weight is None else _column(frame, unit_weight)
    rows_by_stack: dict[
        tuple[datetime, object], dict[datetime, tuple[float, bool, bool, float]]
    ] = {}
    for index in range(frame.table.num_rows):
        unit = values["unit"][index]
        focal_cohort = values["cohort"][index]
        time = values["period"][index]
        if not isinstance(focal_cohort, datetime) or not isinstance(time, datetime):
            raise _INPUT.error(reason="cohort_period_must_be_timestamps")
        weight = float(weights[index])
        if not math.isfinite(weight) or weight <= 0:
            raise _INPUT.error(reason="unit_weights_must_be_positive_finite")
        stack = rows_by_stack.setdefault((focal_cohort, unit), {})
        if time in stack:
            raise _INPUT.error(reason="duplicate_unit_cohort_period")
        stack[time] = (
            float(values["outcome"][index]),
            bool(values["treatment"][index]),
            bool(values["control"][index]),
            weight,
        )
    cohorts = tuple(sorted({row[0] for row in rows_by_stack}))
    if not cohorts:
        raise _INPUT.error(reason="empty_population")
    periods = tuple(meta.comparison_periods)
    n_contrasts = len(cohorts) * len(periods)
    influence_by_unit: dict[object, list[float]] = {}
    effects: list[float] = []
    normalized = _normalized_cohort_weights(cohorts, cohort_weights)
    for focal in cohorts:
        reference_time = focal + timedelta(seconds=meta.reference_period * meta.period_seconds)
        for event_period in periods:
            comparison_time = focal + timedelta(seconds=event_period * meta.period_seconds)
            treated: list[tuple[object, float, float]] = []
            controls: list[tuple[object, float, float]] = []
            for (stack_cohort, unit), times in rows_by_stack.items():
                if stack_cohort != focal:
                    continue
                if reference_time not in times or comparison_time not in times:
                    continue
                baseline, _, _, base_weight = times[reference_time]
                followup, is_treated, is_control, _ = times[comparison_time]
                if is_treated:
                    treated.append((unit, followup - baseline, base_weight))
                elif is_control:
                    controls.append((unit, followup - baseline, base_weight))
            if not treated or not controls:
                raise _INPUT.error(reason="comparison_requires_treated_and_control_units")
            q_t = math.fsum(weight for _, _, weight in treated)
            q_c = math.fsum(weight for _, _, weight in controls)
            mu_t = math.fsum(weight * delta for _, delta, weight in treated) / q_t
            mu_c = math.fsum(weight * delta for _, delta, weight in controls) / q_c
            contrast_index = len(effects)
            effects.append(mu_t - mu_c)
            for unit, delta, weight in treated:
                vector = influence_by_unit.setdefault(unit, [0.0] * n_contrasts)
                vector[contrast_index] += weight * (delta - mu_t) / q_t
            for unit, delta, weight in controls:
                vector = influence_by_unit.setdefault(unit, [0.0] * n_contrasts)
                vector[contrast_index] -= weight * (delta - mu_c) / q_c
    cluster_scores = np.asarray(tuple(influence_by_unit.values()), dtype=np.float64)
    covariance = cluster_scores.T @ cluster_scores
    covariance = (covariance + covariance.T) * 0.5
    standard_errors = np.sqrt(np.maximum(np.diag(covariance), 0.0))
    z = NormalDist().inv_cdf(1 - alpha / 2)
    aggregate_effects: dict[int, float] = {}
    aggregate_ses: dict[int, float] = {}
    for period_index, event_period in enumerate(periods):
        indices = [
            cohort_index * len(periods) + period_index for cohort_index in range(len(cohorts))
        ]
        vector = np.asarray([normalized[cohort.isoformat()] for cohort in cohorts])
        aggregate_effects[event_period] = math.fsum(
            float(vector[i]) * effects[index] for i, index in enumerate(indices)
        )
        variance = float(vector @ covariance[np.ix_(indices, indices)] @ vector)
        aggregate_ses[event_period] = math.sqrt(max(variance, 0.0))
    return StackedEventStudy(
        cohorts,
        periods,
        tuple(effects),
        tuple(tuple(float(value) for value in row) for row in covariance),
        tuple(float(value) for value in standard_errors),
        aggregate_effects,
        aggregate_ses,
        normalized,
        frame.table.num_rows,
        len(influence_by_unit),
        alpha,
        {
            "normalization": "within-group baseline weights normalized separately per CATT",
            "inference": (
                "uncorrected unit-cluster sandwich; full unit contrast vectors summed "
                "before outer products"
            ),
            "control": meta.control,
            "reference_period": meta.reference_period,
            "anticipation_periods": meta.anticipation_periods,
            "confidence_multiplier": z,
        },
    )


def naive_staggered_twfe_warning() -> Diagnostic:
    """Return the required warning for requests to use naive staggered TWFE."""
    return Diagnostic(
        "terrine.estimate.naive_staggered_twfe",
        "warn",
        {"robust": False},
        {},
        {
            "message": (
                "Naive staggered TWFE can mix cohort-time effects and is not robust "
                "stacked DiD inference."
            )
        },
    )
