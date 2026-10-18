"""Evidence-only placebo and negative-control diagnostics."""

from __future__ import annotations

import math

from scipy.stats import norm

from terrine.data import MaterializedFrame
from terrine.results import Diagnostic

__all__ = ["placebo_test", "negative_control_test"]

_INTERPRETATION = "placebo_evidence_does_not_authorize_causal_interpretation"


def _column(frame: MaterializedFrame, name: str):
    if name not in frame.table.column_names:
        return None
    return frame.table[name].to_pylist()


def _mean(values: list[float]) -> float | None:
    if not values:
        return 0.0
    try:
        value = math.fsum(item / len(values) for item in values)
    except OverflowError:
        return None
    return value if math.isfinite(value) else None


def _cluster_contrast(frame: MaterializedFrame, outcome: str, selected: list[bool]):
    treatment_name = frame.roles.treatment
    if treatment_name is None:
        return None
    values = _column(frame, outcome)
    treated = _column(frame, treatment_name)
    units = _column(frame, frame.roles.cluster_unit or frame.roles.unit)
    if values is None or treated is None or units is None:
        return None
    groups: dict[bool, dict[object, list[float]]] = {True: {}, False: {}}
    for value, arm, unit, keep in zip(values, treated, units, selected, strict=True):
        if not keep:
            continue
        if value is None or unit is None:
            return None
        if not isinstance(arm, (bool, int, float)) or arm not in (0, 1):
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(numeric):
            return None
        groups[bool(arm)].setdefault(unit, []).append(numeric)
    if not groups[True] or not groups[False]:
        return None
    means: dict[bool, float] = {}
    unit_means: dict[bool, dict[object, float]] = {}
    for arm in (True, False):
        unit_means[arm] = {}
        for unit, observations in groups[arm].items():
            mean = _mean(observations)
            if mean is None:
                return None
            unit_means[arm][unit] = mean
        mean = _mean(list(unit_means[arm].values()))
        if mean is None:
            return None
        means[arm] = mean
    try:
        effect = math.fsum((means[True], -means[False]))
    except OverflowError:
        return None
    if not math.isfinite(effect):
        return None
    influence: dict[object, float] = {}
    for arm, sign in ((True, 1.0), (False, -1.0)):
        count = len(unit_means[arm])
        for unit, value in unit_means[arm].items():
            try:
                residual = math.fsum((value, -means[arm]))
                contribution = sign * (residual / count)
                total = math.fsum((influence.get(unit, 0.0), contribution))
            except OverflowError:
                return None
            if not math.isfinite(total):
                return None
            influence[unit] = total
    score_scale = max(map(abs, influence.values()), default=0.0)
    if score_scale == 0:
        standard_error = 0.0
    else:
        standard_error = score_scale * math.sqrt(
            math.fsum((value / score_scale) ** 2 for value in influence.values())
        )
    if not math.isfinite(standard_error):
        return None
    return effect, standard_error, len(set(unit_means[True]) | set(unit_means[False]))


def _pvalue(effect: float, standard_error: float) -> float:
    if standard_error == 0:
        return 1.0 if effect == 0 else 0.0
    return float(norm.sf(abs(effect) / standard_error))


def placebo_test(
    frame: MaterializedFrame, *, outcome: str, placebo_periods: tuple[int, ...], alpha: float = 0.05
) -> Diagnostic:
    """Test treatment-group differences on declared pre-treatment event periods."""
    if (
        isinstance(alpha, bool)
        or not isinstance(alpha, (int, float))
        or not math.isfinite(alpha)
        or not 0 < alpha < 1
    ):
        raise ValueError("alpha must be finite and strictly between zero and one")
    if not placebo_periods or any(
        isinstance(period, bool) or not isinstance(period, int) or period >= 0
        for period in placebo_periods
    ):
        raise ValueError("placebo_periods must contain negative integer event periods")
    values = _column(frame, "event_period")
    if values is None:
        return Diagnostic(
            "placebo",
            "unavailable",
            {"n_periods": len(placebo_periods), "max_abs_effect": None, "any_reject": None},
            {"alpha": float(alpha)},
            {"reason": "missing_event_period", "interpretation": _INTERPRETATION},
        )
    effects, p_values, counts = [], [], []
    for period in placebo_periods:
        contrast = _cluster_contrast(frame, outcome, [value == period for value in values])
        if contrast is None:
            return Diagnostic(
                "placebo",
                "unavailable",
                {"n_periods": len(placebo_periods), "max_abs_effect": None, "any_reject": None},
                {"alpha": float(alpha)},
                {
                    "reason": "incomplete_or_missing_treatment_outcome",
                    "event_period": period,
                    "interpretation": _INTERPRETATION,
                },
            )
        effect, se, n_units = contrast
        effects.append(effect)
        p_values.append(_pvalue(effect, se))
        counts.append(n_units)
    return Diagnostic(
        "placebo",
        "warn",
        {
            "n_periods": len(placebo_periods),
            "max_abs_effect": max(map(abs, effects)),
            "any_reject": any(p_value < alpha for p_value in p_values),
            "n_units": min(counts),
        },
        {"alpha": float(alpha)},
        {
            "p_values": tuple(p_values),
            "effects": tuple(effects),
            "event_periods": tuple(placebo_periods),
            "reason": "low_power" if min(counts) < 20 else "placebo_checked",
            "interpretation": _INTERPRETATION,
        },
    )


def negative_control_test(
    frame: MaterializedFrame, *, outcome: str, negative_control: str, alpha: float = 0.05
) -> Diagnostic:
    """Check treatment association with a declared negative-control outcome."""
    if (
        isinstance(alpha, bool)
        or not isinstance(alpha, (int, float))
        or not math.isfinite(alpha)
        or not 0 < alpha < 1
    ):
        raise ValueError("alpha must be finite and strictly between zero and one")
    if _column(frame, negative_control) is None:
        return Diagnostic(
            "negative_control",
            "unavailable",
            {"effect": None, "standard_error": None, "p_value": None, "n_units": 0},
            {"alpha": float(alpha)},
            {
                "reason": "missing_negative_control",
                "negative_control": negative_control,
                "interpretation": _INTERPRETATION,
            },
        )
    if _column(frame, outcome) is None:
        return Diagnostic(
            "negative_control",
            "unavailable",
            {"effect": None, "standard_error": None, "p_value": None, "n_units": 0},
            {"alpha": float(alpha)},
            {
                "reason": "missing_outcome",
                "negative_control": negative_control,
                "interpretation": _INTERPRETATION,
            },
        )
    contrast = _cluster_contrast(frame, negative_control, [True] * frame.table.num_rows)
    if contrast is None:
        return Diagnostic(
            "negative_control",
            "unavailable",
            {"effect": None, "standard_error": None, "p_value": None, "n_units": 0},
            {"alpha": float(alpha)},
            {
                "reason": "incomplete_or_missing_treatment_outcome",
                "negative_control": negative_control,
                "interpretation": _INTERPRETATION,
            },
        )
    effect, se, n_units = contrast
    p_value = _pvalue(effect, se)
    return Diagnostic(
        "negative_control",
        "warn",
        {
            "effect": effect,
            "standard_error": se,
            "p_value": p_value,
            "n_units": n_units,
            "reject": p_value < alpha,
        },
        {"alpha": float(alpha)},
        {
            "negative_control": negative_control,
            "reason": "low_power" if n_units < 20 else "negative_control_checked",
            "interpretation": _INTERPRETATION,
        },
    )
