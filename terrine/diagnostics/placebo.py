"""Evidence-only placebo and negative-control diagnostics."""

from __future__ import annotations

import math

from scipy.stats import chi2

from terrine.data import MaterializedFrame
from terrine.results import Diagnostic

__all__ = ["placebo_test", "negative_control_test"]

_INTERPRETATION = "placebo_evidence_does_not_authorize_causal_interpretation"


def _column(frame: MaterializedFrame, name: str):
    if name not in frame.table.column_names:
        return None
    return frame.table[name].to_pylist()


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
        if value is None or not isinstance(arm, bool) or unit is None:
            return None
        groups[arm].setdefault(unit, []).append(float(value))
    if not groups[True] or not groups[False]:
        return None
    means = {}
    influence = {}
    for arm in (True, False):
        per_unit = {unit: math.fsum(rows) / len(rows) for unit, rows in groups[arm].items()}
        means[arm] = math.fsum(per_unit.values()) / len(per_unit)
        count = len(per_unit)
        influence.update(
            {
                unit: (1.0 if arm else -1.0) * (value - means[arm]) / count
                for unit, value in per_unit.items()
            }
        )
    effect = means[True] - means[False]
    variance = math.fsum(value * value for value in influence.values())
    return effect, math.sqrt(max(variance, 0.0)), len(groups[True]) + len(groups[False])


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
    effects, rejects, counts = [], [], []
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
        rejects.append(abs(effect / se) > 1.959963984540054 if se > 0 else effect != 0)
        counts.append(n_units)
    return Diagnostic(
        "placebo",
        "warn",
        {
            "n_periods": len(placebo_periods),
            "max_abs_effect": max(map(abs, effects)),
            "any_reject": any(rejects),
            "n_units": min(counts),
        },
        {"alpha": float(alpha)},
        {
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
    p_value = float(chi2.sf((effect / se) ** 2, 1)) if se > 0 else float(effect == 0)
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
