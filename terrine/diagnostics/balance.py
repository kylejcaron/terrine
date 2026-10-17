"""Covariate balance with a fixed preweight reliability-variance scale."""

from __future__ import annotations

import json
import math

import numpy as np
from scipy.special import softmax

from terrine.data import MaterializedFrame
from terrine.diagnostics._data import InvalidDiagnosticData, arrays, category_key, unavailable
from terrine.results import Diagnostic, DiagnosticPolicy


def _normalized(log_weights: np.ndarray) -> np.ndarray:
    return softmax(log_weights)


def _moments(values: np.ndarray, weights: np.ndarray) -> tuple[float, float | None]:
    mean = math.fsum(float(w) * float(v) for w, v in zip(weights, values, strict=True))
    denominator = 1 - math.fsum(float(w) ** 2 for w in weights)
    if denominator <= 0:
        return mean, None
    variance = (
        math.fsum(float(w) * float(v - mean) ** 2 for w, v in zip(weights, values, strict=True))
        / denominator
    )
    return mean, variance


def _smd(difference: float, scale: float) -> tuple[float | None, str | None]:
    if scale == 0:
        return (0.0, None) if difference == 0 else (None, "zero_scale_unequal_means")
    value = difference / scale
    return (value, None) if math.isfinite(value) else (None, "nonfinite_standardized_contrast")


def _balance_arrays(
    a: np.ndarray,
    q: np.ndarray,
    features: dict[str, np.ndarray],
    kinds: dict[str, str],
    log_treatment_weights: np.ndarray | None,
    policy: DiagnosticPolicy,
) -> Diagnostic:
    thresholds = {"smd_warn": policy.smd_warn, "smd_fail": policy.smd_fail}
    if not features:
        return Diagnostic("balance", "not_applicable", {}, thresholds, {"reason": "no_features"})
    if log_treatment_weights is not None:
        adjusted = np.asarray(log_treatment_weights, dtype=float)
        if adjusted.shape != a.shape or not np.all(np.isfinite(adjusted)):
            return unavailable("balance", "invalid_complete_adjusted_log_weights")
    else:
        adjusted = None
    before_weights = [_normalized(np.log(q[a == arm])) for arm in (0, 1)]
    after_weights = (
        None if adjusted is None else [_normalized(adjusted[a == arm]) for arm in (0, 1)]
    )
    contrasts = []
    for name, values in features.items():
        if kinds[name] == "categorical":
            levels = sorted(set(values), key=category_key)
            for level in levels:
                label = json.dumps(
                    bool(level) if isinstance(level, (bool, np.bool_)) else str(level),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                contrasts.append((f"{name}[level={label}]", (values == level).astype(float)))
        else:
            contrasts.append((name, values))
    metrics = {}
    reasons = {}
    max_after = 0.0
    undefined_after = False
    unavailable_scale = False
    for name, values in contrasts:
        reference = float(np.min(values)) / 2 + float(np.max(values)) / 2
        centered = values - reference
        magnitude = float(np.max(np.abs(centered)))
        normalized = centered / (magnitude if magnitude else 1.0)
        moments = [_moments(normalized[a == arm], before_weights[arm]) for arm in (0, 1)]
        variance0, variance1 = moments[0][1], moments[1][1]
        if variance0 is None or variance1 is None:
            before = after = None
            reason_before = reason_after = "insufficient_preweight_variance_degrees_of_freedom"
            unavailable_scale = True
        else:
            scale = math.sqrt((variance0 + variance1) / 2)
            before, reason_before = _smd(moments[1][0] - moments[0][0], scale)
            if after_weights is None:
                after, reason_after = None, "no_balance_adjustment"
            else:
                means = [
                    math.fsum(
                        float(w) * float(v)
                        for w, v in zip(after_weights[arm], normalized[a == arm], strict=True)
                    )
                    for arm in (0, 1)
                ]
                after, reason_after = _smd(means[1] - means[0], scale)
        metrics[f"{name}:before"] = before
        metrics[f"{name}:after"] = after
        if before is None:
            reasons[f"{name}:before"] = reason_before
        if after is None:
            reasons[f"{name}:after"] = reason_after
            if after_weights is not None:
                undefined_after = True
        else:
            max_after = max(max_after, abs(after))
    metrics["max_abs_after_smd"] = None if after_weights is None or undefined_after else max_after
    if metrics["max_abs_after_smd"] is None:
        reasons["max_abs_after_smd"] = (
            "no_balance_adjustment" if after_weights is None else "undefined_postweight_contrast"
        )
    if adjusted is None:
        status = "not_applicable"
    elif unavailable_scale:
        status = "unavailable"
    elif undefined_after or max_after > policy.smd_fail:
        status = "fail"
    elif max_after > policy.smd_warn:
        status = "warn"
    else:
        status = "pass"
    context = {
        "metric_reasons": reasons,
        "scale": "fixed_target_preweight_reliability_variance",
        "adjusted_weight_semantics": "complete_weights_including_target",
    }
    if status == "not_applicable":
        context["reason"] = "no_balance_adjustment"
    elif status == "unavailable":
        context["reason"] = "insufficient_preweight_variance_degrees_of_freedom"
    return Diagnostic("balance", status, metrics, thresholds, context)


def balance(
    frame: MaterializedFrame,
    *,
    log_treatment_weights: np.ndarray | None = None,
    policy: DiagnosticPolicy | None = None,
) -> Diagnostic:
    """Describe target imbalance and screen only genuinely adjusted postweight contrasts."""
    try:
        a, q, features, kinds = arrays(frame)
        return _balance_arrays(
            a, q, features, kinds, log_treatment_weights, policy or DiagnosticPolicy()
        )
    except InvalidDiagnosticData as exc:
        return unavailable("balance", str(exc))
    except (ValueError, TypeError, OverflowError):
        return unavailable("balance", "invalid_measured_input_conversion")
