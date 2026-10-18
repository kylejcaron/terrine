"""Typed, portable estimation outputs and diagnostic policy metadata."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Literal

from terrine.errors import freeze, thaw

__all__ = ["Diagnostic", "Estimate", "DiagnosticPolicy"]

_DIAGNOSTIC_STATUSES = {"pass", "warn", "fail", "not_applicable", "unavailable"}


def _finite_number(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")


def _immutable_map(value: Mapping, label: str):
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return freeze(value)


@dataclass(frozen=True, slots=True)
class Diagnostic:
    name: str
    status: Literal["pass", "warn", "fail", "not_applicable", "unavailable"]
    metrics: Mapping[str, float | int | str | bool | None]
    thresholds: Mapping[str, float]
    context: Mapping = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("diagnostic name must be a non-empty string")
        if self.status not in _DIAGNOSTIC_STATUSES:
            raise ValueError("invalid diagnostic status")
        if not isinstance(self.metrics, Mapping) or any(
            not isinstance(k, str) or not isinstance(v, (float, int, str, bool, type(None)))
            for k, v in self.metrics.items()
        ):
            raise TypeError("metrics must map strings to scalar values or None")
        for value in self.metrics.values():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("diagnostic metrics must be finite")
        if not isinstance(self.thresholds, Mapping) or any(
            not isinstance(k, str) for k in self.thresholds
        ):
            raise TypeError("thresholds must map strings to finite numbers")
        for value in self.thresholds.values():
            _finite_number(value, "threshold")
        context = _immutable_map(self.context, "context")
        if self.status in ("not_applicable", "unavailable") and not context.get("reason"):
            raise ValueError(
                "not-applicable and unavailable diagnostics require a reason in context"
            )
        if any(value is None for value in self.metrics.values()) and not (
            context.get("reason") or context.get("metric_reasons")
        ):
            raise ValueError("null diagnostic metrics require their reason in context")
        object.__setattr__(self, "metrics", _immutable_map(self.metrics, "metrics"))
        object.__setattr__(self, "thresholds", _immutable_map(self.thresholds, "thresholds"))
        object.__setattr__(self, "context", context)

    def to_dict(self) -> dict:
        """Return detached JSON-compatible diagnostic evidence."""
        return {item.name: thaw(getattr(self, item.name)) for item in fields(self)}

    @classmethod
    def from_dict(cls, payload: Mapping) -> Diagnostic:
        return cls(**dict(payload))


@dataclass(frozen=True, slots=True)
class Estimate:
    method: Literal["ipw", "aipw", "regression_adjustment", "panel_aipw"]
    estimand: Literal["ATE", "trial_weighted_ATE"]
    outcome: str
    value: float
    se: float
    ci: tuple[float, float]
    alpha: float
    n: int
    n_clusters: int
    weighting: Literal["unit", "target"]
    inference: Literal["crossfit_parametric_sandwich", "unit_cluster_crossfit_sandwich"]
    diagnostics: tuple[Diagnostic, ...]
    provenance: Mapping

    def __post_init__(self) -> None:
        if self.method not in ("ipw", "aipw", "regression_adjustment", "panel_aipw") or (
            self.estimand not in ("ATE", "trial_weighted_ATE")
        ):
            raise ValueError("unsupported estimate method or estimand")
        if (self.method == "panel_aipw") != (self.estimand == "trial_weighted_ATE"):
            raise ValueError("panel AIPW requires the trial-weighted ATE estimand")
        if not isinstance(self.outcome, str) or not self.outcome:
            raise ValueError("outcome must be a non-empty string")
        for label, value in (("value", self.value), ("se", self.se), ("alpha", self.alpha)):
            _finite_number(value, label)
        if self.se < 0 or not 0 < self.alpha < 1:
            raise ValueError("se must be nonnegative and alpha strictly between zero and one")
        if not isinstance(self.ci, tuple) or len(self.ci) != 2:
            raise TypeError("ci must be a pair")
        for endpoint in self.ci:
            _finite_number(endpoint, "confidence interval endpoint")
        for label, value in (("n", self.n), ("n_clusters", self.n_clusters)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{label} must be a positive integer")
        if self.method != "panel_aipw" and self.n_clusters != self.n:
            raise ValueError("n_clusters must equal n for fixed-cohort estimators")
        if (
            self.weighting not in ("unit", "target")
            or self.inference
            not in ("crossfit_parametric_sandwich", "unit_cluster_crossfit_sandwich")
            or (self.method == "panel_aipw") != (self.inference == "unit_cluster_crossfit_sandwich")
        ):
            raise ValueError("unsupported weighting or inference")
        if not isinstance(self.diagnostics, tuple) or not all(
            isinstance(item, Diagnostic) for item in self.diagnostics
        ):
            raise TypeError("diagnostics must be a tuple of Diagnostic")
        object.__setattr__(self, "provenance", _immutable_map(self.provenance, "provenance"))

    def to_dict(self) -> dict:
        """Return a portable result without observations or fitted row predictions."""
        payload = {
            item.name: thaw(getattr(self, item.name))
            for item in fields(self)
            if item.name != "diagnostics"
        }
        payload["diagnostics"] = [item.to_dict() for item in self.diagnostics]
        return {"version": 1, **payload}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_dict(cls, payload: Mapping) -> Estimate:
        values = dict(payload)
        if values.pop("version") != 1:
            raise ValueError("unsupported estimate version")
        values["ci"] = tuple(values["ci"])
        values["diagnostics"] = tuple(Diagnostic.from_dict(item) for item in values["diagnostics"])
        return cls(**values)

    @classmethod
    def from_json(cls, serialized: str) -> Estimate:
        return cls.from_dict(json.loads(serialized))


@dataclass(frozen=True, slots=True)
class DiagnosticPolicy:
    overlap_warn: float = 0.05
    overlap_fail: float = 0.01
    max_fail_share: float = 0.01
    min_arm_ess: float = 20
    smd_warn: float = 0.10
    smd_fail: float = 0.25

    def __post_init__(self) -> None:
        for name in (
            "overlap_warn",
            "overlap_fail",
            "max_fail_share",
            "min_arm_ess",
            "smd_warn",
            "smd_fail",
        ):
            _finite_number(getattr(self, name), name)
        if not 0 < self.overlap_fail < self.overlap_warn < 0.5:
            raise ValueError("overlap thresholds must satisfy 0 < fail < warn < 0.5")
        if not 0 <= self.max_fail_share <= 1:
            raise ValueError("max_fail_share must lie in [0, 1]")
        if self.min_arm_ess <= 0:
            raise ValueError("min_arm_ess must be positive")
        if not 0 <= self.smd_warn < self.smd_fail:
            raise ValueError("SMD thresholds must satisfy 0 <= warn < fail")
