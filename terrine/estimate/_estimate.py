"""Shared baseline ATE assembly; estimators differ only in their declared score."""

from __future__ import annotations

import math
from dataclasses import replace
from typing import Literal

import numpy as np

from terrine.data import MaterializedFrame
from terrine.diagnostics.balance import _balance_arrays
from terrine.diagnostics.overlap import _overlap_arrays
from terrine.estimate._data import DIAGNOSTIC_FAILURE, prepare
from terrine.estimate.inference import (
    NONFINITE_RESULT,
    _mean,
    critical_value,
    pseudo_outcome,
    sandwich,
)
from terrine.estimate.nuisance import NuisanceSpec, _crossfit_prepared
from terrine.results import DiagnosticPolicy, Estimate


def estimate(
    frame: MaterializedFrame,
    *,
    method: Literal["ipw", "aipw", "regression_adjustment"],
    outcome: str | None,
    estimand: str,
    folds: int,
    seed: int,
    alpha: float,
    nuisance: NuisanceSpec | None,
    diagnostics: DiagnosticPolicy | None,
) -> Estimate:
    z = critical_value(alpha)
    data = prepare(frame, outcome=outcome, estimand=estimand)
    specification = NuisanceSpec() if nuisance is None else nuisance
    policy = DiagnosticPolicy() if diagnostics is None else diagnostics
    if not isinstance(specification, NuisanceSpec) or not isinstance(policy, DiagnosticPolicy):
        raise TypeError("nuisance and diagnostics require NuisanceSpec and DiagnosticPolicy")
    pred = _crossfit_prepared(data, method=method, folds=folds, seed=seed, nuisance=specification)
    overlap = _overlap_arrays(data.a, data.q, pred.log_e, pred.log1m_e, policy)
    overlap = replace(
        overlap,
        context={
            **overlap.context,
            "weight_role": "diagnostic_only_propensity_reference"
            if method == "regression_adjustment"
            else "estimator_inverse_probability_weights",
        },
    )
    adjusted = (
        None
        if method == "regression_adjustment"
        else np.log(data.q) - np.where(data.a == 1, pred.log_e, pred.log1m_e)
    )
    balance = _balance_arrays(data.a, data.q, data.features, data.kinds, adjusted, policy)
    evidence = (overlap, balance)
    if any(item.status in ("fail", "unavailable") for item in evidence):
        raise DIAGNOSTIC_FAILURE.error(
            diagnostics=[item.to_dict() for item in evidence],
            route=(
                "Inspect support, independent sample size, and declared nuisance terms; "
                "no automatic trimming is performed."
            ),
        )
    psi = pseudo_outcome(method, data.a, data.y, pred.log_e, pred.log1m_e, pred.mu0, pred.mu1)
    value = _mean(psi, data.q)
    se = sandwich(data, pred, method, value)
    width = z * se
    ci = (value - width, value + width)
    if not all(math.isfinite(v) for v in (*ci, width)):
        raise NONFINITE_RESULT.error(stage="confidence_interval")
    assumptions = [
        "consistency and well-defined baseline treatment",
        "no interference between independent units",
        "baseline conditional exchangeability",
        "positivity on target support",
        "correct temporal construction and complete follow-up",
        "features are not treatment descendants",
        "finite second moments and nonsingular population Jacobian",
        "regular fixed-dimensional nuisance fits and fixed finite fold count",
    ]
    assumptions.append(
        {
            "ipw": "correct propensity model",
            "aipw": "at least one of propensity or arm outcome model families is correct",
            "regression_adjustment": "correct arm outcome model family",
        }[method]
    )
    return Estimate(
        method=method,
        estimand="ATE",
        outcome=data.outcome,
        value=float(value),
        se=se,
        ci=ci,
        alpha=alpha,
        n=data.n,
        n_clusters=data.n,
        weighting="unit" if frame.roles.weight is None else "target",
        inference="crossfit_parametric_sandwich",
        diagnostics=evidence,
        provenance={
            "input": frame.to_metadata(),
            "input_precision": data.precision,
            "nuisance": specification.model_dump(mode="json"),
            "folds": {
                "version": "terrine-fold-v1",
                "seed": seed,
                "count": folds,
                "digest": pred.digest,
                "encoding": (
                    "type tag i/s, uint64 byte length, signed int64 big-endian two's "
                    "complement or exact UTF-8; SHA256 first uint64 modulo count"
                ),
            },
            "weight_semantics": "unit"
            if frame.roles.weight is None
            else "fixed_baseline_target_function_mean_normalized_after_max_scaling",
            "inference_assumptions": assumptions,
            "inference_scope": (
                "asymptotic parametric, not finite-sample or generic black-box inference"
            ),
            "diagnostic_policy": {
                name: getattr(policy, name)
                for name in (
                    "overlap_warn",
                    "overlap_fail",
                    "max_fail_share",
                    "min_arm_ess",
                    "smd_warn",
                    "smd_fail",
                )
            },
        },
    )
