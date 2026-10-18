"""Trial-weighted AIPW with unit-grouped cross-fitting and cluster inference."""

from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pyarrow as pa

from terrine.data import MaterializedFrame
from terrine.diagnostics.overlap import _overlap_arrays
from terrine.errors import RefusalSpec
from terrine.estimate._data import (
    INVALID_OUTCOME,
    MISSING_DATA,
    POSTBASELINE_COVARIATE,
    UNSUPPORTED_DESIGN,
    PreparedData,
    _numeric,
    normalize_weights,
    safe_cutoff,
)
from terrine.estimate.folds import assign_unit_folds, fold_digest
from terrine.estimate.inference import (
    NONFINITE_RESULT,
    _mean,
    _standard_error,
    _unit_influence,
    critical_value,
    pseudo_outcome,
)
from terrine.estimate.nuisance import (
    CrossFitPredictions,
    Encoder,
    FoldFit,
    NuisanceSpec,
    fit_logistic,
    fit_outcome,
)
from terrine.results import DiagnosticPolicy, Estimate

_PANEL_INPUT = RefusalSpec(
    "terrine.estimate.panel_input",
    "Panel AIPW requires complete unsampled trial rows with declared roles and weights.",
)
_PANEL_SUPPORT = RefusalSpec(
    "terrine.estimate.panel_support",
    "Each trial and unit-held-out training complement must have both arms and supported fits.",
)
_TRIAL_WEIGHT = RefusalSpec(
    "terrine.estimate.invalid_trial_weights",
    "Declared positive aggregation weights must cover exactly the observed trials.",
)


def _array(frame: MaterializedFrame, name: str, *, weight: bool = False) -> np.ndarray:
    if name not in frame.table.column_names:
        raise _PANEL_INPUT.error(reason="missing_column", column=name)
    column = frame.table[name]
    if column.null_count:
        raise MISSING_DATA.error(column=name)
    values, _ = _numeric(column, name, weight=weight)
    return values


def _trial_weights(frame: MaterializedFrame, trials: tuple[str, ...]) -> dict[str, float]:
    panel = frame.provenance.panel
    meta = panel.trial_weight if panel else None
    if meta is None:
        raise _TRIAL_WEIGHT.error(reason="missing_declaration")
    domain = set(panel.trials) if panel else set()
    if meta.kind == "equal_trial":
        if not domain or domain != set(trials):
            raise _TRIAL_WEIGHT.error(reason="trial_domain_mismatch")
        return dict.fromkeys(trials, 1.0 / len(trials))
    values = meta.values
    if (
        not isinstance(values, Mapping)
        or set(values) != set(trials)
        or (domain and domain != set(trials))
    ):
        raise _TRIAL_WEIGHT.error(reason="trial_domain_mismatch")
    raw = [float(values[t]) for t in trials]
    if any(not math.isfinite(v) or v <= 0 for v in raw):
        raise _TRIAL_WEIGHT.error(reason="positive_finite_masses_required")
    scale = max(raw)
    scaled = [v / scale for v in raw]
    total = math.fsum(scaled)
    if not math.isfinite(total) or total <= 0:
        raise _TRIAL_WEIGHT.error(reason="invalid_normalization")
    return {trial: value / total for trial, value in zip(trials, scaled, strict=True)}


def panel_aipw(
    frame: MaterializedFrame,
    *,
    outcome: str,
    treatment: str,
    trial: str,
    target_weight: str | None = None,
    folds: int = 5,
    seed: int = 0,
    alpha: float = 0.05,
    nuisance: NuisanceSpec | None = None,
) -> Estimate:
    """Estimate the declared weighted average of trial-specific AIPW effects."""
    z = critical_value(alpha)
    if not isinstance(frame, MaterializedFrame):
        raise UNSUPPORTED_DESIGN.error(reason="typed_materialized_frame_required")
    roles, provenance, table = frame.roles, frame.provenance, frame.table
    if (
        provenance.shape not in ("sequential", "event_panel")
        or roles.trial != trial
        or roles.treatment != treatment
        or trial is None
        or treatment is None
        or provenance.treatment_design == "grace_initiation"
        or provenance.panel is None
        or provenance.panel.treatment is None
    ):
        raise _PANEL_INPUT.error(reason="trial_roles_or_treatment_design")
    if provenance.sampling is not None or provenance.weight_kind == "sampled_control":
        raise _PANEL_INPUT.error(reason="sampled_analysis_population_unsupported")
    if outcome not in roles.outcomes or outcome not in frame.columns:
        raise INVALID_OUTCOME.error(reason="unknown_outcome")
    if target_weight is None:
        target_weight = roles.target_weight
    if target_weight != roles.target_weight:
        raise _PANEL_INPUT.error(reason="target_weight_must_be_declared_in_roles")
    if target_weight is not None and not safe_cutoff(provenance.feature_cutoffs.get(target_weight)):
        raise POSTBASELINE_COVARIATE.error(column=target_weight)
    feature_names = roles.features
    for required in (roles.unit, roles.t0, trial, treatment):
        if required not in table.column_names:
            raise _PANEL_INPUT.error(reason="missing_column", column=required)
    for name in feature_names:
        if not safe_cutoff(provenance.feature_cutoffs.get(name)):
            raise POSTBASELINE_COVARIATE.error(column=name)
    meta = frame.columns[outcome]
    if (
        meta.role != "outcome"
        or meta.window is None
        or meta.window.start < 0
        or meta.window.end <= meta.window.start
        or meta.complete_column is None
        or meta.complete_column not in table.column_names
    ):
        raise INVALID_OUTCOME.error(reason="fixed_complete_trial_outcome_required", outcome=outcome)
    complete = table[meta.complete_column]
    if (
        not pa.types.is_boolean(complete.type)
        or complete.null_count
        or not all(complete.to_pylist())
    ):
        raise _PANEL_INPUT.error(reason="incomplete_trial_outcome", outcome=outcome)
    t0 = table[roles.t0]
    if not pa.types.is_timestamp(t0.type) or t0.type.tz is None or t0.null_count:
        raise _PANEL_INPUT.error(reason="aware_nonnull_trial_time_zero_required")
    ids = table[roles.unit]
    if ids.null_count or not (
        pa.types.is_int64(ids.type)
        or pa.types.is_string(ids.type)
        or pa.types.is_large_string(ids.type)
    ):
        raise _PANEL_INPUT.error(reason="nonnull_int64_or_utf8_units_required")
    units = ids.to_pylist()
    if roles.cluster_unit is not None and roles.cluster_unit not in table.column_names:
        raise _PANEL_INPUT.error(reason="missing_cluster_unit_column")
    clusters = units if roles.cluster_unit is None else table[roles.cluster_unit].to_pylist()
    if len(clusters) != len(units) or any(
        value != unit for value, unit in zip(clusters, units, strict=True)
    ):
        raise _PANEL_INPUT.error(reason="cluster_unit_must_equal_independent_unit")
    trial_values = table[trial].to_pylist()
    if any(value is None or not isinstance(value, str) for value in trial_values):
        raise _PANEL_INPUT.error(reason="nonnull_string_trial_required")
    if len(set(zip(units, trial_values, strict=True))) != len(units):
        raise _PANEL_INPUT.error(reason="duplicate_unit_trial_rows")
    observed = tuple(sorted(set(trial_values)))
    if len(observed) < 2:
        raise _TRIAL_WEIGHT.error(reason="at_least_two_trials_required")
    lambdas = _trial_weights(frame, observed)
    a = _array(frame, treatment)
    y = _array(frame, outcome)
    if not np.all((a == 0) | (a == 1)):
        raise _PANEL_INPUT.error(reason="binary_treatment_required")
    if target_weight is None:
        q_raw = np.ones(len(units))
    else:
        q_raw = _array(frame, target_weight, weight=True)
    features: dict[str, np.ndarray] = {}
    kinds: dict[str, str] = {}
    for name in feature_names:
        values = table[name]
        if values.null_count:
            raise MISSING_DATA.error(column=name)
        if (
            pa.types.is_floating(values.type)
            or pa.types.is_integer(values.type)
            or pa.types.is_decimal(values.type)
        ):
            numeric, _ = _numeric(values, name)
            features[name] = numeric
            kinds[name] = "numeric"
        elif (
            pa.types.is_string(values.type)
            or pa.types.is_large_string(values.type)
            or pa.types.is_boolean(values.type)
        ):
            features[name] = np.asarray(values.to_pylist(), dtype=object)
            kinds[name] = "categorical"
        else:
            raise _PANEL_INPUT.error(reason="unsupported_feature_type", column=name)
    global_units = tuple(sorted(set(units), key=lambda u: (type(u).__name__, u)))
    unit_fold = assign_unit_folds(global_units, folds=folds, seed=seed)
    fold_by_unit = dict(zip(global_units, unit_fold, strict=True))
    row_folds = np.fromiter(
        (fold_by_unit[unit] for unit in units), dtype=np.int64, count=len(units)
    )
    spec = NuisanceSpec() if nuisance is None else nuisance
    if not isinstance(spec, NuisanceSpec):
        raise TypeError("nuisance requires NuisanceSpec")
    cluster_if: dict[int | str, float] = dict.fromkeys(global_units, 0.0)
    estimate = 0.0
    for trial_name in observed:
        index = np.flatnonzero(np.asarray(trial_values, dtype=object) == trial_name)
        if not len(index) or len(np.unique(a[index])) != 2:
            raise _PANEL_SUPPORT.error(trial=trial_name, reason="both_arms_required")
        q = normalize_weights(q_raw[index], target_weight or "unit_target_weight")
        local_features = {name: values[index] for name, values in features.items()}
        local = PreparedData(
            [units[i] for i in index],
            a[index],
            y[index],
            q,
            local_features,
            kinds,
            outcome,
            {},
        )
        pred_e = np.empty(local.n)
        pred_1m = np.empty(local.n)
        pred0 = np.empty(local.n)
        pred1 = np.empty(local.n)
        fits = []
        local_folds = row_folds[index]
        for fold in range(folds):
            train = local_folds != fold
            heldout = ~train
            if not np.any(heldout):
                continue
            if len(np.unique(local.a[train])) != 2:
                raise _PANEL_SUPPORT.error(
                    trial=trial_name, fold=fold, reason="training_complement_arms"
                )
            encoder = Encoder.fit(local.features, local.kinds, train, spec.propensity)
            x = encoder.transform_n(local.features, local.n)
            outcome_encoder = Encoder.fit(local.features, local.kinds, train, spec.outcome)
            ox = outcome_encoder.transform_n(local.features, local.n)
            train0, train1 = train & (local.a == 0), train & (local.a == 1)
            if not np.any(train0) or not np.any(train1):
                raise _PANEL_SUPPORT.error(
                    trial=trial_name, fold=fold, reason="training_arm_absent"
                )
            theta_e = fit_logistic(x[train], local.a[train], local.q[train], fold=fold)
            theta0 = fit_outcome(ox[train0], local.y[train0], local.q[train0], fold=fold, arm=0)
            theta1 = fit_outcome(ox[train1], local.y[train1], local.q[train1], fold=fold, arm=1)
            eta = x[heldout] @ theta_e
            pred_e[heldout] = -np.logaddexp(0, -eta)
            pred_1m[heldout] = -np.logaddexp(0, eta)
            pred0[heldout], pred1[heldout] = ox[heldout] @ theta0, ox[heldout] @ theta1
            fits.append(FoldFit(fold, train, x, ox, theta_e, theta0, theta1))
        pred = CrossFitPredictions(
            pred_e,
            pred_1m,
            pred0,
            pred1,
            local_folds,
            tuple(fits),
            fold_digest(local.units, local_folds),
        )
        support = _overlap_arrays(local.a, local.q, pred.log_e, pred.log1m_e, DiagnosticPolicy())
        if support.status == "fail":
            raise _PANEL_SUPPORT.error(
                trial=trial_name, reason="unsupported_propensity_overlap", metrics=support.metrics
            )
        psi = pseudo_outcome("aipw", local.a, local.y, pred.log_e, pred.log1m_e, pred.mu0, pred.mu1)
        tau = _mean(psi, local.q)
        estimate += lambdas[trial_name] * tau
        influence = _unit_influence(local, pred, "aipw", tau)
        global_scale = len(global_units) / local.n
        for unit, contribution in zip(local.units, influence, strict=True):
            cluster_if[unit] += lambdas[trial_name] * global_scale * float(contribution)
    values = np.asarray(list(cluster_if.values()), dtype=float)
    se = _standard_error(values)
    width = z * se
    ci = (estimate - width, estimate + width)
    if not all(math.isfinite(v) for v in (*ci, width)):
        raise NONFINITE_RESULT.error(stage="confidence_interval")
    return Estimate(
        "panel_aipw",
        "trial_weighted_ATE",
        outcome,
        estimate,
        se,
        ci,
        alpha,
        len(units),
        len(global_units),
        "target" if target_weight is not None else "unit",
        "unit_cluster_crossfit_sandwich",
        (),
        {
            "trials": observed,
            "trial_weights": lambdas,
            "target_weight": target_weight,
            "fold_digest": fold_digest(global_units, unit_fold),
            "folds": folds,
            "seed": seed,
            "n_trial_rows": len(units),
            "n_clusters": len(global_units),
            "influence_normalization": "trial_target_weighted_unit_cluster",
        },
    )
