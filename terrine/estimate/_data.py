"""Baseline design validation and one-pass measured-input conversion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

import numpy as np
import pyarrow as pa

from terrine.data import FeatureCutoff, MaterializedFrame
from terrine.errors import RefusalSpec
from terrine.estimate.folds import ONE_ARM

TREATMENT_REQUIRED = RefusalSpec(
    "terrine.estimate.treatment_required", "Declare a baseline binary treatment before estimation."
)
UNSUPPORTED_ESTIMAND = RefusalSpec(
    "terrine.estimate.unsupported_estimand", "Only the baseline ATE is supported."
)
UNSUPPORTED_DESIGN = RefusalSpec(
    "terrine.estimate.unsupported_design", "Unsupported baseline design: {reason}."
)
POSTBASELINE_COVARIATE = RefusalSpec(
    "terrine.estimate.postbaseline_covariate", "Column {column} needs a safe prebaseline cutoff."
)
INVALID_OUTCOME = RefusalSpec(
    "terrine.estimate.invalid_outcome", "Select one declared complete fixed-horizon outcome."
)
VARIABLE_ORIGIN_OUTCOME = RefusalSpec(
    "terrine.estimate.variable_origin_outcome",
    "Estimators require an outcome measured from fixed time zero.",
)
MISSING_DATA = RefusalSpec(
    "terrine.estimate.missing_data",
    "Column {column} contains missing values; no imputation or row removal is performed.",
)
INCOMPLETE_FOLLOWUP = RefusalSpec(
    "terrine.estimate.incomplete_followup",
    "Outcome {outcome} lacks complete follow-up; supply complete labels "
    "or a supported censoring method.",
)
NONFINITE_DATA = RefusalSpec(
    "terrine.estimate.nonfinite_data", "Column {column} contains nonfinite numerical values."
)
INVALID_WEIGHT = RefusalSpec(
    "terrine.estimate.invalid_weight",
    "Column {column} must contain finite strictly positive baseline target weights.",
)
UNSUPPORTED_WEIGHT_KIND = RefusalSpec(
    "terrine.estimate.unsupported_weight_kind",
    "Only fixed prebaseline treatment-independent target weights are supported.",
)
DUPLICATE_UNIT = RefusalSpec(
    "terrine.estimate.duplicate_unit", "Baseline ATE requires one row per independent unit."
)
UNSUPPORTED_FEATURE_DTYPE = RefusalSpec(
    "terrine.estimate.unsupported_feature_dtype",
    "Column {column} has an unsupported measured-input type.",
)
INPUT_PRECISION = RefusalSpec(
    "terrine.estimate.input_precision",
    "Column {column} violates the numerical input policy: {reason}.",
)
DIAGNOSTIC_FAILURE = RefusalSpec(
    "terrine.estimate.diagnostic_failure",
    "Required diagnostic evidence does not authorize an estimate.",
)


@dataclass(frozen=True)
class PreparedData:
    units: list[int] | list[str]
    a: np.ndarray
    y: np.ndarray
    q: np.ndarray
    features: dict[str, np.ndarray]
    kinds: dict[str, str]
    outcome: str
    precision: dict[str, dict[str, Any]]

    @property
    def n(self) -> int:
        return len(self.a)


def safe_cutoff(cutoff: FeatureCutoff | None) -> bool:
    return cutoff is not None and (
        cutoff.latest_seconds < 0 or (cutoff.latest_seconds == 0 and cutoff.strict)
    )


def normalize_weights(weights: np.ndarray, column: str) -> np.ndarray:
    if weights.size == 0 or not np.all(np.isfinite(weights)) or np.any(weights <= 0):
        raise INVALID_WEIGHT.error(column=column)
    scaled = weights / np.max(weights)
    if np.any(scaled == 0):
        raise INPUT_PRECISION.error(column=column, reason="target_weight_dynamic_range")
    # The mean-one convention conditions the estimating equations without changing the target.
    return scaled * (len(weights) / math.fsum(scaled))


def _numeric(
    column: pa.ChunkedArray, name: str, *, weight: bool = False
) -> tuple[np.ndarray, dict[str, Any]]:
    dtype = column.type
    info: dict[str, Any] = {
        "conversion_policy": "float64_v1",
        "max_conversion_error": 0.0,
        "max_input_rounding_bound": 0.0,
    }
    original = None
    if pa.types.is_integer(dtype):
        original = column.to_numpy(zero_copy_only=False)
        outside = original > 2**53
        if pa.types.is_signed_integer(dtype):
            outside |= original < -(2**53)
        if np.any(outside):
            raise INPUT_PRECISION.error(column=name, reason="integer_outside_exact_float64_domain")
    elif pa.types.is_decimal(dtype):
        originals = column.to_pylist()
        casts = []
        with localcontext() as ctx:
            ctx.prec = 200
            errors = []
            for value in originals:
                cast = float(value)
                if not math.isfinite(cast):
                    raise INPUT_PRECISION.error(column=name, reason="decimal_overflow")
                error = abs(Decimal.from_float(cast) - value)
                if error > Decimal("1e-12") + Decimal("1e-12") * abs(value):
                    raise INPUT_PRECISION.error(column=name, reason="decimal_rounding_policy")
                bound = float(error)
                if Decimal.from_float(bound) < error:
                    bound = math.nextafter(bound, math.inf)
                errors.append(bound)
                casts.append(cast)
            info["max_conversion_error"] = max(errors, default=0.0)
        values = np.asarray(casts, dtype=np.float64)
    elif not (pa.types.is_floating(dtype) or pa.types.is_boolean(dtype)):
        raise UNSUPPORTED_FEATURE_DTYPE.error(column=name, dtype=str(dtype))
    if not pa.types.is_decimal(dtype):
        try:
            if original is None:
                original = column.to_numpy(zero_copy_only=False)
            values = original.astype(np.float64, copy=False)
        except (ValueError, TypeError, OverflowError) as exc:
            raise INPUT_PRECISION.error(column=name, reason="float64_conversion") from exc
    if not np.all(np.isfinite(values)):
        raise (INVALID_WEIGHT if weight else NONFINITE_DATA).error(column=name)
    if pa.types.is_floating(dtype):
        assert original is not None
        format_info = np.finfo(original.dtype)
        precision_ratio = 2.0 ** (52 - format_info.nmant)
        minimum_spacing = float(format_info.smallest_subnormal)
        # Compute the original format's spacing in binary64, including its top binade.
        # Bounds round outward when half a binary64 subnormal is not representable.
        info["max_input_rounding_bound"] = max(
            (
                max(max(math.ulp(float(v)) * precision_ratio, minimum_spacing) / 2, math.ulp(0.0))
                for v in values
            ),
            default=0.0,
        )
    return values, info


def prepare(
    frame: MaterializedFrame, *, outcome: str | None = None, estimand: str = "ATE"
) -> PreparedData:
    if estimand != "ATE":
        raise UNSUPPORTED_ESTIMAND.error(estimand=str(estimand))
    if not isinstance(frame, MaterializedFrame):
        raise UNSUPPORTED_DESIGN.error(reason="typed_materialized_frame_required")
    roles, provenance, table = frame.roles, frame.provenance, frame.table
    if roles.treatment is None:
        raise TREATMENT_REQUIRED.error()
    if outcome is None:
        if len(roles.outcomes) != 1:
            raise INVALID_OUTCOME.error(
                reason="explicit_outcome_required", count=len(roles.outcomes)
            )
        outcome = roles.outcomes[0]
    if outcome not in roles.outcomes or outcome not in frame.columns:
        raise INVALID_OUTCOME.error(reason="unknown_outcome")
    timing = provenance.expression_timings.get(outcome)
    if timing is None or timing.kind != "fixed_t0":
        raise VARIABLE_ORIGIN_OUTCOME.error()
    if (
        provenance.shape != "cross_section"
        or provenance.treatment_design != "baseline_binary"
        or not provenance.treatment_known_at_t0
        or provenance.treatment_knowledge is None
    ):
        raise UNSUPPORTED_DESIGN.error(reason="fixed_cohort_baseline_treatment_required")
    for name in roles.features:
        if not safe_cutoff(provenance.feature_cutoffs.get(name)):
            raise POSTBASELINE_COVARIATE.error(column=name)
    for name, cutoff in provenance.feature_cutoffs.items():
        if not safe_cutoff(cutoff):
            raise POSTBASELINE_COVARIATE.error(column=name)
    for name, cutoff in provenance.eligibility_cutoffs.items():
        if not safe_cutoff(cutoff):
            raise UNSUPPORTED_DESIGN.error(reason="unsafe_eligibility_cutoff", column=name)
    summary = provenance.eligibility_summary
    if provenance.eligibility_cutoffs and summary is None:
        raise UNSUPPORTED_DESIGN.error(reason="missing_eligibility_summary")
    if summary is not None and (
        summary.unknown != 0
        or summary.retained != table.num_rows
        or (
            not provenance.eligibility_cutoffs
            and (summary.excluded_false != 0 or summary.candidate != table.num_rows)
        )
    ):
        raise UNSUPPORTED_DESIGN.error(reason="unresolved_or_inconsistent_eligibility_summary")
    if roles.weight is not None and (
        provenance.weight_kind != "baseline_function"
        or not safe_cutoff(provenance.weight_cutoff)
        or provenance.weight_depends_on_treatment is not False
    ):
        raise UNSUPPORTED_WEIGHT_KIND.error(reason="baseline_function_proof_required")
    cache: dict[str, pa.ChunkedArray] = {}

    def get(name: str) -> pa.ChunkedArray:
        if name not in table.column_names:
            raise UNSUPPORTED_DESIGN.error(reason="missing_column", column=name)
        if name not in cache:
            cache[name] = table[name]
        return cache[name]

    ids = get(roles.unit)
    if (
        not (
            pa.types.is_int64(ids.type)
            or pa.types.is_string(ids.type)
            or pa.types.is_large_string(ids.type)
        )
        or ids.null_count
    ):
        raise UNSUPPORTED_DESIGN.error(reason="nonnull_int64_or_utf8_unit_required")
    units = ids.to_pylist()
    if len(set(units)) != len(units):
        raise DUPLICATE_UNIT.error(n=table.num_rows, n_units=len(set(units)))
    if roles.cluster_unit is not None:
        clusters = get(roles.cluster_unit)
        if clusters.null_count or len(set(clusters.to_pylist())) != table.num_rows:
            raise UNSUPPORTED_DESIGN.error(reason="cluster_must_identify_independent_units")
    t0 = get(roles.t0)
    if (
        not pa.types.is_timestamp(t0.type)
        or t0.type.tz is None
        or t0.null_count
        or len(np.unique(t0.cast(pa.int64()).to_numpy(zero_copy_only=False))) != 1
    ):
        raise UNSUPPORTED_DESIGN.error(reason="common_aware_time_zero_required")
    meta = frame.columns[outcome]
    if (
        meta.role != "outcome"
        or meta.window is None
        or meta.window.start < 0
        or meta.window.end <= meta.window.start
    ):
        raise INVALID_OUTCOME.error(reason="fixed_postbaseline_horizon_required", outcome=outcome)
    if meta.complete_column is None or meta.complete_column not in table.column_names:
        raise INCOMPLETE_FOLLOWUP.error(outcome=outcome, reason="missing_completeness_evidence")
    complete = get(meta.complete_column)
    if (
        not pa.types.is_boolean(complete.type)
        or complete.null_count
        or not all(complete.to_pylist())
    ):
        reasons = (
            []
            if meta.reason_column is None
            else sorted({v for v in get(meta.reason_column).to_pylist() if isinstance(v, str)})
        )
        raise INCOMPLETE_FOLLOWUP.error(outcome=outcome, reasons=reasons)
    precision: dict[str, dict[str, Any]] = {}
    features: dict[str, np.ndarray] = {}
    kinds: dict[str, str] = {}
    needed = tuple(
        dict.fromkeys(
            (roles.treatment, outcome, *roles.features, *((roles.weight,) if roles.weight else ()))
        )
    )
    converted: dict[str, np.ndarray] = {}
    for name in needed:
        column = get(name)
        metadata = frame.columns.get(name)
        certificate: dict[str, Any] = {}
        if metadata is not None:
            certificate["compiler_policy"] = metadata.numeric_policy
            if metadata.numeric_policy == "checked_float64_v1":
                bound_name = metadata.absolute_error_column
                if bound_name is None or bound_name not in table.column_names:
                    raise INPUT_PRECISION.error(column=name, reason="missing_compiler_error_bound")
                bounds = get(bound_name).to_pylist()
                nulls = column.is_null().to_numpy(zero_copy_only=False)
                reasons = (
                    get(metadata.reason_column).to_pylist()
                    if metadata.reason_column
                    else [None] * len(bounds)
                )
                for is_null, bound, reason in zip(nulls, bounds, reasons, strict=True):
                    if is_null:
                        valid = bound is None and isinstance(reason, str) and bool(reason)
                    else:
                        valid = (
                            isinstance(bound, (int, float, Decimal))
                            and not isinstance(bound, bool)
                            and math.isfinite(float(bound))
                            and bound >= 0
                        )
                    if not valid:
                        raise INPUT_PRECISION.error(
                            column=name, reason="invalid_compiler_error_bound"
                        )
                certificate["compiler_max_absolute_error"] = max(
                    (float(b) for b in bounds if b is not None), default=0.0
                )
        if column.null_count:
            raise MISSING_DATA.error(column=name)
        dtype = column.type
        categorical = name in roles.features and (
            pa.types.is_boolean(dtype)
            or pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
            or pa.types.is_dictionary(dtype)
        )
        if categorical:
            if pa.types.is_dictionary(dtype):
                dtype = dtype.value_type
                if not (
                    pa.types.is_boolean(dtype)
                    or pa.types.is_string(dtype)
                    or pa.types.is_large_string(dtype)
                    or pa.types.is_integer(dtype)
                    or pa.types.is_floating(dtype)
                    or pa.types.is_decimal(dtype)
                ):
                    raise UNSUPPORTED_FEATURE_DTYPE.error(column=name, dtype=str(column.type))
            labels = column.to_pylist()
            if any(value is None for value in labels):
                raise MISSING_DATA.error(column=name)
            if pa.types.is_floating(dtype) or pa.types.is_decimal(dtype):
                if any(not math.isfinite(value) for value in labels):
                    raise NONFINITE_DATA.error(column=name)
            converted[name] = np.asarray(labels, dtype=object)
            kinds[name] = "categorical"
            precision[name] = certificate
        else:
            converted[name], info = _numeric(column, name, weight=name == roles.weight)
            precision[name] = info | certificate
            if name in roles.features:
                kinds[name] = "numeric"
        if name in roles.features:
            features[name] = converted[name]
    a = converted[roles.treatment]
    if not np.all((a == 0) | (a == 1)):
        raise UNSUPPORTED_DESIGN.error(reason="binary_treatment_required")
    if len(np.unique(a)) != 2:
        raise ONE_ARM.error(fold=None, reason="both_treatment_arms_required")
    q = (
        np.ones(table.num_rows)
        if roles.weight is None
        else normalize_weights(converted[roles.weight], roles.weight)
    )
    return PreparedData(units, a, converted[outcome], q, features, kinds, outcome, precision)
