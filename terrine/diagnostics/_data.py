"""Small descriptive Arrow reader, independent of causal estimation."""

from __future__ import annotations

import math
from decimal import Decimal

import numpy as np
import pyarrow as pa

from terrine.data import MaterializedFrame
from terrine.results import Diagnostic


class InvalidDiagnosticData(ValueError):
    """A descriptive data defect with a row-free reason code."""


def _float_values(column: pa.ChunkedArray) -> np.ndarray:
    values = column.to_numpy(zero_copy_only=False)
    if pa.types.is_integer(column.type):
        outside = values > 2**53
        if pa.types.is_signed_integer(column.type):
            outside |= values < -(2**53)
        if np.any(outside):
            raise InvalidDiagnosticData("integer_outside_exact_float64_domain")
    return values.astype(np.float64, copy=False)


def arrays(
    frame: MaterializedFrame, *, include_features: bool = True
) -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray], dict[str, str]]:
    if not isinstance(frame, MaterializedFrame) or frame.roles.treatment is None:
        raise InvalidDiagnosticData("baseline_treatment_required")
    cache = {}

    def get(name):
        if name not in cache:
            if name not in frame.table.column_names or frame.table[name].null_count:
                raise InvalidDiagnosticData("missing_diagnostic_data")
            cache[name] = frame.table[name]
        return cache[name]

    a = _float_values(get(frame.roles.treatment))
    if not np.all((a == 0) | (a == 1)) or len(np.unique(a)) != 2:
        raise InvalidDiagnosticData("both_binary_treatment_arms_required")
    if frame.roles.weight is None:
        q = np.ones(len(a))
    else:
        q = _float_values(get(frame.roles.weight))
        if not np.all(np.isfinite(q)) or np.any(q <= 0):
            raise InvalidDiagnosticData("invalid_target_weights")
        q = q / np.max(q)
        if np.any(q == 0):
            raise InvalidDiagnosticData("target_weight_dynamic_range")
        q = q * (len(q) / math.fsum(q))
    features = {}
    kinds = {}
    for name in frame.roles.features if include_features else ():
        column = get(name)
        dtype = column.type.value_type if pa.types.is_dictionary(column.type) else column.type
        if (
            pa.types.is_dictionary(column.type)
            or pa.types.is_boolean(dtype)
            or pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
        ):
            if not (
                pa.types.is_boolean(dtype)
                or pa.types.is_string(dtype)
                or pa.types.is_large_string(dtype)
                or pa.types.is_integer(dtype)
                or pa.types.is_floating(dtype)
                or pa.types.is_decimal(dtype)
            ):
                raise InvalidDiagnosticData("unsupported_feature_dtype")
            kinds[name] = "categorical"
            labels = column.to_pylist()
            if any(value is None for value in labels):
                raise InvalidDiagnosticData("missing_diagnostic_data")
            if pa.types.is_floating(dtype) or pa.types.is_decimal(dtype):
                if any(not math.isfinite(value) for value in labels):
                    raise InvalidDiagnosticData("nonfinite_features")
            features[name] = np.asarray(labels, dtype=object)
        elif (
            pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_decimal(dtype)
        ):
            kinds[name] = "numeric"
            features[name] = _float_values(column)
            if not np.all(np.isfinite(features[name])):
                raise InvalidDiagnosticData("nonfinite_features")
        else:
            raise InvalidDiagnosticData("unsupported_feature_dtype")
    return a, q, features, kinds


def unavailable(name: str, reason: str) -> Diagnostic:
    return Diagnostic(name, "unavailable", {}, {}, {"reason": reason})


def category_key(value: object) -> tuple:
    """Order Boolean, integer, floating, decimal and exact UTF-8 category labels."""
    if isinstance(value, (bool, np.bool_)):
        return 0, int(value)
    if isinstance(value, (int, np.integer)):
        return 1, int(value)
    if isinstance(value, (float, np.floating)):
        return 2, float(value)
    if isinstance(value, Decimal):
        return 3, value
    return 4, str(value).encode("utf-8")
