"""Fit exact training-only limits and apply frozen post-aggregation transforms."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc

from terrine.data import MaterializedFrame
from terrine.errors import RefusalSpec
from terrine.export.preflight import _complete_outcomes
from terrine.spec.transforms import Winsorization, WinsorizationFit, decimal_value
from terrine.temporal.nonadditive import exact_quantile

_TRAINING_REQUIRED = RefusalSpec(
    "terrine.transform.training_split_required",
    "Fitting winsorization requires a declared training split. {route}",
)
_UNSUPPORTED_TYPE = RefusalSpec(
    "terrine.transform.winsorization_type_unsupported",
    "Exact winsorization does not support {value_type!r} for {column!r}. {route}",
)
_PRECISION = RefusalSpec(
    "terrine.transform.winsorization_precision_unavailable",
    "Winsorization output for {column!r} needs unavailable exact Decimal precision. {route}",
)
_IDENTITY = RefusalSpec(
    "terrine.transform.winsorization_identity_mismatch",
    "Frozen winsorization for {column!r} does not match this view. {route}",
)
_EMPTY = RefusalSpec(
    "terrine.transform.empty_training_population",
    "Exact winsorization for {column!r} requires non-null training values. {route}",
)


def require_training_split() -> None:
    raise _TRAINING_REQUIRED.error(
        route="Use training_set(..., split=TimeSplit(...) or GroupSplit(...)) with training rows."
    )


def identity_error(column: str):
    return _IDENTITY.error(
        column=column, route="Use the original training view or its compatible scoring recipe."
    )


def require_numeric_type(dtype: pa.DataType, column: str) -> None:
    if not (pa.types.is_integer(dtype) or pa.types.is_decimal128(dtype)):
        raise _UNSUPPORTED_TYPE.error(
            column=column,
            value_type=str(dtype),
            route="Declare an exactly representable integer or Decimal128 feature before fitting.",
        )


def _decimal_coefficient(coefficient: int, scale: int) -> Decimal:
    return Decimal((int(coefficient < 0), tuple(int(c) for c in str(abs(coefficient))), -scale))


def _clamp(value, lower: Decimal, upper: Decimal):
    return lower if value < lower else upper if value > upper else value


def bind_winsorization(transform: Winsorization, dtype: pa.DataType) -> Winsorization:
    require_numeric_type(dtype, transform.column)
    if transform.source_type is not None and transform.source_type != str(dtype):
        raise identity_error(transform.column)
    lower, upper = decimal_value(transform.lower), decimal_value(transform.upper)
    if pa.types.is_integer(dtype):
        signed = pa.types.is_signed_integer(dtype)
        minimum = -(1 << (dtype.bit_width - 1)) if signed else 0
        maximum = (1 << (dtype.bit_width - int(signed))) - 1
        source_scale = 0
    else:
        coefficient = 10**dtype.precision - 1
        source_scale = dtype.scale
        minimum = _decimal_coefficient(-coefficient, source_scale)
        maximum = _decimal_coefficient(coefficient, source_scale)
    image = (_clamp(minimum, lower, upper), _clamp(maximum, lower, upper))
    integer_digits = max(0 if -1 < value < 1 else Decimal(value).adjusted() + 1 for value in image)
    scale = max(0, source_scale, *(-int(value.as_tuple().exponent) for value in (lower, upper)))
    precision = max(1, integer_digits + scale)
    if precision > 38:
        raise _PRECISION.error(
            column=transform.column,
            route="Use exactly representable limits and source scale within Decimal128 precision.",
        )
    if transform.output_precision is not None and (
        transform.output_precision != precision or transform.output_scale != scale
    ):
        raise identity_error(transform.column)
    return replace(
        transform, source_type=str(dtype), output_precision=precision, output_scale=scale
    )


def apply_winsorization[Arrow: (pa.Table, pa.RecordBatch)](
    table: Arrow, transforms: tuple[Winsorization, ...]
) -> Arrow:
    for transform in transforms:
        index = table.schema.get_field_index(transform.column)
        if index < 0 or transform.source_type is None:
            raise identity_error(transform.column)
        bound = bind_winsorization(transform, table.schema.field(index).type)
        lower, upper = decimal_value(bound.lower), decimal_value(bound.upper)
        values = table.column(index).to_pylist()
        result = pa.array(
            [None if value is None else _clamp(value, lower, upper) for value in values],
            type=pa.decimal128(bound.output_precision, bound.output_scale),
        )
        table = table.set_column(index, transform.column, result)
    return table


def fit_winsorization(
    data: MaterializedFrame,
    *,
    column: str,
    lower: Decimal | str | int,
    upper: Decimal | str | int,
    recipe_hash: str,
    feature_definition_hash: str,
) -> Winsorization:
    view = data.provenance.dataset_view
    if (
        view is None
        or view.kind != "training"
        or view.split_policy is None
        or "__terrine_split" not in data.table.column_names
    ):
        require_training_split()
    assert view is not None and view.split_policy is not None
    probabilities = (decimal_value(lower), decimal_value(upper))
    if not Decimal(0) <= probabilities[0] <= probabilities[1] <= Decimal(1):
        from terrine.spec.transforms import _invalid

        raise _invalid("probabilities must be ordered in [0, 1]")
    dtype = data.table.schema.field(column).type
    require_numeric_type(dtype, column)
    checked = _complete_outcomes(data)
    population = checked.table.filter(
        pc.call_function("equal", [checked.table["__terrine_split"], pa.scalar("train")])
    )
    values = population[column].to_pylist()
    non_null = sum(value is not None for value in values)
    if not non_null:
        raise _EMPTY.error(
            column=column,
            route="Provide non-null complete feature values in the declared training split.",
        )
    limits = tuple(exact_quantile(values, p) for p in probabilities)
    transform = bind_winsorization(Winsorization(column, lower=limits[0], upper=limits[1]), dtype)
    names = tuple(dict.fromkeys((data.roles.unit, data.roles.anchor, data.roles.t0, column)))
    selected = population.select(names)
    rows = sorted(
        json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        for row in selected.to_pylist()
    )
    payload = {"schema": [(field.name, str(field.type)) for field in selected.schema], "rows": rows}
    population_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    fit = WinsorizationFit(
        recipe_hash=recipe_hash,
        view_hash=view.view_hash,
        design_hash=data.provenance.design_hash,
        model_hash=data.provenance.model_hash,
        feature_definition_hash=feature_definition_hash,
        population_hash=population_hash,
        snapshot=data.to_metadata()["provenance"]["execution_snapshot"],
        split_policy=view.split_policy,
        probabilities=probabilities,
        row_count=population.num_rows,
        non_null_count=non_null,
    )
    return replace(transform, fit=fit)
