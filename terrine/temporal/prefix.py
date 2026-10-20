"""Sparse exact coefficients and checked binary64 reduction certificates."""

from __future__ import annotations

from collections.abc import Callable
from importlib.metadata import version
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import sqlglot.expressions as sge
from ibis.common.exceptions import IbisError

from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure

__all__ = ["sparse_prefix"]

FLOAT64_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.float64_unsupported",
    "Backend {backend!r} lacks {capability}. Use a supported checked backend or "
    "correctly declared integer/decimal facts.",
)
FLOAT64_PRECISION = RefusalSpec(
    "terrine.temporal.float64_precision",
    "Float64 certification failed ({failure}). Use correctly declared exact facts "
    "or a smaller semantically sufficient history with explicit completeness.",
)
_EXACT_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.exact_unsupported",
    "Backend {backend!r} lacks exact sparse BIGNUM state. Use the supported DuckDB backend.",
)
type Float64Failure = Literal[
    "nonfinite_input",
    "nonfinite_state",
    "count_limit",
    "bound_overflow",
    "uncertifiable_cast",
    "denominator_interval_zero",
    "tolerance",
]

# These are native builtin bindings, not Python row functions. BIGNUM remains internal.
_EXACT = dt.Unknown(raw_type=sge.DataType.build("BIGNUM", dialect="duckdb"))


@ibis.udf.agg.builtin(name="sum", signature=((_EXACT,), _EXACT))
def exact_sum(value):
    """Native exact coefficient/count sum."""


@ibis.udf.scalar.builtin(name="subtract", signature=((_EXACT, _EXACT), _EXACT))
def exact_subtract(left, right):
    """Native exact coefficient/count subtraction."""


@ibis.udf.scalar.builtin(name="nextafter", signature=((dt.float64, dt.float64), dt.float64))
def _nextafter(value, _direction):
    """Native adjacent binary64 value."""


@ibis.udf.scalar.builtin(name="isfinite", signature=((dt.float64,), dt.boolean))
def _native_isfinite(value):
    """Native finite-state predicate."""


def _isfinite(value) -> ir.BooleanValue:
    return cast(ir.BooleanValue, _native_isfinite(value))


@ibis.udf.scalar.builtin(name="add", signature=((dt.float64, dt.float64), dt.float64))
def _add(left, right):
    """Native binary64 addition."""


@ibis.udf.scalar.builtin(name="subtract", signature=((dt.float64, dt.float64), dt.float64))
def _subtract(left, right):
    """Native binary64 subtraction."""


@ibis.udf.scalar.builtin(name="multiply", signature=((dt.float64, dt.float64), dt.float64))
def _multiply(left, right):
    """Native binary64 multiplication."""


@ibis.udf.scalar.builtin(name="divide", signature=((dt.float64, dt.float64), dt.float64))
def _divide(left, right):
    """Native binary64 division."""


def exact_literal(value: int):
    """Construct an exact native integer without an inferred fixed-width literal."""
    return ibis.literal(str(value)).cast(_EXACT)


def _require_backend(table: ibis.Table, *, floating: bool = False) -> None:
    """Check static backend/build metadata; never run a capability query here."""
    try:
        backend = table._find_backend(use_default=False).name
    except IbisError:
        backend = "unbound"
    supported = backend == "duckdb" and version("duckdb") == "1.5.6"
    if not supported:
        if floating:
            raise FLOAT64_UNSUPPORTED.error(
                backend=backend,
                capability="DuckDB 1.5.6 native checked SUM and outward primitives",
            )
        raise _EXACT_UNSUPPORTED.error(backend=backend)


def _outward(value, upper: bool):
    return _isfinite(value).ifelse(
        _nextafter(value, float("inf") if upper else -float("inf")), value
    )


def outward_add(left, right, *, upper: bool = True):
    """One addition rounded outward; inputs are binary64 endpoints."""
    return _outward(_add(left, right), upper)


def outward_subtract(left, right, *, upper: bool = True):
    """One subtraction rounded outward; inputs are binary64 endpoints."""
    return _outward(_subtract(left, right), upper)


def outward_multiply(left, right, *, upper: bool = True):
    """One endpoint product rounded outward (caller chooses signs/endpoints)."""
    return _outward(_multiply(left, right), upper)


def outward_divide(left, right, *, upper: bool = True):
    """One endpoint quotient rounded outward; caller proves denominator excludes zero."""
    return _outward(_divide(left, right), upper)


def float64_rounding_radius(value):
    """Upper bound (u*abs(value)+eta)/(1-u), rounding each operation outward."""
    numerator = outward_add(outward_multiply(2**-53, value.abs()), 2**-1074)
    denominator = outward_subtract(1.0, 2**-53, upper=False)
    return outward_divide(numerator, denominator)


def float64_tolerance(value):
    """Downward threshold for the fixed checked_float64_v1 final-result policy."""
    relative = ibis.greatest(outward_multiply(2**-30, value.abs(), upper=False), 0.0)
    return ibis.greatest(outward_add(2**-40, relative, upper=False), 0.0)


def _float64_conforms(value):
    """Execution-thread checks under the standard RN/gradual-underflow precondition.

    Depend on the leaf so the arithmetic is evaluated with the aggregate input,
    rather than solely constant-folded by the planner. Finite probes are not a
    proof against a changing floating-point environment.
    """
    one = (value.fill_null(0.0) >= 0.0).ifelse(1.0, -1.0)
    half_ulp = _multiply(one, 2**-53)
    neighbor = _nextafter(one, _multiply(one, 2.0))
    tiny = _nextafter(_multiply(one, 0.0), one)
    return (
        (_add(one, half_ulp) == one)
        & (_add(neighbor, half_ulp) == _nextafter(neighbor, _multiply(one, 2.0)))
        & (_add(tiny, tiny) == _multiply(one, 2**-1073))
        & (_divide(tiny, 2.0) == 0.0)
        & (tiny != 0.0)
    )


def float64_certificate(state: ibis.Table) -> ibis.Table:
    """Attach checked bounds to canonical sum/absolute/count state.

    For k=max(n-1,0), u=2^-53, eta=2^-1074, compute outward upper
    g>=ku/(1-ku), b>=k*eta/(1-ku), A>=(Ahat+b)/(1-g), E>=g*A+b.
    Project each intermediate once: nesting nonfinite-preserving primitives
    recursively would expand the compiled expression tree exponentially.
    The domain requires exact nonnegative n and k*u<1/4. Invalid states stay
    explicit flags; only a consumer's pinned executor may accept these states.
    """
    columns = state.columns
    count = state.nonnull_count
    zero, one = exact_literal(0), exact_literal(1)
    prior_bad = (
        state.float64_nonfinite_state
        if "float64_nonfinite_state" in columns
        else ibis.literal(False)
    )
    stage = state.mutate(
        _cert_k=(count > zero).ifelse(exact_subtract(count, one), zero),
        _cert_no_additions=count <= one,
        _cert_empty=count == zero,
        float64_nonfinite_state=prior_bad
        | (
            ~_isfinite(state.sum_value)
            | ~_isfinite(state.sum_abs_value)
            | state.sum_value.isnull()
            | state.sum_abs_value.isnull()
        ).fill_null(True),
    )
    stage = stage.mutate(
        _cert_k_float=stage._cert_k.cast("float64"),
        float64_count_limit=(
            count.isnull() | (count < zero) | (stage._cert_k >= exact_literal(2**51))
        ).fill_null(True),
    )
    stage = stage.mutate(
        _cert_ku=outward_multiply(stage._cert_k_float, 2**-53),
        _cert_tiny=outward_multiply(stage._cert_k_float, 2**-1074),
    )
    stage = stage.mutate(_cert_denominator=outward_subtract(1.0, stage._cert_ku, upper=False))
    stage = stage.mutate(
        _cert_g=stage._cert_no_additions.ifelse(
            0.0, outward_divide(stage._cert_ku, stage._cert_denominator)
        ),
        _cert_b=stage._cert_no_additions.ifelse(
            0.0, outward_divide(stage._cert_tiny, stage._cert_denominator)
        ),
    )
    stage = stage.mutate(
        _cert_absolute_denominator=outward_subtract(1.0, stage._cert_g, upper=False),
        _cert_absolute_numerator=outward_add(stage.sum_abs_value, stage._cert_b),
    )
    stage = stage.mutate(
        sum_abs_upper=stage._cert_empty.ifelse(
            0.0,
            stage._cert_no_additions.ifelse(
                stage.sum_abs_value,
                outward_divide(stage._cert_absolute_numerator, stage._cert_absolute_denominator),
            ),
        ),
    )
    stage = stage.mutate(_cert_product=outward_multiply(stage._cert_g, stage.sum_abs_upper))
    stage = stage.mutate(
        prefix_error=(stage._cert_empty | stage._cert_no_additions).ifelse(
            0.0, outward_add(stage._cert_product, stage._cert_b)
        )
    )
    stage = stage.mutate(
        float64_bound_overflow=(
            ~_isfinite(stage._cert_g)
            | ~_isfinite(stage._cert_b)
            | ~_isfinite(stage.sum_abs_upper)
            | ~_isfinite(stage.prefix_error)
            | (stage._cert_denominator <= 0.0)
            | (stage._cert_absolute_denominator <= 0.0)
            | (stage.sum_abs_value < 0.0)
            | (stage.prefix_error < 0.0)
        ).fill_null(True)
    )
    outputs = (
        "sum_abs_upper",
        "prefix_error",
        "float64_count_limit",
        "float64_bound_overflow",
        "float64_nonfinite_state",
    )
    return stage.select(*(name for name in columns if name not in outputs), *outputs)


_COUNT_FIELDS = (
    "event_count",
    "nonnull_count",
    "unknown_filter_count",
    "unknown_filter_null_count",
    "unknown_filter_missing_count",
    "unknown_filter_literal_count",
    "unknown_filter_nonfinite_count",
)


def _ever(value, window):
    return value.fill_null(True).cast("int8").max().over(window) > 0


def sparse_prefix(
    state: ibis.Table,
    measure: BoundMeasure,
    *,
    resolution: Literal["day", "event"],
) -> ibis.Table:
    """Accumulate sparse groups, retaining exact raw counts and invalid history.

    Day states are usable only at aligned local-midnight endpoints. Event states
    must first group equal timestamps with daily._sparse_group. A row represents
    state through its group; a strict lookup uses group timestamp < cutoff.
    """
    if resolution not in {"day", "event"}:
        raise ValueError("resolution must be day or event")
    required = ("_unit", "_timestamp", *_COUNT_FIELDS, "sum_value")
    if resolution == "day":
        required += ("local_day",)
    for column in required:
        if column not in state.columns:
            raise _SCHEMA.error(field="sufficient state", column=column)
    floating = state.sum_value.type().is_floating()
    _require_backend(state, floating=floating)
    window = ibis.window(group_by=state._unit, order_by=state._timestamp, rows=(None, 0))
    fields = {
        name: exact_sum(state[name]).over(window)
        if name in {"event_count", "nonnull_count"} or measure.filters
        else exact_literal(0)
        for name in _COUNT_FIELDS
    }
    if not floating:
        fields["sum_value"] = exact_sum(state.sum_value).over(window).fill_null(exact_literal(0))
        cumulative = state.mutate(**fields)
        return cumulative.mutate(
            observed_null_count=exact_subtract(cumulative.event_count, cumulative.nonnull_count)
        )
    fields.update(
        sum_value=state.sum_value.sum().over(window).fill_null(0.0),
        sum_abs_value=state.sum_abs_value.sum().over(window).fill_null(0.0),
        float64_nonfinite_input=_ever(state.float64_nonfinite_input, window),
        float64_conformance_failure=_ever(state.float64_conformance_failure, window),
        float64_nonfinite_state=_ever(state.float64_nonfinite_state, window),
    )
    cumulative = state.mutate(**fields)
    cumulative = cumulative.mutate(
        observed_null_count=exact_subtract(cumulative.event_count, cumulative.nonnull_count)
    )
    certified = float64_certificate(cumulative)
    history = ibis.window(group_by=certified._unit, order_by=certified._timestamp, rows=(None, 0))
    return certified.mutate(
        **{
            name: _ever(certified[name], history)
            for name in ("float64_nonfinite_state", "float64_count_limit", "float64_bound_overflow")
        }
    )


def validate_float64(
    state: ibis.Table,
    *,
    execute: Callable[[ibis.Table], Any],
    source: str,
    measure: str,
    output: str,
) -> None:
    """Refuse invalid required prefix states via the caller's snapshot executor.

    Select only needed endpoint states before calling. This checks constituent
    certificates, not the final subtraction/division/tolerance of a window.
    """
    if not state.sum_value.type().is_floating():
        return
    flags = {
        "conformance": state.float64_conformance_failure,
        "nonfinite_input": state.float64_nonfinite_input,
        "nonfinite_state": state.float64_nonfinite_state,
        "count_limit": state.float64_count_limit,
        "bound_overflow": state.float64_bound_overflow,
    }
    checks = state.aggregate(
        [
            cast(
                ir.Scalar,
                exact_sum(flag.fill_null(True).ifelse(exact_literal(1), exact_literal(0)))
                .fill_null(exact_literal(0))
                .cast("string")
                .name(name),
            )
            for name, flag in flags.items()
        ]
    )
    result = execute(checks)
    if not hasattr(result, "to_pylist"):
        raise TypeError("The supplied numerical executor must return a PyArrow table.")
    counts = result.to_pylist()[0]
    if int(counts["conformance"]):
        raise FLOAT64_UNSUPPORTED.error(
            backend="duckdb",
            capability="execution-thread RN binary64 and gradual underflow",
            failed_count=int(counts["conformance"]),
        )
    for failure in ("nonfinite_input", "nonfinite_state", "count_limit", "bound_overflow"):
        if int(counts[failure]):
            raise FLOAT64_PRECISION.error(
                failure=failure,
                failed_count=int(counts[failure]),
                source=source,
                measure=measure,
                output=output,
            )
