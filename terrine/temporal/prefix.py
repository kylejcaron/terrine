"""Sparse prefixes over grouped sufficient state: exact coefficients or native binary64 sums."""

from __future__ import annotations

from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.common.exceptions import IbisError
from ibis.expr.builders import WindowBuilder

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import count_literal, exact_literal, exact_subtract, exact_sum
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure

__all__ = ["native_arithmetic", "sparse_prefix"]

FLOAT64_PRECISION = RefusalSpec(
    "terrine.temporal.float64_precision",
    "Float64 result unavailable ({reason}). Use finite inputs with magnitude below "
    "10**29 whose quantized result fits binary64, or declared integer/decimal values.",
)
_EXACT_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.exact_unsupported",
    "Backend {backend!r} lacks exact sparse BIGNUM state. Use the supported DuckDB backend.",
)
# Invalid float inputs or a published result beyond binary64's range.
type Float64Failure = Literal["nonfinite_input", "out_of_range", "bound_overflow"]


def _require_backend(table: ibis.Table) -> None:
    """Check static backend/build metadata; never run a capability query here.

    The policy in effect (the admitted scope, else the table's own backend)
    decides whether it can hold this backend's exact prefix state.
    """
    try:
        backend = table._find_backend(use_default=False).name
    except IbisError:
        backend = "unbound"
    policy = current_dialect(*(table[name] for name in table.columns))
    if not policy.supports_exact_prefix(backend):
        raise _EXACT_UNSUPPORTED.error(backend=backend)


_COUNT_FIELDS = ("event_count", "nonnull_count")
# Provenance of events whose filter predicate is unknown; only a filtered
# measure's state holds them, since an unfiltered predicate is the literal
# TRUE and every such count would be zero.
_UNKNOWN_FIELDS = (
    "unknown_filter_count",
    "unknown_filter_null_count",
    "unknown_filter_missing_count",
    "unknown_filter_literal_count",
    "unknown_filter_nonfinite_count",
)
# Invalid inputs refused before quantized decimal accumulation.
_EXACT_FLOAT_COUNTS = ("float64_nonfinite_count", "float64_unrepresentable_count")
# Non-finite inputs a native binary64 sum counts instead of adding.
_NATIVE_FLOAT_COUNTS = ("float64_nonfinite_count",)


def native_arithmetic(measure: BoundMeasure) -> bool:
    """Whether a resolved measure accumulates its float values in native binary64.

    Only a float sum or average declared ``exact=False`` does; counts and
    integer or decimal values keep exact state.
    """
    return (
        not measure.exact
        and measure.aggregation != "count"
        and measure.value_type is not None
        and dt.dtype(measure.value_type).is_floating()
    )


def _running_sum(value: ir.Value, window: WindowBuilder, *, admitted: bool = False) -> ir.Value:
    """The exact cumulative sum over ``window``, zero where no state has contributed.

    The fill takes the sum's own type, which is wider than a stored count's.
    """
    total = exact_sum(value, admitted=admitted).over(window)
    return total.fill_null(exact_literal(0, total))


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
    A native measure's prefix is the binary64 running sum of its group sums and
    needs no exact backend state; where the policy bounds float arithmetic the
    running sum is withheld, like a fixed-precision coefficient prefix, once
    the magnitudes accumulated through a state may leave half the range.
    """
    if resolution not in {"day", "event"}:
        raise ValueError("resolution must be day or event")
    counts = (*_COUNT_FIELDS, *(_UNKNOWN_FIELDS if measure.filters else ()))
    required = ("_unit", "_timestamp", *counts, "sum_value")
    if resolution == "day":
        required += ("local_day",)
    native = native_arithmetic(measure)
    # Float state carries counts of inputs outside the quantized domain.
    exact_floats = (
        not native
        and measure.value_type is not None
        and dt.dtype(measure.value_type).is_floating()
        and measure.aggregation != "count"
    )
    float_counts: tuple[str, ...] = (
        _EXACT_FLOAT_COUNTS if exact_floats else _NATIVE_FLOAT_COUNTS if native else ()
    )
    required += float_counts
    # A policy that bounds this state's accumulation carries the group's
    # accumulated magnitude bound and its count of withheld rows.
    bounded = "sum_withheld" in state.columns
    if bounded:
        required += ("sum_bound",)
    for column in required:
        if column not in state.columns:
            raise _SCHEMA.error(field="sufficient state", column=column)
    if native and not state.sum_value.type().is_floating():
        raise _SCHEMA.error(field="native float state", column="sum_value")
    if not native:
        _require_backend(state)
    window = ibis.window(group_by=state._unit, order_by=state._timestamp, rows=(None, 0))
    fields = {name: exact_sum(state[name]).over(window) for name in counts}
    if bounded:
        fields["sum_bound"] = state.sum_bound.sum().over(window).fill_null(ibis.literal(0.0))
    elif native:
        fields["sum_value"] = state.sum_value.sum().over(window).fill_null(ibis.literal(0.0))
    else:
        fields["sum_value"] = _running_sum(state.sum_value, window)
    for name in float_counts:
        fields[name] = _running_sum(state[name], window)
    cumulative = state.mutate(**fields)
    if bounded:
        # A state is withheld when the magnitudes accumulated through it may
        # not fit the domain, or when its own group withheld rows. The running
        # sum then adds only complete admitted states, so the backend never
        # evaluates a sum outside the domain, and the running count of withheld
        # states lets the window consumer refuse instead of reading a partial
        # prefix: the running magnitude bound alone is not monotone under the
        # backend's summation order.
        state_type = state.sum_value.type()
        policy = current_dialect(cumulative.sum_bound)
        limit = (
            policy.float_magnitude_limit()
            if native
            else policy.magnitude_limit(cast(dt.Decimal, state_type))
        )
        admitted = policy.admits_accumulation(cumulative.sum_bound, cumulative.nonnull_count, limit)
        withheld = ~admitted | (cumulative.sum_withheld > exact_literal(0, cumulative.sum_withheld))
        held = withheld.ifelse(ibis.null().cast(state_type), cumulative.sum_value)
        running = ibis.window(
            group_by=cumulative._unit, order_by=cumulative._timestamp, rows=(None, 0)
        )
        total = (
            cast(ir.FloatingColumn, held).sum().over(running).fill_null(ibis.literal(0.0))
            if native
            else _running_sum(held, running, admitted=True)
        )
        cumulative = cumulative.mutate(
            sum_value=total,
            sum_withheld=_running_sum(withheld.ifelse(count_literal(1), count_literal(0)), running),
        ).drop("sum_bound")
    return cumulative.mutate(
        observed_null_count=exact_subtract(cumulative.event_count, cumulative.nonnull_count)
    )
