"""Sparse exact coefficient prefixes over grouped sufficient state."""

from __future__ import annotations

from typing import Literal

import ibis
import ibis.expr.datatypes as dt
from ibis.common.exceptions import IbisError

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import exact_literal, exact_subtract, exact_sum
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure

__all__ = ["sparse_prefix"]

FLOAT64_PRECISION = RefusalSpec(
    "terrine.temporal.float64_precision",
    "Float64 result unavailable ({failure}). Use finite inputs whose result fits "
    "binary64, or correctly declared integer/decimal facts.",
)
_EXACT_UNSUPPORTED = RefusalSpec(
    "terrine.temporal.exact_unsupported",
    "Backend {backend!r} lacks exact sparse BIGNUM state. Use the supported DuckDB backend.",
)
# A non-finite input at or before a non-empty window's end, or a result beyond
# binary64's range.
type Float64Failure = Literal["nonfinite_input", "bound_overflow"]


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


_COUNT_FIELDS = (
    "event_count",
    "nonnull_count",
    "unknown_filter_count",
    "unknown_filter_null_count",
    "unknown_filter_missing_count",
    "unknown_filter_literal_count",
    "unknown_filter_nonfinite_count",
)
# Inputs exact float state cannot hold: non-finite, or beyond the exact type.
_EXACT_FLOAT_COUNTS = ("float64_nonfinite_count", "float64_unrepresentable_count")


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
    # A float fact's exact state carries its unit's scale and the counts of
    # inputs it could not hold.
    exact_floats = (
        measure.value_type is not None
        and dt.dtype(measure.value_type).is_floating()
        and measure.aggregation != "count"
    )
    if exact_floats:
        required += ("exact_scale", *_EXACT_FLOAT_COUNTS)
    # A fixed-precision policy's state carries the group's accumulated
    # magnitude bound and its count of withheld rows.
    bounded = "sum_withheld" in state.columns
    if bounded:
        required += ("sum_bound",)
    for column in required:
        if column not in state.columns:
            raise _SCHEMA.error(field="sufficient state", column=column)
    _require_backend(state)
    window = ibis.window(group_by=state._unit, order_by=state._timestamp, rows=(None, 0))
    fields = {
        name: exact_sum(state[name]).over(window)
        if name in {"event_count", "nonnull_count"} or measure.filters
        else exact_literal(0)
        for name in _COUNT_FIELDS
    }
    state_zero = exact_literal(0, state.sum_value)
    if bounded:
        fields["sum_bound"] = state.sum_bound.sum().over(window).fill_null(ibis.literal(0.0))
    else:
        fields["sum_value"] = exact_sum(state.sum_value).over(window).fill_null(state_zero)
    if exact_floats:
        for name in _EXACT_FLOAT_COUNTS:
            fields[name] = exact_sum(state[name]).over(window).fill_null(exact_literal(0))
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
        admitted = policy.admits_accumulation(
            cumulative.sum_bound, cumulative.nonnull_count, state_type
        )
        withheld = ~admitted | (cumulative.sum_withheld > exact_literal(0))
        held = withheld.ifelse(ibis.null().cast(state_type), cumulative.sum_value)
        running = ibis.window(
            group_by=cumulative._unit, order_by=cumulative._timestamp, rows=(None, 0)
        )
        cumulative = cumulative.mutate(
            sum_value=exact_sum(held, admitted=True).over(running).fill_null(state_zero),
            sum_withheld=exact_sum(withheld.ifelse(exact_literal(1), exact_literal(0)))
            .over(running)
            .fill_null(exact_literal(0)),
        ).drop("sum_bound")
    return cumulative.mutate(
        observed_null_count=exact_subtract(cumulative.event_count, cumulative.nonnull_count)
    )
