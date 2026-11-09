"""Sparse prefixes over grouped sufficient state: exact coefficients or native binary64 sums."""

from __future__ import annotations

from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.expr.builders import WindowBuilder

from terrine.backends.dialect import current_dialect
from terrine.backends.numeric import count_literal, exact_literal, exact_subtract, exact_sum
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure
from terrine.temporal.daily import (
    _EXACT_FLOAT_COUNTS,
    _NATIVE_FLOAT_COUNTS,
    _UNKNOWN_FIELDS,
    _require_backend,
    native_arithmetic,
)

__all__ = ["sparse_prefix"]


_COUNT_FIELDS = ("event_count", "nonnull_count")


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
