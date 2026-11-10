"""Strict half-open windows with deferred observation and arithmetic checks."""

from __future__ import annotations

import itertools
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
import pyarrow as pa

import terrine.reasons as reason_codes
from terrine.backends.dialect import current_dialect
from terrine.backends.extrema import greatest, least
from terrine.backends.numeric import (
    EXACT_OVERFLOW as _EXACT_OVERFLOW,
)
from terrine.backends.numeric import (
    FLOAT64_PRECISION,
    Float64Failure,
    exact_literal,
    exact_to_float,
    float_is_finite,
    float_ratio,
)
from terrine.backends.timestamps import (
    _compare_timestamp_keys,
    _compare_timestamp_parts,
    _timestamp_comparison_key,
    _TimestampComparisonKey,
    compare_timestamps,
    day_start,
    local_day,
    timestamp_parts,
    utc_wall_clock,
)
from terrine.data import ColumnMeta, RowCheck
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure, BoundMetric, BoundSource
from terrine.semantic.rules import (
    UNALIGNED_CALENDAR_AVERAGE,
    UNKNOWN_FILTER,
    UNSUPPORTED_TEMPORAL_KIND,
    UNSUPPORTED_TRANSFORM,
    calendar_average_alignment,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.daily import (
    _batch_projections,
    _require_backend,
    contributions,
    native_arithmetic,
    reduce_payload,
    resolve_measure,
)
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

__all__ = ["AggregateRequest", "TemporalResult", "aggregate_windows"]

_DAY_ALIGNMENT = RefusalSpec(
    "terrine.temporal.unaligned_day_resolution",
    "Per-day windows need both time bounds at local midnight. "
    "Choose matching t0 and window bounds, or use event-time windows for other times.",
)


def _counts_row(result: Any, expected: list[str]) -> pa.Table:
    """One executed row holding exactly the named failed counts, fully validated."""
    if not isinstance(result, pa.Table):
        raise TypeError("The supplied temporal executor must return a PyArrow table.")
    if result.num_rows != 1 or result.column_names != expected:
        raise ValueError("Temporal validation requires one row of named failed counts.")
    result.validate(full=True)
    return result


@dataclass(frozen=True, slots=True)
class _Check:
    table: ibis.Table
    failures: tuple[RowCheck, ...]

    def reads(self) -> tuple[ibis.Table, ...]:
        return (self.table,)

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        expected = [failure.column for failure in self.failures]
        result = _counts_row(execute(self.table), expected)
        counts = []
        for name in expected:
            column = result[name]
            if not pa.types.is_string(column.type) and not pa.types.is_large_string(column.type):
                raise TypeError("Temporal validation counts must be exact integer strings.")
            value = column[0].as_py()
            if not isinstance(value, str) or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
                raise ValueError("Temporal validation received a malformed failed count.")
            counts.append(int(value))
        for failure, count in zip(self.failures, counts, strict=True):
            if count:
                raise failure.refusal.error(**failure.context, failed_count=count)


@dataclass(frozen=True, slots=True)
class TemporalResult:
    """A lazy result whose checks must precede export under the caller's snapshot.

    ``row_checks`` name boolean columns of ``table``: a TRUE row fails that
    check, a NULL row passes. ``validate`` counts them all in one aggregate.
    """

    table: ibis.Table
    columns: Mapping[str, ColumnMeta]
    _checks: tuple[_Check, ...] = field(default=(), repr=False)
    row_checks: tuple[RowCheck, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", MappingProxyType(dict(self.columns)))
        object.__setattr__(self, "_checks", tuple(self._checks))
        object.__setattr__(self, "row_checks", tuple(self.row_checks))
        for check in self.row_checks:
            if check.column not in self.table.columns:
                raise ValueError(f"row check column {check.column!r} is not in its relation")

    def _row_check_counts(self) -> ibis.Table:
        """The failed-row count of every row check, as one row of ``int64`` sums."""
        return self.table.aggregate(
            **{
                f"failed_{index}": self.table[check.column]
                .ifelse(1, 0)
                .sum()
                .fill_null(0)
                .cast("int64")
                for index, check in enumerate(self.row_checks)
            }
        )

    def reads(self) -> tuple[ibis.Table, ...]:
        relations = [check.table for check in self._checks]
        if self.row_checks:
            relations.append(self._row_check_counts())
        return tuple(relations)

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Run bounded scalar checks using the same pinned executor as final export."""
        for check in self._checks:
            check.validate(execute)
        if not self.row_checks:
            return
        expected = [f"failed_{index}" for index in range(len(self.row_checks))]
        result = _counts_row(execute(self._row_check_counts()), expected)
        counts = []
        for name in expected:
            column = result[name]
            if not pa.types.is_integer(column.type):
                raise TypeError("Temporal validation counts must be integers.")
            value = column[0].as_py()
            if not isinstance(value, int) or value < 0:
                raise ValueError("Temporal validation received a malformed failed count.")
            counts.append(value)
        for check, count in zip(self.row_checks, counts, strict=True):
            if count:
                raise check.refusal.error(**check.context, failed_count=count)


class _Builder:
    """Project shared arithmetic intermediates before reusing them.

    Intermediates are staged in one pending projection and projected only when a
    later expression reads one of them, so independent intermediates share a
    layer instead of each nesting another projection. Fewer nested projections
    keep SQL compilation time from growing superlinearly with intermediate count.
    """

    def __init__(self, table: ibis.Table, output: str) -> None:
        self._table = table
        self._staged: dict[str, ir.Value] = {}
        self.used = {name.casefold() for name in (*table.columns, output)}
        self.counter = itertools.count()
        self.numeric: list[tuple[str, RefusalSpec, Mapping[str, Any]]] = []

    @property
    def table(self) -> ibis.Table:
        """The relation holding every intermediate, projecting staged ones first."""
        if self._staged:
            self._table = self._table.mutate(**self._staged)
            self._staged = {}
        return self._table

    @table.setter
    def table(self, table: ibis.Table) -> None:
        if self._staged:
            raise ValueError("staged intermediates must be projected before replacing the table")
        self._table = table

    def fork(self, table: ibis.Table) -> _Builder:
        """Build an independent projection branch with shared fresh names."""
        branch = _Builder(table, "")
        branch.used = self.used
        branch.counter = self.counter
        return branch

    def name(self, label: str) -> str:
        # Intermediates share the relation with declared outputs, so they stay in
        # the reserved namespace; the label's initials keep the name short, since
        # it is listed at every level of the statement.
        initials = "".join(word[0] for word in label.split("_") if word)
        while True:
            name = f"__terrine_w{next(self.counter)}_{initials}"
            if name.casefold() not in self.used:
                self.used.add(name.casefold())
                return name

    def put(self, label: str, expression) -> str:
        name = self.name(label)
        self._staged[name] = expression
        return name

    def put_many(self, *expressions: tuple[str, ir.Value]) -> tuple[str, ...]:
        columns = {self.name(label): expression for label, expression in expressions}
        self._staged.update(columns)
        return tuple(columns)

    def col(self, name: str):
        return (self.table if name in self._staged else self._table)[name]

    def flag(self, expression, failure: Float64Failure, context: Mapping[str, Any]) -> None:
        name = self.put(failure, expression)
        self.numeric.append((name, FLOAT64_PRECISION, {**context, "reason": failure}))

    def check(self, flags, *, active=None) -> tuple[RowCheck, ...]:
        """Stage one boolean column per flag: TRUE where the row fails its refusal.

        A NULL flag fails closed; ``active`` restricts the check to rows it
        holds for. Nothing is aggregated here: the result counts every check
        in one pass over its relation.
        """
        checks = []
        for name, refusal, context in flags:
            predicate = self.col(name).fill_null(True)
            if active is not None:
                predicate = active & predicate
            checks.append(RowCheck(self.put("check", predicate), refusal, context))
        return tuple(checks)


@dataclass(frozen=True, slots=True)
class _Quantity:
    value: str
    # A reason that makes the value unavailable (NULL), or NULL.
    reason: str
    known_zero: str
    scale: int = 0
    count: bool = False
    # Published as binary64, either a scaled exact coefficient or a quotient.
    binary64: bool = False
    # A native binary64 sum, already checked finite, publishes unconverted.
    native: bool = False
    # A reason published beside an available value, describing what it omits
    # (``below_resolution``), or NULL; absent when the arithmetic cannot omit.
    note: str | None = None


def narrow_exact(coefficient, scale: int, target: str | dt.DataType):
    """Convert an exact coefficient to ``target``; NULL when it does not fit.

    PostgreSQL and Snowflake lower TRY_CAST to an ordinary CAST that fails on
    overflow, so the range is checked on the exact coefficient before casting.
    """
    return current_dialect(coefficient).narrow_exact(coefficient, scale, target)


def _aligned(timestamp, minutes: int):
    """Whether an instant is the start of its fixed-offset local day."""
    return timestamp == day_start(timestamp, minutes)


def _merged_note(builder: _Builder, *quantities: _Quantity) -> str | None:
    """One note column for a quantity derived from ``quantities``: the first
    operand's note, else the next's. ``None`` when no operand can carry one."""
    notes = [builder.col(q.note) for q in quantities if q.note is not None]
    if not notes:
        return None
    return builder.put("note", ibis.coalesce(*notes) if len(notes) > 1 else notes[0])


def _quotient(
    builder: _Builder, numerator: _Quantity, denominator: _Quantity, reason: str, context
) -> _Quantity:
    """Convert each exact operand at its own scale, then divide once in float64.

    A native binary64 operand enters the division as it is. An operand's note
    describes the quotient too.
    """
    known_zero = builder.put(
        "quotient_zero",
        builder.col(numerator.known_zero) & ~builder.col(denominator.known_zero),
    )
    n, d = builder.col(numerator.value), builder.col(denominator.value)
    if numerator.native or denominator.native:
        n_float = exact_to_float(n, numerator.scale)[0]
        d_float = exact_to_float(d, denominator.scale)[0]
        # Withheld exact state may be physically zero without being a known zero.
        divisor = d_float.nullif(ibis.literal(0.0))
        quotient = n_float / divisor
    else:
        quotient = float_ratio(n, numerator.scale, d, denominator.scale)
    value = builder.put("quotient", quotient)
    # Only a zero denominator or missing operand may yield NULL.
    builder.flag(
        builder.col(value).isnull()
        & n.notnull()
        & d.notnull()
        & ~builder.col(denominator.known_zero),
        "bound_overflow",
        context,
    )
    builder.flag(
        ~float_is_finite(builder.col(value)).fill_null(ibis.literal(True)),
        "bound_overflow",
        context,
    )
    note = _merged_note(builder, numerator, denominator)
    return _Quantity(
        value,
        reason,
        known_zero,
        binary64=True,
        note=note,
        native=numerator.native or denominator.native,
    )


def _finalize_window(
    builder: _Builder,
    anchors: ibis.Table,
    metric: BoundMetric,
    window: Window,
    output: str,
    role: str,
    quantities: Sequence[_Quantity],
    available: str,
    unavailable: str,
    observed_from: str,
    observed_until: str,
    row_checks: list[RowCheck],
    unknown_flags,
) -> TemporalResult:
    if metric.kind == "ratio":
        numerator, denominator = quantities
        reason = builder.put(
            "ratio_reason",
            builder.col(numerator.reason)
            .notnull()
            .ifelse(
                builder.col(numerator.reason),
                builder.col(denominator.reason)
                .notnull()
                .ifelse(
                    builder.col(denominator.reason),
                    builder.col(denominator.known_zero).ifelse(
                        reason_codes.ZERO_DENOMINATOR, ibis.null().cast("string")
                    ),
                ),
            ),
        )
        quantity = _quotient(
            builder,
            numerator,
            denominator,
            reason,
            {
                "source": ",".join(m.source_name for m in metric.measures),
                "measure": ",".join(m.event for m in metric.measures),
                "output": output,
            },
        )
    else:
        quantity = quantities[0]
    # The value is published only where nothing makes it unavailable; a note
    # then fills the reason without withdrawing the value.
    blocking = builder.col(available).ifelse(builder.col(quantity.reason), builder.col(unavailable))
    if quantity.note is None:
        final_reason = builder.put("reason", blocking)
        evaluated = builder.put("evaluated", builder.col(final_reason).isnull())
    else:
        blocking_name = builder.put("blocking_reason", blocking)
        evaluated = builder.put("evaluated", builder.col(blocking_name).isnull())
        final_reason = builder.put(
            "reason", builder.col(blocking_name).fill_null(builder.col(quantity.note))
        )
    if unknown_flags:
        row_checks.extend(builder.check(unknown_flags, active=builder.col(available)))
    numeric_context = {
        "source": ",".join(m.source_name for m in metric.measures),
        "measure": ",".join(m.event for m in metric.measures),
        "output": output,
    }
    exact_output = metric.kind != "conversion" and not quantity.binary64
    if exact_output:
        assert isinstance(quantity.scale, int)
        target = "int64" if quantity.count else dt.Decimal(38, quantity.scale)
        value = builder.put(
            "exact_output",
            narrow_exact(builder.col(quantity.value), quantity.scale, target),
        )
        exact_value = builder.col(quantity.value)
        overflow = builder.put(
            "exact_overflow",
            builder.col(value).isnull()
            | (
                (exact_value < exact_literal(0, exact_value))
                if quantity.count
                else ibis.literal(False)
            ),
        )
        row_checks.extend(
            builder.check(
                [
                    (
                        overflow,
                        _EXACT_OVERFLOW,
                        {
                            "source": metric.measures[0].source_name,
                            "measure": metric.measures[0].event,
                            "output": output,
                        },
                    )
                ],
                active=builder.col(evaluated),
            )
        )
    elif quantity.binary64 and not quantity.native:
        rounded, nonfinite, out_of_range = exact_to_float(
            builder.col(quantity.value), quantity.scale
        )
        value = builder.put("rounded", rounded)
        builder.flag(nonfinite, "nonfinite_input", numeric_context)
        builder.flag(out_of_range, "bound_overflow", numeric_context)
    else:
        value = quantity.value
    if builder.numeric:
        row_checks.extend(builder.check(builder.numeric, active=builder.col(evaluated)))
    public_value = builder.col(evaluated).ifelse(
        builder.col(value), ibis.null().cast(builder.col(value).type())
    )
    result = builder.table.mutate(**{output: public_value}).select(
        *dict.fromkeys(
            [
                *anchors.columns,
                output,
                observed_from,
                observed_until,
                available,
                final_reason,
                *(check.column for check in row_checks),
            ]
        )
    )
    meta = ColumnMeta(
        window=window,
        role=role,
        observed_from_column=observed_from,
        observed_until_column=observed_until,
        complete_column=available,
        reason_column=final_reason,
        numeric_policy=None if metric.kind == "conversion" or quantity.binary64 else "exact",
        arithmetic="native" if any(native_arithmetic(m) for m in metric.measures) else "exact",
    )
    return TemporalResult(result, {output: meta}, row_checks=tuple(row_checks))


@dataclass(frozen=True, slots=True)
class AggregateRequest:
    metric: BoundMetric
    window: Window
    output: str
    role: str


@dataclass(frozen=True, slots=True)
class _WindowContextKey:
    start_seconds: int
    end_seconds: int
    source_start: datetime
    source_end: datetime
    boundary: str
    resolution: Literal["event", "day"]
    calendar_alignment: bool


@dataclass(frozen=True, slots=True)
class _WindowContext:
    start: str
    end: str
    available: str
    unavailable: str
    observed_from: str
    observed_until: str
    unaligned: str | None


def _window_context(
    builder: _Builder,
    metric: BoundMetric,
    window: Window,
    readiness: Readiness,
    boundary: str,
    output: str,
    resolution: Literal["event", "day"] = "event",
    *,
    shared: dict[_WindowContextKey, _WindowContext],
    shared_support: dict[tuple[Any, ...], tuple[str, str]] | None = None,
) -> tuple[_WindowContext, tuple[RowCheck, ...]]:
    boundary_timezone(boundary)
    calendars = [m for m in metric.measures if m.aggregation == "avg_calendar_day"]
    domain_context = {
        "metric": metric.name,
        "output": output,
        "window_start": window.start.seconds,
        "window_end": window.end.seconds,
        "boundary": boundary,
        "phase": "pre_output",
    }
    for measure in calendars:
        if calendar_average_alignment(window, anchor=None, day_boundary=boundary) == "unaligned":
            raise UNALIGNED_CALENDAR_AVERAGE.error(measure=measure.event, **domain_context)

    coverages = [readiness.for_fact(measure.source_name) for measure in metric.measures]
    key = _WindowContextKey(
        window.start.seconds,
        window.end.seconds,
        max(coverage.start for coverage in coverages),
        min(coverage.end_exclusive for coverage in coverages),
        boundary,
        resolution,
        bool(calendars),
    )
    if key not in shared:
        if shared_support is None:
            shared[key] = _window_fields(builder, key)
        else:
            support_key = (
                tuple(measure.source_name for measure in metric.measures),
                key.source_start,
                key.source_end,
                ibis.literal(key.source_start).type(),
                ibis.literal(key.source_end).type(),
            )
            shared[key] = _window_fields(builder, key, support=shared_support.get(support_key))
            shared_support[support_key] = shared[key].observed_from, shared[key].observed_until
    context = shared[key]
    checks = ()
    if context.unaligned is not None:
        refusal = UNALIGNED_CALENDAR_AVERAGE if calendars else _DAY_ALIGNMENT
        error_context = (
            {**domain_context, "measure": calendars[0].event} if calendars else domain_context
        )
        checks = (RowCheck(context.unaligned, refusal, error_context),)
    return context, checks


def _window_fields(
    builder: _Builder, key: _WindowContextKey, *, support: tuple[str, str] | None = None
) -> _WindowContext:
    """Build fields shared by requests with the same coverage and window semantics."""
    anchors = builder.table

    def shifted(seconds: int):
        t0 = builder.table.t0
        return t0 + ibis.interval(seconds=seconds) if seconds else t0

    start = builder.put("start", shifted(key.start_seconds))
    end = builder.put("end", shifted(key.end_seconds))
    unaligned = None
    if key.calendar_alignment or key.resolution == "day":
        offset = boundary_timezone(key.boundary).utcoffset(None)
        minutes = (offset.days * 86400 + offset.seconds) // 60
        invalid = builder.put(
            "unaligned",
            builder.table.t0.notnull()
            & ~(_aligned(builder.col(start), minutes) & _aligned(builder.col(end), minutes)),
        )
        unaligned = builder.put("check", builder.col(invalid).fill_null(True))
    source_start, source_end = key.source_start, key.source_end
    source_complete = builder.put(
        "source_complete",
        (builder.col(start) >= ibis.literal(source_start))
        & (builder.col(end) <= ibis.literal(source_end)),
    )
    observed_from = ibis.literal(source_start)
    observed_until = ibis.literal(source_end)
    bounds = []
    if "_observed_from" in anchors.columns:
        bound = builder.table._observed_from
        if support is None:
            observed_from = greatest(observed_from, bound.fill_null(observed_from))
        bounds.append(bound.isnull() | (builder.col(start) >= bound))
    if "_observed_until" in anchors.columns:
        bound = builder.table._observed_until
        if support is None:
            observed_until = least(observed_until, bound.fill_null(observed_until))
        bounds.append(bound.isnull() | (builder.col(end) <= bound))
    if support is None:
        observed_from = builder.put("observed_from", observed_from)
        observed_until = builder.put("observed_until", observed_until)
    else:
        observed_from, observed_until = support
    observation_complete = cast(ir.BooleanValue, ibis.literal(True))
    if bounds:
        conjunction = bounds[0] if len(bounds) == 1 else bounds[0] & bounds[1]
        observation_complete = builder.col(builder.put("observed", conjunction))
    available = builder.put("complete", builder.col(source_complete) & observation_complete)
    unavailable = builder.put(
        "unavailable_reason",
        (~builder.col(source_complete)).ifelse(
            reason_codes.INCOMPLETE_COVERAGE,
            (~observation_complete).ifelse(
                ibis.literal(reason_codes.INCOMPLETE_OBSERVATION), ibis.null().cast("string")
            ),
        ),
    )
    return _WindowContext(
        start, end, available, unavailable, observed_from, observed_until, unaligned
    )


def _local_component(
    builder: _Builder,
    measure: BoundMeasure,
    fields: Mapping[str, str],
    *,
    days: int,
    conversion: bool,
    context: Mapping[str, Any],
) -> _Quantity:
    """Finalize one reduced primitive without reading any cumulative state."""
    events = builder.col(fields["event_count"])
    zero = exact_literal(0, events)
    empty = builder.put("empty", events == zero)
    no_reason = ibis.null(dt.string)
    if conversion:
        value = builder.put("converted", events > zero)
        return _Quantity(value, builder.put("reason", no_reason), empty)
    if measure.aggregation == "count":
        return _Quantity(fields["event_count"], builder.put("reason", no_reason), empty, count=True)
    nonnull = builder.col(fields["nonnull_count"])
    all_null = (events > zero) & (nonnull == exact_literal(0, nonnull))
    reason = builder.put(
        "reason",
        all_null.ifelse(
            reason_codes.ALL_VALUES_NULL,
            builder.col(empty).ifelse(reason_codes.NO_OBSERVATIONS, no_reason)
            if measure.aggregation == "avg_event"
            else no_reason,
        ),
    )
    value = builder.col(fields["sum_value"])
    invalid = cast(ir.BooleanValue, ibis.literal(False))
    if "sum_withheld" in fields:
        count = builder.col(fields["sum_withheld"])
        withheld = builder.put("sum_withheld", count > exact_literal(0, count))
        builder.numeric.append((withheld, _EXACT_OVERFLOW, context))
        invalid |= builder.col(withheld)
    for name, failure in (
        ("float64_nonfinite_count", "nonfinite_input"),
        ("float64_unrepresentable_count", "out_of_range"),
    ):
        if name in fields:
            count = builder.col(fields[name])
            flag = count > exact_literal(0, count)
            builder.flag(flag, failure, context)
            invalid |= flag
    native = native_arithmetic(measure)
    if native:
        builder.flag(~float_is_finite(value), "bound_overflow", context)
        value_zero = ibis.literal(0.0)
    else:
        value_zero = exact_literal(0, value)
    known_zero = builder.put("sum_zero", (value == value_zero) & ~invalid)
    note = None
    if "float64_below_resolution_count" in fields:
        count = builder.col(fields["float64_below_resolution_count"])
        note = builder.put(
            "note",
            (count > exact_literal(0, count)).ifelse(reason_codes.BELOW_RESOLUTION, no_reason),
        )
    assert measure.value_type is not None
    dtype = dt.dtype(measure.value_type)
    scale = (
        9
        if dtype.is_floating() and not native
        else ((dtype.scale or 0) if isinstance(dtype, dt.Decimal) else 0)
    )
    quantity = _Quantity(
        fields["sum_value"],
        reason,
        known_zero,
        scale=scale,
        binary64=dtype.is_floating(),
        native=native,
        note=note,
    )
    if measure.aggregation in {"avg_event", "avg_calendar_day"}:
        divisor = (
            fields["nonnull_count"]
            if measure.aggregation == "avg_event"
            else builder.put("days", exact_literal(days, events))
        )
        divisor_zero = builder.put(
            "divisor_zero", builder.col(divisor) == exact_literal(0, builder.col(divisor))
        )
        denominator = _Quantity(divisor, reason, divisor_zero, count=True)
        quantity = _quotient(builder, quantity, denominator, reason, context)
    return quantity


@dataclass(frozen=True, slots=True)
class _Primitive:
    measure: BoundMeasure
    window: Window
    counting: bool


@dataclass(frozen=True, slots=True)
class _MembershipKey:
    source: str
    window: Window
    available: tuple[str, ...]


def _membership_keys(
    source: BoundSource, primitives: Mapping[_Primitive, Sequence[_WindowContext]]
) -> dict[_Primitive, _MembershipKey]:
    return {
        primitive: _MembershipKey(
            source.name,
            primitive.window,
            tuple(dict.fromkeys(context.available for context in contexts)),
        )
        for primitive, contexts in primitives.items()
    }


def _available(table: ibis.Table, membership: _MembershipKey):
    if not membership.available:
        return ibis.literal(False)
    names = iter(membership.available)
    active = table[next(names)]
    for name in names:
        active |= table[name]
    return active


def _attach_timestamp_comparison_key(
    table: ibis.Table,
    timestamp: ir.TimestampValue,
    *,
    rank_name: str,
    coordinate_name: str,
) -> tuple[ibis.Table, _TimestampComparisonKey]:
    key = _timestamp_comparison_key(timestamp)
    table = table.mutate(**{rank_name: key.rank, coordinate_name: key.nanoseconds})
    return table, _TimestampComparisonKey(
        cast(ir.IntegerValue, table[rank_name]),
        cast(ir.DecimalValue, table[coordinate_name]),
    )


def _interval(
    timestamp,
    t0,
    window: Window,
    *,
    timestamp_key: _TimestampComparisonKey | None = None,
    t0_key: _TimestampComparisonKey | None = None,
    shifted_points: Mapping[int, ir.TimestampValue] | None = None,
):
    def compare(offset: int, op: Literal["ge", "lt"]):
        if shifted_points is not None and offset in shifted_points:
            return compare_timestamps(timestamp, shifted_points[offset], op)
        if (
            timestamp_key is not None
            and t0_key is not None
            and (offset or current_dialect(timestamp, t0).comparison_keys(timestamp, t0) is None)
        ):
            return _compare_timestamp_keys(timestamp_key, t0_key, op, right_offset_seconds=offset)
        return compare_timestamps(timestamp, t0, op, right_offset_seconds=offset)

    return compare(window.start.seconds, "ge") & compare(window.end.seconds, "lt")


def _reduce_source(
    builder: _Builder,
    events: ibis.Table,
    source: BoundSource,
    primitives: Mapping[_Primitive, Sequence[_WindowContext]],
    origin_bounds: tuple[datetime, datetime] | None,
) -> tuple[ibis.Table, dict[_Primitive, dict[str, str]], tuple[str, str]]:
    """Join one bounded event source and reduce each selected primitive at anchor grain."""
    unit, anchor_id, t0 = (builder.name(name) for name in ("unit", "anchor_id", "t0"))
    available = dict.fromkeys(
        context.available for contexts in primitives.values() for context in contexts
    )
    table = builder.table
    points = table.select(
        table.unit.name(unit), table.anchor_id.name(anchor_id), table.t0.name(t0), *available
    )
    timestamp_key = t0_key = None
    key_names = None
    offsets = dict.fromkeys(
        offset
        for primitive in primitives
        for offset in (primitive.window.start.seconds, primitive.window.end.seconds)
    )
    dialect = current_dialect(events._timestamp, points[t0])
    shifted_names = {}
    shifted_values = {}
    if origin_bounds is not None:
        for offset in offsets:
            if offset:
                native = dialect.native_shifted_comparison_keys(
                    events._timestamp,
                    points[t0],
                    offset_seconds=offset,
                    right_bounds=origin_bounds,
                )
                if native is not None:
                    name = builder.name("shifted_point")
                    shifted_names[offset] = name
                    shifted_values[name] = native[1]
    if shifted_values:
        points = points.mutate(**shifted_values)
    if any(
        offset not in shifted_names
        and (offset or dialect.comparison_keys(events._timestamp, points[t0]) is None)
        for offset in offsets
    ):
        event_rank, event_coordinate, point_rank, point_coordinate = (
            builder.name(name)
            for name in ("event_rank", "event_coordinate", "point_rank", "point_coordinate")
        )
        key_names = event_rank, event_coordinate, point_rank, point_coordinate
        events, timestamp_key = _attach_timestamp_comparison_key(
            events,
            events._timestamp,
            rank_name=event_rank,
            coordinate_name=event_coordinate,
        )
        points, t0_key = _attach_timestamp_comparison_key(
            points,
            points[t0],
            rank_name=point_rank,
            coordinate_name=point_coordinate,
        )
    shifted_points = {
        offset: cast(ir.TimestampValue, points[name]) for offset, name in shifted_names.items()
    }
    membership_keys = _membership_keys(source, primitives)
    unique_memberships = dict.fromkeys(membership_keys.values())
    matched = ibis.literal(False)
    for membership in unique_memberships:
        matched |= _available(points, membership) & _interval(
            events._timestamp,
            points[t0],
            membership.window,
            timestamp_key=timestamp_key,
            t0_key=t0_key,
            shifted_points=shifted_points,
        )
    joined = events.inner_join(points, (events._unit == points[unit]) & matched)
    if key_names is not None:
        event_rank, event_coordinate, point_rank, point_coordinate = key_names
        timestamp_key = _TimestampComparisonKey(
            cast(ir.IntegerValue, joined[event_rank]),
            cast(ir.DecimalValue, joined[event_coordinate]),
        )
        t0_key = _TimestampComparisonKey(
            cast(ir.IntegerValue, joined[point_rank]),
            cast(ir.DecimalValue, joined[point_coordinate]),
        )
    shifted_points = {
        offset: cast(ir.TimestampValue, joined[name]) for offset, name in shifted_names.items()
    }
    masks = {membership: builder.name("consumed") for membership in unique_memberships}
    joined = joined.mutate(
        **{
            name: _available(joined, membership)
            & _interval(
                joined._timestamp,
                joined[t0],
                membership.window,
                timestamp_key=timestamp_key,
                t0_key=t0_key,
                shifted_points=shifted_points,
            )
            for membership, name in masks.items()
        }
    )
    aliases: dict[_Primitive, dict[str, str]] = {}
    payload = []
    branches = []
    for primitive in primitives:
        consumed = joined[masks[membership_keys[primitive]]]
        projected, fields = contributions(
            joined,
            primitive.measure,
            source=source,
            consumed=consumed,
            counting=primitive.counting,
            _name=builder.name,
        )
        names = {name: builder.name(name) for name in fields}
        aliases[primitive] = names
        payload.extend(names.values())
        branches.append(projected.select(**{names[name]: value for name, value in fields.items()}))
    leaf = _batch_projections(joined, branches).select(unit, anchor_id, *payload)
    reduced = _reduce_payload(leaf, aliases, [unit, anchor_id])
    return reduced, aliases, (unit, anchor_id)


def _reduce_payload(leaf, aliases, keys):
    native, exact = [], []
    for primitive, names in aliases.items():
        if not primitive.counting:
            (native if native_arithmetic(primitive.measure) else exact).append(names["sum_value"])
    reduced, withheld = reduce_payload(leaf, keys, native_values=native, exact_values=exact)
    for names in aliases.values():
        name = names.get("sum_value")
        if name in withheld:
            flag = withheld[name]
            if "sum_withheld" in names:
                previous = names["sum_withheld"]
                reduced = reduced.mutate(**{previous: reduced[previous] + reduced[flag]}).drop(flag)
            else:
                names["sum_withheld"] = flag
    return reduced


@dataclass(frozen=True, slots=True)
class _BucketPrimitive:
    measure: BoundMeasure
    counting: bool


def _day_consumed(bucket, points, membership, contexts):
    """An available aligned request contains the fixed-offset bucket."""
    context = contexts[membership.available[0]]
    return (
        _available(points, membership)
        & (bucket >= points[context.start])
        & (bucket < points[context.end])
    )


def _attach_timestamp_parts(
    table: ibis.Table,
    timestamp: ir.TimestampValue,
    *,
    names: tuple[str, str, str],
) -> tuple[ibis.Table, tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue]]:
    """Project lossless integer coordinates before repeated endpoint comparisons."""
    parts = timestamp_parts(timestamp)
    fields = tuple(part.op() for part in parts)
    if current_dialect(timestamp).comparison_keys(timestamp, timestamp) is not None and all(
        isinstance(part, ops.StructField) for part in fields
    ):
        struct_fields = cast(tuple[ops.StructField, ...], fields)
        key = struct_fields[0].arg
        if all(part.arg == key for part in struct_fields):
            # Keep the complete nullable key intact: its guards still withhold
            # unsafe instants before conversion, even when the fields are shared.
            name = f"{names[0]}_key"
            used = {column.casefold() for column in (*table.columns, *names)}
            while name.casefold() in used:
                name += "_"
            table = table.mutate(**{name: key.to_expr()})
            parts = cast(
                tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
                tuple(table[name][part.field] for part in struct_fields),
            )
    table = table.mutate(**dict(zip(names, parts, strict=True)))
    return table, cast(
        tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
        tuple(table[name] for name in names),
    )


def _masked_day_events(
    builder: _Builder,
    events: ibis.Table,
    contexts: Iterable[_WindowContext],
    *,
    endpoint_bounds: tuple[datetime, datetime] | None = None,
) -> tuple[ibis.Table, str]:
    """Mask unused instants before any fixed-offset calendar conversion."""
    original_fields = events.columns
    builder.used.update(name.casefold() for name in events.columns)
    table = builder.table
    unit, lower, upper, timestamp = (
        builder.name(name) for name in ("scope_unit", "scope_start", "scope_end", "day_timestamp")
    )
    starts, ends = [], []
    for context in contexts:
        active = table[context.available]
        if context.unaligned is not None:
            active &= ~table[context.unaligned].fill_null(True)
        start, end = table[context.start], table[context.end]
        starts.append(active.ifelse(start, ibis.null(start.type())))
        ends.append(active.ifelse(end, ibis.null(end.type())))
    rows = table.select(table.unit.name(unit), **{lower: least(*starts), upper: greatest(*ends)})
    envelope = rows.group_by(unit).aggregate(**{lower: rows[lower].min(), upper: rows[upper].max()})
    dialect = current_dialect(events._timestamp, envelope[lower], envelope[upper])
    comparison_fields = {lower: lower, upper: upper}
    event_type = cast(dt.Timestamp, events._timestamp.type())
    # Active envelope endpoints are requested bounds, all covered by the derived
    # finite host interval. Unbounded UTC timestamps need not survive conversion.
    if (
        endpoint_bounds is not None
        and event_type.timezone is None
        and event_type.scale in (None, 6)
        and dialect.comparison_keys(events._timestamp, events._timestamp) is not None
    ):
        clocks = {}
        for name in (lower, upper):
            dtype = cast(dt.Timestamp, envelope[name].type())
            if (
                dtype.timezone == "UTC"
                and dtype.scale in (None, 6)
                and dialect.comparison_keys(events._timestamp, envelope[name]) is None
            ):
                clock = utc_wall_clock(envelope[name])
                if dialect.comparison_keys(events._timestamp, clock) is not None:
                    field = builder.name("scope_clock")
                    clocks[field] = clock
                    comparison_fields[name] = field
        if clocks:
            envelope = envelope.mutate(**clocks)
    fallback = {
        name
        for name in (lower, upper)
        if dialect.comparison_keys(events._timestamp, envelope[comparison_fields[name]]) is None
    }
    event_names = None
    endpoint_names = {}
    if fallback:
        event_names = cast(
            tuple[str, str, str],
            tuple(builder.name(label) for label in ("event_rank", "event_seconds", "event_nanos")),
        )
        events, _ = _attach_timestamp_parts(events, events._timestamp, names=event_names)
        branches = []
        for name in (lower, upper):
            if name not in fallback:
                continue
            names = cast(
                tuple[str, str, str],
                tuple(
                    builder.name(label) for label in ("bound_rank", "bound_seconds", "bound_nanos")
                ),
            )
            projected, _ = _attach_timestamp_parts(
                envelope, envelope[comparison_fields[name]], names=names
            )
            branches.append(projected)
            endpoint_names[name] = names
        envelope = _batch_projections(envelope, branches)
    joined = events.left_join(envelope, events._unit == envelope[unit])

    def compare(name: str, op: Literal["ge", "lt"]) -> ir.BooleanValue:
        if event_names is None or name not in endpoint_names:
            return compare_timestamps(joined._timestamp, joined[comparison_fields[name]], op)
        return _compare_timestamp_parts(
            cast(
                tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
                tuple(joined[field] for field in event_names),
            ),
            cast(
                tuple[ir.IntegerValue, ir.IntegerValue, ir.IntegerValue],
                tuple(joined[field] for field in endpoint_names[name]),
            ),
            op,
        )

    inside = compare(lower, "ge") & compare(upper, "lt")
    instant = joined._timestamp
    masked = inside.ifelse(instant, ibis.null(instant.type()))
    return joined.select(*original_fields, **{timestamp: masked}), timestamp


def _reduce_daily_source(
    builder: _Builder,
    events: ibis.Table,
    source: BoundSource,
    primitives: Mapping[_Primitive, Sequence[_WindowContext]],
    boundary: str,
    origin_bounds: tuple[datetime, datetime] | None,
) -> tuple[ibis.Table, dict[_Primitive, dict[str, str]], tuple[str, str]]:
    """Scope unique unit/day identities before evaluating any numerical payload."""
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
    bucket, scope_unit, scope_bucket, unit, anchor_id = (
        builder.name(name) for name in ("bucket", "scope_unit", "scope_bucket", "unit", "anchor_id")
    )
    window_contexts = {
        context.available: context for contexts in primitives.values() for context in contexts
    }
    endpoint_bounds = None
    if origin_bounds is not None:
        offsets = [
            seconds
            for primitive in primitives
            for seconds in (primitive.window.start.seconds, primitive.window.end.seconds)
        ]
        try:
            endpoint_bounds = (
                origin_bounds[0] + timedelta(seconds=min(offsets)),
                origin_bounds[1] + timedelta(seconds=max(offsets)),
            )
        except OverflowError:
            pass
    events, timestamp = _masked_day_events(
        builder, events, window_contexts.values(), endpoint_bounds=endpoint_bounds
    )
    events = events.mutate(**{bucket: local_day(events[timestamp], minutes)[1]})
    identities = events.select(
        events._unit.name(scope_unit), events[bucket].name(scope_bucket)
    ).distinct()
    bounds = dict.fromkeys(
        name
        for contexts in primitives.values()
        for context in contexts
        for name in (context.available, context.start, context.end)
    )
    table = builder.table
    points = table.select(table.unit.name(unit), table.anchor_id.name(anchor_id), *bounds)
    membership_keys = _membership_keys(source, primitives)
    buckets: dict[_BucketPrimitive, str] = {}
    bucket_memberships: dict[_BucketPrimitive, dict[_MembershipKey, None]] = {}
    for primitive, membership in membership_keys.items():
        key = _BucketPrimitive(primitive.measure, primitive.counting)
        if key not in buckets:
            buckets[key] = builder.name("consumed")
        bucket_memberships.setdefault(key, {})[membership] = None
    count_donors = {}
    # An unfiltered day's row count is independent of its value binding. A donor
    # can cover extra days, but each count keeps its own anchor/window mask below.
    for key, windows in bucket_memberships.items():
        if key.counting and not key.measure.filters and not source.where:
            for donor, donor_windows in bucket_memberships.items():
                if (
                    not donor.counting
                    and not donor.measure.filters
                    and windows.keys() <= donor_windows.keys()
                ):
                    count_donors[key] = donor
                    break
    masks = {
        membership: builder.name("consumed")
        for membership in dict.fromkeys(membership_keys.values())
    }
    required = identities.inner_join(points, identities[scope_unit] == points[unit])
    required = required.select(
        scope_unit,
        scope_bucket,
        unit,
        anchor_id,
        **{
            name: _day_consumed(required[scope_bucket], required, membership, window_contexts)
            for membership, name in masks.items()
        },
    )
    names = iter(masks.values())
    matched = required[next(names)]
    for name in names:
        matched |= required[name]
    required = required.filter(matched)
    bucket_predicates = {}
    # The required rows already satisfy at least one complete membership.
    always_consumed = {
        key
        for key, windows in bucket_memberships.items()
        if not key.measure.filters and not source.where and windows.keys() == masks.keys()
    }
    for key, windows in bucket_memberships.items():
        if key in count_donors or key in always_consumed:
            continue
        members = iter(windows)
        predicate = required[masks[next(members)]]
        for window in members:
            predicate |= required[masks[window]]
        bucket_predicates[buckets[key]] = predicate
    if bucket_predicates:
        scope = required.mutate(**bucket_predicates)
        scope = scope.group_by(scope_unit, scope_bucket).aggregate(
            **{name: scope[name].any() for name in bucket_predicates}
        )
    else:
        scope = required.select(scope_unit, scope_bucket).distinct()
    selected = events.inner_join(
        scope,
        (events._unit == scope[scope_unit]) & (events[bucket] == scope[scope_bucket]),
    )
    bucket_aliases = {}
    payload = []
    branches = []
    for key, mask in buckets.items():
        if key in count_donors:
            continue
        projected, fields = contributions(
            selected,
            key.measure,
            source=source,
            consumed=cast(
                ir.BooleanValue, ibis.literal(True) if key in always_consumed else selected[mask]
            ),
            counting=key.counting,
            _name=builder.name,
        )
        names = {name: builder.name(name) for name in fields}
        bucket_aliases[key] = names
        payload.extend(names.values())
        branches.append(projected.select(**{names[name]: value for name, value in fields.items()}))
    leaf = _batch_projections(selected, branches).select(scope_unit, scope_bucket, *payload)
    daily = _reduce_payload(leaf, bucket_aliases, [scope_unit, scope_bucket])
    for key, donor in count_donors.items():
        bucket_aliases[key] = {"event_count": bucket_aliases[donor]["event_count"]}
    joined = daily.inner_join(
        required,
        (daily[scope_unit] == required[scope_unit])
        & (daily[scope_bucket] == required[scope_bucket]),
    )
    aliases = {}
    payload = {}
    event_counts = {}
    for primitive, membership in membership_keys.items():
        key = _BucketPrimitive(primitive.measure, primitive.counting)
        consumed_flag = joined[masks[membership]]
        names = {}
        aliases[primitive] = names
        for name, original in bucket_aliases[key].items():
            term = membership, original
            if name == "event_count" and term in event_counts:
                names[name] = event_counts[term]
                continue
            names[name] = builder.name(name)
            if name == "event_count":
                event_counts[term] = names[name]
            value = joined[original]
            payload[names[name]] = consumed_flag.ifelse(value, ibis.null(value.type()))
    leaf = joined.select(unit, anchor_id, **payload)
    reduced = _reduce_payload(leaf, aliases, [unit, anchor_id])
    return reduced, aliases, (unit, anchor_id)


def _finalization_fields(
    anchors: ibis.Table,
    context: _WindowContext,
    keys: Sequence[_Primitive],
    reduced_fields: Mapping[_Primitive, Mapping[str, str]],
) -> tuple[str, ...]:
    """Keep the original anchors and this output's payload, support and checks."""
    names = list(anchors.columns)
    for key in keys:
        names.extend(reduced_fields[key].values())
    names.extend(
        (
            context.available,
            context.unavailable,
            context.observed_from,
            context.observed_until,
        )
    )
    if context.unaligned is not None:
        names.append(context.unaligned)
    return tuple(dict.fromkeys(names))


def aggregate_windows(
    anchors: ibis.Table,
    requests: Sequence[AggregateRequest],
    sources: Mapping[str, tuple[ibis.Table, BoundSource]],
    readiness: Readiness,
    *,
    boundary: str,
    resolution: Literal["event", "day"] = "event",
    _origin_bounds: tuple[datetime, datetime] | None = None,
) -> TemporalResult:
    """Reduce selected raw or scoped daily contributions for half-open windows.

    Availability masks arithmetic per output. Independent sources are reduced
    before joining, preserving full anchor identity and avoiding event fan-out.
    Call ``validate`` before exporting the result under the same snapshot.
    """
    if resolution not in {"event", "day"}:
        raise ValueError("resolution must be event or day")
    for name in ("unit", "anchor_id", "t0"):
        if name not in anchors.columns:
            raise _SCHEMA.error(field="anchor", column=name)
    for name in ("t0", "_observed_from", "_observed_until"):
        if name in anchors.columns:
            dtype = anchors[name].type()
            if not dtype.is_timestamp() or dtype.timezone != "UTC":
                raise _SCHEMA.error(field="UTC-aware anchor or observation", column=name)
    builder = _Builder(anchors, "")
    builder.used.update(
        name.casefold() for events, _ in sources.values() for name in events.columns
    )
    contexts = []
    context_checks = []
    shared_contexts: dict[_WindowContextKey, _WindowContext] = {}
    # Data aliases share this exact anchor relation; checks remain per request.
    shared_support: dict[tuple[Any, ...], tuple[str, str]] = {}
    context_branches = []
    primitives: dict[str, dict[_Primitive, list[_WindowContext]]] = {}
    components = []
    outputs = {name.casefold() for name in anchors.columns}
    for request in requests:
        metric, window, output = request.metric, request.window, request.output
        if not isinstance(window, Window):
            raise TypeError("window must be a Window")
        if (
            not output
            or not isinstance(output, str)
            or not request.role
            or not isinstance(request.role, str)
        ):
            raise ValueError("output and role must be nonempty names")
        if output.casefold() in outputs:
            raise ValueError("outputs must be unique and not replace anchor fields")
        outputs.add(output.casefold())
        builder.used.add(output.casefold())
        if metric.kind not in {"mean", "conversion", "ratio"}:
            raise UNSUPPORTED_TEMPORAL_KIND.error(kind=metric.kind)
        if metric.transforms:
            raise UNSUPPORTED_TRANSFORM.error(transform=type(metric.transforms[0]).__name__)
        if len(metric.measures) != (2 if metric.kind == "ratio" else 1):
            raise ValueError("metric kind has an invalid number of measures")
        branch = builder.fork(anchors)
        context, checks = _window_context(
            branch,
            metric,
            window,
            readiness,
            boundary,
            output,
            resolution,
            shared=shared_contexts,
            shared_support=shared_support if resolution == "day" else None,
        )
        if branch.table is not anchors:
            context_branches.append(branch.table)
        context_checks.append(checks)
        contexts.append(context)
        keys = []
        for measure in metric.measures:
            if measure.value_type is None:
                raise ValueError("aggregate_windows requires resolved measure descriptors")
            if measure.source_name not in sources:
                raise ValueError("a resolved metric measure has no supplied event source")
            events, source = sources[measure.source_name]
            resolve_measure(events, measure, source=source)
            counting = metric.kind == "conversion" or measure.aggregation == "count"
            if counting or not native_arithmetic(measure):
                _require_backend(events)
            canonical = replace(
                measure,
                aggregation="count" if counting else "sum",
                exact=not native_arithmetic(measure) if not counting else True,
            )
            key = _Primitive(canonical, window, counting)
            primitives.setdefault(measure.source_name, {}).setdefault(key, []).append(context)
            keys.append(key)
        components.append(keys)
    builder.table = _batch_projections(anchors, context_branches)
    reductions = [
        (
            _reduce_daily_source(
                builder, *sources[name], source_primitives, boundary, _origin_bounds
            )
            if resolution == "day"
            else _reduce_source(builder, *sources[name], source_primitives, _origin_bounds)
        )
        for name, source_primitives in primitives.items()
    ]
    reduced_fields = {}
    for reduced, aliases, (unit, anchor_id) in reductions:
        table = builder.table
        joined = table.left_join(
            reduced, [table.unit == reduced[unit], table.anchor_id == reduced[anchor_id]]
        ).drop(unit, anchor_id)
        fields = [field_name for names in aliases.values() for field_name in names.values()]
        builder.table = joined.mutate(
            **{
                name: joined[name].fill_null(
                    ibis.literal(0.0)
                    if joined[name].type().is_floating()
                    else exact_literal(0, joined[name])
                )
                for name in fields
            }
        )
        reduced_fields.update(aliases)
    columns = {}
    row_checks = []
    retained = builder.table
    output_branches = []
    for request, context, checks, keys in zip(
        requests, contexts, context_checks, components, strict=True
    ):
        required_names = _finalization_fields(anchors, context, keys, reduced_fields)
        branch = builder.fork(retained.select(*required_names))
        quantities, unknown_flags = [], []
        for measure, key in zip(request.metric.measures, keys, strict=True):
            fields = reduced_fields[key]
            numeric_context = {
                "source": measure.source_name,
                "measure": measure.event,
                "output": request.output,
            }
            if measure.filters:
                count = branch.col(fields["unknown_filter_count"])
                flag = branch.put("unknown_filter", count > exact_literal(0, count))
                unknown_flags.append((flag, UNKNOWN_FILTER, numeric_context))
            quantities.append(
                _local_component(
                    branch,
                    measure,
                    fields,
                    days=(request.window.end.seconds - request.window.start.seconds) // 86400,
                    conversion=request.metric.kind == "conversion",
                    context=numeric_context,
                )
            )
        result = _finalize_window(
            branch,
            anchors,
            request.metric,
            request.window,
            request.output,
            request.role,
            quantities,
            context.available,
            context.unavailable,
            context.observed_from,
            context.observed_until,
            list(checks),
            unknown_flags,
        )
        output_branches.append(result.table)
        columns.update(result.columns)
        row_checks.extend(result.row_checks)
    builder.table = _batch_projections(retained, output_branches)
    public = [*anchors.columns]
    for output, meta in columns.items():
        assert meta.observed_from_column is not None
        assert meta.observed_until_column is not None
        assert meta.complete_column is not None
        assert meta.reason_column is not None
        public.extend(
            [
                output,
                meta.observed_from_column,
                meta.observed_until_column,
                meta.complete_column,
                meta.reason_column,
            ]
        )
    public.extend(check.column for check in row_checks)
    return TemporalResult(
        builder.table.select(*dict.fromkeys(public)), columns, row_checks=tuple(row_checks)
    )
