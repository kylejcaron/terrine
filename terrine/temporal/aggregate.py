"""Strict half-open windows with deferred observation and arithmetic checks."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
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
    exact_subtract,
    exact_to_float,
    float_is_finite,
    float_ratio,
)
from terrine.backends.timestamps import compare_timestamps, day_start
from terrine.data import ColumnMeta, RowCheck
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure, BoundMetric, BoundSource
from terrine.semantic.rules import (
    UNALIGNED_CALENDAR_AVERAGE,
    UNKNOWN_FILTER,
    UNSUPPORTED_TEMPORAL_AGGREGATION,
    UNSUPPORTED_TEMPORAL_KIND,
    UNSUPPORTED_TRANSFORM,
    calendar_average_alignment,
    supports_temporal_aggregation,
)
from terrine.semantic.time import boundary_timezone
from terrine.temporal.daily import (
    _EXACT_FLOAT_COUNTS,
    _NATIVE_FLOAT_COUNTS,
    _UNKNOWN_FIELDS,
    _require_backend,
    contributions,
    native_arithmetic,
    reduce_payload,
    resolve_measure,
)
from terrine.temporal.lookup import lookup_prefix
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

__all__ = ["TemporalResult", "aggregate_window"]

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

    def __init__(self, table: ibis.Table, output: str, *, practical: bool = False) -> None:
        self._table = table
        self._staged: dict[str, ir.Value] = {}
        self.used = {name.casefold() for name in (*table.columns, output)}
        self.counter = 0
        self.numeric: list[tuple[str, RefusalSpec, Mapping[str, Any]]] = []
        self.practical = practical

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

    def name(self, label: str) -> str:
        # Intermediates share the relation with declared outputs, so they stay in
        # the reserved namespace; the label's initials keep the name short, since
        # it is listed at every level of the statement.
        initials = "".join(word[0] for word in label.split("_") if word)
        while True:
            name = f"__terrine_w{self.counter}_{initials}"
            self.counter += 1
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
    if builder.practical and (numerator.native or denominator.native):
        n_float = exact_to_float(n, numerator.scale)[0]
        d_float = exact_to_float(d, denominator.scale)[0]
        divisor = builder.col(denominator.known_zero).ifelse(ibis.null(dt.float64), d_float)
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
        native=builder.practical and (numerator.native or denominator.native),
    )


def _state_fields(measure: BoundMeasure, state: ibis.Table, *, counting: bool) -> tuple[str, ...]:
    """The state columns a window reads for ``measure``.

    A count or conversion reads the event count alone; a sum or average adds
    the nonnull count and sum, a float fact its invalid-input counts; a
    fixed-precision state adds its withheld count. Only a filtered measure
    carries the unknown-filter counts.
    """
    fields = ["event_count"]
    if not counting:
        fields += ["nonnull_count", "sum_value"]
        assert measure.value_type is not None  # callers pass resolved measures
        if native_arithmetic(measure):
            fields += _NATIVE_FLOAT_COUNTS
        elif dt.dtype(measure.value_type).is_floating():
            fields += _EXACT_FLOAT_COUNTS
        if "sum_withheld" in state.columns:
            fields.append("sum_withheld")
    if measure.filters:
        fields += _UNKNOWN_FIELDS
    return tuple(fields)


def _native_window_sum(
    builder: _Builder,
    start: Mapping[str, str],
    end: Mapping[str, str],
    *,
    empty: str,
    reason: str,
    context,
) -> _Quantity:
    """The binary64 difference of two running sums, checked finite.

    Its rounding error is bounded by the history prefix through the window end,
    not by the window alone. Non-finite inputs are counted instead of summed, so
    one at or before the window end refuses a non-empty window; a running sum
    that overflowed leaves no finite difference and refuses as ``bound_overflow``.
    Where the policy bounds float arithmetic the prefix withholds such a state
    instead and counts it, and a non-empty window reading a withheld state at
    or before its end refuses the same way.
    """
    zero = ibis.literal(0.0)
    difference = builder.col(end["sum_value"]) - builder.col(start["sum_value"])
    withheld = None
    if "sum_withheld" in end:
        count = builder.col(end["sum_withheld"])
        withheld = builder.put(
            "state_withheld", ~builder.col(empty) & (count > exact_literal(0, count))
        )
        difference = builder.col(withheld).ifelse(ibis.null(dt.float64), difference)
    # Empty windows remain zero, even after invalid input history.
    value = builder.put("sum", builder.col(empty).ifelse(zero, difference))
    held = builder.col(end["float64_nonfinite_count"])
    nonfinite = ~builder.col(empty) & (
        held.fill_null(exact_literal(0, held)) > exact_literal(0, held)
    )
    builder.flag(nonfinite, "nonfinite_input", context)
    if withheld is not None:
        builder.flag(builder.col(withheld), "bound_overflow", context)
    builder.flag(~float_is_finite(builder.col(value)), "bound_overflow", context)
    # As for exact sums, dropped inputs never make a known zero denominator.
    known_zero = builder.put("sum_zero", (builder.col(value) == zero) & ~nonfinite)
    return _Quantity(value, reason, known_zero, binary64=True, native=True)


def _component(
    builder: _Builder,
    measure: BoundMeasure,
    start: Mapping[str, str],
    end: Mapping[str, str],
    *,
    days: int,
    conversion: bool,
    context,
) -> _Quantity:
    event_count = builder.put(
        "events", exact_subtract(builder.col(end["event_count"]), builder.col(start["event_count"]))
    )
    zero = exact_literal(0, builder.col(event_count))
    empty = builder.put("empty", builder.col(event_count) == zero)
    no_reason = ibis.null().cast("string")
    if conversion:
        value = builder.put("converted", builder.col(event_count) > zero)
        return _Quantity(value, builder.put("reason", no_reason), empty)
    if measure.aggregation == "count":
        return _Quantity(event_count, builder.put("reason", no_reason), empty, count=True)
    nonnull_count = builder.put(
        "nonnull",
        exact_subtract(builder.col(end["nonnull_count"]), builder.col(start["nonnull_count"])),
    )
    all_null = builder.put(
        "all_null", (builder.col(event_count) > zero) & (builder.col(nonnull_count) == zero)
    )
    reason = builder.put(
        "reason",
        builder.col(all_null).ifelse(
            reason_codes.ALL_VALUES_NULL,
            (
                builder.col(empty).ifelse(reason_codes.NO_OBSERVATIONS, no_reason)
                if measure.aggregation == "avg_event"
                else no_reason
            ),
        ),
    )
    if native_arithmetic(measure):
        quantity = _native_window_sum(
            builder, start, end, empty=empty, reason=reason, context=context
        )
    else:
        quantity = _exact_window_sum(
            builder, measure, start, end, empty=empty, zero=zero, reason=reason, context=context
        )
    if measure.aggregation in {"avg_event", "avg_calendar_day"}:
        divisor = (
            nonnull_count
            if measure.aggregation == "avg_event"
            else builder.put("days", exact_literal(days, builder.col(event_count)))
        )
        divisor_zero = builder.put("divisor_zero", builder.col(divisor) == zero)
        denominator = _Quantity(divisor, reason, divisor_zero, count=True)
        quantity = _quotient(builder, quantity, denominator, reason, context)
    return quantity


def _exact_window_sum(
    builder: _Builder,
    measure: BoundMeasure,
    start: Mapping[str, str],
    end: Mapping[str, str],
    *,
    empty: str,
    zero: ir.Scalar,
    reason: str,
    context,
) -> _Quantity:
    """The exact difference of two coefficient prefixes.

    A float fact's window also refuses when its state could not hold an input at
    or before the window end, and notes ``below_resolution`` when the window
    holds a nonzero contribution the scale-9 quantization discarded: the
    published sum is then the sum of the retained contributions.
    """
    sums = []
    for endpoint in (start, end):
        # A matched all-null prefix has the mathematical sum zero, not a missing state.
        count = builder.col(endpoint["nonnull_count"])
        state = builder.col(endpoint["sum_value"])
        sums.append(
            builder.put(
                "endpoint_sum",
                (count == exact_literal(0, count)).ifelse(exact_literal(0, state), state),
            )
        )
    assert measure.value_type is not None  # callers pass resolved measures
    dtype = dt.dtype(measure.value_type)
    difference = exact_subtract(builder.col(sums[1]), builder.col(sums[0]))
    state_zero = exact_literal(0, difference)
    if "sum_withheld" in end:
        # Portable fixed-precision state: a state withheld at or before the
        # window end means the prefix is incomplete, so a non-empty window
        # refuses through whichever consumer reads the difference, instead of
        # trusting it or publishing NULL.
        state_type = cast(dt.Decimal, difference.type())
        withheld = builder.put(
            "state_withheld",
            ~builder.col(empty) & (builder.col(end["sum_withheld"]) > zero),
        )
        difference = builder.col(withheld).ifelse(ibis.null().cast(state_type), difference)
        builder.numeric.append((withheld, _EXACT_OVERFLOW, context))
    if dtype.is_floating():
        # Empty windows remain zero, even after invalid input history.
        value = builder.put("sum", builder.col(empty).ifelse(state_zero, difference))
        invalid = cast(ir.BooleanValue, ibis.literal(False))
        # Inputs the exact state could not hold, at or before the window end,
        # refuse a non-empty window.
        for name, failure in (
            ("float64_nonfinite_count", "nonfinite_input"),
            ("float64_unrepresentable_count", "out_of_range"),
        ):
            held = builder.col(end[name])
            flag = ~builder.col(empty) & (
                held.fill_null(exact_literal(0, held)) > exact_literal(0, held)
            )
            builder.flag(flag, failure, context)
            invalid = invalid | flag
        overflow = builder.put(
            "exact_overflow",
            builder.col(value).isnull()
            & builder.col(sums[0]).notnull()
            & builder.col(sums[1]).notnull(),
        )
        builder.numeric.append((overflow, _EXACT_OVERFLOW, context))
        # A sum that only looks zero because inputs were dropped is not a zero
        # denominator: its refusal must not be masked by a zero_denominator reason.
        known_zero = builder.put("sum_zero", (builder.col(value) == state_zero) & ~invalid)
        # The discarded-term count is a prefix difference, so only the window's
        # own discards are noted; an all-zero or all-null window has none.
        discarded = exact_subtract(
            builder.col(end["float64_below_resolution_count"]),
            builder.col(start["float64_below_resolution_count"]),
        )
        note = builder.put(
            "note",
            (~builder.col(empty) & (discarded > exact_literal(0, discarded))).ifelse(
                reason_codes.BELOW_RESOLUTION, ibis.null().cast("string")
            ),
        )
        quantity = _Quantity(value, reason, known_zero, scale=9, binary64=True, note=note)
    else:
        value = builder.put("sum", difference)
        known_zero = builder.put("sum_zero", builder.col(value) == state_zero)
        scale = 0
        if isinstance(dtype, dt.Decimal):
            assert dtype.scale is not None
            scale = dtype.scale
        quantity = _Quantity(value, reason, known_zero, scale=scale)
    return quantity


def aggregate_window(
    anchors: ibis.Table,
    metric: BoundMetric,
    measure_states: Mapping[BoundMeasure, ibis.Table],
    window: Window,
    readiness: Readiness,
    *,
    boundary: str,
    output: str,
    role: str,
    unit_column: str = "unit",
    resolution: Literal["day", "event"] = "event",
    strategy: Literal["native", "union_window"] = "native",
) -> TemporalResult:
    """Build endpoint differences and defer every data-dependent refusal.

    At s=t0+start and e=t0+end, P(e)-P(s) measures [s,e). Exact floats
    difference exact scale-9 coefficients and convert to float64; native floats
    difference binary64 running sums. Averages and ratios convert each exact
    operand once, then divide once in float64. The reason column names why a
    value is NULL, or carries ``below_resolution`` beside an exact float value
    whose window discarded a nonzero contribution below the quantization.
    """
    if not isinstance(window, Window):
        raise TypeError("window must be a Window")
    if resolution not in {"day", "event"}:
        raise ValueError("resolution must be day or event")
    if strategy not in {"native", "union_window"}:
        raise ValueError("strategy must be native or union_window")
    if not isinstance(output, str) or not output or not isinstance(role, str) or not role:
        raise ValueError("output and role must be nonempty names")
    if output.casefold() in {name.casefold() for name in anchors.columns}:
        raise ValueError("output must not replace an existing anchor field")
    if metric.kind not in {"mean", "conversion", "ratio"}:
        raise UNSUPPORTED_TEMPORAL_KIND.error(kind=metric.kind)
    if metric.transforms:
        raise UNSUPPORTED_TRANSFORM.error(transform=type(metric.transforms[0]).__name__)
    expected = 2 if metric.kind == "ratio" else 1
    if len(metric.measures) != expected:
        raise ValueError("metric kind has an invalid number of measures")
    for measure in metric.measures:
        if not supports_temporal_aggregation(measure.aggregation):
            raise UNSUPPORTED_TEMPORAL_AGGREGATION.error(aggregation=measure.aggregation)
        if measure.value_type is None:
            raise ValueError("measure_states requires resolved measure descriptors")
        if measure not in measure_states:
            raise ValueError("a resolved metric measure has no supplied prefix state")
    for name in (unit_column, "anchor_id", "t0"):
        if name not in anchors.columns:
            raise _SCHEMA.error(field="anchor", column=name)
    if not anchors.t0.type().is_timestamp() or anchors.t0.type().timezone != "UTC":
        raise _SCHEMA.error(field="UTC-aware anchor", column="t0")
    for name in ("_observed_from", "_observed_until"):
        if name in anchors.columns:
            dtype = anchors[name].type()
            if not dtype.is_timestamp() or dtype.timezone != "UTC":
                raise _SCHEMA.error(field="UTC-aware observation", column=name)
    builder = _Builder(anchors, output)
    context = _window_context(builder, metric, window, readiness, boundary, output, resolution)
    start, end = context.start, context.end
    available, unavailable = context.available, context.unavailable
    observed_from, observed_until = context.observed_from, context.observed_until
    row_checks = list(context.row_checks)

    # Windows over the same anchors, bounds and coverage read one relation of
    # request points; each lookup chain starts from its own reference to it.
    builder.table = builder.table.view()
    quantities = []
    unknown_flags = []
    conversion = metric.kind == "conversion"
    for measure in metric.measures:
        context = {"source": measure.source_name, "measure": measure.event, "output": output}
        state = measure_states[measure]
        fields = _state_fields(
            measure,
            state,
            counting=conversion or measure.aggregation == "count",
        )
        first = lookup_prefix(
            builder.table,
            state,
            unit_column=unit_column,
            timestamp_column=start,
            strategy=strategy,
            fields=fields,
        )
        second = lookup_prefix(
            first.table,
            state,
            unit_column=unit_column,
            timestamp_column=end,
            strategy=strategy,
            fields=fields,
        )
        builder.table = second.table
        builder.used.update(name.casefold() for name in builder.table.columns)
        if measure.filters:
            for field_name in _UNKNOWN_FIELDS:
                difference = builder.put(
                    field_name,
                    exact_subtract(
                        builder.col(second.columns[field_name]),
                        builder.col(first.columns[field_name]),
                    ),
                )
                if field_name == "unknown_filter_count":
                    flag = builder.put(
                        "unknown_filter",
                        builder.col(difference) > exact_literal(0, builder.col(difference)),
                    )
                    unknown_flags.append((flag, UNKNOWN_FILTER, context))
        quantities.append(
            _component(
                builder,
                measure,
                first.columns,
                second.columns,
                days=(window.end.seconds - window.start.seconds) // 86400,
                conversion=conversion,
                context=context,
            )
        )
    return _finalize_window(
        builder,
        anchors,
        metric,
        window,
        output,
        role,
        quantities,
        available,
        unavailable,
        observed_from,
        observed_until,
        row_checks,
        unknown_flags,
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
class _WindowContext:
    start: str
    end: str
    available: str
    unavailable: str
    observed_from: str
    observed_until: str
    row_checks: tuple[RowCheck, ...]


def _window_context(
    builder: _Builder,
    metric: BoundMetric,
    window: Window,
    readiness: Readiness,
    boundary: str,
    output: str,
    resolution: Literal["event", "day"] = "event",
) -> _WindowContext:
    anchors = builder.table
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
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

    def shifted(seconds: int):
        t0 = builder.table.t0
        return t0 + ibis.interval(seconds=seconds) if seconds else t0

    start = builder.put("start", shifted(window.start.seconds))
    end = builder.put("end", shifted(window.end.seconds))
    row_checks: list[RowCheck] = []
    if calendars or resolution == "day":
        invalid = builder.put(
            "unaligned",
            builder.table.t0.notnull()
            & ~(_aligned(builder.col(start), minutes) & _aligned(builder.col(end), minutes)),
        )
        refusal = UNALIGNED_CALENDAR_AVERAGE if calendars else _DAY_ALIGNMENT
        context = {**domain_context, "measure": calendars[0].event} if calendars else domain_context
        row_checks.extend(builder.check([(invalid, refusal, context)]))
    coverages = [readiness.for_fact(measure.source_name) for measure in metric.measures]
    source_start = max(coverage.start for coverage in coverages)
    source_end = min(coverage.end_exclusive for coverage in coverages)
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
        observed_from = greatest(observed_from, bound.fill_null(observed_from))
        bounds.append(bound.isnull() | (builder.col(start) >= bound))
    if "_observed_until" in anchors.columns:
        bound = builder.table._observed_until
        observed_until = least(observed_until, bound.fill_null(observed_until))
        bounds.append(bound.isnull() | (builder.col(end) <= bound))
    observed_from = builder.put("observed_from", observed_from)
    observed_until = builder.put("observed_until", observed_until)
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
        start, end, available, unavailable, observed_from, observed_until, tuple(row_checks)
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


def _interval(timestamp, t0, window: Window):
    return compare_timestamps(timestamp, t0, "ge", right_offset_seconds=window.start.seconds) & (
        compare_timestamps(timestamp, t0, "lt", right_offset_seconds=window.end.seconds)
    )


def _reduce_source(
    builder: _Builder,
    events: ibis.Table,
    source: BoundSource,
    primitives: Mapping[_Primitive, Sequence[_WindowContext]],
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
    matched = ibis.literal(False)
    for primitive, contexts in primitives.items():
        active = ibis.literal(False)
        for context in contexts:
            active |= points[context.available]
        matched |= active & _interval(events._timestamp, points[t0], primitive.window)
    joined = events.inner_join(points, (events._unit == points[unit]) & matched)
    aliases: dict[_Primitive, dict[str, str]] = {}
    payload = {}
    for primitive, contexts in primitives.items():
        active = ibis.literal(False)
        for context in contexts:
            active |= joined[context.available]
        consumed = active & _interval(joined._timestamp, joined[t0], primitive.window)
        joined, fields = contributions(
            joined,
            primitive.measure,
            source=source,
            consumed=consumed,
            counting=primitive.counting,
        )
        names = {name: builder.name(name) for name in fields}
        aliases[primitive] = names
        payload.update({names[name]: expression for name, expression in fields.items()})
    leaf = joined.select(unit, anchor_id, **payload)
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


def _day_consumed(bucket, points, contexts):
    """An available request contains the entire fixed-offset bucket."""
    consumed = ibis.literal(False)
    end = bucket + ibis.interval(seconds=86400)
    for context in contexts:
        consumed |= (
            points[context.available]
            & (bucket >= points[context.start])
            & (end <= points[context.end])
        )
    return consumed


def _reduce_daily_source(
    builder: _Builder,
    events: ibis.Table,
    source: BoundSource,
    primitives: Mapping[_Primitive, Sequence[_WindowContext]],
    boundary: str,
) -> tuple[ibis.Table, dict[_Primitive, dict[str, str]], tuple[str, str]]:
    """Scope unique unit/day identities before evaluating any numerical payload."""
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
    bucket, scope_unit, scope_bucket, unit, anchor_id = (
        builder.name(name) for name in ("bucket", "scope_unit", "scope_bucket", "unit", "anchor_id")
    )
    events = events.mutate(**{bucket: day_start(events._timestamp, minutes)})
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
    buckets: dict[_BucketPrimitive, str] = {}
    membership = {}
    matched = ibis.literal(False)
    for primitive, contexts in primitives.items():
        key = _BucketPrimitive(primitive.measure, primitive.counting)
        if key not in buckets:
            buckets[key] = builder.name("consumed")
        consumed = _day_consumed(identities[scope_bucket], points, contexts)
        membership[key] = membership.get(key, ibis.literal(False)) | consumed
        matched |= consumed
    required = identities.inner_join(points, (identities[scope_unit] == points[unit]) & matched)
    required = required.select(
        scope_unit,
        scope_bucket,
        **{buckets[key]: expression for key, expression in membership.items()},
    )
    scope = required.group_by(scope_unit, scope_bucket).aggregate(
        **{name: required[name].any() for name in buckets.values()}
    )
    selected = events.inner_join(
        scope,
        (events._unit == scope[scope_unit]) & (events[bucket] == scope[scope_bucket]),
    )
    bucket_aliases = {}
    payload = {}
    for key, mask in buckets.items():
        selected, fields = contributions(
            selected, key.measure, source=source, consumed=selected[mask], counting=key.counting
        )
        names = {name: builder.name(name) for name in fields}
        bucket_aliases[key] = names
        payload.update({names[name]: expression for name, expression in fields.items()})
    leaf = selected.select(scope_unit, scope_bucket, **payload)
    daily = _reduce_payload(leaf, bucket_aliases, [scope_unit, scope_bucket])
    matched = ibis.literal(False)
    for contexts in primitives.values():
        matched |= _day_consumed(daily[scope_bucket], points, contexts)
    joined = daily.inner_join(points, (daily[scope_unit] == points[unit]) & matched)
    aliases = {}
    payload = {}
    for primitive, contexts in primitives.items():
        key = _BucketPrimitive(primitive.measure, primitive.counting)
        consumed = _day_consumed(joined[scope_bucket], joined, contexts)
        names = {name: builder.name(name) for name in bucket_aliases[key]}
        aliases[primitive] = names
        for name, original in bucket_aliases[key].items():
            value = joined[original]
            payload[names[name]] = consumed.ifelse(value, ibis.null(value.type()))
    leaf = joined.select(unit, anchor_id, **payload)
    reduced = _reduce_payload(leaf, aliases, [unit, anchor_id])
    return reduced, aliases, (unit, anchor_id)


def aggregate_windows(
    anchors: ibis.Table,
    requests: Sequence[AggregateRequest],
    sources: Mapping[str, tuple[ibis.Table, BoundSource]],
    readiness: Readiness,
    *,
    boundary: str,
    resolution: Literal["event", "day"] = "event",
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
    builder = _Builder(anchors, "", practical=True)
    builder.used.update(
        name.casefold() for events, _ in sources.values() for name in events.columns
    )
    contexts = []
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
        context = _window_context(builder, metric, window, readiness, boundary, output, resolution)
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
    reductions = [
        (
            _reduce_daily_source(builder, *sources[name], source_primitives, boundary)
            if resolution == "day"
            else _reduce_source(builder, *sources[name], source_primitives)
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
    for request, context, keys in zip(requests, contexts, components, strict=True):
        retained = builder.table
        quantities, unknown_flags = [], []
        builder.numeric = []
        for measure, key in zip(request.metric.measures, keys, strict=True):
            fields = reduced_fields[key]
            numeric_context = {
                "source": measure.source_name,
                "measure": measure.event,
                "output": request.output,
            }
            if measure.filters:
                count = builder.col(fields["unknown_filter_count"])
                flag = builder.put("unknown_filter", count > exact_literal(0, count))
                unknown_flags.append((flag, UNKNOWN_FILTER, numeric_context))
            quantities.append(
                _local_component(
                    builder,
                    measure,
                    fields,
                    days=(request.window.end.seconds - request.window.start.seconds) // 86400,
                    conversion=request.metric.kind == "conversion",
                    context=numeric_context,
                )
            )
        result = _finalize_window(
            builder,
            retained,
            request.metric,
            request.window,
            request.output,
            request.role,
            quantities,
            context.available,
            context.unavailable,
            context.observed_from,
            context.observed_until,
            list(context.row_checks),
            unknown_flags,
        )
        builder.table = result.table
        columns.update(result.columns)
        row_checks.extend(result.row_checks)
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
