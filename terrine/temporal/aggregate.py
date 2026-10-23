"""Strict half-open windows with deferred observation and arithmetic checks."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pyarrow as pa

from terrine.backends.dialect import dialect_of
from terrine.backends.extrema import greatest, least
from terrine.backends.numeric import (
    EXACT_OVERFLOW as _EXACT_OVERFLOW,
)
from terrine.backends.numeric import (
    exact_is_nonfinite,
    exact_literal,
    exact_ratio,
    exact_subtract,
    exact_sum,
    exact_to_float,
    exact_type,
    float_is_finite,
    ieee_divide,
    ieee_subtract,
)
from terrine.backends.timestamps import utc_instant, utc_wall_clock
from terrine.data import ColumnMeta
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA
from terrine.semantic.models import BoundMeasure, BoundMetric
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
from terrine.temporal.daily import _UNKNOWN_FIELDS
from terrine.temporal.lookup import lookup_prefix
from terrine.temporal.prefix import (
    FLOAT64_PRECISION,
    FLOAT64_UNSUPPORTED,
    _outward,
    _require_backend,
    float64_rounding_radius,
    float64_tolerance,
    outward_add,
    outward_divide,
    outward_subtract,
)
from terrine.temporal.readiness import Readiness
from terrine.windows import Window

__all__ = ["TemporalResult", "aggregate_window"]

_DAY_ALIGNMENT = RefusalSpec(
    "terrine.temporal.unaligned_day_resolution",
    "Day prefixes require both endpoints at local midnight; use event prefixes "
    "for arbitrary instants.",
)


@dataclass(frozen=True, slots=True)
class _Failure:
    column: str
    refusal: RefusalSpec
    context: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", MappingProxyType(dict(self.context)))


@dataclass(frozen=True, slots=True)
class _Check:
    table: ibis.Table
    failures: tuple[_Failure, ...]

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        result = execute(self.table)
        expected = [failure.column for failure in self.failures]
        if not isinstance(result, pa.Table):
            raise TypeError("The supplied temporal executor must return a PyArrow table.")
        if result.num_rows != 1 or result.column_names != expected:
            raise ValueError("Temporal validation requires one row of named failed counts.")
        result.validate(full=True)
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
    """A lazy result whose checks must precede export under the caller's snapshot."""

    table: ibis.Table
    columns: Mapping[str, ColumnMeta]
    _checks: tuple[_Check, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", MappingProxyType(dict(self.columns)))
        object.__setattr__(self, "_checks", tuple(self._checks))

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Run bounded scalar checks using the same pinned executor as final export."""
        for check in self._checks:
            check.validate(execute)


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
        self.counter = 0
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

    def name(self, label: str) -> str:
        while True:
            name = f"__terrine_window_{self.counter}_{label}"
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

    def flag(self, expression, failure: str, context: Mapping[str, Any]) -> None:
        name = self.put(failure, expression)
        self.numeric.append((name, FLOAT64_PRECISION, {**context, "failure": failure}))

    def check(self, flags, *, active=None) -> _Check:
        values, failures = [], []
        for index, (name, refusal, context) in enumerate(flags):
            predicate = self.col(name).fill_null(True)
            if active is not None:
                predicate = active & predicate
            column = f"failed_{index}"
            count = exact_sum(predicate.ifelse(exact_literal(1), exact_literal(0)))
            values.append(count.fill_null(exact_literal(0)).cast("string").name(column))
            failures.append(_Failure(column, refusal, context))
        return _Check(self.table.aggregate(values), tuple(failures))


@dataclass(frozen=True, slots=True)
class _Quantity:
    value: str
    reason: str
    known_zero: str
    error: str | None = None
    scale: int = 0
    count: bool = False
    # An exact NUMERIC decimal rounded once to binary64 at output (non-certifying
    # backends); ``scale`` is then 0 because the value is not a coefficient.
    rounded: bool = False


def _scaled_text(coefficient, scale: int):
    text = coefficient.cast("string")
    if scale == 0:
        return text
    sign = text.startswith("-").ifelse("-", "")
    digits = text.replace("-", "")
    digits = digits.lpad(greatest(digits.length(), scale + 1), "0")
    return (
        sign
        + digits.substr(0, digits.length() - scale)
        + "."
        + digits.substr(digits.length() - scale)
    )


def narrow_exact(coefficient, scale: int, target: str | dt.DataType):
    """Convert an exact coefficient to ``target``; NULL when it does not fit.

    PostgreSQL lowers TRY_CAST to an ordinary CAST that aborts the transaction on
    overflow, so the range is checked on the exact coefficient before casting.
    """
    text = _scaled_text(coefficient, scale)
    if not dialect_of() == "postgres":
        return text.try_cast(target)
    target_type = dt.dtype(target)
    if isinstance(target_type, dt.SignedInteger):
        bits = target_type.nbytes * 8
        low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
    elif isinstance(target_type, dt.Decimal) and target_type.precision is not None:
        low, high = -(10**target_type.precision) + 1, 10**target_type.precision - 1
    else:
        # An unbounded NUMERIC target cannot overflow.
        return text.cast(target_type)
    fits = (coefficient >= exact_literal(low)) & (coefficient <= exact_literal(high))
    return fits.ifelse(text.cast(target_type), ibis.null().cast(target_type))


def _aligned(timestamp, minutes: int):
    utc = utc_wall_clock(timestamp)
    local = utc + ibis.interval(minutes=minutes)
    midnight = cast(ir.TimestampValue, local.date().cast("timestamp")) - ibis.interval(
        minutes=minutes
    )
    return timestamp == utc_instant(midnight)


def _float_quantity(builder: _Builder, quantity: _Quantity, context) -> _Quantity:
    if quantity.error is not None:
        return quantity
    _require_backend(builder.table, floating=True)
    value = builder.put(
        "parsed", _scaled_text(builder.col(quantity.value), quantity.scale).try_cast("float64")
    )
    builder.flag(
        builder.col(value).isnull() | ~float_is_finite(builder.col(value)),
        "uncertifiable_cast",
        context,
    )
    lossless = builder.col(quantity.known_zero)
    if quantity.scale == 0:
        coefficient = builder.col(quantity.value)
        lossless = (coefficient >= exact_literal(-(2**53))) & (coefficient <= exact_literal(2**53))
    error = builder.put(
        "cast_error", lossless.ifelse(0.0, float64_rounding_radius(builder.col(value)))
    )
    return _Quantity(value, quantity.reason, quantity.known_zero, error)


def _check_quantity(builder: _Builder, quantity: _Quantity, context) -> None:
    assert quantity.error is not None
    value, error = builder.col(quantity.value), builder.col(quantity.error)
    failures = (
        ("nonfinite_state", ~float_is_finite(value) | value.isnull()),
        ("bound_overflow", ~float_is_finite(error) | error.isnull() | (error < 0.0)),
    )
    columns = builder.put_many(*failures)
    for column, (failure, _) in zip(columns, failures, strict=True):
        builder.numeric.append((column, FLOAT64_PRECISION, {**context, "failure": failure}))


def _certifying(builder: _Builder) -> bool:
    """DuckDB certifies float results; other backends round exact NUMERIC once."""
    return dialect_of(builder.table.t0) == "duckdb"


def _exact_decimal(builder: _Builder, quantity: _Quantity) -> ir.Value:
    value = builder.col(quantity.value)
    if quantity.rounded or quantity.scale == 0:
        return value
    return _scaled_text(value, quantity.scale).cast(exact_type(value))


def _rounded_quotient(
    builder: _Builder, numerator: _Quantity, denominator: _Quantity, reason: str, context
) -> _Quantity:
    """Divide exactly in NUMERIC; ``aggregate_window`` rounds the result once.

    A non-finite component refuses here: division could erase it (1/Infinity = 0).
    """
    for component in (numerator, denominator):
        if component.rounded:
            nonfinite = exact_is_nonfinite(builder.col(component.value)).fill_null(
                ibis.literal(False)
            )
            builder.flag(nonfinite, "nonfinite_input", context)
    exact_numerator = _exact_decimal(builder, numerator)
    exact_denominator = _exact_decimal(builder, denominator)
    value = builder.put("quotient", exact_ratio(exact_numerator, exact_denominator))
    # BigQuery's SAFE_DIVIDE returns NULL on overflow; only a zero denominator may.
    builder.flag(
        builder.col(value).isnull()
        & exact_numerator.notnull()
        & exact_denominator.notnull()
        & ~builder.col(denominator.known_zero),
        "bound_overflow",
        context,
    )
    known_zero = builder.put(
        "quotient_zero",
        builder.col(numerator.known_zero) & ~builder.col(denominator.known_zero),
    )
    return _Quantity(value, reason, known_zero, rounded=True)


def _quotient(
    builder: _Builder, numerator: _Quantity, denominator: _Quantity, reason: str, context
) -> _Quantity:
    """Enclose all four endpoint quotients, then measure error from the returned RN value."""
    if not _certifying(builder):
        return _rounded_quotient(builder, numerator, denominator, reason, context)
    numerator = _float_quantity(builder, numerator, context)
    denominator = _float_quantity(builder, denominator, context)
    assert numerator.error is not None and denominator.error is not None
    ln, un, ld, ud = builder.put_many(
        (
            "lower",
            outward_subtract(
                builder.col(numerator.value), builder.col(numerator.error), upper=False
            ),
        ),
        ("upper", outward_add(builder.col(numerator.value), builder.col(numerator.error))),
        (
            "lower",
            outward_subtract(
                builder.col(denominator.value), builder.col(denominator.error), upper=False
            ),
        ),
        ("upper", outward_add(builder.col(denominator.value), builder.col(denominator.error))),
    )
    invalid_endpoint = ~float_is_finite(builder.col(ln))
    for endpoint in (un, ld, ud):
        invalid_endpoint = invalid_endpoint | ~float_is_finite(builder.col(endpoint))
    builder.flag(invalid_endpoint, "bound_overflow", context)
    excludes_zero = builder.put("excludes_zero", (builder.col(ld) > 0.0) | (builder.col(ud) < 0.0))
    builder.flag(~builder.col(excludes_zero), "denominator_interval_zero", context)
    endpoints = builder.put_many(
        *(
            (label, outward_divide(builder.col(n), builder.col(d), upper=upper))
            for n in (ln, un)
            for d in (ld, ud)
            for label, upper in (("quotient_lower", False), ("quotient_upper", True))
        )
    )
    lower, upper, value = builder.put_many(
        ("quotient_min", least(*(builder.col(n) for n in endpoints[::2]))),
        ("quotient_max", greatest(*(builder.col(n) for n in endpoints[1::2]))),
        ("quotient", ieee_divide(builder.col(numerator.value), builder.col(denominator.value))),
    )
    left_error, right_error = builder.put_many(
        ("left_error", outward_subtract(builder.col(value), builder.col(lower))),
        ("right_error", outward_subtract(builder.col(upper), builder.col(value))),
    )
    error, known_zero = builder.put_many(
        (
            "quotient_error",
            _outward(greatest(builder.col(left_error).abs(), builder.col(right_error).abs()), True),
        ),
        ("quotient_zero", builder.col(numerator.known_zero) & builder.col(excludes_zero)),
    )
    # Zero divided by a finite nonzero value is exact; no arithmetic interval is needed.
    error = builder.put("quotient_error", builder.col(known_zero).ifelse(0.0, builder.col(error)))
    builder.flag(
        ~float_is_finite(builder.col(lower)) | ~float_is_finite(builder.col(upper)),
        "bound_overflow",
        context,
    )
    result = _Quantity(value, reason, known_zero, error)
    _check_quantity(builder, result, context)
    return result


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
    zero = exact_literal(0)
    event_count = builder.put(
        "events", exact_subtract(builder.col(end["event_count"]), builder.col(start["event_count"]))
    )
    nonnull_count = builder.put(
        "nonnull",
        exact_subtract(builder.col(end["nonnull_count"]), builder.col(start["nonnull_count"])),
    )
    empty = builder.put("empty", builder.col(event_count) == zero)
    all_null = builder.put(
        "all_null", (builder.col(event_count) > zero) & (builder.col(nonnull_count) == zero)
    )
    no_reason = ibis.null().cast("string")
    reason = builder.put(
        "reason",
        no_reason
        if conversion or measure.aggregation == "count"
        else builder.col(all_null).ifelse(
            "all_values_null",
            (
                builder.col(empty).ifelse("no_observations", no_reason)
                if measure.aggregation == "avg_event"
                else no_reason
            ),
        ),
    )
    if conversion:
        value = builder.put("converted", builder.col(event_count) > zero)
        return _Quantity(value, reason, empty)
    if measure.aggregation == "count":
        return _Quantity(event_count, reason, empty, count=True)
    floating = builder.col(end["sum_value"]).type().is_floating()
    sums = []
    for endpoint in (start, end):
        # A matched all-null prefix has the mathematical sum zero, not a missing state.
        count = builder.col(endpoint["nonnull_count"])
        sums.append(
            builder.put(
                "endpoint_sum",
                (count == zero).ifelse(
                    0.0 if floating else zero, builder.col(endpoint["sum_value"])
                ),
            )
        )
    if floating:
        history_flags = []
        for name, failure in (
            ("float64_conformance_failure", None),
            ("float64_nonfinite_input", "nonfinite_input"),
            ("float64_nonfinite_state", "nonfinite_state"),
            ("float64_count_limit", "count_limit"),
            ("float64_bound_overflow", "bound_overflow"),
        ):
            flag = builder.put(
                "endpoint_flag",
                ~builder.col(empty)
                & (
                    builder.col(start[name]).fill_null(True)
                    | builder.col(end[name]).fill_null(True)
                ),
            )
            history_flags.append(flag)
            if failure is None:
                builder.numeric.append(
                    (
                        flag,
                        FLOAT64_UNSUPPORTED,
                        {
                            "backend": "duckdb",
                            "capability": "execution-thread RN binary64 and gradual underflow",
                            **context,
                        },
                    )
                )
            else:
                builder.numeric.append((flag, FLOAT64_PRECISION, {"failure": failure, **context}))
        value = builder.put(
            "sum",
            builder.col(empty).ifelse(
                0.0, ieee_subtract(builder.col(sums[1]), builder.col(sums[0]))
            ),
        )
        errors = []
        for endpoint in (start, end):
            errors.append(
                builder.put(
                    "endpoint_error",
                    (builder.col(endpoint["nonnull_count"]) == zero).ifelse(
                        0.0, builder.col(endpoint["prefix_error"])
                    ),
                )
            )
        valid_history = ibis.literal(True)
        for flag in history_flags:
            valid_history = valid_history & ~builder.col(flag)
        known_zero = builder.put(
            "sum_zero",
            builder.col(empty)
            | (
                (builder.col(sums[0]) == builder.col(sums[1]))
                & float_is_finite(builder.col(sums[0]))
                & float_is_finite(builder.col(sums[1]))
                & (builder.col(errors[0]) == 0.0)
                & (builder.col(errors[1]) == 0.0)
                & valid_history
            ),
        )
        endpoint_error = builder.put(
            "endpoint_errors", outward_add(builder.col(errors[0]), builder.col(errors[1]))
        )
        radius = builder.put("subtraction_radius", float64_rounding_radius(builder.col(value)))
        error = builder.put(
            "sum_error",
            builder.col(known_zero).ifelse(
                0.0, outward_add(builder.col(endpoint_error), builder.col(radius))
            ),
        )
        quantity = _Quantity(value, reason, known_zero, error)
        _check_quantity(builder, quantity, context)
    else:
        assert measure.value_type is not None  # callers pass resolved measures
        dtype = dt.dtype(measure.value_type)
        difference = exact_subtract(builder.col(sums[1]), builder.col(sums[0]))
        if dtype.is_floating():
            # Exact float state (non-certifying backends): an empty window is zero
            # even when non-finite history makes the prefix difference NaN.
            value = builder.put("sum", builder.col(empty).ifelse(zero, difference))
            invalid = cast(ir.BooleanValue, ibis.literal(False))
            # Inputs the exact state could not hold, at or before the window end,
            # refuse a non-empty window, as non-finite history does on DuckDB.
            for name, failure in (
                ("float64_nonfinite_count", "nonfinite_input"),
                ("float64_unrepresentable_count", "bound_overflow"),
            ):
                if name in end:
                    flag = ~builder.col(empty) & (builder.col(end[name]).fill_null(zero) > zero)
                    builder.flag(flag, failure, context)
                    invalid = invalid | flag
            # BigQuery's SAFE_SUBTRACT returns NULL on overflow.
            builder.flag(
                builder.col(value).isnull()
                & builder.col(sums[0]).notnull()
                & builder.col(sums[1]).notnull(),
                "bound_overflow",
                context,
            )
            # A sum that only looks zero because inputs were dropped is not a zero
            # denominator: its refusal must not be masked by a zero_denominator reason.
            known_zero = builder.put("sum_zero", (builder.col(value) == zero) & ~invalid)
            quantity = _Quantity(value, reason, known_zero, rounded=True)
        else:
            value = builder.put("sum", difference)
            known_zero = builder.put("sum_zero", builder.col(value) == zero)
            scale = 0
            if isinstance(dtype, dt.Decimal):
                assert dtype.scale is not None
                scale = dtype.scale
            quantity = _Quantity(value, reason, known_zero, scale=scale)
    if measure.aggregation in {"avg_event", "avg_calendar_day"}:
        divisor = (
            nonnull_count
            if measure.aggregation == "avg_event"
            else builder.put("days", exact_literal(days))
        )
        divisor_zero = builder.put("divisor_zero", builder.col(divisor) == zero)
        denominator = _Quantity(divisor, reason, divisor_zero, count=True)
        quantity = _quotient(builder, quantity, denominator, reason, context)
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
    """Build exact endpoint differences and defer every data-dependent refusal.

    At s=t0+start and e=t0+end, P(e)-P(s) measures exactly [s,e).
    Float sums use E=RU(E_s+E_e+R(fl(S_e-S_s))). Fractional results
    enclose both inputs and all four endpoint quotients before reapplying the
    fixed final-result tolerance. Exact coefficients never multiply or divide.
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
    offset = boundary_timezone(boundary).utcoffset(None)
    minutes = (offset.days * 86400 + offset.seconds) // 60
    calendars = [m for m in metric.measures if m.aggregation == "avg_calendar_day"]
    domain_context = {
        "metric": metric.name,
        "output": output,
        "window_start": window.start,
        "window_end": window.end,
        "boundary": boundary,
        "phase": "pre_output",
    }
    for measure in calendars:
        if calendar_average_alignment(window, anchor=None, day_boundary=boundary) == "unaligned":
            raise UNALIGNED_CALENDAR_AVERAGE.error(measure=measure.fact, **domain_context)
    builder = _Builder(anchors, output)
    start = builder.put("start", anchors.t0 + ibis.interval(seconds=window.start))
    end = builder.put("end", builder.table.t0 + ibis.interval(seconds=window.end))
    checks: list[_Check] = []
    if calendars or resolution == "day":
        invalid = builder.put(
            "unaligned",
            builder.table.t0.notnull()
            & ~(_aligned(builder.col(start), minutes) & _aligned(builder.col(end), minutes)),
        )
        refusal = UNALIGNED_CALENDAR_AVERAGE if calendars else _DAY_ALIGNMENT
        context = {**domain_context, "measure": calendars[0].fact} if calendars else domain_context
        checks.append(builder.check([(invalid, refusal, context)]))
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
    observation_complete = cast(ir.BooleanValue, ibis.literal(True))
    if "_observed_from" in anchors.columns:
        bound = builder.table._observed_from
        observed_from = greatest(observed_from, bound.fill_null(observed_from))
        observation_complete = observation_complete & (
            bound.isnull() | (builder.col(start) >= bound)
        )
    if "_observed_until" in anchors.columns:
        bound = builder.table._observed_until
        observed_until = least(observed_until, bound.fill_null(observed_until))
        observation_complete = observation_complete & (bound.isnull() | (builder.col(end) <= bound))
    observed_from = builder.put("observed_from", observed_from)
    observed_until = builder.put("observed_until", observed_until)
    available = builder.put("complete", builder.col(source_complete) & observation_complete)
    unavailable = builder.put(
        "unavailable_reason",
        (~builder.col(source_complete)).ifelse(
            "incomplete_coverage",
            (~observation_complete).ifelse(
                ibis.literal("incomplete_observation"), ibis.null().cast("string")
            ),
        ),
    )
    quantities = []
    unknown_flags = []
    for measure in metric.measures:
        context = {"source": measure.source_name, "measure": measure.fact, "output": output}
        first = lookup_prefix(
            builder.table,
            measure_states[measure],
            unit_column=unit_column,
            timestamp_column=start,
            strategy=strategy,
        )
        second = lookup_prefix(
            first.table,
            measure_states[measure],
            unit_column=unit_column,
            timestamp_column=end,
            strategy=strategy,
        )
        builder.table = second.table
        builder.used.update(name.casefold() for name in builder.table.columns)
        for field_name in _UNKNOWN_FIELDS:
            difference = builder.put(
                field_name,
                exact_subtract(
                    builder.col(second.columns[field_name]), builder.col(first.columns[field_name])
                ),
            )
            if field_name == "unknown_filter_count":
                flag = builder.put("unknown_filter", builder.col(difference) > exact_literal(0))
                unknown_flags.append((flag, UNKNOWN_FILTER, context))
        quantities.append(
            _component(
                builder,
                measure,
                first.columns,
                second.columns,
                days=(window.end - window.start) // 86400,
                conversion=metric.kind == "conversion",
                context=context,
            )
        )
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
                        "zero_denominator", ibis.null().cast("string")
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
                "measure": ",".join(m.fact for m in metric.measures),
                "output": output,
            },
        )
    else:
        quantity = quantities[0]
    final_reason = builder.put(
        "reason",
        builder.col(available).ifelse(builder.col(quantity.reason), builder.col(unavailable)),
    )
    evaluated = builder.put("evaluated", builder.col(final_reason).isnull())
    if unknown_flags:
        checks.append(builder.check(unknown_flags, active=builder.col(available)))
    numeric_context = {
        "source": ",".join(m.source_name for m in metric.measures),
        "measure": ",".join(m.fact for m in metric.measures),
        "output": output,
    }
    if quantity.error is not None:
        builder.flag(
            builder.col(quantity.error) > float64_tolerance(builder.col(quantity.value)),
            "tolerance",
            numeric_context,
        )
    exact_output = metric.kind != "conversion" and quantity.error is None and not quantity.rounded
    if exact_output:
        target = "int64" if quantity.count else dt.Decimal(38, quantity.scale)
        value = builder.put(
            "exact_output",
            narrow_exact(builder.col(quantity.value), quantity.scale, target),
        )
        overflow = builder.put(
            "exact_overflow",
            builder.col(value).isnull()
            | (
                (builder.col(quantity.value) < exact_literal(0))
                if quantity.count
                else ibis.literal(False)
            ),
        )
        checks.append(
            builder.check(
                [
                    (
                        overflow,
                        _EXACT_OVERFLOW,
                        {
                            "source": metric.measures[0].source_name,
                            "measure": metric.measures[0].fact,
                            "output": output,
                        },
                    )
                ],
                active=builder.col(evaluated),
            )
        )
    elif quantity.rounded:
        rounded, nonfinite, out_of_range = exact_to_float(builder.col(quantity.value))
        value = builder.put("rounded", rounded)
        builder.flag(nonfinite, "nonfinite_input", numeric_context)
        builder.flag(out_of_range, "bound_overflow", numeric_context)
    else:
        value = quantity.value
    if builder.numeric:
        checks.append(builder.check(builder.numeric, active=builder.col(evaluated)))
    public_value = builder.col(evaluated).ifelse(
        builder.col(value), ibis.null().cast(builder.col(value).type())
    )
    error_column = None
    if quantity.error is not None:
        error_column = builder.put(
            "absolute_error",
            builder.col(evaluated).ifelse(builder.col(quantity.error), ibis.null().cast("float64")),
        )
    result = builder.table.select(
        *anchors.columns,
        public_value.name(output),
        observed_from,
        observed_until,
        available,
        final_reason,
        *([error_column] if error_column else []),
    )
    meta = ColumnMeta(
        window=window,
        role=role,
        observed_from_column=observed_from,
        observed_until_column=observed_until,
        complete_column=available,
        reason_column=final_reason,
        numeric_policy=(
            None
            if metric.kind == "conversion" or quantity.rounded
            else "checked_float64_v1"
            if quantity.error is not None
            else "exact"
        ),
        absolute_error_column=error_column,
    )
    return TemporalResult(result, {output: meta}, tuple(checks))
