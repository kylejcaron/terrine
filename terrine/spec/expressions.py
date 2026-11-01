"""Immutable, data-only expression and predicate specifications."""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal

from terrine.errors import RefusalSpec, parameter_names
from terrine.windows import Duration, Window, duration

__all__ = [
    "Expr",
    "Predicate",
    "MetricAgg",
    "MetricQuantile",
    "ApproxMetricDistinct",
    "ApproxMetricQuantile",
    "RetentionMetric",
    "retention",
    "AttributeAsOf",
    "EventTime",
    "Gt",
    "Eq",
    "IsNull",
    "And",
    "agg",
    "quantile",
    "approx_quantile",
    "approx_distinct",
    "asof",
    "event_time",
    "since",
    "gt",
    "eq",
    "is_null",
    "all_of",
]

_SKETCH_UNAVAILABLE = RefusalSpec(
    "terrine.metric.sketch_unavailable",
    "Sketch {algorithm!r} version {version!r} is unavailable; install terrine[sketches] "
    "or use the exact form.",
)
_INVALID = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")
_CHOICE = RefusalSpec("terrine.design.invalid", "Design input {field} must be one of {choices}.")
_ELAPSED_SELECTOR = RefusalSpec(
    "terrine.design.invalid",
    "Elapsed event selectors cannot be used here ({reason}). {route}",
)


def _probability(value: Decimal | str, field: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(value, (Decimal, str)):
        raise _INVALID.error(reason="wrong_type", field=field)
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation as exc:
        raise _INVALID.error(reason="not_decimal", field=field) from exc
    if not result.is_finite() or result < 0 or result > 1:
        raise _INVALID.error(reason="out_of_range", field=field)
    if not result:
        return Decimal(0)
    sign, digits, exponent = result.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("probability must be finite")
    canonical = list(digits)
    while canonical[-1] == 0:
        canonical.pop()
        exponent += 1
    return Decimal((sign, tuple(canonical), exponent))


def _window(value: object) -> None:
    if not isinstance(value, Window):
        raise _INVALID.error(reason="wrong_type", field="window")


def _relative_to(value: object) -> None:
    if value is not None and not isinstance(value, EventTime):
        raise _INVALID.error(reason="wrong_type", field="relative_to")
    if isinstance(value, EventTime) and value.measure == "elapsed":
        raise _ELAPSED_SELECTOR.error(
            reason="elapsed_selector_as_origin",
            route="Use event_time() for an origin and since() only as a feature or outcome.",
        )


@dataclass(frozen=True, slots=True)
class MetricQuantile:
    metric: str
    probability: Decimal
    window: Window
    interpolation: Literal["linear_type7"] = "linear_type7"

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability, "probability"))
        _window(self.window)
        if self.interpolation != "linear_type7":
            raise _CHOICE.error(
                reason="unsupported_value", field="interpolation", choices=("linear_type7",)
            )


@dataclass(frozen=True, slots=True)
class ApproxMetricQuantile:
    metric: str
    probability: Decimal
    window: Window

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability, "probability"))
        _window(self.window)


@dataclass(frozen=True, slots=True)
class ApproxMetricDistinct:
    metric: str
    window: Window

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        _window(self.window)


@dataclass(frozen=True, slots=True)
class RetentionMetric:
    """Ordinal bands of distinct occurrence identities in a declared follow-up horizon."""

    event: str
    label_property: str
    target_event: str
    count_thresholds: tuple[int, ...]
    horizon: Duration | None

    def __post_init__(self) -> None:
        for field in ("event", "label_property", "target_event"):
            object.__setattr__(self, field, _name(getattr(self, field), field))
        if not isinstance(self.count_thresholds, Iterable):
            raise _INVALID.error(reason="wrong_type", field="count_thresholds")
        thresholds = tuple(self.count_thresholds)
        if not thresholds:
            raise _INVALID.error(reason="empty", field="count_thresholds")
        if any(type(value) is not int or not 0 < value < 2**63 for value in thresholds):
            raise _INVALID.error(reason="not_positive_integer", field="count_thresholds")
        if tuple(sorted(set(thresholds))) != thresholds:
            raise _INVALID.error(reason="not_increasing", field="count_thresholds")
        object.__setattr__(self, "count_thresholds", thresholds)
        if self.horizon is not None:
            horizon = duration(self.horizon, field="horizon")
            if horizon.seconds <= 0:
                raise _INVALID.error(reason="not_positive", field="horizon")
            object.__setattr__(self, "horizon", horizon)


type JsonScalar = None | bool | int | float | str


def _name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _INVALID.error(reason="empty_name", field=field)
    return value


def _scalar(value: object, field: str) -> JsonScalar:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise _INVALID.error(reason="not_json_scalar", field=field)


@dataclass(frozen=True, slots=True)
class MetricAgg:
    metric: str
    window: Window
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        _window(self.window)
        _relative_to(self.relative_to)


@dataclass(frozen=True, slots=True)
class AttributeAsOf:
    attribute: str
    before: Duration
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attribute", _name(self.attribute, "attribute"))
        object.__setattr__(self, "before", duration(self.before, field="before"))
        _relative_to(self.relative_to)


@dataclass(frozen=True, slots=True)
class EventTime:
    """Select an event time within a window relative to t0 or another event time.

    Args:
        event: Name of an event declared in the model.
        window: Window of elapsed offsets (see `terrine.windows`), including start and excluding
            end.
        label_property: Declared event label name to match; None selects events without a label
            filter.
        equals: Scalar label value paired with label_property; None means no label filter.
        occurrence: Selection rule "first", "last" or "nth"; "first" selects the earliest matching
            time.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        relative_to: EventTime providing the origin; None makes offsets relative to row t0.
        measure: "time" returns an instant; "elapsed" returns time from the event to t0; default
            "time".

    Attributes:
        event: Name of an event declared in the model.
        window: Window of elapsed offsets (see `terrine.windows`), including start and excluding
            end.
        label_property: Declared event label name to match; None selects events without a label
            filter.
        equals: Scalar label value paired with label_property; None means no label filter.
        occurrence: Selection rule "first", "last" or "nth"; "first" selects the earliest matching
            time.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        relative_to: EventTime providing the origin; None makes offsets relative to row t0.
        measure: "time" returns an instant; "elapsed" returns time from the event to t0; default
            "time".

    Example: from terrine.windows import Window; spec.EventTime("orders", Window("-30d", "0s"))
    """

    event: str
    window: Window
    label_property: str | None = None
    equals: JsonScalar = None
    occurrence: Literal["first", "last", "nth"] = "first"
    n: int | None = None
    relative_to: EventTime | None = None
    measure: Literal["time", "elapsed"] = "time"

    def __post_init__(self) -> None:
        if self.measure not in ("time", "elapsed"):
            raise _INVALID.error(reason="unsupported_measure", field="measure")
        object.__setattr__(self, "event", _name(self.event, "event"))
        _window(self.window)
        if (self.label_property is None) != (self.equals is None):
            raise _INVALID.error(reason="unpaired_label_filter", field="label_property")
        if self.label_property is not None:
            object.__setattr__(self, "label_property", _name(self.label_property, "label_property"))
            object.__setattr__(self, "equals", _scalar(self.equals, "equals"))
        if self.occurrence not in ("first", "last", "nth"):
            raise _CHOICE.error(
                reason="unsupported_value", field="occurrence", choices=("first", "last", "nth")
            )
        if self.occurrence in ("first", "last") and self.n is not None:
            raise _INVALID.error(reason="n_requires_nth", field="n")
        if self.occurrence == "nth" and (
            isinstance(self.n, bool) or not isinstance(self.n, int) or self.n < 1
        ):
            raise _INVALID.error(reason="not_positive_integer", field="n")
        _relative_to(self.relative_to)


@dataclass(frozen=True, slots=True)
class Gt:
    expr: Expr
    value: JsonScalar

    def __post_init__(self) -> None:
        _require_expr(self.expr, "expr")
        object.__setattr__(self, "value", _scalar(self.value, "value"))


@dataclass(frozen=True, slots=True)
class Eq:
    expr: Expr
    value: JsonScalar

    def __post_init__(self) -> None:
        _require_expr(self.expr, "expr")
        object.__setattr__(self, "value", _scalar(self.value, "value"))


@dataclass(frozen=True, slots=True)
class IsNull:
    expr: Expr
    negate: bool = False

    def __post_init__(self) -> None:
        _require_expr(self.expr, "expr")
        if not isinstance(self.negate, bool):
            raise _INVALID.error(reason="wrong_type", field="negate")


@dataclass(frozen=True, slots=True)
class And:
    terms: tuple[Predicate, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.terms, Iterable):
            raise _INVALID.error(reason="wrong_type", field="terms")
        terms = tuple(self.terms)
        if not terms:
            raise _INVALID.error(reason="empty", field="terms")
        if any(not isinstance(term, (Gt, Eq, IsNull, And)) for term in terms):
            raise _INVALID.error(reason="wrong_type", field="terms")
        object.__setattr__(self, "terms", terms)


type Expr = (
    MetricAgg
    | MetricQuantile
    | ApproxMetricQuantile
    | ApproxMetricDistinct
    | RetentionMetric
    | AttributeAsOf
    | EventTime
)
type Predicate = Gt | Eq | IsNull | And


def _output_leaves(value: Expr | Predicate) -> Iterator[Expr]:
    """Yield measured expressions in an output or eligibility rule."""
    if isinstance(value, And):
        for term in value.terms:
            yield from _output_leaves(term)
    elif isinstance(value, (Gt, Eq, IsNull)):
        yield value.expr
    else:
        yield value


def _expression_support_seconds(expression: Expr) -> tuple[int, int]:
    if isinstance(expression, EventTime):
        origin = (
            (0, 0)
            if expression.relative_to is None
            else _expression_support_seconds(expression.relative_to)
        )
        window = expression.window
        return (origin[0] + window.start.seconds, origin[1] + window.end.seconds)
    if isinstance(
        expression, (MetricAgg, MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)
    ):
        origin = (
            (0, 0)
            if not isinstance(expression, MetricAgg) or expression.relative_to is None
            else _expression_support_seconds(expression.relative_to)
        )
        window = expression.window
        return (origin[0] + window.start.seconds, origin[1] + window.end.seconds)
    if isinstance(expression, AttributeAsOf):
        origin = (
            (0, 0)
            if expression.relative_to is None
            else _expression_support_seconds(expression.relative_to)
        )
        return (origin[0] + expression.before.seconds, origin[1] + expression.before.seconds)
    raise TypeError("expected a supported expression node")


def _require_expr(value: object, field: str) -> None:
    if not isinstance(
        value,
        (
            MetricAgg,
            MetricQuantile,
            ApproxMetricQuantile,
            ApproxMetricDistinct,
            RetentionMetric,
            AttributeAsOf,
            EventTime,
        ),
    ):
        raise _INVALID.error(reason="wrong_type", field=field)
    if isinstance(value, EventTime) and value.measure == "elapsed":
        raise _ELAPSED_SELECTOR.error(
            reason="elapsed_selector_in_predicate",
            route="Output since() as a feature or outcome, then filter the returned dataframe.",
        )


def _window_pair(
    value: object,
) -> tuple[str | Duration | Literal[0], str | Duration | Literal[0]]:
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise _INVALID.error(reason="not_a_pair", field="window")
    return value[0], value[1]


def agg(
    metric: str,
    *,
    window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]],
    relative_to: EventTime | None = None,
) -> MetricAgg:
    """Aggregate a named metric over a half-open window relative to t0 or an event time.


    Args:
        metric: Name of a metric declared in the model.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        relative_to: EventTime providing the origin; None makes offsets relative to row t0.

    Returns:
        A MetricAgg expression; values and completeness are computed on materialization.

    Example: tr.agg("spend", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return MetricAgg(metric, Window(start, end), relative_to)


def retention(
    event: str,
    *,
    label_property: str,
    target_event: str,
    thresholds: tuple[int, ...],
    horizon: str | Duration | Literal[0, "open_ended"],
) -> RetentionMetric:
    """Return the highest threshold band reached by distinct target events.

    Follow-up ends at the horizon or, for ``"open_ended"``, the last complete event time.

    Args:
        event: Name of an event declared in the model.
        label_property: Declared event label distinguishing the target event type.
        target_event: String label value identifying events counted toward retention thresholds.
        thresholds: Strictly increasing tuple of positive integer occurrence-count thresholds.
        horizon: Positive duration string (see `terrine.windows`) or Duration, or "open_ended" for
            complete history.

    Returns:
        A RetentionMetric expression selecting the highest reached occurrence-count band.

    Example:
        spec.retention(
            "orders", label_property="status", target_event="paid", thresholds=(1, 2), horizon="30d"
        )
    """
    with parameter_names(count_thresholds="thresholds"):
        return RetentionMetric(
            event,
            label_property,
            target_event,
            thresholds,
            None if horizon == "open_ended" else duration(horizon, field="horizon"),
        )


def quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]],
    interpolation: Literal["linear_type7"] = "linear_type7",
) -> MetricQuantile:
    """Request an exact quantile over a window of a metric's integer or Decimal event values.

    Args:
        metric: Name of a metric declared in the model.
        probability: Decimal or decimal string in [0, 1]; binary floats are refused.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        interpolation: Only "linear_type7" is accepted; the default linearly interpolates
            neighboring order statistics.

    Returns:
        A MetricQuantile expression; exact integer/Decimal inputs retain exact quantile semantics.

    Example: spec.quantile("spend", "0.5", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return MetricQuantile(
        metric, _probability(probability, "probability"), Window(start, end), interpolation
    )


def approx_quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]],
) -> ApproxMetricQuantile:
    """Request an approximate event-value quantile using the installed t-digest package.

    Args:
        metric: Name of a metric declared in the model.
        probability: Decimal or decimal string in [0, 1]; binary floating-point inputs are refused.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.

    Returns:
        An ApproxMetricQuantile expression using the installed t-digest implementation.

    Example: spec.approx_quantile("spend", "0.5", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return ApproxMetricQuantile(
        _name(metric, "metric"),
        _probability(probability, "probability"),
        Window(start, end),
    )


def approx_distinct(
    metric: str, *, window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]]
) -> ApproxMetricDistinct:
    """Request an approximate distinct-value count for a metric over a window.

    Args:
        metric: Name of a metric declared in the model.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.

    Returns:
        An ApproxMetricDistinct expression with approximation metadata.

    Example: spec.approx_distinct("spend", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return ApproxMetricDistinct(_name(metric, "metric"), Window(start, end))


def asof(
    attribute: str,
    *,
    before: str | Duration | Literal[0] = "0s",
    relative_to: EventTime | None = None,
) -> AttributeAsOf:
    """Read the value in effect just before the cutoff, never at or after it.

    The cutoff is t0 + before, or the selected event time + before. A change
    effective exactly at t0 is invisible with before="0s", by design, to prevent
    leakage. Positive cutoffs outside treatment raise terrine.design.gap_violation.
    There is no inclusive as-of form.

    Args:
        attribute: Declared static roster attribute, event label or history attribute name.
        before: Cutoff offset as a duration string (see `terrine.windows`) or Duration; "0s" reads
            just before the origin.
        relative_to: EventTime providing the origin; None makes offsets relative to row t0.

    Returns:
        An AttributeAsOf expression carrying strict cutoff semantics and completeness.

    Example: tr.asof("plan", before="0s")
    """
    return AttributeAsOf(attribute, duration(before, field="before"), relative_to)


def event_time(
    event: str,
    *,
    window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]],
    label_property: str | None = None,
    equals: JsonScalar = None,
    occurrence: Literal["first", "last", "nth"] = "first",
    n: int | None = None,
    relative_to: EventTime | None = None,
) -> EventTime:
    """Select the first, last or nth event time in a window, optionally matching a label.


    Args:
        event: Name of an event declared in the model.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        label_property: Declared event label name to match; None selects events without a label
            filter.
        equals: Scalar label value paired with label_property; None means no label filter.
        occurrence: Selection rule "first", "last" or "nth"; "first" selects the earliest matching
            time.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.
        relative_to: EventTime providing the origin; None makes offsets relative to row t0.

    Returns:
        An EventTime expression; materialization returns the selected timestamp or a reasoned null.

    Example: tr.event_time("orders", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return EventTime(event, Window(start, end), label_property, equals, occurrence, n, relative_to)


def since(
    event: str,
    *,
    window: tuple[str | Duration | Literal[0], str | Duration | Literal[0]],
    label_property: str | None = None,
    equals: JsonScalar = None,
    occurrence: Literal["first", "last", "nth"] = "last",
    n: int | None = None,
) -> EventTime:
    """Measure elapsed time from the selected event to t0.

    Returns an Arrow duration after materialization. Filter recency thresholds on
    the returned dataframe; elapsed selectors cannot be predicate operands or
    relative origins. Frame.expr is unavailable: use materialize(), stage(), or
    the dataframe readers instead.


    Args:
        event: Name of an event declared in the model.
        window: Pair of start/end duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        label_property: Declared event label name to match; None selects events without a label
            filter.
        equals: Scalar label value paired with label_property; None means no label filter.
        occurrence: "first", "last" or "nth"; "last" measures recency from the latest matching
            event.
        n: Positive one-based occurrence index for "nth"; None is required for other rules.

    Returns:
        An EventTime elapsed expression; materialization returns an Arrow duration or a reasoned
        null.

    Example: tr.since("orders", window=("-30d", "0s"))
    """
    start, end = _window_pair(window)
    return EventTime(
        event, Window(start, end), label_property, equals, occurrence, n, measure="elapsed"
    )


def gt(expr: Expr, value: JsonScalar) -> Gt:
    """Require an expression's value to be greater than a literal.


    Args:
        expr: Measured expression to compare; elapsed since() selectors are not predicate operands.
        value: Literal scalar value to compare with the expression.

    Returns:
        A greater-than Predicate for output columns or eligibility rules.

    Example: tr.gt(tr.agg("orders", window=("-30d", "0s")), 0)
    """
    return Gt(expr, value)


def eq(expr: Expr, value: JsonScalar) -> Eq:
    """Require an expression's value to equal a literal.


    Args:
        expr: Measured expression to compare; elapsed since() selectors are not predicate operands.
        value: Literal scalar value to compare with the expression.

    Returns:
        An equality Predicate for output columns or eligibility rules.

    Example: tr.eq(tr.agg("orders", window=("-30d", "0s")), 1)
    """
    return Eq(expr, value)


def is_null(expr: Expr, *, negate: bool = False) -> IsNull:
    """Test whether an expression is null, or non-null when ``negate=True``.


    Args:
        expr: Measured expression to compare; elapsed since() selectors are not predicate operands.
        negate: Whether to require a non-null value instead; False tests for null.

    Returns:
        A null-check Predicate for output columns or eligibility rules.

    Example: tr.is_null(tr.event_time("orders", window=("-30d", "0s")))
    """
    return IsNull(expr, negate)


def all_of(*terms: Predicate) -> And:
    """Require every supplied eligibility predicate to hold.


    Args:
        *terms: One or more Predicate arguments; every term must hold for the combined rule to hold.

    Returns:
        An And predicate combining every supplied term.

    Example: tr.all_of(tr.gt(tr.agg("orders", window=("-30d", "0s")), 0))
    """
    return And(tuple(terms))
