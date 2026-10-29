"""Immutable, data-only expression and predicate specifications."""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from terrine.errors import RefusalSpec, parameter_names
from terrine.windows import Window, seconds

__all__ = [
    "Expr",
    "Predicate",
    "MetricAgg",
    "MetricQuantile",
    "SketchRequest",
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


@dataclass(frozen=True, slots=True)
class MetricQuantile:
    metric: str
    probability: Decimal
    window: Window
    interpolation: str = "linear_type7"

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability, "probability"))
        _window(self.window)
        if self.interpolation != "linear_type7":
            raise _CHOICE.error(
                reason="unsupported_value", field="interpolation", choices=("linear_type7",)
            )


@dataclass(frozen=True, slots=True)
class SketchRequest:
    algorithm: str
    version: str
    rank_error: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "algorithm", _name(self.algorithm, "algorithm"))
        object.__setattr__(self, "version", _name(self.version, "version"))
        error = _probability(self.rank_error, "rank_error")
        if error == 0:
            raise _INVALID.error(reason="not_positive", field="rank_error")
        object.__setattr__(self, "rank_error", error)


@dataclass(frozen=True, slots=True)
class ApproxMetricQuantile:
    metric: str
    probability: Decimal
    window: Window
    sketch: SketchRequest

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability, "probability"))
        _window(self.window)
        if not isinstance(self.sketch, SketchRequest):
            raise _INVALID.error(reason="wrong_type", field="sketch")


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
    finite_horizon_seconds: int | None

    def __post_init__(self) -> None:
        for field in ("event", "label_property", "target_event"):
            object.__setattr__(self, field, _name(getattr(self, field), field))
        thresholds = tuple(self.count_thresholds)
        if not thresholds:
            raise _INVALID.error(reason="empty", field="count_thresholds")
        if any(type(value) is not int or not 0 < value < 2**63 for value in thresholds):
            raise _INVALID.error(reason="not_positive_integer", field="count_thresholds")
        if tuple(sorted(set(thresholds))) != thresholds:
            raise _INVALID.error(reason="not_increasing", field="count_thresholds")
        object.__setattr__(self, "count_thresholds", thresholds)
        horizon = self.finite_horizon_seconds
        if horizon is not None and (type(horizon) is not int or not 0 < horizon < 2**63):
            raise _INVALID.error(reason="not_positive", field="finite_horizon_seconds")


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
    before: int
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attribute", _name(self.attribute, "attribute"))
        object.__setattr__(self, "before", seconds(self.before, field="before"))
        _relative_to(self.relative_to)


@dataclass(frozen=True, slots=True)
class EventTime:
    event: str
    window: Window
    label_property: str | None = None
    equals: JsonScalar = None
    occurrence: str = "first"
    n: int | None = None
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event", _name(self.event, "event"))
        _window(self.window)
        if (self.label_property is None) != (self.equals is None):
            raise _INVALID.error(reason="unpaired_label_filter", field="label_property")
        if self.label_property is not None:
            object.__setattr__(self, "label_property", _name(self.label_property, "label_property"))
            object.__setattr__(self, "equals", _scalar(self.equals, "equals"))
        if self.occurrence not in {"first", "last", "nth"}:
            raise _CHOICE.error(
                reason="unsupported_value", field="occurrence", choices=("first", "last", "nth")
            )
        if self.occurrence in {"first", "last"} and self.n is not None:
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
    left: Expr
    value: JsonScalar

    def __post_init__(self) -> None:
        _require_expr(self.left, "left")
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
    elif isinstance(value, Eq):
        yield value.left
    elif isinstance(value, (Gt, IsNull)):
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
        return (origin[0] + expression.window.start, origin[1] + expression.window.end)
    if isinstance(
        expression, (MetricAgg, MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)
    ):
        origin = (
            (0, 0)
            if not isinstance(expression, MetricAgg) or expression.relative_to is None
            else _expression_support_seconds(expression.relative_to)
        )
        return (origin[0] + expression.window.start, origin[1] + expression.window.end)
    if isinstance(expression, AttributeAsOf):
        origin = (
            (0, 0)
            if expression.relative_to is None
            else _expression_support_seconds(expression.relative_to)
        )
        return (origin[0] + expression.before, origin[1] + expression.before)
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


def _window_pair(value: object) -> tuple[str | int, str | int]:
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise _INVALID.error(reason="not_a_pair", field="window")
    return value[0], value[1]


def agg(
    metric: str,
    *,
    window: tuple[str | int, str | int],
    relative_to: EventTime | None = None,
) -> MetricAgg:
    start, end = _window_pair(window)
    return MetricAgg(metric, Window(start, end), relative_to)


def retention(
    event: str,
    *,
    label_property: str,
    target_event: str,
    thresholds: tuple[int, ...],
    horizon: str | int,
) -> RetentionMetric:
    """Count distinct ``event`` identities, then return the highest reached threshold band.

    A finite duration measures [t0, t0+horizon); ``open_ended`` ends at the
    source's declared complete-through endpoint. Unknown follow-up stays null.
    """
    with parameter_names(count_thresholds="thresholds", finite_horizon_seconds="horizon"):
        return RetentionMetric(
            event,
            label_property,
            target_event,
            thresholds,
            None if horizon == "open_ended" else seconds(horizon, field="horizon"),
        )


def quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | int, str | int],
    interpolation: str = "linear_type7",
) -> MetricQuantile:
    start, end = _window_pair(window)
    return MetricQuantile(
        metric, _probability(probability, "probability"), Window(start, end), interpolation
    )


def approx_quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | int, str | int],
    sketch: SketchRequest,
) -> ApproxMetricQuantile:
    start, end = _window_pair(window)
    return ApproxMetricQuantile(
        _name(metric, "metric"),
        _probability(probability, "probability"),
        Window(start, end),
        sketch,
    )


def approx_distinct(metric: str, *, window: tuple[str | int, str | int]) -> ApproxMetricDistinct:
    start, end = _window_pair(window)
    return ApproxMetricDistinct(_name(metric, "metric"), Window(start, end))


def asof(
    attribute: str,
    *,
    before: str | int,
    relative_to: EventTime | None = None,
) -> AttributeAsOf:
    return AttributeAsOf(attribute, seconds(before, field="before"), relative_to)


def event_time(
    event: str,
    *,
    window: tuple[str | int, str | int],
    label_property: str | None = None,
    equals: JsonScalar = None,
    occurrence: str = "first",
    n: int | None = None,
    relative_to: EventTime | None = None,
) -> EventTime:
    start, end = _window_pair(window)
    return EventTime(event, Window(start, end), label_property, equals, occurrence, n, relative_to)


def gt(expr: Expr, value: JsonScalar) -> Gt:
    return Gt(expr, value)


def eq(left: Expr, value: JsonScalar) -> Eq:
    return Eq(left, value)


def is_null(expr: Expr, *, negate: bool = False) -> IsNull:
    return IsNull(expr, negate)


def all_of(*terms: Predicate) -> And:
    return And(tuple(terms))
