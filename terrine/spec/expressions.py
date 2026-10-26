"""Immutable, data-only expression and predicate specifications."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from terrine.errors import RefusalSpec
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


def _probability(value: Decimal | str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(value, (Decimal, str)):
        raise TypeError("probability must be a Decimal or decimal string, never a float")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("probability must be a finite decimal") from exc
    if not result.is_finite() or result < 0 or result > 1:
        raise ValueError("probability must be finite and in [0, 1]")
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


@dataclass(frozen=True, slots=True)
class MetricQuantile:
    metric: str
    probability: Decimal
    window: Window
    interpolation: str = "linear_type7"

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability))
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")
        if self.interpolation != "linear_type7":
            raise ValueError("only linear_type7 interpolation is supported")


@dataclass(frozen=True, slots=True)
class SketchRequest:
    algorithm: str
    version: str
    rank_error: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "algorithm", _name(self.algorithm, "algorithm"))
        object.__setattr__(self, "version", _name(self.version, "version"))
        error = _probability(self.rank_error)
        if error == 0:
            raise ValueError("rank_error must be positive")
        object.__setattr__(self, "rank_error", error)


@dataclass(frozen=True, slots=True)
class ApproxMetricQuantile:
    metric: str
    probability: Decimal
    window: Window
    sketch: SketchRequest

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        object.__setattr__(self, "probability", _probability(self.probability))
        if not isinstance(self.window, Window) or not isinstance(self.sketch, SketchRequest):
            raise TypeError("window and sketch must be typed specifications")


@dataclass(frozen=True, slots=True)
class ApproxMetricDistinct:
    metric: str
    window: Window

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")


@dataclass(frozen=True, slots=True)
class RetentionMetric:
    """Ordinal bands of distinct occurrence identities in a declared follow-up horizon."""

    fact: str
    label_property: str
    target_event: str
    count_thresholds: tuple[int, ...]
    finite_horizon_seconds: int | None

    def __post_init__(self) -> None:
        for field in ("fact", "label_property", "target_event"):
            object.__setattr__(self, field, _name(getattr(self, field), field))
        thresholds = tuple(self.count_thresholds)
        if (
            not thresholds
            or any(type(value) is not int or not 0 < value < 2**63 for value in thresholds)
            or tuple(sorted(set(thresholds))) != thresholds
        ):
            raise ValueError("thresholds must be strictly increasing positive Int64 counts")
        object.__setattr__(self, "count_thresholds", thresholds)
        horizon = self.finite_horizon_seconds
        if horizon is not None and (type(horizon) is not int or not 0 < horizon < 2**63):
            raise ValueError("finite horizon must be positive Int64 elapsed seconds")


type JsonScalar = None | bool | int | float | str


def _name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _scalar(value: object) -> JsonScalar:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise TypeError("predicate values must be finite JSON scalars")


@dataclass(frozen=True, slots=True)
class MetricAgg:
    metric: str
    window: Window
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")
        if self.relative_to is not None and not isinstance(self.relative_to, EventTime):
            raise TypeError("relative_to must be EventTime or None")


@dataclass(frozen=True, slots=True)
class AttributeAsOf:
    attribute: str
    before: int
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attribute", _name(self.attribute, "attribute"))
        object.__setattr__(self, "before", seconds(self.before))
        if self.relative_to is not None and not isinstance(self.relative_to, EventTime):
            raise TypeError("relative_to must be EventTime or None")


@dataclass(frozen=True, slots=True)
class EventTime:
    fact: str
    window: Window
    label_property: str | None = None
    equals: JsonScalar = None
    occurrence: str = "first"
    n: int | None = None
    relative_to: EventTime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "fact", _name(self.fact, "fact"))
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")
        if (self.label_property is None) != (self.equals is None):
            raise ValueError("label_property and equals must be supplied together")
        if self.label_property is not None:
            object.__setattr__(self, "label_property", _name(self.label_property, "label_property"))
            object.__setattr__(self, "equals", _scalar(self.equals))
        if self.occurrence not in {"first", "last", "nth"}:
            raise ValueError("occurrence must be 'first', 'last', or 'nth'")
        if self.occurrence in {"first", "last"} and self.n is not None:
            raise ValueError("n is only valid for nth occurrence")
        if self.occurrence == "nth" and (
            isinstance(self.n, bool) or not isinstance(self.n, int) or self.n < 1
        ):
            raise ValueError("nth occurrence requires positive n")
        if self.relative_to is not None and not isinstance(self.relative_to, EventTime):
            raise TypeError("relative_to must be EventTime or None")


@dataclass(frozen=True, slots=True)
class Gt:
    expr: Expr
    value: JsonScalar

    def __post_init__(self) -> None:
        _require_expr(self.expr)
        object.__setattr__(self, "value", _scalar(self.value))


@dataclass(frozen=True, slots=True)
class Eq:
    left: Expr
    value: JsonScalar

    def __post_init__(self) -> None:
        _require_expr(self.left)
        object.__setattr__(self, "value", _scalar(self.value))


@dataclass(frozen=True, slots=True)
class IsNull:
    expr: Expr
    negate: bool = False

    def __post_init__(self) -> None:
        _require_expr(self.expr)
        if not isinstance(self.negate, bool):
            raise TypeError("negate must be a boolean")


@dataclass(frozen=True, slots=True)
class And:
    terms: tuple[Predicate, ...]

    def __post_init__(self) -> None:
        terms = tuple(self.terms)
        if not terms or any(not isinstance(term, (Gt, Eq, IsNull, And)) for term in terms):
            raise ValueError("And requires one or more predicates")
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


def _require_expr(value: object) -> None:
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
        raise TypeError("expected a supported expression node")


def agg(
    metric: str,
    *,
    window: tuple[str | int, str | int],
    relative_to: EventTime | None = None,
) -> MetricAgg:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return MetricAgg(metric, Window(window[0], window[1]), relative_to)


def retention(
    fact: str,
    *,
    label_property: str,
    target_event: str,
    thresholds: tuple[int, ...],
    horizon: str | int,
) -> RetentionMetric:
    """Count distinct fact identities, then return the highest reached threshold band.

    A finite duration measures [t0, t0+horizon); ``open_ended`` ends at the
    source's declared complete-through endpoint. Unknown follow-up stays null.
    """
    return RetentionMetric(
        fact,
        label_property,
        target_event,
        thresholds,
        None if horizon == "open_ended" else seconds(horizon),
    )


def quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | int, str | int],
    interpolation: str = "linear_type7",
) -> MetricQuantile:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return MetricQuantile(metric, _probability(probability), Window(*window), interpolation)


def approx_quantile(
    metric: str,
    probability: Decimal | str,
    *,
    window: tuple[str | int, str | int],
    sketch: SketchRequest,
) -> ApproxMetricQuantile:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return ApproxMetricQuantile(
        _name(metric, "metric"), _probability(probability), Window(*window), sketch
    )


def approx_distinct(metric: str, *, window: tuple[str | int, str | int]) -> ApproxMetricDistinct:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return ApproxMetricDistinct(_name(metric, "metric"), Window(*window))


def asof(
    attribute: str,
    *,
    before: str | int,
    relative_to: EventTime | None = None,
) -> AttributeAsOf:
    return AttributeAsOf(attribute, seconds(before), relative_to)


def event_time(
    fact: str,
    *,
    window: tuple[str | int, str | int],
    label_property: str | None = None,
    equals: JsonScalar = None,
    occurrence: str = "first",
    n: int | None = None,
    relative_to: EventTime | None = None,
) -> EventTime:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return EventTime(
        fact, Window(window[0], window[1]), label_property, equals, occurrence, n, relative_to
    )


def gt(expr: Expr, value: JsonScalar) -> Gt:
    return Gt(expr, value)


def eq(left: Expr, value: JsonScalar) -> Eq:
    return Eq(left, value)


def is_null(expr: Expr, *, negate: bool = False) -> IsNull:
    return IsNull(expr, negate)


def all_of(*terms: Predicate) -> And:
    return And(tuple(terms))
