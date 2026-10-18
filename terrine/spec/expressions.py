"""Immutable, data-only expression and predicate specifications."""

from __future__ import annotations

import math
from dataclasses import dataclass

from terrine.windows import Window, seconds

__all__ = [
    "Expr",
    "Predicate",
    "MetricAgg",
    "AttributeAsOf",
    "EventTime",
    "Gt",
    "Eq",
    "IsNull",
    "And",
    "agg",
    "asof",
    "event_time",
    "gt",
    "eq",
    "is_null",
    "all_of",
]

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
        if self.occurrence not in {"first", "nth"}:
            raise ValueError("occurrence must be 'first' or 'nth'")
        if self.occurrence == "first" and self.n is not None:
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


type Expr = MetricAgg | AttributeAsOf | EventTime
type Predicate = Gt | Eq | IsNull | And


def _require_expr(value: object) -> None:
    if not isinstance(value, (MetricAgg, AttributeAsOf, EventTime)):
        raise TypeError("expected a supported expression node")


def agg(metric: str, *, window: tuple[str | int, str | int]) -> MetricAgg:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return MetricAgg(metric, Window(window[0], window[1]))


def asof(attribute: str, *, before: str | int) -> AttributeAsOf:
    return AttributeAsOf(attribute, seconds(before))


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
