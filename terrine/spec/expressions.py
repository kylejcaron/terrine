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
    "Gt",
    "Eq",
    "IsNull",
    "And",
    "agg",
    "asof",
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _name(self.metric, "metric"))
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")


@dataclass(frozen=True, slots=True)
class AttributeAsOf:
    attribute: str
    before: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "attribute", _name(self.attribute, "attribute"))
        object.__setattr__(self, "before", seconds(self.before))


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


type Expr = MetricAgg | AttributeAsOf
type Predicate = Gt | Eq | IsNull | And


def _require_expr(value: object) -> None:
    if not isinstance(value, (MetricAgg, AttributeAsOf)):
        raise TypeError("expected a supported expression node")


def agg(metric: str, *, window: tuple[str | int, str | int]) -> MetricAgg:
    if not isinstance(window, (tuple, list)) or len(window) != 2:
        raise ValueError("window must contain a start and exclusive end")
    return MetricAgg(metric, Window(window[0], window[1]))


def asof(attribute: str, *, before: str | int) -> AttributeAsOf:
    return AttributeAsOf(attribute, seconds(before))


def gt(expr: Expr, value: JsonScalar) -> Gt:
    return Gt(expr, value)


def eq(left: Expr, value: JsonScalar) -> Eq:
    return Eq(left, value)


def is_null(expr: Expr, *, negate: bool = False) -> IsNull:
    return IsNull(expr, negate)


def all_of(*terms: Predicate) -> And:
    return And(tuple(terms))
