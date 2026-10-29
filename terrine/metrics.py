"""Metric declarations for ``Events`` bound through ``Model.from_tables``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import ValidationError

from terrine.data import EventMetric, Ratio
from terrine.errors import RefusalSpec
from terrine.semantic.models import Filter

__all__ = ["count", "sum", "mean", "min", "max", "count_distinct", "ratio"]

type JsonScalar = None | bool | int | float | str
type Where = Mapping[str, JsonScalar | Sequence[JsonScalar]]
type _Op = Literal["equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "between"]

_INVALID_FILTER = RefusalSpec(
    "terrine.model.invalid_binding",
    "where= key {key!r} is not a label name or <label>__<op> with a usable value; the "
    "operators are ne, in, not_in, gt, gte, lt, lte and between, comparisons take one "
    "scalar, in and not_in a non-empty list, and between a (low, high) pair.",
)
_INVALID_ARGUMENT = RefusalSpec(
    "terrine.model.invalid_binding",
    "Metric argument {field!r} is unusable: name a column or metric, pass per='event' "
    "or per='day', and give where= as a mapping keyed by label.",
)
_OPERATORS: dict[str, _Op] = {
    "ne": "not_equals",
    "in": "in",
    "not_in": "not_in",
    "gt": "gt",
    "gte": "gte",
    "lt": "lt",
    "lte": "lte",
    "between": "between",
}
_LISTS = (list, tuple, set, frozenset)


def _name(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _INVALID_ARGUMENT.error(reason="invalid_argument", field=field)
    return value


def _filter(key: object, value: object) -> Filter:
    if not isinstance(key, str):
        raise _INVALID_ARGUMENT.error(reason="invalid_argument", field="where")
    label, separator, suffix = key.partition("__")
    if not label or "__" in suffix or (separator and suffix not in _OPERATORS):
        raise _INVALID_FILTER.error(reason="invalid_filter", key=key)
    op: _Op = _OPERATORS[suffix] if separator else "in" if isinstance(value, _LISTS) else "equals"
    if op in {"in", "not_in", "between"}:
        if not isinstance(value, _LISTS):
            raise _INVALID_FILTER.error(reason="invalid_filter", key=key)
        # Sets carry no order; a sorted tuple keeps the catalog and model hash stable.
        values = (
            tuple(sorted(value, key=repr)) if isinstance(value, (set, frozenset)) else tuple(value)
        )
    else:
        values = (value,)
    try:
        return Filter(property=label, op=op, values=values)
    except ValidationError as exc:
        raise _INVALID_FILTER.error(reason="invalid_filter", key=key) from exc


def _filters(where: Where | None) -> tuple[Filter, ...]:
    if where is None:
        return ()
    if not isinstance(where, Mapping):
        raise _INVALID_ARGUMENT.error(reason="invalid_argument", field="where")
    return tuple(_filter(key, value) for key, value in where.items())


def count(*, where: Where | None = None) -> EventMetric:
    """Number of events, optionally only those matching ``where``."""
    return EventMetric("count", None, _filters(where))


def sum(column: str, *, where: Where | None = None) -> EventMetric:
    """Sum of ``column`` over the matching events."""
    return EventMetric("sum", _name(column, "column"), _filters(where))


def mean(
    column: str, *, per: Literal["event", "day"] = "event", where: Where | None = None
) -> EventMetric:
    """Mean of ``column`` per event, or per local calendar day of the window."""
    if per not in ("event", "day"):
        raise _INVALID_ARGUMENT.error(reason="invalid_argument", field="per")
    aggregation = "avg_event" if per == "event" else "avg_calendar_day"
    return EventMetric(aggregation, _name(column, "column"), _filters(where))


def min(column: str, *, where: Where | None = None) -> EventMetric:
    """Smallest ``column`` value over the matching events."""
    return EventMetric("min", _name(column, "column"), _filters(where))


def max(column: str, *, where: Where | None = None) -> EventMetric:
    """Largest ``column`` value over the matching events."""
    return EventMetric("max", _name(column, "column"), _filters(where))


def count_distinct(column: str, *, where: Where | None = None) -> EventMetric:
    """Number of distinct ``column`` values over the matching events."""
    return EventMetric("count_distinct", _name(column, "column"), _filters(where))


def ratio(numerator: str, denominator: str) -> Ratio:
    """Quotient of two metrics declared on the same ``Events``."""
    return Ratio(_name(numerator, "numerator"), _name(denominator, "denominator"))
