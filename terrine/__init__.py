"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError

if TYPE_CHECKING:
    from terrine import recipes as recipes
    from terrine.compile.cost import preview
    from terrine.data import Budget, Coverage, Events, History, UnitSource
    from terrine.dataset.reference import Dataset
    from terrine.errors import UnverifiedBackendWarning
    from terrine.metrics import count, count_distinct, max, mean, min, ratio, sum
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        Design,
        EventAnchor,
        FixedDate,
        Schedule,
        agg,
        all_of,
        asof,
        eq,
        event_time,
        from_yaml,
        gt,
        is_null,
        since,
        to_yaml,
    )

__all__ = [
    "Model",
    "UnitSource",
    "Events",
    "History",
    "Coverage",
    "count",
    "sum",
    "mean",
    "min",
    "max",
    "count_distinct",
    "ratio",
    "Design",
    "FixedDate",
    "EventAnchor",
    "Schedule",
    "AnchorRange",
    "agg",
    "asof",
    "event_time",
    "since",
    "eq",
    "gt",
    "is_null",
    "all_of",
    "recipes",
    "Dataset",
    "Budget",
    "preview",
    "to_yaml",
    "from_yaml",
    "TerrineError",
    "UnverifiedBackendWarning",
]

_LAZY_MODULES = {
    "Dataset": "terrine.dataset.reference",
    "Budget": "terrine.data",
    "preview": "terrine.compile.cost",
    "Coverage": "terrine.data",
    "Events": "terrine.data",
    "History": "terrine.data",
    "Model": "terrine.model",
    "UnitSource": "terrine.data",
    "UnverifiedBackendWarning": "terrine.errors",
    "count": "terrine.metrics",
    "count_distinct": "terrine.metrics",
    "max": "terrine.metrics",
    "mean": "terrine.metrics",
    "min": "terrine.metrics",
    "ratio": "terrine.metrics",
    "sum": "terrine.metrics",
}


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = (
        import_module("terrine.recipes")
        if name == "recipes"
        else getattr(import_module(_LAZY_MODULES.get(name, "terrine.spec")), name)
    )
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
