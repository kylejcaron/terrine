"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError

if TYPE_CHECKING:
    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        Design,
        EventAnchor,
        FixedDate,
        Schedule,
        Treatment,
        agg,
        all_of,
        asof,
        eq,
        from_yaml,
        gt,
        is_null,
        to_yaml,
    )
from terrine.windows import Window

__all__ = [
    "AnchorRange",
    "Coverage",
    "Design",
    "EventAnchor",
    "FixedDate",
    "Model",
    "Schedule",
    "Treatment",
    "UnitSource",
    "Window",
    "TerrineError",
    "agg",
    "all_of",
    "asof",
    "eq",
    "from_yaml",
    "gt",
    "is_null",
    "to_yaml",
]

_LAZY_MODULES = {
    "Coverage": "terrine.data",
    "Model": "terrine.model",
    "UnitSource": "terrine.data",
}


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_LAZY_MODULES.get(name, "terrine.spec")), name)
    globals()[name] = value
    return value
