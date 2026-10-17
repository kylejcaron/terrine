"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError

if TYPE_CHECKING:
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
    "Design",
    "EventAnchor",
    "FixedDate",
    "Schedule",
    "Treatment",
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


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module("terrine.spec"), name)
    globals()[name] = value
    return value
