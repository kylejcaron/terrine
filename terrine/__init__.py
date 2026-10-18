"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError

if TYPE_CHECKING:
    from terrine.compile.cost import preview
    from terrine.data import Budget, Coverage, UnitSource
    from terrine.frame import Frame
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        BinnedMetricState,
        Design,
        EventAnchor,
        EventSequence,
        EventTime,
        FixedDate,
        FunnelSpec,
        Schedule,
        SessionSpec,
        StateObservations,
        StatePanel,
        Treatment,
        agg,
        all_of,
        asof,
        eq,
        event_time,
        from_yaml,
        gt,
        is_null,
        to_yaml,
    )
from terrine.windows import Window

__all__ = [
    "AnchorRange",
    "Budget",
    "EventSequence",
    "EventTime",
    "FunnelSpec",
    "SessionSpec",
    "Coverage",
    "Design",
    "EventAnchor",
    "FixedDate",
    "Frame",
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
    "event_time",
    "gt",
    "is_null",
    "preview",
    "BinnedMetricState",
    "StateObservations",
    "StatePanel",
    "to_yaml",
]

_LAZY_MODULES = {
    "Budget": "terrine.data",
    "Frame": "terrine.frame",
    "preview": "terrine.compile.cost",
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
