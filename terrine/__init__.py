"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine import recipes as recipes
    from terrine.compile.cost import preview
    from terrine.data import Budget, Coverage, Events, UnitSource
    from terrine.dataset.reference import Dataset
    from terrine.errors import UnverifiedBackendWarning
    from terrine.frame import Frame
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        BinnedMetricState,
        CohortGrouping,
        CohortSpec,
        DatasetDefinition,
        Design,
        EventAnchor,
        EventSequence,
        EventTime,
        FixedDate,
        FunnelSpec,
        LifeTable,
        Schedule,
        SessionSpec,
        SketchRequest,
        StateObservations,
        StatePanel,
        TimeToEvent,
        Trajectory,
        Treatment,
        agg,
        all_of,
        approx_distinct,
        approx_quantile,
        asof,
        eq,
        event_time,
        from_yaml,
        gt,
        is_null,
        quantile,
        retention,
        to_yaml,
    )

__all__ = [
    "Dataset",
    "DatasetDefinition",
    "AnchorRange",
    "Budget",
    "EventSequence",
    "EventTime",
    "FunnelSpec",
    "SessionSpec",
    "Coverage",
    "Events",
    "Design",
    "EventAnchor",
    "FixedDate",
    "Frame",
    "retention",
    "Model",
    "Schedule",
    "SketchRequest",
    "Treatment",
    "UnitSource",
    "Window",
    "TerrineError",
    "UnverifiedBackendWarning",
    "agg",
    "all_of",
    "approx_quantile",
    "approx_distinct",
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
    "CohortGrouping",
    "CohortSpec",
    "LifeTable",
    "Trajectory",
    "TimeToEvent",
    "to_yaml",
    "quantile",
    "recipes",
]

_LAZY_MODULES = {
    "Dataset": "terrine.dataset.reference",
    "Budget": "terrine.data",
    "Frame": "terrine.frame",
    "preview": "terrine.compile.cost",
    "Coverage": "terrine.data",
    "Events": "terrine.data",
    "Model": "terrine.model",
    "UnitSource": "terrine.data",
    "UnverifiedBackendWarning": "terrine.errors",
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
