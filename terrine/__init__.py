"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError
from terrine.windows import Window

if TYPE_CHECKING:
    from terrine.compile.cost import preview
    from terrine.data import Budget, Coverage, UnitSource
    from terrine.dataset.export import ExportManifest
    from terrine.dataset.reference import Dataset
    from terrine.frame import CohortTable, Frame
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        BinnedMetricState,
        CohortGrouping,
        CohortSpec,
        DatasetRecipe,
        Design,
        EventAnchor,
        EventSequence,
        EventTime,
        FixedDate,
        FunnelSpec,
        LifeTable,
        Schedule,
        SessionSpec,
        StateObservations,
        TimeToEvent,
        Trajectory,
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

__all__ = [
    "Dataset",
    "ExportManifest",
    "DatasetRecipe",
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
    "CohortGrouping",
    "CohortSpec",
    "LifeTable",
    "CohortTable",
    "Trajectory",
    "TimeToEvent",
    "to_yaml",
]

_LAZY_MODULES = {
    "Dataset": "terrine.dataset.reference",
    "ExportManifest": "terrine.dataset.export",
    "Budget": "terrine.data",
    "Frame": "terrine.frame",
    "preview": "terrine.compile.cost",
    "Coverage": "terrine.data",
    "Model": "terrine.model",
    "UnitSource": "terrine.data",
    "CohortTable": "terrine.frame",
}


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_LAZY_MODULES.get(name, "terrine.spec")), name)
    globals()[name] = value
    return value
