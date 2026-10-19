"""Typed, semantic-layer dataset construction."""

from importlib import import_module
from typing import TYPE_CHECKING

from terrine.errors import TerrineError

if TYPE_CHECKING:
    from terrine.compile.cost import preview
    from terrine.data import Budget, Coverage, UnitSource
    from terrine.frame import CohortTable, Frame
    from terrine.model import Model
    from terrine.spec import (
        AnchorRange,
        ApproxMetricDistinct,
        ApproxMetricQuantile,
        BinnedMetricState,
        CohortGrouping,
        CohortSpec,
        Design,
        EventAnchor,
        EventSequence,
        EventTime,
        FixedDate,
        FunnelSpec,
        LifeTable,
        MetricQuantile,
        RetentionMetric,
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
from terrine.windows import Window

__all__ = [
    "AnchorRange",
    "ApproxMetricQuantile",
    "ApproxMetricDistinct",
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
    "MetricQuantile",
    "RetentionMetric",
    "retention",
    "Model",
    "Schedule",
    "SketchRequest",
    "Treatment",
    "UnitSource",
    "Window",
    "TerrineError",
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
    "CohortTable",
    "Trajectory",
    "TimeToEvent",
    "to_yaml",
    "quantile",
]

_LAZY_MODULES = {
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
