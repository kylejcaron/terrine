"""Immutable design-level inputs, independent of model and query engines."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING

from terrine._time import utc_instant

if TYPE_CHECKING:
    from terrine.frame import Frame
    from terrine.model import Model
    from terrine.spec.panels import (
        CalendarComparisonSpec,
        EventPanelSpec,
        MatchedT0Sample,
        SequentialSpec,
    )

from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.expressions import (
    And,
    AttributeAsOf,
    Eq,
    EventTime,
    Expr,
    Gt,
    IsNull,
    MetricAgg,
    Predicate,
)
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent
from terrine.windows import seconds

__all__ = ["Treatment", "Design"]


def _named_nodes(
    values: Mapping[str, object] | None, allowed: tuple[type, ...], field_name: str
) -> Mapping[str, object]:
    if values is None:
        return MappingProxyType({})
    if not isinstance(values, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    copied: dict[str, object] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field_name} keys must be non-empty strings")
        if not isinstance(value, allowed):
            raise TypeError(f"{field_name} values must be typed specification nodes")
        copied[key] = value
    return MappingProxyType(copied)


@dataclass(frozen=True, slots=True, init=False)
class Treatment:
    attribute: str
    at: int
    known_at_t0: bool

    def __init__(self, attribute: str, at: str | int = "0d", known_at_t0: bool = False) -> None:
        if not isinstance(attribute, str) or not attribute.strip():
            raise ValueError("treatment attribute must be a non-empty string")
        if not isinstance(known_at_t0, bool):
            raise TypeError("known_at_t0 must be a boolean")
        object.__setattr__(self, "attribute", attribute)
        object.__setattr__(self, "at", seconds(at))
        object.__setattr__(self, "known_at_t0", known_at_t0)


@dataclass(frozen=True, slots=True, init=False)
class Design:
    unit: str
    anchor: FixedDate | EventAnchor | Schedule
    features: Mapping[str, Expr]
    outcomes: Mapping[str, Expr]
    eligibility: Mapping[str, Predicate]
    treatment: Treatment | None
    gap: int
    shape: (
        str
        | StatePanel
        | EventSequence
        | SequentialSpec
        | EventPanelSpec
        | CalendarComparisonSpec
        | CohortSpec
        | TimeToEvent
        | LifeTable
    )
    anchor_range: AnchorRange | None
    sample: MatchedT0Sample | None
    administrative_cutoff: datetime | None

    def __init__(
        self,
        unit: str,
        anchor: FixedDate | EventAnchor | Schedule,
        *,
        features: Mapping[str, Expr] | None = None,
        outcomes: Mapping[str, Expr] | None = None,
        eligibility: Mapping[str, Predicate] | None = None,
        treatment: Treatment | None = None,
        gap: str | int = "0d",
        shape: str
        | StatePanel
        | EventSequence
        | SequentialSpec
        | EventPanelSpec
        | CalendarComparisonSpec
        | CohortSpec
        | TimeToEvent
        | LifeTable = "cross_section",
        anchor_range: AnchorRange | None = None,
        sample: MatchedT0Sample | None = None,
        administrative_cutoff: datetime | None = None,
    ) -> None:
        if not isinstance(unit, str) or not unit.strip():
            raise ValueError("unit must be a non-empty string")
        if not isinstance(anchor, (FixedDate, EventAnchor, Schedule)):
            raise TypeError("anchor must be a supported anchor node")
        if treatment is not None and not isinstance(treatment, Treatment):
            raise TypeError("treatment must be Treatment or None")
        if anchor_range is not None and not isinstance(anchor_range, AnchorRange):
            raise TypeError("anchor_range must be AnchorRange or None")
        from terrine.spec.panels import (
            CalendarComparisonSpec,
            EventPanelSpec,
            MatchedT0Sample,
            SequentialSpec,
        )

        if sample is not None and not isinstance(sample, MatchedT0Sample):
            raise TypeError("sample must be MatchedT0Sample or None")

        if not isinstance(
            shape,
            (
                str,
                StatePanel,
                EventSequence,
                SequentialSpec,
                EventPanelSpec,
                CalendarComparisonSpec,
                CohortSpec,
                TimeToEvent,
                LifeTable,
            ),
        ) or (isinstance(shape, str) and not shape.strip()):
            raise ValueError("shape must be a supported design shape")
        if isinstance(shape, CalendarComparisonSpec) and treatment is not None:
            from terrine.spec.panels import _CONFLICT

            raise _CONFLICT.error()
        if isinstance(shape, SequentialSpec) and treatment is not None:
            from terrine.spec.panels import _CONFLICT

            raise _CONFLICT.error()
        gap_seconds = seconds(gap)
        if gap_seconds < 0:
            raise ValueError("gap must be non-negative")
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "anchor", anchor)
        object.__setattr__(
            self,
            "features",
            _named_nodes(features, (MetricAgg, AttributeAsOf, EventTime), "features"),
        )
        object.__setattr__(
            self,
            "outcomes",
            _named_nodes(outcomes, (MetricAgg, AttributeAsOf, EventTime), "outcomes"),
        )
        object.__setattr__(
            self, "eligibility", _named_nodes(eligibility, (Gt, Eq, IsNull, And), "eligibility")
        )
        object.__setattr__(self, "treatment", treatment)
        object.__setattr__(self, "gap", gap_seconds)
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "anchor_range", anchor_range)
        object.__setattr__(self, "sample", sample)
        if administrative_cutoff is not None:
            if (
                not isinstance(administrative_cutoff, datetime)
                or administrative_cutoff.tzinfo is None
                or administrative_cutoff.utcoffset() is None
            ):
                raise ValueError("administrative_cutoff must be timezone-aware or None")
            administrative_cutoff = utc_instant(administrative_cutoff)
        if isinstance(shape, CohortSpec) and (
            not isinstance(anchor, EventAnchor)
            or anchor.which != "first"
            or anchor.fact != shape.origin
        ):
            raise ValueError("CohortSpec requires its matching first-event anchor")
        if isinstance(shape, CohortSpec) and outcomes:
            raise ValueError("CohortSpec generates outcomes; explicit outcomes are not allowed")
        time_to_event_shape = shape.time_to_event if isinstance(shape, LifeTable) else shape
        if isinstance(time_to_event_shape, TimeToEvent) and (
            isinstance(anchor, Schedule)
            or (isinstance(anchor, EventAnchor) and anchor.which == "each")
        ):
            raise ValueError("time-to-event shapes require one analysis entry per unit")
        if isinstance(time_to_event_shape, TimeToEvent) and outcomes:
            raise ValueError("time-to-event shapes generate outcomes; explicit outcomes are not allowed")
        if isinstance(shape, LifeTable) and not set(shape.group_by).issubset(features or {}):
            raise ValueError("LifeTable group_by names must be declared design features")
        object.__setattr__(self, "administrative_cutoff", administrative_cutoff)

    def validate(self, model: Model) -> None:
        """Validate this design against bound model metadata without executing queries."""
        from terrine.spec.validate import validate_design

        validate_design(self, model)

    def build(self, model: Model) -> Frame:
        """Compile a lazy study frame without reading source rows."""
        from terrine.frame import Frame

        return Frame(self, model)

    @property
    def spec_hash(self) -> str:
        from terrine.spec.io import canonical_json

        return hashlib.sha256(canonical_json(self)).hexdigest()
