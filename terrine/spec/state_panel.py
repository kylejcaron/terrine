"""Data-only state source and bounded panel specifications."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from terrine.model import Model
    from terrine.spec.design import Design
    from terrine.spec.expressions import Expr
from terrine.errors import RefusalSpec
from terrine.spec.anchors import Schedule

_INVALID_SCHEDULE = RefusalSpec(
    "terrine.state_panel.invalid_schedule", "State panel schedule is invalid: {reason}."
)
_INVALID_SOURCE = RefusalSpec(
    "terrine.state_panel.invalid_state_source", "State source is invalid: {reason}."
)
_INVALID_INPUT = RefusalSpec(
    "terrine.state_panel.invalid_state_source", "State source input {field} is invalid: {reason}."
)
_INVALID_OBSERVATIONS = RefusalSpec(
    "terrine.state_panel.invalid_observations", "State observations are invalid: {reason}."
)
_UNSUPPORTED = RefusalSpec(
    "terrine.state_panel.unsupported_combination",
    "State panel combination is unsupported: {reason}.",
)

__all__ = ["StateObservations", "BinnedMetricState", "StatePanel"]


def _levels(levels: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(levels, tuple):
        raise _INVALID_INPUT.error(reason="wrong_type", field="levels")
    if len(levels) < 2:
        raise _INVALID_INPUT.error(reason="too_few_levels", field="levels")
    if any(not isinstance(level, str) or not level for level in levels):
        raise _INVALID_INPUT.error(reason="empty_name", field="levels")
    if len(set(levels)) != len(levels):
        raise _INVALID_INPUT.error(reason="duplicate_names", field="levels")
    return levels


@dataclass(frozen=True, slots=True)
class StateObservations:
    """Interpret a source property as point observations, never carried state."""

    property: str
    levels: tuple[str, ...]
    ordered: bool = True
    absorbing: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.property, str) or not self.property.strip():
            raise _INVALID_INPUT.error(reason="empty_name", field="property")
        object.__setattr__(self, "levels", _levels(self.levels))
        if not isinstance(self.ordered, bool):
            raise _INVALID_INPUT.error(reason="wrong_type", field="ordered")
        if not isinstance(self.absorbing, tuple):
            raise _INVALID_INPUT.error(reason="wrong_type", field="absorbing")
        if len(set(self.absorbing)) != len(self.absorbing):
            raise _INVALID_INPUT.error(reason="duplicate_names", field="absorbing")
        if any(value not in self.levels for value in self.absorbing):
            raise _INVALID_INPUT.error(reason="undeclared_name", field="absorbing")
        object.__setattr__(self, "absorbing", tuple(v for v in self.levels if v in self.absorbing))


@dataclass(frozen=True, slots=True)
class BinnedMetricState:
    """Map an exact metric result to right-closed ordered state intervals."""

    metric: str
    cutpoints: tuple[int | float | Decimal, ...]
    levels: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.metric, str) or not self.metric.strip():
            raise _INVALID_INPUT.error(reason="empty_name", field="metric")
        object.__setattr__(self, "levels", _levels(self.levels))
        if not isinstance(self.cutpoints, tuple):
            raise _INVALID_INPUT.error(reason="wrong_type", field="cutpoints")
        if not self.cutpoints:
            raise _INVALID_INPUT.error(reason="empty", field="cutpoints")
        if len(self.levels) != len(self.cutpoints) + 1:
            raise _INVALID_INPUT.error(reason="levels_cutpoints_mismatch", field="levels")
        previous = None
        for point in self.cutpoints:
            _finite_cutpoint(point)
            if previous is not None and not previous < point:
                raise _INVALID_INPUT.error(reason="not_increasing", field="cutpoints")
            previous = point


@dataclass(frozen=True, slots=True, init=False)
class StatePanel:
    """Bounded repeated state rows over a finite schedule."""

    state: StateObservations | BinnedMetricState
    max_periods: int
    baseline_features: Mapping[str, Expr]

    def __init__(
        self,
        state: StateObservations | BinnedMetricState,
        *,
        max_periods: int,
        baseline_features: Mapping[str, Expr] | None = None,
    ) -> None:
        from types import MappingProxyType

        from terrine.spec.design import _named_nodes
        from terrine.spec.expressions import AttributeAsOf, MetricAgg

        if not isinstance(state, (StateObservations, BinnedMetricState)):
            raise _INVALID_INPUT.error(reason="wrong_type", field="state")
        if isinstance(max_periods, bool) or not isinstance(max_periods, int) or max_periods <= 0:
            raise _INVALID_INPUT.error(reason="not_positive_integer", field="max_periods")
        nodes = _named_nodes(baseline_features, (MetricAgg, AttributeAsOf), "baseline_features")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "max_periods", max_periods)
        object.__setattr__(self, "baseline_features", MappingProxyType(dict(nodes)))


def _finite_cutpoint(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise _INVALID_INPUT.error(reason="wrong_type", field="cutpoints")
    if isinstance(value, float) and not math.isfinite(value):
        raise _INVALID_INPUT.error(reason="not_finite", field="cutpoints")
    if isinstance(value, Decimal) and not value.is_finite():
        raise _INVALID_INPUT.error(reason="not_finite", field="cutpoints")


def encode_cutpoint(value: int | float | Decimal) -> dict[str, object]:
    """Encode a cutpoint without losing its declared numeric representation."""
    _finite_cutpoint(value)
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, Decimal):
        item = value.as_tuple()
        return {
            "type": "decimal",
            "sign": item.sign,
            "digits": list(item.digits),
            "exponent": item.exponent,
        }
    return {"type": "float64", "value": value.hex()}


def decode_cutpoint(value: object) -> int | float | Decimal:
    """Decode a strictly tagged cutpoint representation."""
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ValueError("cutpoint must be a tagged mapping")
    tag = value["type"]
    if tag == "int" and set(value) == {"type", "value"} and isinstance(value["value"], str):
        text = value["value"]
        result = int(text)
        if str(result) != text:
            raise ValueError("integer cutpoint is not canonical")
        return result
    if tag == "decimal" and set(value) == {"type", "sign", "digits", "exponent"}:
        sign, digits, exponent = value["sign"], value["digits"], value["exponent"]
        if (
            sign not in (0, 1)
            or not isinstance(digits, list)
            or any(type(d) is not int or not 0 <= d <= 9 for d in digits)
            or type(exponent) is not int
        ):
            raise ValueError("invalid decimal cutpoint")
        result = Decimal((sign, tuple(digits), exponent))
        if not result.is_finite():
            raise ValueError("cutpoints must be finite")
        return result
    if tag == "float64" and set(value) == {"type", "value"} and isinstance(value["value"], str):
        result = float.fromhex(value["value"])
        if not math.isfinite(result) or result.hex() != value["value"]:
            raise ValueError("float cutpoint must be canonical and finite")
        return result
    raise ValueError("unknown or malformed cutpoint encoding")


def schedule_periods(schedule: Schedule) -> int:
    """Number of whole ``every`` periods from ``start`` through ``end`` under a fixed day boundary.

    A state transition's period is a fixed elapsed width, so a calendar-month schedule
    refuses with a route to an elapsed step. The day boundary is a fixed offset, so the
    span is exact day arithmetic; a span that is not a whole number of periods refuses
    rather than truncating.
    """
    if schedule.every is None:
        raise _INVALID_SCHEDULE.error(
            reason="calendar_months_unsupported", every=f"{schedule.months}mo", route="4w"
        )
    elapsed = (schedule.end_exclusive - schedule.start).days * 86_400
    if elapsed % schedule.every:
        raise _INVALID_SCHEDULE.error(
            reason="schedule_not_whole_periods",
            every=schedule.every,
            start=schedule.start.isoformat(),
            end=schedule.end.isoformat(),
        )
    return elapsed // schedule.every


def validate_state_panel(
    design: Design, model: Model, required: set[str], obligations: list[str]
) -> None:
    """Validate schedule shape and state-source references without reading rows."""
    from terrine.spec.expressions import MetricAgg
    from terrine.spec.validate import _expression, _required
    from terrine.windows import Window

    panel = design.shape
    assert isinstance(panel, StatePanel)
    schedule = design.anchor
    if not isinstance(schedule, Schedule):
        raise _INVALID_SCHEDULE.error(reason="a finite Schedule anchor is required")
    periods = schedule_periods(schedule)
    if periods > panel.max_periods:
        raise _INVALID_SCHEDULE.error(
            reason="schedule_exceeds_max_periods", periods=periods, max_periods=panel.max_periods
        )
    if design.outcomes or design.treatment is not None:
        raise _UNSUPPORTED.error(
            reason="state-panel responses are to_state; treatment and outcomes are unsupported"
        )
    if isinstance(panel.state, StateObservations):
        matches = [
            (source, source.properties[panel.state.property])
            for source in model.sources.values()
            if panel.state.property in source.properties
        ]
        if len(matches) != 1:
            raise _INVALID_SOURCE.error(reason="property must resolve to one source-owned property")
        source, prop = matches[0]
        if prop.dimension_name is not None or prop.dtype != "string" or prop.as_of != "event_time":
            raise _INVALID_SOURCE.error(
                reason=(
                    "observed states require a source-owned string property with "
                    "dimension=None and as_of=event_time"
                )
            )
        if design.unit not in source.units:
            raise _INVALID_SOURCE.error(reason="state source lacks the requested unit mapping")
        _required(model, source.name, required)
    else:
        assert schedule.every is not None
        _expression(
            MetricAgg(panel.state.metric, Window(-schedule.every, 0)),
            "state",
            design,
            model,
            required,
            obligations,
        )
        metric = model.metrics.get(panel.state.metric)
        if metric is None:
            raise _INVALID_SOURCE.error(reason="unknown metric")
        if metric.kind == "conversion":
            raise _INVALID_SOURCE.error(
                reason=(
                    "binned states require a numeric non-boolean metric; use a "
                    "numeric metric or declared observations"
                )
            )
