"""Typed requests for bounded event-sequence datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.windows import Window, seconds

_UNSUPPORTED_SHAPE = RefusalSpec(
    "terrine.sequence.unsupported_shape",
    "Event-time expression is unsupported by the selected compiler.",
)
_INVALID = RefusalSpec("terrine.sequence.invalid_spec", "Invalid event sequence request: {reason}.")
_MAX = 2**63 - 1


def _name(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _INVALID.error(reason=f"{field} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True, init=False)
class SessionSpec:
    gap_seconds: int

    def __init__(self, gap: str | int) -> None:
        try:
            normalized = seconds(gap)
        except (TypeError, ValueError) as exc:
            raise _INVALID.error(reason="session gap must be integral elapsed seconds") from exc
        if normalized < 0:
            raise _INVALID.error(reason="session gap must be non-negative")
        object.__setattr__(self, "gap_seconds", normalized)


@dataclass(frozen=True, slots=True, init=False)
class FunnelSpec:
    event_property: str
    steps: tuple[str, ...]
    within_seconds: int | None

    def __init__(
        self, event_property: str, steps: tuple[str, ...], within: str | int | None = None
    ) -> None:
        event_property = _name(event_property, "funnel event_property")
        if not isinstance(steps, (tuple, list)):
            raise _INVALID.error(reason="funnel steps must be an ordered sequence")
        normalized = tuple(steps)
        if len(normalized) < 2 or any(not isinstance(v, str) or not v for v in normalized):
            raise _INVALID.error(reason="funnel steps must contain distinct non-empty strings")
        if len(set(normalized)) != len(normalized):
            raise _INVALID.error(reason="funnel steps must be distinct")
        if within is None:
            within_seconds = None
        else:
            try:
                within_seconds = seconds(within)
            except (TypeError, ValueError) as exc:
                raise _INVALID.error(
                    reason="funnel within must be integral elapsed seconds"
                ) from exc
            if within_seconds <= 0:
                raise _INVALID.error(reason="funnel within must be positive")
        object.__setattr__(self, "event_property", event_property)
        object.__setattr__(self, "steps", normalized)
        object.__setattr__(self, "within_seconds", within_seconds)


@dataclass(frozen=True, slots=True, init=False)
class EventSequence:
    fact: str
    window: Window
    max_events: int
    representation: Literal["positions", "arrays"]
    properties: tuple[str, ...]
    label_property: str | None
    sessions: SessionSpec | None
    funnel: FunnelSpec | None

    def __init__(
        self,
        fact: str,
        window: Window,
        *,
        max_events: int,
        representation: Literal["positions", "arrays"] = "positions",
        properties: tuple[str, ...] = (),
        label_property: str | None = None,
        sessions: SessionSpec | None = None,
        funnel: FunnelSpec | None = None,
    ) -> None:
        fact = _name(fact, "sequence fact")
        if not isinstance(window, Window):
            raise _INVALID.error(reason="sequence window must be a Window")
        if isinstance(max_events, bool) or not isinstance(max_events, int):
            raise _INVALID.error(reason="max_events must be a positive signed-Int64")
        if not 0 < max_events <= _MAX:
            raise _INVALID.error(reason="max_events must be a positive signed-Int64")
        if representation not in {"positions", "arrays"}:
            raise _INVALID.error(reason="unsupported sequence representation")
        if not isinstance(properties, (tuple, list)):
            raise _INVALID.error(reason="properties must be an ordered sequence")
        properties = tuple(_name(v, "property") for v in properties)
        if len(set(properties)) != len(properties):
            raise _INVALID.error(reason="properties must be distinct")
        generated = {
            "unit",
            "anchor_id",
            "t0",
            "is_event",
            "sequence_position",
            "event_id",
            "event_time",
            "event_count",
            "sequence_complete",
            "sequence_reason",
            "event_values",
            "funnel_attempts",
            "funnel_attempts_complete",
            "funnel_attempts_reason",
        }
        if sessions is not None:
            generated.update(
                {
                    "session_index",
                    "session_event_count",
                    "session_start_time",
                    "session_end_time",
                    "previous_event_id",
                    "previous_event_time",
                    "transition_reason",
                    "transition",
                    "transition_from",
                    "transition_to",
                    "sessions",
                    "sessions_complete",
                    "sessions_reason",
                }
            )
        reason_names = {f"{name}_reason" for name in properties}
        if (
            generated.intersection(properties)
            or reason_names.intersection(properties)
            or reason_names.intersection(generated)
        ):
            raise _INVALID.error(
                reason="event property names collide with generated output columns"
            )
        if label_property is not None:
            label_property = _name(label_property, "label_property")
        if sessions is not None and not isinstance(sessions, SessionSpec):
            raise _INVALID.error(reason="sessions must be SessionSpec or None")
        if funnel is not None and not isinstance(funnel, FunnelSpec):
            raise _INVALID.error(reason="funnel must be FunnelSpec or None")
        object.__setattr__(self, "fact", fact)
        object.__setattr__(self, "window", window)
        object.__setattr__(self, "max_events", max_events)
        object.__setattr__(self, "representation", representation)
        object.__setattr__(self, "properties", properties)
        object.__setattr__(self, "label_property", label_property)
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "funnel", funnel)
