"""Typed requests for bounded event-sequence datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from terrine.errors import RefusalSpec
from terrine.windows import Window, duration

_UNSUPPORTED_SHAPE = RefusalSpec(
    "terrine.sequence.unsupported_shape",
    "The design cannot calculate this event-time request. "
    "Use a declared event and label, and a supported occurrence and time window.",
)
_INVALID = RefusalSpec("terrine.sequence.invalid_spec", "Invalid event sequence request: {reason}.")
_MAX = 2**63 - 1


def _name(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _INVALID.error(reason=f"{field} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True, init=False)
class SessionSpec:
    """Group a unit's consecutive events into sessions separated by more than ``gap``.

    Args:
        gap: Nonnegative duration string (see `terrine.windows`); "0s" groups only simultaneous
            events.

    Attributes:
        gap_seconds: Nonnegative threshold in exact integer seconds; zero groups only simultaneous
            events.

    Example: spec.SessionSpec(gap="30m")
    """

    gap_seconds: int

    def __init__(self, gap: str | Literal[0]) -> None:
        normalized = duration(gap, field="session_gap")
        if normalized < 0:
            raise _INVALID.error(reason="session gap must be non-negative")
        object.__setattr__(self, "gap_seconds", normalized)


@dataclass(frozen=True, slots=True, init=False)
class FunnelSpec:
    """Describe ordered event-label steps with an optional time limit.

    Args:
        event_property: Declared event label name whose values define funnel steps.
        steps: Ordered tuple of at least two distinct nonempty string label values to follow.
        within: Positive duration string (see `terrine.windows`); None imposes no funnel time limit.

    Attributes:
        event_property: Declared event label name whose values define funnel steps.
        steps: Ordered tuple of at least two distinct nonempty string label values to follow.
        within_seconds: Elapsed funnel time limit in integer seconds, or None for no limit.

    Example: spec.FunnelSpec("status", ("placed", "paid"), within="7d")
    """

    event_property: str
    steps: tuple[str, ...]
    within_seconds: int | None

    def __init__(
        self, event_property: str, steps: tuple[str, ...], within: str | Literal[0] | None = None
    ) -> None:
        event_property = _name(event_property, "funnel event_property")
        if not isinstance(steps, (tuple, list)):
            raise _INVALID.error(reason="funnel steps must be an ordered sequence")
        normalized = tuple(steps)
        if len(normalized) < 2 or any(not isinstance(v, str) or not v for v in normalized):
            raise _INVALID.error(reason="funnel steps must contain distinct non-empty strings")
        if len(set(normalized)) != len(normalized):
            raise _INVALID.error(reason="funnel steps must be distinct")
        within_seconds = None if within is None else duration(within, field="within")
        if within_seconds is not None and within_seconds <= 0:
            raise _INVALID.error(reason="funnel within must be positive")
        object.__setattr__(self, "event_property", event_property)
        object.__setattr__(self, "steps", normalized)
        object.__setattr__(self, "within_seconds", within_seconds)


@dataclass(frozen=True, slots=True, init=False)
class EventSequence:
    """Describe a bounded sequence of a unit's events within a window.

    Args:
        event: Name of an event declared in the model.
        window: Window built from duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        max_events: Positive integer maximum number of events retained per anchor.
        representation: "positions" emits event rows; "arrays" emits one bounded array row; default
            "positions".
        properties: Tuple of event property names to retain; () retains none.
        label_property: Declared label used for transition_from/transition_to when sessions are
            enabled; None records no transition labels. It does not filter events.
        sessions: SessionSpec grouping events by elapsed gaps; None does not assign sessions.
        funnel: FunnelSpec assigning ordered label steps; None does not assign funnel steps.

    Attributes:
        event: Name of an event declared in the model.
        window: Window built from duration string (see `terrine.windows`) offsets; start included,
            end excluded.
        max_events: Positive integer maximum number of events retained per anchor.
        representation: "positions" emits event rows; "arrays" emits one bounded array row; default
            "positions".
        properties: Tuple of event property names to retain; () retains none.
        label_property: Declared label used for transition_from/transition_to when sessions are
            enabled; None records no transition labels. It does not filter events.
        sessions: SessionSpec grouping events by elapsed gaps; None does not assign sessions.
        funnel: FunnelSpec assigning ordered label steps; None does not assign funnel steps.

    Example:
        from terrine.windows import Window

        spec.EventSequence("orders", Window("0s", "30d"), max_events=10)
    """

    event: str
    window: Window
    max_events: int
    representation: Literal["positions", "arrays"]
    properties: tuple[str, ...]
    label_property: str | None
    sessions: SessionSpec | None
    funnel: FunnelSpec | None

    def __init__(
        self,
        event: str,
        window: Window,
        *,
        max_events: int,
        representation: Literal["positions", "arrays"] = "positions",
        properties: tuple[str, ...] = (),
        label_property: str | None = None,
        sessions: SessionSpec | None = None,
        funnel: FunnelSpec | None = None,
    ) -> None:
        event = _name(event, "sequence event")
        if not isinstance(window, Window):
            raise _INVALID.error(reason="sequence window must be a Window")
        if isinstance(max_events, bool) or not isinstance(max_events, int):
            raise _INVALID.error(reason="max_events must be a positive signed-Int64")
        if not 0 < max_events <= _MAX:
            raise _INVALID.error(reason="max_events must be a positive signed-Int64")
        if representation not in ("positions", "arrays"):
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
        object.__setattr__(self, "event", event)
        object.__setattr__(self, "window", window)
        object.__setattr__(self, "max_events", max_events)
        object.__setattr__(self, "representation", representation)
        object.__setattr__(self, "properties", properties)
        object.__setattr__(self, "label_property", label_property)
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "funnel", funnel)
