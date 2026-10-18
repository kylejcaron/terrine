from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

__all__ = ["Change", "ChangeSet", "affected_by_change", "window_intersects"]

ChangeKind = Literal[
    "insert", "update", "delete", "restatement", "readiness", "definition", "roster"
]
ChangeEvidence = Literal["revision", "change_feed", "checksum", "operator", "conservative_rebuild"]
TimeRange = tuple[datetime, datetime]
KeyRange = tuple[str, str]


@dataclass(frozen=True, slots=True)
class Change:
    kind: ChangeKind
    source: str
    before_key_range: KeyRange | None = None
    after_key_range: KeyRange | None = None
    before_time_range: TimeRange | None = None
    after_time_range: TimeRange | None = None
    revision: str | None = None
    tombstone: bool = False

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("change source must not be empty")
        if self.kind == "update" and (
            (self.before_key_range is None and self.before_time_range is None)
            or (self.after_key_range is None and self.after_time_range is None)
        ):
            raise ValueError("update requires both old and new support")
        if (
            self.kind == "delete"
            and self.before_key_range is None
            and self.before_time_range is None
        ):
            raise ValueError("delete requires previous support")


@dataclass(frozen=True, slots=True)
class ChangeSet:
    changes: tuple[Change, ...]
    evidence: ChangeEvidence

    @classmethod
    def from_readiness_advance(cls, source: str, before: datetime, after: datetime) -> ChangeSet:
        if after < before:
            return cls(
                (Change("readiness", source, before_time_range=(after, before)),), "revision"
            )
        # Advancing completeness is not evidence that source rows changed.
        return cls((), "revision")


def window_intersects(support: TimeRange, window: TimeRange) -> bool:
    left, right = support
    start, end = window
    if left.tzinfo is None or right.tzinfo is None or start.tzinfo is None or end.tzinfo is None:
        raise ValueError("time supports must be timezone-aware")
    if right < left or end < start:
        raise ValueError("time ranges must be ordered")
    return left < end and start < right


def affected_by_change(change: Change, window: TimeRange) -> bool:
    supports = (change.before_time_range, change.after_time_range)
    if any(support is None for support in supports if change.kind == "update"):
        return True
    if change.kind == "delete":
        if change.before_time_range is None:
            return True
        return window_intersects(change.before_time_range, window)
    exact = tuple(support for support in supports if support is not None)
    if not exact:
        return True
    return any(window_intersects(support, window) for support in exact)
