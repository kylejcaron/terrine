from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from terrine.runtime.state import GenerationRef, RuntimeStore

__all__ = ["GCReport", "collect_superseded"]


@dataclass(frozen=True, slots=True)
class GCReport:
    candidates: tuple[GenerationRef, ...]
    removed: tuple[GenerationRef, ...]
    dry_run: bool


def collect_superseded(
    store: RuntimeStore,
    *,
    retain: timedelta,
    now: datetime,
    dry_run: bool = True,
) -> GCReport:
    if retain < timedelta(0):
        raise ValueError("retention must not be negative")
    if now.tzinfo is None:
        raise ValueError("GC clock must be timezone-aware")
    eligible = tuple(
        record.ref
        for record in store.generations()
        if record.superseded_by is not None and now - record.created_at >= retain
    )
    if dry_run:
        return GCReport(eligible, (), True)
    removed: list[GenerationRef] = []
    for ref in eligible:
        try:
            store.delete_generation(ref)
        except ValueError:
            continue
        removed.append(ref)
    return GCReport(eligible, tuple(removed), False)
