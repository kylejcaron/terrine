"""Independent integer references for the version-2 synthetic observation contract."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from terrine.simulate.scale import ScaleConfig

_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)


def _daily(config: ScaleConfig, unit: int, corrected: bool):
    histograms: dict[int, Counter[int]] = {}
    events: Counter[int] = Counter()
    for index in range(config.events_per_unit):
        if corrected and index % 13 == 0:
            continue
        day = ((index // 2) * (1 + unit % 3)) % config.span_days
        events[day] += 1
        value = (
            None
            if (config.seed + unit * 31 + index * 17) % 11 == 0
            else ((config.seed % 101 + unit * 17 + (index // 2) * 13) % 101 - 50)
        )
        if corrected and index % 7 == 0:
            value = 50 if value is None else -value
        if value is not None:
            histograms.setdefault(day, Counter())[value] += 1
    return events, histograms


def _quantile(counts: Counter[int]) -> Decimal | None:
    n = sum(counts.values())
    if n == 0:
        return None
    lower, fraction = divmod(n - 1, 4)
    seen = 0
    left = None
    for value, count in sorted(counts.items()):
        if seen <= lower < seen + count:
            left = value
        if seen <= min(lower + 1, n - 1) < seen + count:
            assert left is not None
            return Decimal((4 - fraction) * left + fraction * value) / Decimal(4)
        seen += count
    raise AssertionError("histogram ranks were not covered")


def metric_rows(
    config: ScaleConfig, case: str, *, corrected: bool = False
) -> Iterator[dict[str, object]]:
    """Yield expected rows without retaining source events or compiler objects."""
    if config.version != 2:
        raise ValueError("metric oracle requires generator version 2")
    if case not in {"frame", "schedule", "sequential", "event_panel", "sketch"}:
        raise ValueError("unknown benchmark case")
    for unit in range(config.units):
        events, daily = _daily(config, unit, corrected)
        if case == "event_panel":
            if not events:
                continue
            days = range(min(events), min(events) + 3)
        elif case in {"schedule", "sequential"}:
            days = range(config.window_days, config.window_days + config.anchor_periods)
        else:
            days = (config.window_days,)
        for day in days:
            counts: Counter[int] = Counter()
            event_count = 0
            for selected in range(day - config.window_days, day):
                counts.update(daily.get(selected, {}))
                event_count += events[selected]
            nonnull = sum(counts.values())
            yield {
                "unit": f"unit-{unit:0{len(str(max(999999, config.units - 1)))}d}",
                "t0": _EPOCH + timedelta(days=day),
                "minimum": min(counts) if counts else None,
                "maximum": max(counts) if counts else None,
                "distinct": len(counts),
                "q25": _quantile(counts),
                "total": sum(value * count for value, count in counts.items())
                if counts
                else (0 if event_count == 0 else None),
                "reason": "no_observations"
                if event_count == 0
                else ("all_values_null" if nonnull == 0 else None),
                "treatment": unit % 2 == 0 and not corrected,
            }
