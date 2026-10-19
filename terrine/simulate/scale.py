"""Bounded deterministic synthetic dataset-scale workloads."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa

__all__ = ["ScaleConfig", "event_batches", "oracle_batches"]

_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_GENERATOR_VERSION = 1


@dataclass(frozen=True, slots=True)
class ScaleConfig:
    """Reproducible event workload parameters; batches bound retained rows."""

    units: int
    events_per_unit: int
    batch_size: int
    seed: int = 0
    version: int = _GENERATOR_VERSION

    def __post_init__(self) -> None:
        for name in ("units", "events_per_unit", "batch_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValueError("seed must be an integer")
        if self.version != _GENERATOR_VERSION:
            raise ValueError(f"unsupported scale generator version {self.version}")

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def event_batches(config: ScaleConfig) -> Iterator[pa.Table]:
    """Yield observation batches without constructing the full event world."""
    if not isinstance(config, ScaleConfig):
        raise TypeError("config must be ScaleConfig")
    rows: list[dict[str, object]] = []
    for unit_index in range(config.units):
        unit_id = f"unit-{unit_index:06d}"
        for event_index in range(config.events_per_unit):
            target = (config.seed + unit_index * 31 + event_index * 17) % 5 == 0
            rows.append(
                {
                    "unit_id": unit_id,
                    "event_id": f"event-{unit_index:06d}-{event_index:09d}",
                    "event_time": _EPOCH + timedelta(seconds=event_index // 2),
                    "event_type": "target" if target else "other",
                }
            )
            if len(rows) == config.batch_size:
                yield _event_table(rows)
                rows = []
    if rows:
        yield _event_table(rows)


def oracle_batches(config: ScaleConfig) -> Iterator[pa.Table]:
    """Yield independent expected target-occurrence counts by unit."""
    if not isinstance(config, ScaleConfig):
        raise TypeError("config must be ScaleConfig")
    rows: list[dict[str, object]] = []
    for unit_index in range(config.units):
        target_count = sum(
            (config.seed + unit_index * 31 + event_index * 17) % 5 == 0
            for event_index in range(config.events_per_unit)
        )
        rows.append({"unit_id": f"unit-{unit_index:06d}", "target_event_count": target_count})
        if len(rows) == config.batch_size:
            yield pa.Table.from_pylist(rows)
            rows = []
    if rows:
        yield pa.Table.from_pylist(rows)


def _event_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                pa.field("unit_id", pa.string()),
                pa.field("event_id", pa.string()),
                pa.field("event_time", pa.timestamp("us", tz="UTC")),
                pa.field("event_type", pa.string()),
            ]
        ),
    )
