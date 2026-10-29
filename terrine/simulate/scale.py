"""Bounded deterministic synthetic dataset-scale observations and independent counts."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from terrine.errors import RefusalSpec

__all__ = [
    "ScaleConfig",
    "anchor_batches",
    "correction_batches",
    "event_batches",
    "oracle_batches",
    "unit_batches",
]

_INVALID_CONFIG = RefusalSpec(
    "terrine.simulate.invalid_config", "Simulation input {field} is invalid: {reason}."
)
_INVALID_SEED = RefusalSpec("terrine.simulate.invalid_seed", "Use an integer seed in [0, 2**64).")
_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_GENERATOR_VERSION = 2
_TIMESTAMP = pa.timestamp("us", tz="UTC")
_EVENT_SCHEMA = pa.schema(
    [
        ("unit_id", pa.string()),
        ("event_id", pa.string()),
        ("occurrence_id", pa.string()),
        ("event_time", _TIMESTAMP),
        ("event_type", pa.string()),
        ("value", pa.int64()),
        ("adopted", pa.string()),
        ("revision", pa.int64()),
    ]
)


@dataclass(frozen=True, slots=True)
class ScaleConfig:
    """Versioned keyed streams; chunking and unit partition order do not alter observations."""

    units: int
    events_per_unit: int
    batch_size: int
    seed: int = 0
    version: int = _GENERATOR_VERSION
    span_days: int = 30
    anchor_periods: int = 4
    window_days: int = 7

    def __post_init__(self) -> None:
        for name in (
            "units",
            "events_per_unit",
            "batch_size",
            "span_days",
            "anchor_periods",
            "window_days",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise _INVALID_CONFIG.error(reason="not_positive_integer", field=name)
        if type(self.seed) is not int:
            raise _INVALID_SEED.error(reason="wrong_type", field="seed")
        if type(self.version) is not int or self.version != _GENERATOR_VERSION:
            raise _INVALID_CONFIG.error(reason="unsupported_version", field="version")
        try:
            _ = self.coverage_start
        except OverflowError as exc:
            raise _INVALID_CONFIG.error(reason="out_of_range", field="window_days") from exc
        try:
            _ = self.coverage_end
        except OverflowError as exc:
            raise _INVALID_CONFIG.error(reason="out_of_range", field=self._end_driver()) from exc

    def _end_driver(self) -> str:
        """The argument whose value sets ``coverage_end``."""
        if self.span_days >= self.window_days + self.anchor_periods + 3:
            return "span_days"
        return "anchor_periods" if self.anchor_periods >= self.window_days else "window_days"

    @property
    def coverage_start(self) -> datetime:
        return _EPOCH - timedelta(days=self.window_days + 3)

    @property
    def coverage_end(self) -> datetime:
        return _EPOCH + timedelta(
            days=max(self.span_days, self.window_days + self.anchor_periods + 3) + 3
        )

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _units(config: ScaleConfig, start: int, stop: int | None) -> range:
    if not isinstance(config, ScaleConfig):
        raise _INVALID_CONFIG.error(reason="wrong_type", field="config")
    stop = config.units if stop is None else stop
    if type(start) is not int or not 0 <= start <= config.units:
        raise _INVALID_CONFIG.error(reason="out_of_range", field="unit_start")
    if type(stop) is not int or not start <= stop <= config.units:
        raise _INVALID_CONFIG.error(reason="out_of_range", field="unit_stop")
    return range(start, stop)


def _batches[Value](
    rows: Iterator[dict[str, Value]], schema: pa.Schema, size: int
) -> Iterator[pa.Table]:
    pending = []
    for row in rows:
        pending.append(row)
        if len(pending) == size:
            batch = pa.Table.from_pylist(pending, schema=schema)
            pending = []
            yield batch
    if pending:
        yield pa.Table.from_pylist(pending, schema=schema)


def _unit_key(config: ScaleConfig, unit: int) -> str:
    """Keep lexical unit order equal to the independent ordinal reference order."""
    return f"{unit:0{max(6, len(str(config.units - 1)))}d}"


def _event(config: ScaleConfig, unit: int, index: int) -> dict[str, object]:
    channel = config.seed + unit * 31 + index * 17
    pair = index // 2
    key = _unit_key(config, unit)
    return {
        "unit_id": f"unit-{key}",
        "event_id": f"event-{key}-{index:09d}",
        "occurrence_id": f"occurrence-{key}-{pair % max(1, config.span_days // 2):06d}",
        "event_time": _EPOCH + timedelta(days=(pair * (1 + unit % 3)) % config.span_days),
        "event_type": None if channel % 19 == 0 else ("target" if channel % 5 == 0 else "other"),
        "value": None
        if channel % 11 == 0
        else ((config.seed % 101 + unit * 17 + pair * 13) % 101 - 50),
        "adopted": "adopted" if unit % 2 == 0 and index == 0 else None,
        "revision": 1,
    }


def event_batches(
    config: ScaleConfig, *, unit_start: int = 0, unit_stop: int | None = None
) -> Iterator[pa.Table]:
    """Yield raw observations directly from keyed positions, retaining at most one row batch."""
    indices = _units(config, unit_start, unit_stop)
    rows = (
        _event(config, unit, index) for unit in indices for index in range(config.events_per_unit)
    )
    yield from _batches(rows, _EVENT_SCHEMA, config.batch_size)


def correction_batches(
    config: ScaleConfig, *, unit_start: int = 0, unit_stop: int | None = None
) -> Iterator[pa.Table]:
    """Yield declared updates/deletions by stable physical event ID."""
    indices = _units(config, unit_start, unit_stop)

    def rows():
        for unit in indices:
            for index in range(config.events_per_unit):
                if index % 13 != 0 and index % 7 != 0:
                    continue
                row = _event(config, unit, index)
                row["change_kind"] = "delete" if index % 13 == 0 else "update"
                if row["change_kind"] == "update":
                    value = row["value"]
                    assert value is None or isinstance(value, int)
                    row["value"] = 50 if value is None else -value
                row["revision"] = 2
                yield row

    yield from _batches(
        rows(), _EVENT_SCHEMA.append(pa.field("change_kind", pa.string())), config.batch_size
    )


def anchor_batches(
    config: ScaleConfig, *, unit_start: int = 0, unit_stop: int | None = None
) -> Iterator[pa.Table]:
    """Yield the declared schedule without deriving anchors from observed events."""
    indices = _units(config, unit_start, unit_stop)
    rows = (
        {
            "unit_id": f"unit-{_unit_key(config, unit)}",
            "anchor_index": period,
            "anchor_time": _EPOCH + timedelta(days=config.window_days + period),
        }
        for unit in indices
        for period in range(config.anchor_periods)
    )
    schema = pa.schema(
        [("unit_id", pa.string()), ("anchor_index", pa.int64()), ("anchor_time", _TIMESTAMP)]
    )
    yield from _batches(rows, schema, config.batch_size)


def unit_batches(
    config: ScaleConfig, *, unit_start: int = 0, unit_stop: int | None = None
) -> Iterator[pa.Table]:
    """Yield roster observation intervals separately from event presence."""
    indices = _units(config, unit_start, unit_stop)
    start, end = config.coverage_start, config.coverage_end
    rows = (
        {
            "unit_id": f"unit-{_unit_key(config, unit)}",
            "observed_from": start,
            "observed_until": end,
        }
        for unit in indices
    )
    schema = pa.schema(
        [("unit_id", pa.string()), ("observed_from", _TIMESTAMP), ("observed_until", _TIMESTAMP)]
    )
    yield from _batches(rows, schema, config.batch_size)


def oracle_batches(
    config: ScaleConfig,
    *,
    unit_start: int = 0,
    unit_stop: int | None = None,
    corrected: bool = False,
) -> Iterator[pa.Table]:
    """Yield separate expected target counts without reading generated observation batches."""
    indices = _units(config, unit_start, unit_stop)
    if type(corrected) is not bool:
        raise _INVALID_CONFIG.error(reason="wrong_type", field="corrected")

    def rows():
        for unit in indices:
            count = sum(
                (config.seed + unit * 31 + index * 17) % 5 == 0
                and (config.seed + unit * 31 + index * 17) % 19 != 0
                and not (corrected and index % 13 == 0)
                for index in range(config.events_per_unit)
            )
            yield {"unit_id": f"unit-{_unit_key(config, unit)}", "target_event_count": count}

    yield from _batches(
        rows(),
        pa.schema([("unit_id", pa.string()), ("target_event_count", pa.int64())]),
        config.batch_size,
    )
