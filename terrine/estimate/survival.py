"""Kaplan–Meier estimation over materialized Arrow rows only."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    from terrine.data import MaterializedFrame

__all__ = ["ElapsedTime", "SurvivalCurve", "kaplan_meier"]

_INVALID = RefusalSpec(
    "terrine.estimate.invalid_survival_data",
    "Survival input is invalid ({reason}).",
)


@dataclass(frozen=True, slots=True, order=True)
class ElapsedTime:
    """Floor-normalized exact elapsed time from the row's analysis origin."""

    seconds: int
    nanoseconds: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.seconds, bool) or not isinstance(self.seconds, int):
            raise TypeError("seconds must be an integer")
        if isinstance(self.nanoseconds, bool) or not isinstance(self.nanoseconds, int):
            raise TypeError("nanoseconds must be an integer")
        if not 0 <= self.nanoseconds < 1_000_000_000:
            raise ValueError("nanoseconds must be normalized")


@dataclass(frozen=True, slots=True)
class SurvivalCurve:
    time: tuple[ElapsedTime, ...]
    at_risk: tuple[int, ...]
    events: tuple[int, ...]
    censored: tuple[int, ...]
    survival: tuple[float, ...]
    variance: tuple[float | None, ...]


def _read_elapsed(value: object, *, row: int, name: str) -> ElapsedTime:
    if not isinstance(value, dict) or set(value) != {"seconds", "nanoseconds"}:
        raise _INVALID.error(reason=f"{name}_must_be_elapsed_struct_at_row_{row}")
    seconds, nanos = value["seconds"], value["nanoseconds"]
    try:
        return ElapsedTime(seconds, nanos)
    except (TypeError, ValueError):
        raise _INVALID.error(reason=f"{name}_must_be_normalized_at_row_{row}") from None


def kaplan_meier(
    data: MaterializedFrame, *, duration: str, event: str, entry: str | None = None
) -> SurvivalCurve:
    """Compute product-limit survival and Greenwood variance from Arrow values."""
    import pyarrow as pa

    from terrine.data import MaterializedFrame

    if not isinstance(data, MaterializedFrame):
        raise _INVALID.error(reason="materialized_frame_required")
    table = data.table
    for name in (duration, event, *((entry,) if entry is not None else ())):
        if name not in table.column_names:
            raise _INVALID.error(reason=f"missing_column_{name}")
    unit_name = data.roles.unit
    units = table[unit_name].to_pylist()
    if any(unit is None for unit in units) or len(set(units)) != len(units):
        raise _INVALID.error(reason="unit_identity_must_be_unique_and_nonnull")
    metadata = data.columns.get(duration)
    complete_column = metadata.complete_column if metadata is not None else None
    if complete_column is not None and complete_column not in table.column_names:
        raise _INVALID.error(reason="missing_followup_completeness_column")
    complete_values = table[complete_column].to_pylist() if complete_column else None
    timestamp_duration = pa.types.is_timestamp(table.schema.field(duration).type)
    timestamp_entry = entry is not None and pa.types.is_timestamp(table.schema.field(entry).type)
    if timestamp_duration or timestamp_entry:
        origin_name = data.roles.t0
        if origin_name not in table.column_names or not pa.types.is_timestamp(
            table.schema.field(origin_name).type
        ):
            raise _INVALID.error(reason="timestamp_times_require_timestamp_t0_role")
    duration_values = None if timestamp_duration else table[duration].to_pylist()
    entry_values = None if entry is None or timestamp_entry else table[entry].to_pylist()
    event_values = table[event].to_pylist()
    observations: list[tuple[ElapsedTime, ElapsedTime, bool]] = []
    for index in range(table.num_rows):
        if complete_values is not None and complete_values[index] is not True:
            continue
        origin = table[origin_name][index] if timestamp_duration or timestamp_entry else None
        if timestamp_duration:
            end = _timestamp_elapsed(table[duration][index], origin, row=index, name=duration)
        else:
            assert duration_values is not None
            end = _read_elapsed(duration_values[index], row=index, name=duration)
        if entry is None:
            entered = ElapsedTime(0)
        elif timestamp_entry:
            entered = _timestamp_elapsed(table[entry][index], origin, row=index, name=entry)
        else:
            assert entry_values is not None
            entered = _read_elapsed(entry_values[index], row=index, name=entry)
        failed = event_values[index]
        if not isinstance(failed, bool):
            raise _INVALID.error(reason=f"event_must_be_boolean_at_row_{index}")
        if entered > end:
            raise _INVALID.error(reason=f"entry_after_exit_at_row_{index}")
        observations.append((entered, end, failed))

    from collections import Counter

    entry_counts: Counter[ElapsedTime] = Counter()
    event_counts: Counter[ElapsedTime] = Counter()
    censor_counts: Counter[ElapsedTime] = Counter()
    for entered, end, failed in observations:
        entry_counts[entered] += 1
        (event_counts if failed else censor_counts)[end] += 1
    instants = sorted(event_counts.keys() | censor_counts.keys())
    entry_times = sorted(entry_counts)
    entry_index = 0
    active = 0
    at_risk: list[int] = []
    events: list[int] = []
    censored: list[int] = []
    survival: list[float] = []
    variance: list[float | None] = []
    estimate = 1.0
    greenwood = 0.0
    variance_unavailable = False
    for instant in instants:
        while entry_index < len(entry_times) and entry_times[entry_index] <= instant:
            active += entry_counts[entry_times[entry_index]]
            entry_index += 1
        risk = active
        failures = event_counts[instant]
        censorings = censor_counts[instant]
        if risk <= 0 or failures > risk or censorings > risk - failures:
            raise _INVALID.error(reason="impossible_risk_set")
        if failures:
            estimate *= 1.0 - failures / risk
            if estimate == 0.0:
                variance_unavailable = True
            elif not variance_unavailable:
                greenwood += failures / (risk * (risk - failures))
        at_risk.append(risk)
        events.append(failures)
        censored.append(censorings)
        survival.append(estimate)
        variance.append(None if variance_unavailable else estimate * estimate * greenwood)
        active -= failures + censorings
    return SurvivalCurve(
        tuple(instants),
        tuple(at_risk),
        tuple(events),
        tuple(censored),
        tuple(survival),
        tuple(variance),
    )


def _timestamp_elapsed(value, origin, *, row: int, name: str) -> ElapsedTime:
    import pyarrow as pa

    if (
        not isinstance(value, pa.TimestampScalar)
        or not isinstance(origin, pa.TimestampScalar)
        or not value.is_valid
        or not origin.is_valid
    ):
        raise _INVALID.error(reason=f"{name}_must_be_nonnull_timestamp_at_row_{row}")
    if value.type.tz != "UTC" or origin.type.tz != "UTC":
        raise _INVALID.error(reason=f"{name}_timestamp_must_be_utc_at_row_{row}")
    factors = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
    delta = value.value * factors[value.type.unit] - origin.value * factors[origin.type.unit]
    seconds, nanos = divmod(delta, 1_000_000_000)
    return ElapsedTime(seconds, nanos)
