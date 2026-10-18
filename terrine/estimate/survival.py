"""Arrow-only survival estimation, earlier-only backtests and surrogate export."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    import pyarrow as pa

    from terrine.data import MaterializedFrame

__all__ = [
    "ArrowSurrogate",
    "ElapsedTime",
    "SurvivalCurve",
    "backtest_survival",
    "kaplan_meier",
    "to_surrogate_arrow",
]

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


_SURROGATE_METADATA_KEY = b"terrine.surrogate_arrow.v1"


@dataclass(frozen=True, slots=True)
class ArrowSurrogate:
    """Versioned Arrow surrogate projection and portable metadata."""

    data: pa.Table
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        import pyarrow as pa

        if not isinstance(self.data, pa.Table):
            raise TypeError("data must be a pyarrow.Table")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @classmethod
    def from_arrow(cls, table: pa.Table) -> ArrowSurrogate:

        raw = (table.schema.metadata or {}).get(_SURROGATE_METADATA_KEY)
        if raw is None:
            raise ValueError("Arrow schema has no Terrine surrogate metadata")
        metadata = json.loads(raw.decode("utf-8"))
        if metadata.get("schema_version") != 1:
            raise ValueError("unsupported Terrine surrogate metadata version")
        clean = dict(table.schema.metadata or {})
        del clean[_SURROGATE_METADATA_KEY]
        return cls(table.replace_schema_metadata(clean or None), metadata)

    def to_arrow(self) -> pa.Table:
        metadata = json.dumps(
            dict(self.metadata), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        schema_metadata = dict(self.data.schema.metadata or {})
        schema_metadata[_SURROGATE_METADATA_KEY] = metadata
        return self.data.replace_schema_metadata(schema_metadata)


def _utc_timestamp_type(table, name: str):
    import pyarrow as pa

    if name not in table.column_names:
        raise _INVALID.error(reason=f"missing_column_{name}")
    dtype = table.schema.field(name).type
    if not pa.types.is_timestamp(dtype) or dtype.tz != "UTC":
        raise _INVALID.error(reason=f"{name}_must_be_utc_timestamp")
    return dtype


def backtest_survival(
    data: MaterializedFrame,
    *,
    duration: str,
    event: str,
    entry: str,
    prediction_cutoffs: Sequence[datetime],
    fitter: Callable[[MaterializedFrame], object],
    knowledge: Literal["measurement_time", "recorded_at"] = "measurement_time",
) -> tuple[object, ...]:
    """Refit on pre-cutoff entrants with follow-up censored at each cutoff."""
    import pyarrow as pa

    from terrine.data import MaterializedFrame

    if not isinstance(data, MaterializedFrame):
        raise _INVALID.error(reason="materialized_frame_required")
    if knowledge not in ("measurement_time", "recorded_at"):
        raise _INVALID.error(reason="unsupported_knowledge_mode")
    if knowledge == "recorded_at":
        # Effective-time cutoffs and feature declarations are not source history.
        raise _INVALID.error(reason="recorded_at_availability_not_proven")
    if not callable(fitter):
        raise _INVALID.error(reason="fitter_must_be_callable")
    table = data.table
    duration_type = _utc_timestamp_type(table, duration)
    _utc_timestamp_type(table, entry)
    origin_type = _utc_timestamp_type(table, data.roles.t0)
    if event not in table.column_names:
        raise _INVALID.error(reason=f"missing_column_{event}")
    if table.schema.field(event).type != pa.bool_():
        raise _INVALID.error(reason="event_must_be_boolean")
    cutoffs = tuple(prediction_cutoffs)
    if any(
        not isinstance(cutoff, datetime) or cutoff.tzinfo is None or cutoff.utcoffset() is None
        for cutoff in cutoffs
    ):
        raise _INVALID.error(reason="cutoffs_must_be_aware")
    normalized = tuple(cutoff.astimezone(UTC) for cutoff in cutoffs)
    if any(left >= right for left, right in zip(normalized, normalized[1:], strict=False)):
        raise _INVALID.error(reason="cutoffs_must_be_strictly_increasing")
    results: list[object] = []
    durations = table[duration].combine_chunks()
    entries = table[entry].combine_chunks()
    origins = table[data.roles.t0].combine_chunks()
    events = table[event].to_pylist()
    for cutoff in normalized:
        kept: list[int] = []
        observed_duration: list[int] = []
        observed_event: list[bool] = []
        for index in range(table.num_rows):
            origin = origins[index]
            entered = entries[index]
            ended = durations[index]
            if not origin.is_valid or not entered.is_valid or not ended.is_valid:
                raise _INVALID.error(reason=f"null_survival_time_at_row_{index}")
            cutoff_scalar = pa.scalar(cutoff, type=origin_type)
            cutoff_elapsed = _timestamp_elapsed(cutoff_scalar, origin, row=index, name="cutoff")
            entry_elapsed = _timestamp_elapsed(entered, origin, row=index, name=entry)
            if entry_elapsed >= cutoff_elapsed:
                continue
            if not isinstance(events[index], bool):
                raise _INVALID.error(reason=f"event_must_be_boolean_at_row_{index}")
            end_elapsed = _timestamp_elapsed(ended, origin, row=index, name=duration)
            kept.append(index)
            if end_elapsed < cutoff_elapsed:
                observed_duration.append(ended.cast(pa.int64()).as_py())
                observed_event.append(events[index] is True)
            else:
                observed_duration.append(cutoff_scalar.cast(duration_type).cast(pa.int64()).as_py())
                observed_event.append(False)
        sample = table.take(pa.array(kept, type=pa.int64()))
        sample = sample.set_column(
            sample.schema.get_field_index(duration),
            duration,
            pa.array(observed_duration, type=duration_type),
        )
        sample = sample.set_column(
            sample.schema.get_field_index(event),
            event,
            pa.array(observed_event, type=pa.bool_()),
        )
        provenance = replace(data.provenance, measurement_knowledge="retrospective_effective_time")
        results.append(fitter(MaterializedFrame(sample, data.roles, data.columns, provenance)))
    return tuple(results)


def to_surrogate_arrow(
    data: MaterializedFrame, *, features: tuple[str, ...], outcomes: tuple[str, ...]
) -> ArrowSurrogate:
    """Project Arrow training data with identity, missingness and provenance."""
    from terrine.data import MaterializedFrame

    if not isinstance(data, MaterializedFrame):
        raise _INVALID.error(reason="materialized_frame_required")
    requested = (*features, *outcomes)
    if len(set(requested)) != len(requested):
        raise _INVALID.error(reason="duplicate_surrogate_column")
    if any(name not in data.table.column_names for name in requested):
        raise _INVALID.error(reason="missing_surrogate_column")
    identity = (data.roles.unit, data.roles.t0)
    support = []
    for name in requested:
        meta = data.columns.get(name)
        if meta is not None:
            support.extend(
                column
                for column in (
                    meta.observed_from_column,
                    meta.observed_until_column,
                    meta.complete_column,
                    meta.reason_column,
                    meta.absolute_error_column,
                )
                if column is not None
            )
    retained = tuple(dict.fromkeys((*identity, *requested, *data.roles.outcomes, *support)))
    missing = [name for name in retained if name not in data.table.column_names]
    if missing:
        raise _INVALID.error(reason=f"missing_surrogate_support_{missing[0]}")
    projected = data.table.select(retained)
    metadata = {
        "schema_version": 1,
        "design_hash": data.provenance.design_hash,
        "model_hash": data.provenance.model_hash,
        "source_generations": dict(data.provenance.source_generations),
        "execution_snapshot": data.to_metadata()["provenance"]["execution_snapshot"],
        "provenance": data.to_metadata()["provenance"],
        "roles": data.to_metadata()["roles"],
        "feature_columns": list(features),
        "outcome_columns": list(outcomes),
        "support_columns": list(support),
        "administrative_cutoff": (
            data.provenance.administrative_cutoff.isoformat()
            if data.provenance.administrative_cutoff is not None
            else None
        ),
        "cohort_operation": data.to_metadata()["provenance"].get("cohort_operation"),
    }
    return ArrowSurrogate(projected, metadata)
