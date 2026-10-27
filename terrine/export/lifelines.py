"""Map observed survival records and descriptive risk counts to lifelines inputs."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from types import MappingProxyType
from typing import Any

import numpy as np
import pyarrow as pa

from terrine.data import MaterializedFrame
from terrine.errors import RefusalSpec
from terrine.export.preflight import _INCOMPLETE_OUTCOME, ExportPreflight, preflight_export

__all__ = ["LifelinesKM", "LifelinesCox", "to_lifelines_km", "to_lifelines_cox"]

_INVALID = RefusalSpec(
    "terrine.lifelines.invalid_input",
    "Invalid lifelines input ({reason}); supply complete, finite time-to-event records.",
)
_SHAPE = RefusalSpec(
    "terrine.lifelines.unsupported_shape",
    "This survival input cannot support the requested conversion ({reason}); use {route}.",
)
_DELAYED_ENTRY = RefusalSpec(
    "terrine.lifelines.delayed_entry",
    "Risk counts cannot recover delayed entry times; use {route}.",
)
_MISSING_DEPENDENCY = RefusalSpec(
    "terrine.convert.missing_dependency",
    "Conversion requires the optional {extra!r} dependency; install terrine[{extra}].",
)
_ROUTE = "per-unit time-to-event records"
_NS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


@dataclass(frozen=True, slots=True)
class LifelinesKM:
    """KM fit arguments; seconds for records, bin indices for risk counts."""

    durations: np.ndarray
    event_observed: np.ndarray
    entry: np.ndarray | None
    weights: np.ndarray | None
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


@dataclass(frozen=True, slots=True)
class LifelinesCox:
    """Feature-only Cox dataframe and its explicitly assigned fit columns."""

    df: Any
    duration_col: str
    event_col: str
    entry_col: str
    weights_col: str | None
    cluster_col: str | None
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


def _required(table: pa.Table, names) -> None:
    missing = sorted(set(names) - set(table.column_names))
    if missing:
        raise _INVALID.error(reason="missing_columns", columns=missing)


def _nanoseconds(table: pa.Table, name: str) -> list[int]:
    column = table[name]
    if not pa.types.is_timestamp(column.type) or column.null_count:
        raise _INVALID.error(reason="timestamp_required", column=name)
    multiplier = _NS_PER_UNIT[column.type.unit]
    return [value * multiplier for value in column.cast(pa.int64()).to_pylist()]

def _duration_nanoseconds(table: pa.Table, name: str) -> list[int]:
    column = table[name]
    if not pa.types.is_duration(column.type) or column.null_count:
        raise _INVALID.error(reason="duration_required", column=name)
    multiplier = _NS_PER_UNIT[column.type.unit]
    return [value * multiplier for value in column.cast(pa.int64()).to_pylist()]


def _record_arrays(data: MaterializedFrame, checked: ExportPreflight) -> LifelinesKM:
    if data.provenance.shape != "time_to_event":
        raise _SHAPE.error(reason="time_to_event_required", route=_ROUTE)
    table, roles = checked.table, data.roles
    _required(table, [roles.t0, "duration", "entry_time", "event"])
    origin = _nanoseconds(table, roles.t0)
    duration = _duration_nanoseconds(table, "duration")
    entered = _nanoseconds(table, "entry_time")
    durations = np.array([value / 1e9 for value in duration])
    entry = np.array([(end - start) / 1e9 for start, end in zip(origin, entered, strict=True)])
    if not len(durations) or np.any(entry < 0) or np.any(durations < entry):
        raise _INVALID.error(reason="entry_duration_order")
    events = table["event"].to_pylist()
    if any(value not in (True, False, 0, 1) or value is None for value in events):
        raise _INVALID.error(reason="binary_event_required")
    weights = None
    if roles.weight is not None:
        _required(table, [roles.weight])
        values = table[roles.weight].to_pylist()
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
            for value in values
        ):
            raise _INVALID.error(reason="positive_finite_weights_required")
        weights = np.asarray(values)
    return LifelinesKM(
        durations, np.asarray(events, dtype=bool), entry, weights, checked.excluded_counts
    )


def _risk_arrays(data: MaterializedFrame, checked: ExportPreflight) -> LifelinesKM:
    if checked.excluded_counts:
        raise _INCOMPLETE_OUTCOME.error(excluded_counts=dict(checked.excluded_counts), route=_ROUTE)
    table = checked.table
    _required(table, [data.roles.unit, "time_bin", "events", "censored", "delayed_entries"])
    if table.num_rows == 0 or len(set(table[data.roles.unit].to_pylist())) != 1:
        raise _SHAPE.error(reason="one_risk_group_required", route=_ROUTE)
    for name in ("events", "censored", "delayed_entries", "time_bin"):
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in table[name].to_pylist()
        ):
            raise _INVALID.error(reason="nonnegative_counts_required", column=name)
    if any(table["delayed_entries"].to_pylist()):
        raise _DELAYED_ENTRY.error(route=_ROUTE)
    if data.roles.weight is not None:
        raise _SHAPE.error(reason="risk_counts_already_frequency_weighted", route=_ROUTE)
    bins = table["time_bin"].to_pylist()
    if len(set(bins)) != len(bins):
        raise _INVALID.error(reason="duplicate_time_bins")
    durations, events, weights = [], [], []
    for row in sorted(
        table.select(["time_bin", "events", "censored"]).to_pylist(),
        key=lambda row: row["time_bin"],
    ):
        for name, observed in (("events", True), ("censored", False)):
            if row[name]:
                durations.append(row["time_bin"])
                events.append(observed)
                weights.append(row[name])
    if not durations:
        raise _INVALID.error(reason="no_terminal_counts")
    return LifelinesKM(
        np.asarray(durations, dtype=float),
        np.asarray(events, dtype=bool),
        None,
        np.asarray(weights),
        checked.excluded_counts,
    )


def to_lifelines_km(data: MaterializedFrame, *, drop_incomplete: bool = False) -> LifelinesKM:
    """Prepare KM arrays without fitting a curve or making a censoring assumption.

    Records use elapsed seconds from each t0, including entry on the same clock.
    Risk tables use coarsened bin indices and compressed terminal-count weights;
    delayed entry and incomplete risk trajectories require per-unit records.
    """
    checked = preflight_export(data, drop_incomplete=drop_incomplete)
    if data.provenance.shape == "life_table":
        return _risk_arrays(data, checked)
    return _record_arrays(data, checked)


def to_lifelines_cox(data: MaterializedFrame, *, drop_incomplete: bool = False) -> LifelinesCox:
    """Prepare Cox inputs using only declared features and assigned fit fields.

    Treatment is not automatically a predictor. Identity/support/outcome fields
    are excluded unless explicitly assigned as the weight or cluster column.
    """
    checked = preflight_export(data, drop_incomplete=drop_incomplete)
    arrays = _record_arrays(data, checked)
    if np.any(arrays.entry >= arrays.durations):
        raise _INVALID.error(reason="cox_entry_must_precede_duration")
    roles = data.roles
    names = list(
        dict.fromkeys(
            [
                *roles.features,
                "duration",
                "event",
                "entry_time",
                *([roles.weight] if roles.weight else []),
                *([roles.cluster_unit] if roles.cluster_unit else []),
            ]
        )
    )
    _required(checked.table, names)
    try:
        import_module("pandas")
    except ImportError as exc:
        raise _MISSING_DEPENDENCY.error(extra="lifelines") from exc
    df = checked.table.select(names).to_pandas()
    df["duration"] = arrays.durations
    df["entry_time"] = arrays.entry
    df["event"] = arrays.event_observed
    return LifelinesCox(
        df,
        "duration",
        "event",
        "entry_time",
        roles.weight,
        roles.cluster_unit,
        checked.excluded_counts,
    )
