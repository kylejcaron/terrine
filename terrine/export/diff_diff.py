"""Encode observed persistent-adoption panels for diff-diff."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from terrine.data import MaterializedFrame
from terrine.errors import RefusalSpec
from terrine.export.preflight import preflight_export

__all__ = ["DiffDiffData", "to_diff_diff"]

_INVALID = RefusalSpec(
    "terrine.diff_diff.invalid_panel",
    "Cannot encode this adoption panel ({reason}); {route}.",
)
_MISSING_DEPENDENCY = RefusalSpec(
    "terrine.convert.missing_dependency",
    "Conversion requires the optional {extra!r} dependency; install terrine[{extra}].",
)
_TIME = "__terrine_period"
_FIRST = "first_treat"
_NS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


@dataclass(frozen=True, slots=True)
class DiffDiffData:
    """Return a dataframe with unit identity, positive period codes and observed adoption times.

    Example: exports.to_diff_diff(adoption_data, period_col="t0", period_seconds=86400)
    """

    df: Any
    unit_col: str
    time_col: str
    first_treat_col: str
    period_mapping: Mapping[Any, int]
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "period_mapping", MappingProxyType(dict(self.period_mapping)))
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


def _period_mapping(column: pa.ChunkedArray, cadence: int | None) -> dict[Any, int]:
    if column.null_count or not len(column):
        raise _INVALID.error(
            reason="missing_periods", route="supply a non-null regular period grid"
        )
    dtype = column.type
    values = column.to_pylist()
    if pa.types.is_integer(dtype):
        ticks = values
        step = 1
    elif pa.types.is_timestamp(dtype) or pa.types.is_date(dtype):
        if isinstance(cadence, bool) or not isinstance(cadence, int) or cadence <= 0:
            raise _INVALID.error(
                reason="cadence_required",
                route="declare period_seconds or use integer period ticks",
            )
        if pa.types.is_timestamp(dtype):
            multiplier = _NS_PER_UNIT[dtype.unit]
            raw = column.cast(pa.int64()).to_pylist()
        elif pa.types.is_date32(dtype):
            multiplier = 86_400_000_000_000
            raw = column.cast(pa.int32()).to_pylist()
        else:
            multiplier = 1_000_000
            raw = column.cast(pa.int64()).to_pylist()
        ticks = [value * multiplier for value in raw]
        step = cadence * 1_000_000_000
    else:
        raise _INVALID.error(
            reason="unsupported_period_type", route="use integer ticks or dated periods"
        )
    origin = min(ticks)
    if any((tick - origin) % step for tick in ticks):
        raise _INVALID.error(
            reason="misaligned_periods", route="supply a regular panel at the declared cadence"
        )
    return {value: (tick - origin) // step + 1 for value, tick in zip(values, ticks, strict=True)}


def to_diff_diff(
    data: MaterializedFrame,
    *,
    period_col: str,
    period_seconds: int | None = None,
    drop_incomplete: bool = False,
) -> DiffDiffData:
    """Encode observed adoption times without changing treatment history or period gaps.

    Zero means no adoption observed in the supplied history, not over a unit's lifetime;
    dated periods need a cadence, and no estimator is fitted.

    Example: exports.to_diff_diff(adoption_data, period_col="t0", period_seconds=86400)
    """
    checked = preflight_export(data, drop_incomplete=drop_incomplete)
    try:
        import_module("pandas")
    except ImportError as exc:
        raise _MISSING_DEPENDENCY.error(extra="diff-diff") from exc
    roles, original = data.roles, data.table
    treatment = roles.treatment
    if treatment is None or any(
        name not in original.column_names for name in (roles.unit, period_col, treatment)
    ):
        raise _INVALID.error(
            reason="missing_roles", route="declare unit, period and binary adoption columns"
        )
    if {_TIME, _FIRST} & set(original.column_names):
        raise _INVALID.error(
            reason="output_collision",
            route="rename the existing first_treat or __terrine_period column",
        )
    history = data.provenance.panel
    if (
        history is not None
        and history.treatment is not None
        and history.treatment.kind != "persistent_adoption"
    ):
        raise _INVALID.error(
            reason="initiation_not_adoption", route="supply persistent-adoption treatment history"
        )
    metadata_cadence = history.period_seconds if history is not None else None
    comparison = data.provenance.calendar_comparison
    if metadata_cadence is None and comparison is not None:
        metadata_cadence = comparison.period_seconds
    if (
        period_seconds is not None
        and metadata_cadence is not None
        and period_seconds != metadata_cadence
    ):
        raise _INVALID.error(
            reason="conflicting_cadence", route="match the panel's declared period_seconds"
        )
    mapping = _period_mapping(
        original[period_col], period_seconds if period_seconds is not None else metadata_cadence
    )
    units = original[roles.unit].to_pylist()
    periods = original[period_col].to_pylist()
    treatments = original[treatment].to_pylist()
    by_unit: dict[Any, list[tuple[int, bool]]] = defaultdict(list)
    seen = set()
    for unit, period, adopted in zip(units, periods, treatments, strict=True):
        if unit is None or adopted is None or adopted not in (True, False, 0, 1):
            raise _INVALID.error(
                reason="invalid_unit_or_treatment",
                route="supply non-null unit IDs and binary adoption",
            )
        code = mapping[period]
        if (unit, code) in seen:
            raise _INVALID.error(
                reason="duplicate_unit_period", route="supply one record per unit and period"
            )
        seen.add((unit, code))
        by_unit[unit].append((code, bool(adopted)))
    first_by_unit = {}
    for unit, observations in by_unit.items():
        first = 0
        for code, adopted in sorted(observations):
            if first and not adopted:
                raise _INVALID.error(
                    reason="treatment_reversal",
                    route="supply persistent-adoption history, not initiation flags",
                )
            if adopted and first == 0:
                first = code
        first_by_unit[unit] = first
    df = checked.table.to_pandas()
    df[_TIME] = [mapping[value] for value in checked.table[period_col].to_pylist()]
    df[_FIRST] = [first_by_unit[unit] for unit in checked.table[roles.unit].to_pylist()]
    return DiffDiffData(df, roles.unit, _TIME, _FIRST, mapping, checked.excluded_counts)
