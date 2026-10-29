"""Immutable metadata shared by independent Terrine dataset workflows."""

from __future__ import annotations

import json
import math
from base64 import b64decode, b64encode
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa

from terrine.backends.registry import get_engine
from terrine.errors import RefusalSpec, freeze, parameter_names, thaw
from terrine.windows import Window, seconds

if TYPE_CHECKING:
    import pandas
    import polars

    from terrine.semantic.models import Filter

type DataFrameLike = pa.Table | pandas.DataFrame | polars.DataFrame

__all__ = [
    "ApproximationMeta",
    "BoundCoverage",
    "Budget",
    "CalendarComparisonMeta",
    "CohortOperationMeta",
    "ColumnMeta",
    "Coverage",
    "CoverageSummary",
    "DatasetViewMeta",
    "EligibilitySummary",
    "Events",
    "ExecutionSnapshot",
    "ExpressionTiming",
    "FeatureCutoff",
    "History",
    "MaterializedFrame",
    "Preview",
    "Provenance",
    "Roles",
    "SamplingMeta",
    "SequenceMeta",
    "StatePanelMeta",
    "TreatmentHistoryMeta",
    "UnitSource",
]


_ROWS_EXCEEDED = RefusalSpec(
    "terrine.budget.rows_exceeded", "Output exceeds the configured row limit {limit}."
)
_OUTPUT_BYTES_EXCEEDED = RefusalSpec(
    "terrine.budget.output_bytes_exceeded", "Output exceeds the configured byte limit {limit}."
)
_INVALID_ELIGIBILITY = RefusalSpec(
    "terrine.metadata.invalid_eligibility_summary",
    "Eligibility counts must be nonnegative and reconcile",
)
_COVERAGE_DAY_OVERFLOW = RefusalSpec(
    "terrine.metadata.unrepresentable_coverage",
    "Complete end day {through!r} leaves no following day to include the whole end day. "
    "Choose an earlier last complete day.",
)
_COVERAGE_BOUND_OVERFLOW = RefusalSpec(
    "terrine.metadata.unrepresentable_coverage",
    "Complete days {start!r} through {through!r} cannot be represented in the model's "
    "local time. Choose days farther from the calendar limits.",
)
_COVERAGE_UTC_OVERFLOW = RefusalSpec(
    "terrine.metadata.unrepresentable_coverage",
    "Complete times {start!r} through {end_exclusive!r} cannot be represented in UTC. "
    "Choose times farther from the calendar limits.",
)
_MISSING_COLUMNS = RefusalSpec(
    "terrine.metadata.missing_columns",
    "Arrow table is missing required columns {columns!r}; "
    "use lossless Arrow with all observation columns.",
)
_UNSUPPORTED_METADATA_VERSION = RefusalSpec(
    "terrine.metadata.unsupported_version",
    "Arrow metadata version {version!r} is unsupported; re-materialize the Design "
    "with the current Terrine version.",
)
_INVALID_ENGINE_STAMP = RefusalSpec(
    "terrine.metadata.invalid_engine_stamp",
    "Engine stamp is {reason} (present fields {fields!r}); re-materialize the Design "
    "with the current Terrine version.",
)
_ENGINE_STAMP_FIELDS = ("dialect", "engine_name", "engine_status")
_ARROW_METADATA_KEY = b"terrine.materialized_frame.v2"
_LEGACY_ARROW_METADATA_KEY = b"terrine.materialized_frame.v1"

_RESERVED_NAME = RefusalSpec(
    "terrine.model.invalid_binding",
    "{kind} name {name!r} contains '__', which separates a label from its where= operator; "
    "rename the {kind}.",
)
_HISTORY_TIME_AXIS = RefusalSpec(
    "terrine.model.invalid_binding",
    "History {table!r} needs exactly one time axis: valid_from, optionally with valid_to, "
    "or changed_at alone.",
)
_UNSUPPORTED_TABLE_INPUT = RefusalSpec(
    "terrine.model.invalid_binding",
    "{field} must be a table name or an in-memory pyarrow, pandas or polars table, not {type}.",
)
_INVALID_BINDING = RefusalSpec(
    "terrine.model.invalid_binding",
    "Model input {field!r} is invalid ({reason}). "
    "Use a declared name, the expected input type and an ordered complete time range.",
)
_INVALID_LIMIT = RefusalSpec(
    "terrine.budget.invalid_limit",
    "Budget {field} is invalid: {reason}; limits are nonnegative integers or None.",
)


def _nonempty(value: str, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")


def _binding_name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise _INVALID_BINDING.error(reason="empty_name", field=field)


def _is_dataframe(value: object, library: str) -> bool:
    """Whether ``value`` is ``library``'s DataFrame, without importing the library."""
    return any(
        (cls.__module__ == library or cls.__module__.startswith(f"{library}."))
        and cls.__name__ == "DataFrame"
        for cls in type(value).__mro__
    )


def _table_input(value: str | DataFrameLike, field: str) -> str | pa.Table:
    """A non-empty table name, or the Arrow form of an in-memory table.

    pandas converts with its index dropped and polars through Arrow; neither
    library is imported. Any other object (an Ibis table, a path, a mapping)
    refuses with ``terrine.model.invalid_binding`` before a connection is touched.
    """
    if isinstance(value, str):
        _binding_name(value, field)
        return value
    if isinstance(value, pa.Table):
        table = value
    elif _is_dataframe(value, "pandas"):
        table = pa.Table.from_pandas(value, preserve_index=False)
    elif _is_dataframe(value, "polars"):
        table = value.to_arrow()
    else:
        kind = type(value)
        raise _UNSUPPORTED_TABLE_INPUT.error(
            reason="unsupported_table_input",
            field=field,
            type=f"{kind.__module__}.{kind.__qualname__}",
        )
    if not table.schema:
        raise _INVALID_BINDING.error(reason="empty_table", field=field)
    return table


def _count(value: int | None, field: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
        raise ValueError(f"{field} must be a nonnegative integer or None")


def _aware(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


def _mapping(value: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} must be a string-keyed mapping")
    return freeze(value)


def _typed_mapping(value: Mapping[str, Any], name: str, value_type: type) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) or not isinstance(item, value_type) for key, item in value.items()
    ):
        raise TypeError(f"{name} must map strings to {value_type.__name__}")
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True, init=False)
class Coverage:
    """Declare the inclusive local calendar days for which events or history are complete.

    Example: tr.Coverage("2025-01-01", "2025-03-20")
    """

    start: date
    through: date

    def __init__(self, start: date | str, through: date | str) -> None:
        object.__setattr__(self, "start", _calendar_day(start, "start"))
        object.__setattr__(self, "through", _calendar_day(through, "through"))
        if self.start > self.through:
            raise _INVALID_BINDING.error(reason="through_before_start", field="through")
        if self.through == date.max:
            raise _COVERAGE_DAY_OVERFLOW.error(through=self.through.isoformat())

    def bind(self, day_boundary: Callable[[date], datetime]) -> BoundCoverage:
        if not callable(day_boundary):
            raise _INVALID_BINDING.error(reason="wrong_type", field="day_boundary")
        try:
            start = day_boundary(self.start)
            end = day_boundary(self.through + timedelta(days=1))
        except OverflowError as exc:
            raise _COVERAGE_BOUND_OVERFLOW.error(
                start=self.start.isoformat(), through=self.through.isoformat()
            ) from exc
        return BoundCoverage(start, end)


@dataclass(frozen=True, slots=True)
class BoundCoverage:
    """Coverage translated once to a half-open pair of UTC instants."""

    start: datetime
    end_exclusive: datetime

    def __post_init__(self) -> None:
        for name in ("start", "end_exclusive"):
            value = getattr(self, name)
            if not isinstance(value, datetime):
                raise _INVALID_BINDING.error(reason="wrong_type", field=name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise _INVALID_BINDING.error(reason="naive_datetime", field=name)
        try:
            start = self.start.astimezone(UTC)
            end = self.end_exclusive.astimezone(UTC)
        except OverflowError as exc:
            raise _COVERAGE_UTC_OVERFLOW.error(
                start=self.start.isoformat(), end_exclusive=self.end_exclusive.isoformat()
            ) from exc
        if start >= end:
            raise _INVALID_BINDING.error(reason="end_not_after_start", field="end_exclusive")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end_exclusive", end)


@dataclass(frozen=True, slots=True, init=False)
class UnitSource:
    """Declare a unit's table and key, using a table name or an in-memory table.

    Example: tr.UnitSource(customers, "customer_id")
    """

    table: str | pa.Table
    key: str
    entered_at: str | None
    exited_at: str | None
    observed_from: str | None
    observed_until: str | None

    def __init__(
        self,
        table: str | DataFrameLike,
        key: str,
        entered_at: str | None = None,
        exited_at: str | None = None,
        observed_from: str | None = None,
        observed_until: str | None = None,
    ) -> None:
        bound = _table_input(table, "table")
        _binding_name(key, "key")
        optional = {
            "entered_at": entered_at,
            "exited_at": exited_at,
            "observed_from": observed_from,
            "observed_until": observed_until,
        }
        for name, value in optional.items():
            if value is not None:
                _binding_name(value, name)
        object.__setattr__(self, "table", bound)
        object.__setattr__(self, "key", key)
        for name, value in optional.items():
            object.__setattr__(self, name, value)


def _calendar_day(value: object, field: str) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise _INVALID_BINDING.error(reason="invalid_date", field=field) from error
    if not isinstance(value, date) or isinstance(value, datetime):
        raise _INVALID_BINDING.error(reason="invalid_date", field=field)
    return value


def _complete(value: tuple[date | str, date | str] | None) -> tuple[date, date] | None:
    if value is None:
        return None
    if isinstance(value, str) or not isinstance(value, Sequence) or len(value) != 2:
        raise _INVALID_BINDING.error(reason="not_a_pair", field="complete")
    with parameter_names(start="complete", through="complete"):
        coverage = Coverage(*value)
    return (coverage.start, coverage.through)


@dataclass(frozen=True, slots=True)
class EventMetric:
    """One aggregation over an event table, optionally restricted by label filters."""

    aggregation: Literal[
        "count", "sum", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ]
    column: str | None
    filters: tuple[Filter, ...]


@dataclass(frozen=True, slots=True)
class Ratio:
    """Two metric names declared on the same ``Events``, neither of them a ratio."""

    numerator: str
    denominator: str


@dataclass(frozen=True, slots=True, init=False)
class Events:
    """Declare an event table's time, unit key, named metrics, labels and complete days.

    Example: tr.Events(orders, time="ordered_at", key="customer_id", complete=complete)
    """

    table: str | pa.Table
    time: str
    key: str
    unit: str | None
    metrics: Mapping[str, EventMetric | Ratio]
    labels: tuple[str, ...]
    complete: tuple[date, date] | None

    def __init__(
        self,
        table: str | DataFrameLike,
        *,
        time: str,
        key: str,
        unit: str | None = None,
        metrics: Mapping[str, EventMetric | Ratio] | None = None,
        labels: Sequence[str] = (),
        complete: tuple[date | str, date | str] | None = None,
    ) -> None:
        bound = _table_input(table, "table")
        _binding_name(time, "time")
        _binding_name(key, "key")
        if unit is not None:
            _binding_name(unit, "unit")
        declared = {} if metrics is None else metrics
        if not isinstance(declared, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(metric, (EventMetric, Ratio))
            for name, metric in declared.items()
        ):
            raise _INVALID_BINDING.error(reason="wrong_type", field="metrics")
        if isinstance(labels, str) or not isinstance(labels, Iterable):
            raise _INVALID_BINDING.error(reason="wrong_type", field="labels")
        names = tuple(labels)
        for label in names:
            _binding_name(label, "labels")
            if "__" in label:
                raise _RESERVED_NAME.error(reason="reserved_name", kind="label", name=label)
        if len(set(names)) != len(names):
            raise _INVALID_BINDING.error(reason="duplicate_names", field="labels")
        object.__setattr__(self, "table", bound)
        object.__setattr__(self, "time", time)
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "metrics", MappingProxyType(dict(declared)))
        object.__setattr__(self, "labels", names)
        object.__setattr__(self, "complete", _complete(complete))


@dataclass(frozen=True, slots=True, init=False)
class History:
    """Declare an attribute's history by unit key and validity times or change time.

    Example: tr.History("plans", key="customer_id", column="plan", changed_at="time")
    """

    table: str | pa.Table
    key: str
    column: str
    unit: str | None
    valid_from: str | None
    valid_to: str | None
    changed_at: str | None
    complete: tuple[date, date] | None

    def __init__(
        self,
        table: str | DataFrameLike,
        *,
        key: str,
        column: str,
        unit: str | None = None,
        valid_from: str | None = None,
        valid_to: str | None = None,
        changed_at: str | None = None,
        complete: tuple[date | str, date | str] | None = None,
    ) -> None:
        bound = _table_input(table, "table")
        _binding_name(key, "key")
        _binding_name(column, "column")
        if unit is not None:
            _binding_name(unit, "unit")
        for name, value in (
            ("valid_from", valid_from),
            ("valid_to", valid_to),
            ("changed_at", changed_at),
        ):
            if value is not None:
                _binding_name(value, name)
        if (valid_from is None) == (changed_at is None) or (
            changed_at is not None and valid_to is not None
        ):
            raise _HISTORY_TIME_AXIS.error(
                reason="history_time_axis",
                table=bound if isinstance(bound, str) else "<in-memory>",
            )
        object.__setattr__(self, "table", bound)
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "column", column)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)
        object.__setattr__(self, "changed_at", changed_at)
        object.__setattr__(self, "complete", _complete(complete))


@dataclass(frozen=True, slots=True)
class Roles:
    unit: str
    anchor: str
    t0: str
    treatment: str | None
    features: tuple[str, ...]
    outcomes: tuple[str, ...]
    weight: str | None = None
    cluster_unit: str | None = None
    prior_state: str | None = None
    trial: str | None = None
    assignment_probability: str | None = None
    target_weight: str | None = None
    trial_weight: str | None = None

    def __post_init__(self) -> None:
        for name in ("unit", "anchor", "t0"):
            _nonempty(getattr(self, name), name)
        for name in (
            "treatment",
            "weight",
            "cluster_unit",
            "prior_state",
            "trial",
            "assignment_probability",
            "target_weight",
            "trial_weight",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        for name in ("features", "outcomes"):
            value = getattr(self, name)
            if isinstance(value, (str, bytes)) or not all(
                isinstance(item, str) and item for item in value
            ):
                raise TypeError(f"{name} must be a tuple of non-empty names")
            object.__setattr__(self, name, tuple(value))
        if len(set(self.features)) != len(self.features) or len(set(self.outcomes)) != len(
            self.outcomes
        ):
            raise ValueError("feature and outcome names must be unique")


@dataclass(frozen=True, slots=True)
class ApproximationMeta:
    algorithm: str
    implementation: str
    version: str
    parameters: Mapping[str, str | int | float | bool]
    error_description: str

    def __post_init__(self) -> None:
        for name in ("algorithm", "implementation", "version", "error_description"):
            _nonempty(getattr(self, name), name)
        if any(
            not isinstance(key, str)
            or not key
            or isinstance(value, float)
            and not math.isfinite(value)
            or not isinstance(value, (str, int, float, bool))
            for key, value in self.parameters.items()
        ):
            raise ValueError("parameters must map names to finite scalar values")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


@dataclass(frozen=True, slots=True)
class ColumnMeta:
    window: Window | None
    role: str
    observed_from_column: str | None
    observed_until_column: str | None
    complete_column: str | None
    reason_column: str | None
    numeric_policy: Literal["exact"] | None = None
    approximation: ApproximationMeta | None = None
    transformation: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.window is not None and not isinstance(self.window, Window):
            raise TypeError("window must be a Window or None")
        _nonempty(self.role, "role")
        for name in (
            "observed_from_column",
            "observed_until_column",
            "complete_column",
            "reason_column",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if self.numeric_policy not in (None, "exact"):
            raise ValueError("unsupported numeric policy")
        if self.transformation is not None:
            object.__setattr__(
                self, "transformation", _mapping(self.transformation, "transformation")
            )


@dataclass(frozen=True, slots=True)
class FeatureCutoff:
    latest_seconds: int
    strict: bool
    knowledge: Literal["recorded_at", "retrospective_effective_time"]
    reference: Literal["row_t0", "panel_start"] = "row_t0"

    def __post_init__(self) -> None:
        value = seconds(self.latest_seconds, field="latest_seconds")
        if value != self.latest_seconds or not isinstance(self.strict, bool):
            raise ValueError("invalid feature cutoff")
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("unsupported cutoff knowledge")
        if self.reference not in ("row_t0", "panel_start"):
            raise ValueError("unsupported cutoff reference")


@dataclass(frozen=True, slots=True)
class ExpressionTiming:
    """Conservative support and exact resolved time-column references for one expression."""

    kind: Literal["fixed_t0", "event_time"]
    selector_ref: str | None
    relative_offsets: tuple[int, int] | None
    support_seconds: tuple[int, int]
    selector_dependencies: tuple[str, ...]
    selector_support_seconds: Mapping[str, tuple[int, int]]
    point_time_column: str | None
    start_bound_column: str | None
    end_bound_column: str | None

    def __post_init__(self) -> None:
        if self.kind not in ("fixed_t0", "event_time"):
            raise ValueError("unsupported expression timing kind")
        for name in ("selector_ref", "point_time_column", "start_bound_column", "end_bound_column"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if self.relative_offsets is not None and (
            not isinstance(self.relative_offsets, tuple)
            or len(self.relative_offsets) != 2
            or any(
                isinstance(item, bool) or not isinstance(item, int)
                for item in self.relative_offsets
            )
            or self.relative_offsets[0] > self.relative_offsets[1]
        ):
            raise ValueError("relative_offsets must be an ordered pair of integer seconds or None")
        if (
            not isinstance(self.support_seconds, tuple)
            or len(self.support_seconds) != 2
            or any(
                isinstance(item, bool) or not isinstance(item, int) for item in self.support_seconds
            )
            or self.support_seconds[0] > self.support_seconds[1]
        ):
            raise ValueError("support_seconds must be an ordered pair of integer seconds")
        dependencies = tuple(self.selector_dependencies)
        if any(not isinstance(item, str) or not item for item in dependencies):
            raise TypeError("selector_dependencies must contain non-empty strings")
        if len(set(dependencies)) != len(dependencies):
            raise ValueError("selector_dependencies must be distinct")
        supports = {}
        for key, value in self.selector_support_seconds.items():
            if (
                not isinstance(key, str)
                or not key
                or not isinstance(value, tuple)
                or len(value) != 2
                or any(isinstance(item, bool) or not isinstance(item, int) for item in value)
                or value[0] > value[1]
            ):
                raise ValueError("selector_support_seconds must map names to ordered integer pairs")
            supports[key] = value
        if set(dependencies) != set(supports):
            raise ValueError("selector dependency names must match support entries")
        object.__setattr__(self, "selector_dependencies", dependencies)
        object.__setattr__(self, "selector_support_seconds", MappingProxyType(supports))


@dataclass(frozen=True, slots=True)
class StatePanelMeta:
    """Portable state-panel definition and structural timing references."""

    state_source: Mapping[str, Any]
    levels: tuple[str, ...]
    ordered: bool
    absorbing: tuple[str, ...]
    panel_start: datetime
    administrative_end: datetime
    period_seconds: int
    baseline_features: tuple[str, ...]
    time_varying_features: tuple[str, ...]
    prior_state_timing: Literal["strict_period", "at_origin"]
    period_column: str
    from_time_column: str
    to_time_column: str
    scheduled_to_column: str
    gap_column: str
    terminal_column: str

    def __post_init__(self) -> None:
        if not isinstance(self.state_source, Mapping):
            raise TypeError("state_source must be a mapping")
        object.__setattr__(self, "state_source", _mapping(self.state_source, "state_source"))
        object.__setattr__(self, "levels", tuple(self.levels))
        object.__setattr__(self, "absorbing", tuple(self.absorbing))
        object.__setattr__(self, "baseline_features", tuple(self.baseline_features))
        object.__setattr__(self, "time_varying_features", tuple(self.time_varying_features))
        if (
            not self.levels
            or any(not isinstance(x, str) or not x for x in self.levels)
            or len(set(self.levels)) != len(self.levels)
        ):
            raise ValueError("levels must be distinct non-empty strings")
        if not isinstance(self.ordered, bool):
            raise TypeError("ordered must be bool")
        if any(value not in self.levels for value in self.absorbing) or len(
            set(self.absorbing)
        ) != len(self.absorbing):
            raise ValueError("invalid absorbing states")
        for name in ("panel_start", "administrative_end"):
            _aware(getattr(self, name), name)
        if self.panel_start >= self.administrative_end:
            raise ValueError("panel bounds must be increasing")
        if (
            isinstance(self.period_seconds, bool)
            or not isinstance(self.period_seconds, int)
            or self.period_seconds <= 0
        ):
            raise ValueError("period_seconds must be positive")
        for name in ("baseline_features", "time_varying_features"):
            value = getattr(self, name)
            if not isinstance(value, tuple) or any(not isinstance(x, str) or not x for x in value):
                raise TypeError(f"{name} must be a tuple of names")
        if self.prior_state_timing not in ("strict_period", "at_origin"):
            raise ValueError("unsupported prior-state timing")
        for name in (
            "period_column",
            "from_time_column",
            "to_time_column",
            "scheduled_to_column",
            "gap_column",
            "terminal_column",
        ):
            _nonempty(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class SequenceMeta:
    """Portable event-sequence schema and stable references to status fields."""

    event: str
    window: Window
    max_events: int
    representation: Literal["positions", "arrays"]
    event_fields: tuple[str, ...]
    event_id_column: str | None
    event_time_column: str | None
    position_column: str | None
    event_count_column: str
    complete_column: str
    reason_column: str
    property_reason_columns: Mapping[str, str]
    label_property: str | None = None
    session_gap_seconds: int | None = None
    session_fields: tuple[str, ...] = ()
    sessions_column: str | None = None
    sessions_complete_column: str | None = None
    sessions_reason_column: str | None = None
    transition_from_column: str | None = None
    transition_to_column: str | None = None
    funnel_event_property: str | None = None
    funnel_steps: tuple[str, ...] = ()
    funnel_within_seconds: int | None = None
    funnel_attempts_column: str | None = None
    funnel_attempts_complete_column: str | None = None
    funnel_attempts_reason_column: str | None = None
    funnel_fields: tuple[str, ...] = ()
    identity_kind: Literal["configured", "timestamp"] = "timestamp"

    def __post_init__(self) -> None:
        _nonempty(self.event, "event")
        if not isinstance(self.window, Window):
            raise TypeError("window must be a Window")
        if (
            isinstance(self.max_events, bool)
            or not isinstance(self.max_events, int)
            or self.max_events <= 0
        ):
            raise ValueError("max_events must be positive")
        if self.representation not in ("positions", "arrays"):
            raise ValueError("unsupported sequence representation")
        fields = tuple(self.event_fields)
        if any(not isinstance(name, str) or not name for name in fields) or len(set(fields)) != len(
            fields
        ):
            raise ValueError("event_fields must contain distinct non-empty names")
        object.__setattr__(self, "event_fields", fields)
        for name in (
            "event_id_column",
            "event_time_column",
            "position_column",
            "event_count_column",
            "complete_column",
            "reason_column",
            "label_property",
            "sessions_column",
            "sessions_complete_column",
            "sessions_reason_column",
            "transition_from_column",
            "transition_to_column",
            "funnel_event_property",
            "funnel_attempts_column",
            "funnel_attempts_complete_column",
            "funnel_attempts_reason_column",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if self.session_gap_seconds is not None and (
            isinstance(self.session_gap_seconds, bool)
            or not isinstance(self.session_gap_seconds, int)
            or self.session_gap_seconds < 0
        ):
            raise ValueError("session_gap_seconds must be non-negative or None")
        if self.funnel_within_seconds is not None and (
            isinstance(self.funnel_within_seconds, bool)
            or not isinstance(self.funnel_within_seconds, int)
            or self.funnel_within_seconds <= 0
        ):
            raise ValueError("funnel_within_seconds must be positive or None")
        if self.identity_kind not in ("configured", "timestamp"):
            raise ValueError("unsupported identity kind")
        for field_name in ("session_fields", "funnel_steps", "funnel_fields"):
            values = tuple(getattr(self, field_name))
            if any(not isinstance(value, str) or not value for value in values):
                raise ValueError(f"{field_name} must contain non-empty strings")
            object.__setattr__(self, field_name, values)
        if any(
            not isinstance(value, str) or not value
            for value in self.property_reason_columns.values()
        ):
            raise TypeError("property_reason_columns must map names to non-empty names")
        object.__setattr__(
            self,
            "property_reason_columns",
            _mapping(self.property_reason_columns, "property_reason_columns"),
        )


@dataclass(frozen=True, slots=True)
class TreatmentHistoryMeta:
    kind: Literal["initiation", "persistent_adoption"]
    source: str
    property: str
    history_start: datetime
    knowledge: Literal["recorded_at", "retrospective_effective_time"]
    adopted_value: str | None = None
    persistence: Literal["absorbing"] | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("initiation", "persistent_adoption"):
            raise ValueError("unsupported treatment-history kind")
        _nonempty(self.source, "source")
        _nonempty(self.property, "property")
        _aware(self.history_start, "history_start")
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("unsupported treatment-history knowledge policy")
        if self.kind == "persistent_adoption":
            if self.adopted_value is None:
                raise ValueError("persistent adoption requires adopted_value")
            _nonempty(self.adopted_value, "adopted_value")
            if self.persistence != "absorbing":
                raise ValueError("persistent adoption requires absorbing semantics")
        elif self.adopted_value is not None or self.persistence is not None:
            raise ValueError("initiation metadata cannot declare persistence")


@dataclass(frozen=True, slots=True)
class TrialWeightMeta:
    kind: Literal["equal_trial", "explicit"]
    values: Mapping[str, float] | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("equal_trial", "explicit"):
            raise ValueError("unsupported trial-weight kind")
        if self.kind == "explicit":
            if not isinstance(self.values, Mapping) or not self.values:
                raise ValueError("explicit trial weights require values")
            if any(value <= 0 for value in self.values.values()):
                raise ValueError("trial weights must be positive")
            object.__setattr__(self, "values", _mapping(self.values, "trial weights"))
        elif self.values is not None:
            raise ValueError("equal_trial weights cannot include values")


@dataclass(frozen=True, slots=True)
class SamplingMeta:
    strata: tuple[str, ...]
    controls_per_treated: int
    seed: int

    def __post_init__(self) -> None:
        strata = tuple(self.strata)
        if any(not isinstance(value, str) or not value.strip() for value in strata):
            raise ValueError("sampling strata must be non-empty names")
        if len(set(strata)) != len(strata):
            raise ValueError("sampling strata must be unique")
        if (
            isinstance(self.controls_per_treated, bool)
            or not isinstance(self.controls_per_treated, int)
            or self.controls_per_treated <= 0
        ):
            raise ValueError("controls_per_treated must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("sampling seed must be nonnegative")
        object.__setattr__(self, "strata", strata)


@dataclass(frozen=True, slots=True)
class PanelMeta:
    """Portable shape-specific identity and treatment-history metadata."""

    shape: Literal["sequential", "event_panel"]
    treatment: TreatmentHistoryMeta | None = None
    trial_weight: TrialWeightMeta | None = None
    periods: tuple[int, ...] = ()
    period_seconds: int | None = None
    reference_period: int | None = None
    trials: tuple[str, ...] = ()
    anticipation_seconds: int = 0

    def __post_init__(self) -> None:
        if self.shape not in ("sequential", "event_panel"):
            raise ValueError("unsupported panel metadata shape")
        if self.treatment is not None and not isinstance(self.treatment, TreatmentHistoryMeta):
            raise TypeError("invalid panel treatment metadata")
        if self.trial_weight is not None and not isinstance(self.trial_weight, TrialWeightMeta):
            raise TypeError("invalid trial weight metadata")
        periods = tuple(self.periods)
        if any(isinstance(period, bool) or not isinstance(period, int) for period in periods):
            raise TypeError("panel periods must be integers")
        object.__setattr__(self, "periods", periods)
        if self.period_seconds is not None and (
            isinstance(self.period_seconds, bool)
            or not isinstance(self.period_seconds, int)
            or self.period_seconds <= 0
        ):
            raise ValueError("period_seconds must be positive")
        trials = tuple(self.trials)
        if any(not isinstance(trial, str) or not trial for trial in trials):
            raise TypeError("panel trials must be non-empty strings")
        if len(set(trials)) != len(trials):
            raise ValueError("panel trial identities must be unique")
        object.__setattr__(self, "trials", trials)
        if (
            isinstance(self.anticipation_seconds, bool)
            or not isinstance(self.anticipation_seconds, int)
            or self.anticipation_seconds < 0
        ):
            raise ValueError("anticipation_seconds must be nonnegative")


@dataclass(frozen=True, slots=True)
class CalendarComparisonMeta:
    """Portable control, cohort and period policy for stacked comparisons."""

    control: Literal["never_treated", "not_yet_treated"]
    cohort_source: str
    cohort_property: str
    history_start: datetime
    knowledge: Literal["recorded_at", "retrospective_effective_time"]
    reference_period: int
    comparison_periods: tuple[int, ...]
    anticipation_periods: tuple[int, ...]
    period_seconds: int

    def __post_init__(self) -> None:
        if self.control not in ("never_treated", "not_yet_treated"):
            raise ValueError("invalid calendar comparison control")
        for name in ("cohort_source", "cohort_property"):
            _nonempty(getattr(self, name), name)
        _aware(self.history_start, "history_start")
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("invalid cohort knowledge policy")
        if self.period_seconds <= 0:
            raise ValueError("period_seconds must be positive")
        for name in ("comparison_periods", "anticipation_periods"):
            object.__setattr__(self, name, tuple(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class CohortOperationMeta:
    bucket_seconds: int
    bucket_origin: datetime
    statistic: Literal["total", "mean"]
    value_column: str
    parent_design_hash: str
    derived_operation_hash: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.bucket_seconds, bool)
            or not isinstance(self.bucket_seconds, int)
            or self.bucket_seconds <= 0
        ):
            raise ValueError("bucket_seconds must be positive")
        _aware(self.bucket_origin, "bucket_origin")
        object.__setattr__(self, "bucket_origin", self.bucket_origin.astimezone(UTC))
        if self.statistic not in ("total", "mean"):
            raise ValueError("statistic must be total or mean")
        for name in ("value_column", "parent_design_hash", "derived_operation_hash"):
            _nonempty(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class EligibilitySummary:
    candidate: int
    excluded_false: int
    unknown: int
    retained: int

    def __post_init__(self) -> None:
        values = (self.candidate, self.excluded_false, self.unknown, self.retained)
        if (
            any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in values
            )
            or self.candidate != self.excluded_false + self.unknown + self.retained
        ):
            raise _INVALID_ELIGIBILITY.error(
                candidate=self.candidate,
                excluded_false=self.excluded_false,
                unknown=self.unknown,
                retained=self.retained,
            )


@dataclass(frozen=True, slots=True)
class RowCheck:
    """A column whose TRUE rows fail ``refusal``; NULL rows pass.

    Inside a one-row aggregate check it names that aggregate's ``failed_N``
    string count instead.
    """

    column: str
    refusal: RefusalSpec
    context: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", MappingProxyType(dict(self.context)))


@dataclass(frozen=True, slots=True)
class RowCheckGroup:
    """Row checks and the relation whose rows they are counted over."""

    table: Any
    checks: tuple[RowCheck, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "checks", tuple(self.checks))


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    snapshot_id: str | None
    backend: str
    source_generations: Mapping[str, str | None]
    input_binding_id: str | None
    replay_equivalence: Literal["verified", "unverified"]

    def __post_init__(self) -> None:
        _nonempty(self.backend, "backend")
        for name in ("snapshot_id", "input_binding_id"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if self.replay_equivalence not in ("verified", "unverified"):
            raise ValueError("invalid replay equivalence")
        if any(
            value is not None and not isinstance(value, str)
            for value in self.source_generations.values()
        ):
            raise TypeError("source generations must map names to strings or None")
        object.__setattr__(
            self, "source_generations", _mapping(self.source_generations, "source_generations")
        )


@dataclass(frozen=True, slots=True)
class DatasetViewMeta:
    """Portable selections and split/exclusion summary for a dataset view."""

    kind: Literal["training", "scoring"]
    selected_features: tuple[str, ...]
    selected_labels: tuple[str, ...]
    view_hash: str
    feature_definition_hash: str
    resolved_as_of: datetime | None = None
    incomplete_labels: Literal["drop", "keep"] | None = None
    split_policy: Mapping[str, Any] | None = None
    counts_by_reason: Mapping[str, int] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in ("training", "scoring"):
            raise ValueError("invalid dataset view kind")
        for name in ("view_hash", "feature_definition_hash"):
            _nonempty(getattr(self, name), name)
        for name in ("selected_features", "selected_labels"):
            values = tuple(getattr(self, name))
            if any(not isinstance(value, str) or not value for value in values) or len(
                set(values)
            ) != len(values):
                raise ValueError(f"{name} must contain unique non-empty names")
            object.__setattr__(self, name, values)
        if self.resolved_as_of is not None:
            _aware(self.resolved_as_of, "resolved_as_of")
            object.__setattr__(self, "resolved_as_of", self.resolved_as_of.astimezone(UTC))
        if self.incomplete_labels not in (None, "drop", "keep"):
            raise ValueError("invalid incomplete-label policy")
        if self.split_policy is not None:
            object.__setattr__(self, "split_policy", _mapping(self.split_policy, "split_policy"))
        counts = dict(self.counts_by_reason)
        if any(
            not isinstance(key, str)
            or not key
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for key, value in counts.items()
        ):
            raise ValueError("counts_by_reason must contain nonnegative integer counts")
        object.__setattr__(self, "counts_by_reason", MappingProxyType(counts))


@dataclass(frozen=True, slots=True)
class Provenance:
    """Design, model, input and execution evidence recorded with every result.

    ``engine_name`` is the actual backend that executed, ``dialect`` the admitted
    expression policy and ``engine_status`` that policy's registered status; a
    portable result records ``"generic"`` and ``"unverified"``.
    """

    feature_cutoffs: Mapping[str, FeatureCutoff]
    eligibility_cutoffs: Mapping[str, FeatureCutoff]
    design_hash: str
    model_hash: str
    source_generations: Mapping[str, str | None]
    execution_snapshot: ExecutionSnapshot
    shape: str
    treatment_design: Literal["baseline_binary", "grace_initiation"] | None
    engine_name: str
    dialect: str
    engine_status: Literal["verified", "unverified"]
    eligibility_summary: EligibilitySummary | None = None
    treatment_knowledge: Literal["recorded_at", "retrospective_effective_time"] | None = None
    weight_kind: Literal["baseline_function", "sampled_control"] | None = None
    weight_cutoff: FeatureCutoff | None = None
    weight_depends_on_treatment: bool | None = None
    treatment_known_at_t0: bool = False
    state_panel: StatePanelMeta | None = None
    expression_timings: Mapping[str, ExpressionTiming] = dataclass_field(default_factory=dict)
    sequence_meta: SequenceMeta | None = None
    panel: PanelMeta | None = None
    sampling: SamplingMeta | None = None
    calendar_comparison: CalendarComparisonMeta | None = None
    cohort_operation: CohortOperationMeta | None = None
    administrative_cutoff: datetime | None = None
    measurement_knowledge: (
        Literal["measurement_time", "retrospective_effective_time", "recorded_at"] | None
    ) = None
    dataset_view: DatasetViewMeta | None = None

    def __post_init__(self) -> None:
        for name in ("design_hash", "model_hash", "shape"):
            _nonempty(getattr(self, name), name)
        object.__setattr__(
            self,
            "feature_cutoffs",
            _typed_mapping(self.feature_cutoffs, "feature_cutoffs", FeatureCutoff),
        )
        object.__setattr__(
            self,
            "eligibility_cutoffs",
            _typed_mapping(self.eligibility_cutoffs, "eligibility_cutoffs", FeatureCutoff),
        )
        object.__setattr__(
            self,
            "expression_timings",
            _typed_mapping(self.expression_timings, "expression_timings", ExpressionTiming),
        )
        if any(
            value is not None and not isinstance(value, str)
            for value in self.source_generations.values()
        ):
            raise TypeError("source generations must map names to strings or None")
        object.__setattr__(
            self, "source_generations", _mapping(self.source_generations, "source_generations")
        )
        if self.treatment_knowledge not in (None, "recorded_at", "retrospective_effective_time"):
            raise ValueError("invalid treatment knowledge")
        if self.weight_kind not in (None, "baseline_function", "sampled_control"):
            raise ValueError("invalid weight kind")
        if self.treatment_design not in (
            None,
            "baseline_binary",
            "grace_initiation",
        ) or not isinstance(self.treatment_known_at_t0, bool):
            raise ValueError("invalid treatment provenance")
        if self.weight_depends_on_treatment is not None and not isinstance(
            self.weight_depends_on_treatment, bool
        ):
            raise TypeError("weight_depends_on_treatment must be bool or None")
        if self.eligibility_summary is not None and not isinstance(
            self.eligibility_summary, EligibilitySummary
        ):
            raise TypeError("eligibility_summary must be EligibilitySummary or None")
        if not isinstance(self.execution_snapshot, ExecutionSnapshot):
            raise TypeError("execution_snapshot must be ExecutionSnapshot")
        _check_engine_stamp(
            self.engine_name,
            self.dialect,
            self.engine_status,
            self.execution_snapshot.backend,
        )
        if self.state_panel is not None and not isinstance(self.state_panel, StatePanelMeta):
            raise TypeError("state_panel must be StatePanelMeta or None")
        if self.dataset_view is not None and not isinstance(self.dataset_view, DatasetViewMeta):
            raise TypeError("dataset_view must be DatasetViewMeta or None")
        if self.sequence_meta is not None and not isinstance(self.sequence_meta, SequenceMeta):
            raise TypeError("sequence_meta must be SequenceMeta or None")
        if self.panel is not None and not isinstance(self.panel, PanelMeta):
            raise TypeError("panel must be PanelMeta or None")
        if self.sampling is not None and not isinstance(self.sampling, SamplingMeta):
            raise TypeError("sampling must be SamplingMeta or None")
        if self.calendar_comparison is not None and not isinstance(
            self.calendar_comparison, CalendarComparisonMeta
        ):
            raise TypeError("calendar_comparison must be CalendarComparisonMeta or None")
        if self.cohort_operation is not None and not isinstance(
            self.cohort_operation, CohortOperationMeta
        ):
            raise TypeError("cohort_operation must be CohortOperationMeta or None")
        if self.measurement_knowledge not in (
            None,
            "measurement_time",
            "retrospective_effective_time",
            "recorded_at",
        ):
            raise ValueError("invalid measurement knowledge provenance")
        if self.administrative_cutoff is not None:
            _aware(self.administrative_cutoff, "administrative_cutoff")
            object.__setattr__(
                self, "administrative_cutoff", self.administrative_cutoff.astimezone(UTC)
            )


@dataclass(frozen=True, slots=True)
class Budget:
    """Limit output rows, output bytes or estimated scan bytes for a dataset.

    Example: tr.Budget(max_rows=1000)
    """

    max_rows: int | None = None
    max_output_bytes: int | None = None
    max_scan_bytes: int | None = None
    strict_scan: bool = True

    def __post_init__(self) -> None:
        for name in ("max_rows", "max_output_bytes", "max_scan_bytes"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise _INVALID_LIMIT.error(reason="not_nonnegative_integer", field=name)
        if not isinstance(self.strict_scan, bool):
            raise _INVALID_LIMIT.error(reason="wrong_type", field="strict_scan")


@dataclass(frozen=True, slots=True)
class CoverageSummary:
    start: datetime
    end_exclusive: datetime

    def __post_init__(self) -> None:
        _aware(self.start, "start")
        _aware(self.end_exclusive, "end_exclusive")
        start = self.start.astimezone(UTC)
        end = self.end_exclusive.astimezone(UTC)
        if start >= end:
            raise ValueError("coverage summary start must precede end_exclusive")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end_exclusive", end)


@dataclass(frozen=True, slots=True)
class Preview:
    planned_rows: int | None
    output_bytes: int | None
    scan_bytes: int | None
    scan_reason: str | None
    coverage: Mapping[str, CoverageSummary]
    strategy: Mapping[str, str]
    budget_warnings: tuple[str, ...]
    candidate_rows: int | None = None
    known_false_rows: int | None = None
    unknown_rows: int | None = None
    eligible_rows: int | None = None
    counted_rows: int | None = None
    count_capped: bool = False

    def __post_init__(self) -> None:
        for name in (
            "planned_rows",
            "candidate_rows",
            "known_false_rows",
            "unknown_rows",
            "eligible_rows",
            "counted_rows",
            "output_bytes",
            "scan_bytes",
        ):
            _count(getattr(self, name), name)
        if self.scan_reason is not None and not isinstance(self.scan_reason, str):
            raise TypeError("scan_reason must be text or None")
        if not isinstance(self.count_capped, bool):
            raise TypeError("count_capped must be bool")
        object.__setattr__(
            self, "coverage", _typed_mapping(self.coverage, "coverage", CoverageSummary)
        )
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in self.strategy.items()):
            raise TypeError("strategy must map names to strings")
        object.__setattr__(self, "strategy", _mapping(self.strategy, "strategy"))
        if isinstance(self.budget_warnings, str) or not all(
            isinstance(v, str) for v in self.budget_warnings
        ):
            raise TypeError("budget_warnings must be a tuple of strings")
        object.__setattr__(self, "budget_warnings", tuple(self.budget_warnings))


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Window):
        return {"$window": [value.start, value.end]}
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _encode(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_encode(item) for item in value]
    return value


def _check_engine_stamp(
    engine_name: Any,
    dialect: Any,
    engine_status: Any,
    backend: Any,
    fields: tuple[str, ...] = _ENGINE_STAMP_FIELDS,
) -> None:
    """Refuse a complete stamp that contradicts its execution evidence.

    ``engine_name`` must be the backend the execution snapshot recorded.
    ``dialect`` names an expression policy, so the registry decides whether it
    exists and which status it carries; a registered engine also executes under
    its own name, whereas the portable policy records the actual backend.
    """
    engine = get_engine(dialect) if isinstance(dialect, str) and dialect else None
    if (
        engine is None
        or not isinstance(engine_name, str)
        or not engine_name
        or engine_name != backend
        or engine_status != engine.status
        or (engine.status == "verified" and engine_name != dialect)
    ):
        raise _INVALID_ENGINE_STAMP.error(reason="inconsistent", fields=fields)


def _decode_engine_stamp(
    value: Mapping[str, Any],
) -> tuple[str, str, Literal["verified", "unverified"]]:
    """Return ``(engine_name, dialect, engine_status)`` from saved provenance.

    Writers before the stamp existed executed only through registered verified
    engines and recorded that engine as ``execution_snapshot.backend``, so a
    payload with all three fields absent decodes as that verified engine.
    Any other incomplete stamp refuses rather than being upgraded.
    """
    snapshot = value.get("execution_snapshot")
    backend = snapshot.get("backend") if isinstance(snapshot, Mapping) else None
    present = tuple(name for name in _ENGINE_STAMP_FIELDS if name in value)
    if not present:
        engine = get_engine(backend) if isinstance(backend, str) else None
        if not isinstance(backend, str) or engine is None or engine.status != "verified":
            raise _INVALID_ENGINE_STAMP.error(reason="unknown_legacy_engine", fields=present)
        return backend, backend, "verified"
    if present != _ENGINE_STAMP_FIELDS:
        raise _INVALID_ENGINE_STAMP.error(reason="partial", fields=present)
    stamp = (value["engine_name"], value["dialect"], value["engine_status"])
    _check_engine_stamp(*stamp, backend, fields=present)
    return stamp


def _decode(value: Any, cls: type) -> Any:
    if cls is Window:
        return Window(*value["$window"])
    if not is_dataclass(cls):
        raise TypeError("metadata target must be a dataclass")
    kwargs: dict[str, Any] = {}
    unknown = set(value) - {field.name for field in fields(cls)}
    if unknown:
        raise ValueError(f"unknown {cls.__name__} fields: {sorted(unknown)}")
    if cls is Provenance:
        kwargs["engine_name"], kwargs["dialect"], kwargs["engine_status"] = _decode_engine_stamp(
            value
        )
    for field in fields(cls):
        if field.name not in value or field.name in kwargs:
            continue
        item = value[field.name]
        if field.name == "approximation" and item is not None:
            kwargs[field.name] = _decode(item, ApproximationMeta)
        elif field.name in ("feature_cutoffs", "eligibility_cutoffs"):
            kwargs[field.name] = {k: _decode(v, FeatureCutoff) for k, v in item.items()}
        elif field.name == "expression_timings":
            timings = {}
            for key, raw_timing in item.items():
                timing: dict[str, Any] = dict(raw_timing)
                timing["relative_offsets"] = (
                    None
                    if timing["relative_offsets"] is None
                    else tuple(timing["relative_offsets"])
                )
                timing["support_seconds"] = tuple(timing["support_seconds"])
                timing["selector_dependencies"] = tuple(timing["selector_dependencies"])
                timing["selector_support_seconds"] = {
                    name: tuple(bounds)
                    for name, bounds in timing["selector_support_seconds"].items()
                }
                timings[key] = ExpressionTiming(**timing)
            kwargs[field.name] = timings
        elif field.name in ("source_generations", "strategy"):
            kwargs[field.name] = item
        elif field.name == "coverage":
            kwargs[field.name] = {k: _decode(v, CoverageSummary) for k, v in item.items()}
        elif field.name == "execution_snapshot" and item is not None:
            kwargs[field.name] = _decode(item, ExecutionSnapshot)
        elif field.name == "eligibility_summary" and item is not None:
            kwargs[field.name] = _decode(item, EligibilitySummary)
        elif field.name == "weight_cutoff" and item is not None:
            kwargs[field.name] = _decode(item, FeatureCutoff)
        elif field.name == "state_panel" and item is not None:
            kwargs[field.name] = _decode(item, StatePanelMeta)
        elif field.name == "dataset_view" and item is not None:
            kwargs[field.name] = _decode(item, DatasetViewMeta)
        elif field.name == "sequence_meta" and item is not None:
            kwargs[field.name] = _decode(item, SequenceMeta)
        elif field.name == "panel" and item is not None:
            treatment = item.get("treatment")
            if treatment is not None:
                treatment = TreatmentHistoryMeta(
                    treatment["kind"],
                    treatment["source"],
                    treatment["property"],
                    datetime.fromisoformat(treatment["history_start"]["$datetime"]),
                    treatment["knowledge"],
                    treatment.get("adopted_value"),
                    treatment.get("persistence"),
                )
            weight = item.get("trial_weight")
            if weight is not None:
                weight = TrialWeightMeta(weight["kind"], weight.get("values"))
            kwargs[field.name] = PanelMeta(
                item["shape"],
                treatment,
                weight,
                tuple(item.get("periods", ())),
                item.get("period_seconds"),
                item.get("reference_period"),
                tuple(item.get("trials", ())),
                item.get("anticipation_seconds", 0),
            )
        elif field.name == "calendar_comparison" and item is not None:
            kwargs[field.name] = CalendarComparisonMeta(
                item["control"],
                item["cohort_source"],
                item["cohort_property"],
                datetime.fromisoformat(item["history_start"]["$datetime"]),
                item["knowledge"],
                item["reference_period"],
                tuple(item["comparison_periods"]),
                tuple(item["anticipation_periods"]),
                item["period_seconds"],
            )
        elif field.name == "sampling" and item is not None:
            kwargs[field.name] = SamplingMeta(
                tuple(item["strata"]), item["controls_per_treated"], item["seed"]
            )
        elif field.name == "cohort_operation" and item is not None:
            kwargs[field.name] = _decode(item, CohortOperationMeta)
        elif field.name in (
            "panel_start",
            "administrative_end",
            "bucket_origin",
            "resolved_as_of",
            "administrative_cutoff",
        ) and isinstance(item, dict):
            kwargs[field.name] = datetime.fromisoformat(item["$datetime"])
        elif field.name == "window" and item is not None:
            kwargs[field.name] = Window(*item["$window"])
        elif (
            field.name in ("start", "end_exclusive")
            and isinstance(item, dict)
            and "$datetime" in item
        ):
            kwargs[field.name] = datetime.fromisoformat(item["$datetime"])
        elif field.name in ("start", "through") and isinstance(item, dict) and "$date" in item:
            kwargs[field.name] = date.fromisoformat(item["$date"])
        else:
            kwargs[field.name] = item
    return cls(**kwargs)


def _metadata_payload(
    roles: Roles, columns: Mapping[str, ColumnMeta], provenance: Provenance
) -> dict[str, Any]:
    return {
        "version": 1,
        "roles": _encode(roles),
        "columns": _encode(columns),
        "provenance": _encode(provenance),
    }


def _bookkeeping_columns(
    names: Sequence[str],
    roles: Roles,
    columns: Mapping[str, ColumnMeta],
    timings: Mapping[str, ExpressionTiming],
) -> tuple[str, ...]:
    bookkeeping = {"_observed_from", "_observed_until", "anchor_provenance"}
    for meta in columns.values():
        bookkeeping.update(
            name
            for name in (meta.observed_from_column, meta.observed_until_column)
            if name is not None
        )
    for timing in timings.values():
        bookkeeping.update(
            name
            for name in (
                timing.point_time_column,
                timing.start_bound_column,
                timing.end_bound_column,
            )
            if name is not None
        )
    bookkeeping.difference_update((roles.unit, roles.anchor, roles.t0, *columns))
    return tuple(name for name in names if name in bookkeeping)


def _support_columns(names: Sequence[str], columns: Mapping[str, ColumnMeta]) -> tuple[str, ...]:
    support = {
        name
        for meta in columns.values()
        for name in (meta.complete_column, meta.reason_column)
        if name is not None and name not in columns
    }
    return tuple(name for name in names if name in support)


def _ordered_columns(
    names: Sequence[str],
    roles: Roles,
    columns: Mapping[str, ColumnMeta],
    timings: Mapping[str, ExpressionTiming],
) -> tuple[str, ...]:
    bookkeeping = _bookkeeping_columns(names, roles, columns, timings)
    support = _support_columns(names, columns)
    ordered = []
    for name in (roles.unit, roles.anchor, roles.t0, *names):
        if name not in names or name in ordered or name in bookkeeping or name in support:
            continue
        ordered.append(name)
        if name in columns:
            meta = columns[name]
            for field in (meta.complete_column, meta.reason_column):
                if field is not None and field in names and field not in ordered:
                    ordered.append(field)
    ordered.extend(name for name in names if name not in ordered and name not in bookkeeping)
    return (*ordered, *bookkeeping)


@dataclass(frozen=True, slots=True)
class MaterializedFrame:
    table: pa.Table
    roles: Roles
    columns: Mapping[str, ColumnMeta]
    provenance: Provenance

    def __post_init__(self) -> None:
        if not isinstance(self.table, pa.Table):
            raise TypeError("table must be a pyarrow.Table")
        if not isinstance(self.roles, Roles) or not isinstance(self.provenance, Provenance):
            raise TypeError("roles and provenance must use Terrine metadata types")
        if any(
            not isinstance(k, str) or not isinstance(v, ColumnMeta) for k, v in self.columns.items()
        ):
            raise TypeError("columns must map names to ColumnMeta")
        object.__setattr__(self, "columns", _typed_mapping(self.columns, "columns", ColumnMeta))
        object.__setattr__(
            self,
            "table",
            self.table.select(
                _ordered_columns(
                    self.table.column_names,
                    self.roles,
                    self.columns,
                    self.provenance.expression_timings,
                )
            ),
        )

    def to_metadata(self) -> dict[str, Any]:
        """Return a detached JSON-compatible metadata value."""
        return thaw(freeze(_metadata_payload(self.roles, self.columns, self.provenance)))

    def to_arrow(self) -> pa.Table:
        """Return lossless Arrow with value/support triples before observation columns."""
        payload = self.to_metadata()
        payload["version"] = 2
        payload["bookkeeping_columns"] = list(self._bookkeeping_columns())
        table = self.table
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        metadata = dict(table.schema.metadata or {})
        metadata[_ARROW_METADATA_KEY] = encoded
        return table.replace_schema_metadata(metadata)

    def _bookkeeping_columns(self) -> tuple[str, ...]:
        return _bookkeeping_columns(
            self.table.column_names, self.roles, self.columns, self.provenance.expression_timings
        )

    def _support_columns(self) -> tuple[str, ...]:
        return _support_columns(self.table.column_names, self.columns)

    @classmethod
    def from_arrow(cls, table: pa.Table) -> MaterializedFrame:
        if not isinstance(table, pa.Table):
            raise TypeError("table must be a pyarrow.Table")
        raw = (table.schema.metadata or {}).get(_ARROW_METADATA_KEY)
        if raw is None and _LEGACY_ARROW_METADATA_KEY in (table.schema.metadata or {}):
            raise _UNSUPPORTED_METADATA_VERSION.error(version=1)
        if raw is None:
            raise ValueError("Arrow schema has no Terrine metadata")
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("version") != 2:
            raise _UNSUPPORTED_METADATA_VERSION.error(version=payload.get("version"))
        roles = _decode(payload["roles"], Roles)
        columns = {key: _decode(item, ColumnMeta) for key, item in payload["columns"].items()}
        provenance = _decode(payload["provenance"], Provenance)
        missing = tuple(
            name for name in payload["bookkeeping_columns"] if name not in table.column_names
        )
        if missing:
            raise _MISSING_COLUMNS.error(columns=missing)
        clean = dict(table.schema.metadata or {})
        del clean[_ARROW_METADATA_KEY]
        return cls(table.replace_schema_metadata(clean or None), roles, columns, provenance)

    def to_json(self) -> str:
        """Serialize metadata plus an Arrow IPC stream, without pickle."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, self.table.schema) as writer:
            writer.write_table(self.table)
        payload = self.to_metadata()
        payload["arrow_ipc"] = b64encode(sink.getvalue().to_pybytes()).decode("ascii")
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_json(cls, serialized: str) -> MaterializedFrame:
        payload = json.loads(serialized)
        if payload.pop("version") != 1:
            raise ValueError("unsupported Terrine metadata version")
        raw = b64decode(payload.pop("arrow_ipc"), validate=True)
        table = pa.ipc.open_stream(pa.py_buffer(raw)).read_all()
        roles = _decode(payload["roles"], Roles)
        columns = {key: _decode(item, ColumnMeta) for key, item in payload["columns"].items()}
        provenance = _decode(payload["provenance"], Provenance)
        return cls(table, roles, columns, provenance)
