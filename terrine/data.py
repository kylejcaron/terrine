"""Immutable metadata shared by independent Terrine dataset workflows."""

from __future__ import annotations

import json
from base64 import b64decode, b64encode
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import Any, Literal

import pyarrow as pa

from terrine.errors import TerrineError, freeze, thaw
from terrine.windows import Window, seconds

__all__ = [
    "Coverage",
    "BoundCoverage",
    "UnitSource",
    "Roles",
    "ColumnMeta",
    "FeatureCutoff",
    "StatePanelMeta",
    "TreatmentHistoryMeta",
    "SamplingMeta",
    "CalendarComparisonMeta",
    "EligibilitySummary",
    "ExecutionSnapshot",
    "Provenance",
    "MaterializedFrame",
    "Budget",
    "CoverageSummary",
    "Preview",
]

_INVALID_ELIGIBILITY = "terrine.metadata.invalid_eligibility_summary"
_UNREPRESENTABLE_COVERAGE = "terrine.metadata.unrepresentable_coverage"
_ARROW_METADATA_KEY = b"terrine.materialized_frame.v1"


def _nonempty(value: str, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")


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
    """Inclusive complete local calendar-day declaration; binding owns timezone."""

    start: date
    through: date

    def __init__(self, start: date | str, through: date | str) -> None:
        object.__setattr__(
            self, "start", date.fromisoformat(start) if isinstance(start, str) else start
        )
        object.__setattr__(
            self, "through", date.fromisoformat(through) if isinstance(through, str) else through
        )
        if (
            not isinstance(self.start, date)
            or isinstance(self.start, datetime)
            or not isinstance(self.through, date)
            or isinstance(self.through, datetime)
        ):
            raise TypeError("coverage endpoints must be date values")
        if self.start > self.through:
            raise ValueError("coverage start must not follow through")
        if self.through == date.max:
            raise TerrineError(
                "Coverage requires a following calendar day; use through before date.max",
                code=_UNREPRESENTABLE_COVERAGE,
                context={"through": self.through.isoformat()},
            )

    def bind(self, day_boundary: Callable[[date], datetime]) -> BoundCoverage:
        if not callable(day_boundary):
            raise TypeError("day_boundary must be callable")
        try:
            start = day_boundary(self.start)
            end = day_boundary(self.through + timedelta(days=1))
        except OverflowError as exc:
            raise TerrineError(
                "Coverage boundary exceeds datetime's range; use interior calendar dates",
                code=_UNREPRESENTABLE_COVERAGE,
                context={"start": self.start.isoformat(), "through": self.through.isoformat()},
            ) from exc
        return BoundCoverage(start, end)


@dataclass(frozen=True, slots=True)
class BoundCoverage:
    """Coverage translated once to a half-open pair of UTC instants."""

    start: datetime
    end_exclusive: datetime

    def __post_init__(self) -> None:
        _aware(self.start, "start")
        _aware(self.end_exclusive, "end_exclusive")
        try:
            start = self.start.astimezone(UTC)
            end = self.end_exclusive.astimezone(UTC)
        except OverflowError as exc:
            raise TerrineError(
                "Coverage exceeds UTC datetime's range; use interior instants",
                code=_UNREPRESENTABLE_COVERAGE,
                context={
                    "start": self.start.isoformat(),
                    "end_exclusive": self.end_exclusive.isoformat(),
                },
            ) from exc
        if start >= end:
            raise ValueError("bound coverage start must precede end_exclusive")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end_exclusive", end)


@dataclass(frozen=True, slots=True)
class UnitSource:
    table: str
    key: str
    entered_at: str | None = None
    exited_at: str | None = None
    observed_from: str | None = None
    observed_until: str | None = None

    def __post_init__(self) -> None:
        _nonempty(self.table, "table")
        _nonempty(self.key, "key")
        for name in ("entered_at", "exited_at", "observed_from", "observed_until"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)


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
class ColumnMeta:
    window: Window | None
    role: str
    observed_from_column: str | None
    observed_until_column: str | None
    complete_column: str | None
    reason_column: str | None
    numeric_policy: Literal["exact", "checked_float64_v1"] | None = None
    absolute_error_column: str | None = None

    def __post_init__(self) -> None:
        if self.window is not None and not isinstance(self.window, Window):
            raise TypeError("window must be a Window or None")
        _nonempty(self.role, "role")
        for name in (
            "observed_from_column",
            "observed_until_column",
            "complete_column",
            "reason_column",
            "absolute_error_column",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if self.numeric_policy not in (None, "exact", "checked_float64_v1"):
            raise ValueError("unsupported numeric policy")
        if (self.numeric_policy == "checked_float64_v1") != (
            self.absolute_error_column is not None
        ):
            raise ValueError("checked float64 metadata requires exactly one error-bound column")


@dataclass(frozen=True, slots=True)
class FeatureCutoff:
    latest_seconds: int
    strict: bool
    knowledge: Literal["recorded_at", "retrospective_effective_time"]
    reference: Literal["row_t0", "panel_start"] = "row_t0"

    def __post_init__(self) -> None:
        value = seconds(self.latest_seconds)
        if value != self.latest_seconds or not isinstance(self.strict, bool):
            raise ValueError("invalid feature cutoff")
        if self.knowledge not in ("recorded_at", "retrospective_effective_time"):
            raise ValueError("unsupported cutoff knowledge")
        if self.reference not in ("row_t0", "panel_start"):
            raise ValueError("unsupported cutoff reference")


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
            raise TerrineError(
                "Eligibility counts must be nonnegative and reconcile",
                code=_INVALID_ELIGIBILITY,
                context={
                    "candidate": self.candidate,
                    "excluded_false": self.excluded_false,
                    "unknown": self.unknown,
                    "retained": self.retained,
                },
            )


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
class Provenance:
    feature_cutoffs: Mapping[str, FeatureCutoff]
    eligibility_cutoffs: Mapping[str, FeatureCutoff]
    design_hash: str
    model_hash: str
    source_generations: Mapping[str, str | None]
    execution_snapshot: ExecutionSnapshot
    shape: str
    treatment_design: Literal["baseline_binary", "grace_initiation"] | None
    eligibility_summary: EligibilitySummary | None = None
    treatment_knowledge: Literal["recorded_at", "retrospective_effective_time"] | None = None
    weight_kind: Literal["baseline_function", "sampled_control"] | None = None
    weight_cutoff: FeatureCutoff | None = None
    weight_depends_on_treatment: bool | None = None
    treatment_known_at_t0: bool = False

    state_panel: StatePanelMeta | None = None
    panel: PanelMeta | None = None
    sampling: SamplingMeta | None = None
    calendar_comparison: CalendarComparisonMeta | None = None

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
        if self.state_panel is not None and not isinstance(self.state_panel, StatePanelMeta):
            raise TypeError("state_panel must be StatePanelMeta or None")
        if self.panel is not None and not isinstance(self.panel, PanelMeta):
            raise TypeError("panel must be PanelMeta or None")
        if self.sampling is not None and not isinstance(self.sampling, SamplingMeta):
            raise TypeError("sampling must be SamplingMeta or None")
        if self.calendar_comparison is not None and not isinstance(
            self.calendar_comparison, CalendarComparisonMeta
        ):
            raise TypeError("calendar_comparison must be CalendarComparisonMeta or None")


@dataclass(frozen=True, slots=True)
class Budget:
    max_rows: int | None = None
    max_output_bytes: int | None = None
    max_scan_bytes: int | None = None
    strict_scan: bool = True

    def __post_init__(self) -> None:
        for name in ("max_rows", "max_output_bytes", "max_scan_bytes"):
            _count(getattr(self, name), name)
        if not isinstance(self.strict_scan, bool):
            raise TypeError("strict_scan must be bool")


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


def _decode(value: Any, cls: type) -> Any:
    if cls is Window:
        return Window(*value["$window"])
    if not is_dataclass(cls):
        raise TypeError("metadata target must be a dataclass")
    kwargs = {}
    unknown = set(value) - {field.name for field in fields(cls)}
    if unknown:
        raise ValueError(f"unknown {cls.__name__} fields: {sorted(unknown)}")
    for field in fields(cls):
        if field.name not in value:
            continue
        item = value[field.name]
        if field.name in ("feature_cutoffs", "eligibility_cutoffs"):
            kwargs[field.name] = {k: _decode(v, FeatureCutoff) for k, v in item.items()}
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
        elif field.name in ("panel_start", "administrative_end") and isinstance(item, dict):
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

    def to_metadata(self) -> dict[str, Any]:
        """Return a detached JSON-compatible metadata value."""
        return thaw(freeze(_metadata_payload(self.roles, self.columns, self.provenance)))

    def to_arrow(self) -> pa.Table:
        """Return the Arrow table with canonical JSON metadata attached to its schema."""
        payload = self.to_metadata()
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        metadata = dict(self.table.schema.metadata or {})
        metadata[_ARROW_METADATA_KEY] = encoded
        return self.table.replace_schema_metadata(metadata)

    @classmethod
    def from_arrow(cls, table: pa.Table) -> MaterializedFrame:
        if not isinstance(table, pa.Table):
            raise TypeError("table must be a pyarrow.Table")
        raw = (table.schema.metadata or {}).get(_ARROW_METADATA_KEY)
        if raw is None:
            raise ValueError("Arrow schema has no Terrine metadata")
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("version") != 1:
            raise ValueError("unsupported Terrine metadata version")
        roles = _decode(payload["roles"], Roles)
        columns = {key: _decode(item, ColumnMeta) for key, item in payload["columns"].items()}
        provenance = _decode(payload["provenance"], Provenance)
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
