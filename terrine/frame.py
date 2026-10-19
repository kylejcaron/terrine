"""Lazy study frames with one verified execution lifetime per export."""

from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from importlib import import_module
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pyarrow as pa

from terrine.compile.cost import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, _scan_policy
from terrine.data import (
    Budget,
    CalendarComparisonMeta,
    CohortOperationMeta,
    ColumnMeta,
    DatasetViewMeta,
    MaterializedFrame,
    PanelMeta,
    Provenance,
    Roles,
    SamplingMeta,
    StatePanelMeta,
    TreatmentHistoryMeta,
    TrialWeightMeta,
)
from terrine.errors import RefusalSpec
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.spec.anchors import FixedDate
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import BinnedMetricState, StatePanel, encode_cutpoint
from terrine.spec.trajectory import CohortGrouping, CohortSpec, LifeTable, TimeToEvent

if TYPE_CHECKING:
    from terrine.compile.columns import CompiledStudy
    from terrine.model import Model
    from terrine.spec.design import Design

__all__ = ["Frame", "CohortTable"]

_MISSING_DEPENDENCY = RefusalSpec(
    "terrine.convert.missing_dependency",
    "Conversion requires the optional {extra!r} dependency; install terrine[{extra}].",
)


def _optional_dependency(name: str):
    try:
        return import_module(name)
    except ImportError as exc:
        raise _MISSING_DEPENDENCY.error(extra=name) from exc


class _SplitPolicy(Protocol):
    def to_record(self) -> dict: ...


@dataclass(frozen=True, slots=True)
class _TrainingView:
    features: tuple[str, ...]
    labels: tuple[str, ...]
    incomplete_labels: Literal["drop", "keep"]
    split: object | None


@dataclass(frozen=True, slots=True)
class _ScoringView:
    parent_design_hash: str
    features: tuple[str, ...]
    resolved_as_of: datetime


def _scoring_view_digest(view: _ScoringView, scoring_design_hash: str) -> str:
    payload = {
        "kind": "scoring",
        "parent_design": view.parent_design_hash,
        "scoring_design": scoring_design_hash,
        "features": view.features,
        "resolved_as_of": view.resolved_as_of.isoformat(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _view_digest(
    features: tuple[str, ...],
    labels: tuple[str, ...],
    policy: Literal["drop", "keep"],
    split: object | None,
    design_hash: str,
) -> str:
    payload = {
        "kind": "training",
        "design": design_hash,
        "features": features,
        "labels": labels,
        "incomplete_labels": policy,
        "split": None if split is None else cast(_SplitPolicy, split).to_record(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _definition_record(value: object) -> object:
    from collections.abc import Mapping
    from dataclasses import fields, is_dataclass
    from datetime import date, datetime
    from enum import Enum

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _definition_record(model_dump(mode="json"))
    if is_dataclass(value):
        return {
            field.name: _definition_record(getattr(value, field.name)) for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _definition_record(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_definition_record(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"Unsupported feature definition value: {type(value).__name__}")


def _feature_definition_hash(design: Design, features: tuple[str, ...], model: Model) -> str:
    from terrine.spec.expressions import AttributeAsOf, EventTime, MetricAgg
    from terrine.spec.io import canonical_json

    record = json.loads(canonical_json(design))["design"]
    semantic: dict[str, object] = {}
    selected_records: dict[str, object] = {}
    for name in features:
        expression = design.features.get(name)
        if expression is not None:
            selected_records[name] = record["features"][name]
        elif isinstance(design.shape, StatePanel):
            if name in design.shape.baseline_features:
                selected_records[name] = record["shape"]["baseline_features"][name]
                expression = design.shape.baseline_features[name]
            elif name == "from_state":
                selected_records[name] = {"prior_state": record["shape"]["state"]}
            else:
                raise ValueError(f"Unknown selected feature {name!r}")
        else:
            raise ValueError(f"Unknown selected feature {name!r}")
        resolved: dict[str, object] = {}
        if isinstance(expression, MetricAgg):
            metric = model.metrics.get(expression.metric)
            if metric is not None:
                resolved["metric"] = _definition_record(metric)
        elif isinstance(expression, AttributeAsOf):
            resolved["attributes"] = [
                {
                    "source": source_name,
                    "relation": _definition_record(source.relation),
                    "day_boundary": source.day_boundary,
                    "timestamp_column": source.timestamp_column,
                    "binding": _definition_record(source.properties[expression.attribute]),
                }
                for source_name, source in sorted(model.sources.items())
                if expression.attribute in source.properties
            ]
        elif isinstance(expression, EventTime):
            resolved["facts"] = [
                {
                    "source": source_name,
                    "relation": _definition_record(source.relation),
                    "day_boundary": source.day_boundary,
                    "timestamp_column": source.timestamp_column,
                    "column": source.facts[expression.fact],
                }
                for source_name, source in sorted(model.sources.items())
                if expression.fact in source.facts
            ]
        semantic[name] = resolved
    payload = {
        "unit": record["unit"],
        "unit_binding": _definition_record(model.units.get(design.unit)),
        "gap": record["gap"],
        "features": selected_records,
        "semantic": semantic,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _apply_training_view(compiled: CompiledStudy, view: _TrainingView) -> CompiledStudy:
    table = compiled.table
    roles = compiled.roles
    label_metadata = [compiled.columns[name] for name in view.labels]
    if view.incomplete_labels == "drop":
        for name, metadata in zip(view.labels, label_metadata, strict=True):
            assert metadata.complete_column is not None
            usable = table[metadata.complete_column].fill_null(False) & table[name].notnull()
            table = table.filter(usable)
    role_columns = (
        roles.treatment,
        roles.weight,
        roles.cluster_unit,
        roles.prior_state,
        roles.trial,
        roles.assignment_probability,
        roles.target_weight,
        roles.trial_weight,
    )
    structural = (
        "period",
        "from_time",
        "to_time",
        "scheduled_to",
        "gap",
        "terminal",
    )
    names = list(dict.fromkeys((roles.unit, roles.anchor, roles.t0, *view.features, *view.labels)))
    for name in (*role_columns, *structural):
        if name is not None and name in compiled.table.columns and name not in names:
            names.append(name)
    timings = {
        name: compiled.expression_timings[name]
        for name in (*view.features, *view.labels)
        if name in compiled.expression_timings
    }
    for timing in timings.values():
        for support in (
            timing.point_time_column,
            timing.start_bound_column,
            timing.end_bound_column,
            *timing.selector_dependencies,
        ):
            if support is not None and support in compiled.table.columns and support not in names:
                names.append(support)
    for name in (*view.features, *view.labels):
        metadata = compiled.columns[name]
        for support in (
            metadata.observed_from_column,
            metadata.observed_until_column,
            metadata.complete_column,
            metadata.reason_column,
            metadata.absolute_error_column,
        ):
            if support is not None and support not in names:
                names.append(support)
    table = table.select(*names)
    selected_columns = {name: compiled.columns[name] for name in names if name in compiled.columns}
    selected_roles = replace(
        roles,
        features=view.features,
        outcomes=view.labels,
        treatment=roles.treatment if roles.treatment in names else None,
        weight=roles.weight if roles.weight in names else None,
        cluster_unit=roles.cluster_unit if roles.cluster_unit in names else None,
        prior_state=roles.prior_state if roles.prior_state in names else None,
        trial=roles.trial if roles.trial in names else None,
        assignment_probability=(
            roles.assignment_probability if roles.assignment_probability in names else None
        ),
        target_weight=roles.target_weight if roles.target_weight in names else None,
        trial_weight=roles.trial_weight if roles.trial_weight in names else None,
    )
    return replace(
        compiled,
        table=table,
        roles=selected_roles,
        columns=selected_columns,
        expression_timings=timings,
    )


def _excluded_label_counts(compiled: CompiledStudy, view: _TrainingView, execute) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in view.labels:
        metadata = compiled.columns[name]
        assert metadata.complete_column is not None and metadata.reason_column is not None
        excluded = compiled.table.filter(
            ~compiled.table[metadata.complete_column].fill_null(False)
            | compiled.table[name].isnull()
        )
        grouped = excluded.group_by(metadata.reason_column).aggregate(rows=excluded.count())
        for row in execute(grouped).to_pylist():
            reason = row[metadata.reason_column]
            key = "unknown" if reason is None else reason
            counts[key] = counts.get(key, 0) + row["rows"]
    return dict(sorted(counts.items()))


def _state_panel_metadata(design: Design, model: Model) -> StatePanelMeta | None:
    from terrine.spec.anchors import Schedule

    if not isinstance(design.shape, StatePanel):
        return None
    schedule = design.anchor
    assert isinstance(schedule, Schedule)
    state = design.shape.state
    if isinstance(state, BinnedMetricState):
        source = {
            "kind": "binned_metric",
            "metric": state.metric,
            "cutpoints": [encode_cutpoint(value) for value in state.cutpoints],
        }
        levels, ordered, absorbing = state.levels, True, ()
        timing = "strict_period"
    else:
        source = {
            "kind": "observations",
            "property": state.property,
            "point_time_columns": {"from_state": "from_time", "to_state": "to_time"},
        }
        levels, ordered, absorbing = state.levels, state.ordered, state.absorbing
        timing = "at_origin"
    return StatePanelMeta(
        state_source=source,
        levels=levels,
        ordered=ordered,
        absorbing=absorbing,
        panel_start=model.day_boundary(schedule.start).astimezone(UTC),
        administrative_end=model.day_boundary(schedule.end).astimezone(UTC),
        period_seconds=schedule.every,
        baseline_features=tuple(design.shape.baseline_features),
        time_varying_features=tuple(design.features),
        prior_state_timing=timing,
        period_column="period_index",
        from_time_column="from_time",
        to_time_column="to_time",
        scheduled_to_column="scheduled_to",
        gap_column="gap",
        terminal_column="terminal",
    )


def _panel_metadata(design: Design) -> PanelMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import EventPanelSpec, Initiation, PersistentAdoption, SequentialSpec

    if isinstance(design.shape, SequentialSpec):
        treatment = design.shape.treatment
        history = TreatmentHistoryMeta(
            "initiation" if isinstance(treatment, Initiation) else "persistent_adoption",
            treatment.source,
            treatment.property,
            treatment.history_start,
            treatment.knowledge,
            treatment.adopted_value if isinstance(treatment, PersistentAdoption) else None,
            treatment.persistence if isinstance(treatment, PersistentAdoption) else None,
        )
        trial_weight = (
            None
            if design.shape.trial_weight is None
            else TrialWeightMeta(
                design.shape.trial_weight.kind,
                None
                if design.shape.trial_weight.values is None
                else dict(design.shape.trial_weight.values),
            )
        )
        assert isinstance(design.anchor, Schedule)
        span = (design.anchor.end - design.anchor.start).days * 86_400
        count = (span - 1) // design.anchor.every + 1
        trials = tuple(f"schedule:{index}" for index in range(count))
        return PanelMeta(
            "sequential",
            history,
            trial_weight,
            trials=trials,
            anticipation_seconds=design.shape.anticipation_seconds,
        )
    if isinstance(design.shape, EventPanelSpec):
        return PanelMeta(
            "event_panel",
            periods=design.shape.periods,
            period_seconds=design.shape.period_seconds,
            reference_period=design.shape.reference_period,
        )
    return None


def _calendar_comparison_metadata(design: Design) -> CalendarComparisonMeta | None:
    from terrine.spec.anchors import Schedule
    from terrine.spec.panels import CalendarComparisonSpec

    if not isinstance(design.shape, CalendarComparisonSpec):
        return None
    assert isinstance(design.anchor, Schedule)
    cohort = design.shape.cohort
    return CalendarComparisonMeta(
        design.shape.control,
        cohort.source,
        cohort.property,
        cohort.history_start,
        cohort.knowledge,
        design.shape.reference_period,
        design.shape.comparison_periods,
        design.shape.anticipation_periods,
        design.anchor.every,
    )


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class Frame:
    """A compiled study, not executed data.

    ``expr`` is an unsafe Ibis escape hatch. Only materialization/conversion
    methods hold execution authority and enforce integrity and output budgets.
    Output limits do not bound upstream scan, query work or peak memory.
    """

    _design: Design
    _model: Model
    _compiled: CompiledStudy
    _training_view: _TrainingView | None
    _scoring_view: _ScoringView | None

    def __init__(self, design: Design, model: Model) -> None:
        from terrine.compile.dispatch import compile_design

        object.__setattr__(self, "_design", design)
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_compiled", compile_design(design, model))
        object.__setattr__(self, "_training_view", None)
        object.__setattr__(self, "_scoring_view", None)

    def _active_compiled(self, compiled: CompiledStudy | None = None) -> CompiledStudy:
        active = self._compiled if compiled is None else compiled
        if self._training_view is None:
            return active
        return _apply_training_view(active, self._training_view)

    @property
    def expr(self):
        """Return the Ibis graph; direct execution bypasses all export safeguards."""
        return self._active_compiled().table

    @property
    def roles(self):
        return self._active_compiled().roles

    @property
    def columns(self):
        return self._active_compiled().columns

    def training_set(
        self,
        *,
        label: str | tuple[str, ...],
        features: tuple[str, ...] | None = None,
        incomplete_labels: Literal["drop", "keep"] = "drop",
        split: object | None = None,
    ) -> Frame:
        """Return a lazy feature/label projection with explicit label completeness."""
        if self._training_view is not None or self._scoring_view is not None:
            from terrine.spec.dataset import INVALID_VIEW

            raise INVALID_VIEW.error(
                reason="views cannot be composed; start from the original Design"
            )
        if split is not None:
            raise RefusalSpec(
                "terrine.dataset.split_not_applied",
                "Frame training views do not apply split assignments yet; omit split or apply "
                "the policy with terrine.dataset.splits.assign_splits.",
            ).error()

        labels = (label,) if isinstance(label, str) else tuple(label)
        if not labels or any(not isinstance(name, str) or not name for name in labels):
            raise ValueError("label must select one or more non-empty outcome names")
        if len(set(labels)) != len(labels):
            raise ValueError("label names must be unique")
        unknown = set(labels) - set(self.roles.outcomes)
        if unknown:
            raise RefusalSpec(
                "terrine.dataset.invalid_selection",
                "Unknown training label selection: {names}",
            ).error(names=tuple(sorted(unknown)))
        selected_features = self.roles.features if features is None else tuple(features)
        if any(not isinstance(name, str) or not name for name in selected_features):
            raise ValueError("features must contain non-empty column names")
        if len(set(selected_features)) != len(selected_features):
            raise ValueError("feature names must be unique")
        available_features = set(self.roles.features)
        if self.roles.prior_state is not None:
            available_features.add(self.roles.prior_state)
        unknown = set(selected_features) - available_features
        if unknown:
            raise RefusalSpec(
                "terrine.dataset.invalid_selection",
                "Unknown training feature selection: {names}",
            ).error(names=tuple(sorted(unknown)))
        if incomplete_labels not in ("drop", "keep"):
            raise ValueError("incomplete_labels must be 'drop' or 'keep'")
        for name in labels:
            metadata = self.columns[name]
            if metadata.complete_column is None or metadata.reason_column is None:
                raise RefusalSpec(
                    "terrine.dataset.invalid_label_support",
                    "Selected label lacks completeness and reason metadata: {label}",
                ).error(label=name)
        result = Frame(self._design, self._model)
        object.__setattr__(
            result,
            "_training_view",
            _TrainingView(selected_features, labels, incomplete_labels, split),
        )
        return result

    def stage(self, table: str, *, budget: Budget | None = None):
        """Publish this Frame as a pinned, bounded-stream Dataset generation."""
        stage_frame = import_module("terrine.dataset.reference").stage_frame
        return stage_frame(self, table, budget=budget)

    def materialize(self, budget: Budget | None = None) -> MaterializedFrame:
        return self._materialize(budget)

    def _materialize(
        self,
        budget: Budget | None = None,
        *,
        transform=None,
        shape: str | None = None,
        cohort_operation: CohortOperationMeta | None = None,
    ) -> MaterializedFrame:
        """Verify, audit and consume one bounded Arrow output stream atomically.

        No partial output is returned on limits, invalid history or stream errors.
        Concurrent writes after the execution view is established are excluded.
        """
        enforce_shape_budget(self._design.shape, budget)
        for warning in _scan_policy(budget):
            warnings.warn(warning, RuntimeWarning, stacklevel=2)
        from terrine.compile.dispatch import compile_design
        from terrine.execution.inputs import open_execution

        model = self._model
        with open_execution(
            model.catalog, model.con, units=model.units, input_binding=model.input_binding
        ) as context:
            from terrine.spec.validate import validate_design

            execution_model = model._for_execution(
                context,
                source_names=set(validate_design(self._design, model).required_sources),
                unit_name=self._design.unit,
            )
            compiled = compile_design(self._design, execution_model)
            excluded_counts = {}
            if self._training_view is not None:
                compiled.validate(execute=context.execute)
                if self._training_view.incomplete_labels == "drop":
                    excluded_counts = _excluded_label_counts(
                        compiled, self._training_view, context.execute
                    )
                compiled = _apply_training_view(compiled, self._training_view)
            if transform is not None:
                compiled = transform(compiled)
            summary = validate_compiled_output(compiled, context.execute)
            query = compiled.table
            chunk_size = 65_536
            is_life_table = isinstance(self._design.shape, LifeTable)
            if budget is not None and budget.max_rows is not None and not is_life_table:
                limit = min(budget.max_rows + 1, 2**63 - 1)
                query = query.limit(limit)
                chunk_size = min(chunk_size, limit)
            batches = []
            row_count = byte_count = 0
            context._check_query(query)
            reader = context.con.to_pyarrow_batches(query, chunk_size=chunk_size)
            try:
                schema = reader.schema
                for batch in reader:
                    if batch.schema != schema:
                        batch = batch.cast(schema)
                    row_count += batch.num_rows
                    byte_count += batch.nbytes
                    if budget is not None and not is_life_table:
                        if budget.max_rows is not None and row_count > budget.max_rows:
                            raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                        if (
                            budget.max_output_bytes is not None
                            and byte_count > budget.max_output_bytes
                        ):
                            raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                    batches.append(batch)
                table = pa.Table.from_batches(batches, schema=schema)
            finally:
                reader.close()
            roles = compiled.roles
            columns = compiled.columns
            if isinstance(self._design.shape, LifeTable):
                from terrine.compile.dispatch import aggregate_life_table

                life_table = self._design.shape
                group_supports: dict[str, str] = {}
                for name in life_table.group_by:
                    meta = compiled.columns[name]
                    for column, policy in (
                        (meta.observed_from_column, "min"),
                        (meta.observed_until_column, "max"),
                        (meta.complete_column, "all"),
                        (meta.reason_column, "reasons"),
                        (meta.absolute_error_column, "max"),
                    ):
                        if column is not None:
                            group_supports.setdefault(column, policy)
                table = aggregate_life_table(
                    table,
                    interval_seconds=life_table.interval,
                    group_by=life_table.group_by,
                    group_supports=group_supports,
                    max_rows=None if budget is None else budget.max_rows,
                )
                if budget is not None:
                    if budget.max_rows is not None and table.num_rows > budget.max_rows:
                        raise _ROWS_EXCEEDED.error(limit=budget.max_rows)
                    if (
                        budget.max_output_bytes is not None
                        and table.nbytes > budget.max_output_bytes
                    ):
                        raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
                roles = Roles(
                    "unit",
                    "anchor_id",
                    "t0",
                    None,
                    (*life_table.group_by, "time_bin"),
                    (
                        "at_risk",
                        "events",
                        "censored",
                        "delayed_entries",
                        "event_rate",
                        "censor_rate",
                    ),
                )
                columns = {name: compiled.columns[name] for name in life_table.group_by}
                columns["time_bin"] = ColumnMeta(None, "feature", None, None, None, None)
                columns.update(
                    {
                        name: ColumnMeta(None, "outcome", None, None, None, None)
                        for name in roles.outcomes
                    }
                )
            if isinstance(self._design.shape, TimeToEvent):
                for name in ("event_time", "censor_time", "duration", "entry_time"):
                    if name not in table.column_names:
                        continue
                    dtype = table.schema.field(name).type
                    if pa.types.is_timestamp(dtype) and dtype.tz is None:
                        timestamp = table[name].cast(pa.timestamp(dtype.unit, tz="UTC"))
                        table = table.set_column(
                            table.schema.get_field_index(name), name, timestamp
                        )
            if isinstance(self._design.shape, StatePanel):
                gap_index = table.schema.get_field_index("gap")
                gap = table.column(gap_index).cast(pa.duration("us"))
                table = table.set_column(gap_index, "gap", gap)
            treatment = self._design.treatment
            treatment_column = compiled.roles.treatment
            baseline_binary = (
                treatment is not None
                and treatment_column is not None
                and isinstance(self._design.anchor, FixedDate)
                and self._design.shape == "cross_section"
                and treatment.at == 0
                and treatment.known_at_t0
                and pa.types.is_boolean(table.schema.field(treatment_column).type)
            )
            calendar_comparison = _calendar_comparison_metadata(self._design)
            panel = _panel_metadata(self._design)

            if isinstance(self._design.shape, StatePanel):
                shape_name = "state_panel"
            elif isinstance(self._design.shape, EventSequence):
                shape_name = "event_sequence"
            elif calendar_comparison is not None:
                shape_name = "calendar_comparison"
            elif panel is not None:
                shape_name = panel.shape
            elif isinstance(self._design.shape, CohortSpec):
                shape_name = "ltv"
            elif isinstance(self._design.shape, LifeTable):
                shape_name = "life_table"
            elif isinstance(self._design.shape, TimeToEvent):
                shape_name = "time_to_event"
            else:
                shape_name = (
                    self._design.shape if isinstance(self._design.shape, str) else "cross_section"
                )
            feature_cutoffs = compiled.feature_cutoffs
            expression_timings = compiled.expression_timings
            if isinstance(self._design.shape, LifeTable):
                feature_cutoffs = {
                    name: cutoff
                    for name, cutoff in compiled.feature_cutoffs.items()
                    if name in self._design.shape.group_by
                }
                expression_timings = {
                    name: timing
                    for name, timing in compiled.expression_timings.items()
                    if name in self._design.shape.group_by
                }
            provenance = Provenance(
                feature_cutoffs=feature_cutoffs,
                eligibility_cutoffs=compiled.eligibility_cutoffs,
                design_hash=self._design.spec_hash,
                model_hash=model.model_hash,
                source_generations=context.snapshot.source_generations,
                execution_snapshot=context.snapshot,
                shape=shape or shape_name,
                treatment_design="baseline_binary" if baseline_binary else None,
                eligibility_summary=summary,
                treatment_knowledge="retrospective_effective_time"
                if treatment is not None
                else None,
                treatment_known_at_t0=treatment.known_at_t0 if treatment is not None else False,
                state_panel=_state_panel_metadata(self._design, model),
                expression_timings=expression_timings,
                sequence_meta=compiled.sequence_meta,
                panel=panel,
                sampling=(
                    SamplingMeta(
                        self._design.sample.strata,
                        self._design.sample.controls_per_treated,
                        self._design.sample.seed,
                    )
                    if self._design.sample is not None
                    else None
                ),
                calendar_comparison=calendar_comparison,
                administrative_cutoff=self._design.administrative_cutoff,
                cohort_operation=cohort_operation,
            )
            if self._training_view is not None:
                view = self._training_view
                split_policy = (
                    None if view.split is None else cast(_SplitPolicy, view.split).to_record()
                )
                provenance = replace(
                    provenance,
                    dataset_view=DatasetViewMeta(
                        kind="training",
                        selected_features=view.features,
                        selected_labels=view.labels,
                        view_hash=_view_digest(
                            view.features,
                            view.labels,
                            view.incomplete_labels,
                            view.split,
                            self._design.spec_hash,
                        ),
                        feature_definition_hash=_feature_definition_hash(
                            self._design, view.features, self._model
                        ),
                        incomplete_labels=view.incomplete_labels,
                        split_policy=split_policy,
                        counts_by_reason=excluded_counts,
                    ),
                )
            elif self._scoring_view is not None:
                view = self._scoring_view
                provenance = replace(
                    provenance,
                    dataset_view=DatasetViewMeta(
                        kind="scoring",
                        selected_features=view.features,
                        selected_labels=(),
                        view_hash=_scoring_view_digest(view, self._design.spec_hash),
                        feature_definition_hash=_feature_definition_hash(
                            self._design, view.features, model
                        ),
                        resolved_as_of=view.resolved_as_of,
                    ),
                )
            return MaterializedFrame(table, roles, columns, provenance)

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        """Execute once and attach portable study metadata to the Arrow schema."""
        return self.materialize(budget).to_arrow()

    def to_pandas(self, *, budget: Budget | None = None):
        """Convert using nullable Arrow-backed dtypes; import pandas only on demand."""
        pandas = _optional_dependency("pandas")
        return self.to_pyarrow(budget=budget).to_pandas(types_mapper=pandas.ArrowDtype)

    def to_polars(self, *, budget: Budget | None = None):
        """Convert using the optional native Polars Arrow conversion."""
        polars = _optional_dependency("polars")
        return polars.from_arrow(self.to_pyarrow(budget=budget))

    def cohorts(self, *, grouping: CohortGrouping, value_column: str) -> CohortTable:
        if not isinstance(self._design.shape, CohortSpec):
            raise TypeError("cohorts requires a trajectory Frame")
        if not isinstance(grouping, CohortGrouping):
            raise TypeError("grouping must be CohortGrouping")
        if not isinstance(value_column, str) or value_column not in {
            "period_value",
            "cumulative_value",
        }:
            raise ValueError("value_column must name a trajectory value")
        if value_column == "cumulative_value" and not self._design.shape.trajectory.cumulative:
            raise ValueError("the trajectory did not request cumulative values")
        return CohortTable(self, grouping, value_column)


@dataclass(frozen=True, slots=True)
class CohortTable:
    parent_frame: Frame
    grouping: CohortGrouping
    value_column: str

    def materialize(self, *, budget: Budget | None = None) -> MaterializedFrame:
        parent_design_hash = self.parent_frame._design.spec_hash
        payload = {
            "bucket_seconds": self.grouping.bucket,
            "bucket_origin": self.grouping.bucket_origin.isoformat(),
            "statistic": self.grouping.statistic,
            "value_column": self.value_column,
            "parent_design_hash": parent_design_hash,
        }
        operation_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        operation = CohortOperationMeta(
            self.grouping.bucket,
            self.grouping.bucket_origin,
            self.grouping.statistic,
            self.value_column,
            parent_design_hash,
            operation_hash,
        )
        from terrine.compile.dispatch import compile_cohort_summary

        return self.parent_frame._materialize(
            budget,
            transform=lambda study: compile_cohort_summary(
                study, grouping=self.grouping, value_column=self.value_column
            ),
            shape="cohort_table",
            cohort_operation=operation,
        )

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        return self.materialize(budget=budget).to_arrow()
