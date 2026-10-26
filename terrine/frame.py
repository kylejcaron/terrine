"""Lazy study frames with one verified execution lifetime per export."""

from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import pyarrow as pa

import terrine.compile.provenance as _provenance
from terrine.compile.cost import _OUTPUT_BYTES_EXCEEDED, _ROWS_EXCEEDED, _scan_policy
from terrine.data import (
    Budget,
    CohortOperationMeta,
    ColumnMeta,
    DatasetViewMeta,
    MaterializedFrame,
    Provenance,
    Roles,
    SamplingMeta,
)
from terrine.errors import RefusalSpec
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.spec.anchors import FixedDate
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import BinnedMetricState, StatePanel
from terrine.spec.trajectory import CohortGrouping, CohortSpec, LifeTable, TimeToEvent

if TYPE_CHECKING:
    from terrine.compile.columns import CompiledStudy
    from terrine.model import Model
    from terrine.spec.design import Design
    from terrine.spec.expressions import EventTime
    from terrine.spec.transforms import Winsorization

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


def _metric_definition(metric: object, model: Model) -> object:
    if metric is None:
        return None
    return {
        "metric": _definition_record(metric),
        "sources": {
            name: _definition_record(model.sources[name])
            for name in getattr(metric, "source_names", ())
        },
    }


def _selector_definition(selector: EventTime, model: Model) -> object:
    fact = model.catalog.facts[selector.fact]
    return {
        "fact": _definition_record(fact),
        "source": _definition_record(model.sources[fact.source]),
        "event_id": model.event_ids.get(fact.source),
        "relative_to": None
        if selector.relative_to is None
        else _selector_definition(selector.relative_to, model),
    }


def _feature_definition_hash(design: Design, features: tuple[str, ...], model: Model) -> str:
    from terrine.compile._asof import resolve_attribute
    from terrine.spec.expressions import (
        ApproxMetricDistinct,
        ApproxMetricQuantile,
        AttributeAsOf,
        EventTime,
        MetricAgg,
        MetricQuantile,
    )
    from terrine.spec.io import canonical_json

    record = json.loads(canonical_json(design))["design"]
    semantic: dict[str, object] = {}
    selected_records: dict[str, object] = {}
    for name in features:
        expression = design.features.get(name)
        evaluation_reference = "row_t0"
        if expression is not None:
            selected_records[name] = record["features"][name]
        elif isinstance(design.shape, StatePanel):
            if name in design.shape.baseline_features:
                selected_records[name] = record["shape"]["baseline_features"][name]
                expression = design.shape.baseline_features[name]
                evaluation_reference = "panel_start"
            elif name == "from_state":
                state = design.shape.state
                selected_records[name] = {"prior_state": record["shape"]["state"]}
                if isinstance(state, BinnedMetricState):
                    metric = model.metrics.get(state.metric)
                    semantic[name] = {
                        "state": record["shape"]["state"],
                        "metric": _metric_definition(metric, model),
                    }
                else:
                    semantic[name] = {
                        "state": record["shape"]["state"],
                        "property_bindings": [
                            {
                                "source": source_name,
                                "relation": _definition_record(source.relation),
                                "day_boundary": source.day_boundary,
                                "timestamp_column": source.timestamp_column,
                                "binding": _definition_record(source.properties[state.property]),
                            }
                            for source_name, source in sorted(model.sources.items())
                            if state.property in source.properties
                        ],
                    }
            else:
                raise ValueError(f"Unknown selected feature {name!r}")
        else:
            raise ValueError(f"Unknown selected feature {name!r}")
        if expression is not None:
            selected_records[name] = {
                "expression": selected_records[name],
                "evaluation_reference": evaluation_reference,
            }
        resolved: dict[str, object] = {}
        if isinstance(
            expression, (MetricAgg, MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)
        ):
            metric = model.metrics.get(expression.metric)
            if metric is not None:
                resolved["metric"] = _metric_definition(metric, model)
        elif isinstance(expression, AttributeAsOf):
            source, prop, dimension = resolve_attribute(model, expression.attribute)
            resolved["attribute"] = {
                "source": source.name,
                "relation": _definition_record(source.relation),
                "entity_column": source.entities.get(design.unit),
                "day_boundary": source.day_boundary,
                "timestamp_column": source.timestamp_column,
                "binding": _definition_record(prop),
                "dimension": None
                if dimension is None
                else {
                    "name": dimension.name,
                    "relation": _definition_record(dimension.relation),
                    "source_entity": dimension.source_entity,
                    "entity_column": dimension.entity_column,
                    "validity": _definition_record(dimension.validity),
                },
            }
        selector = (
            expression
            if isinstance(expression, EventTime)
            else getattr(expression, "relative_to", None)
        )
        if selector is not None:
            resolved["selector"] = _selector_definition(selector, model)
        if name not in semantic:
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
        "period_index",
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
    for name in tuple(names):
        if name not in compiled.columns:
            continue
        metadata = compiled.columns[name]
        for support in (
            metadata.observed_from_column,
            metadata.observed_until_column,
            metadata.complete_column,
            metadata.reason_column,
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
    _winsorizations: tuple[Winsorization, ...]

    def __init__(self, design: Design, model: Model) -> None:
        from terrine.compile.dispatch import compile_design

        object.__setattr__(self, "_design", design)
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_compiled", compile_design(design, model))
        object.__setattr__(self, "_training_view", None)
        object.__setattr__(self, "_scoring_view", None)
        object.__setattr__(self, "_winsorizations", ())

    def _active_compiled(self, compiled: CompiledStudy | None = None) -> CompiledStudy:
        active = self._compiled if compiled is None else compiled
        if self._training_view is not None:
            active = _apply_training_view(active, self._training_view)
        return self._with_winsorization_metadata(active)

    def _with_winsorization_metadata(self, compiled: CompiledStudy) -> CompiledStudy:
        if not self._winsorizations:
            return compiled
        transforms = import_module("terrine.dataset.transforms")
        columns = dict(compiled.columns)
        for transform in self._winsorizations:
            if transform.column not in columns:
                raise transforms.identity_error(transform.column)
            columns[transform.column] = replace(
                columns[transform.column], transformation=transform.to_record()
            )
        return replace(compiled, columns=columns)

    def _winsorize_output(self, table):
        if not self._winsorizations:
            return table
        return import_module("terrine.dataset.transforms").apply_winsorization(
            table, self._winsorizations
        )

    @property
    def expr(self):
        """Return the Ibis graph; direct execution bypasses all export safeguards."""
        if self._winsorizations:
            raise RefusalSpec(
                "terrine.transform.winsorization_expr_unsupported",
                "Post-aggregation winsorization requires guarded Arrow output. {route}",
            ).error(route="Use Frame.materialize(), Frame.stage(), or a Dataset export.")
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
        if isinstance(self._design.shape, (LifeTable, EventSequence)):
            from terrine.spec.dataset import UNSUPPORTED_VIEW

            raise UNSUPPORTED_VIEW.error(
                reason="training selections require row-oriented values; use the specialized "
                "Frame export directly or construct a cross-sectional training Design"
            )
        if split is not None:
            from terrine.semantic.time import boundary_timezone

            policies = import_module("terrine.dataset.splits")
            if not isinstance(split, (policies.TimeSplit, policies.GroupSplit)):
                raise TypeError("split must be TimeSplit, GroupSplit, or None")
            if isinstance(split, policies.TimeSplit):
                split = split.bind(boundary_timezone(self._model.catalog.day_boundary))
                groups = split.groups
            else:
                groups = split
            if groups is not None and groups.group not in (
                self.roles.unit,
                self.roles.cluster_unit,
            ):
                raise ValueError("split groups must select the unit or cluster-unit identity")
            if {"__terrine_split", "__terrine_split_reason"} & set(self._compiled.table.columns):
                raise ValueError("split output names collide with Design columns")

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
        object.__setattr__(result, "_winsorizations", self._winsorizations)
        if self._winsorizations:
            result._active_compiled()
        return result

    def fit_winsorization(
        self,
        column: str,
        *,
        lower: Decimal | str | int,
        upper: Decimal | str | int,
        budget: Budget | None = None,
    ) -> Winsorization:
        """Fit exact type-7 limits on the declared training split, never held-out rows."""
        transforms = import_module("terrine.dataset.transforms")
        from terrine.spec.transforms import _invalid, decimal_value

        if self._training_view is None or self._training_view.split is None:
            transforms.require_training_split()
        if self._winsorizations:
            raise _invalid("fit on the untransformed training view before applying frozen limits")
        if column not in self.roles.features:
            raise _invalid("fit column must be a selected feature")
        probabilities = (decimal_value(lower), decimal_value(upper))
        if not Decimal(0) <= probabilities[0] <= probabilities[1] <= Decimal(1):
            raise _invalid("probabilities must be ordered in [0, 1]")
        transforms.require_numeric_type(
            self._active_compiled().table.schema()[column].to_pyarrow(), column
        )
        definition = import_module("terrine.dataset.reference")._definition_for(self)
        return transforms.fit_winsorization(
            self.materialize(budget),
            column=column,
            lower=probabilities[0],
            upper=probabilities[1],
            definition_hash=definition.digest,
            feature_definition_hash=_feature_definition_hash(self._design, (column,), self._model),
        )

    def winsorize(self, transform: Winsorization) -> Frame:
        """Apply explicit or training-fitted frozen limits after aggregation."""
        from terrine.spec.transforms import Winsorization, _invalid

        transforms = import_module("terrine.dataset.transforms")
        if not isinstance(transform, Winsorization):
            raise _invalid("expected a typed Winsorization record")
        if transform.column not in self.roles.features:
            raise _invalid("transform column must be a selected feature")
        if any(item.column == transform.column for item in self._winsorizations):
            raise _invalid("a feature can have only one declared winsorization")
        bound = transforms.bind_winsorization(
            transform, self._active_compiled().table.schema()[transform.column].to_pyarrow()
        )
        if bound.fit is not None:
            fit = bound.fit
            if self._training_view is not None:
                definition_hash = (
                    import_module("terrine.dataset.reference")
                    ._definition_for(self, include_transforms=False)
                    .digest
                )
                compatible = definition_hash == fit.definition_hash
            else:
                compatible = (
                    self._scoring_view is not None
                    and self._scoring_view.parent_design_hash == fit.design_hash
                )
            if (
                not compatible
                or _feature_definition_hash(self._design, (transform.column,), self._model)
                != fit.feature_definition_hash
            ):
                raise transforms.identity_error(transform.column)
        result = Frame(self._design, self._model)
        object.__setattr__(result, "_training_view", self._training_view)
        object.__setattr__(result, "_scoring_view", self._scoring_view)
        object.__setattr__(result, "_winsorizations", (*self._winsorizations, bound))
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
        from terrine.execution.backend import backend_provider
        from terrine.execution.inputs import open_execution

        backend_provider(self._model.con).capabilities.require(
            "arrow_batches",
            backend=self._model.catalog.dialect,
            route="Materialize with a backend that supports bounded Arrow batches.",
        )
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
            compiled = self._with_winsorization_metadata(compiled)
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
            reader = context.to_arrow_batches(query, chunk_size=chunk_size)
            try:
                schema = reader.schema
                output_schema = self._winsorize_output(
                    pa.Table.from_batches([], schema=schema)
                ).schema
                for batch in reader:
                    if batch.schema != schema:
                        batch = batch.cast(schema)
                    batch = self._winsorize_output(batch)
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
                table = pa.Table.from_batches(batches, schema=output_schema)
            finally:
                reader.close()
            from terrine.compile.columns import resolve_sketch_sidecars

            table = resolve_sketch_sidecars(table, compiled, context.con)
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
                        name: ColumnMeta(None, "outcome", None, None, "complete", "reason")
                        for name in roles.outcomes
                    }
                )
            table = self._normalize_output(table)
            table, split_counts = self._split_output(table, compiled)
            for reason, count in split_counts.items():
                excluded_counts[reason] = excluded_counts.get(reason, 0) + count
            if budget is not None and budget.max_output_bytes is not None:
                if table.nbytes > budget.max_output_bytes:
                    raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
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
            calendar_comparison = _provenance.calendar_comparison_metadata(self._design)
            panel = _provenance.panel_metadata(self._design)

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
                state_panel=_provenance.state_panel_metadata(self._design, model),
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
            provenance = self._view_provenance(provenance, excluded_counts)
            return MaterializedFrame(table, roles, columns, provenance)

    def _view_provenance(self, provenance: Provenance, excluded_counts: dict[str, int]):
        model = self._model
        if self._training_view is not None:
            view = self._training_view
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
                        self._design, view.features, model
                    ),
                    incomplete_labels=view.incomplete_labels,
                    split_policy=None
                    if view.split is None
                    else cast(_SplitPolicy, view.split).to_record(),
                    counts_by_reason=excluded_counts,
                ),
            )
        elif self._scoring_view is not None:
            scoring = self._scoring_view
            provenance = replace(
                provenance,
                dataset_view=DatasetViewMeta(
                    kind="scoring",
                    selected_features=scoring.features,
                    selected_labels=(),
                    view_hash=_scoring_view_digest(scoring, self._design.spec_hash),
                    feature_definition_hash=_feature_definition_hash(
                        self._design, scoring.features, model
                    ),
                    resolved_as_of=scoring.resolved_as_of,
                ),
            )
        return provenance

    def _split_output(self, table: pa.Table, compiled: CompiledStudy):
        view = self._training_view
        if view is None or view.split is None:
            return table, {}
        return import_module("terrine.dataset.training").assign_training_splits(
            table,
            compiled=compiled,
            labels=view.labels,
            policy=cast("Any", view.split),
            state_panel=isinstance(self._design.shape, StatePanel),
            binned_state=isinstance(self._design.shape, StatePanel)
            and isinstance(self._design.shape.state, BinnedMetricState),
        )

    def _normalize_output(self, table: pa.Table) -> pa.Table:
        if isinstance(self._design.shape, TimeToEvent):
            for name in ("event_time", "censor_time", "duration", "entry_time"):
                if name in table.column_names:
                    dtype = table.schema.field(name).type
                    if pa.types.is_timestamp(dtype) and dtype.tz is None:
                        table = table.set_column(
                            table.schema.get_field_index(name),
                            name,
                            table[name].cast(pa.timestamp(dtype.unit, tz="UTC")),
                        )
        if isinstance(self._design.shape, StatePanel):
            index = table.schema.get_field_index("gap")
            table = table.set_column(index, "gap", table.column(index).cast(pa.duration("us")))
        return table

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
