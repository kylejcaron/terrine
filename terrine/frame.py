"""Lazy study frames with one verified execution lifetime per export."""

from __future__ import annotations

import hashlib
import json
import warnings
from collections.abc import Iterable
from copy import copy
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import ibis
import pyarrow as pa
import pyarrow.compute as pc

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
    StudyAssumptions,
    dataframe_view_preflight,
)
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution.gates import (
    GatedStream,
    enforce_shape_budget,
    fold,
    gate_totals,
    gated_output,
    run_schedule,
)
from terrine.semantic.models import BoundDimension, BoundMetric, BoundSource
from terrine.spec.anchors import FixedDate
from terrine.spec.dataset import DatasetDefinition
from terrine.spec.design import _output_shape_name
from terrine.spec.io import _encode
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import BinnedMetricState, StatePanel
from terrine.spec.trajectory import CohortGrouping, CohortSpec, LifeTable, TimeToEvent

if TYPE_CHECKING:
    from terrine.compile.columns import CompiledStudy
    from terrine.compile.dispatch import _StudyRequirements
    from terrine.model import Model
    from terrine.spec.design import Design
    from terrine.spec.expressions import EventTime
    from terrine.spec.transforms import Winsorization

__all__ = ["Frame", "CohortTable"]


def _life_table_group_supports(
    shape: LifeTable, compiled: CompiledStudy
) -> tuple[dict[str, str], list[tuple[str, int]]]:
    supports: dict[str, str] = {}
    synthesized: list[tuple[str, int]] = []
    for name in shape.group_by:
        timing = compiled.expression_timings.get(name)
        if timing is not None:
            # A group has an interval of contributing times, not a row origin.
            for field, offset, policy in (
                ("start_bound_column", timing.support_seconds[0], "min"),
                ("end_bound_column", timing.support_seconds[1], "max"),
            ):
                column = getattr(timing, field)
                if column is None:
                    column = f"__terrine_{name}_{field.removesuffix('_column')}"
                    synthesized.append((column, offset))
                supports[column] = policy
        meta = compiled.columns[name]
        for column, policy in (
            (meta.observed_from_column, "min"),
            (meta.observed_until_column, "max"),
            (meta.complete_column, "all"),
            (meta.reason_column, "reasons"),
        ):
            if column is not None:
                supports.setdefault(column, policy)
    return supports, synthesized


def _add_life_table_timing(table: pa.Table, synthesized: list[tuple[str, int]]) -> pa.Table:
    for column, offset in synthesized:
        table = table.append_column(
            column,
            # pyarrow.compute lacks a stub for this checked Arrow kernel.
            pc.add_checked(  # ty: ignore[unresolved-attribute]
                table["t0"], pa.scalar(offset, pa.duration("s"))
            ),
        )
    return table


_INVALID_SPLIT = RefusalSpec(
    "terrine.dataset.invalid_split", "Split input {field} is invalid: {reason}."
)
_INVALID_SELECTION = RefusalSpec(
    "terrine.dataset.invalid_selection", "Training {field} selection is invalid: {reason}."
)
_INVALID_COHORTS = RefusalSpec(
    "terrine.cohort.invalid_operation", "Cohort input {field} is invalid: {reason}."
)
_COHORT_CHOICE = RefusalSpec(
    "terrine.cohort.invalid_operation", "Cohort input {field} must be one of {choices}."
)


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
        record = {
            field.name: _definition_record(getattr(value, field.name)) for field in fields(value)
        }
        # Preserve existing feature identities when shared-only fields are unused.
        if isinstance(value, BoundSource):
            del record["event_types"]  # Derived metadata, not part of the definition.
            if value.row_key is None:
                del record["row_key"]
        if isinstance(value, BoundDimension) and value.foreign_key is None:
            del record["foreign_key"]
        if isinstance(value, BoundMetric):
            if not value.formula:
                del record["formula"]
            if not value.presence:
                del record["presence"]
        return record
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
    event = model.catalog.events[selector.event]
    return {
        "event": _definition_record(event),
        "source": _definition_record(model.sources[event.source]),
        "event_id": model.event_ids.get(event.source),
        "relative_to": None
        if selector.relative_to is None
        else _selector_definition(selector.relative_to, model),
    }


def _feature_definition_hash(design: Design, features: tuple[str, ...], model: Model) -> str:
    from terrine.semantic.attributes import resolve_attribute
    from terrine.spec.expressions import (
        ApproxMetricDistinct,
        ApproxMetricQuantile,
        AttributeAsOf,
        EventTime,
        MetricAgg,
        MetricQuantile,
        _output_leaves,
    )
    from terrine.spec.io import canonical_json
    from terrine.spec.validate import resolve_unit

    design = resolve_unit(design, model)
    assert design.unit is not None
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
                                "time_column": source.time_column,
                                "where": _definition_record(source.where),
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
        resolved_leaves = []
        for leaf in _output_leaves(expression) if expression is not None else ():
            resolved: dict[str, object] = {}
            if isinstance(
                leaf, (MetricAgg, MetricQuantile, ApproxMetricQuantile, ApproxMetricDistinct)
            ):
                metric = model.metrics.get(leaf.metric)
                if metric is not None:
                    resolved["metric"] = _metric_definition(metric, model)
            elif isinstance(leaf, AttributeAsOf):
                attribute = resolve_attribute(
                    sources=model.sources,
                    properties=model.catalog.properties,
                    roster=model._roster_attributes(design.unit),
                    attribute=leaf.attribute,
                    unit=design.unit,
                )
                dimension = attribute.dimension
                if attribute.owner == "roster":
                    resolved["attribute"] = {
                        "owner": "roster",
                        "relation": _definition_record(dimension.relation),
                        "key": dimension.key,
                        "binding": _definition_record(attribute.prop),
                    }
                else:
                    source = attribute.source
                    assert source is not None
                    resolved["attribute"] = {
                        "source": source.name,
                        "relation": _definition_record(source.relation),
                        "unit_column": source.units.get(design.unit),
                        "day_boundary": source.day_boundary,
                        "time_column": source.time_column,
                        "where": _definition_record(source.where),
                        "binding": _definition_record(attribute.prop),
                        "dimension": None
                        if attribute.prop.dimension_name is None
                        else {
                            "name": dimension.name,
                            "relation": _definition_record(dimension.relation),
                            "unit": dimension.unit,
                            "key": dimension.key,
                            "validity": _definition_record(dimension.validity),
                        },
                    }
            selector = leaf if isinstance(leaf, EventTime) else getattr(leaf, "relative_to", None)
            if selector is not None:
                resolved["selector"] = _selector_definition(selector, model)
            resolved_leaves.append(resolved)
        if name not in semantic:
            semantic[name] = resolved_leaves[0] if len(resolved_leaves) == 1 else resolved_leaves
    payload = {
        "unit": record["unit"],
        "unit_binding": _definition_record(model.units.get(design.unit)),
        "gap": str(design.gap),
        "features": selected_records,
        "semantic": semantic,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _has_deferred_labels(compiled: CompiledStudy, view: _TrainingView) -> bool:
    return any(name in view.labels for sidecar in compiled.sketches for name in sidecar.outputs)


def _apply_training_view(compiled: CompiledStudy, view: _TrainingView) -> CompiledStudy:
    table = compiled.table
    roles = compiled.roles
    label_metadata = [compiled.columns[name] for name in view.labels]
    if view.incomplete_labels == "drop" and not _has_deferred_labels(compiled, view):
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
    if roles.trial == "cohort":
        # Retain the contrast keys, not the realized initiation time after t0.
        structural = (*structural, "cohort", "event_period", "control_eligible")
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
        sketches=tuple(
            replace(sidecar, outputs=outputs)
            for sidecar in compiled.sketches
            if (outputs := tuple(name for name in sidecar.outputs if name in selected_columns))
        ),
    )


def _filter_resolved_labels(batch, compiled, labels, counts):
    """Count each unusable label, then discard a row if any selected label is unusable."""
    keep = None
    for label in labels:
        meta = compiled.columns[label]
        complete = batch[meta.complete_column].fill_null(False)
        usable = pc.call_function("and", [batch[label].is_valid(), complete])
        missing = pc.call_function("invert", [usable])
        reasons = batch[meta.reason_column].filter(missing).fill_null("unknown")
        for entry in reasons.value_counts().to_pylist():
            reason = entry["values"]
            counts[reason] = counts.get(reason, 0) + entry["counts"]
        keep = usable if keep is None else pc.call_function("and", [keep, usable])
    return batch.filter(keep)


def _excluded_label_counts(compiled: CompiledStudy, view: _TrainingView, execute) -> dict[str, int]:
    table = compiled.table
    reasons = {}
    for index, name in enumerate(view.labels):
        metadata = compiled.columns[name]
        assert metadata.complete_column is not None and metadata.reason_column is not None
        unusable = ~table[metadata.complete_column].fill_null(False) | table[name].isnull()
        reasons[f"label_{index}"] = unusable.ifelse(
            table[metadata.reason_column].fill_null("unknown"), ibis.null("string")
        )
    if len(reasons) == 1:
        occurrences = table.select(reason=next(iter(reasons.values())))
    else:
        # Expand label occurrences, not distinct reasons. Only the inline ordinals
        # are unioned; the study appears once and requires no registered memtable.
        ordinals = ibis.union(
            *(ibis.literal(i, type="int64").name("ordinal").as_table() for i in range(len(reasons)))
        )
        expanded = table.select(**reasons).cross_join(ordinals)
        occurrences = expanded.select(
            reason=ibis.cases(
                *((expanded.ordinal == i, expanded[name]) for i, name in enumerate(reasons))
            )
        )
    excluded = occurrences.filter(occurrences.reason.notnull())
    grouped = excluded.group_by("reason").aggregate(rows=excluded.count())
    return dict(sorted((row["reason"], row["rows"]) for row in execute(grouped).to_pylist()))


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class Frame:
    """Represent a dataset design compiled against a model, before reading event rows.

    Materialization and conversion enforce integrity and output limits; direct ``expr``
    execution bypasses them, and output limits do not bound scans or peak memory.


    Args:
        design: Design describing the rows and expressions to compile.
        model: Model binding the design names to tables, metrics, attributes and completeness.

    Example: design.build(model)
    """

    _design: Design
    _model: Model
    _requirements: _StudyRequirements
    _compiled: CompiledStudy
    _training_view: _TrainingView | None
    _scoring_view: _ScoringView | None
    _winsorizations: tuple[Winsorization, ...]

    def __init__(self, design: Design, model: Model) -> None:
        from terrine.compile.dispatch import _prepare_study

        requirements, compiled = _prepare_study(design, model)
        design = requirements.design
        object.__setattr__(self, "_design", design)
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_requirements", requirements)
        object.__setattr__(self, "_compiled", compiled)
        object.__setattr__(self, "_training_view", None)
        object.__setattr__(self, "_scoring_view", None)
        object.__setattr__(self, "_winsorizations", ())

    def __repr__(self) -> str:
        roles = self.roles
        columns = self.columns
        return (
            f"Frame(unit={roles.unit!r}, features={roles.features!r}, "
            f"outcomes={roles.outcomes!r}, columns={len(columns)})"
        )

    def _active_compiled(self, compiled: CompiledStudy | None = None) -> CompiledStudy:
        active = self._compiled if compiled is None else compiled
        return self._with_winsorization_metadata(self._project_view(active))

    def _project_view(self, compiled: CompiledStudy) -> CompiledStudy:
        if self._training_view is not None:
            return _apply_training_view(compiled, self._training_view)
        view = self._scoring_view
        if view is None or view.features == compiled.roles.features:
            return compiled
        selected = set(view.features)
        requested = iter(view.features)
        names = [next(requested) if name in selected else name for name in compiled.table.columns]
        return replace(
            compiled,
            table=compiled.table.select(*names),
            roles=replace(compiled.roles, features=view.features),
        )

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
        if any(meta.measure == "elapsed" for meta in self.columns.values()):
            from terrine.spec.expressions import _ELAPSED_SELECTOR

            raise _ELAPSED_SELECTOR.error(
                reason="elapsed_selector_in_expr",
                route="Use materialize(), stage(), to_pyarrow(), to_pandas(), or to_polars().",
            )
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

    @property
    def assumptions(self) -> StudyAssumptions:
        """Report historical input assumptions and native float decisions without reading rows."""
        from terrine.compile._assumptions import study_assumptions

        view = self._training_view
        return study_assumptions(
            self._design,
            self._model,
            features=None if view is None else view.features,
            labels=None if view is None else view.labels,
        )

    def to_definition(self) -> DatasetDefinition:
        """Return the resolved design, selected view and frozen transforms without reading data.

        The definition records the bound split or absolute scoring time, but not a
        connection, source snapshot or published generation.

        Returns:
            A portable definition that can rebuild this Frame on compatible model bindings.
        """
        return self._project_definition(self._winsorizations)

    def _project_definition(self, transforms: tuple[Winsorization, ...]) -> DatasetDefinition:
        view = None
        if self._training_view is not None:
            training = self._training_view
            view = {
                "kind": "training",
                "features": list(training.features),
                "labels": list(training.labels),
                "incomplete_labels": training.incomplete_labels,
                "split": None
                if training.split is None
                else cast(_SplitPolicy, training.split).to_record(),
            }
        elif self._scoring_view is not None:
            scoring = self._scoring_view
            view = {
                "kind": "scoring",
                "features": list(scoring.features),
                "as_of": scoring.resolved_as_of.isoformat(),
                "eligibility": _encode(self._design.eligibility),
                "parent_design_hash": scoring.parent_design_hash,
            }
        return DatasetDefinition(base_design=self._design, view=view, transforms=transforms)

    def training_set(
        self,
        *,
        label: str | tuple[str, ...],
        features: tuple[str, ...] | None = None,
        incomplete_labels: Literal["drop", "keep"] = "drop",
        split: object | None = None,
    ) -> Frame:
        """Select features and labels lazily, dropping incomplete labels unless asked to keep them.


        TimeSplit uses exclusive t0 cutoffs. Label windows crossing a split cutoff are
        unassigned with __terrine_split_reason="label_overlap", never used to train across the
        cutoff.
        Use to_pandas(values_only=True) for modeling after inspecting completeness and splits.

        Args:
            label: Outcome column name or nonempty tuple of outcome names to use as labels.
            features: Tuple of feature names to retain; None selects all design features.
            incomplete_labels: "drop" removes rows with incomplete labels; "keep" retains them with
                completeness metadata.
            split: TimeSplit or GroupSplit from terrine.dataset; None assigns no split columns.

        Returns:
            A lazy Frame with selected labels/features and optional __terrine_split and
            __terrine_split_reason columns.

        Example:
            from terrine.dataset import TimeSplit

            frame.training_set(
                label="spend_next_30d",
                split=TimeSplit(train_until="2025-03-15", validation_until="2025-04-15"),
            )
        """
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
                raise _INVALID_SPLIT.error(reason="wrong_type", field="split")
            if isinstance(split, policies.TimeSplit):
                split = split.bind(boundary_timezone(self._model.catalog.day_boundary))
                groups = split.groups
            else:
                groups = split
            if groups is not None and groups.group not in (
                self.roles.unit,
                self.roles.cluster_unit,
            ):
                raise _INVALID_SPLIT.error(
                    reason="unsupported_group",
                    field="group",
                    choices=tuple(
                        name
                        for name in (self.roles.unit, self.roles.cluster_unit)
                        if name is not None
                    ),
                )
            if {"__terrine_split", "__terrine_split_reason"} & set(self._compiled.table.columns):
                raise _INVALID_SPLIT.error(reason="reserved_name", field="split")

        if not isinstance(label, (str, Iterable)):
            raise _INVALID_SELECTION.error(reason="wrong_type", field="label")
        labels = (label,) if isinstance(label, str) else tuple(label)
        if not labels or any(not isinstance(name, str) or not name for name in labels):
            raise _INVALID_SELECTION.error(reason="empty_name", field="label")
        if len(set(labels)) != len(labels):
            raise _INVALID_SELECTION.error(reason="duplicate_names", field="label")
        unknown = set(labels) - set(self.roles.outcomes)
        if unknown:
            raise RefusalSpec(
                "terrine.dataset.invalid_selection",
                "Unknown training label selection: {names}",
            ).error(names=tuple(sorted(unknown)))
        if features is not None and (
            isinstance(features, str) or not isinstance(features, Iterable)
        ):
            raise _INVALID_SELECTION.error(reason="wrong_type", field="features")
        selected_features = self.roles.features if features is None else tuple(features)
        if any(not isinstance(name, str) or not name for name in selected_features):
            raise _INVALID_SELECTION.error(reason="empty_name", field="features")
        if len(set(selected_features)) != len(selected_features):
            raise _INVALID_SELECTION.error(reason="duplicate_names", field="features")
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
            from terrine.spec.dataset import INVALID_VIEW

            raise INVALID_VIEW.error(
                reason="unsupported_value", field="incomplete_labels", choices=("drop", "keep")
            )
        for name in labels:
            metadata = self.columns[name]
            if metadata.complete_column is None or metadata.reason_column is None:
                raise RefusalSpec(
                    "terrine.dataset.invalid_label_support",
                    "Selected label lacks completeness and reason metadata: {label}",
                ).error(label=name)
        result = copy(self)
        object.__setattr__(
            result,
            "_training_view",
            _TrainingView(selected_features, labels, incomplete_labels, split),
        )
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
        definition = self._project_definition(())
        return transforms.fit_winsorization(
            self.materialize(budget=budget),
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
                definition = self._project_definition(())
                compatible = definition.digest == fit.definition_hash
            else:
                compatible = (
                    self._scoring_view is not None
                    and self._scoring_view.parent_design_hash == fit.design_hash
                )
            feature_hash = _feature_definition_hash(self._design, (transform.column,), self._model)
            if not compatible or feature_hash != fit.feature_definition_hash:
                raise transforms.identity_error(transform.column)
        result = copy(self)
        object.__setattr__(result, "_winsorizations", (*self._winsorizations, bound))
        return result

    def stage(self, table: str, *, budget: Budget | None = None):
        """Publish a pinned dataset generation on a file-backed DuckDB connection.

        Use a file-backed DuckDB model for publication; in-memory models cannot be staged.

        Args:
            table: Destination dataset table name on the model's file-backed DuckDB connection.
            budget: Optional limits; None uses Budget(strict_scan=False), with no row, output-byte
                or scan cap.

        Returns:
            A Dataset pinned to the newly published immutable generation.

        Example: frame.stage("customer_spend")
        """
        stage_frame = import_module("terrine.dataset.reference").stage_frame
        return stage_frame(self, table, budget=budget)

    def materialize(self, *, budget: Budget | None = None) -> MaterializedFrame:
        """Read dataset rows with integrity checks and retain their metadata and completeness.


        Args:
            budget: Optional limits; None applies no row, output-byte or scan cap.

        Returns:
            A MaterializedFrame containing Arrow rows, roles, column metadata and provenance.

        Example: frame.materialize()
        """
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
        Verified execution views exclude concurrent writes; unverified reads are per statement.
        """
        enforce_shape_budget(self._design.shape, budget)
        for warning in _scan_policy(budget):
            warnings.warn(warning, RuntimeWarning, stacklevel=2)
        from terrine.compile.columns import (
            apply_sketch_estimates,
            collect_sketch_estimates,
            expression_policy,
        )
        from terrine.compile.dispatch import _bind_study
        from terrine.compile.prune import prune_columns
        from terrine.execution.backend import backend_provider

        backend_provider(self._model.con, engine=self._model._engine).capabilities.require(
            "arrow_batches",
            backend=self._model.catalog.dialect,
            route="Materialize with a backend that supports bounded Arrow batches.",
            allow_unverified=self._model._engine.status == "unverified",
        )
        model = self._model
        with model._study_execution(
            lambda: set(self._requirements.required_sources), unit_name=self._design.unit
        ) as (context, execution_model):
            compiled = _bind_study(self._requirements, model, execution_model, prune_output=False)
            # Admit every relation the Frame will run, integrity checks included,
            # so an unsupported primitive refuses before the first source read.
            context.admit(*compiled.reads())
            study = compiled
            compiled = self._project_view(compiled)
            if transform is not None:
                compiled = transform(compiled)
                context.admit(*compiled.reads())
            compiled = self._with_winsorization_metadata(compiled)
            # Keep the original per-relation counts and barriers. Only prune
            # after the final gate and retained rows have become one graph.
            final = fold(compiled._checks)[-1]
            query = gated_output(compiled.table, compiled.audit_table, final)
            chunk_size = 65_536
            study_shape = self._design.shape
            life_table = study_shape if isinstance(study_shape, LifeTable) else None
            max_rows = None if budget is None else budget.max_rows
            view = self._training_view
            deferred_drop = (
                view is not None
                and view.incomplete_labels == "drop"
                and _has_deferred_labels(compiled, view)
            )
            input_limit = max_rows if life_table is None and not deferred_drop else None
            if input_limit is not None:
                limit = min(input_limit + 1, 2**63 - 1)
                query = query.limit(limit)
                chunk_size = min(chunk_size, limit)
            query = prune_columns(query)
            context.admit(query)
            run_schedule(compiled, context.execute)
            stream_life_table = (
                life_table is not None
                and not compiled.sketches
                and transform is None
                and self._training_view is None
            )
            estimates = collect_sketch_estimates(compiled, context.to_arrow_batches)
            excluded_counts: dict[str, int] = {}
            with context.to_arrow_batches(query, chunk_size=chunk_size) as reader:
                stream = GatedStream(
                    reader,
                    final,
                    compiled.table.columns,
                    lambda: context.execute(
                        prune_columns(gate_totals(compiled.audit_table, final))
                    ),
                )
                summary = stream.resolve()
                schema = stream.schema
                empty = self._winsorize_output(pa.Table.from_batches([], schema=schema))

                def output_batches() -> Iterable[pa.RecordBatch]:
                    row_count = 0
                    for batch in stream:
                        if batch.schema != schema:
                            batch = batch.cast(schema)
                        if estimates:
                            batch = apply_sketch_estimates(batch, compiled, estimates)
                        if deferred_drop:
                            assert view is not None
                            batch = _filter_resolved_labels(
                                batch, compiled, view.labels, excluded_counts
                            )
                            if batch.num_rows == 0:
                                continue
                        batch = self._winsorize_output(batch)
                        if max_rows is not None and life_table is None:
                            row_count += batch.num_rows
                            if row_count > max_rows:
                                raise _ROWS_EXCEEDED.error(limit=max_rows)
                        yield batch

                if life_table is not None and stream_life_table:
                    table = self._reduce_life_table_batches(
                        life_table, compiled, empty, output_batches(), max_rows
                    )
                else:
                    table = pa.Table.from_batches(output_batches(), schema=empty.schema)
            if view is not None and view.incomplete_labels == "drop" and not deferred_drop:
                excluded_counts = _excluded_label_counts(study, view, context.execute)
            roles = compiled.roles
            columns = compiled.columns
            if life_table is not None:
                if not stream_life_table:
                    from terrine.compile.dispatch import aggregate_life_table

                    table = self._normalize_output(table)
                    group_supports, synthesized = _life_table_group_supports(life_table, compiled)
                    table = aggregate_life_table(
                        _add_life_table_timing(table, synthesized),
                        interval_seconds=life_table.bin.seconds,
                        group_by=life_table.group_by,
                        group_supports=group_supports,
                        max_rows=max_rows,
                    )
                roles = Roles(
                    unit="unit",
                    anchor="anchor_id",
                    t0="t0",
                    treatment=None,
                    features=(*life_table.group_by, "time_bin"),
                    outcomes=(
                        "at_risk",
                        "events",
                        "censored",
                        "delayed_entries",
                        "event_rate",
                        "censor_rate",
                    ),
                )
                columns = {name: compiled.columns[name] for name in life_table.group_by}
                columns["time_bin"] = ColumnMeta(
                    window=None,
                    role="feature",
                    observed_from_column=None,
                    observed_until_column=None,
                    complete_column=None,
                    reason_column=None,
                )
                columns.update(
                    {
                        name: ColumnMeta(
                            window=None,
                            role="outcome",
                            observed_from_column=None,
                            observed_until_column=None,
                            complete_column=f"{name}__complete",
                            reason_column=f"{name}__reason",
                        )
                        for name in (*roles.outcomes, "censored_by_reason")
                    }
                )
            table = self._normalize_output(table)
            table, split_counts = self._split_output(table, compiled)
            for reason, count in split_counts.items():
                excluded_counts[reason] = excluded_counts.get(reason, 0) + count
            treatment = self._design.treatment
            treatment_column = compiled.roles.treatment
            baseline_binary = (
                treatment is not None
                and treatment_column is not None
                and isinstance(self._design.anchor, FixedDate)
                and self._design.shape == "cross_section"
                and treatment.at.seconds == 0
                and treatment.known_at_t0
                and pa.types.is_boolean(table.schema.field(treatment_column).type)
            )
            calendar_comparison = _provenance.calendar_comparison_metadata(self._design, model)
            panel = _provenance.panel_metadata(self._design, model)

            feature_cutoffs = compiled.feature_cutoffs
            expression_timings = compiled.expression_timings
            if isinstance(self._design.shape, LifeTable):
                feature_cutoffs = {
                    name: cutoff
                    for name, cutoff in compiled.feature_cutoffs.items()
                    if name in self._design.shape.group_by
                }
                expression_timings = {
                    name: replace(
                        timing,
                        point_time_column=None,
                        start_bound_column=(
                            timing.start_bound_column or f"__terrine_{name}_start_bound"
                        ),
                        end_bound_column=timing.end_bound_column or f"__terrine_{name}_end_bound",
                    )
                    for name, timing in compiled.expression_timings.items()
                    if name in self._design.shape.group_by
                }
            provenance = Provenance(
                execution_revision=_provenance.EXECUTION_REVISION,
                feature_cutoffs=feature_cutoffs,
                eligibility_cutoffs=compiled.eligibility_cutoffs,
                design_hash=self._design.spec_hash,
                model_hash=model.model_hash,
                source_generations=context.snapshot.source_generations,
                execution_snapshot=context.snapshot,
                shape=shape or _output_shape_name(self._design),
                treatment_design="baseline_binary" if baseline_binary else None,
                engine_name=context.snapshot.backend,
                dialect=expression_policy(model),
                engine_status=model._engine.status,
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
                        strata=self._design.sample.strata,
                        controls_per_treated=self._design.sample.controls_per_treated,
                        seed=self._design.sample.seed,
                    )
                    if self._design.sample is not None
                    else None
                ),
                calendar_comparison=calendar_comparison,
                administrative_cutoff=self._design.administrative_cutoff,
                cohort_operation=cohort_operation,
            )
            provenance = self._view_provenance(provenance, excluded_counts)
            result = MaterializedFrame(
                table=table, roles=roles, columns=columns, provenance=provenance
            )
            if budget is not None and budget.max_output_bytes is not None:
                if result.table.nbytes > budget.max_output_bytes:
                    raise _OUTPUT_BYTES_EXCEEDED.error(limit=budget.max_output_bytes)
            return result

    def _reduce_life_table_batches(
        self,
        shape: LifeTable,
        compiled: CompiledStudy,
        empty: pa.Table,
        batches: Iterable[pa.RecordBatch],
        max_rows: int | None,
    ) -> pa.Table:
        from terrine.compile.dispatch import _life_table_accumulator

        supports, synthesized = _life_table_group_supports(shape, compiled)
        prepared = _add_life_table_timing(self._normalize_output(empty), synthesized)
        accumulator = _life_table_accumulator(
            prepared.schema,
            interval_seconds=shape.bin.seconds,
            group_by=shape.group_by,
            group_supports=supports,
            max_rows=max_rows,
        )
        # The materialized route reads all rows before normalization, then adds
        # timing columns before reduction. Drain earlier phases on failure so
        # a later batch's read or timestamp refusal still takes precedence.
        failed_at: Literal["normalize", "timing", "reduce"] | None = None
        failure: Exception | None = None
        for batch in batches:
            phase: Literal["normalize", "timing", "reduce"] = "normalize"
            try:
                if failed_at == "normalize":
                    continue
                table = self._normalize_output(pa.Table.from_batches([batch], schema=empty.schema))
                phase = "timing"
                if failed_at == "timing":
                    continue
                table = _add_life_table_timing(table, synthesized)
                phase = "reduce"
                if failed_at == "reduce":
                    continue
                for record_batch in table.to_batches():
                    accumulator.consume(record_batch)
            except (pa.ArrowException, TerrineError, TypeError, ValueError, OverflowError) as exc:
                failed_at, failure = phase, exc
        if failure is not None:
            raise failure
        return accumulator.finish()

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
        for name, meta in self._compiled.columns.items():
            if meta.measure != "elapsed" or name not in table.column_names:
                continue
            selected = table[name]
            if not pa.types.is_timestamp(selected.type):
                continue
            selected_at = f"__terrine_selected_at_{name}"
            if selected_at not in table.column_names:
                table = table.append_column(selected_at, selected)
            origin = table[self._compiled.roles.t0].cast(selected.type)
            elapsed = pc.subtract_checked(  # ty: ignore[unresolved-attribute]
                origin.cast(pa.int64()), selected.cast(pa.int64())
            )
            table = table.set_column(
                table.schema.get_field_index(name),
                name,
                elapsed.cast(pa.duration(selected.type.unit)),
            )
        shape = self._design.shape
        if isinstance(shape, LifeTable):
            shape = shape.time_to_event
        if isinstance(shape, TimeToEvent):
            for name in ("event_time", "censor_time", "entry_time"):
                if name in table.column_names:
                    dtype = table.schema.field(name).type
                    if pa.types.is_timestamp(dtype) and dtype.tz is None:
                        table = table.set_column(
                            table.schema.get_field_index(name),
                            name,
                            table[name].cast(pa.timestamp(dtype.unit, tz="UTC")),
                        )
            if "duration" in table.column_names:
                # Subtract Arrow ticks so ns axes remain exact across backends.
                terminal = table["duration"]
                if pa.types.is_timestamp(terminal.type):
                    t0 = table["t0"]
                    unit = terminal.type.unit
                    timestamp_type = pa.timestamp(unit, tz="UTC")
                    terminal_ticks = terminal.cast(timestamp_type).cast(pa.int64())
                    origin_ticks = t0.cast(timestamp_type).cast(pa.int64())
                    try:
                        # pyarrow.compute lacks a stub for this checked Arrow kernel.
                        elapsed = pc.subtract_checked(  # ty: ignore[unresolved-attribute]
                            terminal_ticks, origin_ticks
                        )
                    except pa.ArrowInvalid as exc:
                        if unit == "ns":
                            from terrine.execution._snowflake import _TIMESTAMP_RANGE

                            raise _TIMESTAMP_RANGE.error(
                                detail="elapsed duration exceeds Arrow's int64 nanosecond range"
                            ) from exc
                        raise
                    table = table.set_column(
                        table.schema.get_field_index("duration"),
                        "duration",
                        elapsed.cast(pa.duration(unit)),
                    )
        if isinstance(self._design.shape, StatePanel):
            index = table.schema.get_field_index("gap")
            table = table.set_column(index, "gap", table.column(index).cast(pa.duration("us")))
        return table

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        """Read dataset rows as Arrow with lossless time, completeness and model metadata.


        Args:
            budget: Optional limits; None applies no row, output-byte or scan cap.

        Returns:
            A PyArrow Table preserving completeness, reason columns and portable schema metadata.

        Example: frame.to_pyarrow()
        """
        return self.materialize(budget=budget).to_pyarrow()

    def to_pandas(
        self,
        *,
        budget: Budget | None = None,
        include_observation_metadata: bool = False,
        values_only: bool = False,
    ):
        """Return a pandas dataframe without observation metadata by default.

        ``values_only=True`` also drops completeness and reason columns.


        Args:
            budget: Optional limits; None applies no row, output-byte or scan cap.
            include_observation_metadata: Whether to retain observation-bound columns; False omits
                them.
            values_only: Whether to omit completeness/reason columns too; False retains them;
                incompatible with observation metadata.

        Returns:
            A pandas DataFrame with Arrow-backed dtypes and the requested metadata columns.

        Example: frame.to_pandas(values_only=True)
        """
        dataframe_view_preflight(
            include_observation_metadata=include_observation_metadata,
            values_only=values_only,
            library="pandas",
        )
        return self.materialize(budget=budget).to_pandas(
            include_observation_metadata=include_observation_metadata,
            values_only=values_only,
        )

    def to_polars(
        self,
        *,
        budget: Budget | None = None,
        include_observation_metadata: bool = False,
        values_only: bool = False,
    ):
        """Return a Polars dataframe without observation metadata by default.

        ``values_only=True`` also drops completeness and reason columns.


        Args:
            budget: Optional limits; None applies no row, output-byte or scan cap.
            include_observation_metadata: Whether to retain observation-bound columns; False omits
                them.
            values_only: Whether to omit completeness/reason columns too; False retains them;
                incompatible with observation metadata.

        Returns:
            A Polars DataFrame with the requested values and metadata columns.

        Example: frame.to_polars(values_only=True)
        """
        dataframe_view_preflight(
            include_observation_metadata=include_observation_metadata,
            values_only=values_only,
            library="polars",
        )
        return self.materialize(budget=budget).to_polars(
            include_observation_metadata=include_observation_metadata,
            values_only=values_only,
        )

    def cohorts(self, *, grouping: CohortGrouping, value_column: str) -> CohortTable:
        """Group a trajectory frame's values by units' origin-time buckets.


        Args:
            grouping: CohortGrouping defining elapsed origin-time buckets and the summary statistic.
            value_column: "period_value" or, for a cumulative trajectory, "cumulative_value".

        Returns:
            A lazy CohortTable that can be materialized or converted to a dataframe.

        Example:
            from datetime import datetime, UTC

            tr.recipes.ltv(start_event="orders", metric="spend", period="7d", max_periods=2).build(
                model
            ).cohorts(
                grouping=spec.CohortGrouping("30d", datetime(2025, 1, 1, tzinfo=UTC)),
                value_column="cumulative_value",
            )
        """
        if not isinstance(self._design.shape, CohortSpec):
            raise _INVALID_COHORTS.error(reason="requires_trajectory", field="frame")
        if not isinstance(grouping, CohortGrouping):
            raise _INVALID_COHORTS.error(reason="wrong_type", field="grouping")
        if not isinstance(value_column, str) or value_column not in {
            "period_value",
            "cumulative_value",
        }:
            raise _COHORT_CHOICE.error(
                reason="unsupported_value",
                field="value_column",
                choices=("period_value", "cumulative_value"),
            )
        if value_column == "cumulative_value" and not self._design.shape.trajectory.cumulative:
            raise _INVALID_COHORTS.error(reason="not_cumulative", field="value_column")
        return CohortTable(self, grouping, value_column)


@dataclass(frozen=True, slots=True)
class CohortTable:
    parent_frame: Frame
    grouping: CohortGrouping
    value_column: str

    def materialize(self, *, budget: Budget | None = None) -> MaterializedFrame:
        parent_design_hash = self.parent_frame._design.spec_hash
        payload = {
            "bucket_seconds": self.grouping.bucket.seconds,
            "bucket_origin": self.grouping.bucket_origin.isoformat(),
            "statistic": self.grouping.statistic,
            "value_column": self.value_column,
            "parent_design_hash": parent_design_hash,
        }
        operation_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        operation = CohortOperationMeta(
            bucket_seconds=self.grouping.bucket.seconds,
            bucket_origin=self.grouping.bucket_origin,
            statistic=self.grouping.statistic,
            value_column=self.value_column,
            parent_design_hash=parent_design_hash,
            derived_operation_hash=operation_hash,
        )
        from terrine.compile.columns import expression_policy
        from terrine.compile.dispatch import compile_cohort_summary

        policy = expression_policy(self.parent_frame._model)
        return self.parent_frame._materialize(
            budget,
            transform=lambda study: compile_cohort_summary(
                study, grouping=self.grouping, value_column=self.value_column, dialect=policy
            ),
            shape="cohort_table",
            cohort_operation=operation,
        )

    def to_pyarrow(self, *, budget: Budget | None = None) -> pa.Table:
        return self.materialize(budget=budget).to_pyarrow()
