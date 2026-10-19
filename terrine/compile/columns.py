"""Lazy named study columns over sparse temporal states and candidate anchors."""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from decimal import Decimal
from importlib.metadata import version as package_version
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol, cast

import ibis
import ibis.expr.types as ir
import pyarrow as pa

from terrine.compile._asof import compile_asof
from terrine.compile._predicates import CompiledValue, compile_predicate
from terrine.compile.anchors import (
    AnchorTable,
    _integrity_check,
    _null_check,
    _precision_check,
    _unique_checks,
    _utc_column,
    compile_anchors,
)
from terrine.compile.layers import build_measure_layer, measure_identity
from terrine.data import (
    ApproximationMeta,
    ColumnMeta,
    ExpressionTiming,
    FeatureCutoff,
    Roles,
    SequenceMeta,
)
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import BoundDimension
from terrine.semantic.rules import calendar_average_alignment
from terrine.spec.anchors import FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import (
    And,
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    AttributeAsOf,
    Eq,
    EventTime,
    Expr,
    MetricAgg,
    MetricQuantile,
    SketchRequest,
)
from terrine.spec.sequence import _UNSUPPORTED_SHAPE
from terrine.spec.validate import _fixed_anchors
from terrine.temporal.aggregate import aggregate_window
from terrine.temporal.attributes import (
    _enriched_property_columns,
    enrich_events,
    validate_history,
)
from terrine.temporal.daily import resolve_measure
from terrine.temporal.nonadditive import (
    aggregate_nonadditive_window,
    approximate_quantile_window_values,
)

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["CompiledStudy", "compile_columns", "resolve_sketch_sidecars"]

_NAME_CONFLICT = RefusalSpec(
    "terrine.spec.output_name_conflict",
    "Study output name {name!r} conflicts with {other!r}.",
)
_RESERVED_NAME = RefusalSpec(
    "terrine.spec.reserved_output_name",
    "Study output name {name!r} is reserved.",
)
_RESERVED = {"unit", "anchor_id", "t0", "treatment", "_observed_from", "_observed_until"}


class _DeferredCheck(Protocol):
    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None: ...


_ANCHOR_KEY = ("unit", "anchor_id")
_META_COLUMNS = (
    "observed_from_column",
    "observed_until_column",
    "complete_column",
    "reason_column",
    "absolute_error_column",
)


def _validate_names(design: Design) -> None:
    from terrine.spec.panels import CalendarComparisonSpec, EventPanelSpec, SequentialSpec
    from terrine.spec.state_panel import StatePanel

    names = [*design.features, *design.outcomes, *design.eligibility]
    if isinstance(design.shape, StatePanel):
        names.extend(design.shape.baseline_features)
        reserved = _RESERVED | {
            "from_time",
            "to_time",
            "scheduled_to",
            "period_index",
            "gap",
            "from_state",
            "to_state",
            "terminal",
            "from_state_complete",
            "from_state_reason",
            "to_state_complete",
            "to_state_reason",
            "from_state_observed_from",
            "from_state_observed_until",
            "to_state_observed_from",
            "to_state_observed_until",
        }
    elif isinstance(design.shape, EventPanelSpec):
        reserved = _RESERVED | {"parent_event_id", "parent_t0", "event_period"}
    elif isinstance(design.shape, SequentialSpec):
        reserved = _RESERVED | {
            "trial_id",
            "trial_weight",
            "assignment_probability",
            "n_treated",
            "n_controls",
        }
    elif isinstance(design.shape, CalendarComparisonSpec):
        reserved = _RESERVED | {"cohort", "period", "event_period", "control_eligible"}
    else:
        reserved = _RESERVED
    seen = {}
    reserved_casefold = {value.casefold() for value in reserved}
    for name in names:
        normalized = unicodedata.normalize("NFC", name).casefold()
        if normalized in reserved_casefold or normalized.startswith("__terrine_"):
            raise _RESERVED_NAME.error(name=name)
        if normalized in seen:
            raise _NAME_CONFLICT.error(name=name, other=seen[normalized])
        seen[normalized] = name


@dataclass(frozen=True, slots=True)
class _HistoryValidation:
    table: ibis.Table
    dimension: BoundDimension

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        validate_history(self.table, self.dimension, execute=execute)


@dataclass(frozen=True, slots=True)
class SketchSidecar:
    outputs: tuple[str, ...]
    rows: ibis.Table
    probability: Decimal
    request: SketchRequest


@dataclass(frozen=True, slots=True)
class CompiledStudy:
    table: ibis.Table
    audit_table: ibis.Table
    roles: Roles
    columns: Mapping[str, ColumnMeta]
    feature_cutoffs: Mapping[str, FeatureCutoff]
    eligibility_cutoffs: Mapping[str, FeatureCutoff]
    strategy: Mapping[str, str]
    _anchors: AnchorTable
    _checks: tuple[_DeferredCheck, ...]
    expression_timings: Mapping[str, ExpressionTiming] = field(default_factory=dict)
    sequence_meta: SequenceMeta | None = None
    sketches: tuple[SketchSidecar, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "columns",
            "feature_cutoffs",
            "eligibility_cutoffs",
            "strategy",
            "expression_timings",
        ):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))
        object.__setattr__(self, "_checks", tuple(self._checks))
        object.__setattr__(self, "sketches", tuple(self.sketches))

    def validate(self, *, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Run every deferred integrity check before audit or output leaves its view."""
        self._anchors.validate(execute=execute)
        for check in self._checks:
            check.validate(execute)


def resolve_sketch_sidecars(table: pa.Table, compiled: CompiledStudy, con) -> pa.Table:
    """Execute optional t-digest sidecars inside the owning execution context."""
    if not compiled.sketches:
        return table
    from terrine.temporal.sketches import approximate_quantiles_by_key

    for sidecar in compiled.sketches:
        reader = con.to_pyarrow_batches(sidecar.rows, chunk_size=65_536)
        try:
            estimates, _ = approximate_quantiles_by_key(
                reader,
                sidecar.probability,
                sidecar.request,
            )
        finally:
            reader.close()
        for output in sidecar.outputs:
            meta = compiled.columns[output]
            complete = (
                table.column(meta.complete_column).to_pylist()
                if meta.complete_column is not None
                else [True] * table.num_rows
            )
            keys = zip(
                table.column("unit").to_pylist(),
                table.column("anchor_id").to_pylist(),
                strict=True,
            )
            values = [
                estimates.get(key) if is_complete else None
                for key, is_complete in zip(keys, complete, strict=True)
            ]
            index = table.schema.get_field_index(output)
            table = table.set_column(index, output, pa.array(values, type=pa.float64()))
    return table


def compile_columns(
    design: Design,
    model: Model,
    *,
    include_membership: bool = True,
    _anchor_table: AnchorTable | None = None,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    """Compile candidates and distinct expressions once without reading source rows."""
    _validate_names(design)
    design.validate(model)
    anchors = _anchor_table or compile_anchors(
        design, model, include_membership=include_membership, persisted=persisted_anchors
    )
    table = anchors.table
    values: dict[Expr, CompiledValue] = {}
    checks: list[_DeferredCheck] = []
    normalized_sources, sources, selector_sources, source_histories, states = {}, {}, {}, {}, {}
    sketches: list[SketchSidecar] = []
    sketch_aliases: dict[str, list[str]] = {}
    selector_identity_checks = []
    checked_selector_sources = set()
    for (source_name, measure_key, unit, resolution), relation in (
        persisted_prefixes or {}
    ).items():
        if unit != design.unit:
            raise ValueError("persisted prefix unit does not match design")
        if not isinstance(relation, ibis.Table):
            raise TypeError("persisted prefix relations must be Ibis tables")
        states[(source_name, measure_key, unit, resolution)] = relation
    columns, feature_cutoffs, eligibility_cutoffs, strategies = {}, {}, {}, {}
    public: dict[str, str] = {}
    selector_identity_columns: dict[str, str] = {}
    source_property_columns = {}
    fixed = (
        _fixed_anchors(design, model) if isinstance(design.anchor, (FixedDate, Schedule)) else None
    )

    def events_for(source_name, *, selector=False):
        if source_name not in normalized_sources:
            source = model.sources[source_name]
            raw = model._relation(source.relation)
            configured_id = model.event_ids.get(source_name)
            extra_columns = None
            if configured_id is not None:
                identity_alias = "__terrine_selector_event_id"
                suffix = 0
                while identity_alias in raw.columns:
                    suffix += 1
                    identity_alias = f"__terrine_selector_event_id_{suffix}"
                selector_identity_columns[source_name] = identity_alias
                extra_columns = {identity_alias: configured_id}
            events = normalize_events(
                raw, source, source.entities[design.unit], extra_columns=extra_columns
            )
            histories = {}
            for dimension in source.dimensions:
                history = model._relation(dimension.relation)
                histories[dimension.name] = history
                checks.append(_HistoryValidation(history, dimension))
            source_property_columns[source_name] = _enriched_property_columns(
                source.dimensions, events.columns
            )
            normalized_sources[source_name] = events
            source_histories[source_name] = histories
            sources[source_name] = enrich_events(events, source, histories)
        if not selector:
            return sources[source_name]
        if source_name not in selector_sources:
            source = model.sources[source_name]
            events = normalized_sources[source_name]
            precision = _precision_check(
                model, events, "_timestamp", source_column=source.timestamp_column
            )
            if precision is not None:
                selector_identity_checks.append(precision)
            timestamp_alias = "__terrine_selector_timestamp"
            suffix = 0
            while timestamp_alias in events.columns:
                suffix += 1
                timestamp_alias = f"__terrine_selector_timestamp_{suffix}"
            utc_events = _utc_column(model, events, "_timestamp", timestamp_alias)
            events = utc_events.drop("_timestamp").rename(_timestamp=timestamp_alias)
            selector_sources[source_name] = enrich_events(
                events, source, source_histories[source_name]
            )
        return selector_sources[source_name]

    selector_columns: dict[EventTime, str] = {}
    selector_metadata = {}
    selector_index = 0

    def resolve_selector(selector: EventTime, source_anchors: ibis.Table) -> tuple[ibis.Table, str]:
        nonlocal selector_index
        origin = source_anchors
        if selector.relative_to is not None:
            origin, _ = resolve_selector(selector.relative_to, source_anchors)
        fact = model.catalog.facts[selector.fact]
        source = model.sources[fact.source]
        events = events_for(source.name, selector=True)
        fact_column = source.facts[selector.fact]
        predicates = [
            origin.unit == events["_unit"],
            events["_timestamp"] >= origin.t0 + ibis.interval(seconds=selector.window.start),
            events["_timestamp"] < origin.t0 + ibis.interval(seconds=selector.window.end),
        ]
        if fact_column is not None:
            predicates.append(events[fact_column].notnull())
        identity = selector_identity_columns.get(source.name)
        if source.name not in checked_selector_sources:
            checked_selector_sources.add(source.name)
            selector_identity_checks.extend(
                (
                    _null_check(events, "_unit", "null_event_unit"),
                    _null_check(events, "_timestamp", "null_event_timestamp"),
                )
            )
            if identity is not None:
                selector_identity_checks.extend(
                    _unique_checks(events, identity, "event", scope=("_unit",))
                )
            else:
                keys = events.select(
                    _selector_check_unit=events["_unit"],
                    _selector_check_time=events["_timestamp"],
                )
                ties = (
                    keys.group_by("_selector_check_unit", "_selector_check_time")
                    .aggregate(_selector_check_count=ibis._.count())
                    .filter(ibis._._selector_check_count > 1)
                    .aggregate(invalid_count=ibis._.count())
                )
                selector_identity_checks.append(_integrity_check("tied_event_timestamps", ties))
        property_found = None
        if selector.label_property is not None:
            binding = source.properties.get(selector.label_property)
            if binding is None:
                binding = next(
                    (
                        dimension.properties[selector.label_property]
                        for dimension in source.dimensions
                        if selector.label_property in dimension.properties
                    ),
                    None,
                )
            if binding is None:
                raise _UNSUPPORTED_SHAPE.error()
            if binding.dimension_name is None:
                if binding.column is None:
                    raise _UNSUPPORTED_SHAPE.error()
                predicates.append(events[binding.column] == selector.equals)
            else:
                property_value, property_found = source_property_columns[source.name][
                    (binding.dimension_name, selector.label_property)
                ]
                predicates.extend(
                    [events[property_found], events[property_value] == selector.equals]
                )

        index = selector_index
        selector_index += 1
        metadata = {
            "complete": f"__terrine_selector_complete_{index}",
            "reason": f"__terrine_selector_reason_{index}",
            "observed_from": f"__terrine_selector_observed_from_{index}",
            "observed_until": f"__terrine_selector_observed_until_{index}",
            "start_bound": f"__terrine_selector_start_{index}",
            "end_bound": f"__terrine_selector_end_{index}",
        }
        start_bound = origin.t0 + ibis.interval(seconds=selector.window.start)
        end_bound = origin.t0 + ibis.interval(seconds=selector.window.end)
        coverage = model.readiness.for_fact(source.name)
        source_ready = (start_bound >= ibis.literal(coverage.start)) & (
            end_bound <= ibis.literal(coverage.end_exclusive)
        )
        unit_observed = cast(ir.BooleanValue, ibis.literal(True))
        if "_observed_from" in origin.columns:
            unit_observed &= origin._observed_from.isnull() | (origin._observed_from <= start_bound)
        if "_observed_until" in origin.columns:
            unit_observed &= origin._observed_until.isnull() | (origin._observed_until >= end_bound)
        parent_complete = cast(ir.BooleanValue, ibis.literal(True))
        parent_reason = cast(ir.StringValue, ibis.literal(None, type="string"))
        if selector.relative_to is not None:
            parent = selector_metadata[selector.relative_to]
            parent_complete = origin[parent["complete"]]
            parent_reason = origin[parent["reason"]]
        origin_present = origin.t0.notnull()
        complete = (origin_present & source_ready & unit_observed & parent_complete).fill_null(
            False
        )
        reason = ibis.cases(
            (~parent_complete, parent_reason.fill_null(ibis.literal("incomplete_selector"))),
            (~origin_present, "missing_selector_origin"),
            (~source_ready, "incomplete_coverage"),
            (~unit_observed, "incomplete_observation"),
            else_=ibis.null().cast("string"),
        )
        observed_from = ibis.literal(coverage.start)
        observed_until = ibis.literal(coverage.end_exclusive)
        if "_observed_from" in origin.columns:
            observed_from = ibis.greatest(
                observed_from, origin._observed_from.fill_null(coverage.start)
            )
        if "_observed_until" in origin.columns:
            observed_until = ibis.least(
                observed_until, origin._observed_until.fill_null(coverage.end_exclusive)
            )
        candidates = origin.mutate(
            **{
                metadata["complete"]: complete,
                metadata["reason"]: reason,
                metadata["observed_from"]: observed_from,
                metadata["observed_until"]: observed_until,
                metadata["start_bound"]: start_bound,
                metadata["end_bound"]: end_bound,
            }
        )
        joined = candidates.join(events, predicates=predicates)
        order = [joined["_timestamp"]]
        if identity is not None:
            order.append(joined[identity])
        ranked = joined.mutate(
            __terrine_selector_rank=ibis.row_number().over(
                group_by=[joined.unit, joined.anchor_id], order_by=order
            )
        )
        occurrence = selector.n if selector.occurrence == "nth" else 1
        if occurrence is None:
            raise _UNSUPPORTED_SHAPE.error()
        selected = ranked.filter(ranked.__terrine_selector_rank == occurrence - 1).select(
            __selector_unit=ranked.unit,
            __selector_anchor=ranked.anchor_id,
            __selector_time=ranked["_timestamp"],
            __selector_event_id=ranked[identity] if identity is not None else ranked["_timestamp"],
        )
        if property_found is not None:
            unknown_predicates = [
                origin.unit == events["_unit"],
                events["_timestamp"] >= start_bound,
                events["_timestamp"] < end_bound,
                ~events[property_found],
            ]
            if fact_column is not None:
                unknown_predicates.append(events[fact_column].notnull())
            unknown_events = origin.join(events, predicates=unknown_predicates).select(
                _unknown_unit=origin.unit,
                _unknown_anchor=origin.anchor_id,
                _unknown_time=events["_timestamp"],
                _unknown_event_id=events[identity]
                if identity is not None
                else events["_timestamp"],
            )
            unknown_selected = unknown_events.left_join(
                selected,
                [
                    unknown_events._unknown_unit == selected.__selector_unit,
                    unknown_events._unknown_anchor == selected.__selector_anchor,
                ],
            )
            unknown_precedes = unknown_selected._unknown_time < unknown_selected.__selector_time
            if identity is not None:
                unknown_precedes |= (
                    unknown_selected._unknown_time == unknown_selected.__selector_time
                ) & (unknown_selected._unknown_event_id <= unknown_selected.__selector_event_id)
            unknown_count_name = f"__terrine_selector_unknown_count_{index}"
            known_count_name = f"__terrine_selector_known_count_{index}"
            preceding_count_name = f"__terrine_selector_preceding_count_{index}"
            unknown_counts = unknown_events.group_by(
                _u_unit=unknown_events._unknown_unit,
                _u_anchor=unknown_events._unknown_anchor,
            ).aggregate(**{unknown_count_name: ibis._.count()})
            known_counts = joined.group_by(
                _k_unit=joined.unit,
                _k_anchor=joined.anchor_id,
            ).aggregate(**{known_count_name: ibis._.count()})
            preceding_counts = (
                unknown_selected.filter(unknown_precedes)
                .group_by(
                    _p_unit=unknown_selected._unknown_unit,
                    _p_anchor=unknown_selected._unknown_anchor,
                )
                .aggregate(**{preceding_count_name: ibis._.count()})
            )
            candidates_with_uncertainty = candidates.left_join(
                unknown_counts,
                [
                    candidates.unit == unknown_counts._u_unit,
                    candidates.anchor_id == unknown_counts._u_anchor,
                ],
            )
            candidates_with_uncertainty = candidates_with_uncertainty.left_join(
                known_counts,
                [
                    candidates_with_uncertainty.unit == known_counts._k_unit,
                    candidates_with_uncertainty.anchor_id == known_counts._k_anchor,
                ],
            )
            candidates_with_uncertainty = candidates_with_uncertainty.left_join(
                preceding_counts,
                [
                    candidates_with_uncertainty.unit == preceding_counts._p_unit,
                    candidates_with_uncertainty.anchor_id == preceding_counts._p_anchor,
                ],
            )
            unknown_count = candidates_with_uncertainty[unknown_count_name].fill_null(0)
            known_count = candidates_with_uncertainty[known_count_name].fill_null(0)
            preceding_count = candidates_with_uncertainty[preceding_count_name].fill_null(0)
            unknown_label = cast(
                ir.BooleanValue,
                ibis.ifelse(
                    known_count >= occurrence,
                    preceding_count > 0,
                    known_count + unknown_count >= occurrence,
                ),
            )
            unresolved = candidates_with_uncertainty[metadata["complete"]] & unknown_label
            candidates = candidates_with_uncertainty.mutate(
                **{
                    metadata["complete"]: (
                        candidates_with_uncertainty[metadata["complete"]] & ~unknown_label
                    ),
                    metadata["reason"]: ibis.cases(
                        (unresolved, "incomplete_label_history"),
                        else_=candidates_with_uncertainty[metadata["reason"]],
                    ),
                }
            ).drop(
                "_u_unit",
                "_u_anchor",
                "_k_unit",
                "_k_anchor",
                "_p_unit",
                "_p_anchor",
                unknown_count_name,
                known_count_name,
                preceding_count_name,
            )
        left = candidates
        column = f"__terrine_selector_time_{index}"
        right = selected
        joined_anchors = left.left_join(
            right,
            [left.unit == right.__selector_unit, left.anchor_id == right.__selector_anchor],
        ).select(
            **{name: left[name] for name in left.columns if name not in {"t0", *metadata.values()}},
            t0=ibis.ifelse(left[metadata["complete"]], right.__selector_time, ibis.null()),
            **{column: ibis.ifelse(left[metadata["complete"]], right.__selector_time, ibis.null())},
            **{name: left[name] for name in metadata.values()},
        )
        selector_columns[selector] = column
        selector_metadata[selector] = metadata
        return joined_anchors, column

    def selector_timing(selector: EventTime):
        if selector.relative_to is None:
            origin_support = (0, 0)
            dependencies = {}
        else:
            origin_support, dependencies = selector_timing(selector.relative_to)
        support = (
            origin_support[0] + selector.window.start,
            origin_support[1] + selector.window.end,
        )
        reference = repr(selector)
        return support, {**dependencies, reference: support}

    def attach_anchor_fields(
        result_table: ibis.Table,
        anchor_source: ibis.Table,
        fields: Mapping[str, ir.Value],
    ) -> ibis.Table:
        if not fields:
            return result_table
        anchor_fields = anchor_source.select(
            **{key: anchor_source[key] for key in _ANCHOR_KEY}, **fields
        )
        joined = result_table.left_join(
            anchor_fields, [result_table[key] == anchor_fields[key] for key in _ANCHOR_KEY]
        )
        return joined.select(
            **{name: result_table[name] for name in result_table.columns},
            **{name: anchor_fields[name] for name in fields},
        )

    timing_column_refs = {}

    def expression(node: Expr) -> CompiledValue:
        nonlocal table
        if node in values:
            return values[node]
        output = f"__terrine_value_{len(values)}"
        expression_anchors = anchors.table
        relative = getattr(node, "relative_to", None)
        selector_point_column: str | None = None
        if isinstance(node, EventTime):
            expression_anchors, selector_point_column = resolve_selector(node, expression_anchors)
            selector_meta = selector_metadata[node]
            result_table = expression_anchors.select(
                **{key: expression_anchors[key] for key in _ANCHOR_KEY},
                **{output: expression_anchors[selector_point_column]},
                **{
                    selector_meta[field]: expression_anchors[selector_meta[field]]
                    for field in ("observed_from", "observed_until", "complete", "reason")
                },
            )
            meta = ColumnMeta(
                None,
                "event_time",
                selector_meta["observed_from"],
                selector_meta["observed_until"],
                selector_meta["complete"],
                selector_meta["reason"],
            )
            selector_support, _ = selector_timing(node)
            cutoff = FeatureCutoff(selector_support[1], True, "retrospective_effective_time")
            timing_column_refs[node] = (
                output,
                selector_meta["start_bound"],
                selector_meta["end_bound"],
            )
            strategy = "event_time_selector"
            result = None
        else:
            if relative is not None:
                expression_anchors, selector_point_column = resolve_selector(
                    relative, expression_anchors
                )
                expression_anchors = expression_anchors.mutate(
                    t0=expression_anchors[selector_point_column]
                )
            result = None
        if not isinstance(node, EventTime) and isinstance(node, AttributeAsOf):
            result = compile_asof(
                expression_anchors,
                model,
                unit=design.unit,
                attribute=node.attribute,
                offset=node.before,
                strict=True,
                output=output,
                role="feature",
            )
            meta = result.meta
            cutoff_seconds = (
                selector_timing(relative)[0][1] + node.before
                if relative is not None
                else node.before
            )
            cutoff = FeatureCutoff(cutoff_seconds, True, "retrospective_effective_time")
            strategy = "strict_history"
            result_table = result.table
        elif isinstance(node, MetricQuantile):
            metric = model.metrics[node.metric]
            if len(metric.measures) != 1:
                raise RefusalSpec(
                    "terrine.metric.quantile_requires_single_measure",
                    "Exact quantile requires metric {metric!r} to have exactly one measure.",
                ).error(metric=node.metric)
            unresolved = metric.measures[0]
            source = model.sources[unresolved.source_name]
            events = events_for(source.name)
            measure = resolve_measure(events, unresolved, source=source, allow_nonadditive=True)
            result = aggregate_nonadditive_window(
                expression_anchors,
                events,
                measure,
                source,
                node.window,
                model.readiness,
                output=output,
                role="feature",
                operation="quantile",
                probability=node.probability,
            )
            meta = result.columns[output]
            cutoff = FeatureCutoff(node.window.end, True, "retrospective_effective_time")
            strategy = "sparse_range_join/exact_type7"
            result_table = result.table
        elif isinstance(node, ApproxMetricQuantile):
            metric = model.metrics[node.metric]
            if len(metric.measures) != 1:
                raise RefusalSpec(
                    "terrine.metric.quantile_requires_single_measure",
                    "Approximate quantile requires metric {metric!r} to have exactly one measure.",
                ).error(metric=node.metric)
            unresolved = metric.measures[0]
            source = model.sources[unresolved.source_name]
            events = events_for(source.name)
            measure = resolve_measure(events, unresolved, source=source, allow_nonadditive=True)
            summary = aggregate_nonadditive_window(
                expression_anchors,
                events,
                measure,
                source,
                node.window,
                model.readiness,
                output=output,
                role="feature",
                operation="min",
            )
            result = summary
            sketches.append(
                SketchSidecar(
                    (output,),
                    approximate_quantile_window_values(
                        expression_anchors, events, measure, source, node.window
                    ),
                    node.probability,
                    node.sketch,
                )
            )
            from terrine.temporal.sketches import quantile_request_metadata

            sketch_metadata = quantile_request_metadata(node.sketch)
            meta = replace(
                summary.columns[output],
                numeric_policy=None,
                approximation=ApproximationMeta(
                    algorithm=sketch_metadata.algorithm,
                    implementation="DataSketches t-digest",
                    version=sketch_metadata.version,
                    parameters=dict(sketch_metadata.parameters),
                    error_description="t-digest empirical, no guaranteed bound",
                ),
            )
            cutoff = FeatureCutoff(node.window.end, True, "retrospective_effective_time")
            strategy = "sparse_range_join/tdigest_sidecar"
            result_table = summary.table.mutate(**{output: ibis.null().cast("float64")})
        elif isinstance(node, ApproxMetricDistinct):
            metric = model.metrics[node.metric]
            if len(metric.measures) != 1:
                raise RefusalSpec(
                    "terrine.metric.approx_distinct_requires_single_measure",
                    "Approximate distinct requires metric {metric!r} to have exactly one measure.",
                ).error(metric=node.metric)
            unresolved = metric.measures[0]
            source = model.sources[unresolved.source_name]
            events = events_for(source.name)
            measure = resolve_measure(events, unresolved, source=source, allow_nonadditive=True)
            result = aggregate_nonadditive_window(
                expression_anchors,
                events,
                measure,
                source,
                node.window,
                model.readiness,
                output=output,
                role="feature",
                operation="approx_count_distinct",
            )
            meta = replace(
                result.columns[output],
                numeric_policy=None,
                approximation=ApproximationMeta(
                    algorithm="hll",
                    implementation="DuckDB approx_count_distinct",
                    version=package_version("duckdb"),
                    parameters={"precision": "engine-default"},
                    error_description="HLL relative standard error ≈ 1.04/√m",
                ),
            )
            cutoff = FeatureCutoff(node.window.end, True, "retrospective_effective_time")
            strategy = "sparse_range_join/hll"
            result_table = result.table
        elif (
            isinstance(node, MetricAgg)
            and len(model.metrics[node.metric].measures) == 1
            and model.metrics[node.metric].measures[0].aggregation
            in {"min", "max", "count_distinct"}
        ):
            metric = model.metrics[node.metric]
            unresolved = metric.measures[0]
            source = model.sources[unresolved.source_name]
            events = events_for(source.name)
            measure = resolve_measure(events, unresolved, source=source, allow_nonadditive=True)
            result = aggregate_nonadditive_window(
                expression_anchors,
                events,
                measure,
                source,
                node.window,
                model.readiness,
                output=output,
                role="feature",
                operation=cast(Literal["min", "max", "count_distinct"], measure.aggregation),
            )
            meta = result.columns[output]
            cutoff_seconds = (
                selector_timing(relative)[0][1] + node.window.end
                if relative is not None
                else node.window.end
            )
            cutoff = FeatureCutoff(cutoff_seconds, True, "retrospective_effective_time")
            strategy = "sparse_range_join/nonadditive"
            result_table = result.table
        elif not isinstance(node, EventTime):
            metric = model.metrics[node.metric]
            resolution: Literal["day", "event"] = (
                "day"
                if fixed
                and relative is None
                and all(
                    calendar_average_alignment(
                        node.window, anchor=instant, day_boundary=model.catalog.day_boundary
                    )
                    == "aligned"
                    for instant in fixed
                )
                else "event"
            )
            resolved = []
            supplied = {}
            for measure in metric.measures:
                source = model.sources[measure.source_name]
                events = events_for(source.name)
                measure = resolve_measure(events, measure, source=source)
                key = (measure.source_name, measure_identity(measure), design.unit, resolution)
                if key not in states:
                    states[key] = build_measure_layer(
                        events,
                        measure,
                        source,
                        boundary=model.catalog.day_boundary,
                        resolution=resolution,
                    )
                resolved.append(measure)
                supplied[measure] = states[key]
            result = aggregate_window(
                expression_anchors,
                replace(metric, measures=tuple(resolved)),
                supplied,
                node.window,
                model.readiness,
                boundary=model.catalog.day_boundary,
                output=output,
                role="feature",
                resolution=resolution,
            )
            meta = result.columns[output]
            cutoff_seconds = (
                selector_timing(relative)[0][1] + node.window.end
                if relative is not None
                else node.window.end
            )
            cutoff = FeatureCutoff(cutoff_seconds, True, "retrospective_effective_time")
            strategy = f"sparse_{resolution}_prefix/native"
            result_table = result.table

        if relative is not None and selector_point_column is None:
            raise _UNSUPPORTED_SHAPE.error()
        relative_point_column = selector_point_column or "t0"
        anchor_fields = {}
        if isinstance(node, EventTime):
            selector_meta = selector_metadata[node]
            timing_refs = (
                output,
                selector_meta["start_bound"],
                selector_meta["end_bound"],
            )
            anchor_fields[selector_meta["start_bound"]] = expression_anchors[
                selector_meta["start_bound"]
            ]
            anchor_fields[selector_meta["end_bound"]] = expression_anchors[
                selector_meta["end_bound"]
            ]
        elif relative is None:
            timing_refs = ("t0", None, None)
            timing_column_refs[node] = timing_refs
        else:
            offsets = (
                (node.before, node.before)
                if isinstance(node, AttributeAsOf)
                else (node.window.start, node.window.end)
            )
            point_name = f"{output}_point_time"
            start_name = f"{output}_start_bound"
            end_name = f"{output}_end_bound"
            point = expression_anchors[relative_point_column]
            anchor_fields[point_name] = point
            anchor_fields[start_name] = point + ibis.interval(seconds=offsets[0])
            anchor_fields[end_name] = point + ibis.interval(seconds=offsets[1])
            timing_refs = (point_name, start_name, end_name)
            timing_column_refs[node] = timing_refs

        if relative is not None and not isinstance(node, EventTime):
            selector_meta = selector_metadata[relative]
            origin_complete_name = f"{output}_selector_complete"
            origin_reason_name = f"{output}_selector_reason"
            origin_point_name = f"{output}_selector_point"
            anchor_fields[origin_complete_name] = expression_anchors[selector_meta["complete"]]
            anchor_fields[origin_reason_name] = expression_anchors[selector_meta["reason"]]
            anchor_fields[origin_point_name] = expression_anchors[relative_point_column]

        result_table = attach_anchor_fields(result_table, expression_anchors, anchor_fields)
        if relative is not None and not isinstance(node, EventTime):
            complete_column = meta.complete_column
            reason_column = meta.reason_column
            if complete_column is None or reason_column is None:
                raise _UNSUPPORTED_SHAPE.error()
            origin_complete = result_table[origin_complete_name]
            has_origin = result_table[origin_point_name].notnull()
            result_complete = result_table[complete_column]
            result_reason = result_table[reason_column]
            combined_complete = f"{output}_relative_complete"
            combined_reason = f"{output}_relative_reason"
            relative_values = {
                output: ibis.ifelse(
                    origin_complete & has_origin,
                    result_table[output],
                    ibis.null().cast(result_table[output].type()),
                ),
                combined_complete: (origin_complete & has_origin & result_complete).fill_null(
                    False
                ),
                combined_reason: ibis.cases(
                    (~origin_complete, result_table[origin_reason_name]),
                    (~has_origin, "missing_selector_origin"),
                    (~result_complete, result_reason),
                    else_=ibis.null().cast("string"),
                ),
            }
            if meta.absolute_error_column is not None:
                error_column = meta.absolute_error_column
                relative_values[error_column] = ibis.ifelse(
                    origin_complete & has_origin,
                    result_table[error_column],
                    ibis.null().cast(result_table[error_column].type()),
                )
            result_table = result_table.mutate(**relative_values)
            meta = replace(meta, complete_column=combined_complete, reason_column=combined_reason)
        # Each value derives from the candidates independently, then joins back by
        # null-safe anchor identity, so query nesting does not grow with column count.
        # Internal support names are only unique within one result; rename them here.
        renamed = {
            field: f"{output}_{field.removesuffix('_column')}"
            for field in _META_COLUMNS
            if getattr(meta, field) is not None
        }
        timing_support = {
            name: result_table[name]
            for name in dict.fromkeys(timing_refs)
            if name is not None
            and name not in {*_ANCHOR_KEY, "t0"}
            and name != output
            and name not in renamed.values()
        }
        right = result_table.select(
            **{f"{output}_key_{key}": result_table[key] for key in _ANCHOR_KEY},
            **{output: result_table[output]},
            **{name: result_table[getattr(meta, field)] for field, name in renamed.items()},
            **timing_support,
        )
        assert cutoff is not None
        meta = replace(meta, **renamed)
        table = table.left_join(
            right,
            [table[key].identical_to(right[f"{output}_key_{key}"]) for key in _ANCHOR_KEY],
        )
        if result is not None:
            checks.append(result)
        compiled = CompiledValue(output, meta, cutoff)
        values[node] = compiled
        strategies[output] = strategy
        return compiled

    def predicate_expressions(node):
        if isinstance(node, And):
            for term in node.terms:
                predicate_expressions(term)
        else:
            expression(node.left if isinstance(node, Eq) else node.expr)

    expression_timings = {}
    for role, named in (("feature", design.features), ("outcome", design.outcomes)):
        for name, node in named.items():
            value = expression(node)
            public[name] = value.name
            if any(value.name in sidecar.outputs for sidecar in sketches):
                sketch_aliases.setdefault(value.name, []).append(name)
            columns[name] = replace(value.meta, role=role)
            relative = getattr(node, "relative_to", None)
            if isinstance(node, EventTime):
                support, dependency_supports = selector_timing(node)
                selector_ref = repr(node)
                offsets = None
                point_column = name
                kind = "event_time"
            elif relative is not None:
                origin, dependency_supports = selector_timing(relative)
                selector_ref = repr(relative)
                offsets = (
                    (node.before, node.before)
                    if isinstance(node, AttributeAsOf)
                    else (node.window.start, node.window.end)
                )
                support = (origin[0] + offsets[0], origin[1] + offsets[1])
                point_column = timing_column_refs[node][0]
                kind = "event_time"
            else:
                support = (
                    (node.before, node.before)
                    if isinstance(node, AttributeAsOf)
                    else (node.window.start, node.window.end)
                )
                dependency_supports = {}
                selector_ref = None
                offsets = None
                point_column = timing_column_refs[node][0]
                kind = "fixed_t0"
            _, start_ref, end_ref = timing_column_refs[node]
            expression_timings[name] = ExpressionTiming(
                kind=kind,
                selector_ref=selector_ref,
                relative_offsets=offsets,
                support_seconds=support,
                selector_dependencies=tuple(dependency_supports),
                selector_support_seconds=dependency_supports,
                point_time_column=point_column,
                start_bound_column=start_ref,
                end_bound_column=end_ref,
            )
            strategies[name] = strategies[value.name]
            if role == "feature" and value.cutoff is not None:
                feature_cutoffs[name] = value.cutoff
    for node in design.eligibility.values():
        predicate_expressions(node)
    for name, node in design.eligibility.items():
        result = compile_predicate(table, node, values, output=name)
        table = result.table
        checks.extend(result.checks)
        public[name] = name
        columns[name] = result.meta
        eligibility_cutoffs[name] = result.cutoff
        strategies[name] = "three_valued"
    if design.treatment is not None:
        treatment = design.treatment
        result = compile_asof(
            table,
            model,
            unit=design.unit,
            attribute=treatment.attribute,
            offset=treatment.at,
            strict=False,
            output="treatment",
            role="treatment",
        )
        table = result.table
        checks.append(result)
        public["treatment"] = "treatment"
        columns["treatment"] = result.meta
        strategies["treatment"] = "point_history"
    eligible = ibis.literal(True)
    for name in design.eligibility:
        eligible &= table[name]
    table = table.mutate(__terrine_eligible=eligible)
    support = list(anchors.table.columns)
    for meta in columns.values():
        for name in (
            meta.observed_from_column,
            meta.observed_until_column,
            meta.complete_column,
            meta.reason_column,
            meta.absolute_error_column,
        ):
            if name is not None and name not in support:
                support.append(name)
    for timing in expression_timings.values():
        for name in (
            timing.point_time_column,
            timing.start_bound_column,
            timing.end_bound_column,
        ):
            if name is not None and name in table.columns and name not in support:
                support.append(name)
    projection = {name: table[name] for name in support}
    projection.update({name: table[value] for name, value in public.items()})
    audit = table.select(**projection, __terrine_eligible=table.__terrine_eligible)
    retained = table.filter(table.__terrine_eligible.fill_null(False)).select(**projection)
    if selector_identity_checks:
        anchors = AnchorTable(anchors.table, (*anchors._checks, *selector_identity_checks))
    return CompiledStudy(
        retained,
        audit,
        Roles(
            "unit",
            "anchor_id",
            "t0",
            "treatment" if design.treatment else None,
            tuple(design.features),
            tuple(design.outcomes),
        ),
        columns,
        feature_cutoffs,
        eligibility_cutoffs,
        {name: strategies[name] for name in public},
        anchors,
        tuple(checks),
        expression_timings,
        sketches=tuple(
            replace(sidecar, outputs=tuple(sketch_aliases.get(sidecar_output, ())))
            for sidecar in sketches
            for sidecar_output in sidecar.outputs
        ),
    )
