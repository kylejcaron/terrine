"""Lazy named study columns over sparse temporal states and candidate anchors."""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol

import ibis
import pyarrow as pa

from terrine.compile._asof import compile_asof
from terrine.compile._predicates import CompiledValue, compile_predicate
from terrine.compile.anchors import AnchorTable, _integrity_check, _unique_checks, compile_anchors
from terrine.data import ColumnMeta, ExpressionTiming, FeatureCutoff, Roles, SequenceMeta
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import BoundDimension
from terrine.semantic.rules import calendar_average_alignment
from terrine.spec.anchors import FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import And, AttributeAsOf, Eq, EventTime, Expr
from terrine.spec.sequence import _UNSUPPORTED_SHAPE
from terrine.spec.validate import _fixed_anchors
from terrine.temporal.aggregate import aggregate_window
from terrine.temporal.attributes import enrich_events, validate_history
from terrine.temporal.daily import _sparse_group, resolve_measure, sparse_daily
from terrine.temporal.prefix import sparse_prefix

if TYPE_CHECKING:
    from terrine.model import Model

__all__ = ["CompiledStudy", "compile_columns"]

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

    def validate(self, *, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Run every deferred integrity check before audit or output leaves its view."""
        self._anchors.validate(execute=execute)
        for check in self._checks:
            check.validate(execute)


def compile_columns(
    design: Design, model: Model, *, include_membership: bool = True
) -> CompiledStudy:
    """Compile candidates and distinct expressions once without reading source rows."""
    _validate_names(design)
    design.validate(model)
    anchors = compile_anchors(design, model, include_membership=include_membership)
    table = anchors.table
    values: dict[Expr, CompiledValue] = {}
    checks: list[_DeferredCheck] = []
    sources, states = {}, {}
    selector_identity_checks = []
    checked_selector_sources = set()
    columns, feature_cutoffs, eligibility_cutoffs, strategies = {}, {}, {}, {}
    public: dict[str, str] = {}
    selector_identity_columns: dict[str, str] = {}
    fixed = (
        _fixed_anchors(design, model) if isinstance(design.anchor, (FixedDate, Schedule)) else None
    )

    def events_for(source_name):
        if source_name not in sources:
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
            sources[source_name] = enrich_events(events, source, histories)
        return sources[source_name]

    selector_columns: dict[EventTime, str] = {}

    def resolve_selector(selector: EventTime, source_anchors: ibis.Table) -> tuple[ibis.Table, str]:
        origin = source_anchors
        if selector.relative_to is not None:
            origin, _ = resolve_selector(selector.relative_to, source_anchors)
        fact = model.catalog.facts[selector.fact]
        source = model.sources[fact.source]
        events = events_for(source.name)
        fact_column = source.facts[selector.fact]
        if fact_column is None:
            raise _UNSUPPORTED_SHAPE.error()
        predicates = [
            origin.unit == events["_unit"],
            events["_timestamp"] >= origin.t0 + ibis.interval(seconds=selector.window.start),
            events["_timestamp"] < origin.t0 + ibis.interval(seconds=selector.window.end),
            events[fact_column].notnull(),
        ]
        identity = selector_identity_columns.get(source.name)
        if source.name not in checked_selector_sources:
            checked_selector_sources.add(source.name)
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
        if selector.label_property is not None:
            binding = source.properties.get(selector.label_property)
            if binding is None or binding.dimension_name is not None or binding.column is None:
                raise _UNSUPPORTED_SHAPE.error()
            predicates.append(events[binding.column] == selector.equals)
        joined = origin.join(events, predicates=predicates)
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
        occurrence -= 1
        selected = ranked.filter(ranked.__terrine_selector_rank == occurrence).select(
            __selector_unit=ranked.unit,
            __selector_anchor=ranked.anchor_id,
            __selector_time=ranked["_timestamp"],
        )
        column = f"__terrine_selector_time_{len(selector_columns)}"
        left = source_anchors
        right = selected
        joined_anchors = left.left_join(
            right,
            [left.unit == right.__selector_unit, left.anchor_id == right.__selector_anchor],
        ).select(
            **{name: left[name] for name in left.columns if name != "t0"},
            t0=right.__selector_time,
            **{column: right.__selector_time},
        )
        selector_columns[selector] = column
        return joined_anchors, column

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
            result_table = expression_anchors.select(
                **{key: expression_anchors[key] for key in _ANCHOR_KEY},
                **{output: expression_anchors[selector_point_column]},
            )
            meta = ColumnMeta(None, "event_time", None, None, None, None)
            cutoff = FeatureCutoff(0, True, "retrospective_effective_time")
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
            cutoff = FeatureCutoff(node.before, True, "retrospective_effective_time")
            strategy = "strict_history"
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
                key = (measure, resolution)
                if key not in states:
                    grouped = (
                        sparse_daily(
                            events, measure, model.catalog.day_boundary, "_unit", source=source
                        )
                        if resolution == "day"
                        else _sparse_group(
                            events,
                            measure,
                            {"_timestamp": events._timestamp},
                            "_unit",
                            source=source,
                        )
                    )
                    states[key] = sparse_prefix(grouped, measure, resolution=resolution)
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
            cutoff = FeatureCutoff(node.window.end, True, "retrospective_effective_time")
            strategy = f"sparse_{resolution}_prefix/native"
            result_table = result.table
        # Each value derives from the candidates independently, then joins back by
        # null-safe anchor identity, so query nesting does not grow with column count.
        # Internal support names are only unique within one result; rename them here.
        renamed = {
            field: f"{output}_{field.removesuffix('_column')}"
            for field in _META_COLUMNS
            if getattr(meta, field) is not None
        }
        right = result_table.select(
            **{f"{output}_key_{key}": result_table[key] for key in _ANCHOR_KEY},
            **{output: result_table[output]},
            **{name: result_table[getattr(meta, field)] for field, name in renamed.items()},
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
        reference = f"{selector.fact}:{selector.occurrence}:{selector.n or 1}"
        return support, {**dependencies, reference: support}

    expression_timings = {}
    for role, named in (("feature", design.features), ("outcome", design.outcomes)):
        for name, node in named.items():
            value = expression(node)
            public[name] = value.name
            columns[name] = replace(value.meta, role=role)
            relative = getattr(node, "relative_to", None)
            if isinstance(node, EventTime):
                support, dependency_supports = selector_timing(node)
                selector_ref = f"{node.fact}:{node.occurrence}:{node.n or 1}"
                offsets = None
                point_column = value.name
                kind = "event_time"
            elif relative is not None:
                origin, dependency_supports = selector_timing(relative)
                selector_ref = f"{relative.fact}:{relative.occurrence}:{relative.n or 1}"
                offsets = (
                    (node.before, node.before)
                    if isinstance(node, AttributeAsOf)
                    else (node.window.start, node.window.end)
                )
                support = (origin[0] + offsets[0], origin[1] + offsets[1])
                point_column = selector_columns.get(relative)
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
                point_column = "t0"
                kind = "fixed_t0"
            expression_timings[name] = ExpressionTiming(
                kind=kind,
                selector_ref=selector_ref,
                relative_offsets=offsets,
                support_seconds=support,
                selector_dependencies=tuple(dependency_supports),
                selector_support_seconds=dependency_supports,
                point_time_column=point_column,
                start_bound_column=None,
                end_bound_column=None,
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
    )
