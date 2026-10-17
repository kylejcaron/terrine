"""Lazy named study columns over sparse temporal states and candidate anchors."""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

import ibis
import pyarrow as pa

from terrine.compile._asof import AsOfResult, compile_asof
from terrine.compile._predicates import CompiledValue, compile_predicate
from terrine.compile.anchors import AnchorTable, compile_anchors
from terrine.data import ColumnMeta, FeatureCutoff, Roles
from terrine.errors import RefusalSpec
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import BoundDimension
from terrine.semantic.rules import calendar_average_alignment
from terrine.spec.anchors import FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.expressions import And, AttributeAsOf, Eq, Expr
from terrine.spec.validate import _fixed_anchors
from terrine.temporal.aggregate import TemporalResult, _Check, aggregate_window
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
_ANCHOR_KEY = ("unit", "anchor_id")
_META_COLUMNS = (
    "observed_from_column",
    "observed_until_column",
    "complete_column",
    "reason_column",
    "absolute_error_column",
)


def _validate_names(design: Design) -> None:
    seen = {}
    for name in (*design.features, *design.outcomes, *design.eligibility):
        normalized = unicodedata.normalize("NFC", name).casefold()
        if normalized in _RESERVED or normalized.startswith("__terrine_"):
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
    _checks: tuple[TemporalResult | AsOfResult | _HistoryValidation | _Check, ...]

    def __post_init__(self) -> None:
        for name in ("columns", "feature_cutoffs", "eligibility_cutoffs", "strategy"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))
        object.__setattr__(self, "_checks", tuple(self._checks))

    def validate(self, *, execute: Callable[[ibis.Table], pa.Table]) -> None:
        """Run every deferred integrity check before audit or output leaves its view."""
        self._anchors.validate(execute=execute)
        for check in self._checks:
            check.validate(execute)


def compile_columns(design: Design, model: Model) -> CompiledStudy:
    """Compile candidates and distinct expressions once without reading source rows."""
    _validate_names(design)
    design.validate(model)
    anchors = compile_anchors(design, model)
    table = anchors.table
    values: dict[Expr, CompiledValue] = {}
    checks: list[TemporalResult | AsOfResult | _HistoryValidation | _Check] = []
    sources, states = {}, {}
    columns, feature_cutoffs, eligibility_cutoffs, strategies = {}, {}, {}, {}
    public: dict[str, str] = {}
    fixed = (
        _fixed_anchors(design, model) if isinstance(design.anchor, (FixedDate, Schedule)) else None
    )

    def events_for(source_name):
        if source_name not in sources:
            source = model.sources[source_name]
            raw = model._relation(source.relation)
            events = normalize_events(raw, source, source.entities[design.unit])
            histories = {}
            for dimension in source.dimensions:
                history = model._relation(dimension.relation)
                histories[dimension.name] = history
                checks.append(_HistoryValidation(history, dimension))
            sources[source_name] = enrich_events(events, source, histories)
        return sources[source_name]

    def expression(node: Expr) -> CompiledValue:
        nonlocal table
        if node in values:
            return values[node]
        output = f"__terrine_value_{len(values)}"
        if isinstance(node, AttributeAsOf):
            result = compile_asof(
                anchors.table,
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
        else:
            metric = model.metrics[node.metric]
            resolution: Literal["day", "event"] = (
                "day"
                if fixed
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
                anchors.table,
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
        # Each value derives from the candidates independently, then joins back by
        # null-safe anchor identity, so query nesting does not grow with column count.
        # Internal support names are only unique within one result; rename them here.
        renamed = {
            field: f"{output}_{field.removesuffix('_column')}"
            for field in _META_COLUMNS
            if getattr(meta, field) is not None
        }
        right = result.table.select(
            **{f"{output}_key_{key}": result.table[key] for key in _ANCHOR_KEY},
            **{output: result.table[output]},
            **{name: result.table[getattr(meta, field)] for field, name in renamed.items()},
        )
        meta = replace(meta, **renamed)
        table = table.left_join(
            right,
            [table[key].identical_to(right[f"{output}_key_{key}"]) for key in _ANCHOR_KEY],
        )
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

    for role, named in (("feature", design.features), ("outcome", design.outcomes)):
        for name, node in named.items():
            value = expression(node)
            public[name] = value.name
            columns[name] = replace(value.meta, role=role)
            strategies[name] = strategies[value.name]
            if role == "feature":
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
    )
