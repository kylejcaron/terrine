"""As-of columns with explicit history integrity and observation continuity."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

import ibis
import ibis.expr.types as ir
import pyarrow as pa

from terrine.backends.extrema import greatest, least
from terrine.backends.timestamps import compare_timestamps
from terrine.data import ColumnMeta
from terrine.semantic.models import BoundDimension, BoundValidity
from terrine.spec.validate import _MISSING
from terrine.temporal.attributes import attribute_asof, history_checks, validate_history

if TYPE_CHECKING:
    from terrine.model import Model


@dataclass(frozen=True, slots=True)
class AsOfResult:
    table: ibis.Table
    meta: ColumnMeta
    history: ibis.Table
    dimension: BoundDimension

    def reads(self) -> tuple[ibis.Table, ...]:
        """The lookup and the validation queries, each projected to what it uses."""
        return (self.table, *(query for query, _ in history_checks(self.history, self.dimension)))

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        validate_history(self.history, self.dimension, execute=execute)


def resolve_attribute(model: Model, attribute: str):
    declaration = model.catalog.properties.get(attribute)
    for source in model.sources.values():
        if declaration is not None and source.name != declaration.source:
            continue
        for dimension in source.dimensions:
            if attribute in dimension.properties and (
                declaration is None or declaration.dimension == dimension.name
            ):
                return source, dimension.properties[attribute], dimension
        prop = source.properties.get(attribute)
        if prop is not None and prop.dimension_name is None:
            return source, prop, None
    raise _MISSING.error(kind="property", name=attribute)


def compile_asof(
    points: ibis.Table,
    model: Model,
    *,
    unit: str,
    attribute: str,
    offset: int,
    strict: bool,
    output: str,
    role: str,
) -> AsOfResult:
    """A carried change point is known only from its matched point through cutoff."""
    source, prop, dimension = resolve_attribute(model, attribute)
    if dimension is None:
        raw = model._relation(source.relation)
        key = source.entities[unit]
        static = prop.as_of == "static"
        fields = list(
            dict.fromkeys([key, prop.column, *([] if static else [source.timestamp_column])])
        )
        history = raw.select(*fields).distinct()
        dimension = BoundDimension(
            name=f"{source.name}:{attribute}",
            relation=source.relation,
            source_entity=unit,
            entity_column=key,
            validity=None if static else BoundValidity(None, None, source.timestamp_column),
            properties=MappingProxyType({attribute: prop}),
        )
    else:
        history = model._relation(dimension.relation)
    used = {name.casefold() for name in (*points.columns, output)}

    def name(label):
        stem = f"__terrine_asof_{label}"
        candidate, index = stem, 0
        while candidate.casefold() in used:
            index += 1
            candidate = f"{stem}_{index}"
        used.add(candidate.casefold())
        return candidate

    cutoff, matched_at = name("cutoff"), name("matched_at")
    table = points.mutate(**{cutoff: points.t0 + ibis.interval(seconds=offset)})
    table = attribute_asof(
        table,
        history,
        dimension,
        attribute,
        unit_column="unit",
        cutoff_column=cutoff,
        strict=strict,
        matched_at_column=matched_at,
    )
    coverage = model.readiness.for_fact(source.name)
    lower = cast(ir.TimestampScalar, ibis.literal(coverage.start))
    upper = cast(ir.TimestampScalar, ibis.literal(coverage.end_exclusive))
    source_ready = compare_timestamps(
        lower, table[cutoff], "lt" if strict else "le"
    ) & compare_timestamps(table[cutoff], upper, "le" if strict else "lt")
    continuity = dimension.validity is not None and dimension.validity.changed_at is not None
    source_continuous = (
        compare_timestamps(lower, table[matched_at], "le")
        if continuity
        else cast(ir.BooleanScalar, ibis.literal(True))
    )
    unit_ready = cast(ir.BooleanValue, ibis.literal(True))
    observed_from: ir.Value = lower
    observed_until: ir.Value = upper
    if "_observed_from" in table.columns:
        bound = table._observed_from
        observed_from = greatest(lower, bound.fill_null(lower))
        unit_ready &= bound.isnull() | compare_timestamps(
            bound, table[cutoff], "lt" if strict else "le"
        )
        if continuity:
            unit_ready &= bound.isnull() | compare_timestamps(bound, table[matched_at], "le")
    if "_observed_until" in table.columns:
        bound = table._observed_until
        observed_until = least(upper, bound.fill_null(upper))
        unit_ready &= bound.isnull() | compare_timestamps(
            table[cutoff], bound, "le" if strict else "lt"
        )
    found = table.__terrine_attribute_found
    available = (source_ready & found & source_continuous & unit_ready).fill_null(False)
    value = table.__terrine_attribute_value
    reason = ibis.cases(
        (~source_ready, "incomplete_coverage"),
        (~found | ~source_continuous, "missing_history"),
        (~unit_ready, "incomplete_observation"),
        (value.isnull(), "all_values_null"),
        else_=ibis.null().cast("string"),
    )
    from_name, until_name = name("observed_from"), name("observed_until")
    complete_name, reason_name = name("complete"), name("reason")
    table = table.select(
        *points.columns,
        available.ifelse(value, ibis.null().cast(value.type())).name(output),
        observed_from.name(from_name),
        observed_until.name(until_name),
        available.name(complete_name),
        reason.name(reason_name),
    )
    dtype = value.type()
    meta = ColumnMeta(
        window=None,
        role=role,
        observed_from_column=from_name,
        observed_until_column=until_name,
        complete_column=complete_name,
        reason_column=reason_name,
        numeric_policy="exact" if dtype.is_integer() or dtype.is_decimal() else None,
    )
    return AsOfResult(table, meta, history, dimension)
