"""Exact typed comparisons and three-valued boolean columns."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import ibis

import terrine.reasons as reason_codes
from terrine.backends.extrema import greatest, least
from terrine.data import ColumnMeta, FeatureCutoff
from terrine.semantic.filters import lower_filter
from terrine.semantic.models import Filter
from terrine.spec.expressions import And, Expr, Gt, IsNull, Predicate
from terrine.temporal.aggregate import _Builder


@dataclass(frozen=True, slots=True)
class CompiledValue:
    name: str
    meta: ColumnMeta
    cutoff: FeatureCutoff


@dataclass(frozen=True, slots=True)
class CompiledPredicate:
    table: ibis.Table
    meta: ColumnMeta
    cutoff: FeatureCutoff


def conjunction_cutoff(cutoffs: tuple[FeatureCutoff, ...]) -> FeatureCutoff:
    latest = max(item.latest_seconds for item in cutoffs)
    return FeatureCutoff(
        latest_seconds=latest,
        strict=all(item.strict for item in cutoffs if item.latest_seconds == latest),
        knowledge=(
            "retrospective_effective_time"
            if any(item.knowledge == "retrospective_effective_time" for item in cutoffs)
            else "recorded_at"
        ),
    )


def compile_predicate(
    table: ibis.Table,
    predicate: Predicate,
    values: Mapping[Expr, CompiledValue],
    *,
    output: str,
    role: Literal["feature", "outcome", "eligibility"],
) -> CompiledPredicate:
    """Gt is certain outside its threshold; Eq is true only for an equal singleton."""
    builder = _Builder(table, output)

    def compile_node(node):
        if isinstance(node, And):
            terms = [compile_node(term) for term in node.terms]
            status = ibis.literal(True)
            for value, _, _, _, _ in terms:
                status &= builder.col(value)
            status_name = builder.put("conjunction", status)
            status = builder.col(status_name)
            reason = ibis.null().cast("string")
            for value, cause, _, _, _ in reversed(terms):
                reason = builder.col(value).isnull().ifelse(builder.col(cause), reason)
            reason = status.isnull().ifelse(reason, ibis.null().cast("string"))
            lower = greatest(*(builder.col(item[2]) for item in terms))
            upper = least(*(builder.col(item[3]) for item in terms))
            cutoff = conjunction_cutoff(tuple(item[4] for item in terms))
        else:
            compiled = values[node.expr]
            meta = compiled.meta
            assert meta.complete_column is not None and meta.reason_column is not None
            assert meta.observed_from_column is not None and meta.observed_until_column is not None
            value = builder.col(compiled.name)
            complete = builder.col(meta.complete_column)
            cause = builder.col(meta.reason_column)
            lower, upper = (
                builder.col(meta.observed_from_column),
                builder.col(meta.observed_until_column),
            )
            cutoff = compiled.cutoff
            if isinstance(node, IsNull):
                comparison = value.notnull() if node.negate else value.isnull()
                comparison = complete.ifelse(comparison, ibis.null().cast("boolean"))
                reason = complete.ifelse(ibis.null().cast("string"), cause)
            else:
                op = "gt" if isinstance(node, Gt) else "equals"
                comparison = lower_filter(
                    builder.table,
                    Filter(property=compiled.name, op=op, values=(node.value,)),
                    value,
                )
                comparison = complete.ifelse(comparison, ibis.null().cast("boolean"))
                reason = ibis.cases(
                    (~complete, cause),
                    (value.isnull(), cause.fill_null(reason_codes.ALL_VALUES_NULL)),
                    (comparison.isnull(), reason_codes.NULL_COMPARISON),
                    else_=ibis.null().cast("string"),
                )
            status_name = builder.put("predicate", comparison)
        reason_name, from_name, until_name = builder.put_many(
            ("predicate_reason", reason), ("predicate_from", lower), ("predicate_until", upper)
        )
        return status_name, reason_name, from_name, until_name, cutoff

    value, reason, lower, upper, cutoff = compile_node(predicate)
    complete = builder.put("predicate_complete", builder.col(value).notnull())
    result = builder.table.mutate(**{output: builder.col(value)})
    meta = ColumnMeta(
        window=None,
        role=role,
        observed_from_column=lower,
        observed_until_column=upper,
        complete_column=complete,
        reason_column=reason,
    )
    return CompiledPredicate(result, meta, cutoff)
