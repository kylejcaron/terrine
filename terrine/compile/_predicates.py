"""Exact typed comparisons and certified three-valued eligibility."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import ibis

from terrine.backends.numeric import float_is_finite
from terrine.data import ColumnMeta, FeatureCutoff
from terrine.semantic.models import BoundFilter
from terrine.spec.expressions import And, Eq, Expr, Gt, IsNull, Predicate
from terrine.temporal.aggregate import _Builder, _Check
from terrine.temporal.daily import _filter_comparison, _filter_literal
from terrine.temporal.prefix import FLOAT64_PRECISION, outward_add, outward_subtract


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
    checks: tuple[_Check, ...]


def conjunction_cutoff(cutoffs: tuple[FeatureCutoff, ...]) -> FeatureCutoff:
    latest = max(item.latest_seconds for item in cutoffs)
    return FeatureCutoff(
        latest,
        all(item.strict for item in cutoffs if item.latest_seconds == latest),
        "retrospective_effective_time"
        if any(item.knowledge == "retrospective_effective_time" for item in cutoffs)
        else "recorded_at",
    )


def compile_predicate(
    table: ibis.Table,
    predicate: Predicate,
    values: Mapping[Expr, CompiledValue],
    *,
    output: str,
) -> CompiledPredicate:
    """Gt is certain outside its threshold; Eq is true only for an equal singleton."""
    builder = _Builder(table, output)
    checks: list[_Check] = []

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
            lower = ibis.greatest(*(builder.col(item[2]) for item in terms))
            upper = ibis.least(*(builder.col(item[3]) for item in terms))
            cutoff = conjunction_cutoff(tuple(item[4] for item in terms))
        else:
            expression = node.left if isinstance(node, Eq) else node.expr
            compiled = values[expression]
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
                comparison = _filter_comparison(
                    value, BoundFilter(compiled.name, op, (node.value,))
                )
                uncertain = ibis.literal(False)
                if meta.numeric_policy == "checked_float64_v1":
                    assert meta.absolute_error_column is not None
                    error = builder.col(meta.absolute_error_column)
                    low_name, high_name = builder.put_many(
                        (
                            "comparison_lower",
                            (error == 0.0).ifelse(
                                value, outward_subtract(value, error, upper=False)
                            ),
                        ),
                        (
                            "comparison_upper",
                            (error == 0.0).ifelse(value, outward_add(value, error)),
                        ),
                    )
                    lo, hi = builder.col(low_name), builder.col(high_name)
                    threshold = _filter_literal(value, node.value, compiled.name)
                    invalid = builder.put(
                        "invalid_comparison_bound", ~float_is_finite(lo) | ~float_is_finite(hi)
                    )
                    checks.append(
                        builder.check(
                            [
                                (
                                    invalid,
                                    FLOAT64_PRECISION,
                                    {"output": output, "failure": "bound_overflow"},
                                )
                            ],
                            active=builder.col(meta.complete_column)
                            & builder.col(compiled.name).notnull(),
                        )
                    )
                    if isinstance(node, Gt):
                        comparison = ibis.cases(
                            (lo > threshold, True),
                            (hi <= threshold, False),
                            else_=ibis.null().cast("boolean"),
                        )
                    else:
                        comparison = ibis.cases(
                            ((lo == hi) & (lo == threshold), True),
                            ((threshold < lo) | (threshold > hi), False),
                            else_=ibis.null().cast("boolean"),
                        )
                    uncertain = value.notnull() & threshold.notnull() & comparison.isnull()
                comparison = complete.ifelse(comparison, ibis.null().cast("boolean"))
                reason = ibis.cases(
                    (~complete, cause),
                    (value.isnull(), cause.fill_null("all_values_null")),
                    (uncertain, "uncertain_comparison"),
                    (comparison.isnull(), "null_comparison"),
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
    meta = ColumnMeta(None, "eligibility", lower, upper, complete, reason)
    return CompiledPredicate(result, meta, cutoff, tuple(checks))
