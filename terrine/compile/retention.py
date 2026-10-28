"""Bind occurrence identities and follow-up coverage to named retention columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import ibis
import ibis.expr.types as ir

from terrine.backends.extrema import greatest, least
from terrine.backends.timestamps import compare_timestamps
from terrine.data import ColumnMeta
from terrine.semantic.models import BoundSource
from terrine.spec.expressions import RetentionMetric
from terrine.spec.validate import _retention_window
from terrine.temporal.aggregate import TemporalResult
from terrine.temporal.ranges import RangePlan
from terrine.temporal.retention import retention_threshold_bands

if TYPE_CHECKING:
    from terrine.model import Model


def compile_retention(
    anchors: ibis.Table,
    events: ibis.Table,
    expression: RetentionMetric,
    source: BoundSource,
    model: Model,
    *,
    output: str,
    range_plan: RangePlan,
    identity_column: str | None,
    property_columns: dict[tuple[str, str], tuple[str, str]],
) -> TemporalResult:
    identity = source.events[expression.fact] or identity_column
    assert identity is not None
    binding = source.properties.get(expression.label_property)
    if binding is None:
        binding = next(
            dimension.properties[expression.label_property]
            for dimension in source.dimensions
            if expression.label_property in dimension.properties
        )
    if binding.dimension_name is None:
        assert binding.column is not None
        label = events[binding.column]
    else:
        value, found = property_columns[(binding.dimension_name, expression.label_property)]
        label = events[found].ifelse(events[value], ibis.null().cast(events[value].type()))
    occurrences = events.select(
        unit=events._unit,
        event_id=events[identity],
        event_name=label,
        timestamp=events._timestamp,
    )
    coverage = model.readiness.for_fact(source.name)
    window = _retention_window(expression, model)
    finite = expression.finite_horizon_seconds is not None
    bands = retention_threshold_bands(
        occurrences,
        anchors,
        unit="unit",
        event_id="event_id",
        event_name="event_name",
        target_event=expression.target_event,
        timestamp="timestamp",
        anchor_id="anchor_id",
        anchor_timestamp="t0",
        observed_until="_observed_until",
        count_thresholds=expression.count_thresholds,
        finite_horizon_seconds=expression.finite_horizon_seconds,
        source_complete_from=coverage.start,
        source_complete_through=None if finite else coverage.end_exclusive,
        range_plan=range_plan,
    )
    joined = anchors.inner_join(
        bands, [anchors.unit == bands.unit, anchors.anchor_id == bands.anchor_id]
    )
    end = (
        (anchors.t0 + ibis.interval(seconds=window.end))
        if finite
        else ibis.literal(coverage.end_exclusive)
    )
    source_ready = compare_timestamps(
        cast(ir.TimestampValue, end),
        cast(ir.TimestampValue, ibis.literal(coverage.end_exclusive)),
        "le",
    )
    unit_ready = anchors._observed_from.isnull() | compare_timestamps(
        anchors.t0, anchors._observed_from, "ge"
    )
    reason = ibis.cases(
        (~source_ready, "incomplete_coverage"),
        (~unit_ready, "incomplete_observation"),
        else_=joined.reason,
    )
    complete = reason.isnull()
    observed_from = greatest(
        ibis.literal(coverage.start), anchors._observed_from.fill_null(coverage.start)
    )
    observed_until = least(
        ibis.literal(coverage.end_exclusive),
        anchors._observed_until.fill_null(coverage.end_exclusive),
    )
    table = joined.select(
        unit=anchors.unit,
        anchor_id=anchors.anchor_id,
        **{
            output: complete.ifelse(joined.retention_band, ibis.null().cast("int64")),
            "_retention_complete": complete,
            "_retention_reason": reason,
            "_retention_observed_from": observed_from,
            "_retention_observed_until": observed_until,
            f"{output}_end_bound": end,
        },
    )
    meta = ColumnMeta(
        window if finite else None,
        "outcome",
        "_retention_observed_from",
        "_retention_observed_until",
        "_retention_complete",
        "_retention_reason",
        "exact",
    )
    return TemporalResult(table, {output: meta})
