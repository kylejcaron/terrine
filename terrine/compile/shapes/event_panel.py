"""Compiler for explicit event-relative panel periods."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from terrine.compile.anchors import AnchorTable, compile_anchors
from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.spec.design import Design
from terrine.spec.panels import EventPanelSpec


def compile_event_panel(
    design: Design,
    model,
    *,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    spec = design.shape
    if not isinstance(spec, EventPanelSpec):
        raise TypeError("compile_event_panel requires an EventPanel Design")
    parent = compile_anchors(design, model, persisted=persisted_anchors)
    expanded = []
    for period in spec.periods:
        offset = ibis.interval(seconds=period * spec.period_seconds)
        expanded.append(
            parent.table.mutate(
                parent_event_id=parent.table.anchor_id,
                parent_t0=parent.table.t0,
                event_period=ibis.literal(period),
                anchor_id=ibis.struct(
                    {"event_id": parent.table.anchor_id, "period": ibis.literal(period)}
                ),
                t0=parent.table.t0 + offset,
            )
        )
    table = expanded[0]
    for period_table in expanded[1:]:
        table = table.union(period_table, distinct=False)
    anchors = AnchorTable(table, parent._checks)
    return compile_columns(
        design, model, _anchor_table=anchors, persisted_prefixes=persisted_prefixes
    )
