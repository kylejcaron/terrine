from __future__ import annotations

import hashlib
from collections.abc import Mapping

import ibis

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.compile.layers import LayerKey
from terrine.data import Budget
from terrine.errors import RefusalSpec
from terrine.model import Model
from terrine.spec.design import Design
from terrine.spec.state_panel import StatePanel

_STRICT_SCAN_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
)
_STATE_PANEL_UNSUPPORTED = RefusalSpec(
    "terrine.compile.persisted_state_panel_unsupported",
    "Persisted-layer composition does not support StatePanel designs; use Design.build(model).",
)
_ANCHOR_LAYER_INVALID = RefusalSpec(
    "terrine.compile.persisted_anchor_invalid",
    "Persisted anchor layer identity does not match compiler requirements.",
)


def compose_materialized_frame(
    design: Design,
    model: Model,
    *,
    persisted: Mapping[LayerKey, ibis.Table],
    budget: Budget | None = None,
) -> CompiledStudy:
    """Compose standard compiler output from verified logical anchor and prefix layers.

    Persisted relations are compiler logical inputs, never final frame partitions.
    The caller verifies physical generation provenance before crossing this boundary.
    """
    if budget is not None and not isinstance(budget, Budget):
        raise TypeError("budget must be Budget or None")
    if budget is not None and budget.max_scan_bytes is not None and budget.strict_scan:
        raise _STRICT_SCAN_UNKNOWN.error()
    if isinstance(design.shape, StatePanel):
        raise _STATE_PANEL_UNSUPPORTED.error()
    definition_hash = hashlib.sha256(f"{design.spec_hash}:{model.model_hash}".encode()).hexdigest()
    snapshot_ids = {key.snapshot_id for key in persisted}
    if len(snapshot_ids) > 1 or (snapshot_ids and not next(iter(snapshot_ids))):
        raise ValueError("persisted layers must belong to one non-empty snapshot")
    substitutions: dict[tuple[str, str, str, str], ibis.Table] = {}
    persisted_anchors: ibis.Table | None = None
    for key, relation in persisted.items():
        if (
            key.unit != design.unit
            or key.definition_hash != definition_hash
            or key.resolution not in {"day", "event", "point", "period"}
        ):
            raise ValueError("persisted layer identity does not match compiler requirements")
        if not isinstance(relation, ibis.Table):
            raise TypeError("persisted layer relations must be Ibis tables")
        if key.layer == "anchor":
            if (
                key.source != "__design__"
                or key.measure is not None
                or key.grain != "unit_anchor"
                or key.resolution != "point"
                or persisted_anchors is not None
            ):
                raise _ANCHOR_LAYER_INVALID.error()
            persisted_anchors = relation
            continue
        if (
            key.layer != "prefix"
            or key.grain != "unit"
            or key.measure is None
            or key.resolution not in {"day", "event"}
        ):
            raise ValueError("persisted layer identity does not match compiler requirements")
        identity = (key.source, key.measure, key.unit, key.resolution)
        if identity in substitutions:
            raise ValueError("duplicate persisted logical layer key")
        substitutions[identity] = relation
    return compile_columns(
        design,
        model,
        persisted_prefixes=substitutions,
        persisted_anchors=persisted_anchors,
    )
