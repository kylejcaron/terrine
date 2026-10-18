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

_STRICT_SCAN_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
)


def compose_materialized_frame(
    design: Design,
    model: Model,
    *,
    persisted: Mapping[LayerKey, ibis.Table],
    budget: Budget | None = None,
) -> CompiledStudy:
    """Compose normal compiler output using verified logical prefix substitutions.

    Relations are prefix sufficient-state relations, not final frame partitions. The
    caller is responsible for verifying the physical record and translating it to a
    logical key before entering this compiler-owned API.
    """
    if budget is not None and not isinstance(budget, Budget):
        raise TypeError("budget must be Budget or None")
    if budget is not None and budget.max_scan_bytes is not None and budget.strict_scan:
        raise _STRICT_SCAN_UNKNOWN.error()
    definition_hash = hashlib.sha256(f"{design.spec_hash}:{model.model_hash}".encode()).hexdigest()
    snapshot_ids = {key.snapshot_id for key in persisted}
    if len(snapshot_ids) > 1 or (snapshot_ids and not next(iter(snapshot_ids))):
        raise ValueError("persisted layers must belong to one non-empty snapshot")
    substitutions: dict[tuple[str, str, str], ibis.Table] = {}
    for key, relation in persisted.items():
        if (
            key.layer != "prefix"
            or key.grain != "unit"
            or key.definition_hash != definition_hash
            or key.measure is None
            or key.resolution not in {"day", "event"}
        ):
            raise ValueError("persisted layer identity does not match compiler requirements")
        if not isinstance(relation, ibis.Table):
            raise TypeError("persisted prefix relations must be Ibis tables")
        identity = (key.source, key.measure, key.resolution)
        if identity in substitutions:
            raise ValueError("duplicate persisted logical layer key")
        substitutions[identity] = relation
    return compile_columns(design, model, persisted_prefixes=substitutions)
