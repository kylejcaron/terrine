from __future__ import annotations

from collections.abc import Mapping

import ibis

from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.compile.layers import LayerKey
from terrine.data import Budget
from terrine.model import Model
from terrine.spec.design import Design


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
    del budget  # output budgets are enforced at collection/staging, not query composition.
    definition_hash = design.spec_hash
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
