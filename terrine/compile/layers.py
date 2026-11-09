from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

import ibis

from terrine.execution._metadata import metadata_operation
from terrine.spec.design import Design

LayerResolution = Literal["point"]
LayerName = Literal["anchor"]


@dataclass(frozen=True, slots=True)
class LayerKey:
    layer: LayerName
    source: str
    measure: str | None
    unit: str
    grain: str
    resolution: LayerResolution
    definition_hash: str
    snapshot_id: str


@dataclass(frozen=True, slots=True)
class LayerPlan:
    keys: tuple[LayerKey, ...]


@metadata_operation()
def build_layer(design: Design, model, key: LayerKey, *, relation_resolver=None) -> ibis.Table:
    """Build a compiler-planned anchor relation."""
    from dataclasses import replace

    from terrine.compile.anchors import compile_anchors
    from terrine.model import Model
    from terrine.spec.validate import resolve_unit

    design = resolve_unit(design, model)
    if relation_resolver is None:
        bound_model = model
    else:
        from terrine.model import Model

        bound_model = Model._bind(
            model.catalog,
            con=model.con,
            readiness=model.readiness,
            units=model.units,
            event_ids=model.event_ids,
            resolver=relation_resolver,
            input_binding=model.input_binding,
            source_names=set(model.sources),
            engine=model._engine,
        )
    expected = plan_layers(design, bound_model).keys

    if replace(key, snapshot_id="unbound") not in expected:
        raise ValueError("layer key does not belong to this Design and Model")
    return compile_anchors(design, bound_model).table


@metadata_operation()
def plan_layers(design: Design, model) -> LayerPlan:
    """Describe compiler input layers for a design, without consulting runtime state."""
    from terrine.spec.validate import resolve_unit

    design = resolve_unit(design, model)
    unit_name = design.unit
    assert unit_name is not None
    source_schema = {
        name: {
            column: str(dtype.copy(nullable=True))
            for column, dtype in model._resolver(source.relation).schema().items()
        }
        for name, source in model.sources.items()
    }
    definition_hash = hashlib.sha256(
        json.dumps(
            [design.spec_hash, model.model_hash, source_schema],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return LayerPlan(
        (
            LayerKey(
                "anchor",
                "__design__",
                None,
                unit_name,
                "unit_anchor",
                "point",
                definition_hash,
                "unbound",
            ),
        )
    )
