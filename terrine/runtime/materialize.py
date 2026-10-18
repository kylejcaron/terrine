"""Runtime composition boundary that gates compiler output before staging."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from terrine.compile.columns import CompiledStudy
from terrine.compile.layers import LayerKey
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.model import Model
from terrine.spec.design import Design


def compose_runtime_frame(
    design: Design,
    model: Model,
    *,
    persisted: Mapping[LayerKey, ibis.Table],
    budget: Budget | None = None,
) -> CompiledStudy:
    """Compose and validate a runtime frame before it can enter publication staging."""
    enforce_shape_budget(design.shape, budget)
    compiled = compose_materialized_frame(design, model, persisted=persisted, budget=budget)
    validate_compiled_output(compiled, model.con.to_pyarrow)
    return compiled
