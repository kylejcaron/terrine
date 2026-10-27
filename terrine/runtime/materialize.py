"""Runtime composition boundary that gates compiler output before staging."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from terrine.compile.columns import CompiledStudy
from terrine.compile.layers import LayerKey
from terrine.compile.materialize import compose_materialized_frame
from terrine.data import Budget
from terrine.errors import RefusalSpec
from terrine.execution.gates import enforce_shape_budget, validate_compiled_output
from terrine.model import Model
from terrine.spec.design import Design
from terrine.spec.expressions import ApproxMetricQuantile, _output_leaves
from terrine.spec.validate import _check_predicate_operands

_RUNTIME_SKETCH_UNAVAILABLE = RefusalSpec(
    "terrine.runtime.sketch_sidecar_unavailable",
    "Approximate quantile sidecars are unavailable in runtime planning and composition; "
    "use ordinary Frame materialization.",
)


def ensure_runtime_expressions_supported(design: Design) -> None:
    """Refuse unsupported runtime expressions before resolving source inputs."""
    for named in (design.features, design.outcomes, design.eligibility):
        for value in named.values():
            _check_predicate_operands(value)
    if any(
        isinstance(expression, ApproxMetricQuantile)
        for expressions in (design.features, design.outcomes)
        for value in expressions.values()
        for expression in _output_leaves(value)
    ):
        raise _RUNTIME_SKETCH_UNAVAILABLE.error()


def compose_runtime_frame(
    design: Design,
    model: Model,
    *,
    persisted: Mapping[LayerKey, ibis.Table],
    budget: Budget | None = None,
) -> CompiledStudy:
    """Compose and validate a runtime frame before it can enter publication staging."""
    enforce_shape_budget(design.shape, budget)
    ensure_runtime_expressions_supported(design)
    compiled = compose_materialized_frame(design, model, persisted=persisted, budget=budget)
    validate_compiled_output(compiled, model.con.to_pyarrow)
    return compiled
