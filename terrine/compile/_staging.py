"""Metadata-only presentation of the default retained capture."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ibis.common.exceptions import IbisError
from ibis.common.graph import traverse
from ibis.expr import operations as ops

from terrine.data import CoverageSummary, RetainedRoot, RetainedUse, StagingFootprint
from terrine.errors import TerrineError
from terrine.execution._metadata import metadata_operation
from terrine.execution.gates import fold
from terrine.spec._capture import _design_root_names
from terrine.spec.validate import ensure_runtime_shape_supported

if TYPE_CHECKING:
    from collections.abc import Sequence

    import ibis

    from terrine.compile.columns import CompiledStudy
    from terrine.execution.inputs import PhysicalRelationId
    from terrine.model import Model
    from terrine.spec.design import Design


def _leaves(relations: Sequence[ibis.Table]) -> set[ops.Relation]:
    def leaf(node):
        if isinstance(node, (ops.DatabaseTable, ops.SQLQueryResult)):
            return False, node
        return True, None

    return set(traverse(leaf, [relation.op() for relation in relations]))


def _check_leaves(planned: CompiledStudy) -> set[ops.Relation] | None:
    try:
        relations = list(planned._anchors.reads())
        for item in fold(planned._checks):
            if isinstance(item, list):
                relations.extend(check.table for check in item)
            else:
                relations.extend(item.reads())
        return _leaves(relations)
    except (TerrineError, IbisError):
        return None


@metadata_operation()
def staging_footprint(design: Design, model: Model, planned: CompiledStudy) -> StagingFootprint:
    """Describe retained inputs without running study queries or authorizing capture."""
    capabilities = model._engine.capabilities
    if not (
        capabilities.retained_snapshot is True
        and capabilities.table_publication is True
        and capabilities.atomic_manifest_publish is True
    ):
        return StagingFootprint("unavailable", "backend_staging_unsupported")
    if planned.sketches:
        return StagingFootprint("unavailable", "runtime_sketch_unsupported")
    try:
        ensure_runtime_shape_supported(design.shape, anchor=design.anchor)
    except TerrineError:
        return StagingFootprint("unavailable", "runtime_shape_unsupported")
    try:
        names = _design_root_names(model, design)
    except TerrineError:
        return StagingFootprint("unknown", "capture_selection_unknown")

    import duckdb

    from terrine.execution._publish import _inspect_capture_roots

    try:
        roots = _inspect_capture_roots(model.catalog, model.units, model.con, names)
    except TerrineError:
        return StagingFootprint("unknown", "capture_metadata_unknown")
    if roots is None:
        return StagingFootprint("unavailable", "capture_admission_unavailable")

    checks = _check_leaves(planned)
    values = _leaves([planned.table, planned.audit_table]) if checks is not None else set()
    uses: dict[PhysicalRelationId, list[RetainedUse]] = {}
    columns: dict[PhysicalRelationId, tuple[str, ...]] = {}
    check_roots: set[PhysicalRelationId] = set()
    value_roots: set[PhysicalRelationId] = set()
    uncertain: set[PhysicalRelationId] = set()
    for (kind, name, relation), identity in roots:
        coverage = model.readiness.sources.get(name) if kind == "source" else None
        summary = CoverageSummary(coverage.start, coverage.end_exclusive) if coverage else None
        root_uses = uses.setdefault(identity, [])
        role = "fact" if kind == "source" else kind
        root_uses.append(RetainedUse(role, name, summary))
        try:
            bound = model._relation(relation)
            columns[identity] = tuple(bound.columns)
        except (TerrineError, IbisError, duckdb.Error):
            uncertain.add(identity)
            continue
        if checks is not None:
            node = bound.op()
            if node in checks:
                check_roots.add(identity)
                check_use = RetainedUse("check", name, summary)
                if check_use not in root_uses:
                    root_uses.append(check_use)
            if node in values:
                value_roots.add(identity)

    return StagingFootprint(
        "described",
        roots=tuple(
            RetainedRoot(
                kind=identity.kind,
                catalog=identity.catalog,
                schema=identity.schema,
                name=identity.name,
                columns=columns.get(identity),
                row_scope="all_relation_rows" if identity.kind == "table" else "all_result_rows",
                uses=tuple(root_uses),
                check_only=(
                    None
                    if checks is None or identity in uncertain
                    else identity in check_roots and identity not in value_roots
                ),
            )
            for identity, root_uses in uses.items()
        ),
        storage="inputs.duckdb under the owner's snapshot root",
        release="Once no owner pins remain, explicit snapshot GC can collect the file.",
    )
