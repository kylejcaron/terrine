from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from terrine.runtime.state import LayerNode

__all__ = ["DependencyGraph", "dependency_graph"]


@dataclass(frozen=True, slots=True)
class DependencyGraph:
    nodes: tuple[LayerNode, ...]
    edges: tuple[tuple[LayerNode, LayerNode], ...]

    def dependencies(self, node: LayerNode) -> tuple[LayerNode, ...]:
        return tuple(source for source, target in self.edges if target == node)

    def topological_order(self) -> tuple[LayerNode, ...]:
        remaining = list(self.nodes)
        ordered: list[LayerNode] = []
        while remaining:
            ready = next(
                (
                    node
                    for node in remaining
                    if all(parent in ordered for parent in self.dependencies(node))
                ),
                None,
            )
            if ready is None:
                raise ValueError("dependency graph contains a cycle")
            ordered.append(ready)
            remaining.remove(ready)
        return tuple(ordered)


def dependency_graph(
    model: object | None = None,
    design: object | None = None,
    *,
    sources: Iterable[str] | None = None,
    definition_hash: str | None = None,
    partitions: Iterable[str] = ("all",),
    include_exports: bool = False,
) -> DependencyGraph:
    """Build stable logical layer dependencies; no runtime execution occurs."""
    source_names = tuple(sources) if sources is not None else _model_sources(model)
    if not source_names:
        raise ValueError("at least one source is required")
    fingerprint = definition_hash or getattr(design, "spec_hash", None) or "unbound"
    partition_names = tuple(partitions)
    nodes: list[LayerNode] = []
    edges: list[tuple[LayerNode, LayerNode]] = []
    from terrine.spec.state_panel import StatePanel

    state_panel = isinstance(getattr(design, "shape", None), StatePanel)
    tracks_certificates = design is not None
    for source in sorted(set(source_names)):
        for partition in partition_names:
            daily = LayerNode("daily", source, partition, fingerprint, "pending")
            prefix = LayerNode("prefix", source, partition, fingerprint, "pending")
            anchor = LayerNode("anchor", source, partition, fingerprint, "pending")
            frame = LayerNode("frame", source, partition, fingerprint, "pending")
            nodes.extend((daily, prefix, anchor))
            edges.append((daily, prefix))
            if tracks_certificates:
                prefix_certificate = LayerNode(
                    "prefix_certificate", source, partition, fingerprint, "pending"
                )
                nodes.append(prefix_certificate)
                edges.append((prefix, prefix_certificate))
                edges.append((prefix_certificate, anchor))
            else:
                edges.append((prefix, anchor))
            if state_panel:
                state_point = LayerNode("state_point", source, partition, fingerprint, "pending")
                state_period = LayerNode("state_period", source, partition, fingerprint, "pending")
                nodes.extend((state_point, state_period))
                edges.extend(((anchor, state_point), (state_point, state_period)))
                dependencies = [state_period]
                if getattr(getattr(design, "shape", None), "baseline_features", None):
                    baseline = LayerNode("baseline", source, partition, fingerprint, "pending")
                    nodes.append(baseline)
                    edges.append((anchor, baseline))
                    dependencies.append(baseline)
                nodes.append(frame)
                edges.extend((dependency, frame) for dependency in dependencies)
            else:
                nodes.append(frame)
                edges.append((anchor, frame))
            if include_exports:
                export = LayerNode("export", source, partition, fingerprint, "pending")
                nodes.append(export)
                edges.append((frame, export))
    return DependencyGraph(tuple(nodes), tuple(edges))


def _model_sources(model: object | None) -> tuple[str, ...]:
    catalog = getattr(model, "catalog", None)
    sources = getattr(catalog, "sources", None)
    if isinstance(sources, dict):
        return tuple(sources)
    if sources is not None:
        return tuple(getattr(source, "name", str(source)) for source in sources)
    return ()
