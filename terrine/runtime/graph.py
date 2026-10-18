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
    for source in sorted(set(source_names)):
        for partition in partition_names:
            daily, prefix, anchor, frame = (
                LayerNode(layer, source, partition, fingerprint, "pending")
                for layer in ("daily", "prefix", "anchor", "frame")
            )
            nodes.extend((daily, prefix, anchor, frame))
            edges.extend(((daily, prefix), (prefix, anchor), (anchor, frame)))
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
