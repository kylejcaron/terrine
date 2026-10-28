from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass

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
    expression_timings: Mapping[str, object] | None = None,
) -> DependencyGraph:
    """Build stable logical layer dependencies; no runtime execution occurs."""
    source_names = tuple(sources) if sources is not None else _model_sources(model)
    selector_nodes = _selector_nodes(model, design, expression_timings)
    if not source_names and not selector_nodes:
        units = getattr(model, "units", None) or {}
        unit_name = getattr(design, "unit", None)
        if not isinstance(unit_name, str) or unit_name not in units:
            raise ValueError("at least one source is required")
        return _roster_only_graph(
            unit_name,
            design,
            definition_hash=definition_hash,
            partitions=partitions,
            include_exports=include_exports,
        )
    source_names = tuple(sorted(set(source_names) | set(selector_nodes)))
    fingerprint = definition_hash or getattr(design, "spec_hash", None) or "unbound"
    partition_names = tuple(partitions)
    nodes: list[LayerNode] = []
    edges: list[tuple[LayerNode, LayerNode]] = []
    from terrine.spec.state_panel import StatePanel

    state_panel = isinstance(getattr(design, "shape", None), StatePanel)
    for source in sorted(set(source_names)):
        for partition in partition_names:
            daily = LayerNode("daily", source, partition, fingerprint, "pending")
            prefix = LayerNode("prefix", source, partition, fingerprint, "pending")
            anchor = LayerNode("anchor", source, partition, fingerprint, "pending")
            frame_source = source_names[0] if design is not None else source
            frame = LayerNode("frame", frame_source, partition, fingerprint, "pending")
            nodes.extend((daily, prefix, anchor))
            edges.extend(((daily, prefix), (prefix, anchor)))
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
                if frame not in nodes:
                    nodes.append(frame)
                edges.extend((dependency, frame) for dependency in dependencies)
            else:
                if frame not in nodes:
                    nodes.append(frame)
                edges.append((anchor, frame))
            if include_exports:
                export = LayerNode("export", frame_source, partition, fingerprint, "pending")
                if export not in nodes:
                    nodes.append(export)
                edges.append((frame, export))
            for selector_source in sorted(selector_nodes):
                selector = LayerNode("selector", selector_source, partition, fingerprint, "pending")
                if selector not in nodes:
                    nodes.append(selector)
                edges.append((selector, frame))
    return DependencyGraph(tuple(nodes), tuple(dict.fromkeys(edges)))


def _roster_only_graph(
    unit: str,
    design: object,
    *,
    definition_hash: str | None,
    partitions: Iterable[str],
    include_exports: bool,
) -> DependencyGraph:
    fingerprint = definition_hash or getattr(design, "spec_hash", None) or "unbound"
    nodes: list[LayerNode] = []
    edges: list[tuple[LayerNode, LayerNode]] = []
    for partition in partitions:
        roster = LayerNode("roster", unit, partition, fingerprint, "pending")
        anchor = LayerNode("anchor", unit, partition, fingerprint, "pending")
        frame = LayerNode("frame", unit, partition, fingerprint, "pending")
        nodes.extend((roster, anchor, frame))
        edges.extend(((roster, anchor), (anchor, frame)))
        if include_exports:
            export = LayerNode("export", unit, partition, fingerprint, "pending")
            nodes.append(export)
            edges.append((frame, export))
    return DependencyGraph(tuple(nodes), tuple(edges))


def _selector_nodes(
    model: object | None,
    design: object | None,
    expression_timings: Mapping[str, object] | None,
) -> set[str]:
    if model is None or design is None:
        return set()
    from terrine.spec.expressions import EventTime

    catalog = getattr(model, "catalog", None)
    events = getattr(catalog, "events", None)
    if events is None:
        return set(_model_sources(model))

    selectors: dict[str, EventTime] = {}

    def visit(value: object) -> None:
        if isinstance(value, EventTime):
            selectors[repr(value)] = value
            if value.relative_to is not None:
                visit(value.relative_to)
        elif isinstance(value, Mapping):
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            for child in value:
                visit(child)
        elif is_dataclass(value) and not isinstance(value, type):
            for item in fields(value):
                visit(getattr(value, item.name))

    for role in ("features", "outcomes", "eligibility"):
        visit(getattr(design, role, {}))
    sources: set[str] = set()
    unknown_reference = False
    for timing in (expression_timings or {}).values():
        references = (
            getattr(timing, "selector_ref", None),
            *getattr(timing, "selector_dependencies", ()),
        )
        for reference in references:
            if reference is None:
                continue
            if reference not in selectors:
                unknown_reference = True
                continue
            selectors.setdefault(reference, selectors[reference])
    for selector in selectors.values():
        event = events.get(selector.fact)
        if event is None:
            unknown_reference = True
        else:
            sources.add(event.source)
    if unknown_reference:
        return set(_model_sources(model))
    return sources


def _model_sources(model: object | None) -> tuple[str, ...]:
    catalog = getattr(model, "catalog", None)
    sources = getattr(catalog, "sources", None)
    if isinstance(sources, dict):
        return tuple(sources)
    if sources is not None:
        return tuple(getattr(source, "name", str(source)) for source in sources)
    return ()
