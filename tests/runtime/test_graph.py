from terrine.runtime.graph import dependency_graph


def test_dependency_graph_orders_daily_prefix_anchor_frame():
    graph = dependency_graph(
        sources=("events",),
        definition_hash="definition",
        partitions=("2025-01",),
    )
    ordered = tuple(node.layer for node in graph.topological_order())
    assert ordered == ("daily", "prefix", "anchor", "frame")
    assert graph.dependencies(next(node for node in graph.nodes if node.layer == "frame")) == (
        next(node for node in graph.nodes if node.layer == "anchor"),
    )
