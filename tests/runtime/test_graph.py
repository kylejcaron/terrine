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


def test_partition_iterator_is_reused_for_every_source():
    graph = dependency_graph(
        sources=("events", "payments"),
        definition_hash="definition",
        partitions=iter(("2025-01", "2025-02")),
    )
    partitions = {(node.source, node.partition) for node in graph.nodes if node.layer == "daily"}
    assert partitions == {
        ("events", "2025-01"),
        ("events", "2025-02"),
        ("payments", "2025-01"),
        ("payments", "2025-02"),
    }
