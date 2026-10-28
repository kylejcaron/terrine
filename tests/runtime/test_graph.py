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


def test_design_graph_connects_prefixes_straight_to_anchors():
    from types import SimpleNamespace

    design = SimpleNamespace(spec_hash="d", shape="cross_section")
    graph = dependency_graph(design=design, sources=("events",), partitions=("p1", "p2"))
    assert {node.layer for node in graph.nodes} == {"daily", "prefix", "anchor", "frame"}
    for partition in ("p1", "p2"):
        anchor = next(
            node for node in graph.nodes if node.layer == "anchor" and node.partition == partition
        )
        assert [node.layer for node in graph.dependencies(anchor)] == ["prefix"]


def test_state_panel_graph_tracks_point_period_and_frame_dependencies():
    from types import SimpleNamespace

    from terrine.spec.state_panel import StateObservations, StatePanel

    design = SimpleNamespace(
        spec_hash="definition",
        shape=StatePanel(StateObservations("status", ("new", "old")), max_periods=3),
    )
    graph = dependency_graph(design=design, sources=("events",), partitions=("p1", "p2"))
    layers = {node.layer for node in graph.nodes}
    assert {"state_point", "state_period", "frame"} <= layers
    for partition in ("p1", "p2"):
        period = next(
            node
            for node in graph.nodes
            if node.layer == "state_period" and node.partition == partition
        )
        frame = next(
            node for node in graph.nodes if node.layer == "frame" and node.partition == partition
        )
        assert period in graph.dependencies(frame)


def test_nested_event_selector_dependencies_invalidate_frame_outside_window():
    from types import SimpleNamespace

    from terrine.data import ExpressionTiming
    from terrine.spec.expressions import EventTime
    from terrine.windows import Window

    first = EventTime("amount", Window("-1d", "0s"))
    nested = EventTime("status_time", Window("-1h", "1h"), relative_to=first)
    design = SimpleNamespace(
        spec_hash="definition",
        shape="cross_section",
        features={"nested": nested},
        outcomes={},
        eligibility={},
    )
    model = SimpleNamespace(
        catalog=SimpleNamespace(
            sources={"events": object(), "statuses": object()},
            events={
                "amount": SimpleNamespace(source="events"),
                "status_time": SimpleNamespace(source="statuses"),
            },
        )
    )
    timing = ExpressionTiming(
        kind="event_time",
        selector_ref=repr(nested),
        relative_offsets=(-3600, 3600),
        support_seconds=(-90000, 3600),
        selector_dependencies=(repr(first), repr(nested)),
        selector_support_seconds={repr(first): (-86400, 0), repr(nested): (-90000, 3600)},
        point_time_column="nested",
        start_bound_column="start",
        end_bound_column="end",
    )
    graph = dependency_graph(
        model,
        design,
        partitions=("p1",),
        expression_timings={"nested": timing},
    )
    frame = next(node for node in graph.nodes if node.layer == "frame")
    selector_sources = {
        node.source for node in graph.dependencies(frame) if node.layer == "selector"
    }
    assert selector_sources == {"events", "statuses"}
    selector_order = [node.source for node in graph.nodes if node.layer == "selector"]
    assert selector_order == ["events", "statuses"]


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
