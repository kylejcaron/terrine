import duckdb
import pyarrow as pa
import pytest

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef


def _plan(definition_hash="def"):
    graph = dependency_graph(
        sources=("events",), definition_hash=definition_hash, partitions=("p1",)
    )
    return plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=definition_hash,
        snapshot_id="snapshot",
        graph=graph,
    )


def test_request_key_replays_same_fingerprint_for_same_destination():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    published = []
    first = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="run-1",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: published.append("g1") or "g1",
    )
    replay = execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="lost-response-retry",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [2]}),
        publish=lambda plan, outputs, lease: published.append("unexpected"),
    )

    assert replay == first
    assert published == ["g1"]


def test_request_key_changed_fingerprint_refuses():
    con = duckdb.connect(":memory:")
    destination = DestinationRef("out.duckdb", "analytics", "orders")
    execute_plan(
        _plan(),
        con=con,
        namespace="owner",
        run_id="run-1",
        request_key="client-request",
        destination=destination,
        run_task=lambda task, dependencies: pa.table({"id": [1]}),
        publish=lambda plan, outputs, lease: "g1",
    )

    with pytest.raises(ValueError, match="different plan"):
        execute_plan(
            _plan("changed"),
            con=con,
            namespace="owner",
            run_id="run-2",
            request_key="client-request",
            destination=destination,
            run_task=lambda task, dependencies: pa.table({"id": [2]}),
            publish=lambda plan, outputs, lease: "g2",
        )


def test_request_key_is_scoped_to_destination():
    con = duckdb.connect(":memory:")
    destinations = (
        DestinationRef("out.duckdb", "analytics", "orders"),
        DestinationRef("out.duckdb", "warehouse", "orders"),
    )
    receipts = []
    for run_id, destination in zip(("run-1", "run-2"), destinations, strict=True):
        receipts.append(
            execute_plan(
                _plan(),
                con=con,
                namespace="owner",
                run_id=run_id,
                request_key="same-key",
                destination=destination,
                run_task=lambda task, dependencies: pa.table({"id": [1]}),
                publish=lambda plan, outputs, lease, generation=run_id: generation,
            )
        )

    assert [receipt.run_id for receipt in receipts] == ["run-1", "run-2"]
