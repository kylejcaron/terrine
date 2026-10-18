import duckdb
import pyarrow as pa

from terrine.runtime.execute import execute_plan
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.status import cancel_run


def test_durable_cancellation_before_publication_prevents_generation():
    con = duckdb.connect(":memory:")
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash="d",
        snapshot_id="s",
        graph=dependency_graph(sources=("events",), definition_hash="d", partitions=("p",)),
    )
    published = []

    def run_task(task, dependencies):
        cancel_run("cancel-me", con=con, namespace="owner")
        return pa.table({"id": [1]})

    result = execute_plan(
        plan,
        con=con,
        namespace="owner",
        run_id="cancel-me",
        run_task=run_task,
        publish=lambda *args: published.append("published"),
    )
    assert result.state == "cancelled"
    assert published == []
