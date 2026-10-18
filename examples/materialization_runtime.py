"""Small owner-local DuckDB example for durable plan execution and pinned reads."""

import duckdb

from terrine.runtime import open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.graph import dependency_graph
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef


class DuckDBExecutor:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.backend = DuckDBBackend(con, namespace="owner")
        self.generation = 0

    def run_task(self, task, dependencies):
        return self.con.sql("SELECT order_id FROM source_orders").to_arrow_table()

    def publish(self, plan, outputs, lease, run_id):
        self.generation += 1
        generation = f"g{self.generation}"
        return self.backend.publish_generation(
            StagedGeneration(
                plan.dataset,
                generation,
                outputs,
                run_id=run_id,
                snapshot_id=plan.snapshot_id,
            ),
            expected_manifest=f"g{self.generation - 1}" if self.generation > 1 else None,
            lease=lease,
        )


def main() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE source_orders AS SELECT * FROM (VALUES (1), (2)) t(order_id)")
    definition_hash = "orders-definition-v1"
    graph = dependency_graph(
        sources=("orders",), definition_hash=definition_hash, partitions=("all",)
    )
    executor = DuckDBExecutor(con)
    destination = DestinationRef("analytics.duckdb", "warehouse", "orders")

    # Model/Design changes produce a new definition hash; operator-declared source
    # corrections produce a new snapshot/plan and a new request key.
    plan = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=definition_hash,
        snapshot_id="orders-snapshot-1",
        graph=graph,
    )
    serialized = plan.serialize()  # persist these bytes before requesting execution
    receipt = run_serialized_plan(
        serialized,
        con=con,
        namespace="owner",
        request_key="orders:initial",
        destination=destination,
        executor=executor,
    )
    published = open_published_generation(receipt, con=con, namespace="owner")
    print(published.execute())

    # A declared correction is a distinct plan/request, not an automatic freshness guess.
    con.execute("INSERT INTO source_orders VALUES (3)")
    correction = plan_refresh(
        namespace="owner",
        dataset="orders",
        definition_hash=definition_hash,
        snapshot_id="orders-snapshot-correction-1",
        graph=graph,
    )
    corrected = run_serialized_plan(
        correction.serialize(),
        con=con,
        namespace="owner",
        request_key="orders:correction-1",
        destination=destination,
        executor=executor,
    )
    print(open_published_generation(corrected, con=con, namespace="owner").execute())
    assert serialized == plan.serialize()


if __name__ == "__main__":
    main()
