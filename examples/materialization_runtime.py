"""Owner-local Model/Design materialization with retained snapshots and exact reads."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import ibis
import pyarrow as pa

from terrine.compile.columns import compile_columns
from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.runtime import open_published_generation, run_serialized_plan
from terrine.runtime.backends.base import StagedGeneration
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.graph import dependency_graph
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planner import plan_refresh
from terrine.runtime.recipe import DestinationRef, ModelReference, SavedPlanRecipe
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["amount"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"amount": {"source": "events", "column": "amount"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


class DesignExecutor:
    """Execute this example's declared one-day total from the retained event input."""

    def __init__(self, con, catalog: Catalog, design: Design, namespace: str) -> None:
        self.con = con
        self.catalog = catalog
        self.design = design
        self.backend = DuckDBBackend(con, namespace=namespace)
        self.expected_manifest: str | None = None

    def run_snapshot_task(self, context, task, dependencies):
        if task.layer != "frame":
            return pa.table(
                {"user_id": pa.array([], type=pa.int64()), "total": pa.array([], type=pa.int64())}
            )
        events = context.resolve(self.catalog.sources["events"].relation)
        anchor = ibis.timestamp("2025-01-02 00:00:00")
        selected = events.filter(
            (events.occurred_at >= anchor - ibis.interval(days=1)) & (events.occurred_at < anchor)
        )
        result = selected.group_by("user_id").aggregate(total=selected.amount.sum())
        return context.execute(result)

    def publish(self, plan, outputs, lease, run_id):
        next_generation = "generation-1" if self.expected_manifest is None else "generation-2"
        publication = self.backend.publish_generation(
            StagedGeneration(
                plan.dataset,
                next_generation,
                outputs,
                run_id=run_id,
                snapshot_id=plan.snapshot_id,
            ),
            expected_manifest=self.expected_manifest,
            lease=lease,
        )
        self.expected_manifest = publication.generation
        return publication


class ModelResolver:
    def __init__(self, model: Model) -> None:
        self.model = model

    def resolve(self, reference: ModelReference) -> Model:
        return self.model


def _clean_rebuild_rows(model: Model, design: Design) -> list[dict[str, int]]:
    compiled = compile_columns(design, model)
    compiled.validate(execute=model.con.to_pyarrow)
    rows = model.con.to_pyarrow(compiled.table).to_pylist()
    return sorted(
        [{"user_id": row["unit"], "total": row["total"]} for row in rows],
        key=lambda row: row["user_id"],
    )


def _saved_recipe(
    *,
    con,
    model: Model,
    design: Design,
    authority: SnapshotAuthority,
    store: RuntimeStore,
    destination: DestinationRef,
    request_name: str,
    expected_manifest: str | None,
) -> SavedPlanRecipe:
    snapshot = authority.capture(
        model,
        design=design,
        con=con,
        namespace="analytics_owner",
        run_id=request_name,
        budget=Budget(),
    )
    plan = plan_refresh(
        namespace="analytics_owner",
        dataset=destination.dataset,
        definition_hash=design.spec_hash,
        snapshot_id=snapshot.snapshot_id,
        graph=dependency_graph(model, design, partitions=("all",)),
    )
    recipe = SavedPlanRecipe.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="events-model"),
        destination=destination,
        namespace="analytics_owner",
        expected_base_manifest=expected_manifest,
    )
    recipe.pin_snapshot(store)
    return recipe


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="terrine-materialization-") as directory:
        state_path = Path(directory) / "owner.duckdb"
        con = ibis.duckdb.connect(str(state_path))
        try:
            con.raw_sql("CREATE TABLE events(occurred_at TIMESTAMP, user_id BIGINT, amount BIGINT)")
            con.raw_sql(
                "INSERT INTO events VALUES "
                "('2025-01-01 09:00:00', 1, 3), ('2025-01-01 10:00:00', 2, 4)"
            )
            con.raw_sql("CREATE TABLE users(user_id BIGINT PRIMARY KEY)")
            con.raw_sql("INSERT INTO users VALUES (1), (2)")
            catalog = _catalog()
            model = Model.from_catalog(
                catalog,
                con=con,
                readiness={"events": Coverage("2025-01-01", "2025-02-01")},
                units={"user": UnitSource(table="users", key="user_id")},
            )
            design = Design(
                "user",
                FixedDate("2025-01-02T00:00:00+00:00"),
                features={"total": agg("total", window=("-1d", "0s"))},
            )
            owner = RuntimeOwner(state_path=state_path, snapshot_root=Path(directory) / "snapshots")
            authority = SnapshotAuthority(owner)
            store = RuntimeStore(con.con, namespace="analytics_owner")
            store.initialize()
            destination = DestinationRef(str(state_path), "published", "orders")
            executor = DesignExecutor(con.con, catalog, design, namespace="analytics_owner")
            resolver = ModelResolver(model)

            recipe = _saved_recipe(
                con=con,
                model=model,
                design=design,
                authority=authority,
                store=store,
                destination=destination,
                request_name="initial",
                expected_manifest=None,
            )
            saved_path = Path(directory) / "orders.plan.json"
            saved_path.write_text(recipe.to_json(), encoding="utf-8")
            receipt = run_serialized_plan(
                saved_path.read_bytes(),
                con=con.con,
                namespace="analytics_owner",
                request_key="orders:initial",
                executor=executor,
                resolver=resolver,
                snapshot_authority=authority,
            )
            initial = open_published_generation(receipt, con=con.con, namespace="analytics_owner")
            initial_rows = initial.execute().to_dict("records")
            assert sorted(initial_rows, key=lambda row: row["user_id"]) == _clean_rebuild_rows(
                model, design
            )
            print(
                json.dumps(
                    {
                        "generation": receipt.published_generation,
                        "rows": initial_rows,
                    }
                )
            )

            # This correction is explicit: capture a new input receipt and bind it to a new recipe.
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
            corrected_recipe = _saved_recipe(
                con=con,
                model=model,
                design=design,
                authority=authority,
                store=store,
                destination=destination,
                request_name="correction-1",
                expected_manifest=receipt.published_generation,
            )
            saved_path.write_text(corrected_recipe.to_json(), encoding="utf-8")
            corrected = run_serialized_plan(
                saved_path.read_bytes(),
                con=con.con,
                namespace="analytics_owner",
                request_key="orders:correction-1",
                executor=executor,
                resolver=resolver,
                snapshot_authority=authority,
            )
            published = open_published_generation(
                corrected, con=con.con, namespace="analytics_owner"
            )
            corrected_rows = published.execute().to_dict("records")
            assert sorted(corrected_rows, key=lambda row: row["user_id"]) == _clean_rebuild_rows(
                model, design
            )
            print(
                json.dumps(
                    {
                        "generation": corrected.published_generation,
                        "rows": corrected_rows,
                    }
                )
            )
        finally:
            con.disconnect()


if __name__ == "__main__":
    main()
