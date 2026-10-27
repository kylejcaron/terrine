"""Owner-local Model/Design materialization with retained snapshots and exact reads."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import ibis

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.runtime import (
    DestinationRef,
    ModelReference,
    RuntimeOwner,
    SavedPlan,
    SnapshotAuthority,
    dependency_graph,
    open_published_frame,
    plan_refresh,
    run_serialized_plan,
)
from terrine.runtime.state import RuntimeStore
from terrine.spec import Design, FixedDate, agg, asof


def _catalog() -> dict:
    return {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["amount"],
                    "properties": ["tier"],
                    "dimensions": ["history"],
                }
            },
            "facts": {"amount": {"source": "events", "column": "amount"}},
            "properties": {
                "tier": {
                    "source": "events",
                    "column": "tier",
                    "dtype": "string",
                    "as_of": "event_time",
                    "dimension": "history",
                }
            },
            "dimensions": {
                "history": {
                    "relation": {"kind": "table", "name": "user_tier"},
                    "source_entity": "user",
                    "entity_column": "user_id",
                    "properties": ["tier"],
                    "validity": {"valid_from": "starts", "valid_to": "ends"},
                }
            },
            "measures": {"total": {"fact": "amount", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }


class ModelResolver:
    def __init__(self, model: Model) -> None:
        self.model = model

    def resolve(self, reference: ModelReference) -> Model:
        return self.model


def _clean_rebuild_rows(model: Model, design: Design) -> list[dict[str, object]]:
    rows = design.build(model).materialize(
        Budget(max_rows=100, max_output_bytes=1_000_000)
    ).table.to_pylist()
    return sorted(
        [{"user_id": row["unit"], "total": row["total"], "tier": row["tier"]} for row in rows],
        key=lambda row: row["user_id"],
    )


def _saved_plan(
    *,
    con,
    model: Model,
    design: Design,
    authority: SnapshotAuthority,
    store: RuntimeStore,
    destination: DestinationRef,
    request_name: str,
    expected_manifest: str | None,
) -> SavedPlan:
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
    saved_plan = SavedPlan.create(
        plan_json=plan.serialize(),
        design=design,
        model=ModelReference.from_model(model, owner_key="events-model"),
        destination=destination,
        namespace="analytics_owner",
        expected_base_manifest=expected_manifest,
    )
    saved_plan.pin_snapshot(store)
    return saved_plan


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
            con.raw_sql(
                "CREATE TABLE user_tier("
                "user_id BIGINT, tier VARCHAR, starts TIMESTAMP, ends TIMESTAMP)"
            )
            con.raw_sql(
                "INSERT INTO user_tier VALUES "
                "(1, 'gold', '2024-01-01', '2026-01-01'), "
                "(2, 'gold', '2024-01-01', '2026-01-01')"
            )
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
                features={
                    "total": agg("total", window=("-1d", "0s")),
                    "tier": asof("tier", before="0s"),
                },
            )
            owner = RuntimeOwner(state_path=state_path, snapshot_root=Path(directory) / "snapshots")
            authority = SnapshotAuthority(owner)
            store = RuntimeStore(con.con, namespace="analytics_owner")
            store.initialize()
            destination = DestinationRef(str(state_path), "published", "orders")
            resolver = ModelResolver(model)

            saved_plan = _saved_plan(
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
            saved_path.write_text(saved_plan.to_json(), encoding="utf-8")
            receipt = run_serialized_plan(
                saved_path.read_bytes(),
                con=con.con,
                namespace="analytics_owner",
                request_key="orders:initial",
                resolver=resolver,
                snapshot_authority=authority,
            )
            initial = open_published_frame(receipt, con=con.con, namespace="analytics_owner")
            initial_rows = [
                {
                    "user_id": row["unit"],
                    "total": int(row["total"]),
                    "tier": row["tier"],
                }
                for row in initial.to_arrow().to_pylist()
            ]
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

            # Capture a new input receipt and bind it to a new saved plan for this correction.
            con.raw_sql("INSERT INTO events VALUES ('2025-01-01 11:00:00', 1, 5)")
            con.raw_sql("UPDATE user_tier SET tier='silver'")
            corrected_saved_plan = _saved_plan(
                con=con,
                model=model,
                design=design,
                authority=authority,
                store=store,
                destination=destination,
                request_name="correction-1",
                expected_manifest=receipt.published_generation,
            )
            saved_path.write_text(corrected_saved_plan.to_json(), encoding="utf-8")
            corrected = run_serialized_plan(
                saved_path.read_bytes(),
                con=con.con,
                namespace="analytics_owner",
                request_key="orders:correction-1",
                resolver=resolver,
                snapshot_authority=authority,
            )
            published = open_published_frame(corrected, con=con.con, namespace="analytics_owner")
            corrected_rows = [
                {
                    "user_id": row["unit"],
                    "total": int(row["total"]),
                    "tier": row["tier"],
                }
                for row in published.to_arrow().to_pylist()
            ]
            assert sorted(corrected_rows, key=lambda row: row["user_id"]) == _clean_rebuild_rows(
                model, design
            )
            retained = open_published_frame(receipt, con=con.con, namespace="analytics_owner")
            assert retained.to_arrow().equals(initial.to_arrow())
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
