import json
from dataclasses import replace

from terrine.agent import DatasetTools
from terrine.runtime.api import run_serialized_plan
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.planning import OwnerPlanner, PlanRequest
from terrine.runtime.recipe import DestinationRef
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.spec.io import from_json


def test_status_events_and_cancel_require_run_destination_ownership(tools, tmp_path):
    import ibis

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.recipe import ModelReference

    con = ibis.duckdb.connect(database=str(tmp_path / "runtime.duckdb"))
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.raw_sql("CREATE TABLE users (id INTEGER)")
        model = Model.from_catalog(
            tools.model.catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "id")},
        )
        reference = ModelReference.from_model(model, owner_key="owner")
        authority = SnapshotAuthority(
            RuntimeOwner(tmp_path / "runtime.duckdb", tmp_path / "snapshots")
        )

        class Resolver:
            def resolve(self, reference):
                return model

        resolver = Resolver()
        planner = OwnerPlanner(resolver=resolver, snapshot_authority=authority)
        design = from_json(tools.design_example().output.serialized)
        destinations = [
            DestinationRef(str(tmp_path / "runtime.duckdb"), "agent", name)
            for name in ("allowed", "private")
        ]
        planner.plan(
            PlanRequest("other-spelling", reference, design, destinations[0]),
            con=con.con,
            namespace="AGENT",
        )
        runs = []
        for index, destination in enumerate(destinations):
            invocation = planner.plan(
                PlanRequest(f"feedback-{index}", reference, design, destination),
                con=con.con,
                namespace="agent",
            )
            runs.append(
                run_serialized_plan(
                    invocation.recipe_json,
                    con=con.con,
                    namespace="agent",
                    request_key=invocation.request_key,
                    resolver=resolver,
                    snapshot_authority=authority,
                )
            )
        policy = replace(
            tools.policy,
            read=True,
            cancel=True,
            model_reference=reference,
            allowed_catalogs=frozenset({reference.catalog_hash}),
            allowed_destinations=frozenset({destinations[0]}),
        )
        feedback = DatasetTools(model, policy, namespace="agent")
        status = feedback.get_run_status(runs[0].run_id)
        assert status.status == "ok" and status.output is not None
        assert status.output.durable_state == "succeeded"
        assert status.output.pinned_result_ref == runs[0].published_generation
        page = feedback.get_run_events(runs[0].run_id, limit=1)
        assert page.output is not None and len(page.output.events) <= 1
        next_page = feedback.get_run_events(runs[0].run_id, after=page.output.next_cursor, limit=1)
        assert next_page.output is not None
        assert next_page.output.events[0].sequence > page.output.events[0].sequence
        cancelled = feedback.request_cancel(runs[0].run_id)
        assert cancelled.output is not None
        assert cancelled.output.disposition == "already_terminal"
        for result in (
            feedback.get_run_status(runs[1].run_id),
            feedback.get_run_events(runs[1].run_id),
            feedback.request_cancel(runs[1].run_id),
        ):
            assert result.code == "terrine.agent.scope_denied"
        assert (
            feedback.get_run_events(runs[0].run_id, limit=0).code == "terrine.agent.invalid_request"
        )
        safe = json.loads(status.model_dump_json())
        assert "recipe_json" not in str(safe) and "database" not in str(safe)
    finally:
        con.disconnect()
