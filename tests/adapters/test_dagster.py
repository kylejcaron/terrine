from typing import Any, cast

import pytest

pytest.importorskip("dagster")

from types import SimpleNamespace

from dagster import DagsterInstance, materialize

from terrine.orchestration.dagster import build_materialization_asset
from terrine.runtime.control import ControlEndpoint
from terrine.runtime.planning import PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def _request(partition="2025-01-15"):
    return PlanRequest(
        "provider-id",
        ModelReference(
            "owner-model", "catalog-hash", "model-hash", "units-hash", "ready-hash", "ids-hash"
        ),
        Design("user", FixedDate(partition), features={}, outcomes={}),
        DestinationRef("catalog.duckdb", "published", "study"),
    )


def test_dagster_partition_reaches_owner_plan_and_receipt_metadata(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    seen = []

    class Client:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            seen.append(request)
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            assert serialized == "saved-recipe"
            return type("Receipt", (), {"run_id": "run-1", "published_generation": 1})()

        def get_run(self, run_id):
            return type("Status", (), {"run_id": run_id, "durable_state": "succeeded"})()

    monkeypatch.setattr(adapter, "ControlClient", Client)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(context["partition_key"]),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    for partition in ("2025-01-14", "2025-01-15"):
        result = materialize([asset], partition_key=partition, instance=DagsterInstance.ephemeral())
        assert result.success
    assert seen[0].design.anchor == FixedDate("2025-01-14")
    assert seen[1].design.anchor == FixedDate("2025-01-15")
    assert seen[0].destination.dataset != seen[1].destination.dataset
    assert [request.destination.dataset for request in seen] == [
        "study_p2025_01_14",
        "study_p2025_01_15",
    ]
    assert [request.requested_scope for request in seen] == ["2025-01-14", "2025-01-15"]
    assert seen[0].invocation_id != seen[1].invocation_id
    assert all(request.invocation_id != "provider-id" for request in seen)


def test_dagster_historical_partitions_publish_distinct_owner_outputs(monkeypatch, tmp_path):
    from datetime import UTC, datetime

    import ibis

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.api import get_run, open_published_frame, run_serialized_plan
    from terrine.runtime.control import ControlEndpoint
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec import agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "historical.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql(
        "INSERT INTO events VALUES ('2025-01-13 12:00:00', 1, 3), ('2025-01-14 12:00:00', 1, 4)"
    )
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    reference_model = ModelReference.from_model(model, owner_key="owner-model")

    class Resolver:
        def resolve(self, reference):
            assert reference == reference_model
            return model

    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    planned = []
    invocations = []
    receipts = []

    class LocalOwnerClient:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            planned.append(request)
            invocation = planner.plan(request, con=connection.con, namespace="owner")
            invocations.append(invocation)
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            receipt = run_serialized_plan(
                serialized_plan,
                con=connection.con,
                namespace="owner",
                request_key=request_key,
                destination=destination,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
            receipts.append(receipt)
            return receipt

        def get_run(self, run_id):
            return get_run(run_id, con=connection.con, namespace="owner")

    import terrine.orchestration.dagster as adapter

    monkeypatch.setattr(adapter, "ControlClient", LocalOwnerClient)
    endpoint = ControlEndpoint(tmp_path / "local-owner.sock")
    asset = build_materialization_asset(
        plan_request_provider=lambda context: PlanRequest(
            "provider-id",
            reference_model,
            Design(
                "user",
                FixedDate(context["partition_key"]),
                features={"total": agg("total", window=("-1d", "0s"))},
                outcomes={},
            ),
            DestinationRef(str(database), "published", "study"),
        ),
        endpoint=endpoint,
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    assert asset.node_def.tags["dagster/concurrency_key"] == "terrine-owner"
    from dagster._utils.tags import TagConcurrencyLimitsCounter

    queue_limit = TagConcurrencyLimitsCounter(
        [
            {
                "key": "dagster/concurrency_key",
                "value": "terrine-owner",
                "limit": 1,
            }
        ],
        [cast(Any, SimpleNamespace(tags=dict(asset.node_def.tags)))],
    )
    assert queue_limit.is_blocked(cast(Any, SimpleNamespace(tags=dict(asset.node_def.tags))))
    try:
        instance = DagsterInstance.ephemeral()
        results = [
            materialize([asset], partition_key=partition, instance=instance)
            for partition in ("2025-01-14", "2025-01-15")
        ]
        assert all(result.success for result in results)
        assert len({receipt.published_generation for receipt in receipts}) == 2
        assert [request.requested_scope for request in planned] == [
            "2025-01-14",
            "2025-01-15",
        ]
        assert [request.design.anchor for request in planned] == [
            FixedDate("2025-01-14"),
            FixedDate("2025-01-15"),
        ]
        assert [request.destination.dataset for request in planned] == [
            "study_p2025_01_14",
            "study_p2025_01_15",
        ]
        assert len({invocation.snapshot_id for invocation in invocations}) == 2
        assert len(receipts) == 2
        assert all(
            receipt.state == "succeeded" and receipt.published_generation for receipt in receipts
        )
        published = [
            open_published_frame(receipt, con=connection.con, namespace="owner")
            for receipt in receipts
        ]
        assert [frame.table.select(["unit", "t0", "total"]).to_pylist() for frame in published] == [
            [{"unit": 1, "t0": datetime(2025, 1, 14, tzinfo=UTC), "total": 3}],
            [{"unit": 1, "t0": datetime(2025, 1, 15, tzinfo=UTC), "total": 4}],
        ]
        assert all(frame.provenance.eligibility_summary.retained == 1 for frame in published)
        assert [frame.provenance.execution_snapshot.snapshot_id for frame in published] == [
            invocation.snapshot_id for invocation in invocations
        ]
    finally:
        connection.disconnect()


def test_dagster_same_root_failed_retry_reuses_fence_scoped_outputs(monkeypatch, tmp_path):
    import ibis
    import pyarrow as pa

    from terrine.data import Coverage, UnitSource
    from terrine.model import Model
    from terrine.runtime.api import (
        find_run_by_request_key,
        get_run,
        run_serialized_plan,
    )
    from terrine.runtime.control import ControlEndpoint
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.spec import agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "retry.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-15', 1, 4)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    model = Model.from_catalog(
        _model(connection).catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    reference_model = ModelReference.from_model(model, owner_key="owner-model")

    class Resolver:
        def resolve(self, reference):
            assert reference == reference_model
            return model

    class FailsOnce:
        def __init__(self):
            self.completed = []
            self.failed = False

        def _run(self, task):
            if len(self.completed) == 1 and not self.failed:
                self.failed = True
                raise RuntimeError("transient task failure")
            self.completed.append(task.task_id)
            return pa.table({"task_id": [task.task_id]})

        def run_task(self, task, dependencies):
            return self._run(task)

        def run_snapshot_task(self, context, task, dependencies):
            return self._run(task)

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    executor = FailsOnce()
    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    invocations = []
    receipts = []

    class LocalOwnerClient:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            invocation = planner.plan(request, con=connection.con, namespace="owner")
            invocations.append(invocation)
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            receipt = run_serialized_plan(
                serialized_plan,
                con=connection.con,
                namespace="owner",
                request_key=request_key,
                destination=destination,
                executor=executor,
                resolver=Resolver(),
                snapshot_authority=authority,
            )
            receipts.append(receipt)
            return receipt

        def get_run(self, run_id):
            return get_run(run_id, con=connection.con, namespace="owner")

    import terrine.orchestration.dagster as adapter

    monkeypatch.setattr(adapter, "ControlClient", LocalOwnerClient)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: PlanRequest(
            "provider-id",
            reference_model,
            Design(
                "user",
                FixedDate(context["partition_key"]),
                features={"total": agg("total", window=("-1d", "0s"))},
                outcomes={},
            ),
            DestinationRef(str(database), "published", "study"),
        ),
        endpoint=ControlEndpoint(tmp_path / "local-owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )

    class Context:
        partition_key = "2025-01-15"
        run = SimpleNamespace(run_id="retry-run", root_run_id="same-root")

        class Instance:
            def add_run_tags(self, run_id, tags):
                pass

        instance = Instance()

        def add_output_metadata(self, metadata):
            pass

    compute = asset.node_def.compute_fn.decorated_fn
    try:
        with pytest.raises(RuntimeError, match="transient task failure"):
            compute(Context())
        failed = find_run_by_request_key(
            invocations[0].request_key,
            destination=invocations[0].destination,
            con=connection.con,
            namespace="owner",
        )
        assert failed is not None
        first_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
            [failed.run_id],
        ).fetchone()[0]
        completed_before_retry = tuple(executor.completed)

        compute(Context())

        advanced_fence = connection.con.execute(
            "SELECT attempt_fence FROM owner.runtime_runs WHERE run_id=?",
            [failed.run_id],
        ).fetchone()[0]
        assert len(invocations) == 2
        assert invocations[0].invocation_id == invocations[1].invocation_id
        assert invocations[0].request_key == invocations[1].request_key
        assert receipts[0].run_id == failed.run_id
        assert advanced_fence > first_fence
        assert completed_before_retry
        assert executor.completed[: len(completed_before_retry)] == list(completed_before_retry)
        assert len(executor.completed) == len(set(executor.completed))
    finally:
        connection.disconnect()


def test_dagster_waits_for_cancel_request_and_reports_final_generation(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    class Client:
        def __init__(self, endpoint, *, namespace):
            self.states = iter(
                [
                    SimpleNamespace(
                        run_id="run-1",
                        durable_state="cancel_requested",
                        pinned_result_ref=None,
                    ),
                    SimpleNamespace(
                        run_id="run-1",
                        durable_state="succeeded",
                        pinned_result_ref="generation-2",
                    ),
                ]
            )

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="run-1", published_generation=None)

        def get_run(self, run_id):
            return next(self.states)

    monkeypatch.setattr(adapter, "ControlClient", Client)
    monkeypatch.setattr(adapter.time, "sleep", lambda seconds: None)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(context["partition_key"]),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    result = materialize([asset], partition_key="2025-01-15", instance=DagsterInstance.ephemeral())
    event = result.get_asset_materialization_events()[0]
    data = cast(Any, event.event_specific_data)
    assert data.materialization.metadata["published_generation"].value == "generation-2"


def test_dagster_rejects_non_succeeded_owner_receipt(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="run-1", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state="failed")

    monkeypatch.setattr(adapter, "ControlClient", Client)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(context["partition_key"]),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    with pytest.raises(RuntimeError, match="finished as failed"):
        materialize([asset], partition_key="2025-01-15", instance=DagsterInstance.ephemeral())


def test_dagster_cancelled_run_fails_without_materialization_retry(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="run-1", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state="cancelled")

    monkeypatch.setattr(adapter, "ControlClient", Client)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(context["partition_key"]),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )

    from dagster import Failure

    with pytest.raises(Failure, match="cancelled") as error:
        materialize([asset], partition_key="2025-01-15", instance=DagsterInstance.ephemeral())

    assert not error.value.allow_retries
