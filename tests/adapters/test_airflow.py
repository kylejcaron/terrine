import subprocess
import sys
from typing import Any, cast

import pytest

pytest.importorskip("airflow")

from airflow import DAG
from airflow.sdk import BaseOperator, TaskGroup

from terrine.orchestration.airflow import build_materialization_group
from terrine.runtime.control import ControlEndpoint
from terrine.runtime.planning import PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def _plan_request() -> PlanRequest:
    return PlanRequest(
        "airflow-test",
        ModelReference("owner", "catalog", "model", "units", "readiness", "events"),
        Design("user", FixedDate("2025-01-15")),
        DestinationRef("warehouse.duckdb", "published", "study"),
    )


def test_airflow_group_uses_one_owner_task_and_bounds_whole_run(tmp_path):
    with DAG(dag_id="materialize", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: _plan_request(),
            endpoint=ControlEndpoint(tmp_path / "owner.sock"),
            namespace="owner",
            max_attempts=3,
            group_id="terrine",
            pool="terrine-owner",
        )

    assert isinstance(group, TaskGroup)
    task = cast(BaseOperator, next(iter(group.children.values())))
    assert task.retries == 2
    assert task.pool == "terrine-owner"
    assert task.task_id == "terrine.materialize"


def test_airflow_reconnect_waits_for_existing_run_before_releasing_pool_slot(monkeypatch, tmp_path):
    from types import SimpleNamespace

    import terrine.orchestration.airflow as airflow_adapter

    states = iter(("running", "succeeded"))
    sleeps = []
    cancellations = []

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                serialized_plan="{}",
                recipe_json="saved-recipe",
                request_key="same-key",
                destination=request.destination,
                snapshot_id="snapshot",
                to_dict=lambda: {},
            )

        def run(self, serialized, *, request_key, destination):
            assert serialized == "saved-recipe"
            return SimpleNamespace(run_id="active-run", published_generation=1, state="running")

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state=next(states))

        def find_run_by_request_key(self, request_key, *, destination):
            return SimpleNamespace(run_id="active-run")

        def cancel_run(self, run_id):
            cancellations.append(run_id)

    monkeypatch.setattr(airflow_adapter, "ControlClient", Client)
    monkeypatch.setattr(
        airflow_adapter, "time", SimpleNamespace(sleep=sleeps.append), raising=False
    )
    with DAG(dag_id="active-retry", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: _plan_request(),
            endpoint=ControlEndpoint(tmp_path / "owner.sock"),
            namespace="owner",
            pool="terrine-owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))
    result = task.execute(cast(Any, {"ti": SimpleNamespace(xcom_push=lambda **kwargs: None)}))

    assert result["run_id"] == "active-run"
    assert sleeps == [1]
    assert task.pool == "terrine-owner"
    task.on_kill()
    assert cancellations == ["active-run"]


def test_base_import_does_not_load_airflow():
    script = (
        "import sys, terrine; "
        "assert not any(name == 'airflow' or name.startswith('airflow.') "
        "for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_airflow_operator_runs_and_retries_persisted_owner_invocation(tmp_path):
    from pathlib import Path
    from uuid import uuid4

    import ibis
    import pyarrow as pa

    from terrine.data import Coverage, UnitSource
    from terrine.errors import TerrineError
    from terrine.model import Model
    from terrine.runtime.control import ControlClient, ControlServer
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.planning import OwnerPlanner
    from terrine.runtime.snapshot import SnapshotAuthority
    from terrine.runtime.state import RuntimeStore
    from terrine.spec import agg
    from tests.runtime.test_recipe import _model

    database = tmp_path / "operator.duckdb"
    connection = ibis.duckdb.connect(database=str(database))
    connection.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    connection.raw_sql("INSERT INTO events VALUES ('2025-01-10', 1, 3)")
    connection.raw_sql("CREATE TABLE users (id INTEGER)")
    connection.raw_sql("INSERT INTO users VALUES (1)")
    base_model = _model(connection)
    model = Model.from_catalog(
        base_model.catalog,
        con=connection,
        units={"user": UnitSource("users", "id")},
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
    )
    request = PlanRequest(
        "airflow-stable-invocation",
        ModelReference.from_model(model, owner_key="owner-model"),
        Design(
            "user",
            FixedDate("2025-01-15"),
            features={"total": agg("total", window=("-1d", "0s"))},
            outcomes={},
        ),
        DestinationRef(str(database), "published", "study"),
    )

    class Resolver:
        def resolve(self, reference):
            return model

    class Executor:
        calls = 0

        def run_snapshot_task(self, context, task, dependencies):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("planned transient failure")
            return pa.table({"task_id": [task.task_id]})

        def publish(self, plan, outputs, lease, run_id):
            return "generation-1"

    authority = SnapshotAuthority(RuntimeOwner(database, tmp_path / "snapshots"))
    planner = OwnerPlanner(resolver=Resolver(), snapshot_authority=authority)
    endpoint = ControlEndpoint(Path("/tmp") / f"terrine-airflow-{uuid4().hex}.sock")
    executor = Executor()
    server = ControlServer(
        RuntimeStore(connection.con, namespace="owner"),
        namespace="owner",
        endpoint=endpoint,
        executor=executor,
        plan_planner=planner,
        resolver=Resolver(),
        snapshot_authority=authority,
    )
    with DAG(dag_id="owner-retry", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: request,
            endpoint=endpoint,
            namespace="owner",
            pool="terrine-owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))
    operator = cast(Any, task)
    from types import SimpleNamespace

    context = cast(Any, {"ti": SimpleNamespace(xcom_push=lambda **kwargs: None)})
    server.start()
    try:
        with pytest.raises(TerrineError):
            task.execute(context)
        retried = task.execute(context)
        assert retried["state"] == "succeeded"
        assert retried["snapshot_id"] == operator._invocation.snapshot_id
        receipt = ControlClient(endpoint, namespace="owner").find_run_by_request_key(
            operator._invocation.request_key,
            destination=operator._invocation.destination,
        )
        assert receipt is not None
        assert receipt.state == "succeeded"
        assert executor.calls > 1
    finally:
        server.close()
        connection.disconnect()
