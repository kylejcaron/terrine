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

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                serialized_plan="{}",
                request_key="same-key",
                destination=request.destination,
                snapshot_id="snapshot",
                to_dict=lambda: {},
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="active-run", published_generation=1, state="running")

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state=next(states))

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


def test_base_import_does_not_load_airflow():
    script = (
        "import sys, terrine; "
        "assert not any(name == 'airflow' or name.startswith('airflow.') "
        "for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
