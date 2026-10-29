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
from terrine.runtime.saved_plan import DestinationRef, ModelReference
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
                saved_plan_json="saved-plan",
                request_key="same-key",
                destination=request.destination,
                snapshot_id="snapshot",
                to_dict=lambda: {},
            )

        def run(self, serialized, *, request_key, destination):
            assert serialized == "saved-plan"
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


def test_airflow_reconnect_waits_through_pending_cancellation_then_fails_terminally(
    monkeypatch, tmp_path
):
    from types import SimpleNamespace

    from airflow.sdk.exceptions import AirflowFailException

    import terrine.orchestration.airflow as adapter

    statuses = iter(("cancel_requested", "cancelled"))
    sleeps = []

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                saved_plan_json="saved-plan",
                request_key="same-key",
                destination=request.destination,
                snapshot_id="snapshot",
                to_dict=lambda: {},
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="active-run", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state=next(statuses))

    monkeypatch.setattr(adapter, "ControlClient", Client)
    monkeypatch.setattr(adapter.time, "sleep", sleeps.append)
    with DAG(dag_id="cancel-reconnect", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: _plan_request(),
            endpoint=ControlEndpoint(tmp_path / "owner.sock"),
            namespace="owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))

    with pytest.raises(AirflowFailException):
        task.execute(cast(Any, {"ti": SimpleNamespace(xcom_push=lambda **kwargs: None)}))

    assert sleeps == [1]


def test_airflow_response_loss_returns_final_authoritative_generation(monkeypatch, tmp_path):
    from types import SimpleNamespace

    import terrine.orchestration.airflow as adapter

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                saved_plan_json="saved-plan",
                request_key="same-key",
                destination=request.destination,
                snapshot_id="snapshot",
                to_dict=lambda: {},
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="active-run", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(
                run_id=run_id, durable_state="succeeded", pinned_result_ref="generation-2"
            )

    monkeypatch.setattr(adapter, "ControlClient", Client)
    with DAG(dag_id="generation-reconnect", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: _plan_request(),
            endpoint=ControlEndpoint(tmp_path / "owner.sock"),
            namespace="owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))
    result = task.execute(cast(Any, {"ti": SimpleNamespace(xcom_push=lambda **kwargs: None)}))

    assert result["published_generation"] == "generation-2"


def test_airflow_cancel_transport_failure_does_not_claim_runtime_stopped(monkeypatch, tmp_path):
    from types import SimpleNamespace

    import terrine.orchestration.airflow as adapter

    runtime_state = {"value": "running"}

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def find_run_by_request_key(self, request_key, *, destination):
            return SimpleNamespace(run_id="still-running")

        def cancel_run(self, run_id):
            raise OSError("owner unavailable")

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state=runtime_state["value"])

    monkeypatch.setattr(adapter, "ControlClient", Client)
    with DAG(dag_id="cancel-transport-failure", schedule=None, start_date=None):
        group = build_materialization_group(
            plan_request_provider=lambda context: _plan_request(),
            endpoint=ControlEndpoint(tmp_path / "owner.sock"),
            namespace="owner",
        )
    task = cast(BaseOperator, next(iter(group.children.values())))
    task._invocation = SimpleNamespace(
        request_key="same-key", destination=_plan_request().destination
    )

    task.on_kill()

    assert cast(Any, task)._invocation is not None
    status = Client(None, namespace="owner").get_run("still-running")
    assert status.durable_state == "running"


@pytest.mark.parametrize(
    ("options", "code", "field"),
    [
        ({"max_attempts": 0}, "terrine.runtime.invalid_configuration", "max_attempts"),
        ({"endpoint": None}, "terrine.runtime.invalid_configuration", "endpoint"),
        ({"namespace": ""}, "terrine.dataset.invalid_destination", "namespace"),
        ({"pool": ""}, "terrine.runtime.invalid_configuration", "pool"),
    ],
)
def test_airflow_group_configuration_refuses_with_codes(tmp_path, options, code, field):
    from terrine.errors import TerrineError

    arguments: dict[str, Any] = {
        "plan_request_provider": lambda context: _plan_request(),
        "endpoint": ControlEndpoint(tmp_path / "owner.sock"),
        "namespace": "owner",
        **options,
    }
    with pytest.raises(TerrineError) as error:
        build_materialization_group(**arguments)
    assert error.value.code == code
    assert error.value.context["field"] == field
