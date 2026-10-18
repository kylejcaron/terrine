import subprocess
import sys
from typing import cast

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


def test_base_import_does_not_load_airflow():
    script = (
        "import sys, terrine; "
        "assert not any(name == 'airflow' or name.startswith('airflow.') "
        "for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
