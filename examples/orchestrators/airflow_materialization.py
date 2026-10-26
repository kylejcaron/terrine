"""Airflow DAG that submits one owner-planned Terrine materialization."""

from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from airflow import DAG

from terrine.orchestration.airflow import build_materialization_group
from terrine.runtime import PlanRequest
from terrine.runtime.control import ControlEndpoint


def plan_request(context: dict[str, Any]) -> PlanRequest:
    """Read caller-authored intent; the scheduler never opens the owner's database."""
    dag_run = context["dag_run"]
    raw_request = dag_run.conf.get("terrine_plan_request")
    if not isinstance(raw_request, dict):
        raise ValueError("dag_run.conf.terrine_plan_request must be a PlanRequest object")
    request = PlanRequest.from_dict(raw_request)
    # Retries keep root run identity; attempt number is deliberately not part of the ID.
    invocation_id = hashlib.sha256(dag_run.run_id.encode()).hexdigest()
    return replace(request, invocation_id=f"airflow-{invocation_id}")


with DAG(
    dag_id="terrine_materialization",
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    build_materialization_group(
        plan_request_provider=plan_request,
        endpoint=ControlEndpoint(Path(os.environ["TERRINE_RUNTIME_SOCKET"])),
        namespace=os.environ["TERRINE_RUNTIME_NAMESPACE"],
        max_attempts=2,
        group_id="terrine",
        pool="terrine_owner",
    )
