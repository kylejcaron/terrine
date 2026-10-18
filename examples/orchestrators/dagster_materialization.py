"""Dagster daily partitions that delegate planning and execution to a Terrine owner."""

from __future__ import annotations

import json
import os
from pathlib import Path

from terrine.orchestration.dagster import build_materialization_asset
from terrine.runtime.control import ControlEndpoint
from terrine.runtime.planning import PlanRequest


def plan_request(context: dict[str, str]) -> PlanRequest:
    """Load reviewed, caller-authored intent; the Dagster process never opens the warehouse."""
    payload = json.loads(os.environ["TERRINE_PLAN_REQUEST"])
    if not isinstance(payload, dict):
        raise ValueError("TERRINE_PLAN_REQUEST must contain a PlanRequest JSON object")
    return PlanRequest.from_dict(payload)


terrine_dataset = build_materialization_asset(
    plan_request_provider=plan_request,
    endpoint=ControlEndpoint(Path(os.environ["TERRINE_RUNTIME_SOCKET"])),
    namespace=os.environ["TERRINE_RUNTIME_NAMESPACE"],
    partitions_start=os.environ.get("TERRINE_PARTITIONS_START", "2025-01-01"),
    resource_concurrency_key="terrine-owner",
)
