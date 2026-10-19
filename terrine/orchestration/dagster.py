from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.planning import PlanRequest

__all__ = ["build_materialization_asset"]


def build_materialization_asset(
    *,
    plan_request_provider: Callable[[Mapping[str, str]], PlanRequest],
    endpoint: ControlEndpoint,
    namespace: str,
    partitions_start: str,
    resource_concurrency_key: str,
    asset_key: str = "terrine_dataset",
) -> Any:
    """Build a partitioned Dagster asset whose execution is owned by Terrine."""
    from dagster import DailyPartitionsDefinition, Failure, asset

    if not isinstance(endpoint, ControlEndpoint):
        raise TypeError("endpoint must be ControlEndpoint")
    if not namespace or not resource_concurrency_key or not asset_key:
        raise ValueError("namespace, concurrency key, and asset key must be non-empty")

    @asset(
        name=asset_key,
        partitions_def=DailyPartitionsDefinition(start_date=partitions_start),
        op_tags={"dagster/concurrency_key": resource_concurrency_key},
        required_resource_keys=set(),
    )
    def terrine_materialization(context) -> None:
        partition = context.partition_key
        run = context.run
        root_run_id = getattr(run, "root_run_id", None) or run.run_id
        invocation_id = hashlib.sha256(
            f"{root_run_id}\0{asset_key}\0{partition}".encode()
        ).hexdigest()
        request = plan_request_provider({"partition_key": partition, "run_id": run.run_id})
        if not isinstance(request, PlanRequest):
            raise RuntimeError("Terrine planning provider must return PlanRequest")
        request = replace(
            request,
            invocation_id=invocation_id,
            requested_scope=partition,
            destination=replace(
                request.destination,
                dataset=f"{request.destination.dataset}_p{partition.replace('-', '_')}",
            ),
        )
        client = ControlClient(endpoint, namespace=namespace)
        invocation = client.plan(request)
        context.instance.add_run_tags(
            run.run_id,
            {
                "terrine/invocation_id": invocation.invocation_id,
                "terrine/request_key": invocation.request_key,
                "terrine/run_id": "pending",
                "terrine/snapshot_id": invocation.snapshot_id,
            },
        )
        receipt = client.run(
            invocation.recipe_json,
            request_key=invocation.request_key,
            destination=invocation.destination,
        )
        context.instance.add_run_tags(run.run_id, {"terrine/run_id": receipt.run_id})
        status = client.get_run(receipt.run_id)
        while status.run_id == receipt.run_id and status.durable_state in {
            "running",
            "cancel_requested",
        }:
            time.sleep(1)
            status = client.get_run(receipt.run_id)
        if status.durable_state == "cancelled":
            raise Failure(
                f"Terrine runtime run {receipt.run_id} was cancelled and is terminal",
                allow_retries=False,
            )
        if status.run_id != receipt.run_id or status.durable_state != "succeeded":
            raise RuntimeError(
                f"Terrine runtime run {receipt.run_id} finished as {status.durable_state}"
            )
        published_generation = (
            getattr(status, "pinned_result_ref", None) or receipt.published_generation
        )
        context.add_output_metadata(
            {
                "run_id": receipt.run_id,
                "partition": partition,
                "snapshot_id": invocation.snapshot_id,
                "published_generation": published_generation,
                "state": status.durable_state,
            }
        )

    return terrine_materialization
