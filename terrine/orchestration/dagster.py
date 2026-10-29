from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from terrine.errors import RefusalSpec
from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.planning import PlanRequest

__all__ = ["build_materialization_asset"]

_INVALID_CONFIGURATION = RefusalSpec(
    "terrine.runtime.invalid_configuration",
    "Owner runtime configuration {field} is invalid: {reason}.",
)
_INVALID_NAMESPACE = RefusalSpec(
    "terrine.dataset.invalid_destination", "Runtime {field} is invalid: {reason}."
)
_INVALID_REQUEST = RefusalSpec(
    "terrine.runtime.plan_request_invalid", "Owner plan request input {field} is invalid: {reason}."
)
_RUN_FAILED = RefusalSpec(
    "terrine.runtime.execution_failed", "Terrine runtime run {run_id} finished as {state}."
)


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
        raise _INVALID_CONFIGURATION.error(reason="wrong_type", field="endpoint")
    if not isinstance(namespace, str) or not namespace:
        raise _INVALID_NAMESPACE.error(reason="empty_name", field="namespace")
    for field, value in (
        ("resource_concurrency_key", resource_concurrency_key),
        ("asset_key", asset_key),
    ):
        if not isinstance(value, str) or not value:
            raise _INVALID_CONFIGURATION.error(reason="empty_name", field=field)

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
            raise _INVALID_REQUEST.error(reason="wrong_type", field="plan_request_provider")
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
            invocation.saved_plan_json,
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
            raise _RUN_FAILED.error(
                run_id=receipt.run_id,
                state=status.durable_state,
                error_code=getattr(status, "error_code", None),
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
