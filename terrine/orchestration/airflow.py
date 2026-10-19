from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, TaskGroup
from airflow.sdk.exceptions import AirflowFailException

from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.planning import PlanInvocation, PlanRequest

__all__ = ["build_materialization_group"]


class _MaterializationOperator(BaseOperator):
    template_fields = ("namespace",)

    def __init__(
        self,
        *,
        plan_request_provider: Callable[[Mapping[str, Any]], PlanRequest],
        endpoint: ControlEndpoint,
        namespace: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.plan_request_provider = plan_request_provider
        self.endpoint = endpoint
        self.namespace = namespace
        self._invocation: PlanInvocation | None = None

    def execute(self, context: Mapping[str, Any]) -> dict[str, str | None]:
        request = self.plan_request_provider(context)
        if not isinstance(request, PlanRequest):
            raise AirflowException("Terrine planning provider must return PlanRequest")
        client = ControlClient(self.endpoint, namespace=self.namespace)
        invocation = client.plan(request)
        self._invocation = invocation
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("Airflow task context has no task instance")
        task_instance.xcom_push(key="terrine_plan_invocation", value=invocation.to_dict())
        receipt = client.run(
            invocation.recipe_json,
            request_key=invocation.request_key,
            destination=invocation.destination,
        )
        status = client.get_run(receipt.run_id)
        while status.run_id == receipt.run_id and status.durable_state in {
            "running",
            "cancel_requested",
        }:
            time.sleep(1)
            status = client.get_run(receipt.run_id)
        if status.durable_state == "cancelled":
            raise AirflowFailException(
                f"Terrine runtime run {receipt.run_id} was cancelled and is terminal"
            )
        if status.run_id != receipt.run_id or status.durable_state != "succeeded":
            raise AirflowException(
                f"Terrine runtime run {receipt.run_id} finished as {status.durable_state}"
            )
        return {
            "run_id": receipt.run_id,
            "state": status.durable_state,
            "published_generation": (
                getattr(status, "pinned_result_ref", None) or receipt.published_generation
            ),
            "snapshot_id": invocation.snapshot_id,
        }

    def on_kill(self) -> None:
        invocation = self._invocation
        if invocation is None:
            return
        client = ControlClient(self.endpoint, namespace=self.namespace)
        try:
            receipt = client.find_run_by_request_key(
                invocation.request_key, destination=invocation.destination
            )
            if receipt is not None:
                client.cancel_run(receipt.run_id)
        except Exception:
            # A cancellation transport failure is not evidence that execution stopped.
            return


def build_materialization_group(
    *,
    plan_request_provider: Callable[[Mapping[str, Any]], PlanRequest],
    endpoint: ControlEndpoint,
    namespace: str,
    max_attempts: int = 1,
    group_id: str = "terrine_materialization",
    pool: str = "terrine_owner",
) -> TaskGroup:
    """Create one bounded Airflow task that delegates planning and execution to the owner."""
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer including the initial attempt")
    if not isinstance(endpoint, ControlEndpoint):
        raise TypeError("endpoint must be ControlEndpoint")
    if not namespace or not pool:
        raise ValueError("namespace and Airflow pool must be non-empty")
    with TaskGroup(group_id=group_id) as group:
        _MaterializationOperator(
            task_id="materialize",
            plan_request_provider=plan_request_provider,
            endpoint=endpoint,
            namespace=namespace,
            retries=max_attempts - 1,
            pool=pool,
        )
    return group
