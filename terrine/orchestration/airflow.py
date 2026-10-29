from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, TaskGroup
from airflow.sdk.exceptions import AirflowFailException

from terrine.errors import RefusalSpec
from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.planning import PlanInvocation, PlanRequest

__all__ = ["build_materialization_group"]

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
            raise _INVALID_REQUEST.error(reason="wrong_type", field="plan_request_provider")
        client = ControlClient(self.endpoint, namespace=self.namespace)
        invocation = client.plan(request)
        self._invocation = invocation
        task_instance = context.get("ti")
        if task_instance is None:
            raise AirflowException("Airflow task context has no task instance")
        task_instance.xcom_push(key="terrine_plan_invocation", value=invocation.to_dict())
        receipt = client.run(
            invocation.saved_plan_json,
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
            raise _RUN_FAILED.error(
                run_id=receipt.run_id,
                state=status.durable_state,
                error_code=getattr(status, "error_code", None),
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
        raise _INVALID_CONFIGURATION.error(reason="not_positive_integer", field="max_attempts")
    if not isinstance(endpoint, ControlEndpoint):
        raise _INVALID_CONFIGURATION.error(reason="wrong_type", field="endpoint")
    if not isinstance(namespace, str) or not namespace:
        raise _INVALID_NAMESPACE.error(reason="empty_name", field="namespace")
    if not isinstance(pool, str) or not pool:
        raise _INVALID_CONFIGURATION.error(reason="empty_name", field="pool")
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
