"""Call Terrine after a dbt Core build has released its DuckDB writer lock."""

from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping
from pathlib import Path

from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.execute import RunReceipt
from terrine.runtime.planning import PlanRequest


def reviewed_dbt_relation_names(
    manifest_path: str,
    *,
    reviewed_node_ids: Collection[str],
) -> tuple[str, ...]:
    """Return relation names only for explicitly reviewed dbt model node IDs."""
    if not reviewed_node_ids:
        raise ValueError("at least one reviewed dbt node ID is required")
    manifest = json.loads(Path(manifest_path).read_text())
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        raise ValueError("dbt manifest has no model node map")
    relations = []
    for node_id in sorted(reviewed_node_ids):
        node = nodes.get(node_id)
        if not isinstance(node, Mapping) or node.get("resource_type") != "model":
            raise ValueError(f"reviewed dbt node is not a model: {node_id}")
        relation_name = node.get("relation_name")
        if not isinstance(relation_name, str) or not relation_name:
            raise ValueError(f"reviewed dbt model has no relation identity: {node_id}")
        relations.append(relation_name)
    if len(relations) != len(set(relations)):
        raise ValueError("reviewed dbt model identities must be unique")
    return tuple(relations)


def run_after_dbt_build(
    *,
    build_id: str,
    plan_request_provider: Callable[[str], PlanRequest],
    endpoint: ControlEndpoint,
    namespace: str,
) -> RunReceipt:
    """Submit one stable dbt build invocation to the local runtime owner.

    The caller must stop the owner before dbt opens a shared DuckDB file and
    restart it after ``dbt build`` exits. The owner persists invocation replay;
    this function holds no retry state and never manages owner processes.
    """
    if not build_id or not namespace:
        raise ValueError("build_id and namespace must be non-empty")
    request = plan_request_provider(build_id)
    if not isinstance(request, PlanRequest):
        raise TypeError("plan_request_provider must return PlanRequest")
    if request.invocation_id != build_id:
        raise ValueError("PlanRequest.invocation_id must equal build_id")

    client = ControlClient(endpoint, namespace=namespace)
    invocation = client.plan(request)
    if invocation.invocation_id != build_id:
        raise RuntimeError("owner returned a mismatched plan invocation")
    receipt = client.run(
        invocation.serialized_plan,
        request_key=invocation.request_key,
        destination=invocation.destination,
    )
    status = client.get_run(receipt.run_id)
    if status.run_id != receipt.run_id or status.durable_state != "succeeded":
        raise RuntimeError(
            f"Terrine runtime run {receipt.run_id} finished as {status.durable_state}"
        )
    return receipt
