import pytest

pytest.importorskip("dagster")

from types import SimpleNamespace

from dagster import DagsterInstance, materialize

from terrine.orchestration.dagster import build_materialization_asset
from terrine.runtime.control import ControlEndpoint
from terrine.runtime.planning import PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def _request():
    return PlanRequest(
        "provider-id",
        ModelReference(
            "owner-model", "catalog-hash", "model-hash", "units-hash", "ready-hash", "ids-hash"
        ),
        Design("user", FixedDate("2025-01-15"), features={}, outcomes={}),
        DestinationRef("catalog.duckdb", "published", "study"),
    )


def test_dagster_partition_reaches_owner_plan_and_receipt_metadata(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    seen = []

    class Client:
        def __init__(self, endpoint, *, namespace):
            assert namespace == "owner"

        def plan(self, request):
            seen.append(request)
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            assert serialized == "saved-recipe"
            return type("Receipt", (), {"run_id": "run-1", "published_generation": 1})()

        def get_run(self, run_id):
            return type("Status", (), {"run_id": run_id, "durable_state": "succeeded"})()

    monkeypatch.setattr(adapter, "ControlClient", Client)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    for partition in ("2025-01-14", "2025-01-15"):
        result = materialize([asset], partition_key=partition, instance=DagsterInstance.ephemeral())
        assert result.success
    assert [request.requested_scope for request in seen] == ["2025-01-14", "2025-01-15"]
    assert seen[0].invocation_id != seen[1].invocation_id
    assert all(request.invocation_id != "provider-id" for request in seen)


def test_dagster_rejects_non_succeeded_owner_receipt(monkeypatch, tmp_path):
    import terrine.orchestration.dagster as adapter

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, request):
            return SimpleNamespace(
                invocation_id=request.invocation_id,
                request_key="request-key",
                destination=request.destination,
                snapshot_id="snap",
                recipe_json="saved-recipe",
            )

        def run(self, serialized, *, request_key, destination):
            return SimpleNamespace(run_id="run-1", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state="failed")

    monkeypatch.setattr(adapter, "ControlClient", Client)
    asset = build_materialization_asset(
        plan_request_provider=lambda context: _request(),
        endpoint=ControlEndpoint(tmp_path / "owner.sock"),
        namespace="owner",
        partitions_start="2025-01-01",
        resource_concurrency_key="terrine-owner",
    )
    with pytest.raises(RuntimeError, match="finished as failed"):
        materialize([asset], partition_key="2025-01-15", instance=DagsterInstance.ephemeral())
