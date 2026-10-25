from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("dbt")

from terrine.runtime.control import ControlEndpoint
from terrine.runtime.planning import PlanRequest
from terrine.runtime.recipe import DestinationRef, ModelReference
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design


def test_dbt_relations_require_explicit_model_node_identity(tmp_path):
    from examples.orchestrators.dbt_core_after_build import reviewed_dbt_relation_names

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        '{"nodes":{"model.project.events":{"resource_type":"model",'
        '"relation_name":"main.events"},'
        '"model.project.users":{"resource_type":"model",'
        '"relation_name":"main.users"}}}'
    )

    assert reviewed_dbt_relation_names(
        str(manifest_path),
        reviewed_node_ids={"model.project.users", "model.project.events"},
    ) == ("main.events", "main.users")
    with pytest.raises(ValueError):
        reviewed_dbt_relation_names(
            str(manifest_path),
            reviewed_node_ids={"source.project.events"},
        )


def _request(invocation_id: str) -> PlanRequest:
    return PlanRequest(
        invocation_id,
        ModelReference(
            "owner-model", "catalog-hash", "model-hash", "units-hash", "ready-hash", "ids-hash"
        ),
        Design("user", FixedDate("2025-01-15"), features={}, outcomes={}),
        DestinationRef("catalog.duckdb", "published", "study"),
    )


def test_dbt_after_build_replays_stable_build_invocation(monkeypatch):
    import examples.orchestrators.dbt_core_after_build as recipe

    request = _request("build-17")
    invocation = SimpleNamespace(
        invocation_id="build-17",
        recipe_json="saved-recipe",
        request_key="destination-key",
        destination=object(),
    )
    calls = []

    class Client:
        def __init__(self, endpoint, *, namespace):
            assert endpoint.path.name == "owner.sock"
            assert namespace == "owner"

        def plan(self, received):
            calls.append(("plan", received.invocation_id))
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            calls.append(("run", serialized_plan, request_key, destination))
            return SimpleNamespace(
                run_id="runtime-run",
                state="succeeded",
                published_generation="generation-1",
            )

        def get_run(self, run_id):
            assert run_id == "runtime-run"
            return SimpleNamespace(
                run_id=run_id,
                durable_state="succeeded",
                pinned_result_ref=None,
            )

    monkeypatch.setattr(recipe, "ControlClient", Client)
    for _ in range(2):
        receipt = recipe.run_after_dbt_build(
            build_id="build-17",
            plan_request_provider=lambda build_id: request,
            endpoint=ControlEndpoint(Path("owner.sock")),
            namespace="owner",
        )
        assert receipt.run_id == "runtime-run"
    assert calls == [
        ("plan", "build-17"),
        ("run", "saved-recipe", "destination-key", invocation.destination),
        ("plan", "build-17"),
        ("run", "saved-recipe", "destination-key", invocation.destination),
    ]


def test_dbt_after_build_rejects_request_for_different_build(monkeypatch):
    import examples.orchestrators.dbt_core_after_build as recipe

    request = _request("other-build")

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

    monkeypatch.setattr(recipe, "ControlClient", Client)
    with pytest.raises(ValueError):
        recipe.run_after_dbt_build(
            build_id="build-17",
            plan_request_provider=lambda build_id: request,
            endpoint=ControlEndpoint(Path("owner.sock")),
            namespace="owner",
        )


def test_dbt_after_build_refuses_non_succeeded_runtime_status(monkeypatch):
    import examples.orchestrators.dbt_core_after_build as recipe

    request = _request("build-17")
    invocation = SimpleNamespace(
        invocation_id="build-17",
        recipe_json="saved-recipe",
        request_key="destination-key",
        destination=object(),
    )

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, received):
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            return SimpleNamespace(run_id="runtime-run", state="succeeded")

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state="failed")

    monkeypatch.setattr(recipe, "ControlClient", Client)
    with pytest.raises(RuntimeError):
        recipe.run_after_dbt_build(
            build_id="build-17",
            plan_request_provider=lambda build_id: request,
            endpoint=ControlEndpoint(Path("owner.sock")),
            namespace="owner",
        )


def test_dbt_after_build_reconnects_to_active_run_until_terminal(monkeypatch):
    import examples.orchestrators.dbt_core_after_build as recipe

    request = _request("build-17")
    invocation = SimpleNamespace(
        invocation_id="build-17",
        recipe_json="saved-recipe",
        request_key="destination-key",
        destination=object(),
    )
    statuses = iter(
        (
            SimpleNamespace(run_id="runtime-run", durable_state="running", pinned_result_ref=None),
            SimpleNamespace(
                run_id="runtime-run", durable_state="cancel_requested", pinned_result_ref=None
            ),
            SimpleNamespace(
                run_id="runtime-run", durable_state="succeeded", pinned_result_ref="generation-2"
            ),
        )
    )

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, received):
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            return SimpleNamespace(run_id="runtime-run", state="running", published_generation=None)

        def get_run(self, run_id):
            return next(statuses)

    sleeps = []
    monkeypatch.setattr(recipe, "ControlClient", Client)
    monkeypatch.setattr(recipe, "time", SimpleNamespace(sleep=sleeps.append), raising=False)
    receipt = recipe.run_after_dbt_build(
        build_id="build-17",
        plan_request_provider=lambda build_id: request,
        endpoint=ControlEndpoint(Path("owner.sock")),
        namespace="owner",
    )

    assert receipt.run_id == "runtime-run"
    assert receipt.state == "succeeded"
    assert receipt.published_generation == "generation-2"
    assert sleeps == [1, 1]


def test_dbt_after_build_treats_cancelled_runtime_status_as_terminal(monkeypatch):
    import examples.orchestrators.dbt_core_after_build as recipe

    request = _request("build-17")
    invocation = SimpleNamespace(
        invocation_id="build-17",
        recipe_json="saved-recipe",
        request_key="destination-key",
        destination=object(),
    )

    class Client:
        def __init__(self, endpoint, *, namespace):
            pass

        def plan(self, received):
            return invocation

        def run(self, serialized_plan, *, request_key, destination):
            return SimpleNamespace(run_id="runtime-run", published_generation=None)

        def get_run(self, run_id):
            return SimpleNamespace(run_id=run_id, durable_state="cancelled")

    monkeypatch.setattr(recipe, "ControlClient", Client)

    with pytest.raises(RuntimeError):
        recipe.run_after_dbt_build(
            build_id="build-17",
            plan_request_provider=lambda build_id: request,
            endpoint=ControlEndpoint(Path("owner.sock")),
            namespace="owner",
        )
