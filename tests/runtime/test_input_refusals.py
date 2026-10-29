"""Owner runtime inputs refuse with stable codes before any runtime state is written."""

import json
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from terrine.adapters.opentelemetry import OpenTelemetryAdapter
from terrine.errors import TerrineError
from terrine.runtime.api import find_run_by_request_key
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.control import ControlEndpoint, ControlServer
from terrine.runtime.graph import dependency_graph
from terrine.runtime.leases import LeaseStore
from terrine.runtime.planner import RefreshPlan, plan_refresh
from terrine.runtime.planning import PlanInvocation, PlanRequest
from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.runtime.state import RuntimeStore
from terrine.spec.anchors import FixedDate
from terrine.spec.design import Design

_DESTINATION = DestinationRef("warehouse.duckdb", "published", "study")
_MODEL = ModelReference("owner", "catalog", "model", "units", "readiness", "events")
_BAD_DESTINATION = {"database": "warehouse.duckdb", "schema": "bad-name", "dataset": "study"}


def _serialized_plan(**changes: Any) -> str:
    graph = dependency_graph(sources=("events",), definition_hash="def", partitions=("p1",))
    plan = plan_refresh(
        namespace="owner", dataset="orders", definition_hash="def", snapshot_id="s", graph=graph
    )
    return json.dumps({**json.loads(plan.serialize()), **changes})


def _request(**changes: Any) -> dict[str, Any]:
    request = PlanRequest("run-1", _MODEL, Design("user", FixedDate("2025-01-15")), _DESTINATION)
    return {**request.to_dict(), **changes}


CASES = [
    (
        "terrine.dataset.invalid_destination",
        lambda: LeaseStore(duckdb.connect(), namespace="bad-name"),
        {"reason": "invalid_namespace", "field": "namespace"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: RuntimeStore(duckdb.connect(), namespace=cast(Any, 5)),
        {"reason": "invalid_namespace", "field": "namespace"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: DuckDBBackend(duckdb.connect(), namespace="two words"),
        {"reason": "invalid_namespace", "field": "namespace"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: DestinationRef("warehouse.duckdb", "bad-schema", "study"),
        {"reason": "invalid_identifier", "field": "schema"},
    ),
    (
        "terrine.runtime.plan_request_invalid",
        lambda: find_run_by_request_key(
            "", destination=_DESTINATION, con=duckdb.connect(), namespace="owner"
        ),
        {"reason": "invalid_request_key", "field": "request_key"},
    ),
    (
        "terrine.runtime.plan_request_invalid",
        lambda: find_run_by_request_key(
            "key", destination=cast(Any, None), con=duckdb.connect(), namespace="owner"
        ),
        {"reason": "wrong_type", "field": "destination"},
    ),
    (
        "terrine.runtime.plan_request_invalid",
        lambda: PlanRequest.from_dict(_request(schema_version=2)),
        {"reason": "unsupported_request_version"},
    ),
    (
        "terrine.runtime.saved_plan_mismatch",
        lambda: RefreshPlan.deserialize("[]"),
        {"reason": "malformed_plan"},
    ),
    (
        "terrine.runtime.saved_plan_mismatch",
        lambda: RefreshPlan.deserialize(_serialized_plan(schema_version=1)),
        {"reason": "unsupported_plan_version"},
    ),
    (
        "terrine.runtime.saved_plan_mismatch",
        lambda: RefreshPlan.deserialize(_serialized_plan(fingerprint="0" * 64)),
        {"reason": "fingerprint_mismatch"},
    ),
    (
        "terrine.runtime.saved_plan_mismatch",
        lambda: SavedPlan.create(
            plan_json="{}",
            design=Design("user", FixedDate("2025-01-15")),
            model=cast(Any, None),
            destination=_DESTINATION,
            namespace="owner",
            expected_base_manifest=None,
        ),
        {"reason": "wrong_type", "field": "model"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: SavedPlan.create(
            plan_json="{}",
            design=Design("user", FixedDate("2025-01-15")),
            model=_MODEL,
            destination=_DESTINATION,
            namespace="",
            expected_base_manifest=None,
        ),
        {"reason": "empty_name", "field": "namespace"},
    ),
    (
        "terrine.runtime.invalid_configuration",
        lambda: ControlServer(
            RuntimeStore(duckdb.connect(), namespace="owner"),
            namespace="other",
            endpoint=ControlEndpoint(Path("unused.sock")),
        ),
        {"reason": "namespace_mismatch", "field": "namespace"},
    ),
    (
        "terrine.runtime.invalid_configuration",
        lambda: SnapshotAuthority(cast(Any, None)),
        {"reason": "wrong_type", "field": "owner"},
    ),
    (
        "terrine.runtime.invalid_configuration",
        lambda: OpenTelemetryAdapter(None, None, queue_size=0),
        {"reason": "not_positive_integer", "field": "queue_size"},
    ),
    # decoders keep a coded refusal raised while rebuilding their records
    (
        "terrine.dataset.invalid_destination",
        lambda: PlanRequest.from_dict(_request(destination=_BAD_DESTINATION)),
        {"reason": "invalid_identifier", "field": "schema"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: PlanInvocation.from_dict(
            {
                "invocation_id": "run-1",
                "serialized_plan": "{}",
                "request_key": "key",
                "destination": _BAD_DESTINATION,
                "snapshot_id": "s",
                "expected_base_manifest": None,
                "schema_version": 1,
            }
        ),
        {"reason": "invalid_identifier", "field": "schema"},
    ),
    (
        "terrine.dataset.invalid_destination",
        lambda: SavedPlan.from_json(
            json.dumps(
                {
                    "schema_version": 1,
                    "plan_json": "{}",
                    "design_json": "{}",
                    "design_hash": "0" * 64,
                    "model": _MODEL.to_dict(),
                    "destination": _BAD_DESTINATION,
                    "namespace": "owner",
                    "expected_base_manifest": None,
                    "plan_hash": "0" * 64,
                }
            )
        ),
        {"reason": "invalid_identifier", "field": "schema"},
    ),
    (
        "terrine.budget.invalid_limit",
        lambda: RefreshPlan.deserialize(_serialized_plan(budget={"max_rows": -1})),
        {"reason": "not_nonnegative_integer", "field": "max_rows"},
    ),
]


@pytest.mark.parametrize(("code", "build", "context"), CASES)
def test_runtime_inputs_refuse_with_codes(code, build, context):
    with pytest.raises(TerrineError) as error:
        build()
    assert error.value.code == code
    assert {key: error.value.context.get(key) for key in context} == context
