"""Exercise restricted dataset tools on a local owner-bound DuckDB Model."""

import json
from dataclasses import replace
from pathlib import Path

import ibis
import pytest
import yaml

from terrine.agent import AgentPolicy, DatasetTools
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.runtime.saved_plan import ModelReference

_ROOT = Path(__file__).parents[2]
_CASES = yaml.safe_load((_ROOT / "evals/agent_dataset_tools.yaml").read_text())["cases"]


@pytest.mark.parametrize("case", _CASES, ids=[case["name"] for case in _CASES])
def test_restricted_metadata_evaluations(case, tmp_path):
    con = ibis.duckdb.connect(database=str(tmp_path / "owner.duckdb"))
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.raw_sql("CREATE TABLE users (id INTEGER)")
        model = Model.from_catalog(
            _ROOT / "tests/agent/fixtures/catalog.yaml",
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-02-01")},
            units={"user": UnitSource("users", "id")},
        )
        reference = ModelReference.from_model(model, owner_key="owner")
        policy = AgentPolicy(
            model_reference=reference,
            allowed_catalogs=frozenset({reference.catalog_hash}),
            allowed_sources=frozenset({"events"}),
            design_root=tmp_path / "designs",
        )
        tools = DatasetTools(model, policy, namespace="agent")
        example = tools.design_example().output
        assert example is not None
        payload = json.loads(example.serialized)
        payload["design"]["features"] = {
            "history": {
                "type": "MetricAgg",
                "metric": "total",
                "window": {"type": "Window", "start": -10, "end": 0},
            }
        }
        payload["design"].update(case.get("edit", {}))
        if case.get("deny_sources"):
            tools = DatasetTools(
                model, replace(policy, allowed_sources=frozenset()), namespace="agent"
            )
        if case.get("exhaust_requests"):
            tools = DatasetTools(model, replace(policy, max_requests=1), namespace="agent")
            assert tools.inspect_catalog().status == "ok"
            for _ in range(2):
                assert tools.inspect_catalog().code == case["code"]
        operation = case["operation"]
        if operation == "validate_design":
            result = tools.validate_design(json.dumps(payload))
        elif operation == "propose_design":
            result = tools.propose_design(json.dumps(payload))
        elif operation == "load_design":
            result = tools.load_design(case["name_argument"])
        else:
            assert operation == "inspect_catalog"
            result = tools.inspect_catalog()
        assert result.status == "refused"
        assert result.code == case["code"] and result.suggested_route
        assert "secret" not in result.model_dump_json()
        assert (
            con.con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='agent'"
            ).fetchone()[0]
            == 0
        )
    finally:
        con.disconnect()
