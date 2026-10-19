import json
from dataclasses import replace
from pathlib import Path

import ibis
import pytest

from terrine.agent import AgentPolicy
from terrine.agent.tools import DatasetTools
from terrine.data import Coverage, UnitSource
from terrine.model import Model
from terrine.runtime.recipe import ModelReference
from terrine.spec.io import from_json


@pytest.fixture
def tools(tmp_path):
    con = ibis.duckdb.connect()
    con.raw_sql("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    con.raw_sql("CREATE TABLE users (id INTEGER)")
    model = Model.from_catalog(
        Path(__file__).parent / "fixtures/catalog.yaml",
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    reference = ModelReference.from_model(model, owner_key="owner")
    policy = AgentPolicy(
        allowed_sources=frozenset({"events"}),
        allowed_catalogs=frozenset({reference.catalog_hash}),
        model_reference=reference,
        design_root=tmp_path / "designs",
    )
    try:
        yield DatasetTools(model, policy, namespace="agent")
    finally:
        con.disconnect()


def test_inspect_catalog_returns_only_allowed_semantic_metadata(tools):
    result = tools.inspect_catalog()
    assert result.status == "ok"
    assert result.output.sources[0].name == "events"
    assert result.output.metrics[0].name == "total"
    text = result.model_dump_json()
    assert "SELECT" not in text and "connection" not in text and "relation" not in text
    denied = DatasetTools(
        tools.model, replace(tools.policy, allowed_sources=frozenset()), namespace="agent"
    )
    denied_output = denied.inspect_catalog().output
    assert denied_output is not None
    assert not denied_output.sources
    assert not denied_output.metrics
    assert "cross_section" in tools.list_capabilities().output.runtime_shapes


def test_propose_design_from_discovered_schema(tools):
    schema = tools.get_design_schema().output
    assert schema.schema_version == 1
    assert "MetricAgg" in schema.document["$defs"]
    example = tools.design_example().output
    document = json.loads(example.serialized)
    metric = tools.inspect_catalog().output.metrics[0].name
    document["design"]["features"]["history"] = {
        "type": "MetricAgg",
        "metric": metric,
        "window": {"type": "Window", "start": -86400, "end": 0},
    }
    proposed = tools.propose_design(json.dumps(document), base=example.serialized)
    assert proposed.status == "ok"
    assert proposed.output.changed_paths == ("/design/features/history",)
    assert proposed.output.spec_hash == from_json(proposed.output.serialized).spec_hash
    assert (
        tools.propose_design(proposed.output.serialized).output.serialized
        == proposed.output.serialized
    )


def test_validation_uses_existing_metadata_contracts(tools, monkeypatch):
    def no_rows(*args, **kwargs):
        raise AssertionError("metadata tools read source rows")

    monkeypatch.setattr(type(tools.model.con), "to_pyarrow", no_rows)
    example = tools.design_example().output.serialized
    assert tools.validate_design(example).status == "ok"
    payload = json.loads(example)
    payload["design"]["features"] = {
        "bad": {
            "type": "MetricAgg",
            "metric": "unknown",
            "window": {"type": "Window", "start": -10, "end": 0},
        }
    }
    missing = tools.validate_design(json.dumps(payload))
    assert missing.code == "terrine.design.missing_reference" and missing.suggested_route
    payload["design"]["features"]["bad"]["window"]["start"] = 10
    assert tools.validate_design(json.dumps(payload)).status == "refused"
    payload["design"]["sql"] = "SELECT secret"
    assert tools.propose_design(json.dumps(payload)).code == "terrine.agent.invalid_request"


def test_design_save_load_is_confined_to_owner_root(tools, tmp_path):
    serialized = tools.design_example().output.serialized
    saved = tools.save_design("baseline", serialized)
    assert saved.status == "ok"
    assert tools.load_design("baseline").output == saved.output
    for name in ("../escape", "/tmp/escape", "nested/name", ".", ""):
        assert tools.save_design(name, serialized).code == "terrine.agent.storage_denied"
    outside = tmp_path / "outside.json"
    outside.write_text("unchanged")
    (tools.policy.design_root / "link.json").symlink_to(outside)
    assert tools.load_design("link").status == "refused"
    assert tools.save_design("link", serialized).status == "refused"
    assert outside.read_text() == "unchanged"
    with pytest.raises(TypeError):
        tools.save_design("baseline", serialized, root=tmp_path)


@pytest.mark.parametrize("roster", [False, True])
def test_design_example_handles_event_only_and_multiple_entity_models(tools, roster):
    source = tools.model.catalog.sources["events"]
    source = source.model_copy(update={"entities": {"account": "user_id", "user": "user_id"}})
    catalog = tools.model.catalog.model_copy(update={"sources": {"events": source}})
    model = Model.from_catalog(
        catalog,
        con=tools.model.con,
        readiness={"events": Coverage("2025-01-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")} if roster else {},
    )
    reference = ModelReference.from_model(model, owner_key="owner")
    policy = replace(
        tools.policy,
        model_reference=reference,
        allowed_catalogs=frozenset({reference.catalog_hash}),
    )
    result = DatasetTools(model, policy, namespace="agent").design_example()
    assert result.status == "ok"
    assert result.output is not None
    design = from_json(result.output.serialized)
    assert design.unit == ("user" if roster else "account")
