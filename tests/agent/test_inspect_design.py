import json
from dataclasses import replace

import pytest

from terrine.agent.tools import DatasetTools
from terrine.data import Coverage, UnitSource
from terrine.dataset.splits import GroupSplit
from terrine.model import Model
from terrine.runtime.saved_plan import ModelReference
from terrine.spec.io import from_json


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


def test_validate_design_returns_a_summary(tools):
    example = json.loads(tools.design_example().output.serialized)
    example["design"]["features"] = {
        "recent_total": {
            "type": "MetricAgg",
            "metric": "total",
            "window": {"type": "Window", "start": -86400, "end": 0},
        }
    }
    serialized = json.dumps(example)
    validated = tools.validate_design(serialized).output
    assert validated.spec_hash and "events" in validated.summary
    assert "recent_total" in validated.summary
    assert tools.propose_design(serialized).output.summary == validated.summary


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
    smuggled = tools.propose_design(json.dumps(payload))
    assert smuggled.code == "terrine.design.invalid"
    assert smuggled.suggested_route


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
    source = source.model_copy(update={"units": {"account": "user_id", "user": "user_id"}})
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
    agent = DatasetTools(model, policy, namespace="agent")
    result = agent.design_example()
    assert result.status == "ok"
    assert result.output is not None
    design = from_json(result.output.serialized)
    assert design.unit == ("user" if roster else "account")
    document = json.loads(result.output.serialized)
    document["design"]["unit"] = None
    proposed = agent.propose_design(json.dumps(document), base=result.output.serialized)
    if roster:
        assert proposed.status == "ok"
        assert proposed.output is not None
        assert from_json(proposed.output.serialized) == design
        assert proposed.output.spec_hash == design.spec_hash
        assert proposed.output.changed_paths == ()
    else:
        assert proposed.code == "terrine.design.invalid"
        assert agent.validate_design(json.dumps(document)).code == "terrine.design.invalid"


def test_propose_design_and_definition_from_discovered_schema(tools, monkeypatch):
    from jsonschema import Draft202012Validator

    from terrine.dataset.splits import TimeSplit
    from terrine.errors import thaw
    from terrine.spec.dataset import DatasetDefinition

    example = tools.design_example().output
    document = json.loads(example.serialized)["design"]
    metric = tools.inspect_catalog().output.metrics[0].name
    document["features"] = {
        "history": {
            "type": "MetricAgg",
            "metric": metric,
            "window": {"type": "Window", "start": -86400, "end": 0},
        }
    }
    document["outcomes"] = {
        "future": {
            "type": "MetricAgg",
            "metric": metric,
            "window": {"type": "Window", "start": 0, "end": 86400},
        }
    }
    definition = {
        "schema_version": 2,
        "base_design": document,
        "view": {
            "kind": "training",
            "features": ["history"],
            "labels": ["future"],
            "incomplete_labels": "keep",
            "split": TimeSplit(train_until="2025-01-15").to_record(),
        },
    }
    discovered = tools.get_dataset_definition_schema()
    assert discovered.output is not None
    Draft202012Validator(thaw(discovered.output.document)).validate(definition)
    result = tools.propose_definition(json.dumps(definition))
    assert result.status == "ok"
    assert (
        result.output.definition_digest
        == DatasetDefinition.from_json(result.output.serialized).digest
    )
    definition["view"]["split"] = TimeSplit(train_until="2025-01-16").to_record()
    changed = tools.propose_definition(json.dumps(definition), base=result.output.serialized)
    assert changed.output.changed_paths == ("/view/split/train_until",)
    assert tools.save_design("training", changed.output.serialized).status == "ok"
    assert tools.load_design("training").output.serialized == changed.output.serialized
    definition["view"]["split"]["callback"] = "builtins.eval"
    assert tools.propose_definition(json.dumps(definition)).status == "refused"
    del definition["view"]["split"]["callback"]
    document["outcomes"] = {}
    definition["view"] = {
        "kind": "scoring",
        "features": ["history"],
        "as_of": "2025-01-01T00:00:00+00:00",
        "eligibility": {},
        "parent_design_hash": "a" * 64,
    }
    Draft202012Validator(thaw(discovered.output.document)).validate(definition)
    assert tools.validate_definition(json.dumps(definition)).status == "ok"
    definition["view"]["as_of"] = "2025-01-01"
    assert tools.validate_definition(json.dumps(definition)).status == "refused"


def _scoring_record(tools):
    document = json.loads(tools.design_example().output.serialized)["design"]
    document["features"] = {
        "history": {
            "type": "MetricAgg",
            "metric": tools.inspect_catalog().output.metrics[0].name,
            "window": {"type": "Window", "start": -86400, "end": 0},
        }
    }
    return {
        "schema_version": 2,
        "base_design": document,
        "view": {
            "kind": "scoring",
            "features": ["history"],
            "as_of": "2025-01-01T00:00:00+00:00",
            "eligibility": {},
            "parent_design_hash": "a" * 64,
        },
    }


@pytest.mark.parametrize("mutation", ["instant", "precision", "features", "shape", "anchor"])
def test_scoring_definition_refuses_inconsistent_reconstruction(tools, mutation):
    definition = _scoring_record(tools)
    if mutation == "instant":
        definition["view"]["as_of"] = "2025-01-15T00:00:00+00:00"
    elif mutation == "precision":
        definition["view"]["as_of"] = "2025-01-01T00:00:00.000000001Z"
    elif mutation == "features":
        definition["view"]["features"] = []
    elif mutation == "shape":
        definition["base_design"]["shape"] = "panel"
    else:
        definition["base_design"]["anchor"] = {
            "type": "EventAnchor",
            "event": "event",
            "which": "first",
        }
    result = tools.propose_definition(json.dumps(definition))
    assert result.status == "refused"
    assert result.code == "terrine.dataset.invalid_definition"


def test_scoring_definition_canonicalizes_eligibility_defaults(tools):
    definition = _scoring_record(tools)
    eligibility = {
        "missing": {"type": "IsNull", "expr": definition["base_design"]["features"]["history"]}
    }
    definition["base_design"]["eligibility"] = eligibility
    definition["view"]["eligibility"] = eligibility
    assert tools.propose_definition(json.dumps(definition)).status == "ok"


@pytest.mark.parametrize(
    "split",
    [
        {
            "kind": "time",
            "train_until": "2025-01-15",
            "validation_until": None,
            "embargo_microseconds": 10**30,
            "groups": None,
        },
        {**GroupSplit().to_record(), "train_fraction": "1.5"},
    ],
    ids=["embargo_overflow", "train_fraction_out_of_range"],
)
def test_definition_split_refusal_keeps_its_split_code(tools, split):
    definition = _scoring_record(tools)
    definition["view"] = {
        "kind": "training",
        "features": ["history"],
        "labels": ["future"],
        "incomplete_labels": "keep",
        "split": split,
    }
    for operation in (
        tools.propose_definition,
        tools.validate_definition,
        lambda value: tools.save_design("split", value),
    ):
        result = operation(json.dumps(definition))
        assert result.status == "refused"
        assert result.code == "terrine.dataset.invalid_split"


def test_scoring_definition_malformed_eligibility_is_a_coded_refusal(tools):
    definition = _scoring_record(tools)
    definition["view"]["eligibility"] = {"bad": {"type": "Unknown"}}
    for operation in (
        tools.propose_definition,
        tools.validate_definition,
        lambda value: tools.save_design("malformed", value),
    ):
        result = operation(json.dumps(definition))
        assert result.status == "refused"
        assert result.code == "terrine.design.invalid"
