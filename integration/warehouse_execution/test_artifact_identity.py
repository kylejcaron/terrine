"""Stored publications, definitions and saved plans keep their recorded identities."""

import json
from dataclasses import replace
from pathlib import Path

import ibis
import pytest

from terrine.agent import AgentPolicy, DatasetTools
from terrine.data import Coverage, UnitSource
from terrine.dataset.manifest import DatasetManifest
from terrine.dataset.reference import Dataset
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.saved_plan import DestinationRef, ModelReference, SavedPlan
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.spec import Design, FixedDate, agg
from terrine.spec import io as design_io
from terrine.spec.dataset import DatasetDefinition, DatasetDefinitionStore

_CATALOG = Path(__file__).parents[2] / "tests/agent/fixtures/catalog.yaml"


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    database = tmp_path_factory.mktemp("identity") / "owner.duckdb"
    con = ibis.duckdb.connect(str(database))
    con.raw_sql("CREATE TABLE events(ts TIMESTAMP,user_id INTEGER,value INTEGER)")
    con.raw_sql("CREATE TABLE users(id INTEGER)")
    con.raw_sql("INSERT INTO users VALUES (1),(2)")
    con.raw_sql("INSERT INTO events VALUES ('2025-01-01 12:00:00',1,3),('2025-01-02',2,4)")
    model = Model.from_catalog(
        _CATALOG,
        con=con,
        readiness={"events": Coverage("2024-12-01", "2025-02-01")},
        units={"user": UnitSource("users", "id")},
    )
    owner = RuntimeOwner.from_connection(con)
    model_reference = ModelReference.from_model(model, owner_key=owner.reference)

    class Resolver:
        def resolve(self, reference):
            assert reference == model_reference
            return model

    destination = DestinationRef(str(database), "agent", "study")
    tools = DatasetTools(
        model,
        AgentPolicy(
            prepare=True,
            write=True,
            read=True,
            preview=True,
            preview_columns=frozenset({"history"}),
            allowed_sources=frozenset({"events"}),
            allowed_catalogs=frozenset({model_reference.catalog_hash}),
            allowed_destinations=frozenset({destination}),
            model_reference=model_reference,
            resolver=Resolver(),
            snapshot_authority=SnapshotAuthority(owner),
        ),
        namespace="agent",
    )
    definition = DatasetDefinition(
        base_design=Design(
            "user",
            FixedDate("2025-01-02"),
            features={"history": agg("total", window=("-1d", "0s"))},
        )
    )
    receipt = tools.prepare_dataset(definition, destination=destination, invocation_id="identity")
    assert receipt.output is not None, receipt.code
    staged = tools.stage(receipt.output, request_key="identity")
    assert staged.output is not None, staged.code
    yield tools, staged.output
    con.disconnect()


def test_published_dataset_and_agent_reference_survive_encoder_change(
    published, tmp_path, monkeypatch
):
    tools, staged = published
    con = tools.model.con
    recipe_json, manifest_json = con.con.execute(
        "SELECT recipe_json,manifest_json FROM agent.runtime_dataset_recipes "
        "WHERE dataset=? AND generation=?",
        [staged.dataset, staged.generation],
    ).fetchone()
    recorded = json.loads(manifest_json)["definition_hash"]
    opened = Dataset.open_generation(staged.dataset, staged.generation, con=con, namespace="agent")
    store = DatasetDefinitionStore(tmp_path / "definitions", owner="owner")
    envelope = replace(
        opened.manifest, definition_reference=store.put(opened.manifest.definition)
    ).to_json()

    encode = design_io._encode

    def encode_with_future_field(value):
        node = encode(value)
        return {**node, "future": None} if isinstance(value, Design) else node

    monkeypatch.setattr(design_io, "_encode", encode_with_future_field)

    reopened = Dataset.open_generation(
        staged.dataset, staged.generation, con=con, namespace="agent"
    )
    assert reopened.manifest.definition.digest != recorded
    assert reopened.manifest.definition_hash == recorded == staged.definition_reference
    by_reference = Dataset.from_manifest(
        DatasetManifest.from_json(envelope, definition_store=store), con=con, namespace="agent"
    )
    assert by_reference.manifest.definition_hash == recorded
    assert SavedPlan.from_json(recipe_json).design_hash == json.loads(recipe_json)["design_hash"]
    preview = tools.preview_rows(staged, limit=2, columns=("history",))
    assert preview.status == "ok", preview.code


def test_edited_manifest_cannot_authenticate_as_publication(published):
    tools, staged = published
    con = tools.model.con
    manifest = Dataset.open_generation(
        staged.dataset, staged.generation, con=con, namespace="agent"
    ).manifest
    edited_design = replace(manifest.definition.base_design, gap="1d")
    for edited in (
        replace(manifest, columns=manifest.columns[:-1]),
        replace(manifest, definition=DatasetDefinition(base_design=edited_design)),
    ):
        with pytest.raises(TerrineError) as error:
            Dataset.from_manifest(edited, con=con, namespace="agent")
        assert error.value.code == "terrine.dataset.invalid_generation"
