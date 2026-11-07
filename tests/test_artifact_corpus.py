"""Every saved-artifact generation decodes and verifies its recorded identities."""

import base64
import json
from datetime import datetime
from pathlib import Path

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.write_artifact_corpus import _is_internal_support
from terrine.data import BoundCoverage, MaterializedFrame, UnitSource
from terrine.dataset.manifest import DatasetManifest
from terrine.model import Model
from terrine.runtime.snapshot import ReopenableSnapshot
from terrine.semantic.io import catalog_payload, load_catalog_text
from terrine.spec import DatasetDefinition, from_json, from_yaml
from terrine.spec.io import canonical_digest

ARTIFACTS = Path(__file__).parent / "artifacts"
GENERATIONS = sorted(path.name for path in ARTIFACTS.iterdir() if path.is_dir())

RECIPE_DESIGNS = sorted(
    path
    for path in ARTIFACTS.glob("1/**/design.json")
    if path.parent != ARTIFACTS / "1"
)


def test_corpus_covers_every_recipe():
    from terrine import recipes

    assert {path.parent.name for path in RECIPE_DESIGNS} == set(recipes.__all__)


@pytest.mark.parametrize("design_path", RECIPE_DESIGNS, ids=lambda path: path.parent.name)
def test_recipe_documents_decode(design_path):
    from_json(design_path.read_text(encoding="utf-8"))
    from_yaml(design_path.with_name("design.yaml").read_text(encoding="utf-8"))
    DatasetDefinition.from_json(
        design_path.with_name("definition.json").read_text(encoding="utf-8")
    )






@pytest.mark.parametrize("generation", GENERATIONS)
def test_generation_documents_decode_and_verify(generation):
    directory = ARTIFACTS / generation

    def read(name: str) -> str:
        return (directory / name).read_text(encoding="utf-8")

    from_json(read("design.json"))
    from_yaml(read("design.yaml"))
    DatasetDefinition.from_json(read("definition.json"))
    manifest = DatasetManifest.from_json(read("manifest.json"))
    assert manifest.definition_hash == canonical_digest(json.loads(read("definition.json")))

    receipt = ReopenableSnapshot.from_json(read("receipt.json"))
    snapshots = manifest.source_snapshots
    assert (snapshots["snapshot_id"], snapshots["receipt_hash"]) == (
        receipt.snapshot_id,
        receipt.receipt_hash,
    )
    assert catalog_payload(load_catalog_text(read("catalog.yaml"))) == json.loads(
        receipt.catalog_json
    )

    metadata = json.loads(read("arrow_metadata.json"))
    schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(metadata["ARROW:schema"])))
    roles_by_name = {column.name: column.role for column in manifest.columns}
    assert [
        (field.name, str(field.type))
        for field in schema
        if roles_by_name[field.name] != "support" or not _is_internal_support(field.name)
    ] == [
        (column.name, column.data_type)
        for column in manifest.columns
        if column.role != "support" or not _is_internal_support(column.name)
    ]
    roles = MaterializedFrame.from_arrow(schema.empty_table()).roles
    assert {
        column.name: column.role
        for column in manifest.columns
        if column.role in ("feature", "outcome")
    } == dict.fromkeys(roles.features, "feature") | dict.fromkeys(roles.outcomes, "outcome")
    if generation == "1":
        expected_names = {
            column["name"]
            for column in json.loads(read("expected_schema.json"))["definition.json"]
        }
        assert {
            "orders_30d__complete",
            "orders_30d__reason",
            "__terrine_split",
            "__terrine_split_reason",
        } <= expected_names


@pytest.mark.parametrize("generation", GENERATIONS)
def test_generation_catalog_binds_to_recorded_model_hash(generation):
    """A Catalog document keeps the model hash it had when its generation was staged."""
    directory = ARTIFACTS / generation
    receipt = ReopenableSnapshot.from_json((directory / "receipt.json").read_text(encoding="utf-8"))
    coverage = DatasetManifest.from_json(
        (directory / "manifest.json").read_text(encoding="utf-8")
    ).source_coverage
    con = ibis.duckdb.connect()
    try:
        for path in sorted((directory / "data").glob("*.parquet")):
            con.create_table(path.stem, pq.read_table(path))
        model = Model.from_catalog(
            directory / "catalog.yaml",
            con=con,
            units={
                name: UnitSource(**unit) for name, unit in json.loads(receipt.units_json).items()
            },
            readiness={
                name: BoundCoverage(datetime.fromisoformat(start), datetime.fromisoformat(end))
                for name, (start, end) in coverage.items()
            },
            event_ids=json.loads(receipt.event_ids_json),
        )
    finally:
        con.disconnect()
    assert model.model_hash == receipt.model_hash
