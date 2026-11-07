"""Every saved-artifact generation decodes and verifies its recorded identities."""

import base64
import json
from datetime import datetime
from pathlib import Path

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from terrine.data import BoundCoverage, MaterializedFrame, UnitSource
from terrine.dataset.manifest import DatasetManifest
from terrine.model import Model
from terrine.runtime.snapshot import ReopenableSnapshot
from terrine.semantic.io import catalog_payload, load_catalog_text
from terrine.spec import DatasetDefinition, from_json, from_yaml
from terrine.spec.io import canonical_digest

ARTIFACTS = Path(__file__).parent / "artifacts"
GENERATIONS = sorted(path.name for path in ARTIFACTS.iterdir() if path.is_dir())


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
    assert [(field.name, str(field.type)) for field in schema] == [
        (column.name, column.data_type) for column in manifest.columns
    ]
    roles = MaterializedFrame.from_arrow(schema.empty_table()).roles
    assert {
        column.name: column.role
        for column in manifest.columns
        if column.role in ("feature", "outcome")
    } == dict.fromkeys(roles.features, "feature") | dict.fromkeys(roles.outcomes, "outcome")


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
