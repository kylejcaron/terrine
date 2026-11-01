"""Every stored Design compiles with the current code to its recorded schema and roles."""

import json
from pathlib import Path

import ibis
import pytest

from scripts.write_artifact_corpus import bind_model, output_schema
from terrine.spec import DatasetDefinition, from_json, from_yaml

ARTIFACTS = Path(__file__).parents[1] / "tests" / "artifacts"
GENERATIONS = sorted(path.name for path in ARTIFACTS.iterdir() if path.is_dir())


@pytest.mark.parametrize("generation", GENERATIONS)
def test_generation_designs_compile_to_recorded_schema(generation):
    directory = ARTIFACTS / generation

    def read(name: str) -> str:
        return (directory / name).read_text(encoding="utf-8")

    con = ibis.duckdb.connect()
    try:
        model = bind_model(con, directory / "data")
        frames = {
            "design.json": from_json(read("design.json")).build(model),
            "design.yaml": from_yaml(read("design.yaml")).build(model),
            "definition.json": DatasetDefinition.from_json(read("definition.json")).build(model),
        }
        schemas = {name: output_schema(frame) for name, frame in frames.items()}
    finally:
        con.disconnect()
    assert schemas == json.loads(read("expected_schema.json"))
