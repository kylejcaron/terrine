"""Every stored Design compiles with the current code to its recorded schema and roles."""

import json
from pathlib import Path

import ibis
import pytest

from scripts.write_artifact_corpus import (
    _reproducible_staging,
    bind_model,
    output_schema,
)
from terrine.spec import DatasetDefinition, from_json, from_yaml

ARTIFACTS = Path(__file__).parents[1] / "tests" / "artifacts"

DESIGNS = sorted(ARTIFACTS.glob("1/**/design.json"))


@pytest.mark.parametrize(
    "design_path", DESIGNS, ids=lambda path: str(path.parent.relative_to(ARTIFACTS))
)
def test_generation_designs_compile_to_recorded_schema(design_path):
    directory = design_path.parent

    def read(name: str) -> str:
        return (directory / name).read_text(encoding="utf-8")

    with _reproducible_staging():
        con = ibis.duckdb.connect()
        try:
            model = bind_model(con, ARTIFACTS / "1" / "data")
            frames = {
                "design.json": from_json(read("design.json")).build(model),
                "design.yaml": from_yaml(read("design.yaml")).build(model),
                "definition.json": DatasetDefinition.from_json(read("definition.json")).build(
                    model
                ),
            }
            schemas = {name: output_schema(frame) for name, frame in frames.items()}
        finally:
            con.disconnect()
    assert schemas == json.loads(read("expected_schema.json"))
