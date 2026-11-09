"""Current-generation Designs compile to their recorded schema and roles."""

import json
from pathlib import Path

import ibis
import pytest

from scripts.write_artifact_corpus import (
    _reproducible_staging,
    bind_model,
    output_schema,
    write_generation,
)
from terrine.spec import DatasetDefinition, from_json, from_yaml

ARTIFACTS = Path(__file__).parents[1] / "tests" / "artifacts"
CORPUS = ARTIFACTS / "2"

DESIGNS = sorted(CORPUS.rglob("design.json"))


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
            model = bind_model(con, CORPUS / "data")
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
    expected_schema = json.loads(read("expected_schema.json"))
    assert schemas == expected_schema
    if directory.name == "ltv":
        assert "age" in {column["name"] for column in schemas["design.json"]}


def test_corpus_generator_is_deterministic_without_mutating_baseline(tmp_path):
    baseline = CORPUS
    regenerated = tmp_path / "generation"
    manifest = json.loads((baseline / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["frame_metadata"]["provenance"].get("execution_revision") == 2

    def contents(directory: Path) -> dict[Path, bytes]:
        return {
            path.relative_to(directory): path.read_bytes()
            for path in directory.rglob("*")
            if path.is_file()
        }

    expected = contents(baseline)
    historical = contents(ARTIFACTS / "1")
    write_generation(regenerated)

    actual = contents(regenerated)
    assert actual == expected, [path for path in expected if actual.get(path) != expected[path]]
    assert contents(baseline) == expected
    assert contents(ARTIFACTS / "1") == historical
