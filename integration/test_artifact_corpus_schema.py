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
CORPUS = ARTIFACTS / "4"

DESIGNS = sorted(CORPUS.rglob("design.json"))


@pytest.mark.parametrize(
    "design_path", DESIGNS, ids=lambda path: str(path.parent.relative_to(ARTIFACTS))
)
def test_generation_designs_compile_to_recorded_schema(design_path):
    directory = design_path.parent

    def read(name: str) -> str:
        return (directory / name).read_text(encoding="utf-8")

    definitions = {
        "design.json": DatasetDefinition(base_design=from_json(read("design.json"))),
        "design.yaml": DatasetDefinition(base_design=from_yaml(read("design.yaml"))),
        "definition.json": DatasetDefinition.from_json(read("definition.json")),
    }
    recipe = directory != CORPUS
    if recipe:
        canonical = definitions["definition.json"].canonical_json()
        for name in ("design.json", "design.yaml"):
            assert definitions[name].canonical_json() == canonical, (
                f"{name} differs from definition.json"
            )

    with _reproducible_staging():
        con = ibis.duckdb.connect()
        try:
            model = bind_model(con, CORPUS / "data")
            if recipe:
                schema = output_schema(definitions["definition.json"].build(model))
                schemas = dict.fromkeys(definitions, schema)
            else:
                schemas = {
                    name: output_schema(definition.build(model))
                    for name, definition in definitions.items()
                }
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
    assert manifest["frame_metadata"]["provenance"]["execution_revision"] == 3

    def contents(directory: Path) -> dict[Path, bytes]:
        return {
            path.relative_to(directory): path.read_bytes()
            for path in directory.rglob("*")
            if path.is_file()
        }

    expected = contents(baseline)
    historical = {name: contents(ARTIFACTS / name) for name in ("1", "2", "3")}
    write_generation(regenerated)

    actual = contents(regenerated)
    assert actual == expected, [path for path in expected if actual.get(path) != expected[path]]
    assert contents(baseline) == expected
    assert {name: contents(ARTIFACTS / name) for name in historical} == historical
