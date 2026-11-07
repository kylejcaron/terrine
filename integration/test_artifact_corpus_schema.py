"""Every stored Design compiles with the current code to its recorded schema and roles."""

import json
import shutil
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
            schemas = {
                name: [column for column in output_schema(frame) if column["role"] != "support"]
                for name, frame in frames.items()
            }
        finally:
            con.disconnect()
    expected_schema = {
        name: [column for column in columns if column["role"] != "support"]
        for name, columns in json.loads(read("expected_schema.json")).items()
    }
    assert schemas == expected_schema
    if directory.name == "ltv":
        assert "age" in {column["name"] for column in schemas["design.json"]}


def test_corpus_generator_is_deterministic_without_mutating_baseline(tmp_path):
    baseline = ARTIFACTS / "1"
    regenerated = tmp_path / "generation"
    shutil.copytree(baseline, regenerated)

    def contents(directory: Path) -> dict[Path, bytes]:
        return {
            path.relative_to(directory): path.read_bytes()
            for path in directory.rglob("*")
            if path.is_file()
        }

    expected = contents(baseline)
    write_generation(regenerated)

    actual = contents(regenerated)
    assert actual == expected, [path for path in expected if actual.get(path) != expected[path]]
    assert contents(baseline) == expected
