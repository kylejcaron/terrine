import tomllib
from pathlib import Path


def test_polars_optional_extra_remains_available():
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]

    assert "polars" in project["optional-dependencies"]
    assert any(
        requirement.startswith("polars")
        for requirement in project["optional-dependencies"]["polars"]
    )
