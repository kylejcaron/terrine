"""Native semantics do not depend on an optional definition format."""

import json

import pytest

from terrine.errors import TerrineError
from terrine.semantic.io import load_catalog


@pytest.mark.parametrize(
    ("foreign_key", "definition"),
    [
        ("semantic_models", {"name": "events", "model": "ref('events')"}),
        ("views", {"name": "events", "sql_table_name": "events"}),
    ],
    ids=["metricflow", "lookml_shaped_yaml"],
)
def test_native_catalog_does_not_guess_foreign_semantic_mappings(tmp_path, foreign_key, definition):
    path = tmp_path / "foreign.yaml"
    path.write_text(json.dumps({foreign_key: [definition]}), encoding="utf-8")
    with pytest.raises(TerrineError) as error:
        load_catalog(path)
    assert error.value.code == "terrine.catalog.invalid_definition"
    assert any(
        issue["location"] == (foreign_key,) and issue["kind"] == "extra_forbidden"
        for issue in error.value.context["issues"]
    )
