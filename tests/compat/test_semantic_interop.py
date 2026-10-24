"""Native semantics do not depend on an optional definition format."""

import json
import subprocess
import sys

import pytest

from terrine.errors import TerrineError
from terrine.semantic.io import load_catalog
from tests.compat.test_optional_increment import _BLOCK_INCREMENT, _NATIVE_CATALOG


def test_native_catalog_executes_a_frame_without_increment(tmp_path):
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(_NATIVE_CATALOG, encoding="utf-8")
    code = (
        _BLOCK_INCREMENT
        + """
import ibis
from terrine import Budget, Coverage, Design, EventAnchor, Model
con = ibis.duckdb.connect()
try:
    model = Model.from_catalog(
        sys.argv[1], con=con,
        readiness={'events': Coverage('2025-01-01', '2025-01-31')},
    )
    frame = Design('user_id', EventAnchor('event')).build(model)
    arrow = frame.to_pyarrow(budget=Budget(max_rows=1))
    assert arrow['unit'].to_pylist() == [1]
    assert not any(name == 'increment' or name.startswith('increment.') for name in sys.modules)
finally:
    con.disconnect()
"""
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(catalog_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


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
