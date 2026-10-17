import subprocess
import sys

import pytest

_NATIVE_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: duckdb
sources:
  events:
    relation:
      kind: sql
      sql: >-
        SELECT 1 AS user_id, TIMESTAMP '2025-01-01' AS occurred_at,
        1 AS event
    timestamp: occurred_at
    entities: {user_id: user_id}
    facts: [event]
    properties: []
    dimensions: []
facts:
  event: {source: events, column: event}
properties: {}
dimensions: {}
measures:
  event_count: {fact: event, aggregation: count}
metrics:
  events: {kind: conversion, entity: user_id, measure: event_count}
"""

_BLOCK_INCREMENT = """
import importlib.abc
import sys
class BlockIncrement(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'increment' or fullname.startswith('increment.'):
            raise ModuleNotFoundError('increment intentionally blocked', name=fullname)
sys.meta_path.insert(0, BlockIncrement())
"""


@pytest.mark.slow
def test_native_binding_and_missing_extra_are_isolated_from_increment(tmp_path):
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(_NATIVE_CATALOG, encoding="utf-8")
    missing_path = tmp_path / "does-not-exist.yaml"
    code = (
        _BLOCK_INCREMENT
        + """
import ibis
from datetime import datetime
from terrine.data import Coverage
from terrine.model import Model
from terrine.errors import TerrineError
from terrine.semantic.models import SqlQuery
try:
    Model.from_increment(sys.argv[2], con=object(), readiness={})
except TerrineError as error:
    assert error.code == 'terrine.increment.missing_extra'
    assert error.context['extra'] == 'increment'
else:
    raise AssertionError('expected missing optional dependency refusal')
model = Model.from_catalog(
    sys.argv[1], con=ibis.duckdb.connect(),
    readiness={'events': Coverage('2025-01-01', '2025-01-31')},
)
relation = model.catalog.sources['events'].relation
assert isinstance(relation, SqlQuery)
rows = model.con.sql(relation.sql).to_pyarrow().to_pylist()
assert rows == [{'user_id': 1, 'occurred_at': datetime(2025, 1, 1), 'event': 1}]
assert not any(name == 'increment' or name.startswith('increment.') for name in sys.modules)
model.con.disconnect()
"""
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(catalog_path), str(missing_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
