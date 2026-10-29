import subprocess
import sys

_NATIVE_CATALOG = """schema_version: 2
day_boundary: UTC
dialect: duckdb
sources:
  events:
    relation:
      kind: sql
      sql: >-
        SELECT 1 AS user_id, TIMESTAMP '2025-01-01' AS occurred_at,
        1 AS event
    time: occurred_at
    units: {user_id: user_id}
    events: [event]
    properties: []
    dimensions: []
events:
  event: {source: events, column: event}
properties: {}
dimensions: {}
measures:
  event_count: {event: event, aggregation: count}
metrics:
  events: {kind: conversion, unit: user_id, measure: event_count}
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
