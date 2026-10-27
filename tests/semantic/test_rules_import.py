import subprocess
import sys


def test_rules_import_without_query_or_temporal_packages():
    code = """
import importlib.abc
import sys
from datetime import UTC, datetime
class NoExecutionImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ibis' or fullname.startswith(
            ('ibis.', 'terrine.temporal', 'terrine.compile')
        ):
            raise ImportError('execution dependency imported: ' + fullname)
sys.meta_path.insert(0, NoExecutionImports())
from terrine.semantic.models import Catalog
from terrine.semantic.rules import calendar_average_alignment, supports_event_resolution
from terrine.windows import Window
catalog = Catalog.model_validate(dict(
    schema_version=1, day_boundary='UTC-05:00', dialect='duckdb',
    sources={}, facts={}, properties={}, dimensions={}, measures={}, metrics={},
))
assert calendar_average_alignment(
    Window('-1d', '0s'), anchor=datetime(2025, 3, 1, 5, tzinfo=UTC),
    day_boundary=catalog.day_boundary,
) == 'aligned'
for name in ('DUCKDB', 'POSTGRES', 'BIGQUERY', 'SNOWFLAKE'):
    assert supports_event_resolution(name)
assert supports_event_resolution('generic')
assert not supports_event_resolution('unregistered')
"""
    subprocess.run([sys.executable, "-c", code], check=True)
