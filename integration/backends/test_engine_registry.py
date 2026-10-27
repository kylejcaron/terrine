import subprocess
import sys


def test_registry_import_is_metadata_only():
    code = """
import importlib.abc
import sys
blocked = ('ibis', 'duckdb', 'psycopg', 'psycopg2', 'snowflake', 'google.cloud',
           'terrine.compile', 'terrine.temporal', 'terrine.execution', 'terrine.runtime')
class MetadataOnly(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == item or fullname.startswith(item + '.') for item in blocked):
            raise ImportError('execution dependency imported: ' + fullname)
sys.meta_path.insert(0, MetadataOnly())
from terrine.backends.registry import ENGINES, engine_supports, get_engine
assert get_engine('POSTGRES') is ENGINES['postgres']
assert get_engine('missing') is None
for name in ENGINES:
    assert engine_supports(name.upper(), 'event_resolution')
assert not engine_supports('missing', 'event_resolution')
assert not any(name == item or name.startswith(item + '.')
               for name in sys.modules for item in blocked)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)
