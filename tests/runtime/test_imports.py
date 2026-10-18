import subprocess
import sys


def test_runtime_metadata_imports_do_not_require_duckdb_backend():
    code = """
import builtins
original_import = builtins.__import__
def reject_duckdb(name, *args, **kwargs):
    if name == 'duckdb' or name.startswith('ibis.backends.duckdb'):
        raise ImportError('DuckDB backend intentionally unavailable')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_duckdb
import terrine.runtime
from terrine.runtime import dependency_graph, resolve_plan
assert callable(dependency_graph) and callable(resolve_plan)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
