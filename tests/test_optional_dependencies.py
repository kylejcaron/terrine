import subprocess
import sys
import tomllib
from pathlib import Path


def test_polars_optional_extra_remains_available():
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]

    assert "polars" in project["optional-dependencies"]
    assert any(
        requirement.startswith("polars")
        for requirement in project["optional-dependencies"]["polars"]
    )


def test_backend_metadata_imports_without_optional_drivers_or_dataframes():
    code = """
import builtins
original = builtins.__import__
blocked = ('duckdb', 'ibis.backends.duckdb', 'snowflake', 'google.cloud',
           'psycopg', 'psycopg2', 'pandas', 'polars', 'dbt', 'increment')
def guarded(name, *args, **kwargs):
    if any(name == item or name.startswith(item + '.') for item in blocked):
        raise ImportError('optional integration intentionally unavailable')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
import terrine
from terrine.execution.backend import backend_capabilities
from terrine.runtime.backends.base import BackendCapabilities
class Unverified:
    name = 'snowflake'
assert backend_capabilities(Unverified()).dialect_admission is None
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)
