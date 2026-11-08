from __future__ import annotations

import pytest

from terrine.backends.engines import duckdb as duckdb_engine
from terrine.backends.engines.duckdb import DuckDBDialect


@pytest.mark.parametrize("build", ["1.5.0", "1.5.6", "1.6.3", "2.0.0"])
def test_exact_prefix_supported_on_any_duckdb_at_or_above_floor(monkeypatch, build):
    monkeypatch.setattr(duckdb_engine, "version", lambda _: build)

    assert DuckDBDialect("duckdb").supports_exact_prefix("duckdb")


def test_exact_prefix_rejects_other_backends(monkeypatch):
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.6.3")

    assert not DuckDBDialect("duckdb").supports_exact_prefix("postgres")


