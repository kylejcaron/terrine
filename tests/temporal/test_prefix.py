from __future__ import annotations

import ibis
import pytest

from terrine.backends.dialect import dialect_scope
from terrine.errors import TerrineError
from terrine.temporal.prefix import _require_backend


def test_exact_prefix_refusal_names_minimum_version(monkeypatch):
    from terrine.backends.engines import duckdb as duckdb_engine

    table = ibis.table({"value": "int64"})

    class Backend:
        name = "duckdb"

    monkeypatch.setattr(ibis.Table, "_find_backend", lambda *args, **kwargs: Backend())
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.4.0")

    with dialect_scope("duckdb"), pytest.raises(TerrineError) as caught:
        _require_backend(table)

    assert caught.value.context["minimum"] == "1.5"
