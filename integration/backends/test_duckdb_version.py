from __future__ import annotations

import ibis
import pytest

import terrine as tr
from terrine.backends.engines import duckdb as duckdb_engine
from terrine.execution import _native
from tests.fixtures.readme import readme_model, readme_tables


@pytest.fixture
def readme_backend():
    con = ibis.duckdb.connect()
    try:
        for name, table in readme_tables().items():
            con.create_table(name, table)
        yield con, readme_model(con)
    finally:
        con.disconnect()


def test_count_only_design_runs_on_duckdb_above_minimum(monkeypatch, readme_backend):
    _, model = readme_backend
    witnesses = []
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.6.0")
    monkeypatch.setattr(_native, "verify_exact_arithmetic", lambda raw: witnesses.append(raw))
    design = tr.recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"n": tr.agg("orders", window=("-30d", "0s"))},
    )

    frame = design.build(model)

    assert len(frame.to_pandas()) == 6
    assert witnesses == []


def test_exact_measure_runs_arithmetic_witness(readme_backend, monkeypatch):
    _, model = readme_backend
    witnesses = []
    monkeypatch.setattr(_native, "verify_exact_arithmetic", lambda raw: witnesses.append(raw))
    design = tr.recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"spend": tr.agg("spend", window=("-30d", "0s"))},
    )

    design.build(model).to_pandas()

    assert len(witnesses) == 1
