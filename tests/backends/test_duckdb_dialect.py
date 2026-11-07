from __future__ import annotations

import pytest

import terrine as tr
from terrine.backends.engines import duckdb as duckdb_engine
from terrine.backends.engines.duckdb import DuckDBDialect
from tests.fixtures.readme import readme_model


@pytest.mark.parametrize("build", ["1.5.0", "1.5.6", "1.6.3", "2.0.0"])
def test_exact_prefix_supported_on_any_duckdb_at_or_above_floor(monkeypatch, build):
    monkeypatch.setattr(duckdb_engine, "version", lambda _: build)

    assert DuckDBDialect("duckdb").supports_exact_prefix("duckdb")


def test_exact_prefix_rejects_other_backends(monkeypatch):
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.6.3")

    assert not DuckDBDialect("duckdb").supports_exact_prefix("postgres")


def test_count_only_design_runs_on_patched_duckdb_version(monkeypatch):
    monkeypatch.setattr(duckdb_engine, "version", lambda _: "1.6.0")
    design = tr.recipes.time_series(
        every="1mo",
        start="2025-02-01",
        end="2025-03-01",
        features={"n": tr.agg("orders", window=("-30d", "0s"))},
    )

    frame = design.build(readme_model())

    assert len(frame.to_pandas()) == 6
