from __future__ import annotations

import copy
import pickle
from dataclasses import FrozenInstanceError, replace
from types import MappingProxyType

import ibis
import ibis.expr.datatypes as dt
import pytest

from terrine.backends import registry
from terrine.backends.dialect import dialect_scope, unavailable
from terrine.backends.numeric import exact_type
from terrine.backends.registry import ENGINES, engine_supports, get_engine, resolve_engine
from terrine.errors import TerrineError
from terrine.execution.backend import backend_capabilities


def test_registry_metadata_is_immutable_and_case_insensitive():
    assert tuple(ENGINES) == ("duckdb", "postgres", "bigquery", "snowflake")
    assert get_engine("POSTGRES") is ENGINES["postgres"]
    assert get_engine("missing") is None
    for name, engine in ENGINES.items():
        assert engine.name == name
        assert engine.status == "verified"
        assert engine_supports(name.upper(), "event_resolution")
        assert engine_supports(name, "warehouse_preflight") == (name == "duckdb")
        with pytest.raises(FrozenInstanceError):
            engine.name = "other"  # ty: ignore[invalid-assignment]
    assert not engine_supports("missing", "event_resolution")
    assert not engine_supports("duckdb", "missing")
    with pytest.raises(TypeError):
        ENGINES["missing"] = ENGINES["duckdb"]  # ty: ignore[invalid-assignment]


def test_scope_selection_restores_after_nested_error():
    baseline = exact_type()
    with dialect_scope("postgres"):
        outer_type = exact_type()
        assert outer_type != baseline
        with pytest.raises(RuntimeError), dialect_scope("SNOWFLAKE"):
            assert exact_type() == dt.Decimal(38, 0)
            raise RuntimeError("stop inner scope")
        assert exact_type() == outer_type
    assert exact_type() == baseline


def test_unavailable_route_names_only_verified_operation_providers(monkeypatch):
    def destinations(operation):
        error = unavailable(operation, "missing")
        assert error.code == "terrine.backend.capability_unavailable"
        assert {key: value for key, value in error.context.items() if key != "route"} == {
            "backend": "missing",
            "operation": operation,
            "status": "unsupported",
        }
        for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
            assert restored.code == error.code
            assert restored.context == error.context
        return {
            name
            for name, engine in ENGINES.items()
            if engine.display_name in error.context["route"]
        }

    assert destinations("exact_state") == set(ENGINES)
    assert destinations("exact_ratio") == {"postgres", "bigquery"}
    entries = dict(ENGINES)
    entries["postgres"] = replace(entries["postgres"], status="unverified")
    monkeypatch.setattr(registry, "ENGINES", MappingProxyType(entries))
    assert destinations("exact_ratio") == {"bigquery"}
    monkeypatch.setattr(
        ENGINES["bigquery"].dialect,
        "operations",
        ENGINES["bigquery"].dialect.operations - {"exact_ratio"},
    )
    assert destinations("exact_ratio") == set()


def test_static_inspection_and_admission_preserve_native_handle_boundary():
    con = ibis.duckdb.connect()
    try:
        assert resolve_engine(con) is ENGINES["duckdb"]
        assert backend_capabilities(con) is ENGINES["duckdb"].capabilities
    finally:
        con.disconnect()

    class InvalidDuckDB:
        __module__ = "ibis.backends.duckdb"
        name = "duckdb"

    assert backend_capabilities(InvalidDuckDB()).dialect_admission is None
    with pytest.raises(TerrineError) as error:
        resolve_engine(InvalidDuckDB())
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert {key: value for key, value in error.value.context.items() if key != "route"} == {
        "backend": "duckdb",
        "operation": "query",
        "status": "unverified",
    }


def test_postgres_identity_preserves_invalid_driver_refusal():
    class InvalidPostgres:
        __module__ = "ibis.backends.postgres"
        name = "postgres"
        con = object()

    with pytest.raises(TerrineError) as error:
        resolve_engine(InvalidPostgres())
    assert error.value.code == "terrine.execution.snapshot_unavailable"
    assert error.value.context == {
        "reason": "connection is not a psycopg-backed Ibis PostgreSQL backend"
    }
    with pytest.raises(TypeError):
        error.value.context["reason"] = "other"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.code == error.value.code
        assert restored.context == error.value.context
