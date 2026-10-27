from __future__ import annotations

import copy
import pickle
from dataclasses import FrozenInstanceError, replace
from importlib import metadata
from types import MappingProxyType, SimpleNamespace

import ibis
import ibis.expr.datatypes as dt
import pytest

from terrine.backends import registry
from terrine.backends.dialect import Dialect, dialect_for_name, dialect_scope, unavailable
from terrine.backends.numeric import exact_type
from terrine.backends.registry import ENGINES, engine_supports, get_engine, resolve_engine
from terrine.errors import TerrineError
from terrine.execution.backend import backend_capabilities
from terrine.temporal.prefix import _require_backend


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


@pytest.mark.parametrize(
    ("engine", "method", "operands", "operation"),
    [
        ("duckdb", "exact_ratio", (ibis.literal(1), ibis.literal(2)), "exact_ratio"),
        ("duckdb", "exact_is_nonfinite", (ibis.literal(1),), "exact_is_nonfinite"),
        ("snowflake", "exact_ratio", (ibis.literal(1), ibis.literal(2)), "exact_ratio"),
        ("snowflake", "exact_is_nonfinite", (ibis.literal(1),), "exact_is_nonfinite"),
        ("postgres", "wide_type", (), "normalized"),
        ("postgres", "wide_quotient", (ibis.literal(6), ibis.literal(3)), "normalized"),
        ("postgres", "trim_leading_zeros", (ibis.literal("007"),), "normalized"),
        ("bigquery", "wide_type", (), "normalized"),
        ("bigquery", "wide_quotient", (ibis.literal(6), ibis.literal(3)), "normalized"),
        ("bigquery", "trim_leading_zeros", (ibis.literal("007"),), "normalized"),
    ],
)
def test_registered_engines_keep_native_operation_refusals(engine, method, operands, operation):
    """A portable base default never makes a registered engine advertise more."""
    dialect = dialect_for_name(engine)
    assert operation not in dialect.operations
    with pytest.raises(TerrineError) as caught:
        getattr(dialect, method)(*operands)
    error = caught.value
    assert error.code == "terrine.backend.capability_unavailable"
    assert {key: value for key, value in error.context.items() if key != "route"} == {
        "backend": engine,
        "operation": operation,
        "status": "unsupported",
    }
    with pytest.raises(TypeError):
        error.context["status"] = "verified"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context


def test_registered_operation_declarations_cover_every_portable_default():
    """Each registered engine either declares an operation or refuses it explicitly."""
    refused = {
        "duckdb": {"exact_ratio", "exact_is_nonfinite"},
        "postgres": {"normalized"},
        "bigquery": {"normalized"},
        "snowflake": {"exact_ratio", "exact_is_nonfinite"},
    }
    for name, engine in ENGINES.items():
        declared = engine.dialect.operations
        assert declared | refused[name] == Dialect.operations
        assert not declared & refused[name]


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


@pytest.mark.parametrize(
    ("backend", "build", "scope", "accepted"),
    [
        # A registered policy admits only its own backend, DuckDB only on its build.
        ("duckdb", "0.0.0", "duckdb", False),
        ("postgres", "1.5.6", "duckdb", False),
        ("bigquery", "1.5.6", "postgres", False),
        ("snowflake", "1.5.6", "bigquery", False),
        ("duckdb", "1.5.6", "snowflake", False),
        ("unbound", "1.5.6", "postgres", False),
        ("duckdb", "1.5.6", "duckdb", True),
        ("postgres", "1.5.6", "postgres", True),
        ("bigquery", "1.5.6", "bigquery", True),
        ("snowflake", "1.5.6", "snowflake", True),
        # The portable policy needs no native type or build; execution admission
        # refuses a backend without usable decimals.
        ("duckdb", "0.0.0", "generic", True),
        ("postgres", "1.5.6", "generic", True),
        ("unregistered", "1.5.6", "generic", True),
        ("unbound", "1.5.6", "generic", True),
    ],
)
def test_exact_prefix_preserves_build_and_scope_admission_without_queries(
    monkeypatch, backend, build, scope, accepted
):
    table = ibis.table({"value": "int64"})

    class NoQueries:
        name = backend

        def raw_sql(self, *args, **kwargs):
            raise AssertionError("exact-prefix admission queried the backend")

        sql = raw_sql

    if backend != "unbound":
        monkeypatch.setattr(ibis.Table, "_find_backend", lambda *args, **kwargs: NoQueries())
    monkeypatch.setattr(metadata, "distribution", lambda name: SimpleNamespace(version=build))
    with dialect_scope(scope):
        if accepted:
            _require_backend(table)
            return
        with pytest.raises(TerrineError) as caught:
            _require_backend(table)
    error = caught.value
    assert error.code == "terrine.temporal.exact_unsupported"
    assert dict(error.context) == {"backend": backend}
    with pytest.raises(TypeError):
        error.context["backend"] = "other"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context
