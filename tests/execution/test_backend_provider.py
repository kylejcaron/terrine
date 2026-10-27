from __future__ import annotations

import copy
import pickle
import subprocess
import sys

import duckdb
import ibis
import pytest

from terrine.errors import TerrineError
from terrine.semantic.models import SqlQuery, TableRef
from tests.execution.test_inputs import _catalog


def test_backend_provider_preserves_duckdb_relation_admission_and_identity():
    from terrine.execution.backend import backend_provider

    con = ibis.duckdb.connect()
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 7)")
        provider = backend_provider(con)
        assert provider.matches(con)
        assert provider.dialect(con) == "duckdb"
        relation = TableRef(kind="table", name="events")
        identities = provider.required_relations(_catalog(), con)
        assert [(identity.kind, identity.schema, identity.name) for identity in identities] == [
            ("table", "main", "events")
        ]
        assert con.to_pyarrow(provider.resolve_relation(con, relation))["value"].to_pylist() == [7]
        with provider.open_execution(_catalog(), con) as context:
            assert context.owner_con is con
            assert context.execute(context.resolve(relation)).to_pylist()[0]["value"] == 7
        with pytest.raises(TerrineError) as error:
            provider.resolve_relation(con, SqlQuery(kind="sql", sql="DELETE FROM events"))
        assert error.value.code == "terrine.sql.invalid_read_query"
        assert con.con.execute("SELECT count(*) FROM events").fetchone() == (1,)
    finally:
        con.disconnect()


def test_unknown_backend_refuses_before_schema_or_source_query():
    from terrine import Model
    from terrine.backends.registry import resolve_engine
    from terrine.execution.backend import backend_provider

    class Unknown:
        name = "snowflake"

        def table(self, *args, **kwargs):
            raise AssertionError("unverified backend accessed source schema")

        def sql(self, *args, **kwargs):
            raise AssertionError("unverified backend executed source SQL")

    con = Unknown()
    for operation in (
        lambda: resolve_engine(con),
        lambda: backend_provider(con),
        lambda: Model.from_catalog(_catalog(), con=con, readiness={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert {key: value for key, value in error.value.context.items() if key != "route"} == {
            "backend": "snowflake",
            "operation": "query",
            "status": "unverified",
        }
        assert error.value.context["route"]


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_provider_releases_owned_execution_resources_after_error_or_cancel(failure):
    from terrine.execution.backend import backend_provider

    con = ibis.duckdb.connect()
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        provider = backend_provider(con)
        with pytest.raises(failure), provider.open_execution(_catalog(), con) as context:
            private = context.con.con
            raise failure()
        with pytest.raises(duckdb.ConnectionException):
            private.execute("SELECT 1")
        assert con.con.execute("SELECT 1").fetchone() == (1,)
        with provider.open_execution(_catalog(), con) as again:
            assert again.execute(again.con.sql("SELECT 2 AS value")).to_pylist() == [{"value": 2}]
    finally:
        con.disconnect()


def test_compiler_and_execution_imports_do_not_depend_on_runtime():
    code = """
import builtins
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'terrine.runtime' or name.startswith('terrine.runtime.'):
        raise ImportError('runtime intentionally unavailable')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
import terrine.compile.anchors
import terrine.execution.backend
import terrine.backends.capabilities
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)


def test_provider_does_not_accept_a_duckdb_name_without_native_connection():
    from terrine.execution._native import verify_native_backend
    from terrine.execution.backend import backend_provider

    class Unverified:
        name = "duckdb"

    with pytest.raises(TerrineError):
        backend_provider(Unverified())
    with pytest.raises(TerrineError) as error:
        verify_native_backend(Unverified())
    assert error.value.code == "terrine.execution.snapshot_unavailable"


@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "bigquery", "sqlite"])
def test_unverified_backend_evidence_does_not_admit_queries(dialect):
    from terrine.execution.backend import backend_capabilities, backend_provider

    class Connection:
        name = dialect

        def table(self, *args, **kwargs):
            raise AssertionError("unverified backend must not inspect schema")

        def sql(self, *args, **kwargs):
            raise AssertionError("unverified backend must not execute")

    con = Connection()
    evidence = backend_capabilities(con)
    assert evidence.dialect_admission is None
    assert evidence.retained_snapshot is None
    assert evidence.atomic_manifest_publish is None
    assert evidence.arrow_batches is None
    assert evidence.native_parquet_unload is None
    with pytest.raises(TerrineError) as error:
        backend_provider(con)
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["status"] == "unverified"
    assert error.value.context["backend"] == dialect


def test_raw_driver_connection_is_not_an_ibis_execution_provider():
    from terrine.backends.registry import resolve_engine
    from terrine.execution._native import verify_native_backend
    from terrine.execution.backend import backend_capabilities, backend_provider

    raw = duckdb.connect()
    try:
        assert verify_native_backend(raw)[0] is raw
        for operation in (backend_provider, resolve_engine):
            with pytest.raises(TerrineError) as error:
                operation(raw)
            assert error.value.code == "terrine.backend.capability_unavailable"
            assert {key: value for key, value in error.value.context.items() if key != "route"} == {
                "backend": "duckdbpyconnection",
                "operation": "query",
                "status": "unverified",
            }
            assert "Ibis" in error.value.context["route"]
        assert backend_capabilities(raw).dialect_admission is None
    finally:
        raw.close()


@pytest.mark.parametrize(
    ("version", "server", "status"),
    [
        (
            "PostgreSQL 16.4 on aarch64-apple-darwin23.4.0, compiled by Apple clang",
            "PostgreSQL",
            "admitted",
        ),
        (
            "PostgreSQL 15.8 on x86_64-pc-linux-gnu, compiled by GCC",
            "PostgreSQL",
            "unverified",
        ),
        (
            "PostgreSQL 17.0 on x86_64-pc-linux-gnu, compiled by GCC",
            "PostgreSQL",
            "unverified",
        ),
        (
            "PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 "
            "(Red Hat 3.4.2-6.fc3), Redshift 1.0.77467",
            "Redshift",
            "unsupported",
        ),
        (
            "PostgreSQL 12.12 (Greenplum Database 7.0.0 build commit:abc) on x86_64-pc-linux-gnu",
            "Greenplum",
            "unsupported",
        ),
        (
            "CockroachDB CCL v23.1.11 (x86_64-pc-linux-gnu, built 2023/09/27)",
            "CockroachDB",
            "unsupported",
        ),
        ("", "unknown", "unverified"),
        (None, "unknown", "unverified"),
        ("unknown version", "unknown", "unverified"),
        ("OtherSQL 16", "OtherSQL", "unverified"),
    ],
)
def test_postgres_engine_identity_preserves_classification(
    postgres_backend, version, server, status
):
    from terrine.backends.registry import ENGINES

    con = postgres_backend(version)
    if status == "admitted":
        ENGINES["postgres"].identify(con)
    else:
        with pytest.raises(TerrineError) as error:
            ENGINES["postgres"].identify(con)
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert {key: value for key, value in error.value.context.items() if key != "route"} == {
            "backend": server,
            "operation": "dialect_admission",
            "status": status,
        }
        with pytest.raises(TypeError):
            error.value.context["status"] = "admitted"  # ty: ignore[invalid-assignment]
        for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
            assert restored.code == error.value.code
            assert restored.context == error.value.context
    assert con.con.queries == ["SELECT pg_catalog.version()"]


def test_postgres_identity_refuses_incompatible_ibis_before_query(postgres_backend, monkeypatch):
    from terrine.backends.engines import postgres
    from terrine.backends.registry import ENGINES

    monkeypatch.setattr(postgres, "version", lambda package: "11.0.0")
    con = postgres_backend("PostgreSQL 16.4")
    with pytest.raises(TerrineError) as error:
        ENGINES["postgres"].identify(con)
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert {key: value for key, value in error.value.context.items() if key != "route"} == {
        "backend": "postgres",
        "operation": "dialect_admission",
        "status": "unsupported",
    }
    with pytest.raises(TypeError):
        error.value.context["status"] = "admitted"  # ty: ignore[invalid-assignment]
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.code == error.value.code
        assert restored.context == error.value.context
    assert con.con.queries == []


@pytest.fixture
def postgres_backend():

    class DriverConnection:
        def __init__(self):
            self.queries = []
            self.rows = []

        def cursor(self):
            connection = self

            class Cursor:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

                def execute(self, sql, params=None):
                    connection.queries.append(sql)
                    assert sql == "SELECT pg_catalog.version()"

                def fetchall(self):
                    return connection.rows

            return Cursor()

    DriverConnection.__module__ = "psycopg"

    class PostgresBackend:
        __module__ = "ibis.backends.postgres"

        def __init__(self):
            self.con = DriverConnection()

        def table(self, *args, **kwargs):
            raise AssertionError("source schema was accessed before identity admission")

        def sql(self, *args, **kwargs):
            raise AssertionError("source schema was accessed before identity admission")

    def make(version):
        con = PostgresBackend()
        con.con.rows = [] if version is None else [(version,)]
        return con

    return make


def test_postgres_admission_refuses_lookalike_before_source_schema_access(postgres_backend):
    from terrine import Model
    from terrine.execution import backend as backend_module
    from terrine.execution.backend import backend_provider

    def connection():
        return postgres_backend("PostgreSQL 8.0.2 Redshift 1.0.77467")

    for operation in (
        lambda con: backend_provider(con),
        lambda con: Model.from_catalog(_catalog(), con=con, readiness={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation(connection())
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["backend"] == "Redshift"
        assert error.value.context["operation"] == "dialect_admission"
        assert error.value.context["status"] == "unsupported"
        assert "DuckDB" in error.value.context["route"]
        for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
            assert restored.code == error.value.code
            assert restored.context == error.value.context

    inspection_con = connection()
    assert backend_module.backend_capabilities(inspection_con).dialect_admission is None
    assert inspection_con.con.queries == []


@pytest.mark.parametrize("allow", [False, True])
@pytest.mark.parametrize("kind", ["raw", "fake", "lookalike"])
def test_opt_in_never_bypasses_connection_identity(postgres_backend, allow, kind):
    from terrine.backends.registry import resolve_engine

    class Fake:
        name = "duckdb"

        def table(self, *args, **kwargs):
            raise AssertionError("source accessed")

    con = (
        duckdb.connect()
        if kind == "raw"
        else (
            postgres_backend("PostgreSQL 8.0.2 Redshift 1.0.77467")
            if kind == "lookalike"
            else Fake()
        )
    )
    try:
        with pytest.raises(TerrineError) as caught:
            resolve_engine(con, allow_unverified=allow)
        assert caught.value.code == "terrine.backend.capability_unavailable"
        assert {k: v for k, v in caught.value.context.items() if k != "route"} == (
            {"backend": "Redshift", "operation": "dialect_admission", "status": "unsupported"}
            if kind == "lookalike"
            else {
                "backend": "duckdbpyconnection" if kind == "raw" else "duckdb",
                "operation": "query",
                "status": "unverified",
            }
        )
    finally:
        if isinstance(con, duckdb.DuckDBPyConnection):
            con.close()
