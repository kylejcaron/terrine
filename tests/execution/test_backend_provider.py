from __future__ import annotations

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
    from terrine.execution.backend import backend_provider

    class Unknown:
        name = "snowflake"

        def table(self, *args, **kwargs):
            raise AssertionError("unverified backend accessed source schema")

        def sql(self, *args, **kwargs):
            raise AssertionError("unverified backend executed source SQL")

    con = Unknown()
    for operation in (
        lambda: backend_provider(con),
        lambda: Model.from_catalog(_catalog(), con=con, readiness={}),
    ):
        with pytest.raises(TerrineError) as error:
            operation()
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["backend"] == "snowflake"
        assert error.value.context["status"] == "unverified"
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
    from terrine.execution._native import verify_native_backend
    from terrine.execution.backend import backend_capabilities, backend_provider

    raw = duckdb.connect()
    try:
        assert verify_native_backend(raw)[0] is raw
        with pytest.raises(TerrineError) as error:
            backend_provider(raw)
        assert error.value.code == "terrine.backend.capability_unavailable"
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
    ],
)
def test_server_identity_classification(version, server, status):
    from terrine.execution._postgres import classify_server_version

    identity = classify_server_version(version)
    assert identity.server == server
    assert identity.status == status


def test_postgres_admission_refuses_lookalike_before_source_schema_access():

    import copy
    import pickle

    from terrine import Model
    from terrine.execution import backend as backend_module
    from terrine.execution.backend import backend_provider

    class DriverConnection:
        def cursor(self):
            class Cursor:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

                def execute(self, sql, params=None):
                    assert sql == "SELECT version()"

                def fetchall(self):
                    return [
                        (
                            "PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC "
                            "gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.77467",
                        )
                    ]

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

    for operation in (
        lambda con: backend_provider(con),
        lambda con: Model.from_catalog(_catalog(), con=con, readiness={}),
        lambda con: backend_module.backend_capabilities(con),
    ):
        with pytest.raises(TerrineError) as error:
            operation(PostgresBackend())
        assert error.value.code == "terrine.backend.capability_unavailable"
        assert error.value.context["backend"] == "Redshift"
        assert error.value.context["operation"] == "dialect_admission"
        assert error.value.context["status"] == "unsupported"
        assert "DuckDB" in error.value.context["route"]
        for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
            assert restored.code == error.value.code
            assert restored.context == error.value.context
