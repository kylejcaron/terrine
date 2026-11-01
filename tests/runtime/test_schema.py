import copy
import pickle

import duckdb
import pytest

from terrine.errors import TerrineError
from terrine.runtime.schema import (
    RUNTIME_SCHEMA_VERSION,
    check_runtime_schema,
    ensure_runtime_schema,
)

_VERSION_ONE_TABLES = {
    "runtime_dataset_recipes",
    "runtime_dataset_requests",
    "runtime_generation_pins",
    "runtime_generations",
    "runtime_lease_fences",
    "runtime_leases",
    "runtime_manifests",
    "runtime_partition_metadata",
    "runtime_partitions",
    "runtime_plan_invocations",
    "runtime_publications",
    "runtime_resource_guards",
    "runtime_run_events",
    "runtime_run_publications",
    "runtime_run_results",
    "runtime_runs",
    "runtime_schema_version",
    "runtime_snapshot_captures",
    "runtime_snapshot_gc",
    "runtime_snapshot_guards",
    "runtime_snapshot_manifests",
    "runtime_snapshot_references",
    "runtime_tasks",
}


def _catalog(con):
    tables = con.execute(
        "SELECT schema_name, table_name, sql FROM duckdb_tables() ORDER BY ALL"
    ).fetchall()
    indexes = con.execute(
        "SELECT schema_name, index_name, sql FROM duckdb_indexes() ORDER BY ALL"
    ).fetchall()
    return tables, indexes


def _versions(con):
    return con.execute("SELECT version FROM owner.runtime_schema_version").fetchall()


def _refusal(function, con) -> TerrineError:
    with pytest.raises(TerrineError) as error:
        function(con, "owner")
    assert error.value.code == "terrine.runtime.schema_unsupported"
    return error.value


def test_fresh_namespace_initializes_version_one():
    con = duckdb.connect(":memory:")
    check_runtime_schema(con, "owner")
    assert _catalog(con) == ([], [])

    ensure_runtime_schema(con, "owner")

    tables, indexes = _catalog(con)
    assert {table for schema, table, _ in tables if schema == "owner"} == _VERSION_ONE_TABLES
    assert [(schema, index) for schema, index, _ in indexes] == [
        ("owner", "runtime_runs_request_destination")
    ]
    assert _versions(con) == [(RUNTIME_SCHEMA_VERSION,)] == [(1,)]
    created = _catalog(con)
    for namespace in ("owner", "OWNER"):
        ensure_runtime_schema(con, namespace)
        check_runtime_schema(con, namespace)
    assert _catalog(con) == created
    assert _versions(con) == [(1,)]


def test_unrelated_runtime_prefixed_table_does_not_block_initialization():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.runtime_metrics (metric VARCHAR, value BIGINT)")
    con.execute("INSERT INTO main.runtime_metrics VALUES ('latency', 7)")
    unrelated = con.execute(
        "SELECT sql FROM duckdb_tables() WHERE table_name='runtime_metrics'"
    ).fetchall()

    check_runtime_schema(con, "main")
    ensure_runtime_schema(con, "main")

    assert con.execute("SELECT version FROM main.runtime_schema_version").fetchall() == [(1,)]
    assert (
        con.execute("SELECT sql FROM duckdb_tables() WHERE table_name='runtime_metrics'").fetchall()
        == unrelated
    )
    assert con.execute("SELECT * FROM main.runtime_metrics").fetchall() == [("latency", 7)]


def test_interrupted_initialization_leaves_no_tables():
    class FailingVersionInsert:
        def __init__(self, con):
            self.con = con

        def execute(self, sql, *args):
            if sql.startswith("INSERT") and "runtime_schema_version" in sql:
                raise RuntimeError("interrupted")
            return self.con.execute(sql, *args)

    con = duckdb.connect(":memory:")
    with pytest.raises(RuntimeError, match="interrupted"):
        ensure_runtime_schema(FailingVersionInsert(con), "owner")
    assert _catalog(con) == ([], [])
    ensure_runtime_schema(con, "owner")
    assert _versions(con) == [(1,)]


def test_unversioned_runtime_tables_refuse_before_writing():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute("CREATE TABLE owner.runtime_generations (dataset VARCHAR, generation VARCHAR)")
    before = _catalog(con)
    for function in (ensure_runtime_schema, check_runtime_schema):
        error = _refusal(function, con)
        assert dict(error.context) == {
            "namespace": "owner",
            "reason": "unversioned",
            "found_version": None,
            "supported_version": 1,
        }
    assert _catalog(con) == before


def test_newer_schema_refuses_without_changes():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute("CREATE TABLE owner.runtime_schema_version (version BIGINT NOT NULL)")
    con.execute("INSERT INTO owner.runtime_schema_version VALUES (2)")
    before = _catalog(con)
    for function in (ensure_runtime_schema, check_runtime_schema):
        error = _refusal(function, con)
        assert dict(error.context) == {
            "namespace": "owner",
            "reason": "newer",
            "found_version": 2,
            "supported_version": 1,
        }
    assert _catalog(con) == before
    assert _versions(con) == [(2,)]


def test_schema_refusal_survives_pickle():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA owner")
    con.execute("CREATE TABLE owner.runtime_runs (run_id VARCHAR)")
    error = _refusal(check_runtime_schema, con)
    for restored in (copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert restored.code == error.code
        assert restored.context == error.context
        with pytest.raises(TypeError):
            restored.context["reason"] = "newer"  # ty: ignore[invalid-assignment]
