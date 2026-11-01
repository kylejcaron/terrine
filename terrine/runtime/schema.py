"""Versioned runtime metadata tables of a DuckDB owner namespace.

Every runtime metadata table and index is declared here and created only by
``ensure_runtime_schema``, together with a ``runtime_schema_version`` row.
Partition tables written by publication hold data, not schema, and stay with
the publisher. A schema change requires a new version: its migration runs
through ``ensure_runtime_schema`` under the owner's exclusive lease, after the
previous owner has drained or recovered every non-terminal run.
"""

from __future__ import annotations

import threading
from typing import Any

from terrine.errors import RefusalSpec

__all__ = [
    "RUNTIME_SCHEMA_VERSION",
    "SCHEMA_UNSUPPORTED",
    "check_runtime_schema",
    "ensure_runtime_schema",
]

RUNTIME_SCHEMA_VERSION = 1

SCHEMA_UNSUPPORTED = RefusalSpec(
    "terrine.runtime.schema_unsupported",
    "Runtime namespace {namespace!r} holds {reason} runtime metadata that this release "
    "(schema version {supported_version}) cannot read; stage into a new namespace or owner "
    "database, or open it with the release that wrote it.",
)

_TABLES = (
    (
        "runtime_generations",
        "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
        "definition_json VARCHAR NOT NULL, snapshot_json VARCHAR NOT NULL, "
        "created_at TIMESTAMP NOT NULL, supersedes VARCHAR, superseded_by VARCHAR, "
        "guard_version BIGINT DEFAULT 0, PRIMARY KEY(dataset, generation)",
    ),
    (
        "runtime_generation_pins",
        "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, reader VARCHAR NOT NULL, "
        "PRIMARY KEY(dataset, generation, reader)",
    ),
    (
        "runtime_snapshot_references",
        "snapshot_id VARCHAR NOT NULL, kind VARCHAR NOT NULL, reference VARCHAR NOT NULL, "
        "PRIMARY KEY(snapshot_id, kind, reference)",
    ),
    ("runtime_snapshot_gc", "snapshot_id VARCHAR PRIMARY KEY"),
    ("runtime_snapshot_guards", "snapshot_id VARCHAR PRIMARY KEY, guard_version BIGINT NOT NULL"),
    (
        "runtime_snapshot_manifests",
        "dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, snapshot_id VARCHAR",
    ),
    (
        "runtime_snapshot_captures",
        "snapshot_id VARCHAR PRIMARY KEY, cost_json VARCHAR NOT NULL, "
        "readiness_json VARCHAR NOT NULL",
    ),
    (
        "runtime_leases",
        "resource VARCHAR PRIMARY KEY, owner VARCHAR NOT NULL, fence BIGINT NOT NULL, "
        "expires_at TIMESTAMP NOT NULL",
    ),
    ("runtime_lease_fences", "resource VARCHAR PRIMARY KEY, last_fence BIGINT NOT NULL"),
    ("runtime_resource_guards", "resource VARCHAR PRIMARY KEY, guard_version BIGINT NOT NULL"),
    (
        "runtime_manifests",
        "dataset VARCHAR PRIMARY KEY, generation VARCHAR NOT NULL, version BIGINT NOT NULL, "
        "snapshot_id VARCHAR",
    ),
    (
        "runtime_publications",
        "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, version BIGINT NOT NULL, "
        "PRIMARY KEY(dataset, generation)",
    ),
    (
        "runtime_run_publications",
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
        "version BIGINT NOT NULL",
    ),
    (
        "runtime_partitions",
        "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, partition VARCHAR NOT NULL, "
        "table_name VARCHAR NOT NULL, digest VARCHAR NOT NULL, "
        "PRIMARY KEY(dataset, generation, partition)",
    ),
    (
        "runtime_partition_metadata",
        "dataset VARCHAR NOT NULL, generation VARCHAR NOT NULL, partition VARCHAR NOT NULL, "
        "schema_metadata BLOB NOT NULL, PRIMARY KEY(dataset, generation, partition)",
    ),
    (
        "runtime_runs",
        "run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, fingerprint VARCHAR NOT NULL, "
        "state VARCHAR NOT NULL, published_generation VARCHAR, attempt_fence BIGINT, "
        "updated_at TIMESTAMP, heartbeat_at TIMESTAMP, snapshot_id VARCHAR, "
        "task_count BIGINT, error_code VARCHAR, request_key VARCHAR, destination_json VARCHAR",
    ),
    (
        "runtime_tasks",
        "run_id VARCHAR NOT NULL, task_id VARCHAR NOT NULL, status VARCHAR NOT NULL, "
        "output_blob BLOB, task_fence BIGINT, attempts BIGINT DEFAULT 0, layer VARCHAR, "
        "output_rows BIGINT, output_bytes BIGINT, PRIMARY KEY(run_id, task_id)",
    ),
    (
        "runtime_run_results",
        "run_id VARCHAR NOT NULL, partition VARCHAR NOT NULL, PRIMARY KEY(run_id, partition)",
    ),
    (
        "runtime_run_events",
        "run_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, event_at TIMESTAMP NOT NULL, "
        "kind VARCHAR NOT NULL, payload VARCHAR NOT NULL, PRIMARY KEY(run_id, sequence)",
    ),
    (
        "runtime_plan_invocations",
        "invocation_id VARCHAR PRIMARY KEY, request_hash VARCHAR NOT NULL, "
        "snapshot_id VARCHAR NOT NULL, invocation_json VARCHAR NOT NULL",
    ),
    ("runtime_dataset_requests", "request_key VARCHAR PRIMARY KEY, recipe_json VARCHAR NOT NULL"),
    (
        "runtime_dataset_recipes",
        "dataset VARCHAR, generation VARCHAR, recipe_json VARCHAR, request_key VARCHAR, "
        "manifest_json VARCHAR, PRIMARY KEY(dataset, generation)",
    ),
)
_UNIQUE_INDEXES = (
    ("runtime_runs_request_destination", "runtime_runs", "request_key, destination_json"),
)
# Serializes first creation among this process's connections to one owner database.
_CREATION = threading.Lock()


def ensure_runtime_schema(con: Any, namespace: str) -> None:
    """Create a namespace's runtime metadata at the supported version, refusing other schemas.

    A namespace without runtime tables receives every metadata table and index and
    its version row in one transaction. A namespace at the supported version is
    left unchanged; any other namespace refuses before anything is written.

    Args:
        con: DuckDB connection of the runtime owner.
        namespace: Runtime schema name.
    """
    if _initialized(con, namespace):
        return
    with _CREATION:
        if _initialized(con, namespace):
            return
        schema = _quote(namespace)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for table, columns in _TABLES:
                con.execute(f"CREATE TABLE {schema}.{table} ({columns})")
            for index, table, columns in _UNIQUE_INDEXES:
                con.execute(f"CREATE UNIQUE INDEX {index} ON {schema}.{table} ({columns})")
            con.execute(f"CREATE TABLE {schema}.runtime_schema_version (version BIGINT NOT NULL)")
            con.execute(
                f"INSERT INTO {schema}.runtime_schema_version VALUES (?)",
                [RUNTIME_SCHEMA_VERSION],
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise


def check_runtime_schema(con: Any, namespace: str) -> None:
    """Refuse, without writing, a namespace whose runtime metadata this release cannot read.

    A namespace without runtime tables passes; nothing is created.

    Args:
        con: DuckDB connection of the runtime owner.
        namespace: Runtime schema name.
    """
    _initialized(con, namespace)


def _initialized(con: Any, namespace: str) -> bool:
    """True at the supported version, False without runtime tables; otherwise refuse."""
    tables = {
        name
        for (name,) in con.execute(
            "SELECT lower(table_name) FROM information_schema.tables "
            "WHERE table_catalog=current_database() AND lower(table_schema)=lower(?) "
            "AND starts_with(lower(table_name), 'runtime_')",
            [namespace],
        ).fetchall()
    }
    if not tables:
        return False
    found = None
    if "runtime_schema_version" in tables:
        found = con.execute(
            f"SELECT max(version) FROM {_quote(namespace)}.runtime_schema_version"
        ).fetchone()[0]
    if found == RUNTIME_SCHEMA_VERSION:
        return True
    newer = isinstance(found, int) and found > RUNTIME_SCHEMA_VERSION
    raise SCHEMA_UNSUPPORTED.error(
        namespace=namespace,
        reason="newer" if newer else "unversioned",
        found_version=found,
        supported_version=RUNTIME_SCHEMA_VERSION,
    )


def _quote(namespace: str) -> str:
    return '"' + namespace.replace('"', '""') + '"'
