"""BigQuery relation identity, admission and private snapshot sessions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from terrine.execution._metadata import invalidate_metadata
from terrine.execution._views import admitted_view_query
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import _admit_read_query_ast, admit_read_query

DIALECT = "bigquery"
# Physical row sources; external tables are not snapshot-consistent.
_TABLE_TYPES = {"TABLE": "table", "SNAPSHOT": "table", "VIEW": "view"}


def _snapshot_error(reason: str):
    from terrine.execution.inputs import _SNAPSHOT_UNAVAILABLE

    return _SNAPSHOT_UNAVAILABLE.error(reason=reason)


def client_of(con: Any) -> Any:
    client = getattr(con, "client", None)
    if client is None or type(client).__module__.split(".")[:3] != ["google", "cloud", "bigquery"]:
        raise _snapshot_error("connection is not a google-cloud-bigquery-backed Ibis backend")
    return client


@dataclass(frozen=True, slots=True)
class _Relation:
    project: str
    dataset: str
    name: str
    kind: str
    created: str
    definition_hash: str

    @property
    def identity(self) -> str:
        return json.dumps([self.project, self.dataset, self.name])


def _defaults(con: Any) -> tuple[str, str]:
    """The connection's data project and default dataset (not its billing project)."""
    return con.data_project, con.dataset


def _reference(con: Any, name: str) -> tuple[str, str, str]:
    parts = name.split(".")
    project, dataset = _defaults(con)
    if len(parts) == 1:
        return project, dataset, parts[0]
    if len(parts) == 2:
        return project, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise _snapshot_error(f"relation name {name!r} is not project.dataset.table")


def _relation(con: Any, project: str, dataset: str, name: str) -> _Relation:
    try:
        table = client_of(con).get_table(f"{project}.{dataset}.{name}")
    except Exception as exc:
        raise _snapshot_error(f"relation {name!r} is not visible to this connection") from exc
    kind = _TABLE_TYPES.get(table.table_type)
    if kind is None:
        raise _snapshot_error(f"relation {name!r} has unsupported type {table.table_type!r}")
    definition = json.dumps(
        {
            "schema": [field.to_api_repr() for field in table.schema],
            "view": table.view_query,
        },
        sort_keys=True,
    )
    return _Relation(
        project,
        dataset,
        name,
        kind,
        table.created.isoformat() if table.created is not None else "",
        hashlib.sha256(definition.encode()).hexdigest(),
    )


def check_routines(sql: str) -> None:
    """Refuse a query that calls a dataset-qualified routine.

    Persistent UDFs and remote functions are always qualified, so this covers
    every user routine; unqualified names always resolve to BigQuery built-ins.
    """
    from sqlglot import exp, parse_one

    try:
        statement = parse_one(sql, read=DIALECT)
    except Exception as exc:
        raise _snapshot_error("view definition could not be parsed") from exc
    for node in statement.walk():
        qualified_call = isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func)
        # A fully backticked name such as `project.dataset.fn` parses as one identifier.
        quoted_path = isinstance(node, exp.Anonymous) and "." in node.name
        if qualified_call or quoted_path:
            raise _snapshot_error("query calls a user-defined routine")


def _tables_in(sql: str, con: Any) -> list[tuple[str, str, str]]:
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    statement = _admit_read_query_ast(sql, DIALECT, DIALECT)
    found = []
    for scope in traverse_scope(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                parts = [part for part in (source.catalog, source.db, source.name) if part]
                found.append(_reference(con, ".".join(parts)))
    return found


def _view_tables(sql: str, con: Any, name: str) -> list[tuple[str, str, str]]:
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    found = []
    for scope in traverse_scope(admitted_view_query(sql, DIALECT, name=name)):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                parts = [part for part in (source.catalog, source.db, source.name) if part]
                found.append(_reference(con, ".".join(parts)))
    return found


def _closure(
    con: Any,
    key: tuple[str, str, str],
    seen: dict[tuple[str, str, str], _Relation],
    path: frozenset[tuple[str, str, str]],
) -> None:
    if key in path:
        raise _snapshot_error("view dependency cycle")
    if key in seen:
        return
    relation = _relation(con, *key)
    seen[key] = relation
    if relation.kind == "view":
        table = client_of(con).get_table(".".join(key))
        for dependency in _view_tables(table.view_query, con, key[-1]):
            _closure(con, dependency, seen, path | {key})


def _closure_for(relation: RelationSpec, con: Any) -> dict[tuple[str, str, str], _Relation]:
    seen: dict[tuple[str, str, str], _Relation] = {}
    if isinstance(relation, TableRef):
        _closure(con, _reference(con, relation.name), seen, frozenset())
    elif isinstance(relation, SqlQuery):
        for key in _tables_in(relation.sql, con):
            _closure(con, key, seen, frozenset())
    return seen


def roots(catalog: Catalog, units: Mapping[str, Any] | None = None) -> tuple[RelationSpec, ...]:
    relations: list[RelationSpec] = [source.relation for source in catalog.sources.values()]
    relations.extend(dimension.relation for dimension in catalog.dimensions.values())
    relations.extend(TableRef(kind="table", name=unit.table) for unit in (units or {}).values())
    return tuple(relations)


def required_relations(
    catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
) -> tuple[Any, ...]:
    from terrine.execution.inputs import PhysicalRelationId

    seen: dict[tuple[str, str, str], _Relation] = {}
    for relation in roots(catalog, units):
        seen.update(_closure_for(relation, con))
    return tuple(
        sorted(
            (
                PhysicalRelationId("table", item.project, item.dataset, item.name)
                for item in seen.values()
                if item.kind == "table"
            ),
            key=lambda item: (item.catalog or "", item.schema or "", item.name),
        )
    )


def admission_fingerprint(relation: RelationSpec, con: Any) -> str:
    """Return the identity of a relation's closure; any change alters it."""
    payload = sorted(
        (item.identity, item.kind, item.created, item.definition_hash)
        for item in _closure_for(relation, con).values()
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def resolve(con: Any, relation: RelationSpec, catalog_dialect: str = DIALECT) -> Any:
    if isinstance(relation, TableRef):
        project, dataset, name = _reference(con, relation.name)
        item = _relation(con, project, dataset, name)
        return con.table(item.name, database=(item.project, item.dataset))
    sql = admit_read_query(relation.sql, catalog_dialect, DIALECT)
    return con.sql(sql, schema=_query_schema(con, sql))


def _query_schema(con: Any, sql: str) -> Any:
    """Describe an admitted query by dry run, resolving unqualified tables in the
    connection's default dataset (Ibis's own dry run omits it)."""
    from google.cloud import bigquery
    from ibis.backends.bigquery.datatypes import BigQuerySchema

    project, dataset = _defaults(con)
    config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        default_dataset=f"{project}.{dataset}" if dataset else None,
    )
    try:
        job = client_of(con).query(sql, job_config=config)
    except Exception as exc:
        raise _snapshot_error("query schema could not be determined") from exc
    return BigQuerySchema.to_ibis(job.schema)


def owned_session(con: Any) -> tuple[Any, Any, str]:
    """Open a private client, storage reader and session holding one snapshot.

    The caller's client, and any session or job configuration it holds, is never
    used for terrine's reads. Both owned clients use the caller's credentials, and
    queries default to the caller's data project and dataset.
    """
    from google.cloud import bigquery
    from google.cloud.bigquery_storage_v1 import BigQueryReadClient

    caller = client_of(con)
    project, dataset = _defaults(con)
    try:
        client = bigquery.Client(
            project=caller.project,
            credentials=caller._credentials,
            location=caller.location,
        )
        storage = BigQueryReadClient(credentials=caller._credentials)
        job = client.query(
            "BEGIN TRANSACTION", job_config=bigquery.QueryJobConfig(create_session=True)
        )
        job.result()
    except Exception as exc:
        raise _snapshot_error("could not open a private BigQuery session") from exc
    session_id = job.session_info.session_id if job.session_info is not None else None
    if session_id is None:
        client.close()
        raise _snapshot_error("BigQuery did not open a session for the snapshot transaction")
    client.default_query_job_config = bigquery.QueryJobConfig(
        default_dataset=f"{project}.{dataset}" if dataset else None,
        connection_properties=[bigquery.ConnectionProperty("session_id", session_id)],
    )
    return client, storage, session_id


class BigQuerySession:
    """One snapshot transaction in a terrine-owned BigQuery session."""

    backend = DIALECT

    def __init__(self, client: Any, storage: Any, session_id: str) -> None:
        self.client = client
        self.storage = storage
        self.session_id = session_id

    def check_query(self, sql: str) -> None:
        check_routines(sql)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        invalidate_metadata()
        with _overflow_refusal():
            return con.to_pyarrow(query)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> pa.RecordBatchReader:
        invalidate_metadata()
        with _overflow_refusal():
            reader = con.to_pyarrow_batches(query, chunk_size=chunk_size)

        def batches():
            with _overflow_refusal():
                yield from reader

        return pa.RecordBatchReader.from_batches(reader.schema, batches())

    def close(self) -> None:
        """Roll back and end the session; cleanup failures never mask a result."""
        for statement in ("ROLLBACK TRANSACTION", "CALL BQ.ABORT_SESSION()"):
            try:
                self.client.query(statement).result()
            except Exception:  # the session also expires server-side
                continue
        self.client.close()
        self.storage.transport.close()


@contextmanager
def _overflow_refusal() -> Generator[None]:
    """BigQuery reports BIGNUMERIC overflow as a query error; refuse with the code."""
    from terrine.backends.numeric import EXACT_OVERFLOW

    try:
        yield
    except Exception as exc:
        if "overflow" in str(exc).casefold():
            raise EXACT_OVERFLOW.error() from exc
        raise
