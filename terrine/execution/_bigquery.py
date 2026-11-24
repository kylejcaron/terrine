"""BigQuery relation identity, admission and private snapshot sessions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import pyarrow as pa

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE, _SNAPSHOT_UNAVAILABLE
from terrine.backends.registry import ENGINES
from terrine.errors import TerrineError
from terrine.execution._metadata import invalidate_metadata, metadata_fact, metadata_operation
from terrine.semantic.models import BoundSource, Catalog, RelationSpec, TableRef
from terrine.semantic.sql import _admit_read_query_ast, _scopes, _verified, admit_read_query

if TYPE_CHECKING:
    import ibis.expr.types as ir

    from terrine.execution.inputs import ArrowReader


DIALECT = "bigquery"
# Physical row sources; external tables are not snapshot-consistent.
_TABLE_TYPES = {"TABLE": "table", "SNAPSHOT": "table", "VIEW": "view"}
type _QualifiedName = tuple[str, str, str]
type _Defaults = tuple[str, str]


def _snapshot_error(reason: str):
    return _SNAPSHOT_UNAVAILABLE.error(reason=reason)


def client_of(con: Any) -> Any:
    client = getattr(con, "client", None)
    if client is None or type(client).__module__.split(".")[:3] != ["google", "cloud", "bigquery"]:
        raise _snapshot_error("connection is not a google-cloud-bigquery-backed Ibis backend")
    return client


@dataclass(frozen=True, slots=True)
class _TimePartition:
    field: str | None
    grain: str | None
    field_type: str | None


@dataclass(frozen=True, slots=True)
class _Relation:
    project: str
    dataset: str
    name: str
    kind: str
    created: str
    definition_hash: str
    view_query: str | None
    partition: _TimePartition | None

    @property
    def identity(self) -> str:
        return json.dumps([self.project, self.dataset, self.name])


def _defaults(con: Any) -> _Defaults:
    """The connection's data project and default dataset (not its billing project)."""
    return con.data_project, con.dataset


def _reference(defaults: _Defaults, name: str) -> _QualifiedName:
    parts = name.split(".")
    project, dataset = defaults
    if len(parts) == 1:
        return project, dataset, parts[0]
    if len(parts) == 2:
        return project, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise _snapshot_error(f"relation name {name!r} is not project.dataset.table")


@metadata_fact
def _relation(client: Any, project: str, dataset: str, name: str) -> _Relation:
    try:
        table = client.get_table(f"{project}.{dataset}.{name}")
    except Exception as exc:
        raise _snapshot_error(f"relation {name!r} is not visible to this connection") from exc
    kind = _TABLE_TYPES.get(table.table_type)
    if kind is None:
        raise _snapshot_error(f"relation {name!r} has unsupported type {table.table_type!r}")
    view_query = table.view_query
    definition = json.dumps(
        {
            "schema": [field.to_api_repr() for field in table.schema],
            "view": view_query,
        },
        sort_keys=True,
    )
    partition = None
    if (
        kind == "table"
        and (table.project, table.dataset_id, table.table_id) == (project, dataset, name)
        and table.external_data_configuration is None
        and (time_partition := table.time_partitioning) is not None
    ):
        field_type = next(
            (field.field_type for field in table.schema if field.name == time_partition.field),
            None,
        )
        partition = _TimePartition(time_partition.field, time_partition.type_, field_type)
    return _Relation(
        project,
        dataset,
        name,
        kind,
        table.created.isoformat() if table.created is not None else "",
        hashlib.sha256(definition.encode()).hexdigest(),
        view_query,
        partition,
    )


def _parsed(sql: str) -> Any:
    from sqlglot import parse_one

    try:
        return _verified(lambda: parse_one(sql, read=DIALECT))
    except TerrineError:
        raise
    except Exception as exc:
        raise _snapshot_error("view definition could not be parsed") from exc


def check_routines(sql: str) -> None:
    """Refuse a query that calls a dataset-qualified routine.

    Persistent UDFs and remote functions are always qualified, so this covers
    every user routine; unqualified names always resolve to BigQuery built-ins.
    """
    from sqlglot import exp

    for node in _parsed(sql).walk():
        qualified_call = isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func)
        # A fully backticked name such as `project.dataset.fn` parses as one identifier.
        quoted_path = isinstance(node, exp.Anonymous) and "." in node.name
        if qualified_call or quoted_path:
            raise _snapshot_error("query calls a user-defined routine")


def _tables_in(sql: str, defaults: _Defaults) -> list[_QualifiedName]:
    from sqlglot import exp

    statement = _admit_read_query_ast(sql, DIALECT, DIALECT)
    found = []
    for scope in _scopes(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                parts = [part for part in (source.catalog, source.db, source.name) if part]
                found.append(_reference(defaults, ".".join(parts)))
    return found


def _view_tables(sql: str, defaults: _Defaults) -> list[_QualifiedName]:
    from sqlglot import exp

    check_routines(sql)
    found = []
    for scope in _scopes(_parsed(sql)):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                parts = [part for part in (source.catalog, source.db, source.name) if part]
                found.append(_reference(defaults, ".".join(parts)))
    return found


@metadata_fact
def _bound_partitions(_client: Any) -> dict[_QualifiedName, tuple[_TimePartition | None, bool]]:
    # Evidence belongs to the resolver's current metadata operation, not the table expression.
    return {}


@metadata_fact
def _completed_closures(
    _client: Any, _defaults: _Defaults
) -> dict[_QualifiedName, frozenset[_Relation]]:
    # The operation owns this memo; entries are immutable, completed subtrees.
    return {}


def _closure(
    client: Any,
    defaults: _Defaults,
    key: _QualifiedName,
    completed: dict[_QualifiedName, frozenset[_Relation]],
    active: set[_QualifiedName],
) -> frozenset[_Relation]:
    if key in active:
        raise _snapshot_error("view dependency cycle")
    if key in completed:
        return completed[key]
    relation = _relation(client, *key)
    if relation.kind == "view":
        if relation.view_query is None:
            raise _snapshot_error("view definition could not be parsed")
        active.add(key)
        try:
            reachable = {relation}
            for dependency in _view_tables(relation.view_query, defaults):
                reachable.update(_closure(client, defaults, dependency, completed, active))
            result = frozenset(reachable)
        finally:
            active.remove(key)
    else:
        result = frozenset((relation,))
    completed[key] = result
    return result


@metadata_fact
def _closure_for(relation: RelationSpec, client: Any, defaults: _Defaults) -> frozenset[_Relation]:
    completed = _completed_closures(client, defaults)
    active: set[_QualifiedName] = set()
    if isinstance(relation, TableRef):
        return _closure(client, defaults, _reference(defaults, relation.name), completed, active)
    return frozenset(
        item
        for key in _tables_in(relation.sql, defaults)
        for item in _closure(client, defaults, key, completed, active)
    )


def roots(catalog: Catalog, units: Mapping[str, Any] | None = None) -> tuple[RelationSpec, ...]:
    relations: list[RelationSpec] = [source.relation for source in catalog.sources.values()]
    relations.extend(dimension.relation for dimension in catalog.dimensions.values())
    relations.extend(TableRef(kind="table", name=unit.table) for unit in (units or {}).values())
    return tuple(relations)


@metadata_operation()
def required_relations(
    catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
) -> tuple[Any, ...]:
    from terrine.execution.inputs import PhysicalRelationId

    client, defaults = client_of(con), _defaults(con)
    seen = {
        item
        for relation in roots(catalog, units)
        for item in _closure_for(relation, client, defaults)
    }
    return tuple(
        sorted(
            (
                PhysicalRelationId("table", item.project, item.dataset, item.name)
                for item in seen
                if item.kind == "table"
            ),
            key=lambda item: (item.catalog or "", item.schema or "", item.name),
        )
    )


def admission_fingerprint(relation: RelationSpec, con: Any) -> str:
    """Return the identity of a relation's closure; any change alters it."""
    return _fingerprint(relation, client_of(con), _defaults(con))


@metadata_fact
def _fingerprint(relation: RelationSpec, client: Any, defaults: _Defaults) -> str:
    payload = sorted(
        (item.identity, item.kind, item.created, item.definition_hash)
        for item in _closure_for(relation, client, defaults)
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def resolve(con: Any, relation: RelationSpec, catalog_dialect: str = DIALECT) -> Any:
    if isinstance(relation, TableRef):
        project, dataset, name = _reference(_defaults(con), relation.name)
        item = _relation(client_of(con), project, dataset, name)
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


def owned_session(con: Any) -> BigQuerySession:
    """Open a private client, storage reader and session holding one snapshot.

    The caller's client, and any session or job configuration it holds, is never
    used for terrine's reads. Both owned clients use the caller's credentials, and
    queries default to the caller's data project and dataset.
    """
    from google.cloud import bigquery
    from google.cloud.bigquery_storage_v1 import BigQueryReadClient

    caller = client_of(con)
    project, dataset = _defaults(con)
    client = storage = session_id = None
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
        session_id = job.session_info.session_id if job.session_info is not None else None
        if not session_id:
            raise _snapshot_error("BigQuery did not open a session for the snapshot transaction")
        client.default_query_job_config = bigquery.QueryJobConfig(
            default_dataset=f"{project}.{dataset}" if dataset else None,
            connection_properties=[bigquery.ConnectionProperty("session_id", session_id)],
        )
        return BigQuerySession(client, storage, session_id)
    except BaseException as exc:
        _release_session(client, storage, session_id)
        if isinstance(exc, Exception) and not isinstance(exc, TerrineError):
            raise _snapshot_error("could not open a private BigQuery session") from exc
        raise


def _release_session(client, storage, session_id):
    """Best-effort cleanup, including failures before the private defaults were installed."""
    if session_id:
        from google.cloud import bigquery

        for statement in ("ROLLBACK TRANSACTION", "CALL BQ.ABORT_SESSION()"):
            with suppress(BaseException):
                config = bigquery.QueryJobConfig(
                    connection_properties=[bigquery.ConnectionProperty("session_id", session_id)]
                )
                client.query(statement, job_config=config).result()
    if client is not None:
        with suppress(BaseException):
            client.close()
    if storage is not None:
        with suppress(BaseException):
            storage.transport.close()


def _submit_arrow(con: Any, query: Any, sql: str) -> tuple[Any, Any]:
    """Run Ibis's Arrow preparation, then submit the already admitted SQL."""
    con._import_pyarrow()
    table = query.as_table()
    schema = table.schema() - ibis.schema({"_TABLE_SUFFIX": "string"})
    con._run_pre_execute_hooks(table)
    return con.raw_sql(sql), schema


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
        from ibis.backends.bigquery import _postprocess_arrow

        invalidate_metadata()
        with _overflow_refusal():
            result, schema = _submit_arrow(con, query, sql)
            table = result.to_arrow(progress_bar_type=None, bqstorage_client=self.storage)
            table = _postprocess_arrow(table, list(schema.names))
            return query.__pyarrow_result__(table, schema=schema)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> ArrowReader:
        from ibis.backends.bigquery import _postprocess_arrow

        from terrine.execution.inputs import _ClosingArrowReader

        invalidate_metadata()
        with _overflow_refusal():
            result, schema = _submit_arrow(con, query, sql)
        names = list(schema.names)

        def batches() -> Generator[pa.RecordBatch]:
            with _overflow_refusal():
                # Google controls storage batch sizes. Open the download lazily
                # so closing an unread result has no download to release.
                source = result.to_arrow_iterable(bqstorage_client=self.storage)
                try:
                    for batch in source:
                        yield cast(pa.RecordBatch, _postprocess_arrow(batch, names))
                finally:
                    if (close := getattr(source, "close", None)) is not None:
                        close()

        source = batches()
        reader = pa.RecordBatchReader.from_batches(schema.to_pyarrow(), source)
        return _ClosingArrowReader(reader, source)

    def close(self) -> None:
        """Roll back and end the session; cleanup failures never mask a result."""
        _release_session(self.client, self.storage, self.session_id)


@contextmanager
def _overflow_refusal() -> Generator[None]:
    """Map type-labelled NUMERIC/BIGNUMERIC overflow, preserving native failures."""
    from terrine.backends.numeric import EXACT_OVERFLOW

    try:
        yield
    except Exception as exc:
        if "numeric overflow" in str(exc).casefold():
            raise EXACT_OVERFLOW.error() from exc
        raise


class _BigQueryProvider:
    capabilities = ENGINES["bigquery"].capabilities

    def matches(self, con: Any) -> bool:
        return type(con).__module__ == ENGINES["bigquery"].ibis_module

    def dialect(self, con: Any) -> str:
        return ENGINES["bigquery"].name

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _bigquery

        return _bigquery.resolve(con, relation)

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution import _bigquery

        return _bigquery.required_relations(catalog, con, units=units)

    def event_partition(
        self, con: Any, source: BoundSource, timestamp: ir.TimestampValue
    ) -> tuple[str | None, str | None]:
        if not isinstance(source.relation, TableRef):
            return None, "layout_unverified"
        key = _reference(_defaults(con), source.relation.name)
        layout, changed = _bound_partitions(client_of(con)).get(key, (None, False))
        if changed:
            return None, "layout_changed"
        if layout is None or layout.grain not in {"HOUR", "DAY", "MONTH", "YEAR"}:
            return None, "layout_unverified"
        if layout.field is None:
            return None, "ingestion_time_partition"
        if layout.field != source.time_column:
            return None, "event_lineage_unverified"
        dtype = timestamp.type()
        if not isinstance(dtype, dt.Timestamp) or dtype.timezone != "UTC":
            return None, "event_lineage_unverified"
        tables = timestamp.op().find(ops.DatabaseTable)
        if len(tables) == 1:
            (table,) = tables
            if (
                isinstance(table, ops.DatabaseTable)
                and table.source is con
                and (table.namespace.catalog, table.namespace.database, table.name) == key
                and layout.field_type == "TIMESTAMP"
            ):
                return layout.field, None
        return None, "event_lineage_unverified"

    @metadata_operation()
    def bind_resolver(self, catalog, con, **kwargs):
        from terrine.execution import _bigquery
        from terrine.execution.inputs import _INVALID

        if kwargs.get("input_binding") is not None:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="bigquery",
                operation="retained_snapshot",
                status="unverified",
                route="Retained input bindings are DuckDB-only; materialize directly.",
            )
        requested = roots(catalog, kwargs.get("units"))
        study_roots = kwargs.get("_study_roots")
        if study_roots is not None and (scope := study_roots()) is not None:
            selected = frozenset(scope)
            requested = tuple(relation for relation in requested if relation in selected)
        fingerprints = {
            relation: _bigquery.admission_fingerprint(relation, con) for relation in requested
        }
        client, defaults = client_of(con), _defaults(con)
        original_partitions = {
            key: _relation(client, *key).partition
            for relation in requested
            if isinstance(relation, TableRef)
            for key in (_reference(defaults, relation.name),)
        }
        owner = kwargs.get("_partition_owner")
        owner_client = client_of(owner) if owner is not None else None
        admitted: dict[RelationSpec, Any] = {}

        @metadata_operation()
        def resolve(relation: RelationSpec) -> Any:
            fingerprint = _bigquery.admission_fingerprint(relation, con)
            original = fingerprints.get(relation)
            if original is not None and original != fingerprint:
                raise _INVALID.error(
                    reason="relation definition or identity changed after Model binding"
                )
            if relation not in admitted:
                admitted[relation] = _bigquery.resolve(con, relation, catalog.dialect)
                fingerprints[relation] = fingerprint
            if isinstance(relation, TableRef):
                key = _reference(defaults, relation.name)
                layout = _relation(client, *key).partition
                changed = layout != original_partitions.setdefault(key, layout)
                if owner_client is not None:
                    changed |= _bound_partitions(owner_client).get(key) != (layout, False)
                _bound_partitions(client)[key] = (layout, changed)
            return admitted[relation]

        return resolve

    @contextmanager
    def open_execution(self, catalog, con, *, units=None, input_binding=None, _study_roots=None):
        from terrine.execution.inputs import _open_bigquery_execution

        with (
            metadata_operation(),
            _open_bigquery_execution(
                self,
                catalog,
                con,
                units=units,
                input_binding=input_binding,
                study_roots=_study_roots,
            ) as context,
        ):
            yield context


PROVIDER = _BigQueryProvider()
