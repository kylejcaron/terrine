"""Native execution authority, exact input bindings, and owned leases."""

from __future__ import annotations

import contextlib
import hashlib
import json
import threading
import uuid
import weakref
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol

import ibis
import pyarrow as pa

from terrine.data import Budget, ExecutionSnapshot
from terrine.errors import RefusalSpec
from terrine.execution import _native
from terrine.execution._metadata import invalidate_metadata, metadata_fact, metadata_operation
from terrine.semantic.models import Catalog, RelationSpec, TableRef
from terrine.semantic.sql import admit_read_query

__all__ = [
    "PhysicalRelationId",
    "RelationHandle",
    "VerifiedInputBinding",
    "PublishedInputs",
    "required_relations",
    "verify_immutable_inputs",
    "publish_native_inputs",
    "open_execution",
    "bind_resolver",
    "ExecutionContext",
]

_INVALID = RefusalSpec(
    "terrine.execution.invalid_input_binding",
    "Execution inputs are not an exact verified binding: {reason}.",
)

_SNAPSHOT_UNAVAILABLE = RefusalSpec(
    "terrine.execution.snapshot_unavailable",
    "A coherent native execution snapshot is unavailable: {reason}.",
)


@dataclass(frozen=True, slots=True)
class PhysicalRelationId:
    kind: Literal["table", "sql_result"]
    catalog: str | None
    schema: str | None
    name: str
    definition_hash: str | None = None


class RelationHandle:
    __slots__ = ("_issuer_ref", "_key")

    def __init__(self, issuer: _Issuer, key: PhysicalRelationId) -> None:
        object.__setattr__(self, "_issuer_ref", weakref.ref(issuer))
        object.__setattr__(self, "_key", key)

    @property
    def _issuer(self) -> _Issuer | None:
        return self._issuer_ref()

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("relation handles are immutable")

    def __reduce__(self):
        raise TypeError("relation handles are live runtime authority and cannot be serialized")

    def __repr__(self) -> str:
        return "<RelationHandle opaque>"


class VerifiedInputBinding:
    __slots__ = ("_issuer", "_catalog_hash", "_units_hash", "_relations", "_con_identity")

    def __init__(
        self,
        issuer: _Issuer,
        catalog_hash: str,
        units_hash: str,
        relations: Mapping[PhysicalRelationId, RelationHandle],
        con: Any,
    ) -> None:
        object.__setattr__(self, "_issuer", issuer)
        object.__setattr__(self, "_catalog_hash", catalog_hash)
        object.__setattr__(self, "_units_hash", units_hash)
        object.__setattr__(self, "_relations", MappingProxyType(dict(relations)))
        object.__setattr__(self, "_con_identity", id(con))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("verified input bindings are immutable")

    def __reduce__(self):
        raise TypeError("verified bindings cannot be deserialized into execution authority")


@dataclass(frozen=True, slots=True)
class _BackingFact:
    catalog: str
    schema: str
    table: str
    columns: tuple[str, ...]
    oid: int


class _Issuer:
    __slots__ = (
        "token",
        "con",
        "raw",
        "closed",
        "leases",
        "lock",
        "cleanup",
        "facts",
        "root_names",
        "root_identities",
        "handles",
        "catalog_hash",
        "units_hash",
        "namespace",
        "function_fingerprint",
        "__weakref__",
    )

    def __init__(self, con: Any, raw: Any, cleanup: Callable[[], None] | None = None) -> None:
        self.token = uuid.uuid4().hex
        self.con = con
        self.raw = raw
        self.closed = False
        self.leases = 0
        self.lock = threading.RLock()
        self.cleanup = cleanup
        self.facts: dict[PhysicalRelationId, _BackingFact] = {}
        self.root_names: dict[tuple[str, str], str] = {}
        self.root_identities: dict[tuple[str, str], PhysicalRelationId] = {}
        self.handles: dict[PhysicalRelationId, RelationHandle] = {}
        self.catalog_hash: str | None = None
        self.units_hash: str | None = None
        self.namespace: tuple[str, str] | None = None
        self.function_fingerprint: str | None = None

    def acquire(self) -> None:
        with self.lock:
            if self.closed:
                raise _INVALID.error(reason="publication owner is closed")
            self.leases += 1

    def release(self) -> None:
        cleanup = None
        with self.lock:
            self.leases -= 1
            if self.closed and self.leases == 0:
                cleanup, self.cleanup = self.cleanup, None
        if cleanup:
            cleanup()

    def close(self) -> None:
        cleanup = None
        with self.lock:
            self.closed = True
            if self.leases == 0:
                cleanup, self.cleanup = self.cleanup, None
        if cleanup:
            cleanup()


class PublishedInputs:
    __slots__ = ("con", "relations", "_issuer")

    def __init__(
        self, con: Any, relations: Mapping[PhysicalRelationId, RelationHandle], issuer: _Issuer
    ) -> None:
        object.__setattr__(self, "con", con)
        object.__setattr__(self, "relations", MappingProxyType(dict(relations)))
        object.__setattr__(self, "_issuer", issuer)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("published inputs are immutable")

    def close(self) -> None:
        self._issuer.close()

    def __enter__(self) -> PublishedInputs:
        if self._issuer.closed:
            raise _INVALID.error(reason="publication owner is closed")
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _catalog_hash(catalog: Catalog) -> str:
    return _hash(catalog.model_dump(mode="json"))


def _units_hash(units: Mapping[str, Any] | None) -> str:
    return _hash(
        {
            key: vars(value)
            if hasattr(value, "__dict__")
            else [
                value.table,
                value.key,
                value.entered_at,
                value.exited_at,
                value.observed_from,
                value.observed_until,
            ]
            for key, value in sorted((units or {}).items())
        }
    )


_PUBLISHED_ISSUERS: dict[int, weakref.ReferenceType[_Issuer]] = {}
_PUBLISHED_ISSUERS_LOCK = threading.RLock()


def _register_published_backend(con: Any, issuer: _Issuer) -> None:
    key = id(con)

    def discard(reference: weakref.ReferenceType[_Issuer]) -> None:
        with _PUBLISHED_ISSUERS_LOCK:
            if _PUBLISHED_ISSUERS.get(key) is reference:
                _PUBLISHED_ISSUERS.pop(key, None)

    with _PUBLISHED_ISSUERS_LOCK:
        _PUBLISHED_ISSUERS[key] = weakref.ref(issuer, discard)


def _published_issuer(con: Any) -> _Issuer | None:
    with _PUBLISHED_ISSUERS_LOCK:
        reference = _PUBLISHED_ISSUERS.get(id(con))
        issuer = reference() if reference is not None else None
        return issuer if issuer is not None and issuer.con is con else None


def _check_published_namespace(issuer: _Issuer) -> None:
    if issuer.namespace is not None and _native._current_namespace(issuer.raw) != issuer.namespace:
        raise _INVALID.error(reason="published namespace changed")
    if (
        issuer.function_fingerprint is not None
        and _native.function_fingerprint(issuer.raw) != issuer.function_fingerprint
    ):
        raise _INVALID.error(reason="published function namespace changed")


def _check_fact_schema(raw: Any, fact: _BackingFact) -> None:
    qualified = ".".join(
        _native._quote_ident(value) for value in (fact.catalog, fact.schema, fact.table)
    )
    try:
        with _native.connection_lock(raw):
            raw.execute(f"SELECT * FROM {qualified} LIMIT 0")
            columns = tuple(column[0] for column in raw.description)
    except Exception as exc:
        raise _INVALID.error(reason="published backing relation no longer exists") from exc
    if columns != fact.columns:
        raise _INVALID.error(reason="published backing schema changed")


def _required_native_relations(
    catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
) -> tuple[PhysicalRelationId, ...]:
    issuer = _published_issuer(con)
    if issuer is None:
        return _native.required_relations(catalog, con, units=units)
    with issuer.lock:
        if issuer.closed:
            raise _INVALID.error(reason="publication owner is closed")
        if issuer.catalog_hash != _catalog_hash(catalog) or issuer.units_hash != _units_hash(units):
            raise _INVALID.error(reason="Catalog or roster declaration changed")
        _revalidate_backing(issuer, con)
        return tuple(issuer.facts)


@metadata_operation()
def verify_immutable_inputs(
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    relations: Mapping[PhysicalRelationId, RelationHandle],
) -> VerifiedInputBinding:
    issuer = None
    for identity, handle in relations.items():
        if (
            not isinstance(handle, RelationHandle)
            or not isinstance(handle._issuer, _Issuer)
            or handle._key != identity
        ):
            raise _INVALID.error(reason="foreign or forged relation handle")
        if issuer is None:
            issuer = handle._issuer
        if handle._issuer is not issuer:
            raise _INVALID.error(reason="handles come from different publication owners")
        if issuer.handles.get(identity) is not handle:
            raise _INVALID.error(reason="handle is not registered by its issuing owner")
    if issuer is None:
        raise _INVALID.error(
            reason="publication is closed, empty, or belongs to another connection"
        )
    with issuer.lock:
        if issuer.closed or issuer.con is not con:
            raise _INVALID.error(
                reason="publication is closed, empty, or belongs to another connection"
            )
        if set(relations) != set(issuer.facts):
            raise _INVALID.error(reason="relation membership is missing or contains extra handles")
        if issuer.catalog_hash != _catalog_hash(catalog) or issuer.units_hash != _units_hash(units):
            raise _INVALID.error(reason="Catalog or roster declaration changed")
        _check_published_namespace(issuer)
        objects = _native.native_objects(issuer.raw)
        for fact in issuer.facts.values():
            if not any(
                obj.identity
                == tuple(
                    _native._ascii_key(value) for value in (fact.catalog, fact.schema, fact.table)
                )
                and obj.kind == "table"
                and obj.oid == fact.oid
                for obj in objects
            ):
                raise _INVALID.error(reason="published backing identity or generation changed")
            _check_fact_schema(issuer.raw, fact)
        return VerifiedInputBinding(
            issuer, _catalog_hash(catalog), _units_hash(units), relations, con
        )


def _resolve_relation(
    con: Any,
    relation: RelationSpec,
    catalog: Catalog,
    admitted: dict[tuple[str, str], Any],
    fingerprints: dict[tuple[str, str], str],
) -> Any:
    key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
    raw = _native.raw_connection(con)
    with _native.connection_lock(raw):
        fingerprint = _native.admission_fingerprint(catalog, relation, raw)
        if key in admitted:
            if fingerprints[key] != fingerprint:
                raise _INVALID.error(
                    reason="native relation definition or identity changed after Model binding"
                )
            return admitted[key]
        if isinstance(relation, TableRef):
            table = con.table(relation.name)
        else:
            admit_read_query(relation.sql, catalog.dialect, str(getattr(con, "name", "duckdb")))
            table = con.sql(relation.sql)
        admitted[key] = table
        fingerprints[key] = fingerprint
        return table


def _bind_native_resolver(
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> Callable[[RelationSpec], Any]:
    admitted: dict[tuple[str, str], Any] = {}
    fingerprints: dict[tuple[str, str], str] = {}
    if input_binding is not None:
        issuer = input_binding._issuer
        _validate_binding(input_binding, catalog, con, units)

        def resolve_published(relation: RelationSpec) -> Any:
            with issuer.lock, _native.connection_lock(issuer.raw):
                if issuer.closed and issuer.leases == 0:
                    raise _INVALID.error(reason="publication owner is closed")
                key = (
                    relation.kind,
                    relation.name if isinstance(relation, TableRef) else relation.sql,
                )
                storage_name = issuer.root_names.get(key)
                if storage_name is None:
                    raise _INVALID.error(reason="root has no issued stored-result substitution")
                return con.table(
                    storage_name,
                    database=issuer.namespace,
                )

        return resolve_published
    if _published_issuer(con) is not None:
        raise _INVALID.error(reason="issued publication requires its exact input binding")
    _native.required_relations(catalog, con, units=units)
    _native.raw_connection(con)

    def resolve(relation: RelationSpec) -> Any:
        return _resolve_relation(con, relation, catalog, admitted, fingerprints)

    return resolve


@metadata_fact
def _revalidate_backing(issuer: _Issuer, con: Any) -> None:
    _check_published_namespace(issuer)
    objects = _native.native_objects(issuer.raw)
    for fact in issuer.facts.values():
        if not any(
            obj.identity
            == tuple(_native._ascii_key(value) for value in (fact.catalog, fact.schema, fact.table))
            and obj.kind == "table"
            and obj.oid == fact.oid
            for obj in objects
        ):
            raise _INVALID.error(reason="published backing identity or generation changed")
    runtime = _native.raw_connection(con)
    if (
        runtime is not issuer.raw
        and issuer.namespace is not None
        and _native._current_namespace(runtime)[0] != issuer.namespace[0]
    ):
        raise _INVALID.error(reason="execution database differs from the issued backing")
    for fact in issuer.facts.values():
        _check_fact_schema(runtime, fact)


def _validate_binding(
    binding: VerifiedInputBinding,
    catalog: Catalog,
    con: Any,
    units: Mapping[str, Any] | None,
) -> None:
    issuer = binding._issuer
    with issuer.lock:
        if issuer.closed or issuer.con is not con:
            raise _INVALID.error(reason="owner is closed or execution connection changed")
        if binding._catalog_hash != _catalog_hash(catalog) or binding._units_hash != _units_hash(
            units
        ):
            raise _INVALID.error(reason="Catalog or roster declaration changed")
        _revalidate_backing(issuer, con)


def _execution_snapshot(
    catalog: Catalog,
    units: Mapping[str, Any] | None = None,
    binding: VerifiedInputBinding | None = None,
    *,
    backend: str = "duckdb",
) -> ExecutionSnapshot:
    generations = {
        json.dumps([kind, name], ensure_ascii=False, separators=(",", ":")): None
        for kind, names in (
            ("source", catalog.sources),
            ("dimension", catalog.dimensions),
            ("roster", units or {}),
        )
        for name in names
    }
    if binding is not None:
        for key in generations:
            generations[key] = binding._issuer.token
    return ExecutionSnapshot(
        snapshot_id=None,
        backend=backend,
        source_generations=generations,
        input_binding_id=binding._issuer.token if binding is not None else None,
        replay_equivalence="unverified",
    )


def _validate_leased_binding(
    binding: VerifiedInputBinding,
    catalog: Catalog,
    owner_con: Any,
    runtime_con: Any,
    units: Mapping[str, Any] | None,
) -> None:
    issuer = binding._issuer
    with issuer.lock:
        if issuer.con is not owner_con:
            raise _INVALID.error(reason="execution connection changed")
        if binding._catalog_hash != _catalog_hash(catalog) or binding._units_hash != _units_hash(
            units
        ):
            raise _INVALID.error(reason="Catalog or roster declaration changed")
        _revalidate_backing(issuer, runtime_con)


class ExecutionSession(Protocol):
    """Backend-owned query admission, execution and transaction lifetime."""

    backend: str

    def check_query(self, sql: str) -> None: ...
    def execute(self, con: Any, query: Any, sql: str) -> pa.Table: ...
    def batches(
        self, con: Any, query: Any, sql: str, *, chunk_size: int
    ) -> pa.RecordBatchReader: ...
    def close(self) -> None: ...


class _DuckDBSession:
    """A private DuckDB cursor holding one owned transaction."""

    backend = "duckdb"

    def __init__(self, raw: Any) -> None:
        self.raw = raw

    def check_query(self, sql: str) -> None:
        _native.check_native_functions(self.raw, sql, None)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        from ibis.backends.duckdb.converter import DuckDBPyArrowData

        try:
            con._run_pre_execute_hooks(query)
            if query.as_table().schema().geospatial:
                con._load_extensions(["spatial"])
        finally:
            invalidate_metadata()
        table = self.raw.sql(sql).to_arrow_table()
        return query.__pyarrow_result__(table, data_mapper=DuckDBPyArrowData)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> pa.RecordBatchReader:
        import pyarrow_hotfix  # noqa: F401

        try:
            con._run_pre_execute_hooks(query)
        finally:
            invalidate_metadata()
        result = con.raw_sql(sql)

        def batches():
            yield from result.fetch_record_batch(rows_per_batch=chunk_size)

        return pa.RecordBatchReader.from_batches(query.as_table().schema().to_pyarrow(), batches())

    def close(self) -> None:
        try:
            self.raw.execute("COMMIT")
        except Exception:
            try:
                self.raw.execute("ROLLBACK")
            except Exception:
                pass
        finally:
            self.raw.close()


@dataclass(slots=True)
class ExecutionContext:
    con: Any
    snapshot: ExecutionSnapshot
    _resolver: Callable[[RelationSpec], Any]
    _session: ExecutionSession | None = None
    owner_con: Any = None
    _revalidate: Callable[[], None] | None = None

    def resolve(self, relation: RelationSpec) -> Any:
        return self._resolver(relation)

    @metadata_operation()
    def _check_query(self, query: Any) -> tuple[ExecutionSession, str]:
        if self._revalidate is not None:
            self._revalidate()
        session = self._session
        if session is None:
            raise _SNAPSHOT_UNAVAILABLE.error(reason="execution context is closed")
        try:
            sql = self.con.compile(query)
        except Exception as exc:
            raise _SNAPSHOT_UNAVAILABLE.error(
                reason="compiled query could not be inspected"
            ) from exc
        session.check_query(sql)
        return session, sql

    def execute(self, query: Any) -> pa.Table:
        session, sql = self._check_query(query)
        return session.execute(self.con, query, sql)

    def to_arrow_batches(self, query: Any, *, chunk_size: int = 65_536) -> pa.RecordBatchReader:
        """Stream the same SQL whose native function ownership was admitted."""
        session, sql = self._check_query(query)
        return session.batches(self.con, query, sql, chunk_size=chunk_size)

    def close(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            session.close()


@contextlib.contextmanager
def _open_native_execution(
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> Generator[ExecutionContext]:
    if input_binding is not None:
        _validate_binding(input_binding, catalog, con, units)
        issuer = input_binding._issuer
        issuer.acquire()
        raw = None
        context = None
        try:
            with issuer.lock, _native.connection_lock(issuer.raw):
                raw = issuer.raw.cursor()
                private = ibis.duckdb.from_connection(raw)
                _native.restore_semantic_settings(issuer.raw, raw)
                _native.begin_owned_transaction(raw)

            admitted: dict[str, Any] = {}

            def validate() -> None:
                _validate_leased_binding(input_binding, catalog, con, private, units)

            def resolve_published(relation: RelationSpec) -> Any:
                validate()
                key = (
                    relation.kind,
                    relation.name if isinstance(relation, TableRef) else relation.sql,
                )
                storage_name = issuer.root_names.get(key)
                if storage_name is None:
                    raise _INVALID.error(reason="root has no issued stored-result substitution")
                if storage_name not in admitted:
                    # Fully qualified backing tables are read-only; keep schemas only
                    # for this execution, after validating the live binding above.
                    admitted[storage_name] = private.table(
                        storage_name,
                        database=issuer.namespace,
                    )
                return admitted[storage_name]

            resolver = resolve_published
            validate()
            context = ExecutionContext(
                private,
                _execution_snapshot(catalog, units, binding=input_binding),
                resolver,
                _session=_DuckDBSession(raw),
                _revalidate=validate,
                owner_con=con,
            )
            yield context
        finally:
            if context is not None:
                context.close()
            elif raw is not None:
                raw.close()
            issuer.release()
        return
    origin = _native.raw_connection(con)
    raw = None
    context = None
    try:
        with _native.connection_lock(origin):
            _native.verify_native_backend(con)
            raw = origin.cursor()
            private = ibis.duckdb.from_connection(raw)
            _native.restore_semantic_settings(origin, raw)
            _native.begin_owned_transaction(raw)
        resolver = bind_resolver(catalog, private, units=units)
        context = ExecutionContext(
            private,
            _execution_snapshot(catalog, units),
            resolver,
            _session=_DuckDBSession(raw),
            owner_con=con,
        )
        yield context
    finally:
        if context is not None:
            context.close()
        elif raw is not None:
            raw.close()


@contextlib.contextmanager
def _open_postgres_execution(
    provider: Any,
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> Generator[ExecutionContext]:
    """Read through a private connection so the caller's transaction is never touched."""
    from terrine.execution import _postgres

    owned = _postgres.owned_connection(con)
    session = _postgres.PostgresSession(owned)
    context = None
    try:
        private = ibis.postgres.from_connection(owned)
        resolver = provider.bind_resolver(
            catalog, private, units=units, input_binding=input_binding
        )
        context = ExecutionContext(
            private,
            _execution_snapshot(catalog, units, backend="postgres"),
            resolver,
            _session=session,
            owner_con=con,
        )
        yield context
    finally:
        if context is not None:
            context.close()
        else:
            session.close()


@contextlib.contextmanager
def _open_bigquery_execution(
    provider: Any,
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> Generator[ExecutionContext]:
    """Read in a private session transaction so the caller's client is never touched."""
    from terrine.execution import _bigquery

    client, session_id = _bigquery.owned_session(con)
    session = _bigquery.BigQuerySession(client, session_id)
    context = None
    try:
        private = ibis.bigquery.from_connection(client, dataset_id=con.current_database)
        resolver = provider.bind_resolver(
            catalog, private, units=units, input_binding=input_binding
        )
        context = ExecutionContext(
            private,
            _execution_snapshot(catalog, units, backend="bigquery"),
            resolver,
            _session=session,
            owner_con=con,
        )
        yield context
    finally:
        if context is not None:
            context.close()
        else:
            session.close()


def required_relations(
    catalog: Catalog, con: Any, *, units: Mapping[str, Any] | None = None
) -> tuple[PhysicalRelationId, ...]:
    from terrine.execution.backend import backend_provider

    return backend_provider(con).required_relations(catalog, con, units=units)


def bind_resolver(
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> Callable[[RelationSpec], Any]:
    from terrine.execution.backend import backend_provider

    return backend_provider(con).bind_resolver(
        catalog, con, units=units, input_binding=input_binding
    )


def open_execution(
    catalog: Catalog,
    con: Any,
    *,
    units: Mapping[str, Any] | None = None,
    input_binding: VerifiedInputBinding | None = None,
) -> contextlib.AbstractContextManager[ExecutionContext]:
    from terrine.execution.backend import backend_provider

    return backend_provider(con).open_execution(
        catalog, con, units=units, input_binding=input_binding
    )


def publish_native_inputs(
    catalog: Catalog,
    *,
    con: Any,
    units: Mapping[str, Any] | None = None,
    budget: Budget | None = None,
) -> PublishedInputs:
    """Capture native roots, optionally refusing unknown/over-budget scans before reading."""
    from terrine.execution._publish import publish

    return publish(catalog, con=con, units=units, budget=budget)


def _capture_native_inputs(
    catalog: Catalog,
    *,
    con: Any,
    units: Mapping[str, Any] | None,
    budget: Budget,
    retained_path: Path,
    source_names: frozenset[str] | None = None,
    dimension_names: frozenset[str] | None = None,
    unit_names: frozenset[str] | None = None,
) -> PublishedInputs:
    from terrine.execution._publish import publish

    return publish(
        catalog,
        con=con,
        units=units,
        budget=budget,
        retained_path=retained_path,
        source_names=source_names,
        dimension_names=dimension_names,
        unit_names=unit_names,
    )


def _reopen_native_inputs(
    catalog: Catalog,
    *,
    units: Mapping[str, Any] | None,
    path: Path,
    roots: tuple[dict[str, Any], ...],
) -> PublishedInputs:
    import duckdb

    raw = duckdb.connect(str(path), read_only=True)
    managed = ibis.duckdb.from_connection(raw)
    issuer = _Issuer(managed, raw)
    issuer.catalog_hash = _catalog_hash(catalog)
    issuer.units_hash = _units_hash(units)
    issuer.namespace = _native._current_namespace(raw)
    issuer.function_fingerprint = _native.function_fingerprint(raw)
    objects = _native.native_objects(raw)
    try:
        for entry in roots:
            identity = PhysicalRelationId(**entry["identity"])
            storage = entry["storage_name"]
            key = (entry["kind"], entry["relation_key"])
            matches = [
                obj
                for obj in objects
                if obj.kind == "table" and obj.name.casefold() == storage.casefold()
            ]
            if len(matches) != 1:
                raise _SNAPSHOT_UNAVAILABLE.error(
                    reason="retained snapshot table identity is not unique"
                )
            obj = matches[0]
            qualified = ".".join(
                _native._quote_ident(value) for value in (obj.catalog, obj.schema, obj.name)
            )
            raw.execute(f"SELECT * FROM {qualified} LIMIT 0")
            columns = tuple(column[0] for column in raw.description)
            issuer.facts[identity] = _BackingFact(
                obj.catalog, obj.schema, obj.name, columns, obj.oid
            )
            issuer.handles[identity] = RelationHandle(issuer, identity)
            issuer.root_names[key] = storage
            issuer.root_identities[key] = identity
        issuer.cleanup = raw.close
        _register_published_backend(managed, issuer)
        return PublishedInputs(managed, issuer.handles, issuer)
    except Exception:
        raw.close()
        raise
