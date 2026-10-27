"""DuckDB metadata admission and private transaction support."""

from __future__ import annotations

import hashlib
import json
import threading
import weakref
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import ibis
from sqlglot import exp

from terrine.backends.capabilities import _SNAPSHOT_UNAVAILABLE
from terrine.backends.registry import ENGINES
from terrine.errors import TerrineError
from terrine.execution._metadata import invalidate_metadata, metadata_fact
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import (
    _CAPTURE_READERS,
    _admit_capture_result_query_ast,
    _admit_capture_stored_view_query_ast,
    _admit_read_query_ast,
    _admit_stored_view_query_ast,
    _render,
    _scopes,
    admit_read_query,
)

_CONNECTION_LOCKS: weakref.WeakKeyDictionary[Any, threading.RLock] = weakref.WeakKeyDictionary()
_CONNECTION_LOCKS_GUARD = threading.Lock()


def connection_lock(raw: Any) -> threading.RLock:
    """Coordinate Terrine operations without extending connection lifetime."""
    with _CONNECTION_LOCKS_GUARD:
        lock = _CONNECTION_LOCKS.get(raw)
        if lock is None:
            lock = threading.RLock()
            _CONNECTION_LOCKS[raw] = lock
        return lock


def _snapshot_error(reason: str):
    return _SNAPSHOT_UNAVAILABLE.error(reason=reason)


def _binding_error(reason: str):
    return _snapshot_error(reason)


@dataclass(frozen=True, slots=True)
class NativeObject:
    catalog: str
    schema: str
    name: str
    kind: str
    oid: int
    definition: str | None
    temporary: bool
    internal: bool

    @property
    def identity(self) -> tuple[str, str, str]:
        return (_ascii_key(self.catalog), _ascii_key(self.schema), _ascii_key(self.name))

    @property
    def definition_hash(self) -> str | None:
        if self.definition is None:
            return None
        return hashlib.sha256(self.definition.encode()).hexdigest()


def raw_connection(con: Any) -> Any:
    """Get a native DuckDB connection only from a real backend/connection."""
    try:
        import duckdb
    except ImportError as exc:
        raise _snapshot_error("DuckDB is not installed") from exc
    candidates = (getattr(con, "con", None), getattr(con, "_con", None), con)
    raw = next(
        (candidate for candidate in candidates if isinstance(candidate, duckdb.DuckDBPyConnection)),
        None,
    )
    if raw is None:
        raise _snapshot_error("connection is not a DuckDB native backend")
    return raw


def _rows(raw: Any, sql: str) -> list[tuple[Any, ...]]:
    try:
        with connection_lock(raw):
            return raw.execute(sql).fetchall()
    except Exception as exc:
        raise _snapshot_error("verified system metadata access failed") from exc


def _ascii_key(value: str) -> str:
    return value.translate(
        str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
    )


@metadata_fact
def native_objects(raw: Any) -> tuple[NativeObject, ...]:
    """Read all native tables/views, including internal and temporary contenders."""
    table_sql = (
        "SELECT database_name, schema_name, table_name, table_oid, sql, "
        "temporary, internal FROM system.main.duckdb_tables()"
    )
    view_sql = (
        "SELECT database_name, schema_name, view_name, view_oid, sql, "
        "temporary, internal FROM system.main.duckdb_views()"
    )
    objects = [
        NativeObject(
            str(db),
            str(schema),
            str(name),
            "table",
            int(oid),
            None if sql is None else str(sql),
            bool(temp),
            bool(internal),
        )
        for db, schema, name, oid, sql, temp, internal in _rows(raw, table_sql)
    ]
    objects.extend(
        NativeObject(
            str(db), str(schema), str(name), "view", int(oid), str(sql), bool(temp), bool(internal)
        )
        for db, schema, name, oid, sql, temp, internal in _rows(raw, view_sql)
    )
    return tuple(objects)


@metadata_fact
def _current_namespace(raw: Any) -> tuple[str, str]:
    row = _rows(raw, "SELECT system.main.current_database(), system.main.current_schema()")[0]
    return str(row[0]), str(row[1])


@metadata_fact
def _function_rows(raw: Any) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        _rows(
            raw,
            "SELECT function_name, internal, function_type, macro_definition "
            "FROM system.main.duckdb_functions()",
        )
    )


@metadata_fact
def function_fingerprint(raw: Any) -> str:
    payload = sorted(
        (str(name), bool(internal), str(kind), None if definition is None else str(definition))
        for name, internal, kind, definition in _function_rows(raw)
    )
    return hashlib.sha256(repr(payload).encode()).hexdigest()


def _match(
    ref: TableRef,
    objects: tuple[NativeObject, ...],
    *,
    allow_temporary: bool = False,
) -> NativeObject:
    matches = [obj for obj in objects if _ascii_key(obj.name) == _ascii_key(ref.name)]
    if not matches:
        raise _binding_error(f"relation {ref.name!r} does not exist")
    if len(matches) > 1:
        raise _binding_error(f"ambiguous relation {ref.name!r}; use qualified SQL or publication")
    obj = matches[0]
    if (obj.temporary and not allow_temporary) or obj.internal:
        raise _binding_error(f"relation {ref.name!r} resolves to a temporary/internal object")
    if obj.kind not in {"table", "view"}:
        raise _binding_error(f"unsupported relation kind for {ref.name!r}")
    return obj


def _match_parts(
    name: str,
    schema: str | None,
    catalog: str | None,
    objects: tuple[NativeObject, ...],
    *,
    allow_temporary: bool = False,
) -> NativeObject:
    if schema is None and catalog is None:
        candidates = [obj for obj in objects if _ascii_key(obj.name) == _ascii_key(name)]
    else:
        candidates = [
            obj
            for obj in objects
            if _ascii_key(obj.name) == _ascii_key(name)
            and (schema is None or _ascii_key(obj.schema) == _ascii_key(schema))
            and (catalog is None or _ascii_key(obj.catalog) == _ascii_key(catalog))
        ]
    if len(candidates) != 1:
        reason = "not found" if not candidates else "ambiguous"
        raise _binding_error(f"{reason} SQL relation {name!r}; qualify it or use publication")
    obj = candidates[0]
    if (obj.temporary and not allow_temporary) or obj.internal:
        raise _binding_error(f"SQL relation {name!r} resolves to a temporary/internal object")
    return obj


def _sql_tables(
    sql: str,
    catalog: Catalog,
    raw: Any,
    *,
    allow_temporary: bool = False,
    _crosscheck: bool = True,
    _capture: bool = False,
    _objects: tuple[NativeObject, ...] | None = None,
) -> tuple[NativeObject, ...]:
    actual = str(getattr(ibis.duckdb, "name", "duckdb"))
    statement = (
        _admit_capture_result_query_ast(sql, catalog.dialect)
        if _capture
        else _admit_read_query_ast(sql, catalog.dialect, actual)
    )
    if _capture:
        check_native_functions(raw, sql, None)
    scopes = _scopes(statement)
    physical: dict[tuple[str, str, str], NativeObject] = {}
    objects = native_objects(raw) if _objects is None else _objects
    for scope in scopes:
        for source_name, source in scope.sources.items():
            if source_name in scope.cte_sources or not hasattr(source, "this"):
                continue
            node = source
            if isinstance(node.this, exp.Func):
                if _capture and node.this.sql_name().upper() in _CAPTURE_READERS:
                    continue
                raise _binding_error(
                    f"opaque relation function {node.this.sql_name()!r} requires owned publication"
                )
            if getattr(node, "db", None) or getattr(node, "catalog", None):
                obj = _match_parts(
                    str(node.name),
                    str(node.db) if node.db else None,
                    str(node.catalog) if node.catalog else None,
                    objects,
                    allow_temporary=allow_temporary,
                )
            else:
                obj = _match_parts(
                    str(node.name), None, None, objects, allow_temporary=allow_temporary
                )
            physical[obj.identity] = obj
    if not _crosscheck:
        return tuple(physical.values())

    def expand(obj: NativeObject, active: set[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
        if obj.kind != "view":
            return {obj.identity}
        if obj.identity in active:
            raise _binding_error("cyclic native view dependency")
        query = (
            _admit_capture_stored_view_query_ast(obj.definition or "", "duckdb")
            if _capture
            else _admit_stored_view_query_ast(obj.definition or "", "duckdb")
        )
        query_sql = _render(query, "duckdb")
        if not _capture:
            check_native_functions(raw, query_sql, "duckdb")
        dependencies = _sql_tables(
            query_sql,
            _dummy_catalog(),
            raw,
            allow_temporary=allow_temporary,
            _crosscheck=False,
            _capture=_capture,
            _objects=objects,
        )
        leaves: set[tuple[str, str, str]] = set()
        next_active = active | {obj.identity}
        for dependency in dependencies:
            leaves.update(expand(dependency, next_active))
        return leaves

    sqlglot_leaves: set[tuple[str, str, str]] = set()
    for obj in physical.values():
        sqlglot_leaves.update(expand(obj, set()))
    try:
        with connection_lock(raw):
            native_result = raw.get_table_names(sql, qualified=True)
    except Exception as exc:
        raise _binding_error("native table-name crosscheck failed") from exc
    if not isinstance(native_result, (list, tuple, set, frozenset)):
        raise _binding_error("native table-name crosscheck returned an invalid result")
    native_physical: set[tuple[str, str, str]] = set()
    for native_name in native_result:
        try:
            parsed_name = __import__("sqlglot", fromlist=["parse_one"]).parse_one(
                f"SELECT * FROM {native_name}", read="duckdb"
            )
            node = parsed_name.find(exp.Table)
            if node is None:
                raise ValueError("not a relation")
        except Exception as exc:
            raise _binding_error("native relation name could not be parsed structurally") from exc
        obj = _match_parts(
            str(node.name),
            str(node.db) if node.db else None,
            str(node.catalog) if node.catalog else None,
            objects,
            allow_temporary=allow_temporary,
        )
        native_physical.add(obj.identity)
    if _capture:
        catalog_by_identity = {obj.identity: obj.catalog for obj in objects}
        if len({catalog_by_identity[identity] for identity in sqlglot_leaves}) > 1:
            raise _binding_error("opaque capture spans multiple mutable native databases")
    if native_physical != sqlglot_leaves:
        raise _binding_error("SQLGlot and native table-name dependency closures disagree")
    return tuple(physical.values())


def admit_capture_root(relation: Any, catalog: Catalog, raw: Any) -> None:
    if isinstance(relation, SqlQuery):
        _sql_tables(relation.sql, catalog, raw, allow_temporary=True, _capture=True)
        return
    if not isinstance(relation, TableRef):
        raise _binding_error("unsupported opaque capture root")
    objects = native_objects(raw)
    obj = _match(relation, objects, allow_temporary=True)
    if obj.kind != "view":
        raise _binding_error("only a view can be captured as an opaque named root")
    query = "SELECT * FROM " + ".".join(
        _quote_ident(value) for value in (obj.catalog, obj.schema, obj.name)
    )
    _sql_tables(query, _dummy_catalog(), raw, allow_temporary=True, _capture=True, _objects=objects)


def _closure(
    obj: NativeObject,
    objects: tuple[NativeObject, ...],
    raw: Any,
    seen: dict[tuple[str, str, str], NativeObject],
    active: set[tuple[str, str, str]],
    *,
    allow_temporary: bool = False,
) -> None:
    if obj.identity in active:
        raise _binding_error("cyclic native view dependency")
    if obj.identity in seen:
        return
    seen[obj.identity] = obj
    if obj.kind != "view":
        return
    active.add(obj.identity)
    definition = obj.definition or ""
    query = _admit_stored_view_query_ast(definition, "duckdb")
    query_sql = _render(query, "duckdb")
    check_native_functions(raw, query_sql, "duckdb")
    dependencies = _sql_tables(
        query_sql, _dummy_catalog(), raw, allow_temporary=allow_temporary, _objects=objects
    )
    for dependency in dependencies:
        _closure(dependency, objects, raw, seen, active, allow_temporary=allow_temporary)
    active.remove(obj.identity)


def _dummy_catalog() -> Catalog:
    return Catalog.model_construct(
        schema_version=1,
        day_boundary="UTC",
        dialect="duckdb",
        sources={},
        facts={},
        properties={},
        dimensions={},
        measures={},
        metrics={},
    )


def roots(catalog: Catalog, units: Any = None) -> tuple[Any, ...]:
    relations = [source.relation for source in catalog.sources.values()]
    relations.extend(dimension.relation for dimension in catalog.dimensions.values())
    if units:
        relations.extend(TableRef(kind="table", name=unit.table) for unit in units.values())
    return tuple(relations)


def required_relations(
    catalog: Catalog,
    con: Any,
    *,
    units: Any = None,
    _include_temporary: bool = False,
    _roots: tuple[Any, ...] | None = None,
) -> tuple[Any, ...]:
    raw = raw_connection(con)
    objects = native_objects(raw)
    seen: dict[tuple[str, str, str], NativeObject] = {}
    for relation in roots(catalog, units) if _roots is None else _roots:
        if isinstance(relation, TableRef):
            root = _match(relation, objects, allow_temporary=_include_temporary)
            _closure(root, objects, raw, seen, set(), allow_temporary=_include_temporary)
        elif isinstance(relation, SqlQuery):
            _admit_read_query_ast(relation.sql, catalog.dialect, "duckdb")
            check_native_functions(raw, relation.sql, catalog.dialect)
            for root in _sql_tables(
                relation.sql, catalog, raw, allow_temporary=_include_temporary, _objects=objects
            ):
                _closure(root, objects, raw, seen, set(), allow_temporary=_include_temporary)
    active_catalogs = {
        _ascii_key(item.catalog)
        for item in seen.values()
        if not item.temporary and not item.internal
    }
    if len(active_catalogs) > 1:
        raise _snapshot_error("input closure spans multiple mutable native databases")
    from terrine.execution.inputs import PhysicalRelationId

    return tuple(
        sorted(
            (
                PhysicalRelationId("table", obj.catalog, obj.schema, obj.name)
                for obj in seen.values()
                if obj.kind == "table"
            ),
            key=lambda item: (item.catalog or "", item.schema or "", item.name),
        )
    )


def admission_fingerprint(catalog: Catalog, relation: Any, raw: Any) -> str:
    """Revalidate the statically admitted closure and return its native identity."""
    objects = native_objects(raw)
    seen: dict[tuple[str, str, str], NativeObject] = {}
    if isinstance(relation, TableRef):
        root = _match(relation, objects)
        _closure(root, objects, raw, seen, set())
    else:
        _admit_read_query_ast(relation.sql, catalog.dialect, "duckdb")
        check_native_functions(raw, relation.sql, catalog.dialect)
        for root in _sql_tables(relation.sql, catalog, raw, _objects=objects):
            _closure(root, objects, raw, seen, set())
    payload = sorted(
        (obj.identity, obj.kind, obj.oid, obj.definition_hash) for obj in seen.values()
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def verify_native_backend(con: Any) -> tuple[Any, None]:
    raw = raw_connection(con)
    with connection_lock(raw):
        check_native_functions(raw, "SELECT system.main.current_transaction_id()", None)
        first = _rows(raw, "SELECT system.main.current_transaction_id()")[0][0]
        second = _rows(raw, "SELECT system.main.current_transaction_id()")[0][0]
        if first == second:
            raise _snapshot_error("caller already owns an active transaction")
    return raw, None


def begin_owned_transaction(raw: Any) -> None:
    try:
        raw.execute("SET python_scan_all_frames = false")
        raw.execute("BEGIN TRANSACTION")
    except Exception as exc:
        raise _snapshot_error("could not establish isolated execution transaction") from exc
    finally:
        invalidate_metadata()


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def semantic_settings(raw: Any) -> tuple[str, str, str, str]:
    rows = _rows(
        raw,
        "SELECT system.main.current_database(), system.main.current_schema(), "
        "system.main.current_setting('TimeZone'), system.main.current_setting('search_path')",
    )
    database, schema, timezone, search_path = rows[0]
    return str(database), str(schema), str(timezone), str(search_path)


def restore_semantic_settings(origin: Any, runtime: Any) -> None:
    database, schema, timezone, search_path = semantic_settings(origin)
    try:
        runtime.execute(f"USE {_quote_ident(database)}.{_quote_ident(schema)}")
        runtime.execute(f"SET TimeZone={_quote_literal(timezone)}")
        runtime.execute(f"SET search_path={_quote_literal(search_path)}")
        if semantic_settings(runtime) != (database, schema, timezone, search_path):
            raise ValueError("settings differ after restoration")
    except Exception as exc:
        raise _snapshot_error("semantic session settings could not be reproduced") from exc
    finally:
        invalidate_metadata()


@metadata_fact
def _function_catalog(raw: Any) -> Mapping[str, tuple[tuple[bool, str, str | None], ...]]:
    metadata: dict[str, list[tuple[bool, str, str | None]]] = {}
    for name, internal, kind, definition in _function_rows(raw):
        metadata.setdefault(str(name).casefold(), []).append(
            (
                bool(internal),
                str(kind).casefold(),
                str(definition) if definition is not None else None,
            )
        )
    return MappingProxyType({name: tuple(entries) for name, entries in metadata.items()})


@metadata_fact
def check_native_functions(raw: Any, sql: str, dialect: str | None) -> None:
    """Validate native function ownership, recursively including trusted macros."""
    if dialect is not None:
        _admit_read_query_ast(sql, dialect, "duckdb")
    metadata = _function_catalog(raw)

    def names_in(statement: str) -> set[str]:
        try:
            payload = _rows(
                raw,
                "SELECT system.main.json_serialize_sql(" + _quote_literal(statement) + ")",
            )[0][0]
            parsed = json.loads(payload) if isinstance(payload, str) else payload
        except Exception as exc:
            raise _binding_error("native parser could not certify function ownership") from exc
        names: set[str] = set()

        def collect(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key.casefold() in {
                        "function_name",
                        "function",
                        "operator_name",
                    } and isinstance(child, str):
                        names.add(child.casefold())
                    collect(child)
            elif isinstance(value, list):
                for child in value:
                    collect(child)

        collect(parsed)
        return names

    def inspect(name: str, active: frozenset[str]) -> None:
        if name in active:
            raise _binding_error(f"trusted macro dependency cycle at {name!r}")
        candidates = metadata.get(name)
        if not candidates or any(not internal for internal, _, _ in candidates):
            raise _binding_error(
                f"function or operator {name!r} has a missing or shadowing non-internal definition"
            )
        for _, kind, definition in candidates:
            if kind != "macro":
                continue
            if not definition:
                raise _binding_error(f"trusted macro {name!r} has no inspectable definition")
            body = definition.strip()
            if not body.casefold().startswith(("select ", "with ")):
                body = "SELECT " + body
            for dependency in names_in(body):
                inspect(dependency, active | {name})

    for name in names_in(sql):
        inspect(name, frozenset())


class _DuckDBProvider:
    capabilities = ENGINES["duckdb"].capabilities

    def matches(self, con: Any) -> bool:
        if type(con).__module__ != ENGINES["duckdb"].ibis_module:
            return False
        try:
            raw_connection(con)
        except TerrineError:
            return False
        return True

    def dialect(self, con: Any) -> str:
        return ENGINES["duckdb"].name

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        raw = raw_connection(con)
        with connection_lock(raw):
            admission_fingerprint(_dummy_catalog(), relation, raw)
            if isinstance(relation, TableRef):
                return con.table(relation.name)
            return con.sql(admit_read_query(relation.sql, "duckdb", "duckdb"))

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution.inputs import _required_native_relations

        return _required_native_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        from terrine.execution.inputs import _bind_native_resolver

        return _bind_native_resolver(catalog, con, **kwargs)

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        self.capabilities.require(
            "transaction_view",
            backend="duckdb",
            route="Use a backend with verified transaction-scoped input identity.",
        )
        from terrine.execution.inputs import _open_native_execution

        return _open_native_execution(catalog, con, units=units, input_binding=input_binding)


PROVIDER = _DuckDBProvider()
