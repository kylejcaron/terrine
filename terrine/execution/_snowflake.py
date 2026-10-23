"""Snowflake relation identity, admission and Time Travel snapshot reads.

Snowflake isolates per statement, so a transaction gives no multi-statement
snapshot. Each execution reads ``CURRENT_TIMESTAMP()`` once and pins every
physical table reference in every query it runs with ``AT (TIMESTAMP => ...)``.
Only reads are issued on the caller's connection: no ``USE``, ``ALTER SESSION``,
transaction or temporary object.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

from terrine.execution._metadata import invalidate_metadata
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import _admit_read_query_ast, admit_read_query

DIALECT = "snowflake"
# Physical row sources with Time Travel, and views over them. Temporary tables
# are session-scoped, external tables and materialized views have no history.
_TABLE_TYPES = {"BASE TABLE": "table", "VIEW": "view"}
_STAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"
_RETENTION_ROUTE = "ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1"


def _snapshot_error(reason: str):
    from terrine.execution.inputs import _SNAPSHOT_UNAVAILABLE

    return _SNAPSHOT_UNAVAILABLE.error(reason=reason)


def _binding_error(reason: str):
    from terrine.execution.inputs import _INVALID

    return _INVALID.error(reason=reason)


def raw_connection(con: Any) -> Any:
    raw = getattr(con, "con", None)
    if raw is None or type(raw).__module__.split(".")[:2] != ["snowflake", "connector"]:
        raise _snapshot_error("connection is not a snowflake-connector-backed Ibis backend")
    return raw


def _rows(con: Any, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
    with raw_connection(con).cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(database: str, schema: str, name: str) -> str:
    return ".".join(_quote(part) for part in (database, schema, name))


@dataclass(frozen=True, slots=True)
class _Relation:
    database: str
    schema: str
    name: str
    kind: str
    created: datetime
    last_ddl: datetime
    definition_hash: str
    view_text: str | None

    @property
    def identity(self) -> str:
        return json.dumps([self.database, self.schema, self.name])


def _defaults(con: Any) -> tuple[str, str]:
    """The connection's current database and schema."""
    raw = raw_connection(con)
    database, schema = raw.database, raw.schema
    if not database or not schema:
        raise _snapshot_error("connection has no current database and schema")
    return database, schema


def _reference(con: Any, name: str, scope: tuple[str, str] | None = None) -> tuple[str, str, str]:
    """Resolve ``name`` like Snowflake does: unqualified names in the current (or
    the referencing view's) database and schema. Identifiers are exact, as Ibis
    quotes every name it binds."""
    parts = name.split(".")
    database, schema = scope or _defaults(con)
    if len(parts) == 1:
        return database, schema, parts[0]
    if len(parts) == 2:
        return database, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise _snapshot_error(f"relation name {name!r} is not database.schema.table")


_IDENTITY_SQL = """
SELECT t.TABLE_TYPE, t.RETENTION_TIME, t.CREATED, t.LAST_DDL, v.VIEW_DEFINITION, v.IS_SECURE
FROM {database}.INFORMATION_SCHEMA.TABLES AS t
LEFT JOIN {database}.INFORMATION_SCHEMA.VIEWS AS v
  ON v.TABLE_SCHEMA = t.TABLE_SCHEMA AND v.TABLE_NAME = t.TABLE_NAME
WHERE t.TABLE_SCHEMA = %s AND t.TABLE_NAME = %s
"""


def _relation(con: Any, database: str, schema: str, name: str) -> _Relation:
    try:
        rows = _rows(con, _IDENTITY_SQL.format(database=_quote(database)), (schema, name))
    except Exception as exc:
        raise _snapshot_error(f"relation {name!r} is not visible to this connection") from exc
    if not rows:
        raise _snapshot_error(f"relation {name!r} is not visible to this connection")
    table_type, retention, created, last_ddl, view_text, secure = rows[0]
    kind = _TABLE_TYPES.get(table_type)
    if kind is None:
        raise _snapshot_error(f"relation {name!r} has unsupported type {table_type!r}")
    if kind == "view":
        if secure == "YES" or not view_text:
            raise _snapshot_error(
                f"view {name!r} hides its definition; bind its tables or a non-secure view"
            )
    elif retention is None or int(retention) < 1:
        raise _snapshot_error(
            f"table {name!r} keeps no Time Travel history; run {_RETENTION_ROUTE}"
        )
    columns = _rows(con, f"DESCRIBE TABLE {_qualified(database, schema, name)}")
    definition = json.dumps(
        {"columns": [[row[0], row[1], row[3]] for row in columns], "view": view_text},
        sort_keys=True,
    )
    return _Relation(
        database,
        schema,
        name,
        kind,
        created,
        last_ddl,
        hashlib.sha256(definition.encode()).hexdigest(),
        view_text,
    )


def check_routines(sql: str) -> None:
    """Refuse a query that calls a database- or schema-qualified routine."""
    from sqlglot import parse_one

    try:
        statement = parse_one(sql, read=DIALECT)
    except Exception as exc:
        raise _snapshot_error("query could not be parsed") from exc
    _check_qualified_routines(statement)


def _check_qualified_routines(statement: Any) -> None:
    from sqlglot import exp

    for node in statement.walk():
        qualified_call = isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func)
        quoted_path = isinstance(node, exp.Anonymous) and "." in node.name
        if qualified_call or quoted_path:
            raise _snapshot_error("query calls a user-defined routine")


def _function_names(statement: Any) -> set[str]:
    from sqlglot import exp

    names: set[str] = set()
    for node in statement.walk():
        if isinstance(node, exp.Anonymous):
            names.add(node.name.upper())
        elif isinstance(node, exp.Func):
            names.update(name.upper() for name in node.sql_names())
    return names


def _check_view_routines(con: Any, database: str, statement: Any) -> None:
    """Refuse a view calling any routine: Snowflake resolves unqualified user
    function names, and a SQL function may read tables outside the snapshot."""
    _check_qualified_routines(statement)
    names = _function_names(statement)
    if not names:
        return
    routines = {
        str(row[0]).upper()
        for row in _rows(
            con, f"SELECT FUNCTION_NAME FROM {_quote(database)}.INFORMATION_SCHEMA.FUNCTIONS"
        )
    }
    if names & routines:
        raise _snapshot_error("view calls a user-defined routine")


def _view_query(definition: str) -> Any:
    from sqlglot import exp, parse_one

    try:
        statement = parse_one(definition, read=DIALECT)
    except Exception as exc:
        raise _snapshot_error("view definition could not be parsed") from exc
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _snapshot_error("view definition is not a query")
    return statement


def _sources(statement: Any) -> list[str]:
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    found = []
    for scope in traverse_scope(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier):
                parts = [part for part in (source.catalog, source.db, source.name) if part]
                found.append(".".join(parts))
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
        assert relation.view_text is not None
        statement = _view_query(relation.view_text)
        _check_view_routines(con, relation.database, statement)
        for name in _sources(statement):
            dependency = _reference(con, name, (relation.database, relation.schema))
            _closure(con, dependency, seen, path | {key})


def _closure_for(relation: RelationSpec, con: Any) -> dict[tuple[str, str, str], _Relation]:
    seen: dict[tuple[str, str, str], _Relation] = {}
    if isinstance(relation, TableRef):
        _closure(con, _reference(con, relation.name), seen, frozenset())
    elif isinstance(relation, SqlQuery):
        statement = _admit_read_query_ast(relation.sql, DIALECT, DIALECT)
        for name in _sources(statement):
            _closure(con, _reference(con, name), seen, frozenset())
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
                PhysicalRelationId("table", item.database, item.schema, item.name)
                for item in seen.values()
                if item.kind == "table"
            ),
            key=lambda item: (item.catalog or "", item.schema or "", item.name),
        )
    )


def admission_fingerprint(
    relation: RelationSpec, con: Any, *, snapshot: datetime | None = None
) -> str:
    """Return the identity of a relation's closure; any change alters it.

    With ``snapshot``, a relation created or altered after the snapshot instant
    refuses: its Time Travel history does not reach back to it.
    """
    closure = _closure_for(relation, con)
    if snapshot is not None:
        for item in closure.values():
            if item.created > snapshot or item.last_ddl > snapshot:
                raise _binding_error(
                    f"relation {item.name!r} was created or altered after the snapshot"
                )
    payload = sorted(
        (item.identity, item.kind, item.created.isoformat(), item.definition_hash)
        for item in closure.values()
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def resolve(con: Any, relation: RelationSpec, catalog_dialect: str = DIALECT) -> Any:
    if isinstance(relation, TableRef):
        database, schema, name = _reference(con, relation.name)
        item = _relation(con, database, schema, name)
        return con.table(item.name, database=(item.database, item.schema))
    sql = admit_read_query(relation.sql, catalog_dialect, DIALECT)
    return con.sql(sql)


def require_utc_session(con: Any) -> None:
    """Terrine's timestamp conversions assume the session renders instants in UTC."""
    rows = _rows(con, "SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
    value = next((row[1] for row in rows if str(row[0]).upper() == "TIMEZONE"), None)
    if value != "UTC":
        raise _snapshot_error(
            f"session TIMEZONE is {value!r}, not 'UTC'; run ALTER SESSION SET TIMEZONE = 'UTC'"
        )


def pin(sql: str, stamp: str) -> str:
    """Attach ``AT (TIMESTAMP => stamp)`` to every physical table reference.

    Common table expressions, derived tables and table functions are not
    physical references; a view reference is pinned, which Snowflake applies to
    the tables the view reads.
    """
    from sqlglot import exp, parse_one
    from sqlglot.optimizer.scope import traverse_scope

    statement = parse_one(sql, read=DIALECT)
    for scope in traverse_scope(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier):
                source.set("when", _historical(stamp))
    return statement.sql(dialect=DIALECT)


def _historical(stamp: str) -> Any:
    from sqlglot import exp

    instant = exp.Anonymous(
        this="TO_TIMESTAMP_TZ",
        expressions=[exp.Literal.string(stamp), exp.Literal.string(_STAMP_FORMAT)],
    )
    return exp.HistoricalData(this=exp.var("AT"), kind=exp.var("TIMESTAMP"), expression=instant)


class SnowflakeSession:
    """Reads pinned to one Time Travel instant on the caller's connection."""

    backend = DIALECT

    def __init__(self, con: Any, snapshot: datetime, stamp: str) -> None:
        self.con = con
        self.snapshot = snapshot
        self.stamp = stamp

    @classmethod
    def open(cls, con: Any) -> SnowflakeSession:
        require_utc_session(con)
        try:
            rows = _rows(
                con,
                f"SELECT CURRENT_TIMESTAMP(9), TO_VARCHAR(CURRENT_TIMESTAMP(9), '{_STAMP_FORMAT}')",
            )
        except Exception as exc:
            raise _snapshot_error("could not read the snapshot instant") from exc
        snapshot, stamp = rows[0]
        return cls(con, snapshot, stamp)

    def check_query(self, sql: str) -> None:
        check_routines(sql)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        from ibis.backends.snowflake.converter import SnowflakePyArrowData

        invalidate_metadata()
        target = query.as_table().schema().to_pyarrow()
        with _overflow_refusal(), raw_connection(con).cursor() as cursor:
            cursor.execute(pin(sql, self.stamp))
            result = cursor.fetch_arrow_all()
        if result is None:
            result = target.empty_table()
        return query.__pyarrow_result__(result, data_mapper=SnowflakePyArrowData)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> pa.RecordBatchReader:
        invalidate_metadata()
        target = query.as_table().schema().to_pyarrow()
        cursor = raw_connection(con).cursor()
        try:
            with _overflow_refusal():
                cursor.execute(pin(sql, self.stamp))
        except Exception:
            cursor.close()
            raise

        def batches():
            try:
                with _overflow_refusal():
                    for table in cursor.fetch_arrow_batches():
                        yield from (
                            table.rename_columns(target.names)
                            .cast(target)
                            .to_batches(max_chunksize=chunk_size)
                        )
            finally:
                cursor.close()

        return pa.RecordBatchReader.from_batches(target, batches())

    def close(self) -> None:
        """Nothing is held open: no transaction, session change or temporary object."""


@contextmanager
def _overflow_refusal() -> Generator[None]:
    """Snowflake reports NUMBER overflow as a query error; refuse with the code."""
    from terrine.backends.numeric import EXACT_OVERFLOW

    try:
        yield
    except Exception as exc:
        if "out of representable range" in str(exc).casefold():
            raise EXACT_OVERFLOW.error() from exc
        raise
