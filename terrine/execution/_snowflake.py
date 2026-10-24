"""Snowflake relation identity, admission and Time Travel snapshot reads.

Snowflake isolates per statement, so a transaction gives no multi-statement
snapshot. Each execution reads ``CURRENT_TIMESTAMP()`` once and pins every
table and view reference in every query it runs with ``AT (TIMESTAMP => ...)``,
which Snowflake applies to the tables a view reads. Time Travel follows the
object, not the name: a table swapped or renamed into place after admission
would answer a pinned read with another object's history, and a replaced view
has no history at all, so the session re-reads every admitted relation's
``CREATED`` and ``LAST_DDL`` after each query (every DDL, including swap and
rename, advances ``LAST_DDL``) and refuses the result on any change. Only reads
are issued on the caller's connection: no ``USE``, ``ALTER SESSION``,
transaction or temporary object.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

from terrine.errors import RefusalSpec
from terrine.execution._metadata import invalidate_metadata
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import _admit_read_query_ast, admit_read_query

DIALECT = "snowflake"
# Physical row sources with Time Travel, and views over them. Temporary tables
# are session-scoped; external, dynamic and event tables and materialized views
# have no usable history.
_TABLE_TYPES = {"BASE TABLE": "table", "VIEW": "view"}
_STAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"
_RETENTION_ROUTE = "ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1"
# Snowflake's own wording for NUMBER(38, 0) sums and differences that leave the
# type, which terrine maps to its exact-overflow refusal.
_OVERFLOW_MESSAGES = ("value overflow in a sum aggregate", "number out of representable range")
# A pinned read of an object created after the snapshot instant.
_HISTORY_UNAVAILABLE = "time travel data is not available"
_FUTURE_UNAVAILABLE = "future data is not yet available"
_TIMESTAMP_RANGE = RefusalSpec(
    "terrine.execution.arrow_timestamp_range",
    "A nanosecond timestamp output lies outside Arrow's range (1677-09-21 to 2262-04-11): "
    "{detail}. Declare the column with microsecond precision, such as TIMESTAMP_TZ(6), or "
    "cast it in a SQL relation.",
)

Key = tuple[str, str, str]


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
    def key(self) -> Key:
        return (self.database, self.schema, self.name)

    @property
    def identity(self) -> str:
        return json.dumps(list(self.key))


def _defaults(con: Any) -> tuple[str, str]:
    """The connection's current database and schema."""
    raw = raw_connection(con)
    database, schema = raw.database, raw.schema
    if not database or not schema:
        raise _snapshot_error("connection has no current database and schema")
    return database, schema


def _normalized(expression: Any) -> Any:
    """Apply Snowflake's identifier rules: unquoted names fold to upper case,
    quoted names are exact (and may contain dots or lower case)."""
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

    return normalize_identifiers(expression, dialect=DIALECT)


def _table_parts(table: Any) -> tuple[str, ...]:
    from sqlglot import exp

    parts = (table.args.get("catalog"), table.args.get("db"), table.this)
    if not all(part is None or isinstance(part, exp.Identifier) for part in parts):
        raise _snapshot_error("relation name is not database.schema.table")
    return tuple(part.name for part in parts if part is not None)


def _reference(con: Any, name: str, scope: tuple[str, str] | None = None) -> Key:
    """Resolve a relation name as Snowflake would: with its identifier rules, and
    unqualified names in the current (or the referencing view's) database and
    schema."""
    from sqlglot import exp

    try:
        table = _normalized(exp.to_table(name, dialect=DIALECT))
    except Exception as exc:
        raise _snapshot_error(f"relation name {name!r} is not database.schema.table") from exc
    parts = _table_parts(table)
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

_CURRENT_SQL = """
SELECT TABLE_NAME, CREATED, LAST_DDL
FROM {database}.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({names})
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
    """Physical relation names a query reads, rendered with exact identifiers."""
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    found = []
    for scope in traverse_scope(_normalized(statement.copy())):
        for source in scope.sources.values():
            if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier):
                found.append(".".join(_quote(part) for part in _table_parts(source)))
    return found


def _closure(
    con: Any,
    key: Key,
    seen: dict[Key, _Relation],
    path: frozenset[Key],
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


def closure_for(relation: RelationSpec, con: Any) -> dict[Key, _Relation]:
    """Every table and view a relation reads, the relation itself first."""
    seen: dict[Key, _Relation] = {}
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

    seen: dict[Key, _Relation] = {}
    for relation in roots(catalog, units):
        seen.update(closure_for(relation, con))
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


def fingerprint(closure: Mapping[Key, _Relation]) -> str:
    """The identity of a relation's closure; any change alters it."""
    payload = sorted(
        (item.identity, item.kind, item.created.isoformat(), item.definition_hash)
        for item in closure.values()
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def resolve(
    con: Any,
    relation: RelationSpec,
    catalog_dialect: str = DIALECT,
    closure: Mapping[Key, _Relation] | None = None,
) -> Any:
    if isinstance(relation, TableRef):
        item = (
            next(iter(closure.values()))
            if closure
            else _relation(con, *_reference(con, relation.name))
        )
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
    """Attach ``AT (TIMESTAMP => stamp)`` to every table and view reference.

    Common table expressions, derived tables and table functions are not
    physical references. Snowflake applies a view's pin to the tables it reads.
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
        self._bound: dict[Key, _Relation] = {}

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

    def admit(self, closure: Iterable[_Relation]) -> None:
        """Bind relations to the snapshot: one created or altered after it has no
        history reaching back to it, and every one is re-verified after each read."""
        for item in closure:
            if item.created > self.snapshot or item.last_ddl > self.snapshot:
                raise _binding_error(
                    f"relation {item.name!r} was created or altered after the snapshot"
                )
            self._bound[item.key] = item

    def verify(self) -> None:
        """Refuse when any admitted relation saw DDL since admission.

        ``LAST_DDL`` is monotonic and advances on replace, swap, rename and
        column changes, so an unchanged value after a read proves the name
        denoted the admitted object throughout.
        """
        by_schema: dict[tuple[str, str], list[_Relation]] = {}
        for item in self._bound.values():
            by_schema.setdefault((item.database, item.schema), []).append(item)
        for (database, schema), items in by_schema.items():
            names = ", ".join("%s" for _ in items)
            sql = _CURRENT_SQL.format(database=_quote(database), names=names)
            try:
                rows = _rows(self.con, sql, (schema, *(item.name for item in items)))
            except Exception as exc:
                raise _binding_error("relation identity could not be re-verified") from exc
            current = {row[0]: (row[1], row[2]) for row in rows}
            for item in items:
                if current.get(item.name) != (item.created, item.last_ddl):
                    raise _binding_error(
                        f"relation {item.name!r} was replaced or altered during execution"
                    )

    def check_query(self, sql: str) -> None:
        check_routines(sql)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        from ibis.backends.snowflake.converter import SnowflakePyArrowData

        invalidate_metadata()
        target = query.as_table().schema().to_pyarrow()
        with _query_refusal(), _arrow_refusal(), raw_connection(con).cursor() as cursor:
            cursor.execute(pin(sql, self.stamp))
            result = cursor.fetch_arrow_all()
        self.verify()
        if result is None:
            result = target.empty_table()
        with _arrow_refusal():
            return query.__pyarrow_result__(result, data_mapper=SnowflakePyArrowData)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> pa.RecordBatchReader:
        invalidate_metadata()
        target = query.as_table().schema().to_pyarrow()
        cursor = raw_connection(con).cursor()
        try:
            with _query_refusal():
                cursor.execute(pin(sql, self.stamp))
        except Exception:
            cursor.close()
            raise

        def batches():
            try:
                with _query_refusal(), _arrow_refusal():
                    for table in cursor.fetch_arrow_batches():
                        yield from (
                            table.rename_columns(target.names)
                            .cast(target)
                            .to_batches(max_chunksize=chunk_size)
                        )
                self.verify()
            finally:
                cursor.close()

        return pa.RecordBatchReader.from_batches(target, batches())

    def close(self) -> None:
        """Nothing is held open: no transaction, session change or temporary object."""


@contextmanager
def _query_refusal() -> Generator[None]:
    """Map Snowflake's query errors for exact overflow and lost history to refusals."""
    from terrine.backends.numeric import EXACT_OVERFLOW

    try:
        yield
    except Exception as exc:
        message = str(exc).casefold()
        if any(text in message for text in _OVERFLOW_MESSAGES):
            raise EXACT_OVERFLOW.error() from exc
        if _HISTORY_UNAVAILABLE in message:
            raise _binding_error("a relation was replaced after the snapshot") from exc
        if _FUTURE_UNAVAILABLE in message:
            raise _snapshot_error("the snapshot instant is ahead of the table's clock") from exc
        raise


@contextmanager
def _arrow_refusal() -> Generator[None]:
    """An instant outside Arrow's nanosecond range either fails in the connector
    (its nanosecond conversion overflows int64) or arrives as a microsecond
    column whose cast to the nanosecond output type fails."""
    try:
        yield
    except Exception as exc:
        message = str(exc)
        if "out of bounds timestamp" in message or "overflows int64 range" in message:
            raise _TIMESTAMP_RANGE.error(detail=message.split(":")[-1].strip()[:200]) from exc
        raise
