"""Snowflake relation identity, admission and Time Travel snapshot reads.

Snowflake isolates per statement, so a transaction gives no multi-statement
snapshot. Each execution reads ``CURRENT_TIMESTAMP()`` once and pins every
table and view reference in every query it runs with ``AT (TIMESTAMP => ...)``,
which Snowflake applies to the tables a view reads. Time Travel follows the
object, not the name: a table swapped or renamed into place after admission
would answer a pinned read with another object's history, and a replaced view
has no history at all, so the session re-reads every admitted relation's
``CREATED`` and ``LAST_DDL`` after each query (every DDL, including swap and
rename, advances ``LAST_DDL``) and refuses the result on any change. Every
physical reference, inside admitted SQL too, is resolved and fully qualified
at admission, so the session's current database and schema never matter at
read time. Only reads are issued on the caller's connection: no ``USE``,
``ALTER SESSION``, transaction or temporary object.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa

from terrine.backends.capabilities import _CAPABILITY_UNAVAILABLE, _SNAPSHOT_UNAVAILABLE
from terrine.backends.registry import ENGINES
from terrine.errors import RefusalSpec, TerrineError
from terrine.execution._metadata import invalidate_metadata
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import _admit_read_query_ast, _render, _verified

DIALECT = "snowflake"
# Physical row sources with Time Travel, and views over them. Temporary tables
# are session-scoped; external, dynamic and event tables and materialized views
# have no usable history.
_TABLE_TYPES = {"BASE TABLE": "table", "VIEW": "view"}
_STAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"
_RETENTION_ROUTE = "ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1"
# Every NUMBER(p, 0) up to this precision fits int64; wider ones keep their digits.
_INT64_DIGITS = 18
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
    columns: ibis.Schema

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


def _resolved(parts: tuple[str, ...], scope: tuple[str, str]) -> Key:
    """An unqualified name resolves in ``scope``: the current (or the
    referencing view's) database and schema."""
    database, schema = scope
    if len(parts) == 1:
        return database, schema, parts[0]
    if len(parts) == 2:
        return database, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise _snapshot_error("relation name is not database.schema.table")


def _reference(con: Any, name: str, scope: tuple[str, str] | None = None) -> Key:
    """Resolve a relation name as Snowflake would: with its identifier rules, and
    unqualified names in the current (or the referencing view's) database and
    schema."""
    from sqlglot import exp

    try:
        table = _normalized(exp.to_table(name, dialect=DIALECT))
    except Exception as exc:
        raise _snapshot_error(f"relation name {name!r} is not database.schema.table") from exc
    return _resolved(_table_parts(table), scope or _defaults(con))


def column_type(type_text: str, *, nullable: bool) -> dt.DataType:
    """Ibis's reading of a Snowflake column type, except that a ``NUMBER(p, 0)``
    wider than the 18 digits every ``int64`` holds keeps its precision as
    ``decimal(p, 0)``, so a wide integer key or property survives the Arrow
    export instead of failing its cast."""
    from ibis.backends.sql.datatypes import SnowflakeType
    from sqlglot import exp

    dtype = SnowflakeType.from_string(type_text, nullable=nullable)
    if not dtype.is_int64():
        return dtype
    parsed = exp.DataType.build(type_text, dialect=DIALECT)
    digits = [int(param.name) for param in parsed.expressions]
    precision, scale = digits if len(digits) == 2 else (38, 0)
    if scale == 0 and precision > _INT64_DIGITS:
        return dt.Decimal(precision, 0, nullable=nullable)
    return dtype


def _schema(described: Iterable[tuple[Any, ...]]) -> ibis.Schema:
    """Columns from ``DESCRIBE`` rows ``(name, type, kind, null?, ...)``."""
    return ibis.schema({row[0]: column_type(row[1], nullable=row[3] == "Y") for row in described})


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
    if created is None or last_ddl is None:
        # Objects predating the identity metadata or newly replicated into the
        # account have nothing to verify a pinned read against.
        raise _snapshot_error(
            f"{kind} {name!r} has no CREATED or LAST_DDL instant to verify its identity; "
            "read from a copy made with CREATE TABLE ... AS SELECT or CLONE, which records both"
        )
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
        _schema(columns),
    )


def _parsed(sql: str, failure: str) -> Any:
    from sqlglot import parse_one

    try:
        return _verified(lambda: parse_one(sql, read=DIALECT))
    except TerrineError:
        raise
    except Exception as exc:
        raise _snapshot_error(failure) from exc


def check_routines(sql: str) -> None:
    """Refuse a query that calls a database- or schema-qualified routine."""
    _check_qualified_routines(_parsed(sql, "query could not be parsed"))


def _check_qualified_routines(statement: Any) -> None:
    from sqlglot import exp

    for node in statement.walk():
        qualified_call = isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func)
        quoted_path = isinstance(node, exp.Anonymous) and "." in node.name
        if qualified_call or quoted_path:
            raise _snapshot_error("query calls a user-defined routine")


def _view_query(definition: str) -> Any:
    from sqlglot import exp

    statement = _parsed(definition, "view definition could not be parsed")
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _snapshot_error("view definition is not a query")
    return statement


def _called_names(definition: str) -> set[str]:
    """Every function name as written in a definition: an identifier directly
    followed by ``(``. Taken from the tokens, not the parse, because sqlglot
    maps some names to other built-ins (``STR_POSITION`` becomes ``CHARINDEX``)
    while Snowflake would execute a user routine of the written name."""
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.tokens import TokenType

    tokens = Dialect.get_or_raise(DIALECT).tokenize(definition)
    names: set[str] = set()
    for token, following in zip(tokens, tokens[1:], strict=False):
        if following.token_type is TokenType.L_PAREN and token.token_type not in (
            TokenType.L_PAREN,
            TokenType.R_PAREN,
            TokenType.COMMA,
        ):
            quoted = token.token_type is TokenType.IDENTIFIER
            names.add(token.text if quoted else token.text.upper())
    return names


_FUNCTIONS_SQL = "SELECT FUNCTION_NAME FROM {database}.INFORMATION_SCHEMA.FUNCTIONS"


def _check_routines_by_name(
    con: Any, definition: str, subject: str, databases: Iterable[str]
) -> None:
    """Refuse a view or SQL relation calling any user routine by the name written
    in its text: Snowflake resolves an unqualified call to a user function of
    that name, in the view's own database or the session's, and a SQL function
    may read tables outside the snapshot. Built-ins are evaluated per query."""
    names = _called_names(definition)
    if not names:
        return
    routines: set[str] = set()
    for database in dict.fromkeys(databases):
        try:
            rows = _rows(con, _FUNCTIONS_SQL.format(database=_quote(database)))
        except Exception as exc:
            raise _snapshot_error(
                f"user routines of database {database!r} could not be listed to admit "
                f"{subject}; materialize its rows into a table"
            ) from exc
        # The catalog stores unquoted names upper-cased and quoted names as written.
        routines.update(str(row[0]) for row in rows)
    called = sorted(names & routines)
    if called:
        raise _snapshot_error(
            f"{subject} calls the user routine {', '.join(called)}, whose result is not "
            "pinned; materialize its rows into a table, or use only built-ins"
        )


def _physical_tables(statement: Any) -> list[Any]:
    """Physical relation references in every scope of a normalized statement:
    common table expressions, derived tables and table functions are not."""
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    found = []
    for scope in traverse_scope(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier):
                found.append(source)
    return found


def qualify(con: Any, statement: Any, scope: tuple[str, str] | None = None) -> list[Key]:
    """Resolve every physical reference of ``statement`` under Snowflake's
    identifier rules and rewrite it, in place, as its fully qualified name, so
    the query reads the admitted objects whatever the session's current
    database and schema become. Returns the references' keys in order."""
    from sqlglot import exp

    keys: list[Key] = []
    tables = _physical_tables(_normalized(statement))
    if not tables:
        return keys
    resolved_scope = scope or _defaults(con)
    for table in tables:
        key = _resolved(_table_parts(table), resolved_scope)
        for argument, part in zip(("catalog", "db", "this"), key, strict=True):
            table.set(argument, exp.to_identifier(part, quoted=True))
        keys.append(key)
    return keys


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
        _check_qualified_routines(statement)
        _check_routines_by_name(
            con,
            relation.view_text,
            f"view {relation.name!r}",
            (relation.database, _defaults(con)[0]),
        )
        for dependency in qualify(con, statement, (relation.database, relation.schema)):
            _closure(con, dependency, seen, path | {key})


def closure_for(relation: RelationSpec, con: Any) -> dict[Key, _Relation]:
    """Every table and view a relation reads, the relation itself first."""
    seen: dict[Key, _Relation] = {}
    if isinstance(relation, TableRef):
        _closure(con, _reference(con, relation.name), seen, frozenset())
    elif isinstance(relation, SqlQuery):
        statement = _admit_read_query_ast(relation.sql, DIALECT, DIALECT)
        for key in qualify(con, statement):
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
    """The relation as an Ibis table whose columns keep their physical precision
    and whose physical references are fully qualified."""
    from ibis.expr import operations as ops

    if isinstance(relation, TableRef):
        item = (
            next(iter(closure.values()))
            if closure
            else _relation(con, *_reference(con, relation.name))
        )
        return ops.DatabaseTable(  # ty: ignore[call-non-callable]
            item.name,
            schema=item.columns,
            source=con,
            namespace=ops.Namespace(catalog=item.database, database=item.schema),
        ).to_expr()
    # sqlglot may rewrite a user routine named like a built-in (STR_POSITION
    # renders as CHARINDEX), so the names as written are checked first.
    _check_routines_by_name(con, relation.sql, "the SQL relation", (_defaults(con)[0],))
    statement = _admit_read_query_ast(relation.sql, catalog_dialect, DIALECT)
    _refuse_relative_time_travel(statement)
    qualify(con, statement)
    sql = _render(statement, DIALECT)
    return con.sql(sql, schema=_query_schema(con, sql))


def _refuse_relative_time_travel(statement: Any) -> None:
    """An explicit ``AT``/``BEFORE`` naming a timestamp or statement is one fixed
    instant and is kept; an ``OFFSET`` is relative to each query's start, so
    successive reads would see different data."""
    from sqlglot import exp

    for clause in statement.find_all(exp.HistoricalData):
        if str(clause.args.get("kind")).upper() == "OFFSET":
            raise _snapshot_error(
                "a Time Travel OFFSET is evaluated per query; name a fixed instant with "
                "AT (TIMESTAMP => ...) or AT (STATEMENT => ...)"
            )


def _query_schema(con: Any, sql: str) -> ibis.Schema:
    """An admitted query's columns, typed like a table's: described from an
    empty result rather than Ibis's own inference, which reads every
    ``NUMBER(p, 0)`` as ``int64``."""
    probe = f"SELECT * FROM ({sql}) LIMIT 0"
    try:
        with raw_connection(con).cursor() as cursor:
            cursor.execute(probe)
            described = cursor.execute("DESCRIBE RESULT LAST_QUERY_ID()").fetchall()
    except Exception as exc:
        raise _snapshot_error("query schema could not be determined") from exc
    return _schema(described)


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
    physical references; identifiers are normalized first so a reference
    resolves to its expression under Snowflake's rules (``CTE`` names
    ``cte``). Snowflake applies a view's pin to the tables it reads.
    """
    from sqlglot import parse_one

    def rewrite() -> str:
        statement = _normalized(parse_one(sql, read=DIALECT))
        for table in _physical_tables(statement):
            # An explicit AT/BEFORE already names one fixed instant; keep it.
            if table.args.get("when") is None:
                table.set("when", _historical(stamp))
        return statement.sql(dialect=DIALECT)

    return _verified(rewrite)


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
        with raw_connection(con).cursor() as cursor:
            with _query_refusal():
                cursor.execute(pin(sql, self.stamp))
            self.verify()
            with _query_refusal(), _arrow_refusal():
                result = cursor.fetch_arrow_all()
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
            # The statement has completed and its result is held by Snowflake:
            # identity is settled before any batch reaches the caller.
            self.verify()
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


class _SnowflakeProvider:
    capabilities = ENGINES["snowflake"].capabilities

    def matches(self, con: Any) -> bool:
        return type(con).__module__ == ENGINES["snowflake"].ibis_module

    def dialect(self, con: Any) -> str:
        return ENGINES["snowflake"].name

    def resolve_relation(self, con: Any, relation: RelationSpec) -> ibis.Table:
        from terrine.execution import _snowflake

        return _snowflake.resolve(con, relation)

    def required_relations(self, catalog, con, *, units=None):
        from terrine.execution import _snowflake

        return _snowflake.required_relations(catalog, con, units=units)

    def bind_resolver(self, catalog, con, **kwargs):
        """Resolve relations; with ``session`` (the execution's Time Travel pin)
        every relation is admitted to it and re-verified after each read."""
        from terrine.execution import _snowflake
        from terrine.execution.inputs import _INVALID

        if kwargs.get("input_binding") is not None:
            raise _CAPABILITY_UNAVAILABLE.error(
                backend="snowflake",
                operation="retained_snapshot",
                status="unverified",
                route="Retained input bindings are DuckDB-only; materialize directly.",
            )
        session = kwargs.get("session")
        _snowflake.required_relations(catalog, con, units=kwargs.get("units"))
        admitted: dict[tuple[str, str], Any] = {}
        fingerprints: dict[tuple[str, str], str] = {}

        def resolve(relation: RelationSpec) -> Any:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            closure = _snowflake.closure_for(relation, con)
            if session is not None:
                session.admit(closure.values())
            fingerprint = _snowflake.fingerprint(closure)
            if key in admitted:
                if fingerprints[key] != fingerprint:
                    raise _INVALID.error(
                        reason="relation definition or identity changed after Model binding"
                    )
                return admitted[key]
            admitted[key] = _snowflake.resolve(con, relation, catalog.dialect, closure)
            fingerprints[key] = fingerprint
            return admitted[key]

        return resolve

    def open_execution(self, catalog, con, *, units=None, input_binding=None):
        from terrine.execution.inputs import _open_snowflake_execution

        return _open_snowflake_execution(
            self, catalog, con, units=units, input_binding=input_binding
        )


PROVIDER = _SnowflakeProvider()
