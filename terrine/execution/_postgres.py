"""PostgreSQL relation identity, query admission and owned read-only sessions."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from itertools import pairwise
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any

import pyarrow as pa

from terrine.errors import RefusalSpec
from terrine.execution._metadata import invalidate_metadata
from terrine.semantic.models import Catalog, RelationSpec, SqlQuery, TableRef
from terrine.semantic.sql import _admit_read_query_ast, _parse_read_query_ast, admit_read_query

_NONFINITE = RefusalSpec(
    "terrine.backend.nonfinite_decimal",
    "Exact decimal column {column!r} contains {value}, which has no exact decimal value. "
    "Filter or replace non-finite numeric values in the relation SQL.",
)

DIALECT = "postgres"
_MIN_IBIS = (12, 0)
# Relation kinds that are physical row sources or views over them.
_TABLE_KINDS = {"r": "table", "p": "table", "m": "table"}
_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")


def _snapshot_error(reason: str):
    from terrine.execution.inputs import _SNAPSHOT_UNAVAILABLE

    return _SNAPSHOT_UNAVAILABLE.error(reason=reason)


def ibis_supported() -> bool:
    """Ibis 11 lowers PostgreSQL as-of joins with unit filters after the nearest row."""
    parts = tuple(int(part) for part in re.findall(r"\d+", version("ibis-framework"))[:2])
    return parts >= _MIN_IBIS


def raw_connection(con: Any) -> Any:
    raw = getattr(con, "con", None)
    if raw is None or type(raw).__module__.split(".")[0] != "psycopg":
        raise _snapshot_error("connection is not a psycopg-backed Ibis PostgreSQL backend")
    return raw


def _rows(raw: Any, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
    with raw.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


@dataclass(frozen=True, slots=True)
class _Relation:
    oid: int
    database: str
    schema: str
    name: str
    kind: str
    definition_hash: str | None

    @property
    def identity(self) -> str:
        return json.dumps([self.database, self.schema, self.name])


_RELATION_SQL = """
SELECT c.oid, current_database(), n.nspname, c.relname, c.relkind, c.relpersistence,
       CASE WHEN c.relkind = 'v' THEN pg_catalog.pg_get_viewdef(c.oid, true) END
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = %s
"""

_VIEW_DEPENDENCIES_SQL = """
SELECT DISTINCT d.refobjid
FROM pg_catalog.pg_rewrite r
JOIN pg_catalog.pg_depend d
  ON d.objid = r.oid
 AND d.classid = 'pg_catalog.pg_rewrite'::regclass
 AND d.refclassid = 'pg_catalog.pg_class'::regclass
WHERE r.ev_class = %s AND d.refobjid <> %s
ORDER BY 1
"""


def _relation(raw: Any, oid: int) -> _Relation:
    row = _rows(raw, _RELATION_SQL, (oid,))
    if not row:
        raise _snapshot_error("relation disappeared during admission")
    oid, database, schema, name, relkind, persistence, definition = row[0]
    if persistence == "t":
        raise _snapshot_error(f"temporary relation {name!r} is not a durable input")
    if relkind == "v":
        kind = "view"
    elif relkind in _TABLE_KINDS:
        kind = "table"
    else:
        raise _snapshot_error(f"relation {name!r} has unsupported kind {relkind!r}")
    digest = hashlib.sha256(definition.encode()).hexdigest() if definition is not None else None
    return _Relation(oid, database, schema, name, kind, digest)


def _lookup(raw: Any, name: str) -> int:
    row = _rows(raw, "SELECT to_regclass(%s)::oid", (name,))
    if not row or row[0][0] is None:
        raise _snapshot_error(f"relation {name!r} is not visible to this connection")
    return int(row[0][0])


def _closure(raw: Any, oid: int, seen: dict[int, _Relation], path: frozenset[int]) -> None:
    if oid in path:
        raise _snapshot_error("view dependency cycle")
    if oid in seen:
        return
    relation = _relation(raw, oid)
    seen[oid] = relation
    if relation.kind == "view":
        _check_view_functions(raw, oid)
        for (dependency,) in _rows(raw, _VIEW_DEPENDENCIES_SQL, (oid, oid)):
            _closure(raw, int(dependency), seen, path | {oid})


_VIEW_FUNCTIONS_SQL = """
SELECT DISTINCT p.proname
FROM pg_catalog.pg_rewrite r
JOIN pg_catalog.pg_depend d
  ON d.objid = r.oid
 AND d.classid = 'pg_catalog.pg_rewrite'::regclass
 AND d.refclassid = 'pg_catalog.pg_proc'::regclass
JOIN pg_catalog.pg_proc p ON p.oid = d.refobjid
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE r.ev_class = %s AND n.nspname <> ALL(%s)
"""


def _check_view_functions(raw: Any, oid: int) -> None:
    """Refuse a view whose stored rules call any function outside the system catalogs."""
    user_functions = _rows(raw, _VIEW_FUNCTIONS_SQL, (oid, list(_SYSTEM_SCHEMAS)))
    if user_functions:
        raise _snapshot_error(
            "view calls a user-defined function: "
            + ", ".join(sorted(row[0] for row in user_functions))
        )


def _table_name(source: Any) -> str:
    """Render a parsed table reference with PostgreSQL identifier rules."""
    parts = (source.args.get("catalog"), source.args.get("db"), source.this)
    return ".".join(part.sql(dialect=DIALECT, comments=False) for part in parts if part is not None)


def _sql_roots(sql: str, raw: Any) -> list[int]:
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope

    statement = _admit_read_query_ast(sql, DIALECT, DIALECT)
    oids: list[int] = []
    for scope in traverse_scope(statement):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                oids.append(_lookup(raw, _table_name(source)))
    return oids


def _closure_for(relation: RelationSpec, raw: Any) -> dict[int, _Relation]:
    seen: dict[int, _Relation] = {}
    if isinstance(relation, TableRef):
        _closure(raw, _lookup(raw, relation.name), seen, frozenset())
    elif isinstance(relation, SqlQuery):
        check_functions(raw, relation.sql)
        for oid in _sql_roots(relation.sql, raw):
            _closure(raw, oid, seen, frozenset())
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

    raw = raw_connection(con)
    seen: dict[int, _Relation] = {}
    for relation in roots(catalog, units):
        seen.update(_closure_for(relation, raw))
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


def admission_fingerprint(relation: RelationSpec, raw: Any) -> str:
    """Return the identity of a relation's closure; any change alters it."""
    payload = sorted(
        (item.identity, item.kind, item.oid, item.definition_hash)
        for item in _closure_for(relation, raw).values()
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def _called_names(sql: str) -> set[str]:
    """Function names a query may call, case-folded.

    Calls are read from the tokens of both the source and its rendered parse: the
    source keeps aliases the parser canonicalizes (``DATE_PART`` parses as
    ``EXTRACT``), and the rendering is what executes. Tokens exclude comments and
    string literals, so neither can name a call.
    """
    from sqlglot import exp
    from sqlglot.dialects.dialect import Dialect

    statement = _parse_read_query_ast(sql, DIALECT)
    dialect = Dialect.get_or_raise(DIALECT)
    names: set[str] = set()
    for text in (sql, statement.sql(dialect=DIALECT, comments=False)):
        tokens = dialect.tokenize(text)
        names.update(
            token.text.casefold()
            for token, following in pairwise(tokens)
            if following.token_type.name == "L_PAREN"
            and token.token_type.name not in _NON_NAME_TOKENS
        )
    for node in statement.walk():
        if isinstance(node, exp.Anonymous):
            names.add(node.name.casefold())
        elif isinstance(node, exp.Func):
            names.add(node.sql_name().casefold())
    return names


_NON_NAME_TOKENS = frozenset(
    {"STRING", "NATIONAL_STRING", "RAW_STRING", "HEREDOC_STRING", "BIT_STRING", "HEX_STRING"}
    | {"BYTE_STRING", "UNICODE_STRING", "NUMBER", "L_PAREN", "R_PAREN", "COMMA", "STAR"}
)


def check_functions(raw: Any, sql: str) -> None:
    """Refuse queries that could call a function defined outside the system catalogs.

    A user-defined function with a matching name in any schema could shadow or
    replace the built-in meaning. Matching is case-insensitive and unions parsed
    and textual names, which only widens refusal.
    """
    names = sorted(_called_names(sql))
    if not names:
        return
    shadowing = _rows(
        raw,
        "SELECT DISTINCT p.proname FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
        "WHERE lower(p.proname) = ANY(%s) AND n.nspname <> ALL(%s)",
        (names, list(_SYSTEM_SCHEMAS)),
    )
    if shadowing:
        raise _snapshot_error(
            "query calls a name with a user-defined function: "
            + ", ".join(sorted(row[0] for row in shadowing))
        )


def _normalized(sql: str) -> str:
    """One statement rendered from its parse, without terminators or comments."""
    return _parse_read_query_ast(sql, DIALECT).sql(dialect=DIALECT, comments=False)


def query_schema(raw: Any, sql: str, type_mapper: Any) -> Any:
    """Describe a read query's columns without creating any database object."""
    import ibis

    sql = _normalized(sql)
    with raw.cursor() as cursor:
        cursor.execute(f"SELECT * FROM ({sql}) AS terrine_probe LIMIT 0")
        result = cursor.pgresult
        names = [column.name for column in cursor.description or ()]
        types = [(result.ftype(index), result.fmod(index)) for index in range(len(names))]
    formatted = [
        _rows(raw, "SELECT pg_catalog.format_type(%s, %s)", (oid, mod))[0][0] for oid, mod in types
    ]
    return ibis.schema(
        {name: type_mapper.from_string(text) for name, text in zip(names, formatted, strict=True)}
    )


def resolve(con: Any, relation: RelationSpec, catalog_dialect: str = DIALECT) -> Any:
    raw = raw_connection(con)
    if isinstance(relation, TableRef):
        item = _relation(raw, _lookup(raw, relation.name))
        return con.table(item.name, database=item.schema)
    sql = _normalized(admit_read_query(relation.sql, catalog_dialect, DIALECT))
    return con.sql(sql, schema=query_schema(raw, sql, con.compiler.type_mapper))


def owned_connection(con: Any) -> Any:
    """Open a terrine-owned connection with the caller's parameters and search path."""
    import psycopg

    raw = raw_connection(con)
    search_path = _rows(raw, "SHOW search_path")[0][0]
    parameters = dict(raw.info.get_parameters())
    password = raw.info.password
    if password:
        parameters["password"] = password
    try:
        owned = psycopg.connect(**parameters, autocommit=False)
    except Exception as exc:
        raise _snapshot_error(
            "could not open a private PostgreSQL connection from the caller's parameters; "
            "connect Ibis with host, port, user, password and database"
        ) from exc
    try:
        with owned.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute("SELECT pg_catalog.set_config('search_path', %s, false)", (search_path,))
        owned.commit()
        owned.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    except Exception:
        owned.close()
        raise
    return owned


class PostgresSession:
    """One REPEATABLE READ READ ONLY transaction on a terrine-owned connection."""

    backend = DIALECT

    def __init__(self, raw: Any) -> None:
        self.raw = raw

    def check_query(self, sql: str) -> None:
        _parse_read_query_ast(sql, DIALECT)
        check_functions(self.raw, sql)

    def _rows(self, sql: str, schema: pa.Schema, chunk_size: int):
        struct = pa.struct(list(schema))
        with self.raw.cursor(name="terrine_cursor") as cursor:
            cursor.execute(sql)
            while batch := cursor.fetchmany(chunk_size):
                try:
                    array = pa.array(batch, type=struct)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    _raise_nonfinite(batch, schema)
                    # psycopg returns uuid.UUID where Ibis expects Arrow strings.
                    array = pa.array(_uuids_as_text(batch), type=struct)
                yield pa.RecordBatch.from_struct_array(array)

    def execute(self, con: Any, query: Any, sql: str) -> pa.Table:
        try:
            con._run_pre_execute_hooks(query)
        finally:
            invalidate_metadata()
        schema = query.as_table().schema().to_pyarrow()
        table = pa.Table.from_batches(list(self._rows(sql, schema, 65_536)), schema=schema)
        return query.__pyarrow_result__(table)

    def batches(self, con: Any, query: Any, sql: str, *, chunk_size: int) -> pa.RecordBatchReader:
        try:
            con._run_pre_execute_hooks(query)
        finally:
            invalidate_metadata()
        schema = query.as_table().schema().to_pyarrow()
        return pa.RecordBatchReader.from_batches(schema, self._rows(sql, schema, chunk_size))

    def close(self) -> None:
        try:
            self.raw.rollback()
        finally:
            self.raw.close()


def _raise_nonfinite(rows: list[tuple[Any, ...]], schema: pa.Schema) -> None:
    from decimal import Decimal

    for row in rows:
        for field, value in zip(schema, row, strict=True):
            if isinstance(value, Decimal) and not value.is_finite():
                raise _NONFINITE.error(column=field.name, value=str(value))


def _uuids_as_text(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    from uuid import UUID

    return [
        tuple(str(value) if isinstance(value, UUID) else value for value in row) for row in rows
    ]
