"""Dialect-aware read-only SQL admission; the original query is never rewritten."""

from sqlglot import exp, parse
from sqlglot.optimizer.scope import traverse_scope

from terrine.errors import RefusalSpec

__all__ = ["admit_read_query"]

DIALECT_MISMATCH = RefusalSpec(
    "terrine.semantic.dialect_mismatch",
    "Catalog dialect {declared!r} does not match connection dialect {execution!r}.",
)
_INVALID = RefusalSpec(
    "terrine.sql.invalid_read_query", "SQL relation is not an admitted read-only query: {reason}."
)

# Canonical SQLGlot names for scalar/window/aggregate built-ins with read-only semantics.
_CAPTURE_READERS = frozenset({"READ_CSV", "READ_CSV_AUTO", "READ_PARQUET"})
_READ_FUNCTIONS = frozenset(
    {
        "ABS",
        "AVG",
        "CASE",
        "CAST",
        "CEIL",
        "COALESCE",
        "CONCAT",
        "CONCAT_WS",
        "COUNT",
        "DATE",
        "DATE_ADD",
        "DATE_DIFF",
        "DATE_SUB",
        "DATE_TRUNC",
        "DATETIME",
        "DAY",
        "DENSE_RANK",
        "EXTRACT",
        "FIRST_VALUE",
        "FLOOR",
        "GREATEST",
        "IF",
        "IFNULL",
        "LAG",
        "LAST_VALUE",
        "LEAD",
        "LEAST",
        "LENGTH",
        "LN",
        "LOG",
        "LOWER",
        "MAX",
        "MIN",
        "MONTH",
        "NULLIF",
        "NTILE",
        "PERCENTILE_CONT",
        "PERCENTILE_DISC",
        "POW",
        "POWER",
        "RANK",
        "REGEXP_EXTRACT",
        "REGEXP_LIKE",
        "REGEXP_REPLACE",
        "REPLACE",
        "ROUND",
        "ROW_NUMBER",
        "SIGN",
        "SPLIT",
        "SQRT",
        "STR_POSITION",
        "SUBSTRING",
        "SUM",
        "TIMESTAMP",
        "TIMESTAMP_ADD",
        "TIMESTAMP_DIFF",
        "TIMESTAMP_SUB",
        "TIMESTAMP_TRUNC",
        "TRIM",
        "TRY_CAST",
        "UPPER",
        "YEAR",
    }
)


def _parse_read_query_ast(sql: str, dialect: str) -> exp.Query:
    """Parse one read-only query structure without admitting its functions."""
    try:
        statements = parse(sql, read=dialect)
    except Exception as exc:
        raise _INVALID.error(reason="parse failed for the execution dialect") from exc
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="expected exactly one statement")
    statement = statements[0]
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="expected a SELECT or other read query")
    if any(
        isinstance(node, (exp.DML, exp.DDL, exp.Command, exp.Into, exp.Lock))
        for node in statement.walk()
    ):
        raise _INVALID.error(reason="write and row-lock constructs are not allowed")
    return statement


def admit_read_query(sql: str, declared_dialect: str, execution_dialect: str) -> str:
    """Accept exactly one read-only query when declaration matches execution."""
    if declared_dialect.casefold() != execution_dialect.casefold():
        raise DIALECT_MISMATCH.error(declared=declared_dialect, execution=execution_dialect)
    statement = _parse_read_query_ast(sql, declared_dialect)
    for node in statement.walk():
        if isinstance(node, exp.Func) and (
            node.sql_name() not in _READ_FUNCTIONS
            or isinstance(node.parent, exp.Dot)
            or (isinstance(node.parent, exp.Table) and (node.parent.db or node.parent.catalog))
        ):
            raise _INVALID.error(
                reason="function has no admitted read-only contract",
                route="Use a supported built-in or bind an owner-reviewed named source relation.",
            )
    return sql


def _admit_read_query_ast(sql: str, declared_dialect: str, execution_dialect: str) -> exp.Query:
    """Return the policy-checked AST and use SQLGlot's lexical scope graph."""
    admit_read_query(sql, declared_dialect, execution_dialect)
    try:
        statement = parse(sql, read=declared_dialect)[0]
        if not isinstance(statement, exp.Query):
            raise _INVALID.error(reason="expected a SELECT or other read query")
        # Materialize the scopes here so all callers use SQLGlot's lexical CTE
        # resolution instead of treating every Table node as a physical read.
        tuple(traverse_scope(statement))
    except Exception as exc:
        raise _INVALID.error(reason="scope analysis failed") from exc
    return statement


def _admit_stored_view_query_ast(definition: str, dialect: str) -> exp.Query:
    """Extract and admit the SELECT AST stored for a native view."""
    try:
        statements = parse(definition, read=dialect)
    except Exception as exc:
        raise _INVALID.error(reason="stored view definition could not be parsed") from exc
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="stored view definition is not one query")
    statement = statements[0]
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="stored view definition does not contain a query")
    return _admit_read_query_ast(statement.sql(dialect=dialect), dialect, dialect)


def _admit_capture_result_query_ast(sql: str, dialect: str) -> exp.Query:
    """Admit one explicit read-result boundary without trusting its functions."""
    statement = _parse_read_query_ast(sql, dialect)
    for node in statement.walk():
        if not isinstance(node, exp.Func):
            continue
        name = node.sql_name().upper()
        if (
            (name not in _READ_FUNCTIONS and name not in _CAPTURE_READERS)
            or isinstance(node.parent, exp.Dot)
            or (isinstance(node.parent, exp.Table) and (node.parent.db or node.parent.catalog))
        ):
            raise _INVALID.error(reason="capture function has no admitted read-only contract")
    tuple(traverse_scope(statement))
    return statement


def _admit_capture_stored_view_query_ast(definition: str, dialect: str) -> exp.Query:
    try:
        statements = parse(definition, read=dialect)
    except Exception as exc:
        raise _INVALID.error(reason="stored capture view could not be parsed") from exc
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="stored capture view is not one query")
    statement = statements[0]
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="stored capture view does not contain a query")
    return _admit_capture_result_query_ast(statement.sql(dialect=dialect), dialect)
