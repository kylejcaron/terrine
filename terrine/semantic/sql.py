"""Dialect-aware read-only SQL admission; the original query is never rewritten."""

import contextvars
import threading
from collections.abc import Callable

from sqlglot import exp, parse
from sqlglot.optimizer.scope import Scope, traverse_scope

from terrine.errors import RefusalSpec, TerrineError

__all__ = ["admit_read_query"]

DIALECT_MISMATCH = RefusalSpec(
    "terrine.semantic.dialect_mismatch",
    "The model uses SQL dialect {declared!r}, but the connection uses {execution!r}. "
    "Set the model dialect to match the connection and use SQL for that dialect.",
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


_TOO_DEEP = "too_deep_to_verify"
_WORKER = threading.local()


def _verified[T](action: Callable[[], T]) -> T:
    """Run one SQL verification step on a fresh thread.

    SQLGlot parses, renders and traverses by recursion, about twenty interpreter
    frames per nesting level of an expression and about twelve per subquery, and
    a compiled study statement nests deeply (under the portable policy a float
    quantization inside a six-feature window needs about 720 frames to parse;
    native PostgreSQL about 430). The recursion limit is shared by every thread,
    but the depth it bounds is counted per thread, so a fresh thread gives the
    step the whole budget whatever the caller's own depth; the limit itself is
    never changed. A statement too deep even for that is refused as unverifiable,
    which is distinct from being shown unsafe. Steps nested inside a running
    step stay on its thread.
    """
    if getattr(_WORKER, "active", False):
        try:
            return action()
        except RecursionError as exc:
            raise _INVALID.error(reason=_TOO_DEEP) from exc
    context = contextvars.copy_context()
    outcome: dict[str, object] = {}

    def run() -> None:
        _WORKER.active = True
        try:
            outcome["value"] = context.run(action)
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="terrine-sql-verification", daemon=True)
    worker.start()
    worker.join()
    if "error" in outcome:
        error = outcome["error"]
        assert isinstance(error, BaseException)
        if isinstance(error, RecursionError):
            raise _INVALID.error(reason=_TOO_DEEP) from error
        raise error
    return outcome["value"]  # ty: ignore[invalid-return-type]


def _render(statement: exp.Expression, dialect: str, *, comments: bool = True) -> str:
    """Render ``statement`` for ``dialect`` as a verification step."""
    return _verified(lambda: statement.sql(dialect=dialect, comments=comments))


def _statements(sql: str, dialect: str, reason: str) -> list[exp.Expression | None]:
    """Parse ``sql`` as a verification step, naming ``reason`` for a failed parse."""
    try:
        return _verified(lambda: parse(sql, read=dialect))
    except TerrineError:
        raise
    except Exception as exc:
        raise _INVALID.error(reason=reason) from exc


def _parse_read_query_ast(sql: str, dialect: str) -> exp.Query:
    """Parse one read-only query structure without admitting its functions."""
    statements = _statements(sql, dialect, "parse failed for the execution dialect")
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


def _scopes(statement: exp.Expression) -> tuple[Scope, ...]:
    """SQLGlot's lexical scope graph, materialized as a verification step.

    Scope traversal recurses per derived table, so dependency discovery reads
    the graph from here rather than traversing again on its own stack.
    """
    try:
        return _verified(lambda: tuple(traverse_scope(statement)))
    except TerrineError:
        raise
    except Exception as exc:
        raise _INVALID.error(reason="scope analysis failed") from exc


def _admit_read_query_ast(sql: str, declared_dialect: str, execution_dialect: str) -> exp.Query:
    """Return the policy-checked AST and use SQLGlot's lexical scope graph."""
    admit_read_query(sql, declared_dialect, execution_dialect)
    statement = _parse_read_query_ast(sql, declared_dialect)
    # Materialize the scopes here so all callers use SQLGlot's lexical CTE
    # resolution instead of treating every Table node as a physical read.
    _scopes(statement)
    return statement


def _admit_stored_view_query_ast(definition: str, dialect: str) -> exp.Query:
    """Extract and admit the SELECT AST stored for a native view."""
    statements = _statements(definition, dialect, "stored view definition could not be parsed")
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="stored view definition is not one query")
    statement = statements[0]
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="stored view definition does not contain a query")
    return _admit_read_query_ast(_render(statement, dialect), dialect, dialect)


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
    _scopes(statement)
    return statement


def _admit_capture_stored_view_query_ast(definition: str, dialect: str) -> exp.Query:
    statements = _statements(definition, dialect, "stored capture view could not be parsed")
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="stored capture view is not one query")
    statement = statements[0]
    if isinstance(statement, exp.Create):
        statement = statement.expression
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="stored capture view does not contain a query")
    return _admit_capture_result_query_ast(_render(statement, dialect), dialect)
