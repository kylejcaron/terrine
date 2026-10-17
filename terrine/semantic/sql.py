"""Dialect-aware read-only SQL admission; the original query is never rewritten."""

from sqlglot import exp, parse

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


def admit_read_query(sql: str, declared_dialect: str, execution_dialect: str) -> str:
    """Accept exactly one read-only query when declaration matches execution."""
    if declared_dialect.casefold() != execution_dialect.casefold():
        raise DIALECT_MISMATCH.error(declared=declared_dialect, execution=execution_dialect)
    try:
        statements = parse(sql, read=declared_dialect)
    except Exception as exc:
        raise _INVALID.error(reason="parse failed for the declared dialect") from exc
    if len(statements) != 1 or statements[0] is None:
        raise _INVALID.error(reason="expected exactly one statement")
    statement = statements[0]
    if not isinstance(statement, exp.Query):
        raise _INVALID.error(reason="expected a SELECT or other read query")
    for node in statement.walk():
        if isinstance(node, (exp.DML, exp.DDL, exp.Command, exp.Into, exp.Lock)):
            raise _INVALID.error(reason="write and row-lock constructs are not allowed")
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
