"""Compiled-statement checks verify deep SQL from any caller and refuse by code."""

from contextlib import contextmanager

import pytest

from terrine.errors import TerrineError
from tests.semantic.test_sql import _DEEP, _TOO_DEEP, _from_depth


class _Raw:
    """A PostgreSQL driver double whose catalog reports no shadowing functions."""

    def __init__(self):
        self.queries: list[tuple[str, tuple[list[str], list[str]]]] = []

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params):
        self.queries.append((sql, params))

    def fetchall(self):
        return []


def _postgres_check(sql: str) -> None:
    from terrine.execution._postgres import PostgresSession

    PostgresSession(_Raw()).check_query(sql)


def _bigquery_check(sql: str) -> None:
    from terrine.execution._bigquery import check_routines

    check_routines(sql)


def _snowflake_check(sql: str) -> None:
    from terrine.execution._snowflake import check_routines

    check_routines(sql)


def _snowflake_pin(sql: str) -> str:
    from terrine.execution._snowflake import pin

    return pin(sql, "2026-01-01 00:00:00.000000000 +0000")


def test_postgres_query_check_verifies_deep_statements_from_any_caller():
    """The validation path parses, renders and re-parses the compiled statement
    before the catalog query; a caller near the recursion limit must not change
    its outcome, and the catalog is consulted with the called names."""
    from terrine.execution._postgres import PostgresSession

    raw = _Raw()
    session = PostgresSession(raw)
    deep = "SELECT " + "(" * 30 + "COALESCE(value, 0)" + ")" * 30 + " AS value FROM events"
    assert _from_depth(120, lambda: session.check_query(deep)) is None
    ((_, (names, _schemas)),) = raw.queries
    assert "coalesce" in names


@pytest.mark.parametrize(
    "check",
    [
        pytest.param(_postgres_check, id="postgres"),
        pytest.param(_bigquery_check, id="bigquery"),
        pytest.param(_snowflake_check, id="snowflake"),
        pytest.param(_snowflake_pin, id="snowflake-pin"),
    ],
)
def test_compiled_statement_checks_refuse_unverifiable_depth_by_code(check):
    assert _from_depth(120, lambda: check(_DEEP)) == check(_DEEP)
    with pytest.raises(TerrineError) as caught:
        check(_TOO_DEEP)
    assert caught.value.code == "terrine.sql.invalid_read_query"
    assert caught.value.context["reason"] == "too_deep_to_verify"
