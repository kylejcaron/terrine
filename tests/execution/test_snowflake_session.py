from __future__ import annotations

import pytest
from sqlglot import exp, parse_one

from terrine.execution._snowflake import pin


def _pinned(sql: str) -> dict[str, bool]:
    """Each table reference's name and whether it carries a Time Travel clause."""
    statement = parse_one(pin(sql, "2026-01-01 00:00:00.000000000 +0000"), read="snowflake")
    return {
        table.name: table.args.get("when") is not None for table in statement.find_all(exp.Table)
    }


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("WITH cte AS (SELECT * FROM events) SELECT * FROM cte", {"EVENTS": True, "CTE": False}),
        # Unquoted names fold to upper case, so this reference names the CTE.
        ("WITH cte AS (SELECT * FROM events) SELECT * FROM CTE", {"EVENTS": True, "CTE": False}),
        (
            'WITH "cte" AS (SELECT * FROM events) SELECT * FROM "cte"',
            {"EVENTS": True, "cte": False},
        ),
    ],
)
def test_pin_reaches_physical_tables_but_not_common_table_expressions(sql, expected):
    assert _pinned(sql) == expected


def test_pin_leaves_a_quoted_name_that_differs_from_its_cte_as_a_table():
    # A quoted lower-case reference is a distinct object from the folded CTE.
    assert _pinned('WITH cte AS (SELECT * FROM events) SELECT * FROM "cte"') == {
        "EVENTS": True,
        "cte": True,
    }
