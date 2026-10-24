from __future__ import annotations

from datetime import UTC, datetime

import ibis
import ibis.expr.datatypes as dt
import pytest
from sqlglot import exp, parse_one

from terrine.errors import TerrineError
from terrine.execution import _snowflake
from terrine.execution._snowflake import pin
from terrine.semantic.models import SqlQuery, TableRef

_INSTANT = datetime(2026, 1, 1, tzinfo=UTC)


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


def test_pin_keeps_an_explicit_historical_instant():
    sql = "SELECT * FROM events AT (TIMESTAMP => '2025-01-01'::TIMESTAMP_TZ)"
    pinned = parse_one(pin(sql, "2026-01-01 00:00:00.000000000 +0000"), read="snowflake")
    (table,) = pinned.find_all(exp.Table)
    assert "2025-01-01" in table.args["when"].sql(dialect="snowflake")


def _qualified_names(sql: str) -> set[str]:
    statement = parse_one(sql, read="snowflake")
    return {
        ".".join(part.sql(dialect="snowflake") for part in table.parts)
        for table in statement.find_all(exp.Table)
        if isinstance(table.this, exp.Identifier) and table.args.get("catalog") is not None
    }


def test_admitted_sql_is_qualified_with_the_resolved_database_and_schema(monkeypatch):
    monkeypatch.setattr(_snowflake, "_defaults", lambda con: ("DB", "A"))
    statement = parse_one(
        'WITH recent AS (SELECT * FROM events) SELECT * FROM recent JOIN "events" USING (id) '
        "JOIN other.t2 USING (id) JOIN db2.s3.t3 USING (id)",
        read="snowflake",
    )
    keys = _snowflake.qualify(object(), statement)
    assert keys == [
        ("DB", "A", "EVENTS"),
        ("DB", "A", "events"),
        ("DB", "OTHER", "T2"),
        ("DB2", "S3", "T3"),
    ]
    sql = statement.sql(dialect="snowflake")
    assert _qualified_names(sql) == {
        '"DB"."A"."EVENTS"',
        '"DB"."A"."events"',
        '"DB"."OTHER"."T2"',
        '"DB2"."S3"."T3"',
    }
    # The CTE reference is not a physical table and the pin follows the
    # qualified names, so a later change of the session's schema is irrelevant.
    assert _pinned(sql) == {"EVENTS": True, "events": True, "T2": True, "T3": True, "RECENT": False}


def _metadata(
    monkeypatch,
    *,
    created=_INSTANT,
    last_ddl=_INSTANT,
    columns=(("user_id", "VARCHAR(16777216)", "Y"),),
    view=None,
    routines=(),
):
    """Stub the catalog reads behind one relation's identity; ``view`` is the
    stored definition of ``SHAPED``, every other name being a base table."""

    def rows(con, sql, params=None):
        if "INFORMATION_SCHEMA.TABLES" in sql:
            if view is not None and params and params[1] == "SHAPED":
                return [("VIEW", None, created, last_ddl, view, "NO")]
            return [("BASE TABLE", 1, created, last_ddl, None, None)]
        if sql.startswith("DESCRIBE TABLE"):
            return [(name, type_text, "COLUMN", nullable) for name, type_text, nullable in columns]
        if "INFORMATION_SCHEMA.FUNCTIONS" in sql:
            return [(routine,) for routine in routines]
        raise AssertionError(sql)

    monkeypatch.setattr(_snowflake, "_rows", rows)
    monkeypatch.setattr(_snowflake, "_defaults", lambda con: ("DB", "S"))


@pytest.mark.parametrize("metadata", [{"created": None}, {"last_ddl": None}])
def test_relations_without_identity_instants_refuse(monkeypatch, metadata):
    # Objects predating the identity metadata, or freshly replicated, have no
    # CREATED or LAST_DDL to verify against.
    _metadata(monkeypatch, **metadata)
    with pytest.raises(TerrineError) as error:
        _snowflake._relation(object(), "DB", "S", "EVENTS")
    assert error.value.code == "terrine.execution.snapshot_unavailable"


@pytest.mark.parametrize(
    ("definition", "routines"),
    [
        ('CREATE VIEW shaped AS SELECT same("user_id") AS "user_id" FROM events', ("SAME",)),
        ('CREATE VIEW shaped AS SELECT s.same("user_id") AS "user_id" FROM events', ()),
        # The name as written is what Snowflake resolves, whatever built-in it
        # parses to (STR_POSITION renders as CHARINDEX).
        (
            'CREATE VIEW shaped AS SELECT STR_POSITION("user_id", \'a\') AS "n" FROM events',
            ("STR_POSITION",),
        ),
        # A quoted routine name keeps its case in the catalog and in the call.
        (
            'CREATE VIEW shaped AS SELECT "str_position"("user_id", \'a\') AS "n" FROM events',
            ("str_position",),
        ),
    ],
    ids=["unqualified", "qualified", "named_like_a_built_in", "quoted_lowercase"],
)
def test_views_calling_a_user_routine_refuse(monkeypatch, definition, routines):
    _metadata(monkeypatch, view=definition, routines=routines)
    with pytest.raises(TerrineError) as error:
        _snowflake.closure_for(TableRef(kind="table", name="shaped"), object())
    assert error.value.code == "terrine.execution.snapshot_unavailable"


def test_views_over_built_ins_bind_their_closure(monkeypatch):
    _metadata(
        monkeypatch,
        view='CREATE VIEW shaped AS SELECT "user_id", COALESCE("amount", 0) AS "amount" '
        'FROM events WHERE "at" < CURRENT_TIMESTAMP()',
        routines=("SAME",),
    )
    closure = _snowflake.closure_for(TableRef(kind="table", name="shaped"), object())
    assert [item.kind for item in closure.values()] == ["view", "table"]


def test_resolved_schemas_keep_integer_precision_beyond_int64(monkeypatch):
    _metadata(
        monkeypatch,
        columns=(
            ("n18", "NUMBER(18,0)", "Y"),
            ("n19", "NUMBER(19,0)", "N"),
            ("n38", "NUMBER(38,0)", "Y"),
            ("amount", "NUMBER(12,2)", "Y"),
            ("at", "TIMESTAMP_TZ(9)", "Y"),
        ),
    )
    relation = _snowflake._relation(object(), "DB", "S", "EVENTS")
    assert relation.columns == ibis.schema(
        {
            "n18": dt.int64,
            "n19": dt.Decimal(19, 0, nullable=False),
            "n38": dt.Decimal(38, 0),
            "amount": dt.Decimal(12, 2),
            "at": dt.Timestamp(timezone="UTC", scale=9),
        }
    )


def test_sql_relation_calling_a_user_routine_named_like_a_built_in_refuses(monkeypatch):
    # sqlglot renders STR_POSITION as CHARINDEX, so the written name is checked.
    _metadata(monkeypatch, routines=("STR_POSITION",))
    relation = SqlQuery(kind="sql", sql="SELECT STR_POSITION(user_id, 'a') AS n FROM events")
    with pytest.raises(TerrineError) as error:
        _snowflake.resolve(object(), relation)
    assert error.value.code == "terrine.execution.snapshot_unavailable"
