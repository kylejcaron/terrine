from __future__ import annotations

from datetime import UTC, datetime

import ibis
import ibis.expr.datatypes as dt
import pytest
from sqlglot import exp, parse_one

from terrine.errors import TerrineError
from terrine.execution import _snowflake
from terrine.execution._snowflake import pin
from terrine.semantic.models import TableRef

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
    policies=(),
    tags=(),
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
        if "POLICY_REFERENCES" in sql:
            if isinstance(policies, Exception):
                raise policies
            return list(policies)
        if "TAG_REFERENCES_ALL_COLUMNS" in sql:
            return list(tags)
        if "INFORMATION_SCHEMA.FUNCTIONS" in sql:
            return [(routine,) for routine in routines]
        raise AssertionError(sql)

    monkeypatch.setattr(_snowflake, "_rows", rows)


@pytest.mark.parametrize(
    "metadata",
    [
        # Objects predating the identity metadata, or freshly replicated, have
        # no CREATED or LAST_DDL to verify against.
        {"created": None},
        {"last_ddl": None},
        # Policies are evaluated at query time and their mapping tables are
        # not pinned, whether the attachment is visible or not inspectable.
        {"policies": [("DB", "S", "P", "ROW_ACCESS_POLICY")]},
        {"policies": [("DB", "S", "M", "MASKING_POLICY")]},
        {"policies": RuntimeError("not authorized")},
        # A tag, direct or inherited, can gain a masking policy without DDL.
        {"tags": [("DB", "S", "PII", "x", "MANUAL", "COLUMN")]},
        {"tags": [("DB", "S", "PII", "x", "INHERITED", "SCHEMA")]},
    ],
)
def test_relations_without_verifiable_identity_refuse(monkeypatch, metadata):
    _metadata(monkeypatch, **metadata)
    with pytest.raises(TerrineError) as error:
        _snowflake._relation(object(), "DB", "S", "EVENTS")
    assert error.value.code == "terrine.execution.snapshot_unavailable"


@pytest.mark.parametrize(
    "definition",
    [
        # Each read evaluates the view: a nondeterministic or clock-dependent
        # built-in, or any function outside the read-only whitelist, changes
        # the rows without DDL.
        'CREATE VIEW shaped AS SELECT "user_id", RANDOM() AS "amount" FROM events',
        'CREATE VIEW shaped AS SELECT "user_id" FROM events WHERE "at" > CURRENT_TIMESTAMP()',
        'CREATE VIEW shaped AS SELECT same("user_id") AS "user_id" FROM events',
        'CREATE VIEW shaped AS SELECT s.same("user_id") AS "user_id" FROM events',
    ],
)
def test_views_outside_the_read_only_policy_refuse(monkeypatch, definition):
    _metadata(monkeypatch, view=definition)
    monkeypatch.setattr(_snowflake, "_defaults", lambda con: ("DB", "S"))
    with pytest.raises(TerrineError) as error:
        _snowflake.closure_for(TableRef(kind="table", name="shaped"), object())
    assert error.value.code == "terrine.execution.snapshot_unavailable"


def test_views_over_admitted_built_ins_bind_their_closure(monkeypatch):
    _metadata(
        monkeypatch,
        view='CREATE VIEW shaped AS SELECT "user_id", COALESCE("amount", 0) AS "amount" '
        'FROM events WHERE "amount" > 0',
    )
    monkeypatch.setattr(_snowflake, "_defaults", lambda con: ("DB", "S"))
    closure = _snowflake.closure_for(TableRef(kind="table", name="shaped"), object())
    assert [item.kind for item in closure.values()] == ["view", "table"]


def test_views_calling_a_user_routine_named_like_a_built_in_refuse(monkeypatch):
    # sqlglot parses STR_POSITION as a built-in and renders CHARINDEX, which the
    # whitelist admits, while Snowflake would run the user's STR_POSITION.
    _metadata(
        monkeypatch,
        view='CREATE VIEW shaped AS SELECT STR_POSITION("user_id", \'a\') AS "amount" FROM events',
        routines=("STR_POSITION",),
    )
    monkeypatch.setattr(_snowflake, "_defaults", lambda con: ("DB", "S"))
    with pytest.raises(TerrineError) as error:
        _snowflake.closure_for(TableRef(kind="table", name="shaped"), object())
    assert error.value.code == "terrine.execution.snapshot_unavailable"
    assert "STR_POSITION" in error.value.context["reason"]


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
