"""Snowflake admission, Time Travel reads on the caller's connection and refusals."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import terrine as tr
from integration.warehouse_execution.snowflake_fixtures import (
    snowflake_scratch as snowflake_scratch,  # noqa: F401
)
from terrine.errors import TerrineError
from terrine.execution.backend import backend_provider
from terrine.execution.inputs import open_execution
from terrine.semantic.models import SqlQuery, TableRef

pytestmark = pytest.mark.snowflake

_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: snowflake
sources:
  sessions:
    relation: {relation}
    timestamp: occurred_at
    entities: {{user: user_id}}
    facts: [session, spend]
    properties: []
    dimensions: []
facts:
  session: {{source: sessions, column: null}}
  spend: {{source: sessions, column: amount}}
properties: {{}}
dimensions: {{}}
measures:
  session_count: {{fact: session, aggregation: count}}
metrics:
  sessions: {{kind: mean, entity: user, measure: session_count}}
"""
_READS = {"SELECT", "DESCRIBE", "SHOW"}


def _events(scratch, *, options: str = "") -> None:
    # Unquoted table names fold to upper case; quoted column names stay as the
    # catalog spells them.
    scratch.sql(
        'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
        f'"amount" NUMBER(12, 2)) {options}'
    )
    scratch.sql("INSERT INTO events VALUES ('a', '2025-01-01 12:00:00 +0000', 1.5)")


def _model(scratch, tmp_path, relation: str = "{kind: table, name: events}"):
    path = tmp_path / "catalog.yaml"
    path.write_text(_CATALOG.format(relation=relation), encoding="utf-8")
    return tr.Model.from_catalog(
        path, con=scratch.con, readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")}
    )


def _count(context, relation) -> int:
    return int(context.execute(context.resolve(relation).count()).as_py())


def _query_types_since(scratch, start) -> set[str]:
    rows = scratch.sql(
        "SELECT QUERY_TYPE FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION("
        f"RESULT_LIMIT => 1000)) WHERE START_TIME >= '{start}'::TIMESTAMP_LTZ"
    )
    return {row[0] for row in rows}


def test_reads_pin_one_instant_on_the_callers_connection(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql('CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events')
    model = _model(scratch, tmp_path)
    events = TableRef(kind="table", name="events")
    view = TableRef(kind="table", name="shaped")
    query = SqlQuery(kind="sql", sql='SELECT "user_id", "occurred_at", "amount" FROM events')
    (started,) = scratch.sql("SELECT CURRENT_TIMESTAMP()")[0]
    with open_execution(model.catalog, scratch.con) as context:
        assert context.con is scratch.con
        assert context.snapshot.backend == "snowflake"
        assert _count(context, events) == 1
        scratch.sql("INSERT INTO events VALUES ('b', '2025-01-01 13:00:00 +0000', 2)")
        # Committed after the snapshot instant: invisible through every route.
        assert _count(context, events) == 1
        assert _count(context, view) == 1
        assert _count(context, query) == 1
        batches = context.to_arrow_batches(context.resolve(events).select("user_id"))
        assert batches.read_all().num_rows == 1
    assert _query_types_since(scratch, started) <= _READS | {"INSERT"}
    assert scratch.con.table("EVENTS").count().execute() == 2


def test_materialization_issues_only_reads(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql('CREATE TABLE users("user_id" VARCHAR)')
    scratch.sql("INSERT INTO users VALUES ('a'), ('b')")
    path = tmp_path / "catalog.yaml"
    path.write_text(_CATALOG.format(relation="{kind: table, name: events}"), encoding="utf-8")
    model = tr.Model.from_catalog(
        path,
        con=scratch.con,
        units={"user": tr.UnitSource("users", "user_id")},
        readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")},
    )
    design = tr.Design(
        "user",
        tr.FixedDate("2025-01-02"),
        features={"sessions_before": tr.agg("sessions", window=("-1d", "0s"))},
    )
    (started,) = scratch.sql("SELECT CURRENT_TIMESTAMP()")[0]
    budget = tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
    rows = design.build(model).materialize(budget).table.to_pylist()
    assert {row["unit"]: row["sessions_before"] for row in rows} == {"a": 1, "b": 0}
    assert _query_types_since(scratch, started) <= _READS


def test_relations_changed_after_binding_refuse(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql('CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events')
    model = _model(scratch, tmp_path)
    resolve = backend_provider(scratch.con).bind_resolver(model.catalog, scratch.con)
    for name, change in (
        ("events", "CREATE OR REPLACE TABLE events AS SELECT * FROM events"),
        (
            "shaped",
            "CREATE OR REPLACE VIEW shaped AS "
            'SELECT "user_id", "occurred_at", "amount" FROM events WHERE "amount" > 0',
        ),
    ):
        relation = TableRef(kind="table", name=name)
        resolve(relation)
        scratch.sql(change)
        with pytest.raises(TerrineError) as error:
            resolve(relation)
        assert error.value.code == "terrine.execution.invalid_input_binding"


@pytest.mark.parametrize(
    "change",
    [
        # Time Travel follows the object: a table swapped into the name has its
        # own history before the snapshot, so the pinned read would succeed with
        # the wrong rows unless identity is re-verified.
        "ALTER TABLE events SWAP WITH staged",
        "CREATE OR REPLACE TABLE events AS SELECT * FROM staged",
        'ALTER TABLE events ADD COLUMN "extra" INT',
        'CREATE OR REPLACE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM staged',
    ],
)
def test_relations_changed_during_execution_refuse(snowflake_scratch, tmp_path, change):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql(
        'CREATE TABLE staged("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
        '"amount" NUMBER(12, 2))'
    )
    scratch.sql(
        "INSERT INTO staged VALUES ('x', '2025-01-01 12:00:00 +0000', 1), "
        "('y', '2025-01-01 12:00:00 +0000', 1)"
    )
    scratch.sql('CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events')
    model = _model(scratch, tmp_path)
    relation = TableRef(kind="table", name="shaped" if "VIEW" in change else "events")
    with open_execution(model.catalog, scratch.con) as context:
        # Resolved once: the change lands between resolution and the pinned
        # read, so only re-verification after the read can catch it.
        table = context.resolve(relation)
        assert int(context.execute(table.count()).as_py()) == 1
        scratch.sql(change)
        with pytest.raises(TerrineError) as error:
            context.execute(table.count())
        assert error.value.code == "terrine.execution.invalid_input_binding"
        # A streamed read refuses before its first batch, not after the last.
        with pytest.raises(TerrineError) as error:
            context.to_arrow_batches(table.select("user_id")).read_next_batch()
        assert error.value.code == "terrine.execution.invalid_input_binding"


def test_pinned_sql_relations_resolve_cte_references_by_snowflake_rules(
    snowflake_scratch, tmp_path
):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    model = _model(scratch, tmp_path)
    # An unquoted reference folds to the CTE's name: it is pinned through the
    # table the CTE reads, never as a table of its own.
    query = SqlQuery(
        kind="sql",
        sql='WITH recent AS (SELECT "user_id", "occurred_at", "amount" FROM events) '
        "SELECT * FROM RECENT",
    )
    with open_execution(model.catalog, scratch.con) as context:
        assert _count(context, query) == 1
        scratch.sql("INSERT INTO events VALUES ('b', '2025-01-01 13:00:00 +0000', 2)")
        assert _count(context, query) == 1


@pytest.mark.parametrize(
    ("setup", "relation"),
    [
        (("DATA_RETENTION_TIME_IN_DAYS = 0",), "events"),
        (
            ('CREATE SECURE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events',),
            "shaped",
        ),
        (
            (
                "CREATE FUNCTION same(x VARCHAR) RETURNS VARCHAR AS 'x'",
                'CREATE VIEW shaped AS SELECT same("user_id") AS "user_id", "occurred_at", '
                '"amount" FROM events',
            ),
            "shaped",
        ),
        (
            (
                "CREATE FUNCTION same(x VARCHAR) RETURNS VARCHAR AS 'x'",
                'CREATE VIEW shaped AS SELECT {schema}.same("user_id") AS "user_id", '
                '"occurred_at", "amount" FROM events',
            ),
            "shaped",
        ),
        # Time Travel pins rows, not evaluation: these built-ins change the
        # view's rows between pinned reads without any DDL.
        (
            (
                'CREATE VIEW shaped AS SELECT "user_id", "occurred_at", RANDOM() AS "amount" '
                "FROM events",
            ),
            "shaped",
        ),
        (
            (
                'CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events '
                'WHERE "occurred_at" < CURRENT_TIMESTAMP()',
            ),
            "shaped",
        ),
        # A user routine whose name sqlglot maps to another built-in (STR_POSITION
        # renders as CHARINDEX) passes the whitelist; Snowflake runs the routine.
        (
            (
                "CREATE FUNCTION str_position(x VARCHAR, y VARCHAR) RETURNS FLOAT AS 'RANDOM()'",
                'CREATE VIEW shaped AS SELECT "user_id", "occurred_at", '
                'STR_POSITION("user_id", \'a\') AS "amount" FROM events',
            ),
            "shaped",
        ),
        # A tag, on a column or inherited from the table or schema, can gain a
        # masking policy (ALTER TAG ... SET MASKING POLICY) between reads
        # without any DDL on the relation.
        (
            ("CREATE TAG pii", "ALTER TABLE events MODIFY COLUMN \"user_id\" SET TAG pii = 'x'"),
            "events",
        ),
        (("CREATE TAG pii", "ALTER TABLE events SET TAG pii = 'x'"), "events"),
        (("CREATE TAG pii", "ALTER SCHEMA \"{schema}\" SET TAG pii = 'x'"), "events"),
        (
            (
                "CREATE TAG pii",
                'CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events',
                "ALTER VIEW shaped MODIFY COLUMN \"user_id\" SET TAG pii = 'x'",
            ),
            "shaped",
        ),
    ],
)
def test_relations_without_a_usable_history_refuse(snowflake_scratch, tmp_path, setup, relation):  # noqa: F811
    scratch = snowflake_scratch
    if relation == "events" and setup[0].startswith("DATA_RETENTION"):
        _events(scratch, options=setup[0])
    else:
        _events(scratch)
        for statement in setup:
            scratch.sql(statement.format(schema=scratch.schema))
    with pytest.raises(TerrineError) as error:
        _model(scratch, tmp_path, relation=f"{{kind: table, name: {relation}}}")
    assert error.value.code == "terrine.execution.snapshot_unavailable"


@pytest.mark.parametrize(
    "attach",
    [
        # Policies are evaluated at query time against mapping tables and the
        # current role, which Time Travel does not pin; a view over a protected
        # table is as unpinnable as the table.
        (
            'CREATE ROW ACCESS POLICY visible AS ("user_id" VARCHAR) RETURNS BOOLEAN -> TRUE',
            'ALTER TABLE events ADD ROW ACCESS POLICY visible ON ("user_id")',
        ),
        (
            "CREATE MASKING POLICY plain AS (val VARCHAR) RETURNS VARCHAR -> val",
            'ALTER TABLE events MODIFY COLUMN "user_id" SET MASKING POLICY plain',
        ),
    ],
    ids=["row_access", "masking"],
)
def test_relations_with_policies_refuse(snowflake_scratch, tmp_path, attach):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql('CREATE VIEW shaped AS SELECT "user_id", "occurred_at", "amount" FROM events')
    for statement in attach:
        scratch.sql(statement)
    for relation in ("events", "shaped"):
        with pytest.raises(TerrineError) as error:
            _model(scratch, tmp_path, relation=f"{{kind: table, name: {relation}}}")
        assert error.value.code == "terrine.execution.snapshot_unavailable", relation


def test_policies_attached_during_execution_refuse(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql('CREATE ROW ACCESS POLICY visible AS ("user_id" VARCHAR) RETURNS BOOLEAN -> TRUE')
    model = _model(scratch, tmp_path)
    with open_execution(model.catalog, scratch.con) as context:
        table = context.resolve(TableRef(kind="table", name="events"))
        assert int(context.execute(table.count()).as_py()) == 1
        # Attaching a policy is table DDL, so identity re-verification catches it.
        scratch.sql('ALTER TABLE events ADD ROW ACCESS POLICY visible ON ("user_id")')
        with pytest.raises(TerrineError) as error:
            context.execute(table.count())
        assert error.value.code == "terrine.execution.invalid_input_binding"


def test_resolved_relations_ignore_a_later_session_schema_change(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    other = f"{scratch.schema}_B"
    # CREATE SCHEMA switches the session to the new schema.
    scratch.sql(f'CREATE SCHEMA "{scratch.database}"."{other}"')
    try:
        scratch.sql(
            'CREATE TABLE events("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
            '"amount" NUMBER(12, 2))'
        )
        scratch.sql(
            "INSERT INTO events VALUES ('x', '2025-01-01 12:00:00 +0000', 1), "
            "('y', '2025-01-01 12:00:00 +0000', 1)"
        )
        scratch.sql(f'USE SCHEMA "{scratch.database}"."{scratch.schema}"')
        model = _model(scratch, tmp_path)
        query = SqlQuery(kind="sql", sql='SELECT "user_id", "occurred_at", "amount" FROM events')
        with open_execution(model.catalog, scratch.con) as context:
            # Resolved once in the model's schema: the admitted SQL names its
            # tables fully, so the pinned read ignores the session's new schema.
            table = context.resolve(query)
            scratch.sql(f'USE SCHEMA "{scratch.database}"."{other}"')
            assert int(context.execute(table.count()).as_py()) == 1
            assert context.to_arrow_batches(table.select("user_id")).read_all().num_rows == 1
        # A fresh resolution under the other schema names another object than
        # the one the Model bound, so a new materialization refuses rather than
        # silently reading the other table.
        design = tr.Design(
            "user",
            tr.EventAnchor("session", which="first"),
            features={"n": tr.agg("sessions", window=("-1d", "0s"))},
        )
        with pytest.raises(TerrineError) as error:
            design.build(model).materialize(
                tr.Budget(max_rows=10, max_output_bytes=1_000_000, strict_scan=False)
            )
        assert error.value.code == "terrine.execution.invalid_input_binding"
    finally:
        scratch.sql(f'DROP SCHEMA IF EXISTS "{scratch.database}"."{other}" CASCADE')


def test_non_utc_session_refuses(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    model = _model(scratch, tmp_path)
    scratch.sql("ALTER SESSION SET TIMEZONE = 'America/New_York'")
    with pytest.raises(TerrineError) as error, open_execution(model.catalog, scratch.con):
        pass
    assert error.value.code == "terrine.execution.snapshot_unavailable"


def test_names_follow_snowflake_identifier_rules(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    # A quoted lower-case name is a different table from the folded EVENTS.
    scratch.sql(
        'CREATE TABLE "events"("user_id" VARCHAR, "occurred_at" TIMESTAMP_TZ(9), '
        '"amount" NUMBER(12, 2))'
    )
    scratch.sql(
        "INSERT INTO \"events\" VALUES ('a', '2025-01-01 12:00:00 +0000', 1), "
        "('b', '2025-01-01 12:00:00 +0000', 1)"
    )
    expected = {
        "events": 1,
        '"events"': 2,
        f"{scratch.database}.{scratch.schema}.events": 1,
        f'"{scratch.database}"."{scratch.schema}"."events"': 2,
    }
    model = _model(scratch, tmp_path)
    with open_execution(model.catalog, scratch.con) as context:
        for name, count in expected.items():
            assert _count(context, TableRef(kind="table", name=name)) == count, name
        sql = 'SELECT "user_id", "occurred_at", "amount" FROM events'
        assert _count(context, SqlQuery(kind="sql", sql=sql)) == 1
        assert _count(context, SqlQuery(kind="sql", sql=sql.replace("events", '"events"'))) == 2


def test_nanosecond_outputs_beyond_arrow_range_refuse(snowflake_scratch, tmp_path):  # noqa: F811
    scratch = snowflake_scratch
    _events(scratch)
    scratch.sql("INSERT INTO events VALUES ('z', '9999-12-31 23:59:59.999999999 +0000', 1)")
    model = _model(scratch, tmp_path)
    events = TableRef(kind="table", name="events")
    with open_execution(model.catalog, scratch.con) as context:
        in_range = context.resolve(events).filter(lambda t: t.user_id == "a")
        row = context.execute(in_range.select("occurred_at")).to_pylist()[0]
        assert row["occurred_at"] == datetime(2025, 1, 1, 12, tzinfo=UTC)
        with pytest.raises(TerrineError) as error:
            context.execute(context.resolve(events).select("occurred_at"))
        assert error.value.code == "terrine.execution.arrow_timestamp_range"


def test_published_datasets_refuse_a_snowflake_connection(snowflake_scratch):  # noqa: F811
    with pytest.raises(TerrineError) as error:
        tr.Dataset.open_generation("orders", "g1", con=snowflake_scratch.con, namespace="runtime")
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == "runtime_reader"
