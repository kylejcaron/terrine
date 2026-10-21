"""PostgreSQL admission, private snapshot reads and refusal boundaries."""

from __future__ import annotations

import pytest

import terrine as tr
from integration.warehouse_execution.postgres_fixtures import connect_postgres
from integration.warehouse_execution.postgres_fixtures import (
    postgres_scratch as postgres_scratch,  # noqa: F401
)
from terrine.errors import TerrineError
from terrine.execution._postgres import PostgresSession
from terrine.execution.backend import backend_provider
from terrine.execution.inputs import open_execution
from terrine.semantic.models import SqlQuery, TableRef

pytestmark = pytest.mark.postgres

_CATALOG = """schema_version: 1
day_boundary: UTC
dialect: postgres
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


def _events(con, amount_type: str = "numeric(12, 2)") -> None:
    con.raw_sql(
        f"CREATE TABLE events(user_id text, occurred_at timestamptz, amount {amount_type})"
    ).close()
    con.raw_sql("INSERT INTO events VALUES ('a', '2025-01-01 12:00:00+00', 1.50)").close()


def _model(con, tmp_path, relation: str = "{kind: table, name: events}"):
    path = tmp_path / "catalog.yaml"
    path.write_text(_CATALOG.format(relation=relation), encoding="utf-8")
    return tr.Model.from_catalog(
        path, con=con, readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")}
    )


def _count(context, relation) -> int:
    return int(context.execute(context.resolve(relation).count()).as_py())


def test_private_read_holds_one_snapshot_and_never_touches_the_callers_transaction(
    postgres_scratch,  # noqa: F811
    tmp_path,
):
    con = postgres_scratch.con
    _events(con)
    model = _model(con, tmp_path)
    events = TableRef(kind="table", name="events")
    con.raw_sql("BEGIN").close()
    con.raw_sql("INSERT INTO events VALUES ('b', '2025-01-01 13:00:00+00', 2.00)").close()
    writer = connect_postgres(schema=postgres_scratch.schema)
    try:
        with open_execution(model.catalog, con) as context:
            assert _count(context, events) == 1  # caller's uncommitted row is invisible
            writer.raw_sql(
                "INSERT INTO events VALUES ('c', '2025-01-01 14:00:00+00', 3.00)"
            ).close()
            assert _count(context, events) == 1  # concurrent commit is outside the snapshot
            assert context.snapshot.backend == "postgres"
        status = con.con.info.transaction_status.name
        assert status == "INTRANS"  # the caller's transaction is still open and usable
        with con.raw_sql("SELECT count(*) FROM events") as cursor:
            assert cursor.fetchone()[0] == 3
    finally:
        con.raw_sql("ROLLBACK").close()
        writer.disconnect()


def test_sql_relation_resolves_inside_the_read_only_transaction(
    postgres_scratch,  # noqa: F811
    tmp_path,
):
    con = postgres_scratch.con
    _events(con)
    sql = "SELECT user_id, occurred_at, amount FROM events WHERE user_id <> 'z'"
    model = _model(con, tmp_path, relation='{kind: sql, sql: "' + sql + '"}')
    with open_execution(model.catalog, con) as context:
        assert _count(context, SqlQuery(kind="sql", sql=sql)) == 1


def test_replaced_relation_is_refused_after_binding(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _events(con)
    model = _model(con, tmp_path)
    resolve = backend_provider(con).bind_resolver(model.catalog, con)
    events = TableRef(kind="table", name="events")
    resolve(events)
    con.raw_sql("DROP TABLE events").close()
    _events(con)
    with pytest.raises(TerrineError) as error:
        resolve(events)
    assert error.value.code == "terrine.execution.invalid_input_binding"


def test_private_connection_closes_when_execution_fails(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _events(con)
    model = _model(con, tmp_path)
    with pytest.raises(RuntimeError), open_execution(model.catalog, con) as context:
        session = context._session
        assert isinstance(session, PostgresSession)
        owned = session.raw
        raise RuntimeError("injected failure")
    assert owned.closed


def test_user_function_shadowing_a_called_name_is_refused(postgres_scratch):  # noqa: F811
    con = postgres_scratch.con
    _events(con)
    con.raw_sql(
        f'CREATE FUNCTION "{postgres_scratch.schema}".lower(text) RETURNS text '
        "LANGUAGE sql AS 'SELECT $1'"
    ).close()
    with pytest.raises(TerrineError) as error:
        backend_provider(con).sql(con, "SELECT lower(user_id) AS u FROM events")
    assert error.value.code == "terrine.execution.snapshot_unavailable"


def test_unconstrained_numeric_column_is_refused_at_binding(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _events(con, amount_type="numeric")
    with pytest.raises(TerrineError) as error:
        _model(con, tmp_path)
    assert error.value.code == "terrine.semantic.decimal_scale_required"


def test_nonfinite_numeric_value_is_refused_at_read(postgres_scratch, tmp_path):  # noqa: F811
    con = postgres_scratch.con
    _events(con)
    con.raw_sql("INSERT INTO events VALUES ('n', '2025-01-01 15:00:00+00', 'NaN')").close()
    model = _model(con, tmp_path)
    with open_execution(model.catalog, con) as context:
        relation = context.resolve(TableRef(kind="table", name="events"))
        with pytest.raises(TerrineError) as error:
            context.execute(relation.select("amount"))
    assert error.value.code == "terrine.backend.nonfinite_decimal"


def test_published_datasets_refuse_a_postgres_connection(postgres_scratch):  # noqa: F811
    with pytest.raises(TerrineError) as error:
        tr.Dataset.open_generation("orders", "g1", con=postgres_scratch.con, namespace="runtime")
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == "runtime_reader"
