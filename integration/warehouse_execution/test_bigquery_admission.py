"""BigQuery admission, private snapshot reads and refusal boundaries."""

from __future__ import annotations

import pytest

import terrine as tr
from integration.warehouse_execution.bigquery_fixtures import (
    bigquery_scratch as bigquery_scratch,  # noqa: F401
)
from terrine.errors import TerrineError
from terrine.execution.backend import backend_provider
from terrine.execution.inputs import open_execution
from terrine.semantic.models import SqlQuery, TableRef

pytestmark = pytest.mark.bigquery

_CATALOG = """schema_version: 2
day_boundary: UTC
dialect: bigquery
sources:
  sessions:
    relation: {relation}
    time: occurred_at
    units: {{user: user_id}}
    events: [session, spend]
    properties: []
    dimensions: []
events:
  session: {{source: sessions, column: null}}
  spend: {{source: sessions, column: amount}}
properties: {{}}
dimensions: {{}}
measures:
  session_count: {{event: session, aggregation: count}}
metrics:
  sessions: {{kind: mean, unit: user, measure: session_count}}
"""


def _events(scratch, amount_type: str = "NUMERIC(12, 2)") -> None:
    scratch.sql(f"CREATE TABLE events(user_id STRING, occurred_at TIMESTAMP, amount {amount_type})")
    scratch.sql("INSERT events VALUES ('a', TIMESTAMP '2025-01-01 12:00:00+00', 1.5)")


def _model(scratch, tmp_path, relation: str = "{kind: table, name: events}"):
    path = tmp_path / "catalog.yaml"
    path.write_text(_CATALOG.format(relation=relation), encoding="utf-8")
    return tr.Model.from_catalog(
        path, con=scratch.con, readiness={"sessions": tr.Coverage("2025-01-01", "2025-01-03")}
    )


def _count(context, relation) -> int:
    return int(context.execute(context.resolve(relation).count()).as_py())


def test_private_session_holds_one_snapshot_and_never_uses_the_callers_client(
    bigquery_scratch,  # noqa: F811
    tmp_path,
):
    scratch = bigquery_scratch
    _events(scratch)
    model = _model(scratch, tmp_path)
    events = TableRef(kind="table", name="events")
    caller_config = scratch.con.client.default_query_job_config
    with open_execution(model.catalog, scratch.con) as context:
        assert _count(context, events) == 1
        scratch.sql("INSERT events VALUES ('b', TIMESTAMP '2025-01-01 13:00:00+00', 2)")
        assert _count(context, events) == 1  # committed after the snapshot began
        assert context.snapshot.backend == "bigquery"
        assert context.con.client is not scratch.con.client
    assert scratch.con.client.default_query_job_config is caller_config
    assert scratch.con.table("events").count().execute() == 2


def test_replaced_table_and_changed_view_refuse_after_binding(bigquery_scratch, tmp_path):  # noqa: F811
    scratch = bigquery_scratch
    _events(scratch)
    scratch.sql("CREATE VIEW shaped AS SELECT user_id, occurred_at, amount FROM events")
    model = _model(scratch, tmp_path)
    resolve = backend_provider(scratch.con).bind_resolver(model.catalog, scratch.con)
    for name, change in (
        ("events", "CREATE OR REPLACE TABLE events AS SELECT * FROM events"),
        (
            "shaped",
            "CREATE OR REPLACE VIEW shaped AS "
            "SELECT user_id, occurred_at, amount FROM events WHERE amount > 0",
        ),
    ):
        relation = TableRef(kind="table", name=name)
        resolve(relation)
        scratch.sql(change)
        with pytest.raises(TerrineError) as error:
            resolve(relation)
        assert error.value.code == "terrine.execution.invalid_input_binding"


@pytest.mark.parametrize(
    "call",
    [
        "`{dataset}`.same(user_id)",
        # A fully backticked path parses as one identifier, not a qualified call.
        "`{project}.{dataset}.same`(user_id)",
    ],
)
def test_view_calling_a_user_routine_is_refused(bigquery_scratch, tmp_path, call):  # noqa: F811
    scratch = bigquery_scratch
    _events(scratch)
    scratch.sql("CREATE FUNCTION same(x STRING) AS (x)")
    routine = call.format(project=scratch.project, dataset=scratch.dataset)
    scratch.sql(
        f"CREATE VIEW shaped AS SELECT {routine} AS user_id, occurred_at, amount FROM events"
    )
    with pytest.raises(TerrineError) as error:
        _model(scratch, tmp_path, relation="{kind: table, name: shaped}")
    assert error.value.code == "terrine.execution.snapshot_unavailable"


def test_unqualified_sql_relation_resolves_in_the_default_dataset(bigquery_scratch, tmp_path):  # noqa: F811
    scratch = bigquery_scratch
    _events(scratch)
    sql = "SELECT user_id, occurred_at, amount FROM events WHERE user_id <> 'z'"
    model = _model(scratch, tmp_path, relation='{kind: sql, sql: "' + sql + '"}')
    with open_execution(model.catalog, scratch.con) as context:
        assert _count(context, SqlQuery(kind="sql", sql=sql)) == 1


def test_unbounded_decimal_column_is_refused_at_binding(bigquery_scratch, tmp_path):  # noqa: F811
    scratch = bigquery_scratch
    _events(scratch, amount_type="BIGNUMERIC")
    with pytest.raises(TerrineError) as error:
        _model(scratch, tmp_path)
    assert error.value.code == "terrine.semantic.decimal_scale_required"


def test_published_datasets_refuse_a_bigquery_connection(bigquery_scratch):  # noqa: F811
    with pytest.raises(TerrineError) as error:
        tr.Dataset.open_generation("orders", "g1", con=bigquery_scratch.con, namespace="runtime")
    assert error.value.code == "terrine.backend.capability_unavailable"
    assert error.value.context["operation"] == "runtime_reader"
