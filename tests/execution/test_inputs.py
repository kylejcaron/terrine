from __future__ import annotations

import duckdb
import ibis
import pytest

from terrine.data import Budget, UnitSource
from terrine.errors import TerrineError
from terrine.execution.inputs import (
    bind_resolver,
    open_execution,
    publish_native_inputs,
    required_relations,
    verify_immutable_inputs,
)
from terrine.semantic.models import Catalog


def test_strict_capture_budget_refuses_unknown_scan_before_reading_sources():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        with pytest.raises(TerrineError) as error:
            publish_native_inputs(
                _catalog(), con=con, budget=Budget(max_scan_bytes=1, strict_scan=True)
            )
        assert error.value.code == "terrine.budget.scan_unknown"
        row = con.execute("SELECT count(*) FROM events").fetchone()
        assert row is not None and row[0] == 0
    finally:
        con.close()


def test_published_execution_uses_private_session_and_sealed_root():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        catalog = _catalog()
        with publish_native_inputs(catalog, con=con) as published:
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            with open_execution(catalog, published.con, input_binding=binding) as execution:
                assert execution.con is not published.con
                published.con.con.execute("CREATE TEMP TABLE input_0 AS SELECT 99 AS value")
                relation = catalog.sources["events"].relation
                table = execution.resolve(relation)
                assert execution.execute(table).to_pylist()[0]["value"] == 3
    finally:
        con.close()


def test_execution_observes_schema_changes_between_operations():
    con = ibis.duckdb.connect()
    try:
        con.con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        catalog = _catalog()
        relation = catalog.sources["events"].relation
        with open_execution(catalog, con) as execution:
            assert execution.resolve(relation).columns == ("ts", "user_id", "value")
        con.con.execute("ALTER TABLE events ADD COLUMN revised BIGINT")
        with open_execution(catalog, con) as execution:
            assert execution.resolve(relation).columns == ("ts", "user_id", "value", "revised")
    finally:
        con.disconnect()


def test_execution_and_batches_preserve_decimal_nanosecond_and_null_values():
    from decimal import Decimal

    import pyarrow as pa

    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE events (ts TIMESTAMP_NS, user_id INTEGER, value DECIMAL(20,2))")
        con.raw_sql(
            "INSERT INTO events VALUES "
            "('1970-01-01 00:00:00.000000001', 1, 123456789012345.67), "
            "('1970-01-01 00:00:00.000000002', 2, NULL)"
        )
        catalog = _catalog()
        expected = pa.table(
            {
                "ts": pa.array([1, 2], type=pa.timestamp("ns")),
                "user_id": pa.array([1, 2], type=pa.int32()),
                "value": pa.array([Decimal("123456789012345.67"), None], type=pa.decimal128(20, 2)),
            }
        )
        with open_execution(catalog, con) as execution:
            query = execution.resolve(catalog.sources["events"].relation).order_by("user_id")
            assert execution.execute(query).equals(expected)
            with execution.to_arrow_batches(query, chunk_size=1) as reader:
                assert pa.Table.from_batches(reader).equals(expected)
    finally:
        con.disconnect()


def test_restoring_session_settings_invalidates_namespace_metadata():
    from terrine.execution import _native
    from terrine.execution._metadata import metadata_operation

    origin = duckdb.connect()
    runtime = origin.cursor()
    try:
        origin.execute("CREATE SCHEMA selected")
        origin.execute("USE memory.selected")
        with metadata_operation():
            assert _native._current_namespace(runtime)[1] == "main"
            _native.restore_semantic_settings(origin, runtime)
            assert _native._current_namespace(runtime)[1] == "selected"
    finally:
        runtime.close()
        origin.close()


def test_published_execution_revalidates_cached_relation_after_function_change():
    from terrine.execution._metadata import metadata_operation

    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        catalog = _catalog()
        relation = catalog.sources["events"].relation
        with publish_native_inputs(catalog, con=con) as published:
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            with open_execution(catalog, published.con, input_binding=binding) as execution:
                with metadata_operation():
                    assert execution.resolve(relation).columns == ("ts", "user_id", "value")
                    assert (
                        execution.execute(execution.resolve(relation)).to_pylist()[0]["value"] == 3
                    )
                published.con.con.execute("CREATE TEMP MACRO sum(x) AS first(x)")
                with metadata_operation():
                    with pytest.raises(TerrineError) as error:
                        execution.resolve(relation)
                    assert error.value.code == "terrine.execution.invalid_input_binding"
    finally:
        con.close()


def test_opaque_publication_refuses_side_effecting_functions_before_invocation():
    con = duckdb.connect()
    try:
        con.execute("CREATE SEQUENCE seq START 1")
        definition = _catalog().model_dump(mode="python")
        definition["sources"]["events"]["relation"] = {
            "kind": "sql",
            "sql": "SELECT nextval('seq') AS ts, 1 AS user_id, 1 AS value",
        }
        catalog = Catalog.model_validate(definition)
        with pytest.raises(TerrineError) as error:
            publish_native_inputs(catalog, con=con)
        assert error.value.code == "terrine.execution.snapshot_unavailable"
        assert con.execute("SELECT nextval('seq')").fetchall()[0][0] == 1
    finally:
        con.close()


def test_published_schema_admission_checks_function_namespace_before_temp_view(tmp_path):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        catalog = _catalog()
        with publish_native_inputs(catalog, con=con) as published:
            calls = []
            path = tmp_path / "probe.csv"
            path.write_text("ts,user_id,value\n2025-01-01,1,3\n")
            raw = published.con.con
            raw.create_function(
                "probe_path",
                lambda value: calls.append(value) or value,
                parameters=[duckdb.sqltype("VARCHAR")],
                return_type=duckdb.sqltype("VARCHAR"),
            )
            raw.execute(f"CREATE TEMP VIEW input_0 AS SELECT * FROM read_csv(probe_path('{path}'))")
            calls.clear()
            with pytest.raises(TerrineError) as error:
                verify_immutable_inputs(catalog, published.con, relations=published.relations)
            assert error.value.code == "terrine.execution.invalid_input_binding"
            assert calls == []
    finally:
        con.close()


def test_public_binding_rejects_foreign_backend_before_schema_lookup():
    con = duckdb.connect()
    foreign = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        catalog = _catalog()
        with publish_native_inputs(catalog, con=con) as published:
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            foreign.execute("CREATE TABLE input_0 (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
            with pytest.raises(TerrineError) as error:
                bind_resolver(catalog, ibis.duckdb.from_connection(foreign), input_binding=binding)
            assert error.value.code == "terrine.execution.invalid_input_binding"
    finally:
        foreign.close()
        con.close()


def test_opaque_view_publication_refuses_sequence_before_capture():
    con = duckdb.connect()
    try:
        con.execute("CREATE SEQUENCE seq START 1")
        con.execute(
            "CREATE VIEW unsafe_events AS SELECT nextval('seq') AS ts, 1 AS user_id, 1 AS value"
        )
        catalog = _catalog(table="unsafe_events")
        with pytest.raises(TerrineError) as error:
            publish_native_inputs(catalog, con=con)
        assert error.value.code == "terrine.execution.snapshot_unavailable"
        assert con.execute("SELECT nextval('seq')").fetchall()[0][0] == 1
    finally:
        con.close()


def test_identical_opaque_roots_share_one_publication_capture(tmp_path):
    con = duckdb.connect()
    path = tmp_path / "shared.csv"
    path.write_text("ts,user_id,value\n2025-01-01,1,7\n")
    try:
        definition = _catalog().model_dump(mode="python")
        query = f"SELECT * FROM read_csv('{path.as_posix()}')"
        definition["sources"]["events"]["relation"] = {"kind": "sql", "sql": query}
        duplicate = dict(definition["sources"]["events"])
        definition["sources"]["copy"] = duplicate
        catalog = Catalog.model_validate(definition)
        with publish_native_inputs(catalog, con=con) as published:
            assert len(published.relations) == 1
            assert len(required_relations(catalog, published.con)) == 1
            assert len(published.con.to_pyarrow(published.con.table("input_0")).to_pylist()) == 1
    finally:
        con.close()


def _catalog(*, table: str = "events") -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 2,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": table},
                    "time": "ts",
                    "units": {"user": "user_id"},
                    "events": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "events": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"event": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "unit": "user", "measure": "total"}},
        }
    )


def test_required_relations_treats_a_quoted_dotted_name_as_one_identifier():
    con = duckdb.connect()
    try:
        con.execute('CREATE TABLE "literal.dot" (ts TIMESTAMP, user_id INTEGER, value INTEGER)')
        catalog = _catalog(table="literal.dot")

        relations = required_relations(catalog, ibis.duckdb.from_connection(con))

        assert len(relations) == 1
        assert relations[0].kind == "table"
        assert relations[0].name.casefold() == "literal.dot"
        assert relations[0].schema is not None
        assert relations[0].schema.casefold() == "main"
    finally:
        con.close()


def test_required_relations_includes_empty_physical_leaf_behind_nested_views():
    from terrine.execution._metadata import metadata_operation

    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("CREATE VIEW v1 AS SELECT * FROM events WHERE false")
        con.execute("CREATE VIEW v2 AS WITH used AS (SELECT * FROM v1) SELECT * FROM used")
        catalog = _catalog(table="v2")

        backend = ibis.duckdb.from_connection(con)
        with metadata_operation():
            relations = required_relations(catalog, backend)
            assert [(r.kind, r.name.casefold()) for r in relations] == [("table", "events")]

        con.execute("CREATE TABLE revised (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("CREATE OR REPLACE VIEW v1 AS SELECT * FROM revised WHERE false")
        with metadata_operation():
            relations = required_relations(catalog, backend)
            assert [(r.kind, r.name.casefold()) for r in relations] == [("table", "revised")]
    finally:
        con.close()


def test_partial_table_reference_with_multiple_matches_is_refused():
    con = duckdb.connect()
    try:
        con.execute("CREATE SCHEMA one")
        con.execute("CREATE SCHEMA two")
        con.execute("CREATE TABLE one.events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("CREATE TABLE two.events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")

        with pytest.raises(TerrineError) as error:
            required_relations(_catalog(), ibis.duckdb.from_connection(con))
        assert error.value.code == "terrine.execution.snapshot_unavailable"
    finally:
        con.close()


def test_sql_query_factors_are_admitted_as_structured_names():
    con = duckdb.connect()
    try:
        con.execute("CREATE SCHEMA source")
        con.execute("CREATE TABLE source.events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        catalog = _catalog()
        data = catalog.model_dump(mode="python")
        data["sources"]["events"]["relation"] = {
            "kind": "sql",
            "sql": "WITH used AS (SELECT * FROM source.events) SELECT * FROM used",
        }

        relations = required_relations(
            Catalog.model_validate(data), ibis.duckdb.from_connection(con)
        )

        assert [(r.schema, r.name.casefold()) for r in relations] == [("source", "events")]
    finally:
        con.close()


def test_required_relations_covers_dimension_and_supplied_roster_roots():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("CREATE TABLE accounts (ts TIMESTAMP, user_id INTEGER)")
        con.execute("CREATE TABLE roster (user_id INTEGER)")
        definition = _catalog().model_dump(mode="python")
        definition["sources"]["events"]["dimensions"] = ["accounts"]
        definition["dimensions"] = {
            "accounts": {
                "relation": {"kind": "table", "name": "accounts"},
                "unit": "user",
                "key": "user_id",
                "validity": {"changed_at": "ts"},
                "properties": [],
            }
        }

        relations = required_relations(
            Catalog.model_validate(definition),
            ibis.duckdb.from_connection(con),
            units={"user": UnitSource("roster", "user_id")},
        )

        assert {relation.name.casefold() for relation in relations} == {
            "events",
            "accounts",
            "roster",
        }
    finally:
        con.close()


def test_published_input_is_detached_from_later_source_mutation():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        catalog = _catalog()

        with publish_native_inputs(catalog, con=con) as published:
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            con.execute("UPDATE events SET value = 9")
            assert binding is not None
            relation = catalog.sources["events"].relation
            table = bind_resolver(catalog, published.con, input_binding=binding)(relation)
            assert published.con.to_pyarrow(table).to_pylist()[0]["value"] == 3
    finally:
        con.close()


def test_published_context_close_revokes_new_authority():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        catalog = _catalog()
        published = publish_native_inputs(catalog, con=con)
        published.close()

        with pytest.raises(TerrineError) as error:
            verify_immutable_inputs(catalog, published.con, relations=published.relations)

        assert error.value.code == "terrine.execution.invalid_input_binding"
    finally:
        con.close()


def test_model_rejects_data_dependent_view_before_calling_path_function(tmp_path):
    from terrine.model import Model

    con = duckdb.connect()
    calls = []
    path = tmp_path / "source.csv"
    path.write_text("ts,user_id,value\n2025-01-01,1,3\n")
    try:
        con.create_function(
            "count_path",
            lambda value: calls.append(value) or value,
            parameters=[duckdb.sqltype("VARCHAR")],
            return_type=duckdb.sqltype("VARCHAR"),
        )
        con.execute(
            f"CREATE VIEW events AS SELECT * FROM read_csv(count_path('{path.as_posix()}'))"
        )
        calls.clear()

        with pytest.raises(TerrineError):
            Model.from_catalog(_catalog(), con=ibis.duckdb.from_connection(con), readiness={})

        assert calls == []
    finally:
        con.close()


def test_changed_view_is_rejected_by_build_and_preview_without_running_its_function(
    native_inputs, tmp_path
):

    import terrine as tr
    from terrine.data import Coverage
    from terrine.model import Model
    from terrine.spec import Design, EventAnchor

    catalog, con = native_inputs
    definitions = catalog.model_dump(mode="python")
    definitions["sources"]["events"]["relation"] = {
        "kind": "table",
        "name": "admitted_events",
    }
    path = tmp_path / "source.csv"
    path.write_text("ts,user_id,value\n2025-01-01,a,1\n")
    calls = []
    raw = con.con
    raw.execute(
        "CREATE VIEW admitted_events AS "
        "SELECT user_id, ts, event_id, alternate_event_id FROM events"
    )
    raw.create_function(
        "count_path",
        lambda value: calls.append(value) or value,
        parameters=[duckdb.sqltype("VARCHAR")],
        return_type=duckdb.sqltype("VARCHAR"),
    )
    model = Model.from_catalog(
        Catalog.model_validate(definitions),
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    raw.execute(
        "CREATE OR REPLACE VIEW admitted_events AS "
        f"SELECT * FROM read_csv(count_path('{path.as_posix()}'))"
    )
    calls.clear()
    design = Design("user", EventAnchor("session"))

    with pytest.raises(TerrineError):
        design.build(model)
    with pytest.raises(TerrineError):
        tr.preview(design, model)

    assert calls == []


def test_open_execution_preserves_origin_timezone_after_ibis_wrap():

    con = ibis.duckdb.connect()
    raw = con.con
    raw.execute("SET TimeZone='America/New_York'")
    raw.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
    try:
        with open_execution(_catalog(), con) as execution:
            value = execution.con.to_pyarrow(
                execution.con.sql(
                    "SELECT TIMESTAMPTZ '2025-07-01 12:00:00+00'::TIMESTAMP AS local_time"
                )
            ).to_pylist()[0]["local_time"]
        assert (
            value
            == con.to_pyarrow(
                con.sql("SELECT TIMESTAMPTZ '2025-07-01 12:00:00+00'::TIMESTAMP AS local_time")
            ).to_pylist()[0]["local_time"]
        )
    finally:
        con.disconnect()


@pytest.mark.parametrize("stream", [False, True])
def test_execution_refuses_persistent_aggregate_macro_shadow_before_query(stream):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 1), ('2025-01-02', 2, 2)")
        con.execute("CREATE MACRO sum(x) AS first(x)")
        with open_execution(_catalog(), ibis.duckdb.from_connection(con)) as execution:
            query = execution.con.sql("SELECT SUM(value) AS total FROM events")
            with pytest.raises(TerrineError) as error:
                if stream:
                    execution.to_arrow_batches(query)
                else:
                    execution.execute(query)
            assert error.value.code == "terrine.execution.snapshot_unavailable"
    finally:
        con.close()


def test_open_execution_refuses_but_does_not_finish_a_caller_transaction():
    con = duckdb.connect()
    backend = ibis.duckdb.from_connection(con)
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("BEGIN")
        transaction_id = con.execute("SELECT system.main.current_transaction_id()").fetchall()[0][0]

        with pytest.raises(TerrineError) as error:
            with open_execution(_catalog(), backend):
                pass

        assert error.value.code == "terrine.execution.snapshot_unavailable"
        assert (
            con.execute("SELECT system.main.current_transaction_id()").fetchall()[0][0]
            == transaction_id
        )
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        con.execute("ROLLBACK")
    finally:
        con.close()


@pytest.mark.parametrize(
    "query",
    ("SELECT * FROM events;", "SELECT * FROM events -- trailing comment"),
)
def test_sql_roots_with_trailing_syntax_capture_natively(query):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        definition = _catalog().model_dump(mode="python")
        definition["sources"]["events"]["relation"] = {"kind": "sql", "sql": query}
        catalog = Catalog.model_validate(definition)
        with publish_native_inputs(catalog, con=con) as published:
            assert published.con.table("input_0").count().execute() == 1
    finally:
        con.close()


def test_sql_capture_preserves_function_spelling_when_rendering_query():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3)")
        con.execute("CREATE SEQUENCE seq START 1")
        con.execute("CREATE MACRO power(x, y) AS nextval('seq')")
        definition = _catalog().model_dump(mode="python")
        definition["sources"]["events"]["relation"] = {
            "kind": "sql",
            "sql": "SELECT ts, user_id, pow(value, 2) AS value FROM events",
        }
        catalog = Catalog.model_validate(definition)
        with publish_native_inputs(catalog, con=con) as published:
            table = published.con.table("input_0")
            rows = published.con.to_pyarrow(table).to_pylist()
            assert rows[0]["value"] == 9
        row = con.execute("SELECT nextval('seq')").fetchone()
        assert row is not None and row[0] == 1
    finally:
        con.close()


def test_published_sql_result_replaces_its_original_dependency():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 3), ('2025-01-02', 2, 0)")
        definition = _catalog().model_dump(mode="python")
        sql = "SELECT * FROM events WHERE value > 0"
        definition["sources"]["events"]["relation"] = {"kind": "sql", "sql": sql}
        catalog = Catalog.model_validate(definition)

        with publish_native_inputs(catalog, con=con) as published:
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            assert [identity.kind for identity in published.relations] == ["sql_result"]
            assert set(required_relations(catalog, published.con)) == set(published.relations)
            con.execute("UPDATE events SET value = 9")
            relation = catalog.sources["events"].relation
            table = bind_resolver(catalog, published.con, input_binding=binding)(relation)

            assert [row["value"] for row in published.con.to_pyarrow(table).to_pylist()] == [3]
    finally:
        con.close()


def test_explicit_publication_seals_one_opaque_sql_result(tmp_path):
    con = duckdb.connect()
    path = tmp_path / "opaque.csv"
    path.write_text("ts,user_id,value\n2025-01-01,1,7\n")
    try:
        sql = f"SELECT * FROM read_csv('{path.as_posix()}')"
        definition = _catalog().model_dump(mode="python")
        definition["sources"]["events"]["relation"] = {"kind": "sql", "sql": sql}
        catalog = Catalog.model_validate(definition)

        with publish_native_inputs(catalog, con=con) as published:
            assert len(published.relations) == 1
            assert next(iter(published.relations)).kind == "sql_result"
            binding = verify_immutable_inputs(catalog, published.con, relations=published.relations)
            path.unlink()
            relation = catalog.sources["events"].relation
            with open_execution(catalog, published.con, input_binding=binding) as execution:
                table = execution.resolve(relation)
                assert execution.execute(table).to_pylist()[0]["value"] == 7
    finally:
        con.close()
