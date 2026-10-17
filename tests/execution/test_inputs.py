from __future__ import annotations

import duckdb
import pytest

from terrine.data import UnitSource
from terrine.errors import TerrineError
from terrine.execution.inputs import (
    bind_resolver,
    open_execution,
    publish_native_inputs,
    required_relations,
    verify_immutable_inputs,
)
from terrine.semantic.models import Catalog


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
                bind_resolver(catalog, foreign, input_binding=binding)
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
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": table},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": ["value"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {"value": {"source": "events", "column": "value"}},
            "properties": {},
            "dimensions": {},
            "measures": {"total": {"fact": "value", "aggregation": "sum"}},
            "metrics": {"total": {"kind": "mean", "entity": "user", "measure": "total"}},
        }
    )


def test_required_relations_treats_a_quoted_dotted_name_as_one_identifier():
    con = duckdb.connect()
    try:
        con.execute('CREATE TABLE "literal.dot" (ts TIMESTAMP, user_id INTEGER, value INTEGER)')
        catalog = _catalog(table="literal.dot")

        relations = required_relations(catalog, con)

        assert len(relations) == 1
        assert relations[0].kind == "table"
        assert relations[0].name.casefold() == "literal.dot"
        assert relations[0].schema is not None
        assert relations[0].schema.casefold() == "main"
    finally:
        con.close()


def test_required_relations_includes_empty_physical_leaf_behind_nested_views():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("CREATE VIEW v1 AS SELECT * FROM events WHERE false")
        con.execute("CREATE VIEW v2 AS WITH used AS (SELECT * FROM v1) SELECT * FROM used")
        catalog = _catalog(table="v2")

        relations = required_relations(catalog, con)

        assert [(r.kind, r.name.casefold()) for r in relations] == [("table", "events")]
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
            required_relations(_catalog(), con)
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

        relations = required_relations(Catalog.model_validate(data), con)

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
                "source_entity": "user",
                "entity_column": "user_id",
                "validity": {"changed_at": "ts"},
                "properties": [],
            }
        }

        relations = required_relations(
            Catalog.model_validate(definition),
            con,
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
            Model.from_catalog(_catalog(), con=con, readiness={})

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
    import ibis

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


def test_execution_refuses_persistent_aggregate_macro_shadow_before_query():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("INSERT INTO events VALUES ('2025-01-01', 1, 1), ('2025-01-02', 2, 2)")
        con.execute("CREATE MACRO sum(x) AS first(x)")
        with open_execution(_catalog(), con) as execution:
            query = execution.con.sql("SELECT SUM(value) AS total FROM events")
            with pytest.raises(TerrineError) as error:
                execution.execute(query)
            assert error.value.code == "terrine.execution.snapshot_unavailable"
    finally:
        con.close()


def test_open_execution_refuses_but_does_not_finish_a_caller_transaction():
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE events (ts TIMESTAMP, user_id INTEGER, value INTEGER)")
        con.execute("BEGIN")
        transaction_id = con.execute("SELECT system.main.current_transaction_id()").fetchall()[0][0]

        with pytest.raises(TerrineError) as error:
            with open_execution(_catalog(), con):
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
