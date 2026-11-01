"""DuckDB native-function extraction agrees with structural JSON traversal."""

from __future__ import annotations

import json

import duckdb
import ibis

from terrine.data import Budget
from terrine.execution import inputs
from terrine.model import Model
from terrine.spec import BinnedMetricState, Design, EventAnchor, EventSequence, Schedule, StatePanel
from terrine.windows import Window
from tests._worlds import events_world
from tests.fixtures.readme import readme_design, readme_model, readme_tables


def _walk_names(raw, statement: str) -> frozenset[str]:
    payload = raw.execute(
        "SELECT system.main.json_serialize_sql(?)", [statement]
    ).fetchone()[0]
    parsed = json.loads(payload) if isinstance(payload, str) else payload
    names: set[str] = set()

    def collect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key.casefold() in {"function_name", "function", "operator_name"} and isinstance(
                    child, str
                ):
                    names.add(child.casefold())
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(parsed)
    return frozenset(names)


def _studies(tmp_path):
    readme_con = ibis.duckdb.connect(str(tmp_path / "readme.duckdb"))
    for name, table in readme_tables().items():
        readme_con.create_table(name, table)
    readme = readme_model(readme_con)

    world = events_world()
    state_con = ibis.duckdb.connect(str(tmp_path / "state.duckdb"))
    for name, table in world.tables.items():
        state_con.create_table(name, table)
    state_model = Model.from_catalog(
        world.catalog, con=state_con, units=world.units, readiness=world.readiness
    )
    state = Design(
        "user",
        Schedule("1d", "2025-01-03", "2025-01-04"),
        shape=StatePanel(BinnedMetricState("sessions", (0,), ("none", "some")), periods=2),
    )
    sequence = Design(
        "user",
        EventAnchor("session"),
        shape=EventSequence("session", Window("0s", "1d"), max_events=3),
    )
    sequence_model = Model.from_catalog(
        world.catalog,
        con=state_con,
        units=world.units,
        readiness=world.readiness,
        event_ids={"events": "event_id", "labels": "event_id"},
    )
    return ((readme_design(), readme), (state, state_model), (sequence, sequence_model))


def test_function_names_match_recursive_walk_for_compiled_studies(tmp_path, monkeypatch):
    seen: list[str] = []
    original = inputs._native.check_native_functions

    def record(raw, sql, dialect):
        seen.append(sql)
        return original(raw, sql, dialect)

    monkeypatch.setattr(inputs._native, "check_native_functions", record)
    for design, model in _studies(tmp_path):
        budget = (
            Budget(max_rows=100, max_output_bytes=1_000_000)
            if isinstance(design.shape, (StatePanel, EventSequence))
            else None
        )
        design.build(model).to_pyarrow(budget=budget)

    assert seen
    raw = duckdb.connect()
    try:
        statement = "SELECT a + b FROM (VALUES (1, 2)) AS values_table(a, b)"
        inputs._native.check_native_functions(raw, statement, None)
        from terrine.execution._native import _function_names

        for statement in seen:
            assert _function_names(raw, statement) == _walk_names(raw, statement)
    finally:
        raw.close()


def test_function_names_keep_macro_body_names_but_ignore_string_contents():
    from terrine.execution._native import _function_names

    raw = duckdb.connect()
    try:
        raw.execute("""CREATE MACRO local_macro(x) AS upper(x) || '{"function_name":"x"}'""")
        statement = """SELECT upper(x) || '{"function_name":"x"}'"""
        names = _function_names(raw, statement)
        assert names == _walk_names(raw, statement)
        assert "x" not in names
        assert "upper" in names
    finally:
        raw.close()


def test_function_names_does_not_hold_cache_lock_during_serialization(monkeypatch):
    from terrine.execution import _native

    raw = duckdb.connect()
    original = _native._rows

    def check_lock(raw, sql):
        assert not _native._NATIVE_FUNCTION_NAMES_LOCK.locked()
        return original(raw, sql)

    monkeypatch.setattr(_native, "_rows", check_lock)
    try:
        assert _native._function_names(raw, "SELECT 918273645") == frozenset()
    finally:
        raw.close()
