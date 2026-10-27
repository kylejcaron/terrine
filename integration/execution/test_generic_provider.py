from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import ibis
import ibis.expr.types as ir
import pytest

from terrine import Coverage, Design, FixedDate, Model, UnitSource, agg
from terrine.errors import TerrineError
from terrine.semantic.models import SqlQuery, TableRef
from tests.execution.test_inputs import _catalog


@pytest.fixture
def sqlite():
    con = ibis.sqlite.connect(":memory:")
    con.create_table("numbers", {"value": [1]})
    con.create_table(
        "events",
        {
            "ts": [datetime(2025, 1, 1)],
            "user_id": [1],
            "value": [7],
        },
    )
    con.create_table("users", {"user_id": [1]})
    try:
        yield con
    finally:
        con.disconnect()


def catalog():
    definitions = _catalog().model_dump()
    definitions["dialect"] = "sqlite"
    definitions["facts"]["value"]["column"] = None
    definitions["measures"]["total"]["aggregation"] = "count"
    return type(_catalog()).model_validate(definitions)


def bind(con):
    from terrine import UnverifiedBackendWarning

    with pytest.warns(UnverifiedBackendWarning):
        return Model.from_catalog(
            catalog(),
            con=con,
            allow_unverified=True,
            readiness={"events": Coverage(date(2025, 1, 1), date(2025, 1, 2))},
            units={"user": UnitSource("users", "user_id")},
        )


def test_explicit_generic_model_and_real_integer_read(sqlite):
    from terrine.execution.inputs import open_execution

    with pytest.raises(TerrineError) as caught:
        Model.from_catalog(catalog(), con=sqlite, readiness={})
    assert caught.value.context["operation"] == "query"
    model = bind(sqlite)
    assert model.catalog.dialect == "sqlite"
    assert model.sources["events"].dialect == "generic"
    with open_execution(model.catalog, sqlite, engine=model._engine) as context:
        query = context.resolve(TableRef(kind="table", name="numbers"))
        assert context.execute(query).to_pylist() == [{"value": 1}]
        assert context.execute(query.aggregate(n=query.count())).to_pylist() == [{"n": 1}]
        assert model._for_execution(context)._engine is model._engine
    with pytest.raises(TerrineError) as second:
        Model.from_catalog(catalog(), con=sqlite, readiness={})
    assert second.value.context["operation"] == "query"


def test_public_count_frame_has_operation_specific_refusal(sqlite):
    model = bind(sqlite)
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, tzinfo=UTC)),
        features={"count": agg("total", window=("-1d", "0d"))},
    )
    try:
        result = design.build(model).materialize()
    except TerrineError as error:
        assert error.code == "terrine.backend.capability_unavailable"
        assert error.context["operation"] not in {"query", "dialect_admission"}
    else:
        assert result.table["count"].to_pylist() == [1]


def _source_reads(executed: list[str]) -> list[str]:
    """Row-reading statements against the sources; PRAGMA schema inspection binds them."""
    return [
        sql
        for sql in executed
        if ("users" in sql or "events" in sql) and "PRAGMA" not in sql.upper()
    ]


@pytest.mark.parametrize("entry", ["materialize", "preview", "tool_preview"])
def test_frame_admission_precedes_every_source_read(sqlite, monkeypatch, entry):
    """Every entry point that compiles a study and then executes its integrity
    checks admits the whole Frame first: on SQLite the decimal witness refuses the
    count Frame with zero roster or event reads, and a decimal-free Frame runs no
    witness at all."""
    from terrine import Budget, preview
    from terrine.execution import _generic

    statements: list[str] = []
    original = _generic.NoSnapshotSession._call_backend

    def recording(self, operation, action):
        statements.append(operation)
        return original(self, operation, action)

    monkeypatch.setattr(_generic.NoSnapshotSession, "_call_backend", recording)
    executed: list[str] = []
    sqlite.con.set_trace_callback(executed.append)
    model = bind(sqlite)

    def run(design):
        if entry == "materialize":
            return design.build(model).materialize()
        if entry == "preview":
            return preview(design, model, bounded_count=1)
        from terrine.agent import AgentPolicy, DatasetTools
        from terrine.runtime.saved_plan import ModelReference
        from terrine.spec.dataset import DatasetDefinition

        reference = ModelReference.from_model(model, owner_key="owner")
        tools = DatasetTools(
            model,
            AgentPolicy(
                preview=True,
                allowed_sources=frozenset({"events"}),
                allowed_catalogs=frozenset({reference.catalog_hash}),
                model_reference=reference,
                budget=Budget(strict_scan=False),
            ),
            namespace="agent",
        )
        definition = DatasetDefinition(schema_version=1, base_design=design, view=None)
        response = tools.preview_dataset(definition, bounded_count=1)
        if response.status != "ok":
            assert response.code is not None
            raise TerrineError(response.code, code=response.code, context=response.context)
        return response.output

    count = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, tzinfo=UTC)),
        features={"count": agg("total", window=("-1d", "0d"))},
    )
    with pytest.raises(TerrineError) as caught:
        run(count)
    assert caught.value.context["operation"] == "exact_state"
    assert _source_reads(executed) == []
    assert "exact_state" in statements

    executed.clear()
    statements.clear()
    roster = run(Design("user", FixedDate(datetime(2025, 1, 2, tzinfo=UTC))))
    if entry == "materialize":
        assert roster.table["unit"].to_pylist() == [1]
    else:
        assert roster.candidate_rows == 1
    assert _source_reads(executed)
    assert "exact_state" not in statements and "utc_wall_clock" not in statements


@pytest.fixture(params=["sqlite", "duckdb"])
def unregistered(request, sqlite):
    """Two genuine unregistered backends with a dimension table carrying a decimal
    column no lookup reads: Ibis SQLite, and a DuckDB backend subclass whose module
    is not the registered one, so admission treats it as unverified while the
    engine itself can compile timestamp deltas."""
    if request.param == "sqlite":
        sqlite.con.execute(
            "CREATE TABLE tiers (user_id INTEGER, tier TEXT, balance DECIMAL(12, 2))"
        )
        sqlite.con.execute("INSERT INTO tiers VALUES (1, 'gold', 12.50)")
        sqlite.con.commit()
        yield sqlite
        return
    from ibis.backends.duckdb import Backend

    class UnregisteredDuckDB(Backend):
        pass

    con = cast(Backend, UnregisteredDuckDB().connect())
    try:
        con.raw_sql(
            "CREATE TABLE events AS SELECT TIMESTAMP '2025-01-01' AS ts, 1::BIGINT AS user_id, "
            "7::BIGINT AS value"
        )
        con.raw_sql("CREATE TABLE users AS SELECT 1::BIGINT AS user_id")
        con.raw_sql(
            "CREATE TABLE tiers AS SELECT 1::BIGINT AS user_id, 'gold' AS tier, "
            "12.50::DECIMAL(12, 2) AS balance"
        )
        yield con
    finally:
        con.disconnect()


@pytest.mark.parametrize("entry", ["materialize", "preview"])
def test_asof_admission_inspects_only_the_columns_it_reads(unregistered, monkeypatch, entry):
    """A static string as-of lookup over a dimension table carrying an unused
    decimal column never asks for the decimal witness: admission inspects the
    lookup and its validation queries projected to their columns, not the whole
    table. Neither backend completes the lookup (Ibis's SQLite compiler has no
    timestamp-delta rule, so the UTC witness refuses it by code before any read;
    the DuckDB-based backend's portable timestamp SQL uses functions outside the
    fail-closed read-only allowlist), but the only witness ever attempted is the
    timestamp one, and no source row is read before a refusal."""
    from terrine import UnverifiedBackendWarning, asof, preview
    from terrine.execution import _generic

    statements: list[str] = []
    original = _generic.NoSnapshotSession._call_backend

    def recording(self, operation, action):
        statements.append(operation)
        return original(self, operation, action)

    monkeypatch.setattr(_generic.NoSnapshotSession, "_call_backend", recording)
    executed: list[str] = []
    if unregistered.name == "sqlite":
        unregistered.con.set_trace_callback(executed.append)
    assert unregistered.table("tiers").schema()["balance"].is_decimal()
    definitions = catalog().model_dump()
    definitions["dialect"] = unregistered.name
    definitions["sources"]["events"]["properties"] = ["tier"]
    definitions["sources"]["events"]["dimensions"] = ["accounts"]
    definitions["properties"]["tier"] = {
        "source": "events",
        "column": "tier",
        "dtype": "string",
        "as_of": "static",
        "dimension": "accounts",
    }
    definitions["dimensions"]["accounts"] = {
        "relation": {"kind": "table", "name": "tiers"},
        "source_entity": "user",
        "entity_column": "user_id",
        "properties": ["tier"],
    }
    with pytest.warns(UnverifiedBackendWarning):
        model = Model.from_catalog(
            type(catalog()).model_validate(definitions),
            con=unregistered,
            allow_unverified=True,
            readiness={"events": Coverage(date(2025, 1, 1), date(2025, 1, 2))},
            units={"user": UnitSource("users", "user_id")},
        )
    design = Design(
        "user",
        FixedDate(datetime(2025, 1, 2, tzinfo=UTC)),
        features={"tier": asof("tier", before="0s")},
    )
    try:
        if entry == "materialize":
            outcome = design.build(model).materialize().table["tier"].to_pylist()
        else:
            outcome = preview(design, model, bounded_count=1).candidate_rows
    except TerrineError as error:
        assert error.context.get("operation") != "exact_state"
        if unregistered.name == "sqlite":
            assert error.code == "terrine.backend.capability_unavailable"
            assert error.context["operation"] == "utc_wall_clock"
            assert _source_reads(executed) == []
        else:
            assert error.code == "terrine.sql.invalid_read_query"
    else:
        assert outcome in (["gold"], 1)
    assert "utc_wall_clock" in statements
    assert "exact_state" not in statements


def test_generic_materialization_round_trips_unverified_stamp(sqlite):
    import pyarrow as pa

    from terrine.data import MaterializedFrame

    result = (
        Design("user", FixedDate(datetime(2025, 1, 2, tzinfo=UTC)))
        .build(bind(sqlite))
        .materialize()
    )
    provenance = result.provenance
    assert result.table["unit"].to_pylist() == [1]
    assert provenance.execution_snapshot.backend == "sqlite"
    assert provenance.execution_snapshot.snapshot_id is None
    assert (provenance.engine_name, provenance.dialect, provenance.engine_status) == (
        "sqlite",
        "generic",
        "unverified",
    )
    stamped = result.to_arrow()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, stamped.schema) as writer:
        writer.write_table(stamped)
    restored = MaterializedFrame.from_arrow(pa.ipc.open_stream(sink.getvalue()).read_all())
    assert restored.provenance == provenance
    assert restored.roles == result.roles
    assert restored.table.equals(result.table)


def test_generic_context_is_statement_scoped_and_borrowed(sqlite):
    from terrine.execution.inputs import open_execution

    model = bind(sqlite)
    with open_execution(model.catalog, sqlite, engine=model._engine) as context:
        assert context.owner_con is sqlite and context.con is sqlite
        assert context.snapshot.snapshot_id is None
        assert context.snapshot.backend == "sqlite"
        assert context.snapshot.replay_equivalence == "unverified"
        assert all(value is None for value in context.snapshot.source_generations.values())
        assert context.execute(sqlite.table("numbers"))["value"].to_pylist() == [1]
        sqlite.con.execute("UPDATE numbers SET value = 2")
        assert context.execute(sqlite.table("numbers"))["value"].to_pylist() == [2]
    assert sqlite.to_pyarrow(sqlite.table("numbers"))["value"].to_pylist() == [2]


def test_sql_admission_uses_actual_dialect_and_precedes_effects(sqlite):
    from terrine.execution.inputs import open_execution

    model = bind(sqlite)
    with open_execution(model.catalog, sqlite, engine=model._engine) as context:
        relation = context.resolve(SqlQuery(kind="sql", sql="SELECT value FROM numbers"))
        assert context.execute(relation).to_pylist() == [{"value": 1}]
        for sql in ("DELETE FROM numbers", "SELECT load_extension('not_read_only')"):
            with pytest.raises(TerrineError) as caught:
                context.resolve(SqlQuery(kind="sql", sql=sql))
            assert caught.value.code == "terrine.sql.invalid_read_query"
        assert context.execute(sqlite.table("numbers")).to_pylist() == [{"value": 1}]
    from terrine import UnverifiedBackendWarning

    with pytest.warns(UnverifiedBackendWarning), pytest.raises(TerrineError) as mismatch:
        Model.from_catalog(
            catalog().model_copy(update={"dialect": "generic"}),
            con=sqlite,
            readiness={},
            allow_unverified=True,
        )
    assert mismatch.value.code == "terrine.semantic.dialect_mismatch"
    assert mismatch.value.context["execution"] == "sqlite"


def test_decimal_unavailability_refuses_only_decimal_query(sqlite):
    from terrine.execution.inputs import open_execution

    model = bind(sqlite)
    with open_execution(model.catalog, sqlite, engine=model._engine) as context:
        numbers = sqlite.table("numbers")
        assert context.execute(numbers.aggregate(n=numbers.count())).to_pylist() == [{"n": 1}]
        decimal = cast(
            ir.DecimalScalar,
            ibis.literal(Decimal("9007199254740993.000000001"), type="decimal(38,9)"),
        )
        with pytest.raises(TerrineError) as caught:
            context.execute((decimal - Decimal("9007199254740993")).name("residual"))
        assert caught.value.code == "terrine.backend.capability_unavailable"
        assert caught.value.context["operation"] == "exact_state"
        assert caught.value.context["backend"] == "sqlite"
        assert context.execute(numbers).to_pylist() == [{"value": 1}]


@pytest.mark.parametrize("finish", ["reader", "context"])
def test_generic_early_close_releases_sqlite_read_lock(tmp_path, finish):
    import sqlite3

    from ibis.backends.sql import SQLBackend
    from ibis.backends.sqlite import Backend

    from terrine import UnverifiedBackendWarning
    from terrine.backends.registry import resolve_engine
    from terrine.execution.inputs import open_execution

    class StreamingSQLite(Backend):
        to_pyarrow_batches = SQLBackend.to_pyarrow_batches

    path = tmp_path / "reader.sqlite"
    con = StreamingSQLite().connect(path)
    try:
        con.create_table("numbers", {"value": [1, 2, 3]})
        with pytest.warns(UnverifiedBackendWarning):
            engine = resolve_engine(con, allow_unverified=True)
        with open_execution(catalog(), con, engine=engine) as context:
            numbers = context.resolve(TableRef(kind="table", name="numbers"))
            reader = context.to_arrow_batches(numbers, chunk_size=1)
            assert next(reader).to_pylist() == [{"value": 1}]
            if finish == "reader":
                reader.close()
            else:
                context.close()
            with sqlite3.connect(path, timeout=0) as writer:
                writer.execute("UPDATE numbers SET value = value + 1")
                writer.commit()
        assert con.to_pyarrow(con.table("numbers"))["value"].to_pylist() == [2, 3, 4]
    finally:
        con.disconnect()


@pytest.mark.parametrize("operation", ["stage", "retained", "publish", "reopen"])
def test_protected_requests_refuse_before_effects(sqlite, tmp_path, operation):
    from terrine import Budget
    from terrine.dataset.reference import _require_runtime_reader, stage_frame
    from terrine.execution.inputs import publish_native_inputs
    from terrine.runtime.owner import RuntimeOwner
    from terrine.runtime.snapshot import SnapshotAuthority

    model = bind(sqlite)
    with pytest.raises(TerrineError) as caught:
        if operation == "stage":
            from types import SimpleNamespace

            stage_frame(SimpleNamespace(_model=model), "output")
        elif operation == "reopen":
            _require_runtime_reader(sqlite)
        elif operation == "retained":
            authority = SnapshotAuthority(
                RuntimeOwner(tmp_path / "state.duckdb", tmp_path / "snapshots")
            )
            authority.capture(
                model,
                design=Design("user", FixedDate("2025-01-02")),
                con=sqlite,
                namespace="output",
                run_id="read",
                budget=Budget(strict_scan=False),
            )
        else:
            publish_native_inputs(model.catalog, con=sqlite)
    assert caught.value.code == "terrine.backend.capability_unavailable"
    assert caught.value.context["status"] == "unsupported"
    assert not list(tmp_path.iterdir())
    assert sqlite.to_pyarrow(sqlite.table("numbers")).to_pylist() == [{"value": 1}]
