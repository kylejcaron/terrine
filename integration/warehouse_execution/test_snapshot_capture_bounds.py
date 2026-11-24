from __future__ import annotations

import tracemalloc

import duckdb
import ibis
import pytest

from terrine import preview
from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg


def test_interrupted_capture_releases_only_its_own_transaction(monkeypatch):
    import terrine as tr
    from terrine.execution import _publish
    from terrine.execution.inputs import publish_native_inputs

    con = ibis.duckdb.connect()
    try:
        con.raw_sql("CREATE TABLE users AS SELECT 1 AS id")
        con.raw_sql("CREATE TABLE events AS SELECT 1 AS id, TIMESTAMP '2025-01-01' AS ts, 7 AS n")
        model = Model.from_tables(
            con, units={"user": UnitSource("users", "id")},
            events={"event": tr.Events(
                "events", key="id", time="ts", metrics={"n": tr.sum("n")},
                complete=("2025-01-01", "2025-01-03"),
            )},
        )
        design = Design(
            "user", FixedDate("2025-01-02"), features={"v": agg("n", window=("-1d", "0s"))}
        )
        admit = _publish._admit_roots

        def interrupt_inside_transaction(catalog, origin, units, roots):
            admit(catalog, origin, units, roots)
            sql = "SELECT current_transaction_id()"
            if origin.execute(sql).fetchone() == origin.execute(sql).fetchone():
                raise KeyboardInterrupt

        with monkeypatch.context() as patch:
            patch.setattr(_publish, "_admit_roots", interrupt_inside_transaction)
            with pytest.raises(KeyboardInterrupt):
                publish_native_inputs(model.catalog, con=con, units=model.units)
        assert design.build(model).materialize().table["v"].to_pylist() == [7]

        con.raw_sql("BEGIN")
        con.raw_sql("UPDATE events SET n=70")
        with pytest.raises(tr.TerrineError) as refused:
            publish_native_inputs(model.catalog, con=con, units=model.units)
        assert refused.value.code == "terrine.execution.snapshot_unavailable"
        assert con.con.execute("SELECT n FROM events").fetchone() == (70,)
        con.raw_sql("ROLLBACK")
        assert con.con.execute("SELECT n FROM events").fetchone() == (7,)
    finally:
        con.disconnect()


def test_capture_keeps_client_buffers_bounded_as_source_rows_increase(tmp_path, monkeypatch):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    catalog = Catalog.model_validate(
        {
            "schema_version": 3,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
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
    authority = SnapshotAuthority(
        RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
    )
    design = Design(
        "user", FixedDate("2025-01-02"), features={"total": agg("total", window=("-1d", "0s"))}
    )
    peaks = []

    def refuse_client_rows(*args, **kwargs):
        raise AssertionError("capture must not transfer source rows through the Ibis client")

    try:
        con.raw_sql(
            "CREATE TABLE events AS SELECT TIMESTAMP '2025-01-01' AS ts, "
            "1::BIGINT AS user_id, i AS value FROM range(0) t(i)"
        )
        initial_model = Model.from_catalog(
            catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
        )
        authority.capture(
            initial_model,
            design=design,
            con=con,
            namespace="runtime",
            run_id="initialize",
            budget=Budget(max_rows=1, max_output_bytes=1),
        )
        for rows in (1_000, 100_000, 1_000_000):
            con.raw_sql(
                "CREATE OR REPLACE TABLE events AS "
                "SELECT TIMESTAMP '2025-01-01' AS ts, 1::BIGINT AS user_id, i AS value "
                f"FROM range({rows}) t(i)"
            )
            model = Model.from_catalog(
                catalog, con=con, readiness={"events": Coverage("2025-01-01", "2025-02-01")}
            )
            with monkeypatch.context() as guarded:
                for method in ("execute", "to_pyarrow", "to_pyarrow_batches"):
                    guarded.setattr(type(con), method, refuse_client_rows)
                tracemalloc.start()
                try:
                    retained = authority.capture(
                        model,
                        design=design,
                        con=con,
                        namespace="runtime",
                        run_id=f"capture-{rows}",
                        budget=Budget(max_rows=1, max_output_bytes=1),
                    )
                    peaks.append(tracemalloc.get_traced_memory()[1])
                finally:
                    tracemalloc.stop()
            execution = authority.reopen(retained, con=con, namespace="runtime")
            try:
                relation = execution.resolve(catalog.sources["events"].relation)
                count = execution.execute(relation.aggregate(rows=relation.count())).to_pylist()
                assert count == [{"rows": rows}]
            finally:
                execution.close()
            print(f"capture_rows={rows} python_peak_bytes={peaks[-1]}")
        assert max(peaks) < 16 * 1024 * 1024
    finally:
        con.disconnect()


def _order_model(con, relation):
    alias = {"kind": "table", "name": "ORDERS"} if relation["kind"] == "table" else relation
    definition = {
        "schema_version": 4,
        "day_boundary": "UTC",
        "dialect": "duckdb",
        "sources": {
            name: {
                "relation": root,
                "time": "ts",
                "units": {"user": "user_id"},
                "events": [name],
                "properties": [],
                "dimensions": ["order_keys"] if name == "orders" else [],
            }
            for name, root in (
                ("orders", relation),
                ("orders_alias", alias),
                ("unused", {"kind": "table", "name": "unused"}),
            )
        },
        "events": {
            name: {"source": name, "column": None} for name in ("orders", "orders_alias", "unused")
        },
        "properties": {},
        "dimensions": {
            "order_keys": {
                "relation": {"kind": "table", "name": "order_keys"},
                "unit": None,
                "foreign_key": "user_id",
                "key": "user_id",
                "properties": [],
            }
        },
        "measures": {
            name: {"event": name, "aggregation": "count"} for name in ("orders", "orders_alias")
        },
        "metrics": {
            name: {"kind": "mean", "unit": "user", "measure": name}
            for name in ("orders", "orders_alias")
        },
    }
    return Model.from_catalog(
        definition,
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={
            name: Coverage("2025-01-01", "2025-02-01")
            for name in ("orders", "orders_alias", "unused")
        },
    )


def _assert_preview_matches_image(model, design, owner, footprint, *, relation_name):
    receipt = SnapshotAuthority(owner).capture(
        model, design=design, con=model.con, namespace="runtime", run_id="compare", budget=Budget()
    )
    with duckdb.connect(receipt.artifact_path, read_only=True) as image:
        tables = {entry["identity"]["name"]: entry["storage_name"] for entry in receipt.roots}
        assert set(tables) == {relation_name, "order_keys", "users"}
        assert len(image.execute("SHOW TABLES").fetchall()) == 3
        for root in footprint.roots:
            storage_name = tables[root.name]
            retained = image.table(storage_name)
            assert tuple(retained.columns) == root.columns
            expected_users = [1, 1, 1] if root.name == relation_name else [1, 2]
            assert sorted(retained.select("user_id").fetchall()) == [(i,) for i in expected_users]
            if root.name == relation_name:
                times = retained.select("ts").fetchall()
                dates = sorted(ts.date().isoformat() for (ts,) in times)
                assert dates == ["2025-01-01", "2025-01-10", "2025-01-20"]


def _metadata_only(method):
    def guarded(connection, statement, *args, **kwargs):
        assert statement.startswith(("DESCRIBE ", "SELECT ")), statement
        if not statement.startswith("DESCRIBE "):
            assert statement.startswith(
                ("SELECT system.main.current_", "SELECT system.main.json_serialize_sql(")
            ) or any(
                f"FROM system.main.duckdb_{table}()" in statement
                for table in ("databases", "tables", "views", "functions")
            ), statement
        return method(connection, statement, *args, **kwargs)

    return guarded


def test_preview_matches_whole_snapshot_and_user_projected_result(tmp_path, monkeypatch):
    path = tmp_path / "owner.duckdb"
    owner = RuntimeOwner(state_path=path, snapshot_root=tmp_path / "snapshots")
    monkeypatch.setenv("TERRINE_RUNTIME_SNAPSHOT_ROOT", str(owner.snapshot_root))
    con = ibis.duckdb.connect(str(path))
    try:
        con.raw_sql(
            "CREATE TABLE orders AS SELECT * FROM (VALUES "
            "(TIMESTAMP '2025-01-01', 1, 'old'), (TIMESTAMP '2025-01-10', 1, 'current'), "
            "(TIMESTAMP '2025-01-20', 1, 'later')) t(ts, user_id, unrelated_note)"
        )
        con.raw_sql("CREATE TABLE unused AS SELECT ts, user_id FROM orders")
        con.raw_sql("CREATE TABLE users AS SELECT unnest([1, 2]) AS user_id")
        con.raw_sql("CREATE TABLE order_keys AS SELECT user_id FROM users")
        wide = _order_model(con, {"kind": "table", "name": "orders"})
        narrow = _order_model(con, {"kind": "sql", "sql": "SELECT ts, user_id FROM orders"})
        design = Design(
            "user",
            FixedDate("2025-01-11"),
            features={name: agg(name, window=("-1d", "0s")) for name in ("orders", "orders_alias")},
        )

        def refuse_client_rows(*args, **kwargs):
            raise AssertionError("ordinary preview must not read source rows")

        with monkeypatch.context() as guarded:
            for method in ("execute", "to_pyarrow", "to_pyarrow_batches"):
                guarded.setattr(type(con), method, refuse_client_rows)
            for method in ("execute", "sql"):
                native = duckdb.DuckDBPyConnection
                guarded.setattr(native, method, _metadata_only(getattr(native, method)))
            report = preview(design, wide)
            narrow_footprint = preview(design, narrow).staging_footprint
        assert not owner.snapshot_root.exists()
        footprint = report.staging_footprint
        assert footprint is not None and footprint.status == "described"
        assert footprint.retained_bytes is None
        roots = {root.name: root for root in footprint.roots}
        assert set(roots) == {"orders", "order_keys", "users"}
        assert roots["orders"].row_scope == "all_relation_rows"
        assert roots["orders"].columns == ("ts", "user_id", "unrelated_note")
        assert {use.name for use in roots["orders"].uses if use.role == "fact"} == {
            "orders",
            "orders_alias",
        }
        assert all(use.coverage == report.coverage[use.name] for use in roots["orders"].uses)
        assert roots["order_keys"].check_only is True
        assert any(use.role == "check" for use in roots["order_keys"].uses)
        assert {use.role for use in roots["users"].uses} >= {"roster"}
        _assert_preview_matches_image(wide, design, owner, footprint, relation_name="orders")

        assert narrow_footprint is not None and narrow_footprint.status == "described"
        result = next(root for root in narrow_footprint.roots if root.kind == "sql_result")
        assert result.row_scope == "all_result_rows"
        assert result.columns == ("ts", "user_id")
        assert "SELECT" not in repr(narrow_footprint)
        _assert_preview_matches_image(
            narrow, design, owner, narrow_footprint, relation_name=result.name
        )

        for model in (wide, narrow):
            rows = (
                design.build(model)
                .materialize()
                .table.select(
                    ["unit", "orders", "orders__complete", "orders_alias", "orders_alias__complete"]
                )
                .to_pylist()
            )
            assert sorted(
                (
                    r["unit"],
                    r["orders"],
                    r["orders__complete"],
                    r["orders_alias"],
                    r["orders_alias__complete"],
                )
                for r in rows
            ) == [(1, 1, True, 1, True), (2, 0, True, 0, True)]
    finally:
        con.disconnect()
