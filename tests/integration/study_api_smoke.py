"""Run the public study workflow against native DuckDB without optional semantics."""

from __future__ import annotations

import importlib.abc
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory


class _NoIncrement(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "increment" or fullname.startswith("increment."):
            raise AssertionError("native study workflow imported optional semantics")
        return None


sys.meta_path.insert(0, _NoIncrement())

# Imports follow the blocker so native-path isolation is exercised during import.
import ibis  # noqa: E402
import pyarrow as pa  # noqa: E402
import yaml  # noqa: E402

import terrine as tr  # noqa: E402
from terrine.data import MaterializedFrame  # noqa: E402
from terrine.execution import publish_native_inputs  # noqa: E402
from terrine.execution.inputs import ExecutionContext  # noqa: E402


def _definitions():
    return {
        "schema_version": 1,
        "day_boundary": "UTC",
        "dialect": "duckdb",
        "sources": {
            "events": {
                "relation": {
                    "kind": "sql",
                    "sql": "SELECT user_id, ts, value FROM events WHERE value >= 0",
                },
                "timestamp": "ts",
                "entities": {"user": "user_id"},
                "facts": ["session"],
                "properties": ["plan", "adopted"],
                "dimensions": ["plans"],
            }
        },
        "facts": {"session": {"source": "events", "column": None}},
        "properties": {
            name: {
                "source": "events",
                "column": name,
                "dtype": dtype,
                "as_of": "event_time",
                "dimension": "plans",
            }
            for name, dtype in (("plan", "string"), ("adopted", "bool"))
        },
        "dimensions": {
            "plans": {
                "relation": {"kind": "table", "name": "plans"},
                "source_entity": "user",
                "entity_column": "user_id",
                "validity": {"valid_from": "valid_from", "valid_to": "valid_to"},
                "properties": ["plan", "adopted"],
            }
        },
        "measures": {"count": {"fact": "session", "aggregation": "count"}},
        "metrics": {"sessions": {"kind": "mean", "entity": "user", "measure": "count"}},
    }


def _assert_result(result):
    assert result.table.column("unit").to_pylist() == ["a"]
    assert result.table.column("pre sessions").to_pylist() == [1]
    assert result.table.column("post").to_pylist() == [2]
    assert result.table.column("previous plan").to_pylist() == [None]
    assert result.table.column("treatment").to_pylist() == [True]
    summary = result.provenance.eligibility_summary
    assert (summary.candidate, summary.excluded_false, summary.unknown, summary.retained) == (
        3,
        2,
        0,
        1,
    )
    meta = result.columns["previous plan"]
    assert result.table.column(meta.complete_column).to_pylist() == [True]
    assert result.provenance.feature_cutoffs["previous plan"].strict is True
    assert (
        result.provenance.feature_cutoffs["previous plan"].knowledge
        == "retrospective_effective_time"
    )
    assert result.provenance.execution_snapshot.replay_equivalence == "unverified"
    restored = MaterializedFrame.from_arrow(result.to_arrow())
    assert restored.table.equals(result.table)
    assert restored.provenance == result.provenance


def main():
    con = ibis.duckdb.connect()
    con.raw_sql("SET TimeZone = 'America/New_York'")
    writer = con.con.cursor()
    t0 = datetime(2025, 1, 3, 12, tzinfo=UTC)
    con.create_table(
        "events",
        pa.table(
            {
                "user_id": ["a", "a", "a", "b"],
                "ts": [
                    datetime(2025, 1, 3, 11, 59, 59, tzinfo=UTC),
                    t0,
                    datetime(2025, 1, 3, 12, 15, tzinfo=UTC),
                    t0,
                ],
                "value": [1, 1, 1, 1],
            }
        ),
    )
    con.create_table("users", pa.table({"user_id": ["a", "b", "c"]}))
    con.create_table(
        "plans",
        pa.table(
            {
                "user_id": ["a", "a", "b", "c"],
                "valid_from": [
                    datetime(2025, 1, 1, tzinfo=UTC),
                    t0,
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ],
                "valid_to": pa.array([t0, None, None, None], type=pa.timestamp("us", tz="UTC")),
                "plan": [None, "paid", "free", None],
                "adopted": [False, True, False, False],
            }
        ),
    )
    design = tr.Design(
        "user",
        tr.FixedDate(t0),
        features={
            "pre sessions": tr.agg("sessions", window=("-1h", "0s")),
            "previous plan": tr.asof("plan", before="0s"),
        },
        outcomes={"post": tr.agg("sessions", window=("0s", "1h"))},
        eligibility={"history": tr.gt(tr.agg("sessions", window=("-1h", "0s")), 0)},
        treatment=tr.Treatment("adopted", known_at_t0=True),
    )
    units = {"user": tr.UnitSource("users", "user_id")}
    readiness = {"events": tr.Coverage("2025-01-01", "2025-01-31")}
    budget = tr.Budget(max_rows=3, max_output_bytes=100_000)
    try:
        with TemporaryDirectory(prefix="terrine-smoke-") as directory:
            catalog_path = Path(directory) / "catalog.yaml"
            catalog_path.write_text(yaml.safe_dump(_definitions()), encoding="utf-8")
            model = tr.Model.from_catalog(catalog_path, con=con, units=units, readiness=readiness)
            report = tr.preview(design, model)
            assert report.candidate_rows is None and report.scan_bytes is None
            frame = design.build(model)
            original_execute = ExecutionContext.execute
            changed = False

            def execute_then_write(self, query):
                nonlocal changed
                result = original_execute(self, query)
                if not changed:
                    # Commit a real competing write after the first actual check.
                    writer.execute("UPDATE events SET value = -1")
                    changed = True
                return result

            ExecutionContext.execute = execute_then_write
            try:
                ordinary = frame.materialize(budget)
            finally:
                ExecutionContext.execute = original_execute
            assert changed
            assert writer.execute("SELECT DISTINCT value FROM events").fetchall() == [(-1,)]
            _assert_result(ordinary)
            assert ordinary.table.schema.field("t0").type == pa.timestamp("us", tz="UTC")
            assert ordinary.table.column("t0").to_pylist() == [t0]
            assert all(value is None for value in ordinary.provenance.source_generations.values())
            assert ordinary.provenance.execution_snapshot.snapshot_id is None
            writer.execute("UPDATE events SET value = 1")
            with publish_native_inputs(model.catalog, con=con, units=units) as inputs:
                binding = model.catalog.verify_immutable_inputs(
                    inputs.con, units=units, relations=inputs.relations
                )
                pinned_model = tr.Model.from_catalog(
                    model.catalog,
                    con=inputs.con,
                    units=units,
                    readiness=readiness,
                    input_binding=binding,
                )
                writer.execute("UPDATE events SET value = -1")
                pinned = design.build(pinned_model).materialize(budget)
                _assert_result(pinned)
                assert pinned.provenance.model_hash == ordinary.provenance.model_hash
                assert set(pinned.provenance.source_generations) == {
                    '["source","events"]',
                    '["dimension","plans"]',
                    '["roster","user"]',
                }
                assert all(
                    value is not None for value in pinned.provenance.source_generations.values()
                )
                assert pinned.provenance.execution_snapshot.input_binding_id is not None
            print(
                json.dumps(
                    {
                        "ordinary_rows": ordinary.table.num_rows,
                        "pinned_rows": pinned.table.num_rows,
                        "pre": 1,
                        "post": 2,
                        "concurrent_write_excluded": True,
                        "replay_equivalence": "unverified",
                        "execution_timezone": "America/New_York",
                        "package": str(Path(tr.__file__).resolve()),
                    },
                    sort_keys=True,
                )
            )
    finally:
        writer.close()
        con.disconnect()


if __name__ == "__main__":
    main()
