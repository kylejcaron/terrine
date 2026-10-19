from __future__ import annotations

import tracemalloc

import ibis

from terrine.data import Budget, Coverage
from terrine.model import Model
from terrine.runtime.owner import RuntimeOwner
from terrine.runtime.snapshot import SnapshotAuthority
from terrine.semantic.models import Catalog
from terrine.spec import Design, FixedDate, agg


def test_capture_keeps_client_buffers_bounded_as_source_rows_increase(tmp_path, monkeypatch):
    path = tmp_path / "owner.duckdb"
    con = ibis.duckdb.connect(str(path))
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "events": {
                    "relation": {"kind": "table", "name": "events"},
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
