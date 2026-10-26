"""Build a bounded weekly engagement-state panel with the native DuckDB backend."""

from datetime import UTC, datetime, timedelta

import ibis
import pyarrow as pa

import terrine as tr
from terrine.data import MaterializedFrame
from terrine.spec import BinnedMetricState, Design, Schedule, StatePanel, agg


def main() -> None:
    con = ibis.duckdb.connect()
    try:
        users = pa.table({"user_id": ["u1", "u2"]})
        start = datetime(2025, 1, 6, tzinfo=UTC)
        event_times = [
            start - timedelta(days=1),
            start + timedelta(days=1),
            start + timedelta(days=9),
            start + timedelta(days=16),
            start + timedelta(days=17),
            start + timedelta(days=30),
            start + timedelta(days=31),
        ]
        events = pa.table(
            {
                "user_id": ["u1", "u1", "u1", "u1", "u1", "u2", "u2"],
                "ts": pa.array(event_times, type=pa.timestamp("us", tz="UTC")),
                "event_id": [f"e{i}" for i in range(len(event_times))],
            }
        )
        statuses = pa.table(
            {
                "user_id": ["u1", "u1", "u1", "u2"],
                "ts": pa.array(
                    [
                        start,
                        start + timedelta(days=77),
                        start + timedelta(days=84),
                        start,
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "status": ["active", "active", "closed", "active"],
            }
        )
        con.create_table("users", users)
        con.create_table("events", events)
        con.create_table("assessments", statuses)
        catalog = {
                "schema_version": 1,
                "day_boundary": "UTC",
                "dialect": "duckdb",
                "sources": {
                    "product_events": {
                        "relation": {"kind": "table", "name": "events"},
                        "timestamp": "ts",
                        "entities": {"user": "user_id"},
                        "facts": ["session"],
                        "properties": [],
                        "dimensions": [],
                    },
                    "assessments": {
                        "relation": {"kind": "table", "name": "assessments"},
                        "timestamp": "ts",
                        "entities": {"user": "user_id"},
                        "facts": [],
                        "properties": ["status"],
                        "dimensions": [],
                    },
                },
                "facts": {"session": {"source": "product_events", "column": "event_id"}},
                "properties": {
                    "status": {
                        "source": "assessments",
                        "column": "status",
                        "dtype": "string",
                        "as_of": "event_time",
                    }
                },
                "dimensions": {},
                "measures": {"session_count": {"fact": "session", "aggregation": "count"}},
                "metrics": {
                    "weekly_sessions": {
                        "kind": "mean",
                        "entity": "user",
                        "measure": "session_count",
                    }
                },
            }
        model = tr.Model.from_catalog(
            catalog,
            con=con,
            readiness={
                "product_events": tr.Coverage("2025-01-01", "2025-04-01"),
                "assessments": tr.Coverage("2025-01-01", "2025-04-01"),
            },
            units={"user": tr.UnitSource("users", "user_id")},
        )
        design = Design(
            unit="user",
            anchor=Schedule("1w", "2025-01-06", "2025-03-31"),
            features={"sessions_before": agg("weekly_sessions", window=("-4w", "0d"))},
            shape=StatePanel(
                BinnedMetricState(
                    "weekly_sessions",
                    cutpoints=(0, 3, 9),
                    levels=("inactive", "occasional", "regular", "power"),
                ),
                max_periods=12,
                baseline_features={
                    "baseline_sessions": agg("weekly_sessions", window=("-1w", "0d"))
                },
            ),
        )
        materialized = design.build(model).materialize(
            tr.Budget(max_rows=10_000, max_output_bytes=8_000_000)
        )
        restored = MaterializedFrame.from_arrow(materialized.to_arrow())
        panel_meta = restored.provenance.state_panel
        assert panel_meta is not None
        assert panel_meta.levels == ("inactive", "occasional", "regular", "power")
        assert restored.roles.prior_state == "from_state"
        assert restored.roles.outcomes == ("to_state",)
        print(restored.table.select(["unit", "period_index", "from_state", "to_state", "terminal"]))
        status_design = Design(
            unit="user",
            anchor=Schedule("1w", "2025-01-06", "2025-03-31"),
            shape=StatePanel(
                tr.StateObservations("status", levels=("active", "closed"), absorbing=("closed",)),
                max_periods=12,
            ),
        )
        status_frame = status_design.build(model).materialize(
            tr.Budget(max_rows=10_000, max_output_bytes=8_000_000)
        )
        status_restored = MaterializedFrame.from_arrow(status_frame.to_arrow())
        status_meta = status_restored.provenance.state_panel
        assert status_meta is not None
        assert status_meta.levels == ("active", "closed")
        assert status_meta.absorbing == ("closed",)
        assert status_meta.ordered is True
        assert status_meta.prior_state_timing == "at_origin"
        assert status_meta.state_source["point_time_columns"] == {
            "from_state": "from_time",
            "to_state": "to_time",
        }
        ordinal = {level: index for index, level in enumerate(status_meta.levels)}
        status_rows = status_restored.table.to_pylist()
        prior_predictor = pa.array(
            [ordinal.get(row["from_state"]) for row in status_rows], type=pa.int8()
        )
        response_target = pa.array(
            [ordinal.get(row["to_state"]) for row in status_rows], type=pa.int8()
        )
        prepared = status_restored.table.append_column("prior_state_ordinal", prior_predictor)
        prepared = prepared.append_column("response_state_ordinal", response_target)
        terminal_rows = [row for row in prepared.to_pylist() if row["terminal"]]
        assert len(terminal_rows) == 1
        assert terminal_rows[0]["prior_state_ordinal"] == ordinal["active"]
        assert terminal_rows[0]["response_state_ordinal"] == ordinal["closed"]
        assert terminal_rows[0]["to_time"] == start + timedelta(days=84)
        print(
            prepared.select(
                [
                    "unit",
                    "period_index",
                    "from_state",
                    "to_state",
                    "prior_state_ordinal",
                    "response_state_ordinal",
                    "to_time",
                    "terminal",
                ]
            )
        )
    finally:
        con.disconnect()


if __name__ == "__main__":
    main()
