from datetime import UTC, datetime, timedelta

import ibis
import pandas as pd
import pyarrow as pa

from terrine.data import Budget, Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog
from terrine.spec import Design, EventAnchor, EventSequence, FunnelSpec, SessionSpec
from terrine.windows import Window


def test_overlapping_event_anchors_match_independent_pandas_oracle():
    con = ibis.duckdb.connect()
    try:
        base = datetime(2025, 1, 1, 12, tzinfo=UTC)
        events = pa.table(
            {
                "user_id": ["u1", "u1", "u1", "u2"],
                "ts": pa.array(
                    [base, base, base + timedelta(hours=1), base],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "event_id": ["z-view", "a-view", "purchase", "null-label"],
                "activity": ["event"] * 4,
                "kind": ["view", "view", "purchase", None],
            }
        )
        con.create_table("users", pa.table({"user_id": ["u1", "u2"]}))
        con.create_table("events", events)
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
                        "facts": ["activity"],
                        "properties": ["kind"],
                        "dimensions": [],
                    }
                },
                "facts": {"activity": {"source": "events", "column": "activity"}},
                "properties": {
                    "kind": {
                        "source": "events",
                        "column": "kind",
                        "dtype": "string",
                        "as_of": "event_time",
                    }
                },
                "dimensions": {},
                "measures": {},
                "metrics": {},
            }
        )
        model = Model.from_catalog(
            catalog,
            con=con,
            readiness={"events": Coverage("2025-01-01", "2025-01-02")},
            units={"user": UnitSource("users", "user_id")},
            event_ids={"events": "event_id"},
        )
        design = Design(
            "user",
            EventAnchor("activity", "each"),
            shape=EventSequence(
                "activity",
                Window("0s", "2h"),
                max_events=4,
                properties=("kind",),
                sessions=SessionSpec("1h"),
                funnel=FunnelSpec("kind", ("view", "purchase"), within="2h"),
            ),
        )
        result = design.build(model).materialize(Budget(max_rows=100, max_output_bytes=1_000_000))
        table = pd.DataFrame(result.table.to_pylist())
        source = pd.DataFrame(events.to_pydict())
        source["ts"] = pd.to_datetime(source["ts"], utc=True)
        oracle = []
        for unit, anchors in source.groupby("user_id", sort=True):
            for _, anchor in anchors.sort_values(["ts", "event_id"]).iterrows():
                selected = anchors[
                    (anchors.ts >= anchor.ts) & (anchors.ts < anchor.ts + pd.Timedelta(hours=2))
                ].sort_values(["ts", "event_id"])
                sessions = []
                session_index = -1
                previous_id = None
                previous_time = None
                for _, event in selected.iterrows():
                    if previous_time is None or event.ts - previous_time > pd.Timedelta(hours=1):
                        session_index += 1
                    sessions.append(
                        (
                            event.event_id,
                            session_index,
                            previous_id,
                            (previous_id, event.event_id) if previous_id is not None else None,
                            len(selected),
                        )
                    )
                    previous_id, previous_time = event.event_id, event.ts
                attempts = []
                for _, first in selected[selected.kind == "view"].iterrows():
                    matched = selected[
                        (selected.ts >= first.ts)
                        & (selected.ts < first.ts + pd.Timedelta(hours=2))
                        & (selected.ts > first.ts)
                        & (selected.kind == "purchase")
                    ]
                    attempts.append((first.event_id, bool(len(matched))))
                oracle.append(
                    (
                        unit,
                        anchor.ts.to_pydatetime(),
                        tuple(sessions),
                        tuple(attempts),
                    )
                )

        actual = []
        for group_key, rows in table.groupby(["unit", "anchor_id"], sort=True):
            if not isinstance(group_key, tuple) or len(group_key) != 2:
                raise AssertionError("Grouping by unit and anchor_id must yield a pair.")
            unit, _anchor_id = group_key
            header = rows[~rows.is_event].iloc[0]
            children = rows[rows.is_event].sort_values("sequence_position")
            session_rows = tuple(
                (
                    row.event_id,
                    int(row.session_index),
                    None if pd.isna(row.previous_event_id) else row.previous_event_id,
                    (
                        (row.transition["from_event_id"], row.transition["to_event_id"])
                        if isinstance(row.transition, dict)
                        else None
                    ),
                    int(row.session_event_count),
                )
                for row in children.itertuples()
            )
            attempts = tuple(
                (attempt["step0_event_id"], attempt["status"] == "complete")
                for attempt in (header.funnel_attempts or [])
            )
            actual.append((unit, header.t0.to_pydatetime(), session_rows, attempts))
        assert sorted(actual) == sorted(oracle)
    finally:
        con.disconnect()
