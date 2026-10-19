"""Opt-in DuckDB scale benchmark with independent expected occurrence counts."""

from __future__ import annotations

import argparse
import json
import time
import tracemalloc
from pathlib import Path

import duckdb
import pyarrow as pa

from terrine.simulate.scale import ScaleConfig, event_batches, oracle_batches


def run(config: ScaleConfig) -> dict[str, object]:
    """Load bounded Arrow batches and compare DuckDB counts to a separate oracle."""
    con = duckdb.connect(":memory:")
    tracemalloc.start()
    started = time.perf_counter()
    input_events = 0
    try:
        for batch_index, batch in enumerate(event_batches(config)):
            input_events += batch.num_rows
            relation = f"event_batch_{batch_index}"
            con.register(relation, batch)
            if batch_index == 0:
                con.execute(f"CREATE TABLE events AS SELECT * FROM {relation}")
            else:
                con.execute(f"INSERT INTO events SELECT * FROM {relation}")
            con.unregister(relation)
        query = """SELECT unit_id, count(*)::BIGINT AS target_event_count
                   FROM events WHERE event_type = 'target' GROUP BY unit_id"""
        actual = con.execute(query).to_arrow_table().sort_by("unit_id")
        expected = pa.concat_tables(list(oracle_batches(config))).sort_by("unit_id")
        if not actual.equals(expected):
            raise AssertionError("DuckDB target-event counts differ from independent oracle")
        elapsed_seconds = time.perf_counter() - started
        _, peak_bytes = tracemalloc.get_traced_memory()
        physical_plan = con.execute(f"EXPLAIN {query}").fetchall()
        return {
            "status": "verified",
            "backend": "duckdb",
            "workload": config.to_dict(),
            "input_events": input_events,
            "anchors": config.units,
            "emitted_rows": actual.num_rows,
            "client_peak_memory_bytes": peak_bytes,
            "elapsed_seconds": elapsed_seconds,
            "scan_bytes": None,
            "scan_cost": None,
            "scan_cost_status": "unknown_local_backend_capability",
            "physical_plan": [row[1] for row in physical_plan],
            "oracle_match": True,
        }
    finally:
        tracemalloc.stop()
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = ScaleConfig(**json.loads(args.config.read_text(encoding="utf-8")))
    print(json.dumps(run(config), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
