"""Summarize observed workload records without treating unavailable resources as zero."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    measurements = []
    for record in records:
        measurements.append(
            {
                "status": record["status"],
                "backend": record["backend"],
                "workload": record["workload"],
                "input_events": record.get("input_events"),
                "anchors": record.get("anchors"),
                "emitted_rows": record.get("emitted_rows"),
                "elapsed_seconds": record.get("elapsed_seconds"),
                "client_peak_memory_bytes": record.get("client_peak_memory_bytes"),
                "process_peak_memory_bytes": record.get("process_peak_memory_bytes"),
                "scan_bytes": record.get("scan_bytes"),
                "scan_cost": record.get("scan_cost"),
                "oracle_match": record.get("oracle_match"),
                "source_digest": record.get("source_digest"),
            }
        )
    return {
        "schema_version": 1,
        "runs": len(records),
        "all_verified": bool(records) and all(record["status"] == "verified" for record in records),
        "measurements": measurements,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path, nargs="+")
    args = parser.parse_args()
    records = [json.loads(path.read_text(encoding="utf-8")) for path in args.records]
    print(json.dumps(summarize(records), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
