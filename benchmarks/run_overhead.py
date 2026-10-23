"""Measure fixed compiler overhead on tiny public Model/Design workflows.

Every case binds a deterministic local DuckDB Model, builds one Design and
materializes it through the ordinary public path, so declared snapshot
validation and output work are inside the timed materialization phase.
Expected values come from integer, Decimal and Fraction arithmetic over the
generated events, never from the compiler. Timed samples are unprofiled; a
separate instrumented pass counts compilations and ``--profile`` records a
separate profiled run that is not a timing sample.
"""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
import platform
import random
import statistics
import subprocess
import sys
import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from importlib.metadata import version
from pathlib import Path
from typing import Any

import ibis
import pyarrow as pa

from terrine import Budget, Coverage, Design, FixedDate, Model, UnitSource, agg
from terrine.data import MaterializedFrame
from terrine.semantic.models import Catalog

_HISTORY_START = datetime(2025, 1, 1, tzinfo=UTC)
_HISTORY_DAYS = 30
_T0 = datetime(2025, 1, 15, tzinfo=UTC)
_WINDOW_SECONDS = 7 * 86400
_SEED = 20251015
_COLUMNS = {
    "count": "purchases",
    "decimal_sum": "spend",
    "float_average": "avg_price",
    "ratio": "spend_per_purchase",
}
_ALL_COLUMNS = tuple(_COLUMNS)


@dataclass(frozen=True, slots=True)
class Workload:
    name: str
    units: int
    events: int
    columns: tuple[str, ...]


WORKLOADS: tuple[Workload, ...] = (
    Workload("one_row_count", 1, 2, ("count",)),
    Workload("one_row_decimal_sum", 1, 2, ("decimal_sum",)),
    Workload("one_row_float_average", 1, 2, ("float_average",)),
    Workload("one_row_ratio", 1, 2, ("ratio",)),
    Workload("one_row_four_columns", 1, 2, _ALL_COLUMNS),
    Workload("few_rows_four_columns", 3, 12, _ALL_COLUMNS),
    Workload("events_10k_count", 100, 10_000, ("count",)),
    Workload("events_10k_four_columns", 100, 10_000, _ALL_COLUMNS),
    Workload("events_100k_count", 100, 100_000, ("count",)),
    Workload("events_100k_four_columns", 100, 100_000, _ALL_COLUMNS),
)


def _catalog() -> Catalog:
    return Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                "purchases": {
                    "relation": {"kind": "table", "name": "events"},
                    "timestamp": "occurred_at",
                    "entities": {"user": "user_id"},
                    "facts": ["purchase", "amount", "price"],
                    "properties": [],
                    "dimensions": [],
                }
            },
            "facts": {
                "purchase": {"source": "purchases", "column": None},
                "amount": {"source": "purchases", "column": "amount"},
                "price": {"source": "purchases", "column": "price"},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "purchase_count": {"fact": "purchase", "aggregation": "count"},
                "amount_total": {"fact": "amount", "aggregation": "sum"},
                "price_avg": {"fact": "price", "aggregation": "avg_event"},
            },
            "metrics": {
                "purchases": {"kind": "mean", "entity": "user", "measure": "purchase_count"},
                "spend": {"kind": "mean", "entity": "user", "measure": "amount_total"},
                "avg_price": {"kind": "mean", "entity": "user", "measure": "price_avg"},
                "spend_per_purchase": {
                    "kind": "ratio",
                    "entity": "user",
                    "numerator": "amount_total",
                    "denominator": "purchase_count",
                },
            },
        }
    )


def _unit(index: int) -> str:
    return f"u{index:05d}"


def generate(workload: Workload) -> tuple[pa.Table, pa.Table]:
    """Deterministic users and events; the last of several units has no events."""
    rng = random.Random(_SEED)
    assignable = max(workload.units - 1, 1)
    users, times, amounts, prices = [], [], [], []
    for index in range(workload.events):
        users.append(_unit(rng.randrange(assignable)))
        times.append(_HISTORY_START + timedelta(seconds=rng.randrange(_HISTORY_DAYS * 86400)))
        amounts.append(Decimal(rng.randrange(1, 100_001)).scaleb(-2))
        prices.append(rng.randrange(0, 1_000_001) / 100)
        if index == 0 and workload.units == 1:
            # The one-row cases keep exactly one event inside the window.
            times[-1] = _T0 - timedelta(days=3)
        elif index == 1 and workload.units == 1:
            times[-1] = _T0 + timedelta(days=1)
    events = pa.table(
        {
            "user_id": pa.array(users, pa.string()),
            "occurred_at": pa.array(times, pa.timestamp("us", tz="UTC")),
            "amount": pa.array(amounts, pa.decimal128(18, 2)),
            "price": pa.array(prices, pa.float64()),
        }
    )
    units = pa.table({"user_id": pa.array([_unit(i) for i in range(workload.units)], pa.string())})
    return units, events


def reference(events: pa.Table, units: int) -> dict[str, dict[str, Any]]:
    """Per-unit window references from exact arithmetic over the generated rows."""
    start, end = _T0 - timedelta(seconds=_WINDOW_SECONDS), _T0
    counts: dict[str, int] = {}
    amounts: dict[str, Decimal] = {}
    prices: dict[str, Fraction] = {}
    for row in events.to_pylist():
        if not start <= row["occurred_at"] < end:
            continue
        unit = row["user_id"]
        counts[unit] = counts.get(unit, 0) + 1
        amounts[unit] = amounts.get(unit, Decimal(0)) + row["amount"]
        prices[unit] = prices.get(unit, Fraction(0)) + Fraction.from_float(row["price"])
    expected = {}
    for index in range(units):
        unit = _unit(index)
        count = counts.get(unit, 0)
        amount = amounts.get(unit, Decimal(0))
        expected[unit] = {
            "count": count,
            "decimal_sum": amount,
            "float_average": prices[unit] / count if count else None,
            "ratio": Fraction(amount) / count if count else None,
        }
    return expected


def _design(workload: Workload) -> Design:
    window = (-_WINDOW_SECONDS, 0)
    return Design(
        "user",
        FixedDate(_T0.date()),
        features={name: agg(_COLUMNS[name], window=window) for name in workload.columns},
    )


def check(result: MaterializedFrame, expected: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Compare exact outputs for equality and checked floats for certificate enclosure."""
    rows = result.table.to_pylist()
    mismatches = 0
    for row in rows:
        want = expected[row["unit"]]
        for name in result.roles.features:
            meta = result.columns[name]
            value = row[name]
            complete = row[meta.complete_column] if meta.complete_column else True
            reason = row[meta.reason_column] if meta.reason_column else None
            target = want[name]
            if not complete:
                mismatches += 1
            elif name in {"count", "decimal_sum"}:
                mismatches += value != target or reason is not None
            elif target is None:
                missing = {"float_average": "no_observations", "ratio": "zero_denominator"}[name]
                mismatches += value is not None or reason != missing
            else:
                assert meta.absolute_error_column is not None
                error = row[meta.absolute_error_column]
                mismatches += (
                    value is None
                    or reason is not None
                    or error is None
                    or error < 0
                    or abs(Fraction.from_float(value) - target) > Fraction.from_float(error)
                )
    return {
        "oracle_match": len(rows) == len(expected) and mismatches == 0,
        "rows_compared": len(rows),
        "mismatches": mismatches,
    }


def _query_kind(expr: Any) -> str:
    columns = set(expr.as_table().columns)
    if {"unit", "anchor_id"} <= columns:
        return "output"
    if columns == {"candidate", "excluded_false", "unknown", "retained"}:
        return "eligibility"
    return "validation"


@contextmanager
def compilations() -> Generator[list[dict[str, Any]]]:
    """Record every SQL compilation's elapsed time, size, digest and query kind."""
    from ibis.backends.sql import SQLBackend

    records: list[dict[str, Any]] = []
    original = SQLBackend.compile

    def compile(self, expr, /, **kwargs):
        start = time.perf_counter()
        sql = original(self, expr, **kwargs)
        elapsed = time.perf_counter() - start
        records.append(
            {
                "kind": _query_kind(expr),
                "seconds": elapsed,
                "characters": len(sql),
                "digest": hashlib.sha256(sql.encode()).hexdigest()[:16],
            }
        )
        return sql

    SQLBackend.compile = compile  # type: ignore[method-assign]
    try:
        yield records
    finally:
        SQLBackend.compile = original  # type: ignore[method-assign]


def _diagnostics(records: list[dict[str, Any]]) -> dict[str, Any]:
    digests = [record["digest"] for record in records]
    by_kind: dict[str, dict[str, Any]] = {}
    for record in records:
        kind = by_kind.setdefault(record["kind"], {"count": 0, "seconds": 0.0, "characters": 0})
        kind["count"] += 1
        kind["seconds"] += record["seconds"]
        kind["characters"] += record["characters"]
    return {
        "compilations": len(records),
        "distinct_expressions": len(set(digests)),
        "repeated_expressions": len(digests) - len(set(digests)),
        "compile_seconds": sum(record["seconds"] for record in records),
        "by_kind": by_kind,
        "queries": [
            {key: record[key] for key in ("kind", "seconds", "characters")} for record in records
        ],
    }


def _sample(design: Design, model: Model, budget: Budget) -> tuple[dict[str, float], Any]:
    start = time.perf_counter()
    frame = design.build(model)
    built = time.perf_counter()
    result = frame.materialize(budget)
    done = time.perf_counter()
    return {
        "build_seconds": built - start,
        "materialize_seconds": done - built,
        "total_seconds": done - start,
    }, result


def _summary(samples: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    summary = {}
    for key in ("build_seconds", "materialize_seconds", "total_seconds"):
        values = [sample[key] for sample in samples]
        summary[key] = {
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        }
    return summary


def run_case(workload: Workload, con: Any, *, repeats: int) -> dict[str, Any]:
    setup_start = time.perf_counter()
    units, events = generate(workload)
    con.create_table("users", units, overwrite=True)
    con.create_table("events", events, overwrite=True)
    model = Model.from_catalog(
        _catalog(),
        con=con,
        units={"user": UnitSource("users", "user_id")},
        readiness={"purchases": Coverage(_HISTORY_START.date(), (_T0 + timedelta(days=16)).date())},
    )
    design = _design(workload)
    budget = Budget(max_rows=workload.units)
    setup_seconds = time.perf_counter() - setup_start
    first, result = _sample(design, model, budget)
    with compilations() as records:
        _, diagnosed = _sample(design, model, budget)
    warm = [_sample(design, model, budget)[0] for _ in range(repeats)]
    expected = reference(events, workload.units)
    oracle = check(result, expected)
    oracle["diagnostic_run_match"] = check(diagnosed, expected)["oracle_match"]
    return {
        "case": workload.name,
        "workload": {
            "units": workload.units,
            "input_events": workload.events,
            "input_arrow_bytes": events.nbytes,
            "columns": {name: _COLUMNS[name] for name in workload.columns},
            "window_seconds": [-_WINDOW_SECONDS, 0],
        },
        "emitted_rows": result.table.num_rows,
        "output_arrow_bytes": result.table.nbytes,
        "setup_seconds": setup_seconds,
        "samples": {"first": first, "warm": warm},
        "warm_summary": _summary(warm),
        "diagnostics": _diagnostics(records),
        **oracle,
    }


def _import_seconds() -> float:
    code = (
        "import time; start = time.perf_counter(); import terrine, ibis, duckdb, pyarrow; "
        "print(time.perf_counter() - start)"
    )
    output = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    ).stdout
    return float(output.strip())


def _environment(threads: int) -> dict[str, Any]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "ibis": version("ibis-framework"),
        "duckdb": version("duckdb"),
        "pyarrow": version("pyarrow"),
        "terrine": version("terrine"),
        "duckdb_threads": threads,
    }


def _connect(threads: int) -> Any:
    con = ibis.duckdb.connect()
    con.raw_sql(f"SET threads = {int(threads)}")
    return con


def run(
    *, repeats: int, cases: tuple[Workload, ...], threads: int, label: str | None
) -> dict[str, Any]:
    """Time every selected case in one process; cases share nothing but the connection."""
    import_seconds = _import_seconds()
    con = _connect(threads)
    try:
        results = [run_case(workload, con, repeats=repeats) for workload in cases]
    finally:
        con.disconnect()
    return {
        "schema_version": 1,
        "label": label,
        "recorded_at": datetime.now(UTC).isoformat(),
        "environment": _environment(threads),
        "import_seconds": import_seconds,
        "repeats": repeats,
        "cases": results,
    }


def profile(path: Path, cases: tuple[Workload, ...], threads: int) -> None:
    """Profile one build and materialization per case; never a timing sample."""
    con = _connect(threads)
    profiler = cProfile.Profile()
    try:
        for workload in cases:
            units, events = generate(workload)
            con.create_table("users", units, overwrite=True)
            con.create_table("events", events, overwrite=True)
            model = Model.from_catalog(
                _catalog(),
                con=con,
                units={"user": UnitSource("users", "user_id")},
                readiness={
                    "purchases": Coverage(_HISTORY_START.date(), (_T0 + timedelta(days=16)).date())
                },
            )
            design = _design(workload)
            budget = Budget(max_rows=workload.units)
            _sample(design, model, budget)
            profiler.enable()
            _sample(design, model, budget)
            profiler.disable()
    finally:
        con.disconnect()
    profiler.dump_stats(str(path))


def _json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported report scalar {type(value).__name__}")


def _print_table(report: dict[str, Any]) -> None:
    header = (
        f"{'case':28} {'rows':>6} {'first':>8} {'median':>8} {'min':>8} {'max':>8} {'compiles':>8}"
    )
    print(header)
    for case in report["cases"]:
        total = case["warm_summary"]["total_seconds"]
        print(
            f"{case['case']:28} {case['emitted_rows']:>6} "
            f"{case['samples']['first']['total_seconds']:>8.3f} {total['median']:>8.3f} "
            f"{total['min']:>8.3f} {total['max']:>8.3f} "
            f"{case['diagnostics']['compilations']:>8}"
            + ("" if case["oracle_match"] else "  ORACLE MISMATCH")
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5, help="warm timed samples per case")
    parser.add_argument("--output", type=Path, help="write the JSON record here")
    parser.add_argument("--profile", type=Path, help="write a separate cProfile run here")
    parser.add_argument("--threads", type=int, default=1, help="DuckDB threads")
    parser.add_argument("--label", help="free-form label stored in the record")
    parser.add_argument(
        "--case", action="append", default=[], help="select cases by exact name; repeatable"
    )
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    cases = WORKLOADS
    if args.case:
        unknown = set(args.case) - {workload.name for workload in WORKLOADS}
        if unknown:
            parser.error(f"unknown cases: {sorted(unknown)}")
        cases = tuple(workload for workload in WORKLOADS if workload.name in args.case)
    report = run(repeats=args.repeats, cases=cases, threads=args.threads, label=args.label)
    _print_table(report)
    if args.output is not None:
        args.output.write_text(json.dumps(report, indent=2, default=_json), encoding="utf-8")
    if args.profile is not None:
        profile(args.profile, cases, args.threads)
    if not all(case["oracle_match"] for case in report["cases"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
