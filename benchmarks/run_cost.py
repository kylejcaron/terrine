"""Opt-in query time of each recipe against its hand-written query; a measurement, not a gate."""

from __future__ import annotations

import argparse
import contextlib
import json
import time
import uuid
from typing import Any

import ibis

from benchmarks.workloads import cost_model, load_tables, recipe_cases
from terrine.simulate.scale import ScaleConfig

# Tracked targets for the ratio of a recipe's time to its hand-written query's.
TARGETS = {"duckdb": 3.0, "postgres": 10.0}


def seconds(con: Any, sql: str, repeats: int) -> float:
    """Fastest of ``repeats`` evaluations of a statement into a temporary table."""
    timings = []
    for _ in range(repeats):
        with contextlib.closing(con.con.cursor()) as cursor:
            started = time.perf_counter()
            cursor.execute(f"CREATE TEMPORARY TABLE cost_probe AS {sql}")
            timings.append(time.perf_counter() - started)
            cursor.execute("DROP TABLE cost_probe")
    return min(timings)


def measure(con: Any, config: ScaleConfig, *, dialect: str, repeats: int) -> list[dict[str, Any]]:
    """Load the workload into an empty workspace and time every recipe and its reference."""
    load_tables(con, config)
    model = cost_model(con, config, dialect=dialect)
    rows = []
    for name, case in recipe_cases(config).items():
        frame = case.design.build(model)
        row: dict[str, Any] = {"backend": dialect, "recipe": name}
        # A PostgreSQL table cannot store an anonymous record, so every backend
        # times struct columns (the event panel's anchor identifier) as text.
        expr = frame.expr
        structs = {
            column: expr[column].cast("string")
            for column, dtype in expr.schema().items()
            if dtype.is_struct()
        }
        if structs:
            expr = expr.mutate(**structs)
        try:
            terrine = seconds(con, con.compile(expr), repeats)
        except Exception as error:  # noqa: BLE001 - reported per recipe
            row.update(terrine_seconds=None, hand_seconds=None, ratio=None, error=repr(error))
        else:
            hand = seconds(con, case.reference, repeats)
            row.update(terrine_seconds=terrine, hand_seconds=hand, ratio=terrine / hand, error=None)
        row["target"] = TARGETS[dialect]
        rows.append(row)
    return rows


def _postgres(url: str, config: ScaleConfig, repeats: int) -> list[dict[str, Any]]:
    """Measure in a uniquely named schema of the given database, dropped afterwards."""
    schema = f"terrine_cost_{uuid.uuid4().hex[:12]}"
    admin: Any = ibis.connect(url)
    admin.raw_sql(f'CREATE SCHEMA "{schema}"').close()
    try:
        con: Any = ibis.connect(url, schema=schema)
        try:
            return measure(con, config, dialect="postgres", repeats=repeats)
        finally:
            con.disconnect()
    finally:
        admin.raw_sql(f'DROP SCHEMA "{schema}" CASCADE').close()
        admin.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--units", type=int, default=10_000)
    parser.add_argument("--events-per-unit", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--postgres-url", help="also measure on this PostgreSQL database")
    parser.add_argument("--json", action="store_true", help="print one JSON record per row")
    args = parser.parse_args()
    config = ScaleConfig(units=args.units, events_per_unit=args.events_per_unit, batch_size=50_000)
    con = ibis.duckdb.connect()
    try:
        rows = measure(con, config, dialect="duckdb", repeats=args.repeats)
    finally:
        con.disconnect()
    if args.postgres_url:
        rows += _postgres(args.postgres_url, config, args.repeats)
    if args.json:
        for row in rows:
            print(json.dumps(row, sort_keys=True))
        return
    print(f"{'backend':<9} {'recipe':<15} {'terrine s':>10} {'hand s':>9} {'ratio':>6} target")
    for row in rows:
        if row["error"] is not None:
            print(f"{row['backend']:<9} {row['recipe']:<15} error: {row['error'][:60]}")
            continue
        print(
            f"{row['backend']:<9} {row['recipe']:<15} {row['terrine_seconds']:>10.3f} "
            f"{row['hand_seconds']:>9.3f} {row['ratio']:>6.1f} {row['target']:.0f}x"
        )


if __name__ == "__main__":
    main()
