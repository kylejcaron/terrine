"""Compare literal SQL and current Terrine on identical DuckDB modeling datasets.

Query-only timings use precompiled SQL and the same Arrow execution path. Public
Frame timings retain admission, validation, compilation and richer metadata.
"""

from __future__ import annotations

import argparse
import contextlib
import cProfile
import hashlib
import json
import math
import platform
import statistics
import subprocess
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any

import duckdb
import ibis
import pyarrow as pa
import sqlglot

from benchmarks.query_strategy_spike import make_design, make_model
from benchmarks.run_overhead import compilations
from benchmarks.workloads import load_tables
from terrine import Budget
from terrine.execution.inputs import _DuckDBSession
from terrine.simulate.scale import ScaleConfig
from tests.fixtures.readme import readme_design, readme_model, readme_tables

_README_SQL = """WITH snapshots(t0) AS (
  VALUES (TIMESTAMPTZ '2025-02-01 00:00:00+00'),
         (TIMESTAMPTZ '2025-03-01 00:00:00+00')
), totals AS (
  SELECT c.customer_id AS unit, s.t0,
    count(o.ordered_at) FILTER (WHERE o.ordered_at < s.t0) AS past_n,
    count(o.amount) FILTER (WHERE o.ordered_at < s.t0) AS past_nn,
    sum(o.amount) FILTER (WHERE o.ordered_at < s.t0) AS past_sum,
    count(o.ordered_at) FILTER (WHERE o.ordered_at >= s.t0) AS future_n,
    count(o.amount) FILTER (WHERE o.ordered_at >= s.t0) AS future_nn,
    sum(o.amount) FILTER (WHERE o.ordered_at >= s.t0) AS future_sum
  FROM customers c CROSS JOIN snapshots s
  LEFT JOIN orders o ON o.customer_id = c.customer_id
    AND o.ordered_at >= s.t0 - INTERVAL '30 days'
    AND o.ordered_at < s.t0 + INTERVAL '30 days'
  GROUP BY c.customer_id, s.t0
)
SELECT unit, t0,
  past_n::BIGINT AS orders_30d,
  TRUE AS orders_30d__complete, NULL::VARCHAR AS orders_30d__reason,
  CASE WHEN past_n = 0 THEN 0.0 ELSE past_sum END AS spend_30d,
  TRUE AS spend_30d__complete,
  CASE WHEN past_n > 0 AND past_nn = 0 THEN 'all_values_null' END AS spend_30d__reason,
  CASE WHEN t0 + INTERVAL '30 days' <= TIMESTAMPTZ '2025-03-21 00:00:00+00'
       THEN CASE WHEN future_n = 0 THEN 0.0 ELSE future_sum END END AS spend_next_30d,
  t0 + INTERVAL '30 days' <= TIMESTAMPTZ '2025-03-21 00:00:00+00'
    AS spend_next_30d__complete,
  CASE WHEN t0 + INTERVAL '30 days' > TIMESTAMPTZ '2025-03-21 00:00:00+00'
       THEN 'incomplete_coverage'
       WHEN future_n > 0 AND future_nn = 0 THEN 'all_values_null'
       END AS spend_next_30d__reason
FROM totals ORDER BY unit, t0"""


def _wide_sql() -> str:
    aggregates = []
    fields = ["unit", "TIMESTAMPTZ '2025-01-15 00:00:00+00' AS t0"]
    for days in range(1, 11):
        within = f"e.event_time >= TIMESTAMPTZ '2025-01-15 00:00:00+00' - INTERVAL '{days} days'"
        aggregates.extend(
            [
                f"count(e.event_id) FILTER (WHERE {within}) AS n{days}",
                f"count(e.value) FILTER (WHERE {within}) AS nn{days}",
                f"sum(e.value) FILTER (WHERE {within}) AS s{days}",
            ]
        )
    for kind in ("events", "total", "average", "per_event", "active"):
        for days in range(1, 11):
            n, nn, total = f"n{days}", f"nn{days}", f"s{days}"
            value = {
                "events": f"{n}::BIGINT",
                "total": f"CASE WHEN {n} = 0 THEN 0.0 ELSE {total} END",
                "average": f"{total} / NULLIF({nn}, 0)",
                "per_event": f"{total} / NULLIF({n}, 0)",
                "active": f"{n} > 0",
            }[kind]
            reason = {
                "events": "NULL::VARCHAR",
                "total": f"CASE WHEN {n} > 0 AND {nn} = 0 THEN 'all_values_null' END",
                "average": f"CASE WHEN {n} = 0 THEN 'no_observations' "
                f"WHEN {nn} = 0 THEN 'all_values_null' END",
                "per_event": f"CASE WHEN {n} = 0 THEN 'zero_denominator' "
                f"WHEN {nn} = 0 THEN 'all_values_null' END",
                "active": "NULL::VARCHAR",
            }[kind]
            name = f"{kind}_{days}d"
            fields.extend(
                [f"{value} AS {name}", f"TRUE AS {name}__complete", f"{reason} AS {name}__reason"]
            )
    return (
        "WITH totals AS (\n  SELECT u.unit_id AS unit,\n    "
        + ",\n    ".join(aggregates)
        + "\n  FROM users u LEFT JOIN events_native e ON e.unit_id = u.unit_id\n"
        "    AND e.event_time >= TIMESTAMPTZ '2025-01-05 00:00:00+00'\n"
        "    AND e.event_time < TIMESTAMPTZ '2025-01-15 00:00:00+00'\n"
        "  GROUP BY u.unit_id\n)\nSELECT "
        + ",\n  ".join(fields)
        + "\nFROM totals ORDER BY unit, t0"
    )


def _execute(con: Any, sql: str) -> pa.Table:
    return con.con.sql(sql).to_arrow_table()


def _expected(con: Any, case: str) -> list[dict[str, Any]]:
    wide = case == "wide"
    users = con.table("users" if wide else "customers").to_pyarrow().to_pylist()
    events = con.table("events_native" if wide else "orders").to_pyarrow().to_pylist()
    grouped: dict[str, list[tuple[datetime, float | None]]] = defaultdict(list)
    for event in events:
        instant = event["event_time" if wide else "ordered_at"]
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        grouped[event["unit_id" if wide else "customer_id"]].append(
            (instant, event["value" if wide else "amount"])
        )
    origins = (
        [datetime(2025, 1, 15, tzinfo=UTC)]
        if wide
        else [datetime(2025, 2, 1, tzinfo=UTC), datetime(2025, 3, 1, tzinfo=UTC)]
    )
    rows = []
    for user in users:
        unit = user["unit_id" if wide else "customer_id"]
        for origin in origins:
            row: dict[str, Any] = {"unit": unit, "t0": origin}
            specs = (
                [
                    (f"{kind}_{days}d", kind, -days, 0)
                    for kind in ("events", "total", "average", "per_event", "active")
                    for days in range(1, 11)
                ]
                if wide
                else [
                    ("orders_30d", "events", -30, 0),
                    ("spend_30d", "total", -30, 0),
                    ("spend_next_30d", "total", 0, 30),
                ]
            )
            for name, kind, start, end in specs:
                selected = [
                    value
                    for instant, value in grouped[unit]
                    if origin + timedelta(days=start) <= instant < origin + timedelta(days=end)
                ]
                values = [value for value in selected if value is not None]
                complete = wide or origin + timedelta(days=end) <= datetime(2025, 3, 21, tzinfo=UTC)
                n = len(selected)
                total = math.fsum(values)
                reason = None
                if not complete:
                    value, reason = None, "incomplete_coverage"
                elif kind == "events":
                    value = n
                elif kind == "active":
                    value = n > 0
                elif kind == "average" and n == 0:
                    value, reason = None, "no_observations"
                elif kind == "per_event" and n == 0:
                    value, reason = None, "zero_denominator"
                elif n > 0 and not values:
                    value, reason = None, "all_values_null"
                elif kind == "total":
                    value = total
                elif kind == "average":
                    value = total / len(values)
                else:
                    value = total / n
                row.update({name: value, name + "__complete": complete, name + "__reason": reason})
            rows.append(row)
    return sorted(rows, key=lambda row: (row["unit"], row["t0"]))


def _assert_schema(actual: pa.Table, reference: pa.Table) -> None:
    assert actual.schema.equals(reference.schema, check_metadata=False), (
        "Dataset schema differs",
        actual.schema,
        reference.schema,
    )


def _assert_rows(table: pa.Table, expected: list[dict[str, Any]]) -> None:
    actual = table.select(list(expected[0])).to_pylist()
    assert [(row["unit"], row["t0"]) for row in actual] == [
        (row["unit"], row["t0"]) for row in expected
    ], "Missing, duplicated or unordered dataset origins"
    for row, target in zip(actual, expected, strict=True):
        for name, value in target.items():
            if isinstance(value, float):
                assert isinstance(row[name], float) and math.isclose(
                    row[name], value, rel_tol=1e-12, abs_tol=1e-12
                ), (row["unit"], name, row[name], value)
            else:
                assert type(row[name]) is type(value) and row[name] == value, (
                    row["unit"],
                    name,
                    row[name],
                    value,
                )


@contextlib.contextmanager
def _capture_study_sql():
    statements = []
    original_execute, original_batches = _DuckDBSession.execute, _DuckDBSession.batches

    def execute(self: Any, con: Any, query: Any, sql: str) -> pa.Table:
        statements.append(sql)
        return original_execute(self, con, query, sql)

    def batches(self: Any, con: Any, query: Any, sql: str, *, chunk_size: int):
        statements.append(sql)
        return original_batches(self, con, query, sql, chunk_size=chunk_size)

    _DuckDBSession.execute, _DuckDBSession.batches = execute, batches
    try:
        yield statements
    finally:
        _DuckDBSession.execute, _DuckDBSession.batches = original_execute, original_batches


def _sizes(sql: str) -> dict[str, int]:
    parsed = sqlglot.parse_one(sql, read="duckdb")
    return {
        "submitted_bytes": len(sql.encode()),
        "submitted_lines": len(sql.splitlines()),
        "compact_bytes": len(parsed.sql(dialect="duckdb", pretty=False).encode()),
        "formatted_bytes": len(parsed.sql(dialect="duckdb", pretty=True).encode()),
    }


@contextlib.contextmanager
def _phases():
    """Time real call boundaries; parent IDs identify overlapping inclusive timers."""
    from ibis.backends.sql import SQLBackend

    import terrine.compile.dispatch as dispatch
    import terrine.frame as frame_module
    from terrine import Design, Model
    from terrine.execution import _native
    from terrine.execution.gates import GatedStream
    from terrine.execution.inputs import ExecutionContext, _ClosingArrowReader
    from terrine.frame import Frame

    records: list[dict[str, Any]] = []
    stack: list[int] = []

    def wrap(owner: Any, attribute: str, phase: str, sql_index: int | None = None):
        original = getattr(owner, attribute)

        @wraps(original)
        def measured(*args, **kwargs):
            record: dict[str, Any] = {
                "id": len(records),
                "parent": stack[-1] if stack else None,
                "phase": phase,
                "boundary": f"{owner.__name__}.{attribute}",
                "status": "ok",
            }
            records.append(record)
            stack.append(record["id"])
            start = time.perf_counter()
            try:
                result = original(*args, **kwargs)
                if phase == "sql_render":
                    record.update(
                        digest=hashlib.sha256(result.encode()).hexdigest()[:16],
                        submitted_bytes=len(result.encode()),
                        scope=(
                            "study"
                            if any(
                                records[index]["phase"] == "statement_render_check"
                                for index in stack
                            )
                            else "unclassified"
                        ),
                    )
                return result
            except StopIteration:
                record["status"] = "exhausted"
                raise
            except BaseException:
                record["status"] = "failed"
                raise
            finally:
                record["seconds"] = time.perf_counter() - start
                stack.pop()
                if sql_index is not None:
                    sql = args[sql_index] if len(args) > sql_index else kwargs["sql"]
                    record.update(
                        digest=hashlib.sha256(sql.encode()).hexdigest()[:16],
                        submitted_bytes=len(sql.encode()),
                        scope=(
                            "metadata_probe"
                            if phase == "metadata_probe"
                            else "admission"
                            if phase == "native_function_admission"
                            else "study"
                        ),
                    )

        setattr(owner, attribute, measured)
        return original

    hooks = (
        (Design, "build", "build", None),
        (Frame, "materialize", "public_materialize", None),
        (dispatch, "compile_design", "compile_design", None),
        (dispatch, "_compile_design", "expression_construction", None),
        (Model, "_for_execution", "execution_model_rebind", None),
        (ExecutionContext, "admit", "primitive_admission", None),
        (ExecutionContext, "_check_query", "statement_render_check", None),
        (SQLBackend, "compile", "sql_render", None),
        (_DuckDBSession, "check_query", "statement_admission", 1),
        (_DuckDBSession, "execute", "study_execute", 3),
        (_DuckDBSession, "batches", "study_stream_open", 3),
        (_ClosingArrowReader, "__next__", "stream_next", None),
        (_ClosingArrowReader, "read_all", "stream_read_all", None),
        (GatedStream, "resolve", "stream_gate_resolve", None),
        (frame_module, "run_schedule", "validation_schedule", None),
        (frame_module, "gated_output", "gated_expression_construction", None),
        (_native, "_rows", "metadata_probe", 1),
        (_native, "check_native_functions", "native_function_admission", 1),
    )
    with contextlib.ExitStack() as restore:
        for owner, attribute, phase, sql_index in hooks:
            original = wrap(owner, attribute, phase, sql_index)
            restore.callback(setattr, owner, attribute, original)
        yield records


def _diagnostic_pass(design: Any, model: Any, out: Path, *, profile: bool):
    """Build and consume a fresh public Frame, separately from every timing sample."""
    profiler = cProfile.Profile() if profile else None
    try:
        with compilations() as translations, _phases() as phases:
            if profiler is not None:
                profiler.enable()
            try:
                frame = design.build(model)
                result = frame.materialize(budget=Budget(strict_scan=False)).table
            finally:
                if profiler is not None:
                    profiler.disable()
    finally:
        if profiler is not None:
            profiler.dump_stats(str(out / "diagnostic.pstats"))
    counts: dict[str, int] = defaultdict(int)
    for record in phases:
        counts[record["phase"]] += 1
    study = [
        record for record in phases if record["phase"] in {"study_execute", "study_stream_open"}
    ]
    report = {
        "timing_sample": False,
        "timers": "inclusive; parent IDs identify nesting, never sum overlapping phases",
        "phase_counts": dict(counts),
        "phases": phases,
        "sql_compilations": translations,
        "sql_compilation_count": len(translations),
        "sql_compilation_bytes": sum(record["bytes"] for record in translations),
        "study_statement_count": len(study),
        "study_statement_bytes": sum(record["submitted_bytes"] for record in study),
        "capture_excludes": (
            "metadata/admission probes, transaction control and catalog introspection"
        ),
        "unseparated": {
            "expression_construction": (
                "compiler dispatch includes shape validation; compile_design also includes "
                "resolution, exact scope and pruning outside this nested timer"
            ),
            "statement_render_check": "includes rendering, ownership checks and admission",
            "study_execute": "includes backend hooks, execution and Arrow conversion",
            "stream_next": "includes deferred engine execution and Arrow batch conversion",
            "public_materialize": (
                "session opening/closing, remaining validation, output assembly and metadata "
                "are not isolated; this timer includes all nested phases"
            ),
            "probes": (
                "metadata_probe records _native._rows only; direct-driver admission witnesses "
                "and transaction control are not separately captured"
            ),
        },
        "duplicate_digest_note": "matching SQL digests do not prove duplicate database work",
        "profile": "diagnostic.pstats" if profile else None,
    }
    return report, result


def run(
    case: str,
    out: Path,
    repeats: int,
    units: int,
    events_per_unit: int,
    *,
    diagnostics: bool = False,
    profile: bool = False,
) -> dict[str, Any]:
    con = ibis.duckdb.connect(threads=1)
    con.con.execute("SET TimeZone='UTC'")
    try:
        start = time.perf_counter()
        if case == "readme":
            for name, table in readme_tables().items():
                con.create_table(name, table)
        else:
            config = ScaleConfig(
                units, events_per_unit, 50_000, span_days=30 if case == "wide" else 75
            )
            load_tables(con, config)
            table = con.table("events")
            if case == "wide":
                con.create_table(
                    "events_native", table.mutate(value=table.value.cast("float64") / 10)
                )
            else:
                con.create_table("customers", con.table("users").select(customer_id="unit_id"))
                con.create_table(
                    "orders",
                    table.select(
                        customer_id="unit_id",
                        ordered_at="event_time",
                        amount=table.value.cast("float64") / 10,
                    ),
                )
        load_seconds = time.perf_counter() - start
        source_tables = {}
        for name in ("users", "events_native") if case == "wide" else ("customers", "orders"):
            source = con.table(name).to_pyarrow()
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, source.schema) as writer:
                writer.write_table(source)
            source_tables[name] = {
                "rows": len(source),
                "schema": str(source.schema),
                "arrow_stream_sha256": hashlib.sha256(sink.getvalue()).hexdigest(),
            }
        model = make_model(con, config, "native", "duckdb") if case == "wide" else readme_model(con)
        design = make_design(1, tuple(range(1, 11))) if case == "wide" else readme_design()
        expected = _expected(con, case)
        start = time.perf_counter()
        frame = design.build(model)
        expression = frame.expr
        build_seconds = time.perf_counter() - start
        projection = expression.select(*expected[0]).order_by("unit", "t0")
        start = time.perf_counter()
        generated = con.compile(projection)
        compile_seconds = time.perf_counter() - start
        full_sql = con.compile(expression)
        hand = _wide_sql() if case == "wide" else _README_SQL
        out.mkdir(parents=True, exist_ok=False)
        (out / "handwritten.sql").write_text(hand + "\n")
        (out / "terrine-projected.sql").write_text(generated + "\n")
        (out / "terrine-full-expression.sql").write_text(full_sql + "\n")
        reference = _execute(con, hand)
        generated_reference = _execute(con, generated)
        _assert_schema(reference, generated_reference)
        _assert_rows(reference, expected)
        _assert_rows(generated_reference, expected)
        with _capture_study_sql() as statements:
            materialized = frame.materialize(budget=Budget(strict_scan=False)).table
        _assert_schema(materialized.select(list(expected[0])), reference)
        _assert_rows(materialized, expected)
        for index, sql in enumerate(statements):
            (out / f"public-study-statement-{index}.sql").write_text(sql + "\n")
        # The oracle must reject missing/duplicate origins and a changed model value.
        for mutated in (reference.slice(1), pa.concat_tables([reference, reference.slice(0, 1)])):
            try:
                _assert_rows(mutated, expected)
            except AssertionError:
                pass
            else:
                raise AssertionError("Support oracle accepted missing/duplicate origin")
        changed = reference.to_pylist()
        first = next(
            name for name in expected[0] if name not in {"unit", "t0"} and "__" not in name
        )
        changed[0][first] += 1
        try:
            _assert_rows(pa.Table.from_pylist(changed, schema=reference.schema), expected)
        except AssertionError:
            pass
        else:
            raise AssertionError("Value oracle accepted corrupted metric")
        wrong_type = reference.set_column(
            reference.schema.get_field_index(first), first, reference[first].cast(pa.float64())
        )
        try:
            _assert_schema(wrong_type, reference)
        except AssertionError:
            pass
        else:
            raise AssertionError("Schema oracle accepted changed column type")
        diagnostic = None
        if diagnostics or profile:
            diagnostic, diagnosed = _diagnostic_pass(design, model, out, profile=profile)
            _assert_schema(diagnosed, materialized)
            _assert_rows(diagnosed, expected)
            _assert_rows(diagnosed, materialized.to_pylist())
            diagnostic.update(
                case=case, input_tables=source_tables, schema_parity=True, result_parity=True
            )
            (out / "diagnostics.json").write_text(json.dumps(diagnostic, indent=2) + "\n")
        times: dict[str, list[float]] = {
            "handwritten": [],
            "terrine_precompiled": [],
            "terrine_public": [],
        }
        for repeat in range(repeats):
            order = [("handwritten", hand), ("terrine_precompiled", generated)]
            for name, sql in order if repeat % 2 == 0 else reversed(order):
                start = time.perf_counter()
                result = _execute(con, sql)
                times[name].append(time.perf_counter() - start)
                _assert_schema(result, reference)
                _assert_rows(result, expected)
            start = time.perf_counter()
            result = frame.materialize(budget=Budget(strict_scan=False)).table
            times["terrine_public"].append(time.perf_counter() - start)
            _assert_schema(result.select(list(expected[0])), reference)
            _assert_rows(result, expected)
        row = {
            "case": case,
            "input_tables": source_tables,
            "load_seconds": load_seconds,
            "build_seconds": build_seconds,
            "projected_compile_seconds": compile_seconds,
            "rows": len(expected),
            "compared_columns": list(expected[0]),
            "public_columns": materialized.num_columns,
            "result_parity_each_sample": True,
            "schema_parity_each_sample": True,
            "oracle_sensitivity": ["missing", "duplicate", "changed_value", "changed_type"],
            "arrow_schema": str(reference.schema),
            "arrow_bytes": {
                "handwritten_projected": reference.nbytes,
                "terrine_projected": generated_reference.nbytes,
                "public": materialized.nbytes,
            },
            "seconds": times,
            "medians_seconds": {name: statistics.median(values) for name, values in times.items()},
            "sql": {
                "handwritten": _sizes(hand),
                "terrine_projected": _sizes(generated),
                "terrine_full_expression": _sizes(full_sql),
            },
            "public_study_sql": {
                "statements": len(statements),
                "bytes": sum(len(sql.encode()) for sql in statements),
                "excludes": (
                    "metadata/admission probes, transaction control and catalog introspection"
                ),
            },
        }
        if diagnostic is not None:
            row["diagnostics"] = {
                "file": "diagnostics.json",
                "timing_sample": False,
                "phase_counts": diagnostic["phase_counts"],
                "sql_compilation_count": diagnostic["sql_compilation_count"],
                "study_statement_count": diagnostic["study_statement_count"],
                "profile": diagnostic["profile"],
                "schema_parity": True,
                "result_parity": True,
            }
        (out / "result.json").write_text(json.dumps(row, indent=2) + "\n")
        print(json.dumps(row), flush=True)
        return row
    finally:
        con.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("readme", "scaled", "wide"), required=True)
    parser.add_argument("--units", type=int, default=10_000)
    parser.add_argument("--events-per-unit", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    identity = {
        "repository_revision": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "production_revision": subprocess.check_output(
            ["git", "-C", str(root), "log", "-1", "--format=%H", "--", "terrine"], text=True
        ).strip(),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "ibis": ibis.__version__,
        "pyarrow": pa.__version__,
        "threads": 1,
        "argv": vars(args) | {"output": str(args.output)},
        "source_sha256": {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                Path(__file__),
                root / "tests/fixtures/readme.py",
                root / "benchmarks/workloads.py",
                root / "benchmarks/query_strategy_spike.py",
                root / "benchmarks/run_overhead.py",
                *sorted((root / "terrine").rglob("*.py")),
            ]
        },
    }
    run(
        args.case,
        args.output,
        args.repeats,
        args.units,
        args.events_per_unit,
        diagnostics=args.diagnostics,
        profile=args.profile,
    )
    for relative, digest in identity["source_sha256"].items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest, relative
    (args.output / "environment.json").write_text(json.dumps(identity, indent=2) + "\n")


if __name__ == "__main__":
    main()
