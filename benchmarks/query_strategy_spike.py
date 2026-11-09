"""Throwaway direct-window experiment, not a supported Terrine compiler.

Run with python -m benchmarks.query_strategy_spike. Only the five cost metrics,
UTC day windows and one event source are implemented. This is evidence for a
future design, never a replacement Frame API. No wall-clock gates.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import platform
import statistics
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import ibis
import pyarrow as pa

from benchmarks.workloads import _UNITS, _cost_catalog, _coverage, load_tables
from terrine import Model
from terrine.backends.extrema import greatest, least
from terrine.semantic.models import Catalog
from terrine.simulate.scale import ScaleConfig
from terrine.spec import Design, FixedDate, Schedule, agg

METRICS = ("events", "total", "average", "per_event", "active")
EPOCH = datetime(2025, 1, 1, tzinfo=UTC)


def make_model(con, config, mode, backend):
    raw = _cost_catalog(backend).model_dump(mode="python")
    if mode == "native":
        for source in raw["sources"].values():
            source["relation"]["name"] = "events_native"
        for name in ("total", "average"):
            raw["measures"][name]["exact"] = False
    return Model.from_catalog(
        Catalog.model_validate(raw),
        con=con,
        units=_UNITS,
        readiness={"events": _coverage(config), "targets": _coverage(config)},
        event_ids={"events": "event_id", "targets": "event_id"},
    )


def make_design(anchor_count, windows):
    start = (EPOCH + timedelta(days=14)).date()
    anchor = (
        FixedDate(start)
        if anchor_count == 1
        else Schedule("1d", start, start + timedelta(days=anchor_count - 1))
    )
    return Design(
        "user",
        anchor,
        features={
            f"{metric}_{days}d": agg(metric, window=(f"-{days}d", "0s"))
            for metric in METRICS
            for days in windows
        },
    )


def direct_query(
    anchors, events, coverage, windows, *, native=False, predicate=None, bucketed=False
):
    """One shared range join, optionally over noncumulative UTC-day summaries."""
    e = events.select(
        event_unit=events.unit_id,
        event_time=events.event_time,
        event_id=events.event_id,
        value=events.value.cast("float64" if native else "decimal(38, 0)"),
        selected=(ibis.literal(True) if predicate is None else predicate),
    )
    if bucketed:
        e = e.mutate(event_time=e.event_time.truncate("D"))
        selected = e.selected.fill_null(False)
        fields = {
            "n": e.event_id.count(where=selected),
            "nn": e.value.count(where=selected),
            "s": e.value.sum(where=selected).fill_null(0),
            "unknown": e.event_id.count(where=e.selected.isnull()),
        }
        if native:
            fields["bad"] = e.event_id.count(where=selected & (e.value.isnan() | e.value.isinf()))
        e = e.group_by(e.event_unit, e.event_time).aggregate(**fields)
    matched = anchors.left_join(
        e,
        [
            anchors.unit == e.event_unit,
            e.event_time >= anchors.t0 - ibis.interval(days=max(windows)),
            e.event_time < anchors.t0,
        ],
    )
    aggregates = {}
    for days in windows:
        within = (matched.event_time >= matched.t0 - ibis.interval(days=days)) & (
            matched.event_time < matched.t0
        )
        if bucketed:
            for field in ("n", "nn", "s", "unknown", *(("bad",) if native else ())):
                value = matched[field].sum(where=within).fill_null(0)
                aggregates[f"{field}_{days}"] = value if field == "s" else value.cast("int64")
        else:
            chosen = within & matched.selected.fill_null(False)
            aggregates[f"n_{days}"] = matched.event_id.count(where=chosen)
            aggregates[f"nn_{days}"] = matched.value.count(where=chosen)
            aggregates[f"s_{days}"] = matched.value.sum(where=chosen).fill_null(0)
            aggregates[f"unknown_{days}"] = matched.event_id.count(
                where=within & matched.selected.isnull()
            )
            if native:
                aggregates[f"bad_{days}"] = matched.event_id.count(
                    where=chosen & (matched.value.isnan() | matched.value.isinf())
                )
    grouped = matched.group_by(*[matched[c] for c in anchors.columns]).aggregate(**aggregates)
    staged = {}
    for days in windows:
        start = grouped.t0 - ibis.interval(days=days)
        source_ok = (start >= ibis.literal(coverage.start)) & (
            grouped.t0 <= ibis.literal(coverage.end_exclusive)
        )
        observed = (grouped._observed_from.isnull() | (start >= grouped._observed_from)) & (
            grouped._observed_until.isnull() | (grouped.t0 <= grouped._observed_until)
        )
        complete = source_ok & observed
        null_reason = ibis.null("string")
        unavailable = (~source_ok).ifelse(
            "incomplete_coverage", (~observed).ifelse("incomplete_observation", null_reason)
        )
        all_null = (grouped[f"n_{days}"] > 0) & (grouped[f"nn_{days}"] == 0)
        staged[f"complete_{days}"] = complete
        staged[f"count_reason_{days}"] = unavailable
        staged[f"sum_reason_{days}"] = complete.ifelse(
            all_null.ifelse("all_values_null", null_reason), unavailable
        )
        staged[f"average_reason_{days}"] = complete.ifelse(
            all_null.ifelse(
                "all_values_null",
                (grouped[f"n_{days}"] == 0).ifelse("no_observations", null_reason),
            ),
            unavailable,
        )
        staged[f"ratio_reason_{days}"] = complete.ifelse(
            all_null.ifelse(
                "all_values_null",
                (grouped[f"n_{days}"] == 0).ifelse("zero_denominator", null_reason),
            ),
            unavailable,
        )
        staged[f"check_unknown_{days}"] = complete & (grouped[f"unknown_{days}"] > 0)
        if native:
            staged[f"check_numeric_{days}"] = complete & (
                (grouped[f"bad_{days}"] > 0)
                | grouped[f"s_{days}"].isinf()
                | grouped[f"s_{days}"].isnan()
            )
    result = grouped.mutate(**staged)
    output = {"unit": result.unit, "t0": result.t0}
    for days in windows:
        values = {
            "events": result[f"n_{days}"],
            "total": result[f"s_{days}"],
            "average": result[f"s_{days}"].cast("float64") / result[f"nn_{days}"].nullif(0),
            "per_event": result[f"s_{days}"].cast("float64") / result[f"n_{days}"].nullif(0),
            "active": result[f"n_{days}"] > 0,
        }
        for metric, value in values.items():
            name = f"{metric}_{days}d"
            reason_kind = {
                "events": "count",
                "active": "count",
                "total": "sum",
                "average": "average",
                "per_event": "ratio",
            }[metric]
            reason = result[f"{reason_kind}_reason_{days}"]
            output[name] = reason.isnull().ifelse(value, ibis.null(value.type()))
            output[f"{name}__complete"] = result[f"complete_{days}"]
            output[f"{name}__reason"] = reason
            output[f"{name}__observed_from"] = greatest(
                ibis.literal(coverage.start),
                result._observed_from.fill_null(ibis.literal(coverage.start)),
            )
            output[f"{name}__observed_until"] = least(
                ibis.literal(coverage.end_exclusive),
                result._observed_until.fill_null(ibis.literal(coverage.end_exclusive)),
            )
        output[f"check_unknown_{days}"] = result[f"check_unknown_{days}"]
        if native:
            output[f"check_numeric_{days}"] = result[f"check_numeric_{days}"]
    return result.select(**output), matched


def canonical(frame, windows):
    table = frame.expr
    selected = {"unit": table.unit, "t0": table.t0}
    for metric in METRICS:
        for days in windows:
            name = f"{metric}_{days}d"
            meta = frame.columns[name]
            selected[name] = table[name]
            for kind in ("complete", "reason", "observed_from", "observed_until"):
                selected[f"{name}__{kind}"] = table[getattr(meta, f"{kind}_column")]
    return table.select(**selected)


def check_table(table):
    for name in table.column_names:
        if name.startswith("check_") and any(value is True for value in table[name].to_pylist()):
            raise ValueError(f"Prototype withheld output: {name}")
    return table.drop([name for name in table.column_names if name.startswith("check_")])


def compare(actual, expected):
    left = actual.sort_by([("unit", "ascending"), ("t0", "ascending")]).to_pydict()
    right = expected.sort_by([("unit", "ascending"), ("t0", "ascending")]).to_pydict()
    assert set(left) == set(right), (set(left) - set(right), set(right) - set(left))
    for name, values in left.items():
        assert len(values) == len(right[name]), name
        for index, (a, b) in enumerate(zip(values, right[name], strict=True)):
            if isinstance(a, float) or isinstance(b, float):
                ok = (
                    a is not None
                    and b is not None
                    and math.isclose(float(a), float(b), rel_tol=1e-10, abs_tol=1e-9)
                )
            else:
                ok = a == b
            assert ok, (name, index, a, b)


def timed_sql(con, sql):
    with contextlib.closing(con.con.cursor()) as cursor:
        begin = time.perf_counter()
        cursor.execute(f"CREATE TEMPORARY TABLE query_spike_output AS {sql}")
        elapsed = time.perf_counter() - begin
        cursor.execute("DROP TABLE query_spike_output")
    return elapsed


def profile(con, sql, backend, path):
    if backend == "duckdb":
        rows = con.raw_sql("EXPLAIN ANALYZE " + sql).fetchall()
        path.write_text("\n".join(str(row) for row in rows))
        return None
    rows = con.raw_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql).fetchall()
    path.write_text(json.dumps(rows, default=str, indent=2))
    return None


def measure_case(con, config, mode, anchor_count, backend, repeats, out, full, variants):
    windows = tuple(range(1, 11))
    label = f"{backend}-{mode}-anchors{anchor_count}"
    model = make_model(con, config, mode, backend)
    begin = time.perf_counter()
    frame = make_design(anchor_count, windows).build(model)
    current_build = time.perf_counter() - begin
    current = canonical(frame, windows)
    instants = [EPOCH + timedelta(days=14 + i) for i in range(anchor_count)]
    times = con.create_table(
        f"spike_times_{anchor_count}", pa.table({"t0": instants}), overwrite=True
    )
    if backend == "postgres":
        con.raw_sql(f'ANALYZE "spike_times_{anchor_count}"').close()
    users = con.table("users")
    anchors = users.select(
        unit=users.unit_id, _observed_from=users.observed_from, _observed_until=users.observed_until
    ).cross_join(times)
    events = con.table("events_native" if mode == "native" else "events")
    begin = time.perf_counter()
    candidate, matched = direct_query(
        anchors, events, model.readiness.for_fact("events"), windows, native=mode == "native"
    )
    candidate_build = time.perf_counter() - begin
    queries = {"current": current, "direct": candidate}
    row = {
        "case": label,
        "units": config.units,
        "events": config.units * config.events_per_unit,
        "anchors_per_unit": anchor_count,
        "outputs": len(METRICS) * len(windows),
        "build_seconds": {"current": current_build, "direct": candidate_build},
        "variants": {},
    }
    bucket_matches = None
    if "bucketed" in variants:
        begin = time.perf_counter()
        bucketed, bucket_matches = direct_query(
            anchors,
            events,
            model.readiness.for_fact("events"),
            windows,
            native=mode == "native",
            bucketed=True,
        )
        row["build_seconds"]["bucketed"] = time.perf_counter() - begin
        queries["bucketed"] = bucketed
    queries = {name: queries[name] for name in variants}
    compiled = {}
    for name, expression in queries.items():
        begin = time.perf_counter()
        sql = con.compile(expression)
        compile_seconds = time.perf_counter() - begin
        pretty = str(ibis.to_sql(expression, dialect=backend, pretty=True))  # noqa: TID251 - measurement
        compact = str(ibis.to_sql(expression, dialect=backend, pretty=False))  # noqa: TID251 - measurement
        compiled[name] = sql
        (out / f"{label}-{name}.sql").write_text(sql)
        row["variants"][name] = {
            "compile_seconds": compile_seconds,
            "submitted_sql_chars": len(sql),
            "formatted_sql_chars": len(pretty),
            "compact_sql_chars": len(compact),
            "server_seconds": [],
        }
    print(json.dumps({"progress": "compiled", "case": label}), flush=True)
    # Alternate order so the same variant does not always benefit from warming.
    for repeat in range(repeats):
        for name in variants if repeat % 2 == 0 else tuple(reversed(variants)):
            row["variants"][name]["server_seconds"].append(timed_sql(con, compiled[name]))
    tables = {}
    for name, expression in queries.items():
        begin = time.perf_counter()
        tables[name] = check_table(expression.to_pyarrow())
        row["variants"][name]["query_to_arrow_seconds"] = time.perf_counter() - begin
        row["variants"][name]["server_median_seconds"] = statistics.median(
            row["variants"][name]["server_seconds"]
        )
        row["variants"][name]["output_arrow_bytes"] = tables[name].nbytes
        profile(con, compiled[name], backend, out / f"{label}-{name}-explain.txt")
    expected = tables.get("direct")
    if expected is None:
        expected = check_table(candidate.to_pyarrow())
    for table in tables.values():
        compare(table, expected)
    from benchmarks.query_strategy_oracle import assert_values

    row["oracle_values_checked"] = assert_values(
        con, next(iter(tables.values())), mode, model.readiness.for_fact("events")
    )
    row["same_values_and_metadata"] = True
    row["comparison_reference"] = "direct" if "current" not in tables else "current and direct"
    row["output_rows"] = len(expected)
    if bucket_matches is not None:
        row["bucket_joined_rows_including_empty"] = int(bucket_matches.count().execute())
    row["matched_events"] = int(matched.event_id.count().execute())
    row["joined_rows_including_empty"] = int(matched.count().execute())
    row["full_frame_materialization_seconds"] = None
    if full:
        begin = time.perf_counter()
        materialized = frame.to_pyarrow()
        row["full_frame_materialization_seconds"] = time.perf_counter() - begin
        row["full_frame_rows"] = len(materialized)
    print(json.dumps(row, sort_keys=True), flush=True)
    (out / f"{label}.json").write_text(json.dumps(row, indent=2, sort_keys=True))
    return row


def run(con, args, out):
    config = ScaleConfig(
        units=args.units, events_per_unit=args.events_per_unit, batch_size=50_000, anchor_periods=21
    )
    start = time.perf_counter()
    load_tables(con, config)
    table = con.table("events")
    con.create_table("events_native", table.mutate(value=table.value.cast("float64") / 10))
    if args.backend == "postgres":
        for name in ("events", "events_native", "users"):
            con.raw_sql(f'ANALYZE "{name}"').close()
    print(json.dumps({"progress": "loaded", "seconds": time.perf_counter() - start}), flush=True)
    results = []
    for mode in args.modes.split(","):
        for anchors in [int(x) for x in args.anchors.split(",")]:
            results.append(
                measure_case(
                    con,
                    config,
                    mode,
                    anchors,
                    args.backend,
                    args.repeats,
                    out,
                    args.full,
                    tuple(args.variants.split(",")),
                )
            )
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("duckdb", "postgres"), default="duckdb")
    parser.add_argument("--postgres-url")
    parser.add_argument("--units", type=int, default=10_000)
    parser.add_argument("--events-per-unit", type=int, default=30)
    parser.add_argument("--modes", default="integer,native")
    parser.add_argument("--anchors", default="1,7,21")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--variants", default="current,direct")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "ibis": ibis.__version__,
        "pyarrow": pa.__version__,
        "args": vars(args),
    }
    (args.output / "environment.json").write_text(json.dumps(environment, default=str, indent=2))
    if args.backend == "duckdb":
        con = ibis.duckdb.connect(threads=1)
        try:
            run(con, args, args.output)
        finally:
            con.disconnect()
    else:
        schema = f"query_spike_{uuid.uuid4().hex[:12]}"
        admin: Any = ibis.connect(args.postgres_url)
        admin.raw_sql(f'CREATE SCHEMA "{schema}"').close()
        con: Any = ibis.connect(args.postgres_url, schema=schema)
        con.raw_sql("SET max_parallel_workers_per_gather = 0").close()
        con.raw_sql("SET statement_timeout = '180s'").close()
        try:
            run(con, args, args.output)
        finally:
            con.disconnect()
            admin.raw_sql(f'DROP SCHEMA "{schema}" CASCADE').close()
            admin.disconnect()


if __name__ == "__main__":
    main()
