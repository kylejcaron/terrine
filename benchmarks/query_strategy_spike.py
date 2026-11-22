"""Throwaway direct-window experiment, not a supported Terrine compiler.

Run with python -m benchmarks.query_strategy_spike. Only the five cost metrics,
UTC day windows and one event source are implemented in the prototype. The
``--frame-only`` path instead measures validated public Frame materialization,
optionally retained stage/correction/refresh. ``--endpoint-comparator`` instead
compares exact count reducers inside the same public call. No wall-clock gates.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import platform
import statistics
import tempfile
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

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


def local_query(model, frame, windows, resolution):
    """Exercise the production reducer directly, without the declaration selector."""
    from dataclasses import replace

    from terrine.compile.columns import exact_scope
    from terrine.semantic.bind import normalize_events
    from terrine.temporal.aggregate import AggregateRequest, aggregate_windows
    from terrine.temporal.daily import resolve_measure
    from terrine.windows import Window

    sources = {}
    requests = []
    with exact_scope(model):
        for metric_name in METRICS:
            metric = model.metrics[metric_name]
            measures = []
            for measure in metric.measures:
                if measure.source_name not in sources:
                    source = model.sources[measure.source_name]
                    events = normalize_events(
                        model._relation(source.relation), source, source.units["user"]
                    )
                    sources[source.name] = (events, source)
                events, source = sources[measure.source_name]
                measures.append(resolve_measure(events, measure, source=source))
            resolved = replace(metric, measures=tuple(measures))
            requests.extend(
                AggregateRequest(
                    resolved, Window(f"-{days}d", 0), f"{metric_name}_{days}d", "feature"
                )
                for days in windows
            )
        result = aggregate_windows(
            frame._compiled._anchors.table,
            requests,
            sources,
            model.readiness,
            boundary=model.catalog.day_boundary,
            resolution=resolution,
        )
    table = result.table
    selected = {"unit": table.unit, "t0": table.t0}
    for output, meta in result.columns.items():
        selected[output] = table[output]
        for kind in ("complete", "reason", "observed_from", "observed_until"):
            selected[f"{output}__{kind}"] = table[getattr(meta, f"{kind}_column")]
    selected.update(
        {f"check_{index}": table[check.column] for index, check in enumerate(result.row_checks)}
    )
    return result, table.select(**selected)


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
    local_results = {}
    for name, resolution in (("local_raw", "event"), ("local_day", "day")):
        if name in variants:
            begin = time.perf_counter()
            local_results[name], queries[name] = local_query(model, frame, windows, resolution)
            row["build_seconds"][name] = time.perf_counter() - begin
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
    expected_reference = check_table(candidate.to_pyarrow())
    # Alternate order so the same variant does not always benefit from warming.
    for repeat in range(repeats):
        for name in variants if repeat % 2 == 0 else tuple(reversed(variants)):
            row["variants"][name]["server_seconds"].append(timed_sql(con, compiled[name]))
    tables = {}
    for name, expression in queries.items():
        if name in local_results:
            validation_times = []
            for _ in range(repeats):
                begin = time.perf_counter()
                local_results[name].validate(con.to_pyarrow)
                tables[name] = check_table(expression.to_pyarrow())
                validation_times.append(time.perf_counter() - begin)
                compare(tables[name], expected_reference)
                from benchmarks.query_strategy_oracle import assert_values

                assert_values(con, tables[name], mode, model.readiness.for_fact("events"))
            row["variants"][name]["validation_to_arrow_seconds"] = validation_times
            row["variants"][name]["validation_to_arrow_median_seconds"] = statistics.median(
                validation_times
            )
        else:
            begin = time.perf_counter()
            tables[name] = check_table(expression.to_pyarrow())
            row["variants"][name]["query_to_arrow_seconds"] = time.perf_counter() - begin
        row["variants"][name]["server_median_seconds"] = statistics.median(
            row["variants"][name]["server_seconds"]
        )
        row["variants"][name]["output_arrow_bytes"] = tables[name].nbytes
        profile(con, compiled[name], backend, out / f"{label}-{name}-explain.txt")
    expected = expected_reference
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


def assert_support(table, expected):
    """Verify the full origin multiset and unique anchor identities, including ties."""
    origins = table.select(["unit", "t0", "anchor_id"]).to_pylist()
    assert Counter((row["unit"], row["t0"]) for row in origins) == expected
    identities = {(row["unit"], json.dumps(row["anchor_id"], sort_keys=True)) for row in origins}
    assert len(identities) == len(origins)
    return len(origins)


def measure_frame_case(
    con, config, mode, anchor_count, repeats, out, *, irregular=False, retained=False
):
    """Measure the public Frame boundary, including validation and Arrow conversion."""
    from benchmarks.query_strategy_oracle import assert_values
    from terrine import Budget
    from terrine.runtime.changes import Change, ChangeSet
    from terrine.spec import EventAnchor

    windows = tuple(range(1, 11))
    label = f"frame-{mode}-{'irregular' if irregular else f'anchors{anchor_count}'}"
    model = make_model(con, config, mode, "duckdb")
    design = make_design(anchor_count, windows)
    if irregular:
        design = Design("user", EventAnchor("event", "each"), features=design.features)
    begin = time.perf_counter()
    frame = design.build(model)
    expression = frame.expr
    build_seconds = time.perf_counter() - begin
    begin = time.perf_counter()
    sql = con.compile(expression)
    compile_seconds = time.perf_counter() - begin
    pretty = str(ibis.to_sql(expression, dialect="duckdb", pretty=True))  # noqa: TID251
    compact = str(ibis.to_sql(expression, dialect="duckdb", pretty=False))  # noqa: TID251
    (out / f"{label}.sql").write_text(sql)
    units = set(con.table("users").to_pyarrow()["unit_id"].to_pylist())
    if irregular:
        origin_rows = con.table("events").select("unit_id", "event_time").to_pyarrow().to_pylist()
        expected_support = Counter(
            (row["unit_id"], row["event_time"]) for row in origin_rows if row["unit_id"] in units
        )
    else:
        expected_support = Counter(
            (unit, EPOCH + timedelta(days=14 + index))
            for unit in units
            for index in range(anchor_count)
        )
    times = []
    checks = 0
    result = None
    budget = Budget(strict_scan=False)
    for _ in range(repeats):
        begin = time.perf_counter()
        result = frame.materialize(budget=budget).table
        times.append(time.perf_counter() - begin)
        assert_support(result, expected_support)
        selected = {"unit": result["unit"], "t0": result["t0"]}
        for metric in METRICS:
            for days in windows:
                name = f"{metric}_{days}d"
                selected[name] = result[name]
                for kind in ("complete", "reason", "observed_from", "observed_until"):
                    column = getattr(frame.columns[name], f"{kind}_column")
                    selected[f"{name}__{kind}"] = result[column]
        checks += assert_values(con, pa.table(selected), mode, model.readiness.for_fact("events"))
    assert result is not None
    profile(con, sql, "duckdb", out / f"{label}-explain.txt")
    events = con.table("events")
    bucket_count = (
        events.select("unit_id", bucket=events.event_time.truncate("D"))
        .distinct()
        .count()
        .execute()
    )
    row = {
        "case": label,
        "units": config.units,
        "expected_output_rows": sum(expected_support.values()),
        "all_samples_support_verified": True,
        "events": config.units * config.events_per_unit,
        "distinct_unit_day_buckets": int(bucket_count),
        "outputs": len(METRICS) * len(windows),
        "output_rows": len(result),
        "output_arrow_bytes": result.nbytes,
        "build_seconds": build_seconds,
        "compile_seconds": compile_seconds,
        "submitted_sql_bytes": len(sql.encode()),
        "formatted_sql_bytes": len(pretty.encode()),
        "compact_sql_bytes": len(compact.encode()),
        "full_frame_materialization_seconds": times,
        "full_frame_median_seconds": statistics.median(times),
        "oracle_values_checked": checks,
        "strategy": dict(frame._compiled.strategy),
    }
    if retained:
        retained_samples = []
        for sample in range(repeats):
            begin = time.perf_counter()
            staged = frame.stage(f"{label.replace('-', '_')}_{sample}", budget=budget)
            stage_seconds = time.perf_counter() - begin
            old_table = staged.to_pyarrow()
            assert_support(old_table, expected_support)
            relation = "events_native" if mode == "native" else "events"
            begin = time.perf_counter()
            con.raw_sql(f'UPDATE "{relation}" SET value=value+1 WHERE value IS NOT NULL')
            correction_seconds = time.perf_counter() - begin
            changes = ChangeSet((Change("update", "events"),), "operator")
            begin = time.perf_counter()
            refreshed = staged.refresh(model, changes=changes, budget=budget)
            refresh_seconds = time.perf_counter() - begin
            actual = refreshed.to_pyarrow()
            expected = frame.materialize(budget=budget).table
            assert_support(actual, expected_support)
            assert_support(expected, expected_support)
            assert actual.equals(expected), "retained correction differs from full public rebuild"
            assert staged.to_pyarrow().equals(old_table), "old generation changed after correction"
            selected = {"unit": actual["unit"], "t0": actual["t0"]}
            for metric in METRICS:
                for days in windows:
                    name = f"{metric}_{days}d"
                    selected[name] = actual[name]
                    for kind in ("complete", "reason", "observed_from", "observed_until"):
                        selected[f"{name}__{kind}"] = actual[
                            getattr(frame.columns[name], f"{kind}_column")
                        ]
            retained_samples.append(
                {
                    "stage_seconds": stage_seconds,
                    "source_correction_seconds": correction_seconds,
                    "refresh_seconds": refresh_seconds,
                    "output_rows": len(actual),
                    "support_verified": True,
                    "oracle_values_checked": assert_values(
                        con, pa.table(selected), mode, model.readiness.for_fact("events")
                    ),
                    "old_generation_unchanged": True,
                }
            )
        row["retained_samples"] = retained_samples
        for operation in ("stage", "refresh"):
            row[f"retained_{operation}_median_seconds"] = statistics.median(
                sample[f"{operation}_seconds"] for sample in retained_samples
            )
    (out / f"{label}.json").write_text(json.dumps(row, default=str, indent=2, sort_keys=True))
    print(json.dumps(row, default=str, sort_keys=True), flush=True)
    return row


def _endpoint_counts(builder, events, source, primitives, *, day, origin_bounds):
    """Benchmark-only exact state at a unit's instants; never selected by Terrine."""
    from terrine.backends.joins import asof_join
    from terrine.backends.numeric import exact_literal, exact_subtract, exact_sum
    from terrine.backends.timestamps import local_day
    from terrine.temporal import aggregate as reducer
    from terrine.temporal.daily import _batch_projections, contributions

    assert source.day_boundary == "UTC" and all(key.counting for key in primitives)
    memberships = reducer._membership_keys(source, primitives)
    contexts = {ctx.available: ctx for group in primitives.values() for ctx in group}
    endpoints = {}
    for key in memberships.values():
        ctx = contexts[key.available[0]]
        endpoints[key.window.start.seconds] = ctx.start
        endpoints[key.window.end.seconds] = ctx.end

    timestamp = "_timestamp"
    if day:
        bounds = (
            origin_bounds[0] + timedelta(seconds=min(endpoints)),
            origin_bounds[1] + timedelta(seconds=max(endpoints)),
        )
        events, timestamp = reducer._masked_day_events(
            builder, events, contexts.values(), endpoint_bounds=bounds
        )
    events = events.filter(events[timestamp].notnull())
    state_unit, tick = builder.name("state_unit"), builder.name("state_tick")
    clock = local_day(events[timestamp], 0)[1] if day else events[timestamp]
    events = events.mutate(**{state_unit: events._unit, tick: clock})
    fields_by_measure = {}
    branches = []
    for primitive in primitives:
        if primitive.measure in fields_by_measure:
            continue
        projected, fields = contributions(
            events, primitive.measure, source=source, counting=True, _name=builder.name
        )
        aliases = {field: builder.name(field) for field in fields}
        fields_by_measure[primitive.measure] = aliases
        branches.append(
            projected.select(**{aliases[field]: value for field, value in fields.items()})
        )
    leaves = _batch_projections(events, branches)
    state_fields = [field for aliases in fields_by_measure.values() for field in aliases.values()]
    grouped = leaves.group_by(state_unit, tick).aggregate(
        **{field: exact_sum(leaves[field]) for field in state_fields}
    )
    prefix = ibis.window(group_by=grouped[state_unit], order_by=grouped[tick], rows=(None, 0))
    state = grouped.mutate(
        **{field: exact_sum(grouped[field]).over(prefix) for field in state_fields}
    )

    unit, anchor, point, index, pair = (
        builder.name(name) for name in ("unit", "anchor_id", "endpoint", "endpoint_index", "pair")
    )
    points = builder.table
    request = points.select(
        points.unit.name(unit),
        points.anchor_id.name(anchor),
        *contexts,
        **{
            pair: ibis.array(
                [
                    ibis.struct({"instant": points[name], "position": i})
                    for i, name in enumerate(endpoints.values())
                ]
            ).unnest()
        },
    )
    request = request.select(
        unit,
        anchor,
        *contexts,
        **{point: request[pair]["instant"], index: request[pair]["position"]},
    )
    # P(e) sums only ticks strictly before e; P(end) - P(start) is [start, end).
    hit = asof_join(
        request,
        state,
        left_unit=unit,
        left_key=point,
        right_unit=state_unit,
        right_key=tick,
        inclusive=False,
    )
    positions = {offset: i for i, offset in enumerate(endpoints)}
    columns = {}
    pivot = {}
    for primitive in primitives:
        for offset in (primitive.window.start.seconds, primitive.window.end.seconds):
            for field in fields_by_measure[primitive.measure].values():
                if (offset, field) not in columns:
                    name = columns[offset, field] = builder.name("endpoint_state")
                    pivot[name] = hit[field].max(where=hit[index] == positions[offset])
    paired = hit.group_by(unit, anchor, *contexts).aggregate(**pivot)

    def zeroed(name):
        value = paired[name]
        return value.fill_null(exact_literal(0, value))

    payload = {}
    reduced_fields = {}
    for primitive, membership in memberships.items():
        active = reducer._available(paired, membership)
        start, end = primitive.window.start.seconds, primitive.window.end.seconds
        aliases = reduced_fields[primitive] = {}
        for field, state_field in fields_by_measure[primitive.measure].items():
            name = aliases[field] = builder.name(field)
            delta = exact_subtract(
                zeroed(columns[end, state_field]), zeroed(columns[start, state_field])
            )
            payload[name] = active.ifelse(delta, exact_literal(0, delta))
    return paired.select(unit, anchor, **payload), reduced_fields, (unit, anchor)


@contextlib.contextmanager
def _count_reducer(variant):
    from terrine.temporal import aggregate as reducer

    def raw(builder, events, source, primitives, boundary, origin_bounds):
        return reducer._reduce_source(builder, events, source, primitives, origin_bounds)

    def day_endpoints(builder, events, source, primitives, boundary, origin_bounds):
        return _endpoint_counts(
            builder, events, source, primitives, day=True, origin_bounds=origin_bounds
        )

    def event_endpoints(builder, events, source, primitives, origin_bounds):
        return _endpoint_counts(
            builder, events, source, primitives, day=False, origin_bounds=origin_bounds
        )

    with contextlib.ExitStack() as restore:
        if variant == "raw":
            restore.enter_context(patch.object(reducer, "_reduce_daily_source", raw))
        elif variant == "endpoints":
            restore.enter_context(patch.object(reducer, "_reduce_daily_source", day_endpoints))
            restore.enter_context(patch.object(reducer, "_reduce_source", event_endpoints))
        yield


def _count_diagnostics(con, submit, path):
    from benchmarks.compare_sql import _capture_study_sql, _phases

    with _capture_study_sql() as statements, _phases() as phases:
        result = submit()
    profiles = []
    for i, sql in enumerate(statements):
        path.with_name(f"{path.name}-{i}.sql").write_text(sql)
        with contextlib.closing(con.con.cursor()) as cursor:
            row = cursor.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql).fetchone()
        assert row is not None
        tree = json.loads(row[1])
        path.with_name(f"{path.name}-{i}-plan.json").write_text(row[1])
        nodes = list(tree["children"])
        operators = []
        while nodes:
            node = nodes.pop()
            nodes.extend(reversed(node.get("children", ())))
            name = node.get("operator_name", "")
            if any(kind in name for kind in ("JOIN", "GROUP_BY", "WINDOW", "UNNEST")):
                operators.append((name, node["operator_cardinality"], node["operator_timing"]))
        profiles.append(operators)
    return result, phases, profiles


def measure_count_case(con, config, args, out):
    from benchmarks.query_strategy_oracle import assert_count_values, count_inputs
    from terrine import Budget
    from terrine.spec import EventAnchor

    first = EPOCH + timedelta(days=30)
    anchors = int(args.anchors)
    windows = tuple(range(3, 31, 3))
    anchor = (
        EventAnchor("event", "each")
        if args.irregular
        else Schedule("1d", first.date(), first.date() + timedelta(days=anchors - 1))
    )
    model = make_model(con, config, "integer", "duckdb")
    design = Design(
        "user",
        anchor,
        features={
            f"{metric}_{days}d": agg(metric, window=(f"-{days}d", "0s"))
            for metric in ("events", "active")
            for days in windows
        },
    )
    expected, timestamps = count_inputs(con, irregular=args.irregular, first=first, anchors=anchors)
    coverage = model.readiness.for_fact("events")
    budget = Budget(strict_scan=False)
    variants = [name for name in args.variants.split(",") if not (args.irregular and name == "raw")]
    timings: dict[str, list[tuple[float, float, float]]] = {name: [] for name in variants}

    def submit():
        begin = time.perf_counter()
        frame = design.build(model)
        built = time.perf_counter()
        result = frame.materialize(budget=budget)
        done = time.perf_counter()
        return result, (built - begin, done - built, done - begin)

    def verify(result):
        assert_support(result.table, expected)
        summary = result.provenance.eligibility_summary
        size = sum(expected.values())
        assert summary is not None
        assert (summary.candidate, summary.excluded_false, summary.unknown, summary.retained) == (
            size,
            0,
            0,
            size,
        )
        return assert_count_values(result, timestamps, windows, coverage)

    records = {}
    for sample in range(args.repeats):
        offset = sample % len(variants)
        for variant in variants[offset:] + variants[:offset]:
            with _count_reducer(variant):
                result, timing = submit()
            verify(result)
            timings[variant].append(timing)
    for variant in variants:
        with _count_reducer(variant):
            result, phases, operators = _count_diagnostics(con, lambda: submit()[0], out / variant)
        checked = verify(result)
        records[variant] = {
            "timings": timings[variant],
            "median_public_seconds": statistics.median(sample[2] for sample in timings[variant]),
            "oracle_values_checked": checked * args.repeats,
            "phase_diagnostic": phases,
            "submitted_statement_join_and_state_operators": operators,
        }
    report = {
        "duckdb": con.version,
        "fixture": config.to_dict(),
        "irregular": args.irregular,
        "sparse": args.poor_compression,
        "outputs": len(windows) * 2,
        "rows": sum(expected.values()),
        "windows_days": windows,
        "timing_columns": ("build_seconds", "materialize_seconds", "public_seconds"),
        "strategies": records,
    }
    (out / "endpoint-comparison.json").write_text(json.dumps(report, indent=2))
    print({name: row["median_public_seconds"] for name, row in records.items()}, flush=True)
    return report


def run(con, args, out):
    spacing_days = 2 if args.endpoint_comparator else 1
    span_days = 60 if args.endpoint_comparator else 30
    if args.poor_compression:
        span_days = max(span_days, spacing_days * args.events_per_unit)
    config = ScaleConfig(
        span_days=span_days,
        units=args.units,
        events_per_unit=args.events_per_unit,
        batch_size=50_000,
        anchor_periods=21,
    )
    start = time.perf_counter()
    load_tables(con, config)
    if args.poor_compression:
        # One row per unit/day, with intraday jitter: summaries cannot compress it.
        events = con.table("events")
        numbered = events.mutate(
            _event_index=ibis.row_number().over(
                ibis.window(group_by=events.unit_id, order_by=events.event_id)
            )
        )
        index = numbered._event_index.cast("int64")
        jitter_index = index * index if args.endpoint_comparator and args.irregular else index
        spread = numbered.mutate(
            event_time=ibis.literal(EPOCH)
            + (index * (86400 * spacing_days) + (jitter_index * 997) % 40000 + 1800).as_interval(
                "s"
            )
        ).drop("_event_index")
        con.create_table("events_spread", spread)
        con.drop_table("events")
        con.create_table("events", con.table("events_spread"))
        con.drop_table("events_spread")
    if args.endpoint_comparator:
        print(
            json.dumps({"progress": "loaded", "seconds": time.perf_counter() - start}), flush=True
        )
        return [measure_count_case(con, config, args, out)]
    table = con.table("events")
    con.create_table("events_native", table.mutate(value=table.value.cast("float64") / 10))
    if args.backend == "postgres":
        for name in ("events", "events_native", "users"):
            con.raw_sql(f'ANALYZE "{name}"').close()
    print(json.dumps({"progress": "loaded", "seconds": time.perf_counter() - start}), flush=True)
    results = []
    for mode in args.modes.split(","):
        anchor_counts = (1,) if args.irregular else [int(x) for x in args.anchors.split(",")]
        for anchors in anchor_counts:
            if args.frame_only:
                results.append(
                    measure_frame_case(
                        con,
                        config,
                        mode,
                        anchors,
                        args.repeats,
                        out,
                        irregular=args.irregular,
                        retained=args.retained,
                    )
                )
                continue
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
    parser.add_argument(
        "--frame-only", action="store_true", help="time validated public materialization"
    )
    parser.add_argument("--poor-compression", action="store_true", help="one event per unit/day")
    parser.add_argument("--irregular", action="store_true", help="event-relative repeated anchors")
    parser.add_argument("--retained", action="store_true", help="also time stage/correct/refresh")
    parser.add_argument("--endpoint-comparator", action="store_true")
    args = parser.parse_args()
    if args.frame_only and args.backend != "duckdb":
        parser.error("--frame-only currently measures DuckDB")
    if args.irregular and not (args.frame_only or args.endpoint_comparator):
        parser.error("--irregular requires --frame-only or --endpoint-comparator")
    if args.retained and not args.frame_only:
        parser.error("--retained requires --frame-only")
    if args.endpoint_comparator:
        if args.backend != "duckdb" or args.frame_only or args.full:
            parser.error("--endpoint-comparator is a separate DuckDB-only public benchmark")
        if args.variants == "current,direct":
            args.variants = "current,raw,endpoints"
        variants = set(args.variants.split(","))
        if not variants <= {"current", "raw", "endpoints"}:
            parser.error("endpoint comparator variants: current, raw, endpoints")
        if args.irregular and variants == {"raw"}:
            parser.error("current already uses raw on irregular anchors")
        if "," in args.anchors or not args.anchors.isdecimal() or int(args.anchors) < 2:
            parser.error("--endpoint-comparator requires one --anchors count of at least two")
        if args.repeats < 1:
            parser.error("--endpoint-comparator requires at least one repeat")
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "ibis": ibis.__version__,
        "pyarrow": pa.__version__,
        "args": {key: value for key, value in vars(args).items() if key != "postgres_url"},
    }
    (args.output / "environment.json").write_text(json.dumps(environment, default=str, indent=2))
    if args.backend == "duckdb":
        with (
            contextlib.ExitStack() as stack,
            tempfile.TemporaryDirectory(prefix="terrine-window-benchmark-") as temporary,
        ):
            if args.endpoint_comparator:
                import fcntl

                directory = Path(os.environ.get("TMPDIR", tempfile.gettempdir()))
                lock = stack.enter_context((directory / "terrine-arch-nj0y-profile.lock").open("a"))
                fcntl.flock(lock, fcntl.LOCK_EX)
            database = str(Path(temporary) / "owner.duckdb") if args.retained else ":memory:"
            con = ibis.duckdb.connect(database, threads=1)
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
