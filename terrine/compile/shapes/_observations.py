"""Point-observation state relations and absorbing-terminal compilation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from terrine.backends.timestamps import epoch_us_sql
from terrine.compile.anchors import (
    AnchorTable,
    _precision_check,
    _unique_checks,
    _utc_column,
)
from terrine.compile.columns import CompiledStudy, compile_columns
from terrine.data import ColumnMeta, Roles
from terrine.semantic.bind import normalize_events
from terrine.semantic.models import TableRef
from terrine.spec.anchors import FixedDate, Schedule
from terrine.spec.design import Design
from terrine.spec.state_panel import _INVALID_OBSERVATIONS, StateObservations, StatePanel

if TYPE_CHECKING:
    from terrine.model import Model


@dataclass(frozen=True, slots=True)
class _ObservationCheck:
    query: ibis.Table
    reason: str

    def validate(self, execute: Callable[[ibis.Table], pa.Table]) -> None:
        result = execute(self.query)
        if (
            not isinstance(result, pa.Table)
            or result.num_rows != 1
            or result.column_names != ["invalid_count"]
        ):
            raise ValueError("observation integrity checks require one named Arrow count")
        result.validate(full=True)
        count = result["invalid_count"][0].as_py()
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("observation integrity count must be a nonnegative integer")
        if count:
            raise _INVALID_OBSERVATIONS.error(reason=self.reason, failed_count=count)


def _count(query: ibis.Table) -> ibis.Table:
    return query.aggregate(invalid_count=ibis._.count())


def _period_design(
    design: Design,
    anchor: FixedDate | Schedule,
    *,
    features,
    filter_population: bool = False,
) -> Design:
    return Design(
        design.unit,
        anchor,
        features=features,
        eligibility=design.eligibility if filter_population else None,
        gap=design.gap,
        shape="cross_section",
        anchor_range=design.anchor_range if filter_population else None,
    )


def _epoch_microseconds(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value.astimezone(UTC) - epoch
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


def _points(design: Design, model: Model, state: StateObservations):
    schedule = design.anchor
    assert isinstance(schedule, Schedule)
    candidates = [
        (source, source.properties[state.property])
        for source in model.sources.values()
        if state.property in source.properties
    ]
    source, prop = candidates[0]
    raw = model._relation(source.relation)
    events = normalize_events(raw, source, source.entities[design.unit])
    precision = _precision_check(model, events, "_timestamp", source_column=source.timestamp_column)
    observations = events.select(
        unit=events._unit,
        point_ts=events._timestamp,
        point_state=events[prop.column],
    )
    observations = _utc_column(model, observations, "point_ts", "point_time")
    coverage = model.readiness.for_fact(source.name)
    start = model.day_boundary(schedule.start).astimezone(UTC)
    end = model.day_boundary(schedule.end).astimezone(UTC)
    all_points = observations.select(
        unit=observations.unit,
        point_time=observations.point_time,
        point_state=observations.point_state,
        point_found=ibis.literal(True),
    )
    bounded = all_points.filter(
        (all_points.point_time >= ibis.literal(coverage.start))
        & (all_points.point_time <= ibis.literal(end))
    )
    raw_checks = (
        _ObservationCheck(
            _count(
                observations.filter(observations.unit.isnull() | observations.point_ts.isnull())
            ),
            "null observation unit or timestamp",
        ),
        _ObservationCheck(
            _count(
                observations.group_by("unit", "point_ts")
                .aggregate(observation_count=ibis._.count())
                .filter(lambda table: table.observation_count > 1)
            ),
            "duplicate (unit, observation_time) key",
        ),
        _ObservationCheck(
            _count(
                observations.filter(
                    observations.point_state.notnull()
                    & ~observations.point_state.isin(state.levels)
                )
            ),
            "observation contains an undeclared state level",
        ),
    )
    # Exact signed microseconds keep schedule-grid validation exact.
    panel_points = all_points.filter(
        (all_points.point_time >= ibis.literal(start))
        & (all_points.point_time <= ibis.literal(end))
    )
    dialect = model.catalog.dialect
    sql = ibis.to_sql(panel_points, dialect=dialect.casefold())
    start_us = _epoch_microseconds(start)
    period_us = schedule.every * 1_000_000
    with_grid = model._sql(
        f"SELECT state_points.*, (({epoch_us_sql(dialect, 'point_time')} - {start_us}) "
        f"% {period_us} = 0) AS __on_grid FROM ({sql}) AS state_points"
    )
    return coverage, bounded, with_grid, precision, raw_checks


def _bounds_relation(design: Design, model: Model):
    unit_source = model.units[design.unit]
    raw = model._relation(TableRef(kind="table", name=unit_source.table))
    checks = _unique_checks(raw, unit_source.key, "roster")
    projection = {"unit": raw[unit_source.key]}
    for name, column in (
        ("observed_from_raw", unit_source.observed_from),
        ("observed_until_raw", unit_source.observed_until),
    ):
        if column is not None:
            projection[name] = raw[column]
            precision = _precision_check(model, raw, column)
            if precision is not None:
                checks.append(precision)
    roster = raw.select(**projection)
    for raw_name, utc_name in (
        ("observed_from_raw", "observed_from_utc"),
        ("observed_until_raw", "observed_until_utc"),
    ):
        if raw_name in roster.columns:
            roster = _utc_column(model, roster, raw_name, utc_name)
    bounds = roster.select(
        unit=roster.unit,
        _observed_from=(
            roster.observed_from_utc
            if "observed_from_utc" in roster.columns
            else ibis.literal(None, type="timestamp('UTC')")
        ),
        _observed_until=(
            roster.observed_until_utc
            if "observed_until_utc" in roster.columns
            else ibis.literal(None, type="timestamp('UTC')")
        ),
    )
    return bounds, AnchorTable(bounds, tuple(checks))


def _observed_usable(points, bounds, coverage):
    point_time = points.point_time
    source_ready = (point_time >= ibis.literal(coverage.start)) & (
        point_time < ibis.literal(coverage.end_exclusive)
    )
    unit_ready = (bounds._observed_from.isnull() | (bounds._observed_from <= point_time)) & (
        bounds._observed_until.isnull() | (point_time < bounds._observed_until)
    )
    usable = source_ready & unit_ready
    return usable


def _status(table, point, *, time, coverage):
    source_ready = (time >= ibis.literal(coverage.start)) & (
        time < ibis.literal(coverage.end_exclusive)
    )
    unit_ready = (table._observed_from.isnull() | (table._observed_from <= time)) & (
        table._observed_until.isnull() | (time < table._observed_until)
    )
    found = point.point_found.fill_null(False)
    complete = source_ready & unit_ready & found
    value = ibis.ifelse(complete, point.point_state, ibis.null().cast("string"))
    reason = ibis.cases(
        (~source_ready, "incomplete_coverage"),
        (~unit_ready, "incomplete_observation"),
        (~found, "missing_state_observation"),
        (point.point_state.isnull(), "state_value_null"),
        else_=ibis.null().cast("string"),
    )
    return value, complete, reason


def compile_observed_state_panel(
    design: Design,
    model: Model,
    panel: StatePanel,
    *,
    persisted_prefixes: Mapping[tuple[str, str, str, str], ibis.Table] | None = None,
    persisted_anchors: ibis.Table | None = None,
) -> CompiledStudy:
    """Compile exact point lookups, independently usable states, and terminal suffixes."""
    state = panel.state
    assert isinstance(state, StateObservations)
    schedule = design.anchor
    assert isinstance(schedule, Schedule)
    coverage, points, grid_points, precision, checks = _points(design, model, state)
    bounds, bounds_anchors = _bounds_relation(design, model)
    point_bounds = points.left_join(bounds, points.unit == bounds.unit)
    usable = _observed_usable(
        point_bounds, point_bounds, coverage
    ) & point_bounds.point_found.fill_null(False)
    terminal_candidates = point_bounds.filter(
        usable & point_bounds.point_state.isin(state.absorbing)
    ).select(
        unit=point_bounds.unit,
        terminal_time=point_bounds.point_time,
        terminal_state=point_bounds.point_state,
    )
    first = terminal_candidates.group_by("unit").aggregate(
        terminal_time=terminal_candidates.terminal_time.min()
    )
    terminals = terminal_candidates.inner_join(
        first,
        (terminal_candidates.unit == first.unit)
        & (terminal_candidates.terminal_time == first.terminal_time),
    ).select(
        unit=terminal_candidates.unit,
        terminal_time=terminal_candidates.terminal_time,
        terminal_state=terminal_candidates.terminal_state,
    )
    later_relation = point_bounds.inner_join(terminals, point_bounds.unit == terminals.unit)
    later_usable = _observed_usable(
        later_relation, later_relation, coverage
    ) & later_relation.point_found.fill_null(False)
    later = later_relation.filter(
        later_usable
        & (later_relation.point_time > later_relation.terminal_time)
        & (
            later_relation.point_state.notnull()
            & (later_relation.point_state != later_relation.terminal_state)
        )
    )
    checks = list(checks)
    checks.append(
        _ObservationCheck(
            _count(later),
            "usable state changes after an absorbing observation",
        )
    )
    off_grid = grid_points.left_join(terminals, grid_points.unit == terminals.unit).filter(
        ~grid_points.__on_grid
        & ~(
            terminals.terminal_time.notnull()
            & (terminals.terminal_time <= grid_points.point_time)
            & (terminals.terminal_state == grid_points.point_state)
        )
    )
    checks.append(_ObservationCheck(_count(off_grid), "off-grid nonterminal observation"))

    output_design = _period_design(
        design,
        schedule,
        features=design.features,
        filter_population=True,
    )
    compiled = compile_columns(
        output_design,
        model,
        persisted_prefixes=persisted_prefixes,
        persisted_anchors=persisted_anchors,
    )
    interval = ibis.interval(seconds=schedule.every)

    def attach(source_rows):
        term = terminals.view()
        origin = points.view()
        endpoint = points.view()
        joined = source_rows.left_join(term, source_rows.unit == term.unit)
        scheduled_to = source_rows.t0 + interval
        terminal_used = (term.terminal_time > source_rows.t0) & (term.terminal_time <= scheduled_to)
        to_time = ibis.cases(
            (terminal_used, term.terminal_time),
            else_=scheduled_to,
        )
        joined = joined.filter(term.terminal_time.isnull() | (term.terminal_time > source_rows.t0))
        joined = joined.left_join(
            origin,
            (source_rows.unit == origin.unit) & (source_rows.t0 == origin.point_time),
        ).left_join(
            endpoint,
            (source_rows.unit == endpoint.unit) & (to_time == endpoint.point_time),
        )
        from_value, from_complete, from_reason = _status(
            source_rows, origin, time=source_rows.t0, coverage=coverage
        )
        to_value, to_complete, to_reason = _status(
            source_rows, endpoint, time=to_time, coverage=coverage
        )
        projection = {name: source_rows[name] for name in source_rows.columns}
        projection.update(
            from_time=source_rows.t0,
            to_time=to_time,
            scheduled_to=scheduled_to,
            period_index=source_rows.anchor_id.re_replace("^schedule:", "").cast("int64"),
            from_state=from_value,
            to_state=to_value,
            terminal=terminal_used.fill_null(False),
            from_state_complete=from_complete,
            from_state_reason=from_reason,
            to_state_complete=to_complete,
            to_state_reason=to_reason,
        )
        relation = joined.select(**projection)
        dialect = model.catalog.dialect
        query = ibis.to_sql(relation, dialect=dialect.casefold())
        later, earlier = epoch_us_sql(dialect, "to_time"), epoch_us_sql(dialect, "from_time")
        gap = f"CAST({later} - {earlier} AS BIGINT)"
        return model._sql(f"SELECT state_rows.*, {gap} AS gap FROM ({query}) AS state_rows")

    retained = attach(compiled.table)
    audit = attach(compiled.audit_table)
    baseline_expression_timings = {}
    baseline_compiled = None
    if panel.baseline_features:
        baseline_design = _period_design(
            design,
            FixedDate(schedule.start),
            features=panel.baseline_features,
        )
        baseline_compiled = compile_columns(
            baseline_design,
            model,
            include_membership=False,
            persisted_prefixes=persisted_prefixes,
        )
        base = baseline_compiled.table
        projection = {"unit": base.unit}
        remapped = {}
        remapped_timings = {}
        for index, name in enumerate(panel.baseline_features):
            projection[name] = base[name]
            meta = baseline_compiled.columns[name]
            renames = {}
            for attribute in (
                "observed_from_column",
                "observed_until_column",
                "complete_column",
                "reason_column",
                "absolute_error_column",
            ):
                old = getattr(meta, attribute)
                if old is not None:
                    alias = f"__terrine_baseline_{index}_{attribute}"
                    projection[alias] = base[old]
                    renames[attribute] = alias
            timing = baseline_compiled.expression_timings.get(name)
            if timing is not None:
                timing_replacements = {}
                for field in (
                    "point_time_column",
                    "start_bound_column",
                    "end_bound_column",
                ):
                    original = getattr(timing, field)
                    if original is not None:
                        alias = f"__terrine_baseline_{index}_{field}"
                        projection[alias] = base[original]
                        timing_replacements[field] = alias
                remapped_timings[name] = replace(timing, **timing_replacements)
            remapped[name] = replace(meta, **renames)
        baseline_expression_timings = remapped_timings
        baseline = base.select(**projection)
        retained = retained.left_join(baseline, retained.unit == baseline.unit)
        audit = audit.left_join(baseline, audit.unit == baseline.unit)
    feature_names = tuple(design.features) + tuple(panel.baseline_features)
    columns = dict(compiled.columns)
    cutoffs = dict(compiled.feature_cutoffs)
    if baseline_compiled is not None:
        columns.update(remapped)
        cutoffs.update(
            {
                name: replace(cutoff, reference="panel_start")
                for name, cutoff in baseline_compiled.feature_cutoffs.items()
            }
        )
    columns.update(
        {
            "from_state": ColumnMeta(
                None, "prior_state", None, None, "from_state_complete", "from_state_reason"
            ),
            "to_state": ColumnMeta(
                None, "outcome", None, None, "to_state_complete", "to_state_reason"
            ),
        }
    )
    roles = Roles(
        unit="unit",
        anchor="anchor_id",
        t0="t0",
        treatment=None,
        features=feature_names,
        outcomes=("to_state",),
        cluster_unit="unit",
        prior_state="from_state",
    )
    projection_names = [
        "unit",
        "anchor_id",
        "t0",
        "from_time",
        "to_time",
        "scheduled_to",
        "period_index",
        "gap",
        "from_state",
        "to_state",
        "terminal",
        *design.eligibility,
        *feature_names,
        "from_state_complete",
        "from_state_reason",
        "to_state_complete",
        "to_state_reason",
    ]
    for meta in columns.values():
        for name in (
            meta.observed_from_column,
            meta.observed_until_column,
            meta.complete_column,
            meta.reason_column,
            meta.absolute_error_column,
        ):
            if name is not None and name not in projection_names:
                projection_names.append(name)
    for timing in (
        *compiled.expression_timings.values(),
        *(baseline_expression_timings.values() if baseline_compiled is not None else ()),
    ):
        for name in (
            timing.point_time_column,
            timing.start_bound_column,
            timing.end_bound_column,
        ):
            if name is not None and name not in projection_names:
                projection_names.append(name)
    panel_start = ibis.literal(model.day_boundary(schedule.start).astimezone(UTC))
    retained = retained.mutate(__terrine_panel_start=panel_start)
    audit = audit.mutate(__terrine_panel_start=panel_start)
    projection_names.append("__terrine_panel_start")
    retained = retained.select(**{name: retained[name] for name in projection_names})
    audit = audit.select(
        **{name: audit[name] for name in projection_names},
        __terrine_eligible=audit.__terrine_eligible,
    )
    anchor_checks = list(compiled._anchors._checks) + list(bounds_anchors._checks)
    if precision is not None:
        anchor_checks.append(precision)
    deferred = list(compiled._checks) + list(checks)
    if baseline_compiled is not None:
        deferred.extend(baseline_compiled._checks)
        anchor_checks.extend(baseline_compiled._anchors._checks)
    anchors = AnchorTable(compiled._anchors.table, _checks=tuple(anchor_checks))
    return CompiledStudy(
        retained,
        audit,
        roles,
        columns,
        cutoffs,
        compiled.eligibility_cutoffs,
        {**compiled.strategy, "from_state": "point_observation", "to_state": "point_observation"},
        anchors,
        tuple(deferred),
        {
            **(
                {
                    name: (
                        replace(timing, point_time_column="__terrine_panel_start")
                        if timing.kind == "fixed_t0"
                        else timing
                    )
                    for name, timing in baseline_expression_timings.items()
                }
                if baseline_compiled is not None
                else {}
            ),
            **compiled.expression_timings,
        },
        range_plans={
            **(baseline_compiled.range_plans if baseline_compiled is not None else {}),
            **compiled.range_plans,
        },
    )
