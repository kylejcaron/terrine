"""Plain-language summary of a validated design, read from bound metadata only."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from decimal import Decimal
from textwrap import fill
from typing import TYPE_CHECKING

from terrine.semantic.time import boundary_timezone
from terrine.spec.anchors import EventAnchor, FixedDate, Schedule
from terrine.spec.expressions import (
    And,
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    AttributeAsOf,
    Eq,
    EventTime,
    Expr,
    Gt,
    IsNull,
    JsonScalar,
    MetricAgg,
    MetricQuantile,
    Predicate,
    RetentionMetric,
)
from terrine.spec.panels import (
    CalendarComparisonSpec,
    EventPanelSpec,
    Initiation,
    PersistentAdoption,
    SequentialSpec,
)
from terrine.spec.sequence import EventSequence
from terrine.spec.state_panel import BinnedMetricState, StatePanel
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent
from terrine.spec.validate import resolve_unit, validate_design
from terrine.windows import Window

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from terrine.model import Model
    from terrine.semantic.models import BoundMeasure
    from terrine.spec.design import Design

__all__ = ["describe_design"]

_WIDTH = 100
_LISTED_DATES = 12
_UNITS = (("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1))
_AGGREGATIONS = {
    "count": "count of",
    "sum": "sum of",
    "avg_event": "mean of",
    "avg_calendar_day": "mean per calendar day of",
    "min": "min of",
    "max": "max of",
    "count_distinct": "distinct count of",
}
_OPERATORS = {
    "equals": "=",
    "not_equals": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}
_PANEL_SHAPES = (SequentialSpec, EventPanelSpec, CalendarComparisonSpec)
_SHAPE_LABELS = (
    (SequentialSpec, "trial panel"),
    (EventPanelSpec, "event panel"),
    (CalendarComparisonSpec, "calendar comparison"),
    (CohortSpec, "trajectory"),
    (TimeToEvent, "time-to-event"),
    (LifeTable, "life table"),
    (StatePanel, "state panel"),
    (EventSequence, "event sequence"),
)

type Line = tuple[str, str]


def _duration(seconds: int, *, weeks: bool = False) -> str:
    """Use the largest exact elapsed unit, with weeks for schedule steps only."""
    magnitude = abs(seconds)
    if not magnitude:
        return "0s"
    if weeks and magnitude % 604_800 == 0:
        return f"{magnitude // 604_800}w"
    for unit, size in _UNITS:
        if magnitude % size == 0:
            return f"{magnitude // size}{unit}"
    raise AssertionError("every integer is a multiple of one second")


def _offset(seconds: int, origin: str) -> str:
    if seconds == 0:
        return origin
    return f"{origin} {'+' if seconds > 0 else '-'} {_duration(seconds)}"


def _window(window: Window, origin: str) -> str:
    return f"[{_offset(window.start, origin)}, {_offset(window.end, origin)})"


def _ordinal(n: int | None) -> str:
    assert n is not None
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _wall_clock(value: datetime) -> str:
    """A local date when the instant is that day's boundary, else date and wall-clock time."""
    if value.time() == time():
        return value.date().isoformat()
    return value.isoformat(sep=" ")


def _instant(value: datetime) -> str:
    return value.isoformat().replace("+00:00", " UTC")


def _scalar(value: JsonScalar | Decimal) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def _names(names: tuple[str, ...]) -> str:
    return ", ".join(names)


def _wrap(indent: str, text: str) -> str:
    return fill(
        text,
        width=_WIDTH,
        initial_indent=indent,
        subsequent_indent=indent + "  ",
        break_long_words=False,
        break_on_hyphens=False,
    )


class _Renderer:
    def __init__(self, design: Design, model: Model, required_sources: tuple[str, ...]) -> None:
        self.design = design
        self.model = model
        self.required_sources = required_sources
        assert design.unit is not None
        self.unit: str = design.unit
        self.zone = boundary_timezone(model.catalog.day_boundary)

    # Bound metadata lookups

    def _subject(self, measure: BoundMeasure) -> str:
        """``source.column`` for a value event, else the event's own name."""
        event = self.model.catalog.events[measure.event]
        return measure.event if event.column is None else f"{event.source}.{event.column}"

    def _filters(self, measure: BoundMeasure) -> str:
        clauses = []
        for item in measure.filters:
            if item.op == "between":
                low, high = item.values
                clauses.append(f"{item.property_name} between {_scalar(low)} and {_scalar(high)}")
            elif item.op in ("in", "not_in"):
                listed = ", ".join(_scalar(value) for value in item.values)
                clauses.append(f"{item.property_name} {item.op.replace('_', ' ')} ({listed})")
            else:
                value = _scalar(item.values[0])
                clauses.append(f"{item.property_name} {_OPERATORS[item.op]} {value}")
        return f" where {' and '.join(clauses)}" if clauses else ""

    def _measure(self, measure: BoundMeasure) -> str:
        aggregation = _AGGREGATIONS[measure.aggregation]
        return f"{aggregation} {self._subject(measure)}{self._filters(measure)}"

    def _metric(self, name: str) -> str:
        metric = self.model.metrics[name]
        if metric.kind == "ratio":
            numerator, denominator = metric.measures
            return f"ratio of {self._measure(numerator)} to {self._measure(denominator)}"
        (measure,) = metric.measures
        if metric.kind == "conversion":
            return f"whether any {self._subject(measure)}{self._filters(measure)} occurs"
        return self._measure(measure)

    def _values(self, name: str) -> str:
        """The event values a quantile or distinct count reads, without the metric's aggregate."""
        return " and ".join(
            f"{self._subject(measure)}{self._filters(measure)}"
            for measure in self.model.metrics[name].measures
        )

    def _property_source(self, attribute: str) -> str:
        declaration = self.model.catalog.properties.get(attribute)
        if declaration is not None:
            return declaration.source
        for source in self.model.sources.values():
            if attribute in source.properties or any(
                attribute in dimension.properties for dimension in source.dimensions
            ):
                return source.name
        raise AssertionError("validation resolves every property to a source")

    def _coverage(self, source: str) -> str:
        coverage = self.model.readiness.for_fact(source)
        first = coverage.start.astimezone(self.zone)
        end = coverage.end_exclusive.astimezone(self.zone)
        if first.time() == time() and end.time() == time():
            last = (end - timedelta(days=1)).date().isoformat()
            boundary = self.model.catalog.day_boundary
            return (
                f"{source} is declared complete for {first.date()} through {last} "
                f"({boundary} days)."
            )
        return (
            f"{source} is declared complete from {_instant(first)} until {_instant(end)} "
            "(exclusive)."
        )

    # Expressions

    def _relative(self, selector: EventTime | None, origin: str) -> str:
        if selector is None:
            return ""
        return f" relative to ({self._expression(selector, origin)})"

    def _expression(self, expr: Expr | Predicate, origin: str = "t0") -> str:
        if isinstance(expr, MetricAgg):
            own = _window(expr.window, "selected time" if expr.relative_to is not None else origin)
            return f"{self._metric(expr.metric)} in {own}{self._relative(expr.relative_to, origin)}"
        if isinstance(expr, MetricQuantile):
            values = self._values(expr.metric)
            return (
                f"{expr.probability} quantile ({expr.interpolation}) of {values} "
                f"in {_window(expr.window, origin)}"
            )
        if isinstance(expr, ApproxMetricQuantile):
            return (
                f"approximate {expr.probability} quantile ({expr.sketch.algorithm} "
                f"{expr.sketch.version}, requested rank error {expr.sketch.rank_error}) "
                f"of {self._values(expr.metric)} in {_window(expr.window, origin)}"
            )
        if isinstance(expr, ApproxMetricDistinct):
            values = self._values(expr.metric)
            return f"approximate distinct count of {values} in {_window(expr.window, origin)}"
        if isinstance(expr, RetentionMetric):
            horizon = (
                f"from {origin} until the end of complete coverage (exclusive)"
                if expr.finite_horizon_seconds is None
                else f"in {_window(Window(0, expr.finite_horizon_seconds), origin)}"
            )
            thresholds = ", ".join(str(value) for value in expr.count_thresholds)
            return (
                f"highest reached band (thresholds {thresholds}) of distinct {expr.event} "
                f"identities with {expr.label_property} = {_scalar(expr.target_event)} {horizon}"
            )
        if isinstance(expr, AttributeAsOf):
            at = _offset(expr.before, "selected time" if expr.relative_to is not None else origin)
            source = self._property_source(expr.attribute)
            relative = self._relative(expr.relative_to, origin)
            return f"{expr.attribute} as of just before {at} (history {source}){relative}"
        if isinstance(expr, EventTime):
            occurrence = _ordinal(expr.n) if expr.occurrence == "nth" else expr.occurrence
            label = (
                f" with {expr.label_property} = {_scalar(expr.equals)}"
                if expr.label_property is not None
                else ""
            )
            own = _window(expr.window, "selected time" if expr.relative_to is not None else origin)
            relative = self._relative(expr.relative_to, origin)
            return f"time of the {occurrence} {expr.event} event{label} in {own}{relative}"
        if isinstance(expr, Gt):
            return f"{self._expression(expr.expr, origin)} > {_scalar(expr.value)}"
        if isinstance(expr, Eq):
            return f"{self._expression(expr.left, origin)} == {_scalar(expr.value)}"
        if isinstance(expr, IsNull):
            state = "not null" if expr.negate else "null"
            return f"{self._expression(expr.expr, origin)} is {state}"
        assert isinstance(expr, And)
        return " and ".join(self._expression(term, origin) for term in expr.terms)

    # Sections

    def _who(self) -> str:
        roster = self.model.units.get(self.unit)
        if roster is not None:
            return f"{self.unit} (table {roster.table}, key {roster.key})"
        anchor = self.design.anchor
        assert isinstance(anchor, EventAnchor)
        source = self.model.sources[self.model.catalog.events[anchor.event].source]
        return f"{self.unit} (key {source.units[self.unit]})"

    def _at_anchor(self) -> str:
        anchor = self.design.anchor
        if isinstance(anchor, Schedule):
            if isinstance(self.design.shape, StatePanel):
                return "at each schedule period"
            return "at each snapshot"
        if isinstance(anchor, FixedDate):
            value = anchor.date
            instant = value if isinstance(value, datetime) else self.model.day_boundary(value)
            return f"at {_instant(instant)}"
        if anchor.which == "each":
            phrase = f"at each {anchor.event} event"
        else:
            which = _ordinal(anchor.n) if anchor.which == "nth" else "first"
            phrase = f"at its {which} {anchor.event} event"
        if anchor.offset:
            phrase += f", shifted by +{_duration(anchor.offset)}"
        return phrase

    def _rows(self) -> Iterator[Line]:
        design, shape = self.design, self.design.shape
        if isinstance(shape, LifeTable):
            groups = f" per {_names(shape.group_by)}" if shape.group_by else ""
            yield (
                "",
                f"One row per {_duration(shape.interval)} time bin{groups}, counting "
                f"{self._who()} entries {self._at_anchor()}.",
            )
        else:
            multiplier = ""
            if isinstance(shape, CohortSpec):
                multiplier = ", one per age period"
            elif isinstance(shape, EventPanelSpec):
                multiplier = ", one per relative period"
            elif isinstance(shape, CalendarComparisonSpec):
                multiplier = ", one per comparison period"
            yield "", f"One row per {self._who()} {self._at_anchor()}{multiplier}."
        anchor_range = design.anchor_range
        if anchor_range is not None:
            bounds = []
            start, end = anchor_range.start, anchor_range.end
            if start is not None:
                bounds.append(f"from {_instant(start) if isinstance(start, datetime) else start}")
            if end is not None:
                bounds.append(
                    f"before {_instant(end)}" if isinstance(end, datetime) else f"through {end}"
                )
            yield "", f"Anchors kept {' '.join(bounds)}."
        if design.gap:
            yield (
                "",
                f"Gap: feature windows and as-of cutoffs end at least "
                f"{_duration(design.gap)} before t0.",
            )
        if design.sample is not None:
            sample = design.sample
            yield (
                "",
                f"Sample: {_plural(sample.controls_per_treated, 'control')} per treated "
                f"{self.unit}, matched on {_names(sample.strata)}, seed {sample.seed}.",
            )
        if design.administrative_cutoff is not None:
            yield "", f"Administrative cutoff: {_instant(design.administrative_cutoff)}."

    def _snapshots(self) -> Iterator[Line]:
        schedule = self.design.anchor
        if not isinstance(schedule, Schedule):
            return
        boundary = self.model.day_boundary
        count = schedule.count(boundary)
        instants = [
            schedule.instant(k, boundary).astimezone(self.zone)
            for k in range(min(count, _LISTED_DATES))
        ]
        listed = ", ".join(_wall_clock(value) for value in instants)
        if count > _LISTED_DATES:
            last = _wall_clock(schedule.instant(count - 1, boundary).astimezone(self.zone))
            listed += f", and {count - _LISTED_DATES} more through {last}"
        clock = f", at {instants[0].strftime('%H:%M')} {self.model.catalog.day_boundary}"
        if schedule.months is not None:
            years, months = divmod(schedule.months, 12)
            every = _plural(years, "year") if months == 0 else _plural(schedule.months, "month")
        else:
            assert schedule.every is not None
            every = _duration(schedule.every, weeks=True)
            if schedule.every % 86_400:
                clock = ""
        yield (
            "",
            f"Snapshots: every {every} from {schedule.start} through {schedule.end} "
            f"({_plural(count, 'date')}: {listed}){clock}.",
        )

    def _named(self, title: str, nodes: Mapping[str, Expr | Predicate]) -> Iterator[Line]:
        if not nodes:
            return
        yield "", f"{title}:"
        for name, node in nodes.items():
            yield "  ", f"{name}: {self._expression(node)}"

    def _treatment(self) -> Iterator[Line]:
        treatment = self.design.treatment
        if treatment is None:
            return
        at = _offset(treatment.at, "t0")
        source = self._property_source(treatment.attribute)
        yield (
            "",
            f"Treatment: {treatment.attribute} as of {at} (a change effective exactly at {at} "
            f"counts), from {source}.",
        )

    def _initiation(self, treatment: Initiation | PersistentAdoption) -> str:
        since = _wall_clock(treatment.history_start.astimezone(self.zone))
        if isinstance(treatment, PersistentAdoption):
            return (
                f"{treatment.event} = {_scalar(treatment.adopted_value)} observed by t0 "
                f"(absorbing; history from {since})"
            )
        return f"{treatment.event} observed by t0 (history from {since})"

    def _panel(self) -> Iterator[Line]:
        shape = self.design.shape
        if isinstance(shape, SequentialSpec):
            parts = [
                f"Trials: each snapshot is a trial; treated = {self._initiation(shape.treatment)}",
                f"reference period {_offset(shape.reference_period_seconds, 't0')}",
            ]
            if shape.anticipation_seconds:
                parts.append(f"anticipation {_duration(shape.anticipation_seconds)}")
            if shape.trial_weight is not None:
                parts.append(f"{shape.trial_weight.kind.replace('_', ' ')} weights")
            yield "", "; ".join(parts) + "."
        elif isinstance(shape, EventPanelSpec):
            anchor = self.design.anchor
            assert isinstance(anchor, EventAnchor)
            periods = ", ".join(str(period) for period in shape.periods)
            yield (
                "",
                f"Panel: periods {periods} of {_duration(shape.period_seconds)} around each "
                f"{anchor.event} event, reference period {shape.reference_period}.",
            )
        elif isinstance(shape, CalendarComparisonSpec):
            anchor = self.design.anchor
            assert isinstance(anchor, Schedule) and anchor.every is not None
            parts = [
                f"Calendar comparison: cohorts by {self._initiation(shape.cohort)}",
                f"control {shape.control.replace('_', ' ')}",
                f"reference period {shape.reference_period}",
                f"comparison periods {', '.join(str(p) for p in shape.comparison_periods)}",
            ]
            if shape.anticipation_periods:
                listed = ", ".join(str(p) for p in shape.anticipation_periods)
                parts.append(f"anticipation periods {listed}")
            parts.append(f"each period {_duration(anchor.every)}")
            yield "", "; ".join(parts) + "."

    def _censoring(self, shape: TimeToEvent) -> str:
        reasons = []
        if shape.exit is not None:
            reasons.append(f"the {shape.exit} event")
        reasons.append("the end of complete coverage")
        if self.design.administrative_cutoff is not None:
            reasons.append("the administrative cutoff")
        censored = (
            reasons[0] if len(reasons) == 1 else f"{', '.join(reasons[:-1])} or {reasons[-1]}"
        )
        entry = ""
        if shape.entry is not None:
            delayed = " (delayed entry)" if shape.delayed_entry else ""
            entry = f"; entry at the {shape.entry} event{delayed}"
        return f"censored by {censored}{entry}"

    def _shape(self) -> Iterator[Line]:
        shape = self.design.shape
        if isinstance(shape, CohortSpec):
            trajectory = shape.trajectory
            kind = "cumulative" if trajectory.cumulative else "per-period"
            yield (
                "",
                f"Trajectory: {kind} {trajectory.metric} ({self._metric(trajectory.metric)}) "
                f"by age, {_plural(trajectory.periods, 'period')} "
                f"of {_duration(trajectory.period)} from the first {shape.origin} event.",
            )
        elif isinstance(shape, TimeToEvent):
            yield (
                "",
                f"Time to event: from t0 to the first {shape.event} event, "
                f"{self._censoring(shape)}.",
            )
        elif isinstance(shape, LifeTable):
            inner = shape.time_to_event
            groups = f", grouped by {_names(shape.group_by)}" if shape.group_by else ""
            yield (
                "",
                f"Life table: time from t0 to the first {inner.event} event, binned by "
                f"{_duration(shape.interval)}{groups}, {self._censoring(inner)}.",
            )
        elif isinstance(shape, StatePanel):
            schedule = self.design.anchor
            assert isinstance(schedule, Schedule) and schedule.every is not None
            state = shape.state
            if isinstance(state, BinnedMetricState):
                cutpoints = ", ".join(str(point) for point in state.cutpoints)
                origin = f"{state.metric} ({self._metric(state.metric)}) binned at {cutpoints}"
                qualifiers = ""
            else:
                origin = f"{state.property} (source {self._property_source(state.property)})"
                qualifiers = f"; absorbing {_names(state.absorbing)}" if state.absorbing else ""
                qualifiers += "" if state.ordered else "; levels unordered"
            yield (
                "",
                f"State panel: {' / '.join(state.levels)} from {origin}, observed every "
                f"{_duration(schedule.every, weeks=True)} for up to "
                f"{_plural(shape.max_periods, 'period')}"
                f"{qualifiers}.",
            )
        elif isinstance(shape, EventSequence):
            parts = [
                f"Event sequence: up to {_plural(shape.max_events, f'{shape.event} event')} in "
                f"{_window(shape.window, 't0')}"
            ]
            if shape.representation == "arrays":
                parts.append("as arrays")
            if shape.label_property is not None:
                parts.append(f"with label {shape.label_property}")
            if shape.properties:
                parts.append(f"properties {_names(shape.properties)}")
            if shape.sessions is not None:
                parts.append(f"sessions split by gaps over {_duration(shape.sessions.gap_seconds)}")
            if shape.funnel is not None:
                funnel = shape.funnel
                within = (
                    f" within {_duration(funnel.within_seconds)}"
                    if funnel.within_seconds is not None
                    else ""
                )
                parts.append(
                    f"funnel {' > '.join(funnel.steps)} by {funnel.event_property}{within}"
                )
            yield "", ", ".join(parts) + "."

    def _completeness(self) -> Iterator[Line]:
        if not self.required_sources:
            return
        first, *rest = self.required_sources
        yield "", f"Completeness: {self._coverage(first)}"
        for source in rest:
            yield "  ", self._coverage(source)
        yield (
            "  ",
            "A value whose window reaches outside those days is null with reason "
            "incomplete_coverage.",
        )
        yield (
            "  ",
            f"A {self.unit} with no events in a complete window gets 0 for counts and sums, and "
            "null for means, minima, maxima and quantiles.",
        )

    def _columns(self) -> Iterator[Line]:
        design, shape = self.design, self.design.shape
        if shape != "cross_section" and not isinstance(shape, _PANEL_SHAPES):
            label = next(label for kind, label in _SHAPE_LABELS if isinstance(shape, kind))
            yield "", f"Columns: the {label} columns (see roles after build)."
            return
        columns = ["unit", "anchor_id", "t0"]
        named = []
        if design.treatment is not None or isinstance(shape, SequentialSpec):
            named.append("treatment")
        elif isinstance(shape, CalendarComparisonSpec):
            columns.append("treatment")
        named.extend((*design.features, *design.outcomes))
        for name in named:
            columns.extend((name, f"{name}__complete", f"{name}__reason"))
        if shape == "cross_section":
            yield "", f"Columns: {', '.join(columns)}."
            return
        label = next(label for kind, label in _SHAPE_LABELS if isinstance(shape, kind))
        owned = f"the {label} columns (see roles after build)"
        yield (
            "",
            f"Columns: {', '.join(columns)}, and {owned}." if columns else f"Columns: {owned}.",
        )

    def render(self) -> str:
        shape = self.design.shape
        baseline = shape.baseline_features if isinstance(shape, StatePanel) else {}
        lines = (
            *self._rows(),
            *self._snapshots(),
            *self._named("Features (before t0)", self.design.features),
            *self._named("Baseline features (before the panel start)", baseline),
            *self._named("Outcomes (after t0)", self.design.outcomes),
            *self._named("Eligibility (rows kept when)", self.design.eligibility),
            *self._treatment(),
            *self._panel(),
            *self._shape(),
            *self._completeness(),
            *self._columns(),
        )
        return "\n".join(_wrap(indent, text) for indent, text in lines)


def describe_design(design: Design, model: Model) -> str:
    """Render what ``design`` will build on ``model`` after resolving and validating it.

    Every name, date and window in the text comes from the design and the bound
    metadata; nothing is compiled or executed.
    """
    resolved = resolve_unit(design, model)
    report = validate_design(resolved, model)
    return _Renderer(resolved, model, report.required_sources).render()
