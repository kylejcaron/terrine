"""Canonical, safe JSON/YAML encoding for data-only study designs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import yaml
from yaml.resolver import BaseResolver

from terrine._time import utc_instant
from terrine.errors import RefusalSpec, TerrineError
from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design, Treatment
from terrine.spec.expressions import (
    And,
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    AttributeAsOf,
    Eq,
    EventTime,
    Gt,
    IsNull,
    MetricAgg,
    MetricQuantile,
    RetentionMetric,
)
from terrine.spec.panels import (
    CalendarComparisonSpec,
    EventPanelSpec,
    Initiation,
    MatchedT0Sample,
    PersistentAdoption,
    SequentialSpec,
    TrialWeight,
)
from terrine.spec.sequence import EventSequence, FunnelSpec, SessionSpec
from terrine.spec.state_panel import (
    BinnedMetricState,
    StateObservations,
    StatePanel,
    decode_cutpoint,
    encode_cutpoint,
)
from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent, Trajectory
from terrine.windows import Window

__all__ = [
    "SCHEMA_VERSION",
    "canonical_digest",
    "canonical_json",
    "to_json",
    "from_json",
    "to_yaml",
    "from_yaml",
]
SCHEMA_VERSION = 4

_INVALID = RefusalSpec("terrine.design.invalid", "Design document is invalid: {reason}.")
_INVALID_INPUT = RefusalSpec("terrine.design.invalid", "Design input {field} is invalid: {reason}.")

# Encoded default of every optional document field, by node tag. Encoders omit a field equal to
# its default and decoders supply it, so an optional field added later leaves existing documents
# and digests unchanged. A default never changes within a document version.
_DEFAULTS: Mapping[str, Mapping[str, Any]] = {
    "Design": {
        "features": {},
        "outcomes": {},
        "eligibility": {},
        "treatment": None,
        "gap": "0s",
        "shape": "cross_section",
        "anchor_range": None,
        "sample": None,
        "administrative_cutoff": None,
    },
    "EventAnchor": {"occurrence": "first", "n": None, "offset": "0s"},
    "AnchorRange": {"start": None, "end": None},
    "StateObservations": {"ordered": True, "absorbing": []},
    "EventSequence": {
        "representation": "positions",
        "properties": [],
        "label_property": None,
        "sessions": None,
        "funnel": None,
    },
    "FunnelSpec": {"within": None},
    "StatePanel": {"baseline_features": {}},
    "Initiation": {"knowledge": "recorded_at"},
    "PersistentAdoption": {"persistence": "absorbing", "knowledge": "recorded_at"},
    "TrialWeight": {"values": None},
    "SequentialSpec": {"trial_weight": None, "anticipation": "0s", "reference_period": "-1d"},
    "CalendarComparisonSpec": {"anticipation_periods": []},
    "Trajectory": {"cumulative": False},
    "TimeToEvent": {"exit_event": None, "entry_event": None, "delayed_entry": False},
    "LifeTable": {"group_by": []},
    "MetricQuantile": {"interpolation": "linear_type7"},
    "MetricAgg": {"relative_to": None},
    "AttributeAsOf": {"relative_to": None},
    "EventTime": {
        "label_property": None,
        "equals": None,
        "occurrence": "first",
        "n": None,
        "relative_to": None,
        "measure": "time",
    },
    "IsNull": {"negate": False},
    "Treatment": {"at": "0s", "known_at_t0": False},
}


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def canonical_digest(payload: Mapping[str, Any]) -> str:
    """Return the SHA-256 digest of a stored JSON object under the canonical JSON rules.

    Keys are sorted, separators compact, text UTF-8 and non-finite numbers refused, so a
    stored record is verified by hashing its stored payload rather than re-encoding the object
    decoded from it.
    """
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _drop_defaults(node: Mapping[str, Any], defaults: Mapping[str, Any]) -> dict[str, Any]:
    """Omit every field of an encoded record whose value equals its encoded default."""
    return {key: item for key, item in node.items() if key not in defaults or item != defaults[key]}


def _omit_defaults(name: str, node: dict[str, Any]) -> dict[str, Any]:
    return _drop_defaults(node, _DEFAULTS.get(name, {}))


def _fields(value: Any, name: str, required: set[str]) -> dict[str, Any]:
    defaults = _DEFAULTS[name]
    return {**defaults, **_object(value, required | set(defaults), required)}


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as error:
            raise ValueError("mapping keys must be scalar") from error
        if duplicate:
            raise ValueError(f"duplicate mapping key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueSafeLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _date_value(value: date | datetime) -> dict[str, str]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cannot serialize a naive datetime")
        return {"type": "datetime", "value": value.astimezone(UTC).isoformat()}
    return {"type": "date", "value": value.isoformat()}


def _encode(value: Any) -> Any:
    if isinstance(value, Design):
        return _omit_defaults(
            "Design",
            {
                "unit": value.unit,
                "anchor": _encode(value.anchor),
                "features": {key: _encode(item) for key, item in value.features.items()},
                "outcomes": {key: _encode(item) for key, item in value.outcomes.items()},
                "eligibility": {key: _encode(item) for key, item in value.eligibility.items()},
                "treatment": _encode(value.treatment),
                "gap": str(value.gap),
                "shape": _encode(value.shape),
                "anchor_range": _encode(value.anchor_range),
                "sample": _encode(value.sample),
                "administrative_cutoff": (
                    None
                    if value.administrative_cutoff is None
                    else _date_value(value.administrative_cutoff)
                ),
            },
        )
    if isinstance(value, Window):
        return {"type": "Window", "start": str(value.start), "end": str(value.end)}
    if isinstance(value, FixedDate):
        return {"type": "FixedDate", "date": _date_value(value.date)}
    if isinstance(value, EventAnchor):
        return _omit_defaults(
            "EventAnchor",
            {
                "type": "EventAnchor",
                "event": value.event,
                "occurrence": value.occurrence,
                "n": value.n,
                "offset": str(value.offset),
            },
        )
    if isinstance(value, Schedule):
        return {
            "type": "Schedule",
            "every": str(value.every),
            "start": _date_value(value.start),
            "end": _date_value(value.end),
        }
    if isinstance(value, AnchorRange):
        return _omit_defaults(
            "AnchorRange",
            {
                "type": "AnchorRange",
                "start": None if value.start is None else _date_value(value.start),
                "end": None if value.end is None else _date_value(value.end),
            },
        )
    if isinstance(value, StateObservations):
        return _omit_defaults(
            "StateObservations",
            {
                "type": "StateObservations",
                "property": value.property,
                "levels": list(value.levels),
                "ordered": value.ordered,
                "absorbing": list(value.absorbing),
            },
        )
    if isinstance(value, BinnedMetricState):
        return {
            "type": "BinnedMetricState",
            "metric": value.metric,
            "cutpoints": [encode_cutpoint(v) for v in value.cutpoints],
            "levels": list(value.levels),
        }
    if isinstance(value, EventSequence):
        return _omit_defaults(
            "EventSequence",
            {
                "type": "EventSequence",
                "event": value.event,
                "window": _encode(value.window),
                "max_events": value.max_events,
                "representation": value.representation,
                "properties": list(value.properties),
                "label_property": value.label_property,
                "sessions": _encode(value.sessions),
                "funnel": _encode(value.funnel),
            },
        )
    if isinstance(value, SessionSpec):
        return {"type": "SessionSpec", "gap": str(value.gap)}
    if isinstance(value, FunnelSpec):
        return _omit_defaults(
            "FunnelSpec",
            {
                "type": "FunnelSpec",
                "event_property": value.event_property,
                "steps": list(value.steps),
                "within": None if value.within is None else str(value.within),
            },
        )
    if isinstance(value, StatePanel):
        return _omit_defaults(
            "StatePanel",
            {
                "type": "StatePanel",
                "state": _encode(value.state),
                "periods": value.periods,
                "baseline_features": {
                    key: _encode(item) for key, item in value.baseline_features.items()
                },
            },
        )
    if isinstance(value, Initiation):
        return _omit_defaults(
            "Initiation",
            {
                "type": "Initiation",
                "event": value.event,
                "history_start": value.history_start.isoformat(),
                "knowledge": value.knowledge,
            },
        )
    if isinstance(value, PersistentAdoption):
        return _omit_defaults(
            "PersistentAdoption",
            {
                "type": "PersistentAdoption",
                "event": value.event,
                "adopted_value": value.adopted_value,
                "history_start": value.history_start.isoformat(),
                "persistence": value.persistence,
                "knowledge": value.knowledge,
            },
        )
    if isinstance(value, MatchedT0Sample):
        return {
            "type": "MatchedT0Sample",
            "strata": list(value.strata),
            "controls_per_treated": value.controls_per_treated,
            "seed": value.seed,
        }
    if isinstance(value, TrialWeight):
        return _omit_defaults(
            "TrialWeight",
            {
                "type": "TrialWeight",
                "kind": value.kind,
                "values": None if value.values is None else dict(value.values),
            },
        )
    if isinstance(value, SequentialSpec):
        return _omit_defaults(
            "SequentialSpec",
            {
                "type": "SequentialSpec",
                "treatment": _encode(value.treatment),
                "trial_weight": _encode(value.trial_weight),
                "anticipation": str(value.anticipation),
                "reference_period": str(value.reference_period),
            },
        )
    if isinstance(value, EventPanelSpec):
        return {
            "type": "EventPanelSpec",
            "periods": list(value.periods),
            "period": str(value.period),
            "reference_period": value.reference_period,
        }
    if isinstance(value, CalendarComparisonSpec):
        return _omit_defaults(
            "CalendarComparisonSpec",
            {
                "type": "CalendarComparisonSpec",
                "cohort": _encode(value.cohort),
                "control": value.control,
                "reference_period": value.reference_period,
                "comparison_periods": list(value.comparison_periods),
                "anticipation_periods": list(value.anticipation_periods),
            },
        )
    if isinstance(value, CohortSpec):
        return {
            "type": "CohortSpec",
            "origin": value.origin,
            "trajectory": _encode(value.trajectory),
        }
    if isinstance(value, TimeToEvent):
        return _omit_defaults(
            "TimeToEvent",
            {
                "type": "TimeToEvent",
                "end_event": value.end_event,
                "exit_event": value.exit_event,
                "entry_event": value.entry_event,
                "delayed_entry": value.delayed_entry,
            },
        )
    if isinstance(value, LifeTable):
        return _omit_defaults(
            "LifeTable",
            {
                "type": "LifeTable",
                "time_to_event": _encode(value.time_to_event),
                "bin": str(value.bin),
                "group_by": list(value.group_by),
            },
        )
    if isinstance(value, Trajectory):
        return _omit_defaults(
            "Trajectory",
            {
                "type": "Trajectory",
                "metric": value.metric,
                "period": str(value.period),
                "periods": value.periods,
                "cumulative": value.cumulative,
            },
        )
    if isinstance(value, MetricQuantile):
        return _omit_defaults(
            "MetricQuantile",
            {
                "type": "MetricQuantile",
                "metric": value.metric,
                "probability": str(value.probability),
                "window": _encode(value.window),
                "interpolation": value.interpolation,
            },
        )
    if isinstance(value, ApproxMetricQuantile):
        return {
            "type": "ApproxMetricQuantile",
            "metric": value.metric,
            "probability": str(value.probability),
            "window": _encode(value.window),
        }
    if isinstance(value, ApproxMetricDistinct):
        return {
            "type": "ApproxMetricDistinct",
            "metric": value.metric,
            "window": _encode(value.window),
        }
    if isinstance(value, RetentionMetric):
        return {
            "type": "RetentionMetric",
            "event": value.event,
            "label_property": value.label_property,
            "target_event": value.target_event,
            "count_thresholds": list(value.count_thresholds),
            "horizon": None if value.horizon is None else str(value.horizon),
        }
    if isinstance(value, MetricAgg):
        return _omit_defaults(
            "MetricAgg",
            {
                "type": "MetricAgg",
                "metric": value.metric,
                "window": _encode(value.window),
                "relative_to": _encode(value.relative_to),
            },
        )
    if isinstance(value, AttributeAsOf):
        return _omit_defaults(
            "AttributeAsOf",
            {
                "type": "AttributeAsOf",
                "attribute": value.attribute,
                "before": str(value.before),
                "relative_to": _encode(value.relative_to),
            },
        )
    if isinstance(value, EventTime):
        return _omit_defaults(
            "EventTime",
            {
                "type": "EventTime",
                "event": value.event,
                "window": _encode(value.window),
                "label_property": value.label_property,
                "equals": value.equals,
                "occurrence": value.occurrence,
                "n": value.n,
                "relative_to": _encode(value.relative_to),
                "measure": value.measure,
            },
        )
    if isinstance(value, Gt):
        return {"type": "Gt", "expr": _encode(value.expr), "value": value.value}
    if isinstance(value, Eq):
        return {"type": "Eq", "expr": _encode(value.expr), "value": value.value}
    if isinstance(value, IsNull):
        return _omit_defaults(
            "IsNull", {"type": "IsNull", "expr": _encode(value.expr), "negate": value.negate}
        )
    if isinstance(value, And):
        return {"type": "And", "terms": [_encode(term) for term in value.terms]}
    if isinstance(value, Treatment):
        return _omit_defaults(
            "Treatment",
            {
                "type": "Treatment",
                "attribute": value.attribute,
                "at": str(value.at),
                "known_at_t0": value.known_at_t0,
            },
        )
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        import math

        if math.isfinite(value):
            return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("mapping keys must be strings")
        return {key: _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    raise TypeError(f"unsupported specification value: {type(value).__name__}")


def _design_payload(design: Design) -> dict[str, Any]:
    if not isinstance(design, Design):
        raise _INVALID_INPUT.error(reason="wrong_type", field="design")
    return _encode(design)


def canonical_json(design: Design) -> bytes:
    return _canonical_bytes({"schema_version": SCHEMA_VERSION, "design": _design_payload(design)})


def to_json(design: Design) -> str:
    """Return a design's versioned JSON document.

    Args:
        design: Design describing the rows and expressions to compile.

    Returns:
        Versioned JSON document text describing the Design.

    Example: spec.to_json(design)
    """
    return canonical_json(design).decode("utf-8")


def to_yaml(design: Design) -> str:
    """Return a design's versioned YAML document.


    Args:
        design: Design describing the rows and expressions to compile.

    Returns:
        Versioned YAML document text describing this Design.

    Example: tr.to_yaml(design)
    """
    payload = json.loads(canonical_json(design))
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=True, default_flow_style=False)


def _object(value: Any, allowed: set[str], required: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
        raise ValueError("expected a mapping")
    required = allowed if required is None else required
    extra, missing = set(value) - allowed, required - set(value)
    if extra or missing:
        raise ValueError(f"invalid fields (extra={sorted(extra)}, missing={sorted(missing)})")
    return value


def _read_date(value: Any) -> date | datetime:
    item = _object(value, {"type", "value"})
    if not isinstance(item["value"], str):
        raise ValueError("date value must be text")
    if item["type"] == "date":
        parsed = date.fromisoformat(item["value"])
        if isinstance(parsed, datetime):
            raise ValueError("date tag requires a calendar date")
        return parsed
    if item["type"] == "datetime":
        return utc_instant(item["value"])
    raise ValueError("unknown temporal value type")


def _decimal_string(value: Any) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("decimal specification values must be serialized as strings")
    return Decimal(value)


def _duration_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("duration must be text")
    return value


def _node(value: Any) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ValueError("expected a tagged specification node")
    tag = value["type"]
    if tag == "FixedDate":
        item = _object(value, {"type", "date"})
        bound = _read_date(item["date"])
        return FixedDate(bound)
    if tag == "EventSequence":
        item = _fields(value, "EventSequence", {"type", "event", "window", "max_events"})
        return EventSequence(
            item["event"],
            _node(item["window"]),
            max_events=item["max_events"],
            representation=item["representation"],
            properties=tuple(item["properties"]),
            label_property=item["label_property"],
            sessions=None if item["sessions"] is None else _node(item["sessions"]),
            funnel=None if item["funnel"] is None else _node(item["funnel"]),
        )
    if tag == "SessionSpec":
        item = _object(value, {"type", "gap"})
        return SessionSpec(_duration_text(item["gap"]))
    if tag == "FunnelSpec":
        item = _fields(value, "FunnelSpec", {"type", "event_property", "steps"})
        return FunnelSpec(
            item["event_property"],
            tuple(item["steps"]),
            None if item["within"] is None else _duration_text(item["within"]),
        )
    if tag == "EventAnchor":
        item = _fields(value, "EventAnchor", {"type", "event"})
        return EventAnchor(
            item["event"], item["occurrence"], item["n"], _duration_text(item["offset"])
        )
    if tag == "Schedule":
        item = _object(value, {"type", "every", "start", "end"})
        start, end = _read_date(item["start"]), _read_date(item["end"])
        if isinstance(start, datetime) or isinstance(end, datetime):
            raise ValueError("Schedule bounds require dates")
        return Schedule(_duration_text(item["every"]), start, end)
    if tag == "AnchorRange":
        item = _fields(value, "AnchorRange", {"type"})
        start = None if item["start"] is None else _read_date(item["start"])
        end = None if item["end"] is None else _read_date(item["end"])
        return AnchorRange(start, end)
    if tag == "StateObservations":
        item = _fields(value, "StateObservations", {"type", "property", "levels"})
        return StateObservations(
            item["property"], tuple(item["levels"]), item["ordered"], tuple(item["absorbing"])
        )
    if tag == "BinnedMetricState":
        item = _object(value, {"type", "metric", "cutpoints", "levels"})
        return BinnedMetricState(
            item["metric"],
            tuple(decode_cutpoint(v) for v in item["cutpoints"]),
            tuple(item["levels"]),
        )
    if tag == "StatePanel":
        item = _fields(value, "StatePanel", {"type", "state", "periods"})
        raw = item["baseline_features"]
        if not isinstance(raw, dict):
            raise ValueError("baseline_features must be a mapping")
        baseline = {key: _node(node) for key, node in raw.items()}
        return StatePanel(_node(item["state"]), periods=item["periods"], baseline_features=baseline)
    if tag == "Initiation":
        item = _fields(value, "Initiation", {"type", "event", "history_start"})
        return Initiation(
            item["event"],
            history_start=datetime.fromisoformat(item["history_start"]),
            knowledge=item["knowledge"],
        )
    if tag == "PersistentAdoption":
        item = _fields(
            value, "PersistentAdoption", {"type", "event", "adopted_value", "history_start"}
        )
        return PersistentAdoption(
            item["event"],
            adopted_value=item["adopted_value"],
            history_start=datetime.fromisoformat(item["history_start"]),
            persistence=item["persistence"],
            knowledge=item["knowledge"],
        )
    if tag == "MatchedT0Sample":
        item = _object(value, {"type", "strata", "controls_per_treated", "seed"})
        return MatchedT0Sample(tuple(item["strata"]), item["controls_per_treated"], item["seed"])
    if tag == "TrialWeight":
        item = _fields(value, "TrialWeight", {"type", "kind"})
        return TrialWeight(item["kind"], item["values"])
    if tag == "SequentialSpec":
        item = _fields(value, "SequentialSpec", {"type", "treatment"})
        weight = None if item["trial_weight"] is None else _node(item["trial_weight"])
        return SequentialSpec(
            _node(item["treatment"]),
            weight,
            _duration_text(item["anticipation"]),
            _duration_text(item["reference_period"]),
        )
    if tag == "EventPanelSpec":
        item = _object(value, {"type", "periods", "period", "reference_period"})
        return EventPanelSpec(
            tuple(item["periods"]), _duration_text(item["period"]), item["reference_period"]
        )
    if tag == "CalendarComparisonSpec":
        item = _fields(
            value,
            "CalendarComparisonSpec",
            {"type", "cohort", "control", "reference_period", "comparison_periods"},
        )
        return CalendarComparisonSpec(
            _node(item["cohort"]),
            item["control"],
            item["reference_period"],
            tuple(item["comparison_periods"]),
            tuple(item["anticipation_periods"]),
        )
    if tag == "Trajectory":
        item = _fields(value, "Trajectory", {"type", "metric", "period", "periods"})
        return Trajectory(
            item["metric"], _duration_text(item["period"]), item["periods"], item["cumulative"]
        )
    if tag == "CohortSpec":
        item = _object(value, {"type", "origin", "trajectory"})
        return CohortSpec(item["origin"], _node(item["trajectory"]))
    if tag == "TimeToEvent":
        item = _fields(value, "TimeToEvent", {"type", "end_event"})
        return TimeToEvent(
            item["end_event"],
            exit_event=item["exit_event"],
            entry_event=item["entry_event"],
            delayed_entry=item["delayed_entry"],
        )
    if tag == "LifeTable":
        item = _fields(value, "LifeTable", {"type", "time_to_event", "bin"})
        groups = item["group_by"]
        if not isinstance(groups, list) or any(not isinstance(name, str) for name in groups):
            raise ValueError("LifeTable group_by must be a list of feature names")
        return LifeTable(_node(item["time_to_event"]), _duration_text(item["bin"]), tuple(groups))
    if tag == "Window":
        item = _object(value, {"type", "start", "end"})
        return Window(_duration_text(item["start"]), _duration_text(item["end"]))
    if tag == "MetricQuantile":
        item = _fields(value, "MetricQuantile", {"type", "metric", "probability", "window"})
        return MetricQuantile(
            item["metric"],
            _decimal_string(item["probability"]),
            _node(item["window"]),
            item["interpolation"],
        )
    if tag == "ApproxMetricQuantile":
        item = _object(value, {"type", "metric", "probability", "window"})
        return ApproxMetricQuantile(
            item["metric"],
            _decimal_string(item["probability"]),
            _node(item["window"]),
        )
    if tag == "ApproxMetricDistinct":
        item = _object(value, {"type", "metric", "window"})
        return ApproxMetricDistinct(item["metric"], _node(item["window"]))
    if tag == "RetentionMetric":
        item = _object(
            value,
            {
                "type",
                "event",
                "label_property",
                "target_event",
                "count_thresholds",
                "horizon",
            },
        )
        return RetentionMetric(
            item["event"],
            item["label_property"],
            item["target_event"],
            tuple(item["count_thresholds"]),
            None if item["horizon"] is None else _duration_text(item["horizon"]),
        )
    if tag == "MetricAgg":
        item = _fields(value, "MetricAgg", {"type", "metric", "window"})
        return MetricAgg(
            item["metric"],
            _node(item["window"]),
            None if item["relative_to"] is None else _node(item["relative_to"]),
        )
    if tag == "AttributeAsOf":
        item = _fields(value, "AttributeAsOf", {"type", "attribute", "before"})
        return AttributeAsOf(
            item["attribute"],
            _duration_text(item["before"]),
            None if item["relative_to"] is None else _node(item["relative_to"]),
        )
    if tag == "EventTime":
        item = _fields(value, "EventTime", {"type", "event", "window"})
        return EventTime(
            item["event"],
            _node(item["window"]),
            item["label_property"],
            item["equals"],
            item["occurrence"],
            item["n"],
            None if item["relative_to"] is None else _node(item["relative_to"]),
            measure=item["measure"],
        )
    if tag == "Gt":
        item = _object(value, {"type", "expr", "value"})
        return Gt(_node(item["expr"]), item["value"])
    if tag == "Eq":
        item = _object(value, {"type", "expr", "value"})
        return Eq(_node(item["expr"]), item["value"])
    if tag == "IsNull":
        item = _fields(value, "IsNull", {"type", "expr"})
        return IsNull(_node(item["expr"]), item["negate"])
    if tag == "And":
        item = _object(value, {"type", "terms"})
        if not isinstance(item["terms"], list):
            raise ValueError("And terms must be a list")
        return And(tuple(_node(term) for term in item["terms"]))
    if tag == "Treatment":
        item = _fields(value, "Treatment", {"type", "attribute"})
        return Treatment(item["attribute"], _duration_text(item["at"]), item["known_at_t0"])
    raise ValueError(f"unknown specification node: {tag}")


def _design(value: Any) -> Design:
    item = _fields(value, "Design", {"unit", "anchor"})

    def named(raw: Any, allowed: tuple[type, ...]) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("named design values must be mappings")
        parsed = {key: _node(node) for key, node in raw.items()}
        if any(not isinstance(node, allowed) for node in parsed.values()):
            raise ValueError("node type is not valid for this design field")
        return parsed

    anchor = _node(item["anchor"])
    treatment = None if item["treatment"] is None else _node(item["treatment"])
    anchor_range = None if item["anchor_range"] is None else _node(item["anchor_range"])
    sample = None if item["sample"] is None else _node(item["sample"])
    cutoff = (
        None if item["administrative_cutoff"] is None else _read_date(item["administrative_cutoff"])
    )
    if cutoff is not None and not isinstance(cutoff, datetime):
        raise ValueError("administrative_cutoff requires a datetime")
    if not isinstance(anchor, (FixedDate, EventAnchor, Schedule)):
        raise ValueError("design anchor has invalid node type")
    if treatment is not None and not isinstance(treatment, Treatment):
        raise ValueError("design treatment has invalid node type")
    if anchor_range is not None and not isinstance(anchor_range, AnchorRange):
        raise ValueError("anchor_range has invalid node type")
    unit = item["unit"]
    if unit is not None and not isinstance(unit, str):
        raise TypeError("unit must be a string or null")
    return Design(
        unit,
        anchor,
        features=named(
            item["features"],
            (
                MetricAgg,
                MetricQuantile,
                ApproxMetricQuantile,
                ApproxMetricDistinct,
                RetentionMetric,
                AttributeAsOf,
                EventTime,
                Gt,
                Eq,
                IsNull,
                And,
            ),
        ),
        outcomes=named(
            item["outcomes"],
            (
                MetricAgg,
                MetricQuantile,
                ApproxMetricQuantile,
                ApproxMetricDistinct,
                RetentionMetric,
                AttributeAsOf,
                EventTime,
                Gt,
                Eq,
                IsNull,
                And,
            ),
        ),
        eligibility=named(item["eligibility"], (Gt, Eq, IsNull, And)),
        treatment=treatment,
        gap=_duration_text(item["gap"]),
        shape=item["shape"] if isinstance(item["shape"], str) else _node(item["shape"]),
        anchor_range=anchor_range,
        sample=sample,
        administrative_cutoff=cutoff,
    )


def _load(payload: Any) -> Design:
    """Decode a parsed document; anything that is not a valid design refuses with a code.

    A constructor's own refusal is kept; a structural error (a missing or extra field, an
    unknown node, a value of the wrong JSON type) is ``malformed_document``.
    """
    try:
        root = _object(payload, {"schema_version", "design"})
        if type(root["schema_version"]) is not int or root["schema_version"] != SCHEMA_VERSION:
            raise _INVALID.error(
                reason="unsupported_schema_version",
                field="schema_version",
                expected=SCHEMA_VERSION,
                schema_version=root["schema_version"],
            )
        return _design(root["design"])
    except TerrineError:
        raise
    except (TypeError, ValueError, ArithmeticError) as error:
        raise _INVALID.error(reason="malformed_document") from error


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate mapping key: {key!r}")
        result[key] = value
    return result


def _reject_constant(item: str) -> Any:
    raise ValueError(item)


def _parse_json(value: str | bytes) -> Any:
    """Parse a JSON design document strictly, before any digest check or decoding."""
    try:
        return json.loads(value, object_pairs_hook=_pairs, parse_constant=_reject_constant)
    except (TypeError, ValueError) as error:
        raise _INVALID.error(reason="malformed_document") from error


def from_json(value: str | bytes) -> Design:
    """Read a design from a supported versioned JSON document.

    Args:
        value: Versioned JSON document text or UTF-8 bytes, not a filesystem path.

    Returns:
        A validated reconstructed Design.

    Example: spec.from_json(spec.to_json(design))
    """
    return _load(_parse_json(value))


def from_yaml(value: str | bytes) -> Design:
    """Read a design from a supported versioned YAML document.


    Args:
        value: YAML document text or UTF-8 bytes returned by to_yaml(), not a filesystem path.

    Returns:
        The reconstructed Design after document validation.

    Example: tr.from_yaml(tr.to_yaml(design))
    """
    try:
        payload = yaml.load(value, Loader=_UniqueSafeLoader)
    except (yaml.YAMLError, ValueError) as error:
        raise _INVALID.error(reason="malformed_document") from error
    return _load(payload)
