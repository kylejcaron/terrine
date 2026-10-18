"""Canonical, safe JSON/YAML encoding for data-only study designs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

import yaml
from yaml.resolver import BaseResolver

from terrine._time import utc_instant
from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design, Treatment
from terrine.spec.expressions import And, AttributeAsOf, Eq, EventTime, Gt, IsNull, MetricAgg
from terrine.spec.sequence import EventSequence, FunnelSpec, SessionSpec
from terrine.spec.state_panel import (
    BinnedMetricState,
    StateObservations,
    StatePanel,
    decode_cutpoint,
    encode_cutpoint,
)
from terrine.windows import Window

__all__ = ["SCHEMA_VERSION", "canonical_json", "to_json", "from_json", "to_yaml", "from_yaml"]
SCHEMA_VERSION = 1


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
        return {
            "unit": value.unit,
            "anchor": _encode(value.anchor),
            "features": {key: _encode(item) for key, item in value.features.items()},
            "outcomes": {key: _encode(item) for key, item in value.outcomes.items()},
            "eligibility": {key: _encode(item) for key, item in value.eligibility.items()},
            "treatment": None if value.treatment is None else _encode(value.treatment),
            "gap": value.gap,
            "shape": _encode(value.shape),
            "anchor_range": None if value.anchor_range is None else _encode(value.anchor_range),
        }
    if isinstance(value, Window):
        return {"type": "Window", "start": value.start, "end": value.end}
    if isinstance(value, FixedDate):
        return {"type": "FixedDate", "date": _date_value(value.date)}
    if isinstance(value, EventAnchor):
        node = {"type": "EventAnchor", "fact": value.fact, "which": value.which}
        if value.n is not None:
            node["n"] = value.n
        return node
    if isinstance(value, Schedule):
        return {
            "type": "Schedule",
            "every": value.every,
            "start": _date_value(value.start),
            "end": _date_value(value.end),
        }
    if isinstance(value, AnchorRange):
        return {
            "type": "AnchorRange",
            "start": None if value.start is None else _date_value(value.start),
            "end": None if value.end is None else _date_value(value.end),
        }
    if isinstance(value, StateObservations):
        return {
            "type": "StateObservations",
            "property": value.property,
            "levels": list(value.levels),
            "ordered": value.ordered,
            "absorbing": list(value.absorbing),
        }
    if isinstance(value, BinnedMetricState):
        return {
            "type": "BinnedMetricState",
            "metric": value.metric,
            "cutpoints": [encode_cutpoint(v) for v in value.cutpoints],
            "levels": list(value.levels),
        }
    if isinstance(value, EventSequence):
        return {
            "type": "EventSequence",
            "fact": value.fact,
            "window": _encode(value.window),
            "max_events": value.max_events,
            "representation": value.representation,
            "properties": list(value.properties),
            "label_property": value.label_property,
            "sessions": None if value.sessions is None else _encode(value.sessions),
            "funnel": None if value.funnel is None else _encode(value.funnel),
        }
    if isinstance(value, SessionSpec):
        return {"type": "SessionSpec", "gap_seconds": value.gap_seconds}
    if isinstance(value, FunnelSpec):
        return {
            "type": "FunnelSpec",
            "event_property": value.event_property,
            "steps": list(value.steps),
            "within_seconds": value.within_seconds,
        }
    if isinstance(value, StatePanel):
        return {
            "type": "StatePanel",
            "state": _encode(value.state),
            "max_periods": value.max_periods,
            "baseline_features": {
                key: _encode(item) for key, item in value.baseline_features.items()
            },
        }
    if isinstance(value, MetricAgg):
        node = {"type": "MetricAgg", "metric": value.metric, "window": _encode(value.window)}
        if value.relative_to is not None:
            node["relative_to"] = _encode(value.relative_to)
        return node
    if isinstance(value, AttributeAsOf):
        node = {"type": "AttributeAsOf", "attribute": value.attribute, "before": value.before}
        if value.relative_to is not None:
            node["relative_to"] = _encode(value.relative_to)
        return node
    if isinstance(value, EventTime):
        node = {
            "type": "EventTime",
            "fact": value.fact,
            "window": _encode(value.window),
            "label_property": value.label_property,
            "equals": value.equals,
            "occurrence": value.occurrence,
            "n": value.n,
        }
        if value.relative_to is not None:
            node["relative_to"] = _encode(value.relative_to)
        return node
    if isinstance(value, Gt):
        return {"type": "Gt", "expr": _encode(value.expr), "value": value.value}
    if isinstance(value, Eq):
        return {"type": "Eq", "left": _encode(value.left), "value": value.value}
    if isinstance(value, IsNull):
        return {"type": "IsNull", "expr": _encode(value.expr), "negate": value.negate}
    if isinstance(value, And):
        return {"type": "And", "terms": [_encode(term) for term in value.terms]}
    if isinstance(value, Treatment):
        return {
            "type": "Treatment",
            "attribute": value.attribute,
            "at": value.at,
            "known_at_t0": value.known_at_t0,
        }
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


def _sort(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sort(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_sort(item) for item in value]
    return value


def canonical_json(design: Design) -> bytes:
    if not isinstance(design, Design):
        raise TypeError("expected a Design")
    payload = {"schema_version": SCHEMA_VERSION, "design": _encode(design)}
    return json.dumps(
        _sort(payload), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


def to_json(design: Design) -> str:
    return canonical_json(design).decode("utf-8")


def to_yaml(design: Design) -> str:
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


def _node(value: Any) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ValueError("expected a tagged specification node")
    tag = value["type"]
    if tag == "FixedDate":
        item = _object(value, {"type", "date"})
        bound = _read_date(item["date"])
        return FixedDate(bound)
    if tag == "EventSequence":
        item = _object(
            value,
            {
                "type",
                "fact",
                "window",
                "max_events",
                "representation",
                "properties",
                "label_property",
                "sessions",
                "funnel",
            },
        )
        return EventSequence(
            item["fact"],
            _node(item["window"]),
            max_events=item["max_events"],
            representation=item["representation"],
            properties=tuple(item["properties"]),
            label_property=item["label_property"],
            sessions=None if item["sessions"] is None else _node(item["sessions"]),
            funnel=None if item["funnel"] is None else _node(item["funnel"]),
        )
    if tag == "SessionSpec":
        item = _object(value, {"type", "gap_seconds"})
        return SessionSpec(item["gap_seconds"])
    if tag == "FunnelSpec":
        item = _object(value, {"type", "event_property", "steps", "within_seconds"})
        return FunnelSpec(item["event_property"], tuple(item["steps"]), item["within_seconds"])
    if tag == "EventAnchor":
        item = _object(value, {"type", "fact", "which", "n"}, {"type", "fact", "which"})
        return EventAnchor(item["fact"], item["which"], item.get("n"))
    if tag == "Schedule":
        item = _object(value, {"type", "every", "start", "end"})
        start, end = _read_date(item["start"]), _read_date(item["end"])
        if isinstance(start, datetime) or isinstance(end, datetime):
            raise ValueError("Schedule bounds require dates")
        return Schedule(item["every"], start, end)
    if tag == "AnchorRange":
        item = _object(value, {"type", "start", "end"})
        start = None if item["start"] is None else _read_date(item["start"])
        end = None if item["end"] is None else _read_date(item["end"])
        return AnchorRange(start, end)
    if tag == "StateObservations":
        item = _object(
            value,
            {"type", "property", "levels", "ordered", "absorbing"},
            {"type", "property", "levels"},
        )
        return StateObservations(
            item["property"],
            tuple(item["levels"]),
            item.get("ordered", True),
            tuple(item.get("absorbing", ())),
        )
    if tag == "BinnedMetricState":
        item = _object(value, {"type", "metric", "cutpoints", "levels"})
        return BinnedMetricState(
            item["metric"],
            tuple(decode_cutpoint(v) for v in item["cutpoints"]),
            tuple(item["levels"]),
        )
    if tag == "StatePanel":
        item = _object(value, {"type", "state", "max_periods", "baseline_features"})
        raw = item["baseline_features"]
        if not isinstance(raw, dict):
            raise ValueError("baseline_features must be a mapping")
        baseline = {key: _node(node) for key, node in raw.items()}
        return StatePanel(
            _node(item["state"]), max_periods=item["max_periods"], baseline_features=baseline
        )
    if tag == "Window":
        item = _object(value, {"type", "start", "end"})
        return Window(item["start"], item["end"])
    if tag == "MetricAgg":
        item = _object(
            value,
            {"type", "metric", "window", "relative_to"},
            {"type", "metric", "window"},
        )
        return MetricAgg(
            item["metric"],
            _node(item["window"]),
            None if item.get("relative_to") is None else _node(item["relative_to"]),
        )
    if tag == "AttributeAsOf":
        item = _object(
            value,
            {"type", "attribute", "before", "relative_to"},
            {"type", "attribute", "before"},
        )
        return AttributeAsOf(
            item["attribute"],
            item["before"],
            None if item.get("relative_to") is None else _node(item["relative_to"]),
        )
    if tag == "EventTime":
        item = _object(
            value,
            {
                "type",
                "fact",
                "window",
                "label_property",
                "equals",
                "occurrence",
                "n",
                "relative_to",
            },
            {"type", "fact", "window", "label_property", "equals", "occurrence", "n"},
        )
        return EventTime(
            item["fact"],
            _node(item["window"]),
            item["label_property"],
            item["equals"],
            item["occurrence"],
            item["n"],
            None if item.get("relative_to") is None else _node(item["relative_to"]),
        )
    if tag == "Gt":
        item = _object(value, {"type", "expr", "value"})
        return Gt(_node(item["expr"]), item["value"])
    if tag == "Eq":
        item = _object(value, {"type", "left", "value"})
        return Eq(_node(item["left"]), item["value"])
    if tag == "IsNull":
        item = _object(value, {"type", "expr", "negate"}, {"type", "expr"})
        return IsNull(_node(item["expr"]), item.get("negate", False))
    if tag == "And":
        item = _object(value, {"type", "terms"})
        if not isinstance(item["terms"], list):
            raise ValueError("And terms must be a list")
        return And(tuple(_node(term) for term in item["terms"]))
    if tag == "Treatment":
        item = _object(
            value, {"type", "attribute", "at", "known_at_t0"}, {"type", "attribute", "known_at_t0"}
        )
        return Treatment(item["attribute"], item.get("at", 0), item["known_at_t0"])
    raise ValueError(f"unknown specification node: {tag}")


def _design(value: Any) -> Design:
    item = _object(
        value,
        {
            "unit",
            "anchor",
            "features",
            "outcomes",
            "eligibility",
            "treatment",
            "gap",
            "shape",
            "anchor_range",
        },
        {"unit", "anchor"},
    )
    item = {
        "features": {},
        "outcomes": {},
        "eligibility": {},
        "treatment": None,
        "gap": 0,
        "shape": "cross_section",
        "anchor_range": None,
        **item,
    }

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
    if not isinstance(anchor, (FixedDate, EventAnchor, Schedule)):
        raise ValueError("design anchor has invalid node type")
    if treatment is not None and not isinstance(treatment, Treatment):
        raise ValueError("design treatment has invalid node type")
    if anchor_range is not None and not isinstance(anchor_range, AnchorRange):
        raise ValueError("anchor_range has invalid node type")
    unit = item["unit"]
    if not isinstance(unit, str):
        raise TypeError("unit must be a string")
    return Design(
        unit,
        anchor,
        features=named(item["features"], (MetricAgg, AttributeAsOf, EventTime)),
        outcomes=named(item["outcomes"], (MetricAgg, AttributeAsOf, EventTime)),
        eligibility=named(item["eligibility"], (Gt, Eq, IsNull, And)),
        treatment=treatment,
        gap=item["gap"],
        shape=item["shape"] if isinstance(item["shape"], str) else _node(item["shape"]),
        anchor_range=anchor_range,
    )


def _load(payload: Any) -> Design:
    root = _object(payload, {"schema_version", "design"})
    if type(root["schema_version"]) is not int or root["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported schema version")
    return _design(root["design"])


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate mapping key: {key!r}")
        result[key] = value
    return result


def from_json(value: str | bytes) -> Design:
    try:
        payload = json.loads(
            value,
            object_pairs_hook=_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("invalid JSON specification") from error
    return _load(payload)


def from_yaml(value: str | bytes) -> Design:
    try:
        payload = yaml.load(value, Loader=_UniqueSafeLoader)
    except yaml.YAMLError as error:
        raise ValueError("invalid YAML specification") from error
    return _load(payload)
