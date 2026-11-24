"""Structural field and expression facts for the versioned Design codec."""

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from terrine.spec.expressions import (
    _PREDICATE_NODES,
    ApproxMetricDistinct,
    ApproxMetricQuantile,
    AttributeAsOf,
    Eq,
    EventTime,
    Gt,
    Gte,
    Lt,
    Lte,
    MetricAgg,
    MetricQuantile,
    Ne,
    RetentionMetric,
)

JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

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
# Single-literal comparisons share one node shape, {"type", "expr", "value"}.
_COMPARISONS: Mapping[str, type[Eq | Ne | Gt | Gte | Lt | Lte]] = {
    "Eq": Eq,
    "Ne": Ne,
    "Gt": Gt,
    "Gte": Gte,
    "Lt": Lt,
    "Lte": Lte,
}
_COMPARISON_TAGS = {kind: tag for tag, kind in _COMPARISONS.items()}


_EXPRESSION_TAGS = {
    "MetricAgg": MetricAgg,
    "MetricQuantile": MetricQuantile,
    "ApproxMetricQuantile": ApproxMetricQuantile,
    "ApproxMetricDistinct": ApproxMetricDistinct,
    "RetentionMetric": RetentionMetric,
    "AttributeAsOf": AttributeAsOf,
    "EventTime": EventTime,
}
OUTPUT_NODES = (*_EXPRESSION_TAGS.values(), *_PREDICATE_NODES)


def _design_definitions() -> dict[str, Any]:
    """Build the record grammar shared by decoding and schema discovery."""

    def ref(name):
        return {"$ref": f"#/$defs/{name}"}

    def union(*names):
        return {"anyOf": [ref(name) for name in names]}

    def nullable(schema):
        return {"anyOf": [schema, {"type": "null"}]}

    def array(schema):
        return {"type": "array", "items": schema}

    def mapping(schema):
        return {"type": "object", "additionalProperties": schema}

    def obj(properties, required=None):
        return {
            "type": "object",
            "properties": properties,
            "required": list(properties) if required is None else required,
            "additionalProperties": False,
        }

    text = {"type": "string"}
    number = {"type": "number"}
    integer = {"type": "integer"}
    period_index = {"type": "integer", "minimum": -(2**63), "maximum": 2**63 - 1}
    boolean = {"type": "boolean"}
    scalar = {"type": ["string", "number", "boolean", "null"]}
    duration = {
        "type": "string",
        "pattern": "^[+-]?(?:[0-9]+(min|[shdw])|[0-9]+h[0-9]+min)$",
        "description": "Elapsed duration: a signed integer with an s, min, h, d or w unit.",
    }
    definitions = {"Temporal": obj({"type": {"enum": ["date", "datetime"]}, "value": text})}
    definitions["Cutpoint"] = {
        "anyOf": [
            obj({"type": {"enum": ["int", "float64"]}, "value": text}),
            obj(
                {
                    "type": {"const": "decimal"},
                    "sign": {"enum": [0, 1]},
                    "digits": array({"type": "integer", "minimum": 0, "maximum": 9}),
                    "exponent": integer,
                }
            ),
        ]
    }
    expression = union(*_EXPRESSION_TAGS)
    predicate = union(*(kind.__name__ for kind in _PREDICATE_NODES))
    outputs = mapping({"anyOf": [expression, predicate]})
    nodes = {
        "Window": {"start": duration, "end": duration},
        "FixedDate": {"date": ref("Temporal")},
        "EventAnchor": {
            "event": text,
            "occurrence": {"enum": ["first", "nth", "each"]},
            "n": nullable(integer),
            "offset": duration,
        },
        "Schedule": {
            "every": {
                "type": "string",
                "pattern": "^(\\+?(?:[0-9]+(?:min|[shdw])|[0-9]+h[0-9]+min)|[0-9]+(mo|y))$",
                "description": (
                    "Positive step between anchors: an elapsed duration such as 1w, or "
                    "calendar months such as 1mo or 1y, each clamped to its month's last day."
                ),
            },
            "start": ref("Temporal"),
            "end": {
                **ref("Temporal"),
                "description": "Last scheduled calendar day, included; start == end is one anchor.",
            },
        },
        "AnchorRange": {
            "start": nullable(ref("Temporal")),
            "end": {
                **nullable(ref("Temporal")),
                "description": (
                    "A date keeps anchors through that whole local day; "
                    "a datetime is an exact exclusive instant."
                ),
            },
        },
        "MetricAgg": {
            "metric": text,
            "window": ref("Window"),
            "relative_to": nullable(ref("EventTime")),
        },
        "MetricQuantile": {
            "metric": text,
            "probability": text,
            "window": ref("Window"),
            "interpolation": {"const": _DEFAULTS["MetricQuantile"]["interpolation"]},
        },
        "ApproxMetricQuantile": {
            "metric": text,
            "probability": text,
            "window": ref("Window"),
        },
        "ApproxMetricDistinct": {"metric": text, "window": ref("Window")},
        "RetentionMetric": {
            "event": text,
            "label_property": text,
            "target_event": text,
            "count_thresholds": {**array(integer), "minItems": 1},
            "horizon": nullable(duration),
        },
        "AttributeAsOf": {
            "attribute": text,
            "before": duration,
            "relative_to": nullable(ref("EventTime")),
        },
        "EventTime": {
            "event": text,
            "window": ref("Window"),
            "label_property": nullable(text),
            "equals": scalar,
            "occurrence": {"enum": ["first", "last", "nth"]},
            "n": nullable(integer),
            "relative_to": nullable(ref("EventTime")),
            "measure": {"enum": ["time", "elapsed"]},
        },
        **{tag: {"expr": expression, "value": scalar} for tag in _COMPARISONS},
        "IsIn": {"expr": expression, "values": {**array(scalar), "minItems": 1}},
        "Between": {"expr": expression, "low": scalar, "high": scalar},
        "IsNull": {"expr": expression, "negate": boolean},
        "And": {"terms": {**array(predicate), "minItems": 1}},
        "Or": {"terms": {**array(predicate), "minItems": 1}},
        "Not": {"term": predicate},
        "Treatment": {"attribute": text, "at": duration, "known_at_t0": boolean},
        "StateObservations": {
            "property": text,
            "levels": array(scalar),
            "ordered": boolean,
            "absorbing": array(scalar),
        },
        "BinnedMetricState": {
            "metric": text,
            "cutpoints": array(ref("Cutpoint")),
            "levels": array(scalar),
        },
        "StatePanel": {
            "state": union("StateObservations", "BinnedMetricState"),
            "periods": integer,
            "baseline_features": mapping(union("MetricAgg", "AttributeAsOf")),
        },
        "Trajectory": {
            "metric": text,
            "period": duration,
            "periods": integer,
            "cumulative": boolean,
        },
        "CohortSpec": {"origin": text, "trajectory": ref("Trajectory")},
        "TimeToEvent": {
            "end_event": text,
            "exit_event": nullable(text),
            "entry_event": nullable(text),
            "delayed_entry": boolean,
        },
        "LifeTable": {
            "time_to_event": ref("TimeToEvent"),
            "bin": duration,
            "group_by": array(text),
        },
        "SessionSpec": {"gap": duration},
        "FunnelSpec": {
            "event_property": text,
            "steps": array(text),
            "within": nullable(duration),
        },
        "EventSequence": {
            "event": text,
            "window": ref("Window"),
            "max_events": integer,
            "representation": text,
            "properties": array(text),
            "label_property": nullable(text),
            "sessions": nullable(ref("SessionSpec")),
            "funnel": nullable(ref("FunnelSpec")),
        },
        "Initiation": {"event": text, "history_start": text, "knowledge": text},
        "PersistentAdoption": {
            "event": text,
            "adopted_value": scalar,
            "history_start": text,
            "persistence": text,
            "knowledge": text,
        },
        "MatchedT0Sample": {
            "strata": array(text),
            "controls_per_treated": integer,
            "seed": integer,
        },
        "TrialWeight": {"kind": text, "values": nullable(mapping(number))},
        "SequentialSpec": {
            "treatment": union("Initiation", "PersistentAdoption"),
            "trial_weight": nullable(ref("TrialWeight")),
            "anticipation": duration,
            "reference_period": duration,
        },
        "EventPanelSpec": {
            "periods": array(period_index),
            "period": duration,
            "reference_period": period_index,
        },
        "CalendarComparisonSpec": {
            "cohort": ref("Initiation"),
            "control": text,
            "reference_period": integer,
            "comparison_periods": array(integer),
            "anticipation_periods": array(integer),
        },
    }

    def record(name, properties):
        defaults = _DEFAULTS.get(name, {})
        return obj(
            {
                key: {**field, "default": defaults[key]} if key in defaults else field
                for key, field in properties.items()
            },
            [key for key in properties if key not in defaults],
        )

    for name, fields in nodes.items():
        definitions[name] = record(name, {"type": {"const": name}, **fields})
    definitions["Design"] = record(
        "Design",
        {
            "unit": nullable(text),
            "anchor": union("FixedDate", "EventAnchor", "Schedule"),
            "features": outputs,
            "outcomes": outputs,
            "eligibility": mapping(predicate),
            "treatment": nullable(ref("Treatment")),
            "gap": duration,
            "shape": {
                "anyOf": [
                    text,
                    union(
                        "StatePanel",
                        "CohortSpec",
                        "TimeToEvent",
                        "LifeTable",
                        "EventSequence",
                        "SequentialSpec",
                        "EventPanelSpec",
                        "CalendarComparisonSpec",
                    ),
                ]
            },
            "anchor_range": nullable(ref("AnchorRange")),
            "sample": nullable(ref("MatchedT0Sample")),
            "administrative_cutoff": nullable(ref("Temporal")),
        },
    )
    return definitions


_DEFINITIONS = _design_definitions()
RECORD_FIELDS = {
    tag: (frozenset(record["properties"]), frozenset(record["required"]))
    for tag, record in _DEFINITIONS.items()
    if "properties" in record
}
TAGGED_RECORDS = RECORD_FIELDS.keys() - {"Design", "Temporal"}


def design_definitions() -> dict[str, Any]:
    """Return independent Draft 2020-12 definitions for the canonical base Design."""
    return deepcopy(_DEFINITIONS)
