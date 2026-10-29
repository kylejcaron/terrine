"""Discovery schema for the existing versioned Design codec."""

from typing import Any

from terrine.spec.io import SCHEMA_VERSION


def design_schema() -> dict[str, Any]:
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
    boolean = {"type": "boolean"}
    scalar = {"type": ["string", "number", "boolean", "null"]}
    duration = {"type": ["integer", "string"]}
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
    expression = union("MetricAgg", "AttributeAsOf", "EventTime")
    predicate = union("Gt", "Eq", "IsNull", "And")
    named = mapping(expression)
    outputs = mapping({"anyOf": [expression, predicate]})
    nodes = {
        "Window": {"start": number, "end": number},
        "FixedDate": {"date": ref("Temporal")},
        "EventAnchor": {
            "event": text,
            "which": {"enum": ["first", "nth", "each"]},
            "n": nullable(integer),
            "offset": duration,
        },
        "Schedule": {"every": duration, "start": ref("Temporal"), "end": ref("Temporal")},
        "AnchorRange": {"start": nullable(ref("Temporal")), "end": nullable(ref("Temporal"))},
        "MetricAgg": {
            "metric": text,
            "window": ref("Window"),
            "relative_to": nullable(ref("EventTime")),
        },
        "AttributeAsOf": {
            "attribute": text,
            "before": number,
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
        },
        "Gt": {"expr": expression, "value": scalar},
        "Eq": {"left": expression, "value": scalar},
        "IsNull": {"expr": expression, "negate": boolean},
        "And": {"terms": array(predicate)},
        "Treatment": {"attribute": text, "at": number, "known_at_t0": boolean},
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
            "max_periods": integer,
            "baseline_features": named,
        },
        "Trajectory": {
            "metric": text,
            "period": duration,
            "periods": integer,
            "cumulative": boolean,
        },
        "CohortSpec": {"origin": text, "trajectory": ref("Trajectory")},
        "TimeToEvent": {
            "event": text,
            "exit": nullable(text),
            "entry": nullable(text),
            "delayed_entry": boolean,
        },
        "LifeTable": {
            "time_to_event": ref("TimeToEvent"),
            "interval": duration,
            "group_by": array(text),
        },
        "SessionSpec": {"gap_seconds": number},
        "FunnelSpec": {
            "event_property": text,
            "steps": array(text),
            "within_seconds": nullable(duration),
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
            "anticipation_seconds": number,
            "reference_period_seconds": number,
        },
        "EventPanelSpec": {
            "periods": array(integer),
            "period_seconds": number,
            "reference_period": integer,
        },
        "CalendarComparisonSpec": {
            "cohort": ref("Initiation"),
            "control": text,
            "reference_period": integer,
            "comparison_periods": array(integer),
            "anticipation_periods": array(integer),
        },
    }
    optional = {
        "EventAnchor": {"n", "offset"},
        "MetricAgg": {"relative_to"},
        "AttributeAsOf": {"relative_to"},
        "EventTime": {"relative_to"},
        "IsNull": {"negate"},
        "Treatment": {"at"},
        "StateObservations": {"ordered", "absorbing"},
    }
    for name, fields in nodes.items():
        properties = {"type": {"const": name}, **fields}
        definitions[name] = obj(
            properties, [key for key in properties if key not in optional.get(name, set())]
        )
    definitions["Design"] = obj(
        {
            "unit": text,
            "anchor": union("FixedDate", "EventAnchor", "Schedule"),
            "features": outputs,
            "outcomes": outputs,
            "eligibility": mapping(predicate),
            "treatment": nullable(ref("Treatment")),
            "gap": number,
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
        ["unit", "anchor"],
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": definitions,
        **obj({"schema_version": {"const": SCHEMA_VERSION}, "design": ref("Design")}),
    }
