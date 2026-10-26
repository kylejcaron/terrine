import json
from typing import Any, cast

import pytest
from pydantic import ValidationError

from terrine.agent.schemas import DesignRequest, Response, ValidationOutput, refusal_response
from terrine.errors import TerrineError, thaw


def test_tool_schemas_are_versioned_strict_and_round_trip():
    request = DesignRequest(serialized='{"schema_version":1}')
    assert request.schema_version == 1
    assert DesignRequest.model_validate_json(request.model_dump_json()) == request
    schema = DesignRequest.model_json_schema()
    assert schema["additionalProperties"] is False
    assert "serialized" in schema["required"]
    for extra in ({"schema_version": 2}, {"sql": "SELECT *"}, {"permissions": ["write"]}):
        with pytest.raises(ValidationError):
            DesignRequest.model_validate({"serialized": "{}", **extra})
    with pytest.raises(ValidationError):
        DesignRequest.model_validate({"serialized": 3})
    response = Response[ValidationOutput](
        status="ok",
        context={"details": [{"count": 2}]},
        output=ValidationOutput(
            spec_hash="hash", required_sources=("events",), runtime_obligations=()
        ),
    )
    with pytest.raises(TypeError):
        cast(Any, response.context)["details"][0]["count"] = 3
    assert json.loads(response.model_dump_json())["context"] == thaw(response.context)
    assert Response[ValidationOutput].model_validate_json(response.model_dump_json()) == response
    with pytest.raises((TypeError, ValueError)):
        Response.model_validate({"status": "ok", "context": {"bad": object()}})
    with pytest.raises((TypeError, ValueError)):
        Response(status="ok", context={"bad": float("nan")})


def test_default_response_context_is_immutable():
    response = Response(status="ok")
    with pytest.raises(TypeError):
        cast(Any, response.context)["secret"] = object()


def test_refusal_does_not_serialize_exception_messages_or_unknown_context():
    response = refusal_response(
        TerrineError(
            "password=secret SELECT customer",
            code="terrine.design.missing_reference",
            context={"kind": "metric", "name": "total", "password": "secret"},
        )
    )
    assert response.status == "refused"
    assert response.code == "terrine.design.missing_reference"
    assert response.suggested_route
    assert "secret" not in response.model_dump_json()
    assert response.context == {"kind": "metric", "name": "total"}


def test_discovery_schema_accepts_canonical_temporal_nodes():
    from jsonschema import Draft202012Validator

    from terrine.agent.design_schema import design_schema
    from terrine.spec import Design, EventAnchor, FixedDate, Schedule
    from terrine.spec.io import to_json
    from terrine.spec.trajectory import CohortSpec, LifeTable, TimeToEvent, Trajectory

    validator = Draft202012Validator(design_schema())
    for anchor, shape in (
        (Schedule("1d", "2025-01-01", "2025-01-05"), "cross_section"),
        (EventAnchor("start"), CohortSpec("start", Trajectory("total", "1d", 2))),
        (FixedDate("2025-01-01"), LifeTable(TimeToEvent("end"), "1d")),
    ):
        design = Design("user", anchor, shape=shape)
        validator.validate(json.loads(to_json(design)))


@pytest.mark.parametrize("variant", ["binned", "calendar", "funnel", "event_time"])
def test_discovery_schema_matches_existing_tagged_codec(variant):
    from datetime import UTC, datetime
    from decimal import Decimal

    from jsonschema import Draft202012Validator

    from terrine.agent.design_schema import design_schema
    from terrine.spec import Design, FixedDate, Schedule
    from terrine.spec.expressions import EventTime
    from terrine.spec.io import from_json, to_json
    from terrine.spec.panels import CalendarComparisonSpec, Initiation
    from terrine.spec.sequence import EventSequence, FunnelSpec
    from terrine.spec.state_panel import BinnedMetricState, StatePanel
    from terrine.windows import Window

    if variant == "binned":
        design = Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-05"),
            shape=StatePanel(
                BinnedMetricState("total", (1, Decimal("1.25"), 2.5), ("a", "b", "c", "d")),
                max_periods=4,
            ),
        )
    elif variant == "calendar":
        design = Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-05"),
            shape=CalendarComparisonSpec(
                Initiation("events", "active", datetime(2024, 1, 1, tzinfo=UTC)),
                "never_treated",
                -2,
                (0, 1),
                (-1,),
            ),
        )
    elif variant == "funnel":
        design = Design(
            "user",
            FixedDate("2025-01-01"),
            shape=EventSequence(
                "value", Window(-10, 0), max_events=5, funnel=FunnelSpec("action", ("a", "b"))
            ),
        )
    else:
        design = Design(
            "user",
            FixedDate("2025-01-01"),
            features={"when": EventTime("value", Window(-10, 0), occurrence="last")},
        )
    payload = json.loads(to_json(design))
    validator = Draft202012Validator(design_schema())
    validator.validate(payload)
    assert from_json(json.dumps(payload)).spec_hash == design.spec_hash
    if variant == "event_time":
        del payload["design"]["features"]["when"]["n"]
        assert not validator.is_valid(payload)
