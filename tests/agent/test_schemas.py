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
