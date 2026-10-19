import json

import pytest
from pydantic import ValidationError

from terrine.agent.schemas import (
    ExecuteRequest,
    PrepareRequest,
    PreviewRequest,
    RowPreviewRequest,
    RunEventsRequest,
)


def test_operation_schemas_reject_executable_and_policy_fields():
    requests = [
        (PreviewRequest, {"serialized": "{}"}),
        (
            PrepareRequest,
            {
                "serialized": "{}",
                "invocation_id": "plan",
                "destination": {"database": "local", "schema": "published", "dataset": "training"},
            },
        ),
        (ExecuteRequest, {"receipt_id": "opaque", "request_key": "request"}),
        (RunEventsRequest, {"run_id": "run"}),
        (
            RowPreviewRequest,
            {
                "reference": {
                    "dataset": "training",
                    "generation": "generation",
                    "definition_hash": "hash",
                    "recipe_reference": "opaque",
                },
                "limit": 5,
                "columns": ["unit"],
            },
        ),
    ]
    for cls, record in requests:
        restored = cls.model_validate_json(json.dumps(record))
        assert restored.schema_version == 1
        assert cls.model_validate_json(restored.model_dump_json()) == restored
        assert cls.model_json_schema()["additionalProperties"] is False
        for key in ("sql", "callback", "executor", "policy", "snapshot_authority", "source"):
            with pytest.raises(ValidationError):
                cls.model_validate_json(json.dumps({**record, key: "untrusted"}))


def test_preview_requests_reject_unbounded_and_noninteger_counts():
    for value in (-1, True, 1.5, "10"):
        with pytest.raises(ValidationError):
            PreviewRequest.model_validate({"serialized": "{}", "bounded_count": value})
    for value in (0, -1, True):
        with pytest.raises(ValidationError):
            RunEventsRequest.model_validate({"run_id": "run", "limit": value})
