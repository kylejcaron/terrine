"""Strict, data-only version-one dataset tool envelopes."""

from collections.abc import Mapping
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from terrine.errors import TerrineError, freeze, thaw

__all__ = [
    "DesignRequest",
    "Response",
    "ValidationOutput",
    "DesignOutput",
    "PlanReceipt",
    "AgentDatasetReference",
]


class Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[1] = 1


class DesignRequest(Record):
    serialized: str
    base: str | None = None


class NamedDesignRequest(DesignRequest):
    name: str


class PlanReceipt(Record):
    receipt_id: str
    design_hash: str
    recipe_hash: str


class AgentDatasetReference(Record):
    dataset: str
    generation: str
    definition_hash: str
    recipe_reference: str
    columns: tuple[str, ...] = ()


class ValidationOutput(Record):
    spec_hash: str
    required_sources: tuple[str, ...]
    runtime_obligations: tuple[str, ...]


class DesignOutput(ValidationOutput):
    serialized: str
    changed_paths: tuple[str, ...] = ()


class Response[T](Record):
    status: Literal["ok", "refused", "pending"]
    code: str | None = None
    context: Mapping[str, JsonValue] = {}
    suggested_route: str | None = None
    output_reference: PlanReceipt | AgentDatasetReference | None = None
    output: T | None = None

    @field_validator("context", mode="after")
    @classmethod
    def _freeze_context(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze(value)

    @field_serializer("context")
    def _serialize_context(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw(value)

    @model_validator(mode="after")
    def _status_contract(self) -> Self:
        if (self.status == "refused") != (self.code is not None):
            raise ValueError("only refused responses carry refusal codes")
        if self.status == "refused" and (
            self.output is not None or self.output_reference is not None
        ):
            raise ValueError("refusals cannot carry successful outputs")
        return self


_CONTEXT_FIELDS = frozenset({"operation", "kind", "name", "role", "gap", "shape", "limit", "route"})
_ROUTES = {
    "terrine.design.missing_reference": "Choose a name from inspect_catalog.",
    "terrine.design.invalid_window": "Choose a window with start before end.",
    "terrine.design.gap_violation": (
        "Keep feature windows before the gap and outcome windows after it."
    ),
    "terrine.budget.scan_unknown": (
        "Use an owner-approved backend with a verified scan bound, "
        "or ask the owner to change strict scan policy."
    ),
    "terrine.agent.request_limit": (
        "Ask the owner to create a new tool session with an approved request budget."
    ),
}


def refusal_response(error: TerrineError) -> Response[Any]:
    """Project stable refusal fields without backend messages or arbitrary context."""
    context = {key: value for key, value in error.context.items() if key in _CONTEXT_FIELDS}
    route = context.get("route")
    return Response(
        status="refused",
        code=error.code,
        context=thaw(context),
        suggested_route=route
        if isinstance(route, str)
        else _ROUTES.get(
            error.code, "Review the typed request and owner-approved dataset configuration."
        ),
    )
