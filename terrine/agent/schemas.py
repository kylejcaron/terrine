"""Strict, data-only version-one dataset tool envelopes."""

from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, validate_default=True, serialize_by_alias=True
    )
    schema_version: Literal[1] = 1


class DesignRequest(Record):
    serialized: str
    base: str | None = None


class NamedDesignRequest(DesignRequest):
    name: str


class PlanReceipt(Record):
    receipt_id: str
    design_hash: str
    plan_hash: str


class DatasetColumn(Record):
    name: str
    data_type: str
    role: str
    complete_column: str | None = None
    reason_column: str | None = None


class AgentDatasetReference(Record):
    dataset: str
    generation: str
    definition_hash: str
    definition_reference: str
    model_hash: str
    manifest_hash: str
    row_count: Annotated[int, Field(ge=0)]
    columns: tuple[DatasetColumn, ...] = ()


class BudgetRequest(Record):
    max_rows: Annotated[int, Field(ge=0)] | None = None
    max_output_bytes: Annotated[int, Field(ge=0)] | None = None
    max_scan_bytes: Annotated[int, Field(ge=0)] | None = None
    strict_scan: bool = True


class DestinationRequest(Record):
    database: str
    schema_name: str = Field(alias="schema")
    dataset: str


class PreviewRequest(Record):
    serialized: str
    budget: BudgetRequest | None = None
    bounded_count: Annotated[int, Field(ge=0)] = 0


class PrepareRequest(Record):
    serialized: str
    destination: DestinationRequest
    invocation_id: str
    budget: BudgetRequest | None = None


class ExecuteRequest(Record):
    receipt_id: str
    request_key: Annotated[str, Field(min_length=1, max_length=256)]


class RunRequest(Record):
    run_id: str


class RunEventsRequest(RunRequest):
    after: str | None = None
    limit: Annotated[int, Field(gt=0)] = 100


class RowPreviewRequest(Record):
    reference: AgentDatasetReference
    limit: Annotated[int, Field(gt=0)]
    columns: tuple[str, ...]


class ValidationOutput(Record):
    spec_hash: str
    required_sources: tuple[str, ...]
    runtime_obligations: tuple[str, ...]


class DesignOutput(ValidationOutput):
    serialized: str
    changed_paths: tuple[str, ...] = ()


class SourceMetadata(Record):
    name: str
    units: tuple[str, ...]
    events: tuple[str, ...]
    properties: tuple[str, ...]


class MetricMetadata(Record):
    name: str
    unit: str
    kind: str
    sources: tuple[str, ...]


class PropertyMetadata(Record):
    name: str
    source: str
    dtype: str
    as_of: str


class CatalogOutput(Record):
    sources: tuple[SourceMetadata, ...]
    metrics: tuple[MetricMetadata, ...]
    properties: tuple[PropertyMetadata, ...]
    units: tuple[str, ...]


class CapabilityOutput(Record):
    operations: tuple[str, ...]
    runtime_shapes: tuple[str, ...]


class SchemaOutput(Record):
    document: Mapping[str, JsonValue]

    @field_validator("document", mode="after")
    @classmethod
    def _freeze_document(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze(value)

    @field_serializer("document")
    def _serialize_document(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw(value)


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


class RunOutput(Record):
    run_id: str
    durable_state: str
    phase: str | None = None
    error_code: str | None = None
    pinned_result_ref: str | None = None
    latest_event_cursor: str | None = None


class CancelOutput(Record):
    run_id: str
    disposition: str
    durable_state: str


class EventOutput(Record):
    sequence: int
    kind: str
    state: str | None = None
    phase: str | None = None


class EventsOutput(Record):
    events: tuple[EventOutput, ...]
    next_cursor: str | None
    has_more: bool
    cursor_gap: bool


class DefinitionOutput(DesignOutput):
    definition_digest: str


class RowsOutput(Record):
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    @field_validator("rows", mode="after")
    @classmethod
    def _rows(cls, value):
        return freeze(value)

    @field_serializer("rows")
    def _serialize_rows(self, value):
        return thaw(value)


class AgentExportManifest(Record):
    dataset: str
    generation: str
    row_count: Annotated[int, Field(ge=0)]
    byte_count: Annotated[int, Field(ge=0)]
    shard_count: Annotated[int, Field(ge=0)]
