"""Strict native Catalog records and immutable bound descriptors."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    StrictInt,
    StrictStr,
    field_serializer,
    model_validator,
)


def _json_value(value: object) -> bool:
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, (list, tuple)):
        return all(_json_value(item) for item in value)
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _json_value(item) for key, item in value.items())
    return False


__all__ = [
    "TableRef",
    "SqlQuery",
    "RelationSpec",
    "Source",
    "Fact",
    "Property",
    "Validity",
    "Dimension",
    "Filter",
    "Measure",
    "Metric",
    "Catalog",
    "BoundFilter",
    "PropertyBinding",
    "BoundValidity",
    "BoundDimension",
    "BoundMeasure",
    "BoundSource",
    "BoundMetric",
]


def _seal(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _seal(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_seal(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_plain(item) for item in value)
    return value


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _seal_fields(self) -> Self:
        for name in type(self).model_fields:
            object.__setattr__(self, name, _seal(getattr(self, name)))
        return self

    @field_serializer("*", mode="wrap")
    def _serialize_field(self, value: Any, handler: SerializerFunctionWrapHandler):
        return handler(_plain(value))

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> Self:
        copied = type(self).model_validate(self.model_dump(mode="python"))
        if memo is not None:
            memo[id(self)] = copied
        return copied


class TableRef(_Record):
    kind: Literal["table"]
    name: StrictStr


class SqlQuery(_Record):
    kind: Literal["sql"]
    sql: StrictStr


RelationSpec = Annotated[TableRef | SqlQuery, Field(discriminator="kind")]


class Source(_Record):
    relation: RelationSpec
    timestamp: StrictStr
    entities: Mapping[StrictStr, StrictStr]
    facts: tuple[StrictStr, ...]
    properties: tuple[StrictStr, ...]
    dimensions: tuple[StrictStr, ...]


class Fact(_Record):
    source: StrictStr
    column: StrictStr | None


class Property(_Record):
    source: StrictStr
    column: StrictStr
    dtype: Literal["string", "int", "float", "bool", "date"]
    as_of: Literal["event_time", "pre_exposure", "static"]
    dimension: StrictStr | None = None


class Validity(_Record):
    valid_from: StrictStr | None = None
    valid_to: StrictStr | None = None
    changed_at: StrictStr | None = None

    @model_validator(mode="after")
    def validate_axis(self):
        if (self.valid_from is None) == (self.changed_at is None) or (
            self.changed_at is not None and self.valid_to is not None
        ):
            raise ValueError("validity requires a range start or a change point, not both")
        return self


class Dimension(_Record):
    relation: RelationSpec
    source_entity: StrictStr
    entity_column: StrictStr
    validity: Validity | None = None
    properties: tuple[StrictStr, ...]


class Filter(_Record):
    property: StrictStr
    op: Literal["equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "between"]
    values: tuple[object, ...]

    @model_validator(mode="before")
    @classmethod
    def accept_single_value(cls, value):
        if isinstance(value, dict) and "value" in value and "values" not in value:
            return {key: item for key, item in value.items() if key != "value"} | {
                "values": (value["value"],)
            }
        return value

    @model_validator(mode="after")
    def validate_arity(self):
        if self.op in {"equals", "not_equals", "gt", "gte", "lt", "lte"} and len(self.values) != 1:
            raise ValueError("comparison filter requires one value")
        if self.op in {"in", "not_in"} and not self.values:
            raise ValueError("membership filter requires values")
        if self.op == "between" and len(self.values) != 2:
            raise ValueError("between filter requires two values")
        if any(not _json_value(v) or isinstance(v, (list, tuple, Mapping)) for v in self.values):
            raise ValueError("filter values must be JSON scalars")
        return self


class Measure(_Record):
    fact: StrictStr
    aggregation: Literal[
        "sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ]
    filters: tuple[Filter, ...] = ()
    window_days: StrictInt | None = None


class Metric(_Record):
    kind: Literal["mean", "conversion", "ratio"] | StrictStr
    entity: StrictStr
    measure: StrictStr | None = None
    numerator: StrictStr | None = None
    denominator: StrictStr | None = None
    winsorization: object | None = None

    @model_validator(mode="after")
    def validate_measure_shape(self):
        if self.kind == "ratio":
            if self.measure is not None or self.numerator is None or self.denominator is None:
                raise ValueError("ratio requires numerator and denominator")
        elif self.measure is None or self.numerator is not None or self.denominator is not None:
            raise ValueError("non-ratio metric requires measure")
        return self

    @model_validator(mode="after")
    def validate_transform(self):
        if self.winsorization is not None and not _json_value(self.winsorization):
            raise ValueError("winsorization must be safe JSON data")
        return self


class Catalog(_Record):
    schema_version: Literal[1]
    day_boundary: StrictStr
    dialect: StrictStr
    sources: Mapping[StrictStr, Source]
    facts: Mapping[StrictStr, Fact]
    properties: Mapping[StrictStr, Property]
    dimensions: Mapping[StrictStr, Dimension]
    measures: Mapping[StrictStr, Measure]
    metrics: Mapping[StrictStr, Metric]

    @model_validator(mode="after")
    def validate_boundary(self):
        from terrine.semantic.time import boundary_timezone

        boundary_timezone(self.day_boundary)
        return self


@dataclass(frozen=True, slots=True)
class BoundFilter:
    property_name: str
    op: Literal["equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "between"]
    values: tuple[None | bool | int | float | str, ...]


@dataclass(frozen=True, slots=True)
class PropertyBinding:
    name: str
    column: str
    dtype: Literal["string", "int", "float", "bool", "date"]
    as_of: Literal["event_time", "pre_exposure", "static"]
    dimension_name: str | None


@dataclass(frozen=True, slots=True)
class BoundValidity:
    valid_from: str | None
    valid_to: str | None
    changed_at: str | None


@dataclass(frozen=True, slots=True)
class BoundDimension:
    name: str
    relation: RelationSpec
    source_entity: str
    entity_column: str
    validity: BoundValidity | None
    properties: Mapping[str, PropertyBinding]


@dataclass(frozen=True, slots=True)
class BoundMeasure:
    fact: str
    source_name: str
    aggregation: Literal[
        "sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ]
    filters: tuple[BoundFilter, ...]
    window_days: int | None
    value_type: str | None


@dataclass(frozen=True, slots=True)
class BoundSource:
    name: str
    relation: RelationSpec
    timestamp_column: str
    entities: Mapping[str, str]
    facts: Mapping[str, str | None]
    properties: Mapping[str, PropertyBinding]
    dimensions: tuple[BoundDimension, ...]
    dialect: str
    day_boundary: str


@dataclass(frozen=True, slots=True)
class BoundMetric:
    name: str
    entity: str
    entity_columns: Mapping[str, str]
    kind: Literal["mean", "conversion", "ratio"]
    measures: tuple[BoundMeasure, ...]
    source_names: tuple[str, ...]
    transforms: tuple[object, ...]
