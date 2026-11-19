"""Strict native Catalog records and immutable bound descriptors."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self, TypeAlias, get_args, get_origin

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FieldSerializationInfo,
    SerializerFunctionWrapHandler,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

if TYPE_CHECKING:
    from terrine.data import UnitSource
    from terrine.execution.inputs import PhysicalRelationId, RelationHandle, VerifiedInputBinding


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
    "Event",
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


def _plain(value: Any, annotation: Any = None) -> Any:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if isinstance(value, Mapping):
        value_type = args[1] if len(args) > 1 else None
        return {key: _plain(item, value_type) for key, item in value.items()}
    if isinstance(value, tuple):
        if origin is list:
            item_type = args[0] if args else None
            return [_plain(item, item_type) for item in value]
        item_type = args[0] if args else None
        return tuple(_plain(item, item_type) for item in value)
    if isinstance(value, list):
        item_type = args[0] if args else None
        return [_plain(item, item_type) for item in value]
    return value


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _seal_fields(self) -> Self:
        for name in type(self).model_fields:
            object.__setattr__(self, name, _seal(getattr(self, name)))
        return self

    @field_serializer("*", mode="wrap")
    def _serialize_field(
        self,
        value: Any,
        handler: SerializerFunctionWrapHandler,
        info: FieldSerializationInfo,
    ):
        field = type(self).model_fields[info.field_name]
        return handler(_plain(value, field.annotation))

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
    time: StrictStr
    units: Mapping[StrictStr, StrictStr]
    events: tuple[StrictStr, ...]
    properties: tuple[StrictStr, ...]
    dimensions: tuple[StrictStr, ...]
    where: tuple[Filter, ...] = ()
    row_key: StrictStr | None = None


class Event(_Record):
    source: StrictStr
    column: StrictStr | None


PropertyType: TypeAlias = Literal[  # noqa: UP040 - pydantic 2.8 lacks PEP 695 alias support.
    "string", "int", "float", "bool", "date", "number", "timestamp"
]


class Property(_Record):
    source: StrictStr
    column: StrictStr
    dtype: PropertyType
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
            raise PydanticCustomError(
                "invalid_validity_axis", "validity declaration uses an unsupported time axis"
            )
        return self


class Dimension(_Record):
    relation: RelationSpec
    unit: StrictStr | None
    key: StrictStr
    validity: Validity | None = None
    properties: tuple[StrictStr, ...]
    foreign_key: StrictStr | None = None

    @model_validator(mode="after")
    def validate_join_key(self) -> Self:
        if (self.unit is None) == (self.foreign_key is None) or (
            self.foreign_key is not None and self.validity is not None
        ):
            raise PydanticCustomError(
                "invalid_dimension_join", "declare one unit or static foreign key"
            )
        return self


# Comparison operators shared by Catalog filters, where= keys and Design comparisons.
FilterOp: TypeAlias = Literal[  # noqa: UP040 - pydantic 2.8 lacks PEP 695 alias support.
    "equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "between"
]


class Filter(_Record):
    property: StrictStr
    op: FilterOp
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
            raise PydanticCustomError(
                "invalid_filter_arity", "filter operation has an unsupported number of values"
            )
        if self.op in {"in", "not_in"} and not self.values:
            raise PydanticCustomError(
                "invalid_filter_arity", "filter operation has an unsupported number of values"
            )
        if self.op == "between" and len(self.values) != 2:
            raise PydanticCustomError(
                "invalid_filter_arity", "filter operation has an unsupported number of values"
            )
        if any(not _json_value(v) or isinstance(v, (list, tuple, Mapping)) for v in self.values):
            raise PydanticCustomError(
                "invalid_filter_values", "filter values are not supported JSON scalars"
            )
        return self


class Measure(_Record):
    """One aggregation of an event; ``exact: false`` allows native float arithmetic.

    Counts and distinct counts are always exact, and binding refuses native arithmetic on
    integer or decimal event values, which always aggregate exactly.
    """

    event: StrictStr
    aggregation: Literal[
        "sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ]
    filters: tuple[Filter, ...] = ()
    window_days: StrictInt | None = None
    exact: StrictBool = True

    @model_validator(mode="after")
    def validate_arithmetic(self):
        if not self.exact and self.aggregation in {"count", "count_distinct"}:
            raise PydanticCustomError(
                "exact_aggregation", "counts and distinct counts are always exact"
            )
        return self


class FormulaLiteral(_Record):
    kind: Literal["literal"]
    value: StrictInt | StrictFloat

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: int | float) -> int | float:
        if (
            not -float.fromhex("0x1.fffffffffffffp1023")
            <= value
            <= (float.fromhex("0x1.fffffffffffffp1023"))
        ):
            raise PydanticCustomError("invalid_formula_literal", "literal must fit binary64")
        return value


class FormulaReference(_Record):
    kind: Literal["measure", "metric"]
    name: StrictStr


class FormulaNegation(_Record):
    kind: Literal["negation"]
    operand: Formula


FormulaOperator: TypeAlias = Literal["+", "-", "*", "/"]  # noqa: UP040


class FormulaBinary(_Record):
    kind: Literal["binary"]
    operator: FormulaOperator
    left: Formula
    right: Formula


Formula: TypeAlias = Annotated[  # noqa: UP040 - pydantic 2.8 lacks PEP 695 alias support.
    FormulaLiteral | FormulaReference | FormulaNegation | FormulaBinary,
    Field(discriminator="kind"),
]


class PopulationRequirement(_Record):
    relation: TableRef
    key: StrictStr
    unit: StrictStr


class Metric(_Record):
    kind: Literal["mean", "conversion", "ratio", "formula"] | StrictStr
    unit: StrictStr
    measure: StrictStr | None = None
    numerator: StrictStr | None = None
    denominator: StrictStr | None = None
    winsorization: object | None = None
    formula: Formula | None = None
    population: StrictStr | None = None

    @model_validator(mode="after")
    def validate_measure_shape(self):
        if self.kind == "formula" and self.formula is not None:
            valid = self.measure is self.numerator is self.denominator is None
        elif self.kind == "ratio":
            valid = (
                self.measure is None and self.numerator is not None and self.denominator is not None
            )
        else:
            # Defer native unsupported kinds to the existing binding refusal.
            valid = self.measure is not None and self.numerator is self.denominator is None
        if not valid or (self.kind != "formula" and self.formula is not None):
            raise PydanticCustomError(
                "invalid_formula_shape" if self.formula is not None else "invalid_metric_shape",
                "metric fields do not match the declared kind",
            )
        return self

    @model_validator(mode="after")
    def validate_transform(self):
        if self.winsorization is not None and not _json_value(self.winsorization):
            raise PydanticCustomError("unsafe_transform", "winsorization is not safe JSON data")
        return self


class Catalog(_Record):
    schema_version: Literal[3, 4]
    day_boundary: StrictStr
    dialect: StrictStr
    sources: Mapping[StrictStr, Source]
    events: Mapping[StrictStr, Event]
    properties: Mapping[StrictStr, Property]
    dimensions: Mapping[StrictStr, Dimension]
    measures: Mapping[StrictStr, Measure]
    metrics: Mapping[StrictStr, Metric]
    populations: Mapping[StrictStr, PopulationRequirement] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_version(self) -> Self:
        if self.schema_version == 3 and (
            self.populations
            or any(source.row_key is not None for source in self.sources.values())
            or any(dim.foreign_key is not None for dim in self.dimensions.values())
            or any(prop.dtype in {"number", "timestamp"} for prop in self.properties.values())
            or any(
                metric.formula is not None or metric.population is not None
                for metric in self.metrics.values()
            )
        ):
            raise PydanticCustomError("shared_requires_version_4", "declare schema_version: 4")
        return self

    @field_validator("day_boundary")
    @classmethod
    def validate_boundary(cls, value: str) -> str:
        from terrine.semantic.time import boundary_timezone

        try:
            boundary_timezone(value)
        except ValueError as exc:
            raise PydanticCustomError(
                "invalid_day_boundary", "day_boundary is not a supported fixed UTC offset"
            ) from exc
        return value

    def required_relations(
        self, con: Any, *, units: Mapping[str, UnitSource] | None = None
    ) -> tuple[PhysicalRelationId, ...]:
        """Discover the exact supported input frontier without reading source rows."""
        from terrine.execution.inputs import required_relations

        return required_relations(self, con, units=units)

    def verify_immutable_inputs(
        self,
        con: Any,
        *,
        units: Mapping[str, UnitSource] | None = None,
        relations: Mapping[PhysicalRelationId, RelationHandle],
    ) -> VerifiedInputBinding:
        """Verify live issued inputs; strings and provenance receipts are not authority."""
        from terrine.execution.inputs import verify_immutable_inputs

        return verify_immutable_inputs(self, con, units=units, relations=relations)


@dataclass(frozen=True, slots=True)
class BoundFilter:
    property_name: str
    op: FilterOp
    values: tuple[None | bool | int | float | str, ...]


@dataclass(frozen=True, slots=True)
class PropertyBinding:
    name: str
    column: str
    dtype: PropertyType
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
    unit: str | None
    key: str
    validity: BoundValidity | None
    properties: Mapping[str, PropertyBinding]
    foreign_key: str | None = None


@dataclass(frozen=True, slots=True)
class BoundMeasure:
    event: str
    source_name: str
    aggregation: Literal[
        "sum", "count", "avg_event", "avg_calendar_day", "min", "max", "count_distinct"
    ]
    filters: tuple[BoundFilter, ...]
    window_days: int | None
    value_type: str | None
    exact: bool = True


@dataclass(frozen=True, slots=True)
class BoundSource:
    name: str
    relation: RelationSpec
    time_column: str
    units: Mapping[str, str]
    events: Mapping[str, str | None]
    properties: Mapping[str, PropertyBinding]
    dimensions: tuple[BoundDimension, ...]
    dialect: str
    day_boundary: str
    where: tuple[Filter, ...] = ()
    row_key: str | None = None


@dataclass(frozen=True, slots=True)
class FormulaOperand:
    index: int


@dataclass(frozen=True, slots=True)
class FormulaOperation:
    operator: FormulaOperator
    left: int
    right: int | None = None


type FormulaStep = FormulaOperand | FormulaLiteral | FormulaOperation


@dataclass(frozen=True, slots=True)
class BoundMetric:
    name: str
    unit: str
    unit_columns: Mapping[str, str]
    kind: Literal["mean", "conversion", "ratio", "formula"]
    measures: tuple[BoundMeasure, ...]
    source_names: tuple[str, ...]
    transforms: tuple[object, ...]
    formula: tuple[FormulaStep, ...] = ()
    presence: tuple[bool, ...] = ()
