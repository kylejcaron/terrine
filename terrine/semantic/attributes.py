"""Row-free ownership resolution for as-of attributes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from terrine.semantic.models import (
    BoundDimension,
    BoundSource,
    BoundValidity,
    Property,
    PropertyBinding,
    RelationSpec,
)

__all__ = ["ResolvedAttribute", "RosterAttributes", "resolve_attribute"]


@dataclass(frozen=True, slots=True)
class RosterAttributes:
    relation: RelationSpec
    key: str
    attributes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResolvedAttribute:
    owner: Literal["roster", "source"]
    prop: PropertyBinding
    dimension: BoundDimension
    source: BoundSource | None


def resolve_attribute(
    *,
    sources: Mapping[str, BoundSource],
    properties: Mapping[str, Property],
    roster: RosterAttributes | None,
    attribute: str,
    unit: str,
) -> ResolvedAttribute:
    """Resolve an attribute to its authoritative relation and temporal binding."""
    if roster is not None and attribute in roster.attributes:
        prop = PropertyBinding(attribute, attribute, "string", "static", None)
        dimension = BoundDimension(
            name=f"roster:{unit}",
            relation=roster.relation,
            unit=unit,
            key=roster.key,
            validity=None,
            properties=MappingProxyType({attribute: prop}),
        )
        return ResolvedAttribute("roster", prop, dimension, None)

    declaration = properties.get(attribute)
    for source in sources.values():
        if declaration is not None and source.name != declaration.source:
            continue
        for dimension in source.dimensions:
            prop = dimension.properties.get(attribute)
            if prop is not None and (
                declaration is None or declaration.dimension == dimension.name
            ):
                return ResolvedAttribute("source", prop, dimension, source)
        prop = source.properties.get(attribute)
        if prop is not None and prop.dimension_name is None:
            key = source.units.get(unit)
            if key is None:
                continue
            dimension = BoundDimension(
                name=f"{source.name}:{attribute}",
                relation=source.relation,
                unit=unit,
                key=key,
                validity=(
                    None
                    if prop.as_of == "static"
                    else BoundValidity(None, None, source.time_column)
                ),
                properties=MappingProxyType({attribute: prop}),
            )
            return ResolvedAttribute("source", prop, dimension, source)
    raise KeyError(attribute)
