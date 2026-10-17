"""Native Model facade for metadata-only Catalog binding."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

from terrine.data import BoundCoverage, Coverage, UnitSource
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA, bind_metric, normalize_source
from terrine.semantic.io import load_catalog
from terrine.semantic.models import BoundMetric, BoundSource, Catalog
from terrine.semantic.time import boundary_timezone
from terrine.temporal.readiness import Readiness

__all__ = ["Model"]

_INVALID = RefusalSpec("terrine.model.invalid_binding", "Model binding input is invalid: {reason}.")


def _boundary_function(boundary: str):
    zone = boundary_timezone(boundary)

    def day_boundary(value: date) -> datetime:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError("day boundary input must be a date")
        return datetime.combine(value, datetime.min.time(), zone)

    return day_boundary


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class Model:
    """Bound semantic metadata and a connection, without executing source data queries."""

    catalog: Catalog
    con: Any
    readiness: Readiness
    units: Mapping[str, UnitSource]
    event_ids: Mapping[str, str]
    sources: Mapping[str, BoundSource]
    metrics: Mapping[str, BoundMetric]
    model_hash: str
    day_boundary: Callable[[date], datetime]

    def __init__(
        self,
        *,
        catalog: Catalog,
        con: Any,
        readiness: Readiness,
        units: Mapping[str, UnitSource] | None,
        event_ids: Mapping[str, str] | None,
        sources: Mapping[str, BoundSource],
        metrics: Mapping[str, BoundMetric],
    ) -> None:
        object.__setattr__(self, "catalog", catalog)
        object.__setattr__(self, "con", con)
        object.__setattr__(self, "readiness", readiness)
        object.__setattr__(self, "units", MappingProxyType(dict(units or {})))
        object.__setattr__(self, "event_ids", MappingProxyType(dict(event_ids or {})))
        object.__setattr__(self, "sources", MappingProxyType(dict(sources)))
        object.__setattr__(self, "metrics", MappingProxyType(dict(metrics)))
        payload = json.dumps(catalog.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        object.__setattr__(self, "model_hash", hashlib.sha256(payload.encode()).hexdigest())
        object.__setattr__(self, "day_boundary", _boundary_function(catalog.day_boundary))

    @classmethod
    def from_catalog(
        cls,
        path_or_catalog: str | Path | Catalog,
        *,
        con: Any,
        readiness: Readiness | Mapping[str, Coverage | BoundCoverage],
        units: Mapping[str, UnitSource] | None = None,
        event_ids: Mapping[str, str] | None = None,
    ) -> Model:
        """Bind native definitions to declared schema metadata only."""
        catalog = (
            path_or_catalog
            if isinstance(path_or_catalog, Catalog)
            else load_catalog(path_or_catalog)
        )
        day = _boundary_function(catalog.day_boundary)
        if isinstance(readiness, Readiness):
            bound_readiness = readiness
        elif isinstance(readiness, Mapping):
            bound = {}
            for name, coverage in readiness.items():
                if isinstance(coverage, Coverage):
                    bound[name] = coverage.bind(day)
                elif isinstance(coverage, BoundCoverage):
                    bound[name] = coverage
                else:
                    raise _INVALID.error(
                        reason=f"readiness for {name!r} must be Coverage or BoundCoverage"
                    )
            bound_readiness = Readiness(bound)
        else:
            raise _INVALID.error(reason="readiness must be a Readiness or source mapping")
        unit_map = dict(units or {})
        event_map = dict(event_ids or {})
        if any(
            not isinstance(key, str) or not isinstance(value, UnitSource)
            for key, value in unit_map.items()
        ):
            raise _INVALID.error(reason="units must map names to UnitSource")
        if any(
            not isinstance(key, str) or not isinstance(value, str) or not value
            for key, value in event_map.items()
        ):
            raise _INVALID.error(reason="event_ids must map source names to column names")
        sources = {name: normalize_source(con, catalog, name) for name in catalog.sources}
        metrics = {name: bind_metric(catalog, name) for name in catalog.metrics}
        for metric in metrics.values():
            for source_name in metric.source_names:
                bound_readiness.for_fact(source_name)
        # Roster and event-id schema checks use relation metadata, never data scans.
        for unit in unit_map.values():
            schema = con.table(unit.table).schema()
            names = set(schema.names)
            for column in (
                unit.key,
                unit.entered_at,
                unit.exited_at,
                unit.observed_from,
                unit.observed_until,
            ):
                if column is not None and column not in names:
                    raise _SCHEMA.error(field="roster", column=column)
            for column in (
                unit.entered_at,
                unit.exited_at,
                unit.observed_from,
                unit.observed_until,
            ):
                if column is not None and not schema[column].is_timestamp():
                    raise _SCHEMA.error(field="roster timestamp", column=column)
        for source_name, column in event_map.items():
            source = sources.get(source_name)
            if source is None:
                raise _INVALID.error(reason=f"event ID source {source_name!r} is not declared")
            relation = (
                con.table(source.relation.name)
                if source.relation.kind == "table"
                else con.sql(source.relation.sql)
            )
            if column not in relation.schema().names:
                raise _INVALID.error(reason=f"event ID column {column!r} is not declared")
        return cls(
            catalog=catalog,
            con=con,
            readiness=bound_readiness,
            units=unit_map,
            event_ids=event_map,
            sources=sources,
            metrics=metrics,
        )
