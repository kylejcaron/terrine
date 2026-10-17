"""Native Model facade for metadata-only Catalog binding."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from terrine.data import BoundCoverage, Coverage, UnitSource
from terrine.errors import RefusalSpec
from terrine.semantic.bind import _SCHEMA, bind_metric, normalize_source
from terrine.semantic.io import load_catalog
from terrine.semantic.models import BoundMetric, BoundSource, Catalog, RelationSpec, TableRef
from terrine.semantic.time import boundary_timezone
from terrine.temporal.readiness import Readiness

if TYPE_CHECKING:
    from terrine.execution.inputs import ExecutionContext, VerifiedInputBinding

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
    input_binding: VerifiedInputBinding | None
    _resolver: Callable[[RelationSpec], Any]

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
        resolver: Callable[[RelationSpec], Any],
        input_binding: VerifiedInputBinding | None,
    ) -> None:
        object.__setattr__(self, "catalog", catalog)
        object.__setattr__(self, "con", con)
        object.__setattr__(self, "readiness", readiness)
        object.__setattr__(self, "units", MappingProxyType(dict(units or {})))
        object.__setattr__(self, "event_ids", MappingProxyType(dict(event_ids or {})))
        object.__setattr__(self, "sources", MappingProxyType(dict(sources)))
        object.__setattr__(self, "metrics", MappingProxyType(dict(metrics)))
        object.__setattr__(self, "input_binding", input_binding)
        object.__setattr__(self, "_resolver", resolver)
        declarations = {
            "catalog": catalog.model_dump(mode="json"),
            "units": {name: asdict(unit) for name, unit in self.units.items()},
            "event_ids": dict(self.event_ids),
            "readiness": {
                name: [coverage.start.isoformat(), coverage.end_exclusive.isoformat()]
                for name, coverage in readiness.sources.items()
            },
        }
        payload = json.dumps(declarations, sort_keys=True, separators=(",", ":"))
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
        input_binding: VerifiedInputBinding | None = None,
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
        from terrine.execution.inputs import bind_resolver

        resolver = bind_resolver(catalog, con, units=unit_map, input_binding=input_binding)
        return cls._bind(
            catalog,
            con=con,
            readiness=bound_readiness,
            units=unit_map,
            event_ids=event_map,
            resolver=resolver,
            input_binding=input_binding,
        )

    @classmethod
    def _bind(
        cls,
        catalog: Catalog,
        *,
        con: Any,
        readiness: Readiness,
        units: Mapping[str, UnitSource],
        event_ids: Mapping[str, str],
        resolver: Callable[[RelationSpec], Any],
        input_binding: VerifiedInputBinding | None,
    ) -> Model:
        sources = {
            name: normalize_source(con, catalog, name, resolver=resolver)
            for name in catalog.sources
        }
        metrics = {name: bind_metric(catalog, name) for name in catalog.metrics}
        for unit in units.values():
            schema = resolver(TableRef(kind="table", name=unit.table)).schema()
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
        for source_name, column in event_ids.items():
            source = sources.get(source_name)
            if source is None:
                raise _INVALID.error(reason=f"event ID source {source_name!r} is not declared")
            if column not in resolver(source.relation).schema().names:
                raise _INVALID.error(reason=f"event ID column {column!r} is not declared")
        return cls(
            catalog=catalog,
            con=con,
            readiness=readiness,
            units=units,
            event_ids=event_ids,
            sources=sources,
            metrics=metrics,
            resolver=resolver,
            input_binding=input_binding,
        )

    def _relation(self, relation: RelationSpec) -> Any:
        return self._resolver(relation)

    def _sql(self, sql: str) -> Any:
        """Admit compiler functions before schema inference can invoke callbacks."""
        from terrine.execution._native import check_native_functions, raw_connection

        check_native_functions(raw_connection(self.con), sql, None)
        return self.con.sql(sql)

    def _for_execution(self, context: ExecutionContext) -> Model:
        relations = (
            *(source.relation for source in self.catalog.sources.values()),
            *(dimension.relation for dimension in self.catalog.dimensions.values()),
            *(TableRef(kind="table", name=unit.table) for unit in self.units.values()),
        )
        for relation in relations:
            if self._relation(relation).schema() != context.resolve(relation).schema():
                raise _SCHEMA.error(field="execution schema", column="<relation>")
        return self._bind(
            self.catalog,
            con=context.con,
            readiness=self.readiness,
            units=self.units,
            event_ids=self.event_ids,
            resolver=context.resolve,
            input_binding=self.input_binding,
        )

    @classmethod
    def from_increment(
        cls,
        path_or_definitions: Any,
        *,
        con: Any,
        readiness: Readiness | Mapping[str, Coverage | BoundCoverage],
        units: Mapping[str, UnitSource] | None = None,
        event_ids: Mapping[str, str] | None = None,
        input_binding: VerifiedInputBinding | None = None,
    ) -> Model:
        """Adapt optional published semantics through the native Catalog workflow."""
        from terrine.adapters.increment import to_catalog

        return cls.from_catalog(
            to_catalog(path_or_definitions, dialect=str(getattr(con, "name", ""))),
            con=con,
            readiness=readiness,
            units=units,
            event_ids=event_ids,
            input_binding=input_binding,
        )
