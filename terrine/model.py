"""Native Model facade for metadata-only Catalog binding."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from importlib import import_module
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from terrine.backends.registry import Engine, resolve_engine
from terrine.data import BoundCoverage, Coverage, EventMetric, Events, History, Ratio, UnitSource
from terrine.errors import RefusalSpec
from terrine.execution._metadata import metadata_operation
from terrine.execution._native import _ascii_key
from terrine.semantic.bind import _SCHEMA, _relation, bind_metric, normalize_source
from terrine.semantic.io import dump_catalog, load_catalog, validate_catalog
from terrine.semantic.models import BoundMetric, BoundSource, Catalog, RelationSpec, TableRef
from terrine.semantic.time import boundary_timezone
from terrine.temporal.readiness import Readiness

if TYPE_CHECKING:
    from terrine.execution.inputs import ExecutionContext, VerifiedInputBinding
__all__ = ["Model"]

_INVALID = RefusalSpec(
    "terrine.model.invalid_binding",
    "The model cannot use this input ({reason}). "
    "Check the unit, event and history declarations passed to the model.",
)
_MISSING_EVENT_COMPLETENESS = RefusalSpec(
    "terrine.model.missing_event_completeness",
    "{source!r} has no complete=(first_day, last_day). Declare the inclusive calendar "
    "days that its event or history table is fully loaded for, not the first and last "
    "event times you happen to have.",
)
_AMBIGUOUS_UNIT = RefusalSpec(
    "terrine.model.ambiguous_unit",
    "{owner} does not say which unit its rows belong to and the model declares "
    "{choices}; pass {parameter}= with one of them.",
)
_NAME_COLLISION = RefusalSpec(
    "terrine.model.invalid_binding",
    "{kind} name {name!r} is declared by {owners}; event, label and history names are "
    "shared by every Events and History of a model, so give each its own name.",
)
_UNSUPPORTED_COLUMN_TYPE = RefusalSpec(
    "terrine.model.invalid_binding",
    "Column {column!r} of {source!r} has type {dtype}, which cannot be a label or history "
    "value; supported types are {supported}.",
)
_MISSING_REFERENCE = RefusalSpec(
    "terrine.semantic.missing_reference",
    "{owner!r} uses {kind} {name!r}, which is not declared; available names are {available!r}. "
    "Use one of these names or add the missing {kind} to the model.",
)
_IN_MEMORY_WITH_CONNECTION = RefusalSpec(
    "terrine.model.invalid_binding",
    "{inputs} are in-memory tables but a connection was given; Terrine never uploads "
    "data, so load them into the warehouse yourself or omit con to build on a private "
    "in-process DuckDB.",
)
_CONNECTION_REQUIRED = RefusalSpec(
    "terrine.model.invalid_binding",
    "{tables} are table names but no connection was given; pass the connection that "
    "holds them, or pass the tables themselves.",
)
_TABLE_NAME_COLLISION = RefusalSpec(
    "terrine.model.invalid_binding",
    "In-memory tables are registered under their mapping keys, which DuckDB matches "
    "without regard to ASCII case, and {name!r} is used by {owners}; give the unit or "
    "the event table its own name.",
)
_MISSING_DEPENDENCY = RefusalSpec(
    "terrine.convert.missing_dependency",
    "In-memory tables run on the optional {extra!r} dependency; {route}, or bind the "
    "tables by name on a connection.",
)
_LABEL_DTYPES = ("string", "int", "float", "bool", "date")


def _label_dtype(dtype: Any, *, source: str, column: str) -> str:
    """The catalog dtype a bound column type reads back as; the inverse of binding."""
    if dtype.is_string():
        return "string"
    if dtype.is_boolean():
        return "bool"
    if dtype.is_integer() or (dtype.is_decimal() and dtype.scale == 0):
        return "int"
    if dtype.is_floating() or dtype.is_decimal():
        return "float"
    if dtype.is_date():
        return "date"
    raise _UNSUPPORTED_COLUMN_TYPE.error(
        reason="unsupported_column_type",
        source=source,
        column=column,
        dtype=str(dtype),
        supported=_LABEL_DTYPES,
    )


def _claim_name(owners: dict[str, str], kind: str, name: str, owner: str) -> None:
    prior = owners.setdefault(name, owner)
    if prior != owner:
        raise _NAME_COLLISION.error(
            reason="name_collision", kind=kind, name=name, owners=tuple(sorted((prior, owner)))
        )


def _value_columns(event: Events) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            metric.column
            for metric in event.metrics.values()
            if isinstance(metric, EventMetric) and metric.column is not None
        )
    )


def _check_names(events: Mapping[str, Events], history: Mapping[str, History]) -> None:
    """Refuse shared event, label, history and metric names before any schema read.

    Events, history attributes and labels share one namespace because
    ``Initiation(name)`` resolves across events and labels; the value events
    ``<source>_<column>`` live in it too. Metrics are a separate namespace.
    """
    owners: dict[str, str] = {}
    for name in events:
        _claim_name(owners, "event", name, f"events:{name}")
    for name in history:
        _claim_name(owners, "event", name, f"history:{name}")
    for name, event in events.items():
        for label in event.labels:
            _claim_name(owners, "label", label, f"labels:{name}")
    for name, event in events.items():
        for column in _value_columns(event):
            _claim_name(owners, "event", f"{name}_{column}", f"events:{name}.{column}")
    metric_owners: dict[str, str] = {}
    for name, event in events.items():
        for metric in event.metrics:
            _claim_name(metric_owners, "metric", metric, f"events:{name}")


def _check_references(name: str, event: Events) -> None:
    owner = f"events:{name}"
    labels = tuple(sorted(event.labels))
    plain = tuple(
        sorted(
            metric
            for metric, declared in event.metrics.items()
            if isinstance(declared, EventMetric)
        )
    )
    for metric in event.metrics.values():
        if isinstance(metric, Ratio):
            for operand in (metric.numerator, metric.denominator):
                if operand not in plain:
                    raise _MISSING_REFERENCE.error(
                        kind="metric", name=operand, owner=owner, available=plain
                    )
        else:
            for filt in metric.filters:
                if filt.property not in event.labels:
                    raise _MISSING_REFERENCE.error(
                        kind="label", name=filt.property, owner=owner, available=labels
                    )


def _resolve_unit(owner: str, declared: str | None, units: Mapping[str, UnitSource]) -> str:
    if declared is not None:
        if declared not in units:
            raise _MISSING_REFERENCE.error(
                kind="unit", name=declared, owner=owner, available=tuple(sorted(units))
            )
        return declared
    if len(units) == 1:
        return next(iter(units))
    if not units:
        raise _INVALID.error(reason="no_unit_source")
    raise _AMBIGUOUS_UNIT.error(owner=owner, choices=tuple(sorted(units)), parameter="unit")


def _in_memory_tables(
    con: Any | None,
    units: Mapping[str, UnitSource],
    events: Mapping[str, Events],
    history: Mapping[str, History],
) -> dict[str, pa.Table] | None:
    """The tables to register on a private DuckDB, keyed by their mapping keys.

    ``None`` when every table is a name in ``con``. A connection beside an
    in-memory table, or a table name beside no connection, refuses: nothing is
    uploaded, and a name needs the connection that holds it.
    """
    declared = [
        (f"{kind}:{name}", name, item.table)
        for kind, mapping in (("units", units), ("events", events), ("history", history))
        for name, item in mapping.items()
    ]
    named = tuple(sorted(owner for owner, _, table in declared if isinstance(table, str)))
    in_memory = [
        (owner, name, table) for owner, name, table in declared if not isinstance(table, str)
    ]
    if con is not None:
        if in_memory:
            raise _IN_MEMORY_WITH_CONNECTION.error(
                reason="in_memory_input_with_connection",
                inputs=tuple(sorted(owner for owner, _, _ in in_memory)),
            )
        return None
    if named:
        raise _CONNECTION_REQUIRED.error(reason="connection_required", tables=named)
    owners: dict[str, str] = {}
    for owner, name, _ in in_memory:
        prior = owners.setdefault(_ascii_key(name), owner)
        if prior != owner:
            raise _TABLE_NAME_COLLISION.error(
                reason="name_collision",
                kind="table",
                name=name,
                owners=tuple(sorted((prior, owner))),
            )
    return {name: table for _, name, table in in_memory}


def _private_connection(tables: Mapping[str, pa.Table]) -> Any:
    """An in-process DuckDB holding ``tables``; nothing leaves the machine."""
    try:
        import_module("duckdb")
    except ImportError as exc:
        raise _MISSING_DEPENDENCY.error(extra="duckdb", route="install terrine[duckdb]") from exc
    import ibis

    con = ibis.duckdb.connect()
    for name, table in tables.items():
        con.create_table(name, table)
    return con


def _boundary_function(boundary: str):
    zone = boundary_timezone(boundary)

    def day_boundary(value: date) -> datetime:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError("day boundary input must be a date")
        return datetime.combine(value, datetime.min.time(), zone)

    return day_boundary


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class Model:
    """Bind units, events, metrics and history to a connection without reading event rows.

    Example: tr.Model.from_tables(units=units, events=events)
    """

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
    _engine: Engine
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
        engine: Engine,
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
        object.__setattr__(self, "_engine", engine)
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
    def from_tables(
        cls,
        con: Any | None = None,
        *,
        units: Mapping[str, UnitSource],
        events: Mapping[str, Events],
        history: Mapping[str, History] | None = None,
        dialect: str | None = None,
        day_boundary: str = "UTC",
        event_ids: Mapping[str, str] | None = None,
        allow_unverified: bool = False,
    ) -> Model:
        """Bind unit, event and history tables with explicit complete days.

        In-memory tables need no connection and materialize only; table names need ``con``,
        and unverified connections must have no pending writes during schema inspection.

        Example: tr.Model.from_tables(units=units, events=events)
        """
        if not isinstance(allow_unverified, bool):
            raise _INVALID.error(reason="wrong_type", field="allow_unverified")
        if not isinstance(units, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(unit, UnitSource)
            for name, unit in units.items()
        ):
            raise _INVALID.error(reason="wrong_type", field="units")
        if not isinstance(events, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(event, Events)
            for name, event in events.items()
        ):
            raise _INVALID.error(reason="wrong_type", field="events")
        attributes = {} if history is None else history
        if not isinstance(attributes, Mapping) or any(
            not isinstance(name, str) or not name or not isinstance(item, History)
            for name, item in attributes.items()
        ):
            raise _INVALID.error(reason="wrong_type", field="history")
        _check_names(events, attributes)
        in_memory = _in_memory_tables(con, units, events, attributes)
        event_units = {
            name: _resolve_unit(f"events:{name}", event.unit, units)
            for name, event in events.items()
        }
        history_units = {
            name: _resolve_unit(f"history:{name}", item.unit, units)
            for name, item in attributes.items()
        }
        coverage: dict[str, Coverage] = {}
        for name, declared in (*events.items(), *attributes.items()):
            if declared.complete is None:
                raise _MISSING_EVENT_COMPLETENESS.error(source=name)
            coverage[name] = Coverage(*declared.complete)
        for name, event in events.items():
            _check_references(name, event)
        if in_memory is not None:
            con = _private_connection(in_memory)
            units = {name: replace(unit, table=name) for name, unit in units.items()}
            events = {name: replace(event, table=name) for name, event in events.items()}
            attributes = {name: replace(item, table=name) for name, item in attributes.items()}
        engine = resolve_engine(con, allow_unverified=allow_unverified)
        if dialect is None:
            from terrine.execution.backend import backend_provider

            dialect = backend_provider(con, engine=engine).dialect(con)

        def schema(table: str):
            return _relation(
                con, TableRef(kind="table", name=table), dialect, engine=engine
            ).schema()

        sources: dict[str, Any] = {}
        catalog_events: dict[str, Any] = {}
        properties: dict[str, Any] = {}
        dimensions: dict[str, Any] = {}
        measures: dict[str, Any] = {}
        metrics: dict[str, Any] = {}
        for name, event in events.items():
            unit = event_units[name]
            value_events = {column: f"{name}_{column}" for column in _value_columns(event)}
            catalog_events[name] = {"source": name, "column": None}
            for column, value_event in value_events.items():
                catalog_events[value_event] = {"source": name, "column": column}
            if event.labels:
                columns = schema(event.table)
                for label in event.labels:
                    if label not in columns.names:
                        raise _SCHEMA.error(field=f"label {label}", column=label)
                    properties[label] = {
                        "source": name,
                        "column": label,
                        "dtype": _label_dtype(columns[label], source=name, column=label),
                        "as_of": "event_time",
                    }
            for metric_name, metric in event.metrics.items():
                if isinstance(metric, Ratio):
                    metrics[metric_name] = {
                        "kind": "ratio",
                        "unit": unit,
                        "numerator": metric.numerator,
                        "denominator": metric.denominator,
                    }
                    continue
                measures[metric_name] = {
                    "event": name if metric.column is None else value_events[metric.column],
                    "aggregation": metric.aggregation,
                    "filters": metric.filters,
                }
                metrics[metric_name] = {"kind": "mean", "unit": unit, "measure": metric_name}
            sources[name] = {
                "relation": {"kind": "table", "name": event.table},
                "time": event.time,
                "units": {unit: event.key},
                "events": [name, *value_events.values()],
                "properties": list(event.labels),
                "dimensions": [],
            }
        for name, item in attributes.items():
            unit = history_units[name]
            columns = schema(item.table)
            if item.column not in columns.names:
                raise _SCHEMA.error(field=f"history {name}", column=item.column)
            relation = {"kind": "table", "name": item.table}
            ranged = item.valid_from is not None
            sources[name] = {
                "relation": relation,
                "time": item.valid_from if ranged else item.changed_at,
                "units": {unit: item.key},
                "events": [],
                "properties": [name],
                "dimensions": [name] if ranged else [],
            }
            properties[name] = {
                "source": name,
                "column": item.column,
                "dtype": _label_dtype(columns[item.column], source=name, column=item.column),
                "as_of": "event_time",
                "dimension": name if ranged else None,
            }
            if ranged:
                dimensions[name] = {
                    "relation": relation,
                    "unit": unit,
                    "key": item.key,
                    "validity": {"valid_from": item.valid_from, "valid_to": item.valid_to},
                    "properties": [name],
                }
        catalog = {
            "schema_version": 2,
            "dialect": dialect,
            "day_boundary": day_boundary,
            "sources": sources,
            "events": catalog_events,
            "properties": properties,
            "dimensions": dimensions,
            "measures": measures,
            "metrics": metrics,
        }
        return cls._from_catalog(
            validate_catalog(catalog),
            con=con,
            units=dict(units),
            readiness=coverage,
            event_ids=event_ids,
            engine=engine,
        )

    def to_catalog(self) -> str:
        """Return this model's event, metric and history definitions as catalog YAML.

        Example: model.to_catalog()
        """
        return dump_catalog(self.catalog)

    @classmethod
    @metadata_operation()
    def from_catalog(
        cls,
        path_or_catalog: str | Path | Catalog | Mapping[str, Any],
        *,
        con: Any,
        readiness: Readiness | Mapping[str, Coverage | BoundCoverage],
        units: Mapping[str, UnitSource] | None = None,
        event_ids: Mapping[str, str] | None = None,
        input_binding: VerifiedInputBinding | None = None,
        allow_unverified: bool = False,
    ) -> Model:
        """Bind catalog definitions to a connection, unit tables and complete event times.

        Unverified connections must have no pending writes during schema inspection.

        Example: tr.Model.from_catalog(model.catalog, con=model.con, **bindings)
        """
        if not isinstance(allow_unverified, bool):
            raise _INVALID.error(reason="wrong_type", field="allow_unverified")
        catalog = (
            path_or_catalog
            if isinstance(path_or_catalog, Catalog)
            else validate_catalog(path_or_catalog)
            if isinstance(path_or_catalog, Mapping)
            else load_catalog(path_or_catalog)
        )
        engine = resolve_engine(con, allow_unverified=allow_unverified)
        return cls._from_catalog(
            catalog,
            con=con,
            readiness=readiness,
            units=units,
            event_ids=event_ids,
            input_binding=input_binding,
            engine=engine,
        )

    @classmethod
    @metadata_operation()
    def _from_catalog(
        cls,
        catalog: Catalog,
        *,
        con: Any,
        readiness: Readiness | Mapping[str, Coverage | BoundCoverage],
        engine: Engine,
        units: Mapping[str, UnitSource] | None = None,
        event_ids: Mapping[str, str] | None = None,
        input_binding: VerifiedInputBinding | None = None,
    ) -> Model:
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
                    raise _INVALID.error(reason="wrong_type", field="readiness", name=str(name))
            bound_readiness = Readiness(bound)
        else:
            raise _INVALID.error(reason="wrong_type", field="readiness")
        if units is not None and not isinstance(units, Mapping):
            raise _INVALID.error(reason="wrong_type", field="units")
        unit_map = dict(units or {})
        if event_ids is not None and not isinstance(event_ids, Mapping):
            raise _INVALID.error(reason="wrong_type", field="event_ids")
        event_map = dict(event_ids or {})
        if any(
            not isinstance(key, str) or not isinstance(value, UnitSource)
            for key, value in unit_map.items()
        ):
            raise _INVALID.error(reason="wrong_type", field="units")
        in_memory = tuple(
            sorted(
                f"units:{name}"
                for name, unit in unit_map.items()
                if not isinstance(unit.table, str)
            )
        )
        if in_memory:
            raise _IN_MEMORY_WITH_CONNECTION.error(
                reason="in_memory_input_with_connection", inputs=in_memory
            )
        if any(
            not isinstance(key, str) or not isinstance(value, str) or not value
            for key, value in event_map.items()
        ):
            raise _INVALID.error(reason="wrong_type", field="event_ids")
        from terrine.execution.inputs import bind_resolver

        resolver = bind_resolver(
            catalog, con, units=unit_map, input_binding=input_binding, engine=engine
        )
        return cls._bind(
            catalog,
            con=con,
            readiness=bound_readiness,
            units=unit_map,
            event_ids=event_map,
            resolver=resolver,
            input_binding=input_binding,
            engine=engine,
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
        engine: Engine,
        source_names: set[str] | None = None,
    ) -> Model:
        selected_sources = (
            catalog.sources
            if source_names is None
            else {name: catalog.sources[name] for name in sorted(source_names)}
        )
        sources = {
            name: normalize_source(con, catalog, name, resolver=resolver, engine=engine)
            for name in selected_sources
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
                raise _INVALID.error(reason="undeclared_name", field="event_ids", name=source_name)
            if column not in resolver(source.relation).schema().names:
                raise _INVALID.error(reason="undeclared_name", field="event_ids", name=column)
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
            engine=engine,
        )

    def _relation(self, relation: RelationSpec) -> Any:
        return self._resolver(relation)

    @metadata_operation()
    def _for_execution(
        self,
        context: ExecutionContext,
        *,
        source_names: set[str] | None = None,
        unit_name: str | None = None,
    ) -> Model:
        selected_sources = set(self.catalog.sources) if source_names is None else source_names
        units = (
            self.units
            if unit_name is None
            else ({unit_name: self.units[unit_name]} if unit_name in self.units else {})
        )
        dimension_names = {
            name for source in selected_sources for name in self.catalog.sources[source].dimensions
        }
        relations = (
            *(self.catalog.sources[name].relation for name in sorted(selected_sources)),
            *(self.catalog.dimensions[name].relation for name in sorted(dimension_names)),
            *(TableRef(kind="table", name=unit.table) for unit in units.values()),
        )
        for relation in relations:
            declared = self._relation(relation).schema()
            retained = context.resolve(relation).schema()
            compatible = declared.names == retained.names and all(
                declared[name].copy(nullable=True) == retained[name].copy(nullable=True)
                for name in declared.names
            )
            if not compatible:
                raise _SCHEMA.error(field="execution schema", column=repr(relation))
        return self._bind(
            self.catalog,
            con=context.con,
            readiness=self.readiness,
            units=units,
            event_ids={
                name: column for name, column in self.event_ids.items() if name in selected_sources
            },
            resolver=context.resolve,
            input_binding=self.input_binding,
            source_names=selected_sources,
            engine=self._engine,
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
        allow_unverified: bool = False,
    ) -> Model:
        """Bind published Increment definitions as a model with units and complete event times.

        Unverified connections must have no pending writes during schema inspection.

        Example: tr.Model.from_increment("semantics.yaml", con=model.con, **bindings)
        """
        if not isinstance(allow_unverified, bool):
            raise _INVALID.error(reason="wrong_type", field="allow_unverified")
        from terrine.adapters.increment import _load_semantics, to_catalog
        from terrine.execution.backend import backend_provider

        _load_semantics()
        engine = resolve_engine(con, allow_unverified=allow_unverified)

        return cls._from_catalog(
            to_catalog(
                path_or_definitions, dialect=backend_provider(con, engine=engine).dialect(con)
            ),
            con=con,
            readiness=readiness,
            units=units,
            event_ids=event_ids,
            input_binding=input_binding,
            engine=engine,
        )
