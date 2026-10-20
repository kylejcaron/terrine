"""Immutable source inputs with fresh database bindings for each test."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

import ibis
import pyarrow as pa

from terrine.data import BoundCoverage, Coverage, UnitSource
from terrine.model import Model
from terrine.semantic.models import Catalog


@dataclass(frozen=True, slots=True)
class WorldInputs:
    tables: Mapping[str, pa.Table]
    catalog: Catalog
    units: Mapping[str, UnitSource]
    readiness: Mapping[str, Coverage | BoundCoverage]

    def __post_init__(self) -> None:
        for name in ("tables", "units", "readiness"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))


def events_world() -> WorldInputs:
    events = pa.table(
        {
            "user_id": ["a", "b"],
            "ts": [datetime(2025, 1, 3, tzinfo=UTC)] * 2,
            "event_id": ["a1", "b1"],
            "alternate_event_id": ["a2", "b2"],
        }
    )
    catalog = Catalog.model_validate(
        {
            "schema_version": 1,
            "day_boundary": "UTC",
            "dialect": "duckdb",
            "sources": {
                name: {
                    "relation": {"kind": "table", "name": name},
                    "timestamp": "ts",
                    "entities": {"user": "user_id"},
                    "facts": [fact],
                    "properties": [],
                    "dimensions": [],
                }
                for name, fact in (("events", "session"), ("labels", "churn"))
            },
            "facts": {
                "session": {"source": "events", "column": None},
                "churn": {"source": "labels", "column": None},
            },
            "properties": {},
            "dimensions": {},
            "measures": {
                "sessions_count": {"fact": "session", "aggregation": "count"},
                "churn_count": {"fact": "churn", "aggregation": "count"},
            },
            "metrics": {
                "sessions": {"kind": "mean", "entity": "user", "measure": "sessions_count"},
                "churned": {"kind": "conversion", "entity": "user", "measure": "churn_count"},
            },
        }
    )
    return WorldInputs(
        tables={
            "events": events,
            "labels": events,
            "users": pa.table({"user_id": ["a", "b"], "alt_id": ["aa", "bb"]}),
        },
        catalog=catalog,
        units={"user": UnitSource("users", "user_id")},
        readiness={
            "events": Coverage("2025-01-01", "2025-01-31"),
            "labels": Coverage("2025-01-01", "2025-01-31"),
        },
    )


def _world_from_model(model: Model, names: tuple[str, ...]) -> WorldInputs:
    return WorldInputs(
        tables={name: model.con.to_pyarrow(model.con.table(name)) for name in names},
        catalog=model.catalog,
        units=model.units,
        readiness=model.readiness.sources,
    )


def winsorization_world() -> WorldInputs:
    from tests.fixtures.winsorization import create_model

    con = ibis.duckdb.connect()
    try:
        return _world_from_model(create_model(con), ("users", "events", "labels"))
    finally:
        con.disconnect()


def interop_world() -> WorldInputs:
    from tests.fixtures.interop import interop_frame

    con = ibis.duckdb.connect()
    try:
        return _world_from_model(
            interop_frame(con, full=False)._model, ("events", "users", "accounts")
        )
    finally:
        con.disconnect()


def bind_model(world: WorldInputs, tmp_path: Path) -> Model:
    con = ibis.duckdb.connect(str(tmp_path / "study.duckdb"))
    try:
        for name, table in world.tables.items():
            con.create_table(name, table)
        return Model.from_catalog(
            world.catalog, con=con, units=world.units, readiness=world.readiness
        )
    except BaseException:
        con.disconnect()
        raise
