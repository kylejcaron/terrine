"""Explicit capture of native input roots into an owned read-only artifact."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Mapping
from typing import Any

import duckdb
import ibis

from terrine.errors import TerrineError
from terrine.execution import _native
from terrine.execution.inputs import (
    _SNAPSHOT_UNAVAILABLE,
    PhysicalRelationId,
    PublishedInputs,
    RelationHandle,
    _BackingFact,
    _Issuer,
    _register_published_backend,
)
from terrine.semantic.models import Catalog, SqlQuery, TableRef


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _root_spec(catalog: Catalog, units: Mapping[str, Any] | None):
    roots = [("source", name, source.relation) for name, source in catalog.sources.items()]
    roots.extend(
        ("dimension", name, dimension.relation) for name, dimension in catalog.dimensions.items()
    )
    roots.extend(
        ("roster", name, TableRef(kind="table", name=unit.table))
        for name, unit in (units or {}).items()
    )
    return roots


def _root_sql(relation: Any, raw: Any) -> str:
    if isinstance(relation, TableRef):
        obj = _native._match(
            relation,
            _native.native_objects(raw),
            _native._current_namespace(raw),
            allow_temporary=True,
        )
        return "SELECT * FROM " + ".".join(
            _quote_ident(component) for component in (obj.catalog, obj.schema, obj.name)
        )
    return relation.sql


def _identity(relation: Any, catalog: Catalog, raw: Any) -> PhysicalRelationId:
    if isinstance(relation, SqlQuery):
        fingerprint = hashlib.sha256(relation.sql.encode()).hexdigest()
        return PhysicalRelationId(
            "sql_result", None, None, f"result_{fingerprint[:20]}", fingerprint
        )
    obj = _native._match(
        relation,
        _native.native_objects(raw),
        _native._current_namespace(raw),
        allow_temporary=True,
    )
    if obj.kind == "table":
        return PhysicalRelationId("table", obj.catalog, obj.schema, obj.name)
    fingerprint = hashlib.sha256((obj.definition or "").encode()).hexdigest()
    return PhysicalRelationId("sql_result", obj.catalog, obj.schema, obj.name, fingerprint)


def _admit_roots(
    catalog: Catalog,
    raw: Any,
    units: Mapping[str, Any] | None,
    roots: list[tuple[str, str, Any]],
) -> None:
    try:
        _native.required_relations(catalog, raw, units=units, _include_temporary=True)
    except TerrineError as exc:
        distinct_roots = {
            (
                relation.kind,
                relation.name if isinstance(relation, TableRef) else relation.sql,
            ): relation
            for _, _, relation in roots
        }
        if len(distinct_roots) != 1:
            raise _SNAPSHOT_UNAVAILABLE.error(
                reason="multiple opaque roots require one owner-staged coherent result"
            ) from exc
        try:
            _native.admit_capture_root(next(iter(distinct_roots.values())), catalog, raw)
        except TerrineError as capture_exc:
            raise _SNAPSHOT_UNAVAILABLE.error(
                reason="opaque result lacks a verified read-only capture contract"
            ) from capture_exc


def publish(
    catalog: Catalog, *, con: Any, units: Mapping[str, Any] | None = None
) -> PublishedInputs:
    with _native.connection_lock(_native.raw_connection(con)):
        return _publish_locked(catalog, con=con, units=units)


def _publish_locked(
    catalog: Catalog, *, con: Any, units: Mapping[str, Any] | None
) -> PublishedInputs:
    origin = _native.raw_connection(con)
    _native.verify_native_backend(origin)
    roots = _root_spec(catalog, units)
    if not roots:
        raise _SNAPSHOT_UNAVAILABLE.error(reason="empty Catalog has no publishable roots")
    _admit_roots(catalog, origin, units, roots)
    artifact_dir = tempfile.TemporaryDirectory(prefix="terrine-inputs-")
    path = os.path.join(artifact_dir.name, "inputs.duckdb")
    writer = None
    raw = None
    try:
        writer = duckdb.connect(path)
        origin.execute("BEGIN TRANSACTION")
        _admit_roots(catalog, origin, units, roots)
        handles: dict[PhysicalRelationId, RelationHandle] = {}
        issuer = _Issuer(None, None)
        from terrine.execution.inputs import _catalog_hash, _units_hash

        issuer.catalog_hash = _catalog_hash(catalog)
        issuer.units_hash = _units_hash(units)
        staged: dict[tuple[str, str], str] = {}
        staged_identities: dict[PhysicalRelationId, str] = {}
        staged_facts: dict[PhysicalRelationId, tuple[str, ...]] = {}
        for _namespace, _name, relation in roots:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            if key in staged:
                continue
            identity = _identity(relation, catalog, origin)
            if identity in staged_identities:
                staged[key] = staged_identities[identity]
                continue
            sql = _root_sql(relation, origin)
            arrow = origin.execute(sql).fetch_arrow_table()
            storage_name = f"input_{len(staged_identities)}"
            writer.register("__terrine_arrow", arrow)
            writer.execute(
                f"CREATE TABLE {_quote_ident(storage_name)} AS SELECT * FROM __terrine_arrow"
            )
            writer.unregister("__terrine_arrow")
            staged[key] = storage_name
            staged_identities[identity] = storage_name
            staged_facts[identity] = tuple(arrow.schema.names)
            if identity not in handles:
                handle = RelationHandle(issuer, identity)
                handles[identity] = handle
                issuer.handles[identity] = handle
        origin.execute("COMMIT")
        writer.close()
        writer = None
        raw = duckdb.connect(path, read_only=True)
        managed = ibis.duckdb.from_connection(raw)
        issuer.con = managed
        _register_published_backend(managed, issuer)
        issuer.raw = raw
        issuer.namespace = _native._current_namespace(raw)
        issuer.function_fingerprint = _native.function_fingerprint(raw)
        backing = _native.native_objects(raw)
        for identity, storage_name in staged_identities.items():
            matches = [
                obj
                for obj in backing
                if obj.kind == "table" and obj.name.casefold() == storage_name.casefold()
            ]
            if len(matches) != 1:
                raise _SNAPSHOT_UNAVAILABLE.error(
                    reason="published backing table identity is not unique"
                )
            obj = matches[0]
            columns = staged_facts[identity]
            issuer.facts[identity] = _BackingFact(
                obj.catalog, obj.schema, obj.name, columns, obj.oid
            )
        for _namespace, _name, relation in roots:
            key = (relation.kind, relation.name if isinstance(relation, TableRef) else relation.sql)
            storage_name = staged[key]
            issuer.root_names[key] = storage_name
        issuer.cleanup = lambda: _remove_artifact(raw, artifact_dir)
        return PublishedInputs(managed, handles, issuer)
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if raw is not None:
            try:
                raw.close()
            except Exception:
                pass
        try:
            origin.execute("ROLLBACK")
        except Exception:
            pass
        artifact_dir.cleanup()
        raise


def _remove_artifact(raw: Any, artifact_dir: tempfile.TemporaryDirectory[str]) -> None:
    try:
        with _native.connection_lock(raw):
            raw.close()
    finally:
        artifact_dir.cleanup()
