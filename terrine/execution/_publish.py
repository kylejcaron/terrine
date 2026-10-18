"""Explicit capture of native input roots into an owned read-only artifact."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
import ibis
from sqlglot import tokenize

from terrine.data import Budget
from terrine.errors import RefusalSpec, TerrineError
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

_CAPTURE_SCAN_UNKNOWN = RefusalSpec(
    "terrine.budget.scan_unknown",
    "A strict scan-byte budget requires a verified scan bound; this backend has none. "
    "Explicitly choose non-strict scan policy after reviewing upstream work.",
)


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


def _root_sql(relation: Any, raw: Any, catalog: Catalog) -> str:
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
    source_sql = relation.sql
    tokens = tokenize(source_sql, read=catalog.dialect)
    if tokens and tokens[-1].token_type.name == "SEMICOLON":
        token = tokens[-1]
        source_sql = source_sql[: token.start] + source_sql[token.end + 1 :]
    return source_sql


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
    catalog: Catalog,
    *,
    con: Any,
    units: Mapping[str, Any] | None = None,
    budget: Budget | None = None,
    retained_path: Path | None = None,
) -> PublishedInputs:
    with _native.connection_lock(_native.raw_connection(con)):
        return _publish_locked(
            catalog,
            con=con,
            units=units,
            budget=budget,
            retained_path=retained_path,
        )


def _publish_locked(
    catalog: Catalog,
    *,
    con: Any,
    units: Mapping[str, Any] | None,
    budget: Budget | None,
    retained_path: Path | None,
) -> PublishedInputs:
    origin = _native.raw_connection(con)
    _native.verify_native_backend(origin)
    if budget is not None:
        if not isinstance(budget, Budget):
            raise TypeError("budget must be Budget or None")
        if budget.max_scan_bytes is not None and budget.strict_scan:
            raise _CAPTURE_SCAN_UNKNOWN.error()
    roots = _root_spec(catalog, units)
    if not roots:
        raise _SNAPSHOT_UNAVAILABLE.error(reason="empty Catalog has no publishable roots")
    _admit_roots(catalog, origin, units, roots)
    if retained_path is None:
        artifact_dir = tempfile.TemporaryDirectory(prefix="terrine-inputs-")
        path = os.path.join(artifact_dir.name, "inputs.duckdb")
    else:
        artifact_dir = None
        if retained_path.exists():
            raise _SNAPSHOT_UNAVAILABLE.error(reason="retained snapshot path already exists")
        retained_path.mkdir(parents=True)
        path = str(retained_path / "inputs.duckdb")
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
            sql = _root_sql(relation, origin, catalog)
            storage_name = f"input_{len(staged_identities)}"
            if retained_path is not None:
                capture_dir = retained_path
            else:
                assert artifact_dir is not None
                capture_dir = Path(artifact_dir.name)
            capture_path = str(capture_dir / f"capture_{len(staged_identities)}.parquet")
            escaped_path = capture_path.replace("'", "''")
            origin.execute(f"COPY ({sql}\n) TO '{escaped_path}' (FORMAT PARQUET)")
            writer.execute(
                f"CREATE TABLE {_quote_ident(storage_name)} "
                f"AS SELECT * FROM read_parquet('{escaped_path}')"
            )
            staged[key] = storage_name
            staged_identities[identity] = storage_name
            columns = writer.execute(
                f"SELECT * FROM {_quote_ident(storage_name)} LIMIT 0"
            ).description
            staged_facts[identity] = tuple(column[0] for column in columns)
            if identity not in handles:
                handle = RelationHandle(issuer, identity)
                handles[identity] = handle
                issuer.handles[identity] = handle
        origin.execute("COMMIT")
        writer.close()
        writer = None
        for capture_file in Path(path).parent.glob("capture_*.parquet"):
            capture_file.unlink()
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
            issuer.root_identities[key] = _identity(relation, catalog, origin)
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
        if artifact_dir is not None:
            artifact_dir.cleanup()
        elif retained_path is not None:
            shutil.rmtree(retained_path, ignore_errors=True)
        raise


def _remove_artifact(raw: Any, artifact_dir: tempfile.TemporaryDirectory[str] | None) -> None:
    try:
        with _native.connection_lock(raw):
            raw.close()
    finally:
        if artifact_dir is not None:
            artifact_dir.cleanup()
