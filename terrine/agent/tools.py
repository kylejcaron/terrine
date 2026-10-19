"""Metadata-only discovery and safe typed Design editing."""

from __future__ import annotations

import json
import os
import re
import secrets
import threading
from collections.abc import Callable
from datetime import date
from typing import Any

from terrine.agent.design_schema import design_schema
from terrine.agent.policy import _LIMIT, AgentPolicy, Permission
from terrine.agent.schemas import (
    CapabilityOutput,
    CatalogOutput,
    DesignOutput,
    DesignRequest,
    MetricMetadata,
    PropertyMetadata,
    Response,
    SchemaOutput,
    SourceMetadata,
    ValidationOutput,
    refusal_response,
)
from terrine.errors import RefusalSpec, TerrineError
from terrine.model import Model
from terrine.runtime.api import Executor
from terrine.runtime.recipe import ModelReference
from terrine.spec.anchors import EventAnchor, FixedDate
from terrine.spec.design import Design
from terrine.spec.io import from_json, from_yaml, to_json
from terrine.spec.validate import ensure_runtime_shape_supported, validate_design

__all__ = ["DatasetTools"]

_INVALID = RefusalSpec("terrine.agent.invalid_request", "Use a valid typed Design payload.")
_STORAGE = RefusalSpec("terrine.agent.storage_denied", "Named Design storage is unavailable.")
_MODEL = RefusalSpec("terrine.agent.model_mismatch", "Owner model configuration has changed.")


def _diff(before: Any, after: Any, path: str = "") -> tuple[str, ...]:
    if isinstance(before, dict) and isinstance(after, dict):
        paths = []
        for key in sorted(before.keys() | after.keys()):
            escaped = key.replace("~", "~0").replace("/", "~1")
            child = f"{path}/{escaped}"
            if key not in before or key not in after:
                paths.append(child)
            else:
                paths.extend(_diff(before[key], after[key], child))
        return tuple(paths)
    return () if before == after else (path,)


class DatasetTools:
    """An owner-bound session with a shared request cap across all operations."""

    def __init__(
        self, model: Model, policy: AgentPolicy, *, namespace: str, executor: Executor | None = None
    ) -> None:
        if not isinstance(model, Model) or not isinstance(policy, AgentPolicy):
            raise TypeError("model and policy must be owner-created typed values")
        if not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
            raise ValueError("namespace must be a simple identifier")
        if executor is not None and executor is not policy.executor:
            raise TypeError("executor must be the owner policy executor")
        self.model = model
        self.policy = policy
        self.namespace = namespace
        self._requests = 0
        self._lock = threading.RLock()

    def _call[T](self, permission: Permission, operation: Callable[[], T]) -> Response[T]:
        try:
            with self._lock:
                if self._requests >= self.policy.max_requests:
                    raise _LIMIT.error()
                self._requests += 1
                self.policy.authorize(permission)
                reference = self.policy.model_reference
                if (
                    reference is not None
                    and ModelReference.from_model(self.model, owner_key=reference.owner_key)
                    != reference
                ):
                    raise _MODEL.error()
                catalog = (
                    reference.catalog_hash
                    if reference
                    else ModelReference.from_model(self.model, owner_key="inspection").catalog_hash
                )
                self.policy.authorize(permission, catalog=catalog)
            return Response[T](status="ok", output=operation())
        except TerrineError as error:
            return refusal_response(error)

    def _parse(self, serialized: str) -> Design:
        try:
            DesignRequest(serialized=serialized)
            return (
                from_json(serialized)
                if serialized.lstrip().startswith("{")
                else from_yaml(serialized)
            )
        except TerrineError:
            raise
        except (TypeError, ValueError) as error:
            raise _INVALID.error(
                route="Use get_design_schema and design_example to construct a Design."
            ) from error

    def _validated(self, design: Design) -> ValidationOutput:
        report = validate_design(design, self.model)
        self.policy.authorize("validate", sources=report.required_sources)
        return ValidationOutput(
            spec_hash=design.spec_hash,
            required_sources=report.required_sources,
            runtime_obligations=report.runtime_obligations,
        )

    def _design_output(self, serialized: str, base: str | None = None) -> DesignOutput:
        design = self._parse(serialized)
        valid = self._validated(design)
        canonical = to_json(design)
        paths = (
            ()
            if base is None
            else _diff(json.loads(to_json(self._parse(base))), json.loads(canonical))
        )
        return DesignOutput(**valid.model_dump(), serialized=canonical, changed_paths=paths)

    def inspect_catalog(self) -> Response[CatalogOutput]:
        def inspect():
            allowed = self.policy.allowed_sources
            return CatalogOutput(
                sources=tuple(
                    SourceMetadata(
                        name=name,
                        entities=tuple(sorted(source.entities)),
                        facts=tuple(sorted(source.facts)),
                        properties=tuple(sorted(source.properties)),
                    )
                    for name, source in sorted(self.model.sources.items())
                    if name in allowed
                ),
                metrics=tuple(
                    MetricMetadata(
                        name=name,
                        entity=metric.entity,
                        kind=metric.kind,
                        sources=metric.source_names,
                    )
                    for name, metric in sorted(self.model.metrics.items())
                    if set(metric.source_names) <= allowed
                ),
                properties=tuple(
                    PropertyMetadata(
                        name=name, source=prop.source, dtype=prop.dtype, as_of=prop.as_of
                    )
                    for name, prop in sorted(self.model.catalog.properties.items())
                    if prop.source in allowed
                ),
                units=tuple(
                    sorted(
                        {
                            unit
                            for name, source in self.model.sources.items()
                            if name in allowed
                            for unit in source.entities
                        }
                    )
                ),
            )

        return self._call("inspect", inspect)

    def list_capabilities(self) -> Response[CapabilityOutput]:
        def capabilities():
            shapes = []
            for shape in ("cross_section", "panel", "sequence", "trajectory"):
                try:
                    ensure_runtime_shape_supported(shape)
                except TerrineError:
                    continue
                shapes.append(shape)
            from terrine.spec.state_panel import StateObservations, StatePanel

            ensure_runtime_shape_supported(
                StatePanel(StateObservations("state", ("a", "b")), max_periods=2)
            )
            shapes.append("StatePanel")
            return CapabilityOutput(
                operations=(
                    "inspect_catalog",
                    "list_capabilities",
                    "get_design_schema",
                    "design_example",
                    "propose_design",
                    "validate_design",
                    "save_design",
                    "load_design",
                ),
                runtime_shapes=tuple(shapes),
            )

        return self._call("inspect", capabilities)

    def get_design_schema(self) -> Response[SchemaOutput]:
        return self._call("inspect", lambda: SchemaOutput(document=design_schema()))

    def design_example(self) -> Response[DesignOutput]:
        def example():
            sources = [
                (name, source)
                for name, source in sorted(self.model.sources.items())
                if name in self.policy.allowed_sources and source.entities
            ]
            for name, source in sources:
                for unit in sorted(source.entities):
                    if unit in self.model.units:
                        coverage = self.model.readiness.sources.get(name)
                        anchor = coverage.start.date() if coverage else date(2000, 1, 1)
                        return self._design_output(to_json(Design(unit, FixedDate(anchor))))
            for name, source in sources:
                if source.facts and name in self.model.readiness.sources:
                    design = Design(
                        sorted(source.entities)[0], EventAnchor(sorted(source.facts)[0])
                    )
                    return self._design_output(to_json(design))
            raise _INVALID.error(
                route="Configure a unit roster or allow an event fact with declared coverage."
            )

        return self._call("validate", example)

    def propose_design(self, serialized: str, *, base: str | None = None) -> Response[DesignOutput]:
        return self._call("validate", lambda: self._design_output(serialized, base))

    def validate_design(self, serialized: str) -> Response[ValidationOutput]:
        return self._call("validate", lambda: self._validated(self._parse(serialized)))

    def _storage(self, name: str) -> tuple[int, str]:
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", name)
            or self.policy.design_root is None
        ):
            raise _STORAGE.error(route="Use a simple name under the owner-configured design root.")
        root = self.policy.design_root
        try:
            root.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            return descriptor, f"{name}.json"
        except OSError as error:
            raise _STORAGE.error() from error

    def save_design(self, name: str, serialized: str) -> Response[DesignOutput]:
        def save():
            output = self._design_output(serialized)
            root, filename = self._storage(name)
            temporary = f".design-{secrets.token_hex(12)}"
            try:
                try:
                    existing = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root)
                except FileNotFoundError:
                    pass
                else:
                    os.close(existing)
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=root,
                )
                with os.fdopen(descriptor, "w") as stream:
                    stream.write(output.serialized)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, filename, src_dir_fd=root, dst_dir_fd=root)
            except OSError as error:
                raise _STORAGE.error() from error
            finally:
                try:
                    os.unlink(temporary, dir_fd=root)
                except FileNotFoundError:
                    pass
                os.close(root)
            return output

        return self._call("validate", save)

    def load_design(self, name: str) -> Response[DesignOutput]:
        def load():
            root, filename = self._storage(name)
            try:
                descriptor = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root)
                with os.fdopen(descriptor) as stream:
                    serialized = stream.read()
            except OSError as error:
                raise _STORAGE.error() from error
            finally:
                os.close(root)
            return self._design_output(serialized)

        return self._call("validate", load)
