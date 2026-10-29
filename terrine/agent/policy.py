"""Owner-only grants and resource limits for dataset operations."""

from collections.abc import Iterable
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import Literal

from terrine.data import Budget
from terrine.errors import RefusalSpec
from terrine.runtime.api import Executor
from terrine.runtime.saved_plan import DestinationRef, ModelReference, ModelResolver
from terrine.runtime.snapshot import SnapshotAuthority

__all__ = ["AgentPolicy"]

type Permission = Literal[
    "inspect", "validate", "prepare", "write", "export", "preview", "read", "cancel"
]

_DENIED = RefusalSpec("terrine.agent.permission_denied", "Owner policy denies this operation.")
_SCOPE = RefusalSpec("terrine.agent.scope_denied", "Requested resources are outside owner policy.")
_LIMIT = RefusalSpec("terrine.agent.request_limit", "Owner tool request limit has been reached.")
_OWNER_INPUT = RefusalSpec(
    "terrine.agent.owner_configuration", "Owner policy input {field} is invalid: {reason}."
)
_REQUEST_INPUT = RefusalSpec(
    "terrine.agent.invalid_request", "Tool request input {field} is invalid: {reason}."
)


def effective_budget(owner: Budget, requested: Budget | None) -> Budget:
    """Intersect optional nonnegative limits; None denotes no numeric bound."""
    if not isinstance(owner, Budget):
        raise _OWNER_INPUT.error(reason="wrong_type", field="budget")
    if requested is not None and not isinstance(requested, Budget):
        raise _REQUEST_INPUT.error(reason="wrong_type", field="budget")
    if requested is None:
        return owner
    values = {}
    for name in ("max_rows", "max_output_bytes", "max_scan_bytes"):
        limits = [
            value for value in (getattr(owner, name), getattr(requested, name)) if value is not None
        ]
        values[name] = min(limits) if limits else None
    return Budget(**values, strict_scan=owner.strict_scan or requested.strict_scan)


def _collection(value: object, field: str) -> frozenset:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise _OWNER_INPUT.error(reason="wrong_type", field=field)
    return frozenset(value)


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentPolicy:
    """Trusted configuration, never deserialized from a tool request.

    Source identities are Catalog source names, destinations are exact owner
    references, and catalogs use the owner ModelReference's catalog hash.
    """

    inspect: bool = True
    validate: bool = True
    prepare: bool = False
    write: bool = False
    export: bool = False
    preview: bool = False
    read: bool = False
    cancel: bool = False
    allowed_sources: frozenset[str] = frozenset()
    allowed_catalogs: frozenset[str] = frozenset()
    allowed_destinations: frozenset[DestinationRef] = frozenset()
    allowed_exports: frozenset[Path] = frozenset()
    design_root: Path | None = None
    budget: Budget = field(default_factory=Budget)
    max_run_seconds: int = 300
    max_concurrency: int = 1
    max_requests: int = 100
    max_preview_rows: int = 20
    max_bounded_count: int = 1000
    batch_cap: int = 100
    preview_columns: frozenset[str] = frozenset()
    redacted_columns: frozenset[str] = frozenset()
    model_reference: ModelReference | None = None
    resolver: ModelResolver | None = field(default=None, repr=False, compare=False)
    snapshot_authority: SnapshotAuthority | None = field(default=None, repr=False, compare=False)
    executor: Executor | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in (
            "inspect",
            "validate",
            "prepare",
            "write",
            "export",
            "preview",
            "read",
            "cancel",
        ):
            if not isinstance(getattr(self, name), bool):
                raise _OWNER_INPUT.error(reason="wrong_type", field=name)
        for name in (
            "max_run_seconds",
            "max_concurrency",
            "max_requests",
            "max_preview_rows",
            "max_bounded_count",
            "batch_cap",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise _OWNER_INPUT.error(reason="not_positive_integer", field=name)
        for name in ("allowed_sources", "allowed_catalogs", "preview_columns", "redacted_columns"):
            value = _collection(getattr(self, name), name)
            if any(not isinstance(item, str) or not item for item in value):
                raise _OWNER_INPUT.error(reason="empty_name", field=name)
            object.__setattr__(self, name, value)
        destinations = _collection(self.allowed_destinations, "allowed_destinations")
        if any(not isinstance(item, DestinationRef) for item in destinations):
            raise _OWNER_INPUT.error(reason="wrong_type", field="allowed_destinations")
        object.__setattr__(self, "allowed_destinations", destinations)
        exports = _collection(self.allowed_exports, "allowed_exports")
        if any(not isinstance(path, (str, PathLike)) for path in exports):
            raise _OWNER_INPUT.error(reason="wrong_type", field="allowed_exports")
        object.__setattr__(
            self,
            "allowed_exports",
            frozenset(Path(path).expanduser().resolve() for path in exports),
        )
        if not isinstance(self.budget, Budget):
            raise _OWNER_INPUT.error(reason="wrong_type", field="budget")
        if self.design_root is not None:
            if not isinstance(self.design_root, (str, PathLike)):
                raise _OWNER_INPUT.error(reason="wrong_type", field="design_root")
            object.__setattr__(self, "design_root", Path(self.design_root).resolve())

    def authorize(
        self,
        permission: Permission,
        *,
        sources: tuple[str, ...] = (),
        catalog: str | None = None,
        destination: DestinationRef | None = None,
    ) -> None:
        if permission not in {
            "inspect",
            "validate",
            "prepare",
            "write",
            "export",
            "preview",
            "read",
            "cancel",
        } or not getattr(self, permission):
            raise _DENIED.error(
                operation=permission, route="Ask the owner to grant this operation."
            )
        if (
            not set(sources) <= self.allowed_sources
            or (catalog is not None and catalog not in self.allowed_catalogs)
            or (destination is not None and destination not in self.allowed_destinations)
        ):
            raise _SCOPE.error(
                operation=permission, route="Use owner-approved sources and destinations."
            )
