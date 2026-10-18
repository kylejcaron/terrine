from terrine.runtime.api import (
    Executor,
    RunReceipt,
    cancel_run,
    find_run_by_request_key,
    get_run,
    open_published_generation,
    plan_refresh,
    read_run_events,
    run_plan,
    run_serialized_plan,
)
from terrine.runtime.events import EventPage, RunEvent
from terrine.runtime.graph import DependencyGraph, dependency_graph
from terrine.runtime.owner import RuntimeOwner, SnapshotRetention
from terrine.runtime.recipe import (
    DestinationRef,
    ModelReference,
    ModelResolver,
    SavedPlanRecipe,
    resolve_plan,
)
from terrine.runtime.snapshot import ReopenableSnapshot, SnapshotAuthority
from terrine.runtime.state import (
    DefinitionVersion,
    GenerationRecord,
    GenerationRef,
    LayerNode,
    RuntimeStore,
    SourceSnapshot,
)
from terrine.runtime.status import CancelReceipt, RunStatus


def __getattr__(name: str):
    if name == "translate_persisted_layers":
        from terrine.runtime.layers import translate_persisted_layers

        return translate_persisted_layers
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CancelReceipt",
    "DefinitionVersion",
    "DependencyGraph",
    "DestinationRef",
    "EventPage",
    "Executor",
    "GenerationRecord",
    "GenerationRef",
    "LayerNode",
    "ModelReference",
    "ModelResolver",
    "ReopenableSnapshot",
    "RunEvent",
    "RunReceipt",
    "RunStatus",
    "RuntimeOwner",
    "RuntimeStore",
    "SavedPlanRecipe",
    "SnapshotAuthority",
    "SnapshotRetention",
    "SourceSnapshot",
    "cancel_run",
    "dependency_graph",
    "find_run_by_request_key",
    "get_run",
    "open_published_generation",
    "plan_refresh",
    "read_run_events",
    "resolve_plan",
    "run_plan",
    "run_serialized_plan",
    "translate_persisted_layers",
]
