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


def __getattr__(name: str):
    if name == "translate_persisted_layers":
        from terrine.runtime.layers import translate_persisted_layers

        return translate_persisted_layers
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DefinitionVersion",
    "DependencyGraph",
    "DestinationRef",
    "GenerationRecord",
    "GenerationRef",
    "LayerNode",
    "ModelReference",
    "ModelResolver",
    "ReopenableSnapshot",
    "RuntimeOwner",
    "RuntimeStore",
    "SavedPlanRecipe",
    "SnapshotAuthority",
    "SnapshotRetention",
    "SourceSnapshot",
    "resolve_plan",
    "dependency_graph",
    "translate_persisted_layers",
]
