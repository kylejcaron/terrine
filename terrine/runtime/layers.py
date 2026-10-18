from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

import ibis

from terrine.compile.layers import LayerKey, plan_layers
from terrine.model import Model
from terrine.runtime.backends.base import PublicationReceipt
from terrine.runtime.backends.duckdb import DuckDBBackend
from terrine.runtime.state import GenerationRef, RuntimeStore
from terrine.spec.design import Design

__all__ = ["translate_persisted_layers"]


def translate_persisted_layers(
    store: RuntimeStore,
    backend: DuckDBBackend,
    *,
    design: Design,
    model: Model,
    ref: GenerationRef,
    receipt: PublicationReceipt,
    partitions: Mapping[LayerKey, str],
) -> Mapping[LayerKey, ibis.Table]:
    """Resolve committed physical partitions to compiler-owned logical identities.

    The caller supplies the durable partition-to-layer index; this boundary verifies
    its generation, definition, and source-snapshot binding before handing relations
    to the compiler. SQL names are resolved only through the backend partition catalog.
    """
    if not isinstance(store, RuntimeStore) or not isinstance(backend, DuckDBBackend):
        raise TypeError("store and backend must be runtime-owned DuckDB adapters")
    if ref.namespace != store.namespace or backend.namespace != store.namespace:
        raise ValueError("generation, runtime store, and backend namespaces differ")
    if (receipt.dataset, receipt.generation) != (ref.dataset, ref.generation):
        raise ValueError("publication receipt does not identify the requested generation")
    generation = store.read_generation(ref)
    if generation is None:
        raise ValueError("physical layers refer to an absent runtime generation")
    if generation.definition.definition_hash != design.spec_hash:
        raise ValueError("runtime generation does not match the requested design")
    expected = set(plan_layers(design, model).keys)
    available = set(receipt.partitions)
    result: dict[LayerKey, ibis.Table] = {}
    for key, partition in partitions.items():
        if (
            replace(key, snapshot_id="unbound") not in expected
            or key.snapshot_id != generation.snapshot.snapshot_id
            or not partition
        ):
            raise ValueError("logical layer identity does not match persisted generation")
        if partition not in available:
            raise ValueError("logical layer refers to a partition outside the publication receipt")
        if key in result:
            raise ValueError("duplicate logical layer key")
        result[key] = backend.read_relation(receipt, partition)
    return MappingProxyType(result)
