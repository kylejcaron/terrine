"""Portable Arrow projection for downstream dataset consumers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    import pyarrow as pa

    from terrine.data import MaterializedFrame

__all__ = ["ArrowSurrogate", "to_surrogate_arrow"]

_INVALID = RefusalSpec(
    "terrine.export.invalid_surrogate",
    "Surrogate export input is invalid ({reason}).",
)
_SURROGATE_METADATA_KEY = b"terrine.surrogate_arrow.v1"


@dataclass(frozen=True, slots=True)
class ArrowSurrogate:
    """Versioned Arrow surrogate projection and portable metadata."""

    data: pa.Table
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        import pyarrow as pa

        if not isinstance(self.data, pa.Table):
            raise TypeError("data must be a pyarrow.Table")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @classmethod
    def from_arrow(cls, table: pa.Table) -> ArrowSurrogate:
        raw = (table.schema.metadata or {}).get(_SURROGATE_METADATA_KEY)
        if raw is None:
            raise ValueError("Arrow schema has no Terrine surrogate metadata")
        metadata = json.loads(raw.decode("utf-8"))
        if metadata.get("schema_version") != 1:
            raise ValueError("unsupported Terrine surrogate metadata version")
        clean = dict(table.schema.metadata or {})
        del clean[_SURROGATE_METADATA_KEY]
        return cls(table.replace_schema_metadata(clean or None), metadata)

    def to_arrow(self) -> pa.Table:
        metadata = json.dumps(
            dict(self.metadata), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        schema_metadata = dict(self.data.schema.metadata or {})
        schema_metadata[_SURROGATE_METADATA_KEY] = metadata
        return self.data.replace_schema_metadata(schema_metadata)


def to_surrogate_arrow(
    data: MaterializedFrame, *, features: tuple[str, ...], outcomes: tuple[str, ...]
) -> ArrowSurrogate:
    """Project Arrow training data with identity, missingness and provenance."""
    from terrine.data import MaterializedFrame

    if not isinstance(data, MaterializedFrame):
        raise _INVALID.error(reason="materialized_frame_required")
    table = data.table
    structural = {
        "time_to_event": ("entry_time",),
        "ltv": ("age",),
        "cohort_table": ("age", "at_risk", "observable"),
        "life_table": ("time_bin", "censored_by_reason"),
    }.get(data.provenance.shape, ())
    requested = (*features, *outcomes)
    if len(set(requested)) != len(requested):
        raise _INVALID.error(reason="duplicate_surrogate_column")
    if any(name not in table.column_names for name in requested):
        raise _INVALID.error(reason="missing_surrogate_column")
    identity = tuple(
        dict.fromkeys(
            name for name in (data.roles.unit, data.roles.anchor, data.roles.t0) if name is not None
        )
    )
    metadata_columns = tuple(
        dict.fromkeys((*requested, *data.roles.outcomes, *structural, *identity))
    )
    support = []
    for name in metadata_columns:
        meta = data.columns.get(name)
        if meta is not None:
            support.extend(
                column
                for column in (
                    meta.observed_from_column,
                    meta.observed_until_column,
                    meta.complete_column,
                    meta.reason_column,
                    meta.absolute_error_column,
                )
                if column is not None
            )
    retained = tuple(
        dict.fromkeys((*identity, *requested, *data.roles.outcomes, *structural, *support))
    )
    missing = [name for name in retained if name not in table.column_names]
    if missing:
        raise _INVALID.error(reason=f"missing_surrogate_support_{missing[0]}")
    projected = table.select(retained)
    serialized = data.to_metadata()
    role_metadata = dict(serialized["roles"])
    projected_outcomes = [
        name for name in (*outcomes, *data.roles.outcomes, *structural) if name in retained
    ]
    role_metadata["features"] = [name for name in features if name in retained]
    role_metadata["outcomes"] = list(dict.fromkeys(projected_outcomes))
    for role in ("unit", "anchor", "t0", "treatment", "weight", "cluster_unit", "prior_state"):
        if role in role_metadata and role_metadata[role] not in retained:
            role_metadata[role] = None
    metadata = {
        "schema_version": 1,
        "design_hash": data.provenance.design_hash,
        "model_hash": data.provenance.model_hash,
        "source_generations": dict(data.provenance.source_generations),
        "execution_snapshot": serialized["provenance"]["execution_snapshot"],
        "provenance": serialized["provenance"],
        "roles": role_metadata,
        "feature_columns": list(features),
        "outcome_columns": list(dict.fromkeys(projected_outcomes)),
        "support_columns": list(support),
        "columns": {
            name: value for name, value in serialized["columns"].items() if name in retained
        },
        "administrative_cutoff": (
            data.provenance.administrative_cutoff.isoformat()
            if data.provenance.administrative_cutoff is not None
            else None
        ),
        "cohort_operation": serialized["provenance"].get("cohort_operation"),
    }
    return ArrowSurrogate(projected, metadata)
