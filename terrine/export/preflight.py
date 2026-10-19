"""Safety checks shared by downstream dataset hand-offs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

import pyarrow as pa

from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    from terrine.data import MaterializedFrame

__all__ = ["ExportPreflight", "preflight_export"]

_INCOMPLETE_OUTCOME = RefusalSpec(
    "terrine.export.incomplete_outcome",
    "Outcome windows are incomplete; provide complete data or explicitly drop incomplete rows.",
)
_UNKNOWN_ELIGIBILITY = RefusalSpec(
    "terrine.export.unknown_eligibility",
    "Eligibility is unknown for some candidate rows; resolve eligibility before export.",
)
_CLUSTER_REQUIRED = RefusalSpec(
    "terrine.export.cluster_required",
    "Repeated rows per independent unit require a cluster column.",
)
_GRACE_BASELINE_TREATMENT = RefusalSpec(
    "terrine.export.grace_baseline_treatment",
    "Grace-defined treatment cannot be exported as a baseline treatment.",
)


@dataclass(frozen=True, slots=True)
class ExportPreflight:
    """Filtered export rows and exact incomplete-outcome exclusion counts."""

    table: pa.Table
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


def preflight_export(
    data: MaterializedFrame, *, drop_incomplete: bool = False
) -> ExportPreflight:
    """Check shared hand-off hazards without imputing or zero-filling outcomes."""
    from terrine.data import MaterializedFrame

    if not isinstance(data, MaterializedFrame):
        raise TypeError("data must be a MaterializedFrame")
    provenance = data.provenance
    if provenance.treatment_design == "grace_initiation":
        raise _GRACE_BASELINE_TREATMENT.error()

    summary = provenance.eligibility_summary
    if (provenance.eligibility_cutoffs and summary is None) or (
        summary is not None and summary.unknown
    ):
        raise _UNKNOWN_ELIGIBILITY.error(
            unknown=summary.unknown if summary is not None else None,
            candidates=summary.candidate if summary is not None else None,
        )

    roles = data.roles
    if roles.cluster_unit is not None and roles.cluster_unit not in data.table.column_names:
        raise _CLUSTER_REQUIRED.error()
    if roles.unit in data.table.column_names:
        units = data.table[roles.unit].to_pylist()
        if len(set(units)) != len(units) and roles.cluster_unit is None:
            raise _CLUSTER_REQUIRED.error()
    excluded: dict[str, int] = {}
    excluded_rows: set[int] = set()
    for outcome in roles.outcomes:
        meta = data.columns.get(outcome)
        complete_name = None if meta is None else meta.complete_column
        reason_name = None if meta is None else meta.reason_column
        if complete_name is None or complete_name not in data.table.column_names:
            incomplete = list(range(data.table.num_rows))
            reasons = ["completeness_evidence_missing"] * len(incomplete)
        else:
            complete = data.table[complete_name].to_pylist()
            incomplete = [i for i, value in enumerate(complete) if value is not True]
            if reason_name is not None and reason_name in data.table.column_names:
                reason_values = data.table[reason_name].to_pylist()
                reasons = [
                    reason_values[i]
                    if isinstance(reason_values[i], str) and reason_values[i]
                    else "reason_unavailable"
                    for i in incomplete
                ]
            else:
                reasons = ["reason_unavailable"] * len(incomplete)
        if incomplete and not drop_incomplete:
            for reason in reasons:
                excluded[reason] = excluded.get(reason, 0) + 1
            raise _INCOMPLETE_OUTCOME.error(outcome=outcome, excluded_counts=excluded)
        for index, reason in zip(incomplete, reasons, strict=True):
            excluded_rows.add(index)
            excluded[reason] = excluded.get(reason, 0) + 1

    if excluded_rows:
        keep = [i not in excluded_rows for i in range(data.table.num_rows)]
        table = data.table.filter(pa.array(keep, type=pa.bool_()))
    else:
        table = data.table
    return ExportPreflight(table, excluded)
