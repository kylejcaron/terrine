"""Safety checks shared by downstream dataset hand-offs."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa

from terrine.data import ColumnMeta, MaterializedFrame, Provenance, Roles
from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    import ibis.expr.types as ir
    from ibis.backends.duckdb import Backend

__all__ = ["ExportPreflight", "WarehousePreflight", "preflight_export", "preflight_warehouse"]

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
    "Repeated rows per independent unit require a valid cluster column; declare the unit cluster.",
)
_GRACE_BASELINE_TREATMENT = RefusalSpec(
    "terrine.export.grace_baseline_treatment",
    "Grace-defined treatment cannot be exported as a baseline treatment; use a baseline design.",
)
_UNSUPPORTED_WEIGHTS = RefusalSpec(
    "terrine.export.unsupported_weights",
    "{consumer} does not apply declared weights; use a downstream method supporting those weights.",
)
_REPEATED_UNITS_UNSUPPORTED = RefusalSpec(
    "terrine.export.repeated_units_unsupported",
    "{consumer} cannot handle repeated units; use one row per unit or a cluster-aware library.",
)
_SNAPSHOT_REQUIRED = RefusalSpec(
    "terrine.export.snapshot_required",
    "Warehouse export needs its own DuckDB snapshot transaction; "
    "use a DuckDB relation outside an existing transaction and fit inside the context.",
)


@dataclass(frozen=True, slots=True)
class ExportPreflight:
    """Filtered export rows and counts of excluded rows per exact reason."""

    table: pa.Table
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


@dataclass(frozen=True, slots=True)
class WarehousePreflight:
    """Snapshot-bound relation, usable only inside its preflight context."""

    table: ir.Table
    excluded_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


def _check_metadata(provenance: Provenance) -> None:
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


def _check_identity(roles: Roles, names: Sequence[str], repeated: bool) -> None:
    if roles.cluster_unit is not None and roles.cluster_unit not in names:
        raise _CLUSTER_REQUIRED.error()
    if repeated and roles.cluster_unit is None:
        raise _CLUSTER_REQUIRED.error()


def _supports(roles: Roles, columns: Mapping[str, ColumnMeta], names: Sequence[str]):
    """Return unique completeness/reason references shared by one or more outcomes."""
    supports: dict[tuple[str | None, str | None], str] = {}
    for outcome in roles.outcomes:
        meta = columns.get(outcome)
        complete = None if meta is None else meta.complete_column
        reason = None if meta is None else meta.reason_column
        supports.setdefault(
            (complete if complete in names else None, reason if reason in names else None), outcome
        )
    return supports


def _check_exclusions(excluded: Mapping[str, int], *, drop: bool) -> None:
    if excluded and not drop:
        raise _INCOMPLETE_OUTCOME.error(excluded_counts=dict(excluded))


def preflight_export(
    data: MaterializedFrame,
    *,
    drop_incomplete: bool = False,
    consumer: Literal["dowhy"] | None = None,
) -> ExportPreflight:
    """Refuse shared hazards before applying consumer-specific limitations.

    Exclusions count each row once per exact reason, even when outcomes share
    completeness evidence. A row with two distinct reasons counts under both.
    """
    if not isinstance(data, MaterializedFrame):
        raise TypeError("data must be a MaterializedFrame")
    if consumer not in (None, "dowhy"):
        raise ValueError("consumer must be None or 'dowhy'")
    _check_metadata(data.provenance)
    roles, table = data.roles, data.table
    units = table[roles.unit].to_pylist() if roles.unit in table.column_names else []
    repeated = len(set(units)) != len(units)
    _check_identity(roles, table.column_names, repeated and data.provenance.shape != "life_table")
    excluded_rows: set[int] = set()
    reason_rows: dict[str, set[int]] = defaultdict(set)
    for complete_name, reason_name in _supports(roles, data.columns, table.column_names):
        complete = table[complete_name].to_pylist() if complete_name else [False] * table.num_rows
        reasons = table[reason_name].to_pylist() if reason_name else [None] * table.num_rows
        for index, value in enumerate(complete):
            if value is True:
                continue
            reason = reasons[index]
            if complete_name is None:
                row_reasons = ["completeness_evidence_missing"]
            elif isinstance(reason, list):
                row_reasons = [item for item in reason if isinstance(item, str) and item]
            else:
                row_reasons = [reason] if isinstance(reason, str) and reason else []
            excluded_rows.add(index)
            for exact_reason in row_reasons or ["reason_unavailable"]:
                reason_rows[exact_reason].add(index)
    excluded = {reason: len(rows) for reason, rows in reason_rows.items()}
    _check_exclusions(excluded, drop=drop_incomplete)
    if consumer == "dowhy":
        if repeated:
            raise _REPEATED_UNITS_UNSUPPORTED.error(consumer=consumer)
        if any(
            getattr(roles, name) is not None for name in ("weight", "target_weight", "trial_weight")
        ):
            raise _UNSUPPORTED_WEIGHTS.error(consumer=consumer)
    if excluded_rows:
        table = table.filter(pa.array([i not in excluded_rows for i in range(table.num_rows)]))
    return ExportPreflight(table, excluded)


@contextmanager
def preflight_warehouse(
    table: ir.Table,
    *,
    roles: Roles,
    columns: Mapping[str, ColumnMeta],
    provenance: Provenance,
    drop_incomplete: bool = False,
) -> Generator[WarehousePreflight]:
    """Check aggregate evidence and consume a relation in one DuckDB snapshot.

    Only aggregate counts/reasons are collected. Create the downstream view and
    call the consumer inside the context, using the same Ibis connection. Exit
    rolls back temporary views as well as any other writes inside the context.
    This checks export hazards, not source integrity: use a verified materialized
    frame table and its matching roles, column metadata and provenance.
    """
    import ibis

    _check_metadata(provenance)
    con = cast("Backend", ibis.get_backend(table))
    if con.name != "duckdb":
        raise _SNAPSHOT_REQUIRED.error()
    try:
        con.raw_sql("BEGIN TRANSACTION")
    except Exception as exc:
        raise _SNAPSHOT_REQUIRED.error() from exc
    try:
        repeated = False
        if roles.unit in table.columns and provenance.shape != "life_table":
            counts = table.aggregate(
                n=table.count(), units=table[roles.unit].nunique()
            ).to_pyarrow()
            repeated = counts["n"][0].as_py() != counts["units"][0].as_py()
        _check_identity(roles, table.columns, repeated)
        reasons = []
        keep: list[ir.BooleanValue] = []
        for complete_name, reason_name in _supports(roles, columns, table.columns):
            complete = (
                table[complete_name].fill_null(False) if complete_name else ibis.literal(False)
            )
            keep.append(cast("ir.BooleanValue", complete))
            if complete_name is None:
                reason = ibis.array(["completeness_evidence_missing"])
            elif reason_name is None:
                reason = ibis.array(["reason_unavailable"])
            else:
                value = table[reason_name]
                if value.type().is_array():
                    values = cast("ir.ArrayValue", value).filter(
                        lambda item: item.notnull() & (item != "")
                    )
                    reason = ibis.ifelse(
                        values.isnull() | (values.length() == 0), ["reason_unavailable"], values
                    )
                else:
                    reason = ibis.array(
                        [ibis.ifelse(value.isnull() | (value == ""), "reason_unavailable", value)]
                    )
            reasons.append(ibis.ifelse(complete, ibis.literal([], type="array<string>"), reason))
        excluded = {}
        if reasons:
            evidence = table.select(reason=ibis.array(reasons).flatten().unique().unnest())
            evidence = evidence.filter(evidence.reason.notnull())
            counts = evidence.group_by("reason").aggregate(n=ibis._.count()).to_pyarrow()
            excluded = {row["reason"]: row["n"] for row in counts.to_pylist()}
        _check_exclusions(excluded, drop=drop_incomplete)
        yield WarehousePreflight(table.filter(keep), excluded)
    finally:
        con.raw_sql("ROLLBACK")
