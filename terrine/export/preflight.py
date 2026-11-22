"""Safety checks shared by downstream dataset hand-offs."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa

import terrine.reasons as reason_codes
from terrine.backends.registry import engine_supports
from terrine.data import ColumnMeta, MaterializedFrame, Provenance, Roles
from terrine.errors import RefusalSpec

if TYPE_CHECKING:
    import ibis.expr.types as ir
    from ibis.backends.duckdb import Backend

__all__ = [
    "CalendarContrast",
    "CalendarContrastSupport",
    "ExportPreflight",
    "WarehousePreflight",
    "preflight_export",
    "preflight_warehouse",
]

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
_INVALID_INPUT = RefusalSpec(
    "terrine.export.invalid_input", "Export input {field} is invalid: {reason}."
)
_CHOICE = RefusalSpec(
    "terrine.export.invalid_input", "Export input {field} must be one of {choices}."
)
_CALENDAR_CONTRAST_UNSUPPORTED = RefusalSpec(
    "terrine.export.calendar_contrast_unsupported",
    "Calendar contrast for {outcome!r} in cohort {cohort}, reference {reference_period}, "
    "comparison {comparison_period}, lacks {arm} support ({reason}); {route}.",
)
_DESCRIPTIVE_ROUTE = "omit calendar_contrast for descriptive output"
_ARMS = ("treated", "control")


@dataclass(frozen=True, slots=True, kw_only=True)
class CalendarContrast:
    """Request support for one outcome and focal cohort against a declared reference period.

    Set ``paired=True`` only for same-unit panel contrasts. The default checks the
    margins, as for repeated cross sections.

    Args:
        outcome: Name of the outcome column to check.
        cohort: Timezone-aware timestamp identifying the focal adopter cohort.
        reference_period: Relative period used as the baseline.
        comparison_periods: Nonempty, distinct relative periods other than the baseline.
        paired: Also require the same units in both periods within each arm.

    Attributes:
        outcome: Outcome checked for support.
        cohort: Focal adopter cohort, normalized to UTC.
        reference_period: Baseline relative period.
        comparison_periods: Relative periods checked against the baseline.
        paired: Whether to require same-unit support.

    Example:
        from datetime import UTC, datetime

        tr.export.CalendarContrast(
            outcome="spend",
            cohort=datetime(2025, 3, 1, tzinfo=UTC),
            reference_period=-1,
            comparison_periods=(0, 1),
        )
    """

    outcome: str
    cohort: datetime
    reference_period: int
    comparison_periods: tuple[int, ...]
    paired: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, str) or not self.outcome:
            raise _INVALID_INPUT.error(field="outcome", reason="requires_nonempty_name")
        if not isinstance(self.cohort, datetime) or self.cohort.utcoffset() is None:
            raise _INVALID_INPUT.error(field="cohort", reason="requires_aware_datetime")
        object.__setattr__(self, "cohort", self.cohort.astimezone(UTC))
        periods = self.comparison_periods
        if (
            type(self.reference_period) is not int
            or not isinstance(periods, tuple)
            or not periods
            or any(type(period) is not int for period in periods)
            or self.reference_period in periods
            or len(periods) != len(set(periods))
        ):
            raise _INVALID_INPUT.error(field="calendar_contrast", reason="invalid_periods")
        if type(self.paired) is not bool:
            raise _INVALID_INPUT.error(field="paired", reason="requires_boolean")


@dataclass(frozen=True, slots=True, kw_only=True)
class CalendarContrastSupport:
    """Usable distinct-unit margins (reference, comparison) and pair counts by arm.

    Args:
        comparison_period: Period compared with the reference.
        treated: Treated-unit counts in the reference and comparison periods.
        control: Control-unit counts in the reference and comparison periods.
        treated_pairs: Treated units usable in both periods.
        control_pairs: Control units usable in both periods.

    Attributes:
        comparison_period: Period compared with the reference.
        treated: Treated-unit margins, reference first.
        control: Control-unit margins, reference first.
        treated_pairs: Treated units usable in both periods.
        control_pairs: Control units usable in both periods.

    Example:
        tr.export.CalendarContrastSupport(
            comparison_period=0,
            treated=(10, 9),
            control=(20, 18),
            treated_pairs=8,
            control_pairs=17,
        )
    """

    comparison_period: int
    treated: tuple[int, int]
    control: tuple[int, int]
    treated_pairs: int
    control_pairs: int


@dataclass(frozen=True, slots=True)
class ExportPreflight:
    """Return checked export rows and exclusion counts by reason.

    Args:
        table: PyArrow Table containing the admitted complete rows.
        excluded_counts: Mapping of missing-value reason codes to excluded row counts.
        calendar_support: Outcome-specific counts per requested comparison, or None.

    Attributes:
        table: PyArrow Table containing the admitted complete rows.
        excluded_counts: Mapping of missing-value reason codes to excluded row counts.
        calendar_support: Outcome-specific counts per requested comparison, or None.

    Example:
        tr.export.preflight_export(
            design.snapshot(model, as_of="2025-03-01", eligibility={}).materialize()
        )
    """

    table: pa.Table
    excluded_counts: Mapping[str, int]
    calendar_support: tuple[CalendarContrastSupport, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded_counts", MappingProxyType(dict(self.excluded_counts)))


@dataclass(frozen=True, slots=True)
class WarehousePreflight:
    """Hold checked rows and exclusions inside a DuckDB export context.

    Args:
        table: Ibis Table of admitted rows, valid only inside the export context.
        excluded_counts: Mapping of missing-value reason codes to excluded row counts.
        calendar_support: Outcome-specific counts per requested comparison, or None.

    Attributes:
        table: Ibis Table of admitted rows, valid only inside the export context.
        excluded_counts: Mapping of missing-value reason codes to excluded row counts.
        calendar_support: Outcome-specific counts per requested comparison, or None.

    Example:
        with tr.export.preflight_warehouse(
            (snapshot := design.snapshot(model, as_of="2025-03-01", eligibility={})).expr,
            roles=snapshot.roles,
            columns=snapshot.columns,
            provenance=snapshot.materialize().provenance,
        ) as rows:
            rows.table.to_pyarrow()
    """

    table: ir.Table
    excluded_counts: Mapping[str, int]
    calendar_support: tuple[CalendarContrastSupport, ...] | None = None

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


def _complete_outcomes(
    data: MaterializedFrame, *, drop_incomplete: bool = False
) -> ExportPreflight:
    table = data.table
    excluded_rows: set[int] = set()
    reason_rows: dict[str, set[int]] = defaultdict(set)
    for complete_name, reason_name in _supports(data.roles, data.columns, table.column_names):
        complete = table[complete_name].to_pylist() if complete_name else [False] * table.num_rows
        reasons = table[reason_name].to_pylist() if reason_name else [None] * table.num_rows
        for index, value in enumerate(complete):
            if value is True:
                continue
            reason = reasons[index]
            if complete_name is None:
                row_reasons = [reason_codes.COMPLETENESS_EVIDENCE_MISSING]
            elif isinstance(reason, list):
                row_reasons = [item for item in reason if isinstance(item, str) and item]
            else:
                row_reasons = [reason] if isinstance(reason, str) and reason else []
            excluded_rows.add(index)
            for exact_reason in row_reasons or [reason_codes.REASON_UNAVAILABLE]:
                reason_rows[exact_reason].add(index)
    excluded = {reason: len(rows) for reason, rows in reason_rows.items()}
    _check_exclusions(excluded, drop=drop_incomplete)
    if excluded_rows:
        table = table.filter(pa.array([i not in excluded_rows for i in range(table.num_rows)]))
    return ExportPreflight(table, excluded)


def _contrast_error(
    request: CalendarContrast,
    *,
    comparison_period: int,
    period: int,
    arm: str,
    reason: str,
    **counts,
):
    return _CALENDAR_CONTRAST_UNSUPPORTED.error(
        outcome=request.outcome,
        cohort=request.cohort.isoformat(),
        reference_period=request.reference_period,
        comparison_periods=request.comparison_periods,
        comparison_period=comparison_period,
        period=period,
        paired=request.paired,
        arm=arm,
        reason=reason,
        route=_DESCRIPTIVE_ROUTE,
        **counts,
    )


def _contrast_evidence(
    request: CalendarContrast,
    roles: Roles,
    columns: Mapping[str, ColumnMeta],
    provenance: Provenance,
    names: Sequence[str],
) -> str:
    """Identify the requested outcome's completeness column and observed grid."""
    if not isinstance(request, CalendarContrast):
        raise _INVALID_INPUT.error(field="calendar_contrast", reason="wrong_type")
    comparison = provenance.calendar_comparison
    outcome = columns.get(request.outcome)
    complete = outcome.complete_column if outcome is not None else None
    observed = (
        {cell.period for cell in comparison.support if cell.cohort == request.cohort}
        if comparison is not None and comparison.support is not None
        else set()
    )
    required = {
        roles.unit,
        "cohort",
        "event_period",
        "treatment",
        "control_eligible",
        request.outcome,
    }
    ready = (
        provenance.shape == "calendar_comparison"
        and comparison is not None
        and request.reference_period == comparison.reference_period
        and request.outcome in roles.outcomes
        and required <= set(names)
        and complete is not None
        and complete in names
    )
    for period in request.comparison_periods:
        if (
            not ready
            or comparison is None
            or period not in comparison.comparison_periods
            or request.reference_period not in observed
            or period not in observed
        ):
            raise _contrast_error(
                request,
                comparison_period=period,
                period=(
                    request.reference_period if request.reference_period not in observed else period
                ),
                arm="both",
                reason="unobservable",
            )
    assert complete is not None
    return complete


def _require_contrast(
    request: CalendarContrast, support: tuple[CalendarContrastSupport, ...]
) -> tuple[CalendarContrastSupport, ...]:
    for cell in support:
        for arm, margins, pairs in (
            ("treated", cell.treated, cell.treated_pairs),
            ("control", cell.control, cell.control_pairs),
        ):
            if 0 not in margins and (not request.paired or pairs):
                continue
            empty_margin = 0 in margins
            raise _contrast_error(
                request,
                comparison_period=cell.comparison_period,
                period=request.reference_period if not margins[0] else cell.comparison_period,
                arm=arm,
                reason="empty_margin" if empty_margin else "empty_pair",
                treated=cell.treated,
                control=cell.control,
                treated_pairs=cell.treated_pairs,
                control_pairs=cell.control_pairs,
            )
    return support


def _arrow_contrast(
    table: pa.Table, request: CalendarContrast, complete: str, unit: str
) -> tuple[CalendarContrastSupport, ...]:
    selected = {request.reference_period, *request.comparison_periods}
    units: dict[tuple[bool, int], set[pa.Scalar]] = defaultdict(set)
    for cohort, period, treated, control, key, known, y in zip(
        table["cohort"].to_pylist(),
        table["event_period"].to_pylist(),
        table["treatment"].to_pylist(),
        table["control_eligible"].to_pylist(),
        table[unit],
        table[complete].to_pylist(),
        table[request.outcome],
        strict=True,
    ):
        if (
            cohort == request.cohort
            and period in selected
            and key.is_valid
            and known is True
            and y.is_valid
            and (treated is True or (treated is False and control is True))
        ):
            units[treated, period].add(key)
    treated_ref, control_ref = (units[arm, request.reference_period] for arm in (True, False))
    support = []
    for period in request.comparison_periods:
        treated, control = units[True, period], units[False, period]
        support.append(
            CalendarContrastSupport(
                comparison_period=period,
                treated=(len(treated_ref), len(treated)),
                control=(len(control_ref), len(control)),
                treated_pairs=len(treated_ref & treated),
                control_pairs=len(control_ref & control),
            )
        )
    return _require_contrast(request, tuple(support))


def _warehouse_contrast(
    table: ir.Table, request: CalendarContrast, complete: str, unit: str
) -> tuple[CalendarContrastSupport, ...]:
    periods = (request.reference_period, *request.comparison_periods)
    usable = table.filter(
        table.cohort == request.cohort,
        table.event_period.isin(periods),
        table[unit].notnull(),
        table[complete].fill_null(False),
        table[request.outcome].notnull(),
    )
    arms = {"treated": usable.treatment, "control": ~usable.treatment & usable.control_eligible}
    # Reduce to presence flags per unit before counting; never transfer the unit IDs.
    flags = {
        f"{arm}_{i}": (eligible & (usable.event_period == period)).any()
        for arm, eligible in arms.items()
        for i, period in enumerate(periods)
    }
    per_unit = usable.group_by(usable[unit].name("__terrine_contrast_unit")).aggregate(**flags)
    counts = {name: per_unit[name].cast("int64").sum().fill_null(0) for name in flags}
    for arm in _ARMS:
        ref = per_unit[f"{arm}_0"]
        for i in range(1, len(periods)):
            pair = ref & per_unit[f"{arm}_{i}"]
            counts[f"{arm}_pair_{i}"] = pair.cast("int64").sum().fill_null(0)
    totals = per_unit.aggregate(**counts).to_pyarrow().to_pylist()[0]
    return _require_contrast(
        request,
        tuple(
            CalendarContrastSupport(
                comparison_period=period,
                treated=(totals["treated_0"], totals[f"treated_{i}"]),
                control=(totals["control_0"], totals[f"control_{i}"]),
                treated_pairs=totals[f"treated_pair_{i}"],
                control_pairs=totals[f"control_pair_{i}"],
            )
            for i, period in enumerate(request.comparison_periods, 1)
        ),
    )


def preflight_export(
    data: MaterializedFrame,
    *,
    drop_incomplete: bool = False,
    consumer: Literal["dowhy"] | None = None,
    calendar_contrast: CalendarContrast | None = None,
) -> ExportPreflight:
    """Check eligibility, unit identity and outcome completeness before export.

    Exclusion counts include each row once per reason; ``drop_incomplete`` permits their removal.

    Args:
        data: MaterializedFrame carrying checked roles, column metadata, completeness and
            provenance.
        drop_incomplete: Whether to drop incomplete outcomes explicitly; False refuses them.
        consumer: "dowhy" applies its additional hand-off restrictions; None applies shared checks
            only.
        calendar_contrast: Check outcome-specific margins and, if paired, usable same-unit pairs.
            None returns descriptive rows without claiming contrast support.

    Returns:
        An ExportPreflight containing admitted rows and excluded counts.

    Example:
        tr.export.preflight_export(
            design.snapshot(model, as_of="2025-03-01", eligibility={}).materialize()
        )
    """
    if not isinstance(data, MaterializedFrame):
        raise _INVALID_INPUT.error(reason="wrong_type", field="data")
    if consumer not in (None, "dowhy"):
        raise _CHOICE.error(reason="unsupported_value", field="consumer", choices=(None, "dowhy"))
    _check_metadata(data.provenance)
    roles, table = data.roles, data.table
    units = table[roles.unit].to_pylist() if roles.unit in table.column_names else []
    repeated = len(set(units)) != len(units)
    _check_identity(roles, table.column_names, repeated and data.provenance.shape != "life_table")
    checked = _complete_outcomes(data, drop_incomplete=drop_incomplete)
    if consumer == "dowhy":
        if repeated:
            raise _REPEATED_UNITS_UNSUPPORTED.error(consumer=consumer)
        if any(
            getattr(roles, name) is not None for name in ("weight", "target_weight", "trial_weight")
        ):
            raise _UNSUPPORTED_WEIGHTS.error(consumer=consumer)
    if calendar_contrast is not None:
        complete = _contrast_evidence(
            calendar_contrast, roles, data.columns, data.provenance, checked.table.column_names
        )
        return ExportPreflight(
            checked.table,
            checked.excluded_counts,
            _arrow_contrast(checked.table, calendar_contrast, complete, roles.unit),
        )
    return checked


@contextmanager
def preflight_warehouse(
    table: ir.Table,
    *,
    roles: Roles,
    columns: Mapping[str, ColumnMeta],
    provenance: Provenance,
    drop_incomplete: bool = False,
    calendar_contrast: CalendarContrast | None = None,
) -> Generator[WarehousePreflight]:
    """Check export hazards for a verified dataset relation inside one DuckDB transaction.

    Use matching roles, columns and provenance and consume rows inside the context;
    exiting rolls back temporary views and all other writes in that transaction.

    Args:
        table: Verified DuckDB Ibis Table to read inside a dedicated export transaction.
        roles: Roles matching this relation, including independent unit and outcome identities.
        columns: Mapping of column names to ColumnMeta for this relation.
        provenance: Provenance belonging to this relation and its admitted snapshot.
        drop_incomplete: Whether to drop incomplete outcomes explicitly; False refuses them.
        calendar_contrast: Check outcome-specific margins and, if paired, usable same-unit pairs
            inside this transaction. None returns descriptive rows without checking a contrast.

    Yields:
        WarehousePreflight: Checked relation and exclusion counts inside the pinned transaction.

    Example:
        with tr.export.preflight_warehouse(
            (snapshot := design.snapshot(model, as_of="2025-03-01", eligibility={})).expr,
            roles=snapshot.roles,
            columns=snapshot.columns,
            provenance=snapshot.materialize().provenance,
        ) as rows:
            rows.table.to_pyarrow()
    """
    import ibis

    _check_metadata(provenance)
    con = cast("Backend", ibis.get_backend(table))
    if not engine_supports(con.name, "warehouse_preflight"):
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
                reason = ibis.array([reason_codes.COMPLETENESS_EVIDENCE_MISSING])
            elif reason_name is None:
                reason = ibis.array([reason_codes.REASON_UNAVAILABLE])
            else:
                value = table[reason_name]
                if value.type().is_array():
                    values = cast("ir.ArrayValue", value).filter(
                        lambda item: item.notnull() & (item != "")
                    )
                    reason = ibis.ifelse(
                        values.isnull() | (values.length() == 0),
                        [reason_codes.REASON_UNAVAILABLE],
                        values,
                    )
                else:
                    reason = ibis.array(
                        [
                            ibis.ifelse(
                                value.isnull() | (value == ""),
                                reason_codes.REASON_UNAVAILABLE,
                                value,
                            )
                        ]
                    )
            reasons.append(ibis.ifelse(complete, ibis.literal([], type="array<string>"), reason))
        excluded = {}
        if reasons:
            evidence = table.select(reason=ibis.array(reasons).flatten().unique().unnest())
            evidence = evidence.filter(evidence.reason.notnull())
            counts = evidence.group_by("reason").aggregate(n=ibis._.count()).to_pyarrow()
            excluded = {row["reason"]: row["n"] for row in counts.to_pylist()}
        _check_exclusions(excluded, drop=drop_incomplete)
        checked = table.filter(keep) if keep else table
        support = None
        if calendar_contrast is not None:
            complete = _contrast_evidence(
                calendar_contrast, roles, columns, provenance, checked.columns
            )
            support = _warehouse_contrast(checked, calendar_contrast, complete, roles.unit)
        yield WarehousePreflight(checked, excluded, support)
    finally:
        con.raw_sql("ROLLBACK")
