from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

import terrine.export as exports
from terrine.data import PanelMeta
from terrine.errors import TerrineError


@pytest.fixture
def panel(frame):
    table = pa.table(
        {
            "id": [1, 1, 1, 2, 2, 2],
            "anchor": list(range(6)),
            "t0": [2, -1, 0, 0, 2, -1],
            "a": [1, 0, 1, 0, 0, 0],
            "x": [1.0] * 6,
            "y": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "complete": [True] * 6,
            "reason": pa.array([None] * 6, type=pa.string()),
        }
    )
    return replace(
        frame,
        table=table,
        roles=replace(frame.roles, cluster_unit="id"),
        provenance=replace(frame.provenance, shape="sequential"),
    )


def test_first_treat_preserves_row_order_gaps_and_never_treated_zero(panel):
    result = exports.to_diff_diff(panel, period_col="t0")
    assert result.df[result.unit_col].tolist() == [1, 1, 1, 2, 2, 2]
    assert result.df[result.time_col].tolist() == [4, 1, 2, 2, 4, 1]
    assert result.df[result.first_treat_col].tolist() == [2, 2, 2, 0, 0, 0]
    assert result.period_mapping == {-1: 1, 0: 2, 2: 4}
    with pytest.raises(TypeError):
        result.period_mapping[-1] = 2  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize("excluded", [[2], [0, 2], [0, 3, 4]])
def test_exclusions_cannot_redefine_adoption_or_calendar_gaps(panel, excluded):
    table = panel.table.set_column(
        6, "complete", pa.array([i not in excluded for i in range(6)])
    ).set_column(
        7, "reason", pa.array(["incomplete_coverage" if i in excluded else None for i in range(6)])
    )
    result = exports.to_diff_diff(
        replace(panel, table=table), period_col="t0", drop_incomplete=True
    )
    assert result.period_mapping == {-1: 1, 0: 2, 2: 4}
    assert result.df.loc[result.df["id"] == 1, result.first_treat_col].tolist() == [2] * (
        3 - len(set(excluded) & {0, 1, 2})
    )
    assert result.excluded_counts == {"incomplete_coverage": len(excluded)}


def test_timestamp_periods_require_cadence_and_preserve_missing_ticks(panel):
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    periods = [origin + timedelta(days=i + 1) for i in [2, -1, 0, 0, 2, -1]]
    panel = replace(panel, table=panel.table.set_column(2, "t0", pa.array(periods)))
    with pytest.raises(TerrineError) as error:
        exports.to_diff_diff(panel, period_col="t0")
    assert error.value.code == "terrine.diff_diff.invalid_panel"
    result = exports.to_diff_diff(panel, period_col="t0", period_seconds=86400)
    assert result.df[result.time_col].tolist() == [4, 1, 2, 2, 4, 1]
    metadata = replace(panel.provenance, panel=PanelMeta("sequential", period_seconds=86400))
    inferred = exports.to_diff_diff(replace(panel, provenance=metadata), period_col="t0")
    assert inferred.period_mapping == result.period_mapping


@pytest.mark.parametrize(
    "change", ["duplicate", "reverse", "nonbinary", "null_period", "collision"]
)
def test_ambiguous_panel_is_refused_with_route_forward(panel, change):
    table = panel.table
    if change == "duplicate":
        table = table.take([0, 0, 1, 2, 3, 4])
    elif change == "reverse":
        table = table.set_column(3, "a", pa.array([1, 1, 0, 0, 0, 0]))
        table = table.set_column(
            6, "complete", pa.array([True, True, False, True, True, True])
        ).set_column(7, "reason", pa.array([None, None, "incomplete_coverage", None, None, None]))
    elif change == "nonbinary":
        table = table.set_column(3, "a", pa.array([2, 0, 1, 0, 0, 0]))
    elif change == "null_period":
        table = table.set_column(2, "t0", pa.array([None, -1, 0, 0, 2, -1]))
    else:
        table = table.append_column("first_treat", pa.array([0] * 6))
    with pytest.raises(TerrineError) as error:
        exports.to_diff_diff(replace(panel, table=table), period_col="t0", drop_incomplete=True)
    assert error.value.code == "terrine.diff_diff.invalid_panel"
    assert "route" in error.value.context


def test_diff_diff_preflight_precedes_mapping_validation(panel):
    candidate = replace(panel, roles=replace(panel.roles, cluster_unit=None))
    with pytest.raises(TerrineError) as error:
        exports.to_diff_diff(candidate, period_col="missing")
    assert error.value.code == "terrine.export.cluster_required"
