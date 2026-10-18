from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest

from terrine.data import (
    CalendarComparisonMeta,
    ColumnMeta,
    ExecutionSnapshot,
    MaterializedFrame,
    Provenance,
    Roles,
    SamplingMeta,
)
from terrine.errors import TerrineError
from terrine.estimate import stacked_event_study
from terrine.windows import Window
from tests.reference.panel_did import clustered_catt_reference


def _frame(*, sampled=False):
    origin = datetime(2025, 1, 1, tzinfo=UTC)
    cohorts = (origin + timedelta(days=3), origin + timedelta(days=6))
    rows = []
    # Both cohorts have treated units and shared never-treated controls. Outcomes
    # contain baseline trends plus cohort-specific effects at comparison time.
    for cohort_index, cohort in enumerate(cohorts):
        for unit, treated in (
            (f"t{cohort_index}a", True),
            (f"t{cohort_index}b", True),
            ("c1", False),
            ("c2", False),
        ):
            for event_period in (-1, 0):
                period = cohort + timedelta(days=event_period)
                baseline = 10.0 + (unit.startswith("t") * 2.0) + (unit == "c2")
                trend = (unit.startswith("t") * 0.5) + (unit == "c2") * 0.25
                effect = (1.5 + cohort_index) if treated and event_period == 0 else 0.0
                rows.append(
                    {
                        "unit": unit,
                        "anchor_id": f"{cohort.isoformat()}:{period.isoformat()}:{unit}",
                        "t0": period,
                        "treatment": treated,
                        "cohort": cohort,
                        "period": period,
                        "event_period": event_period,
                        "control_eligible": not treated,
                        "__terrine_treatment_initiation_time": cohort if treated else None,
                        "y": baseline + trend * event_period + effect,
                        "q": 1.0 if unit != "c2" else 2.0,
                    }
                )
    table = pa.Table.from_pylist(rows)
    calendar = CalendarComparisonMeta(
        "never_treated", "events", "session", origin, "recorded_at", -1, (0,), (), 86400
    )
    provenance = Provenance(
        {},
        {},
        "design",
        "model",
        {},
        ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
        "calendar_comparison",
        None,
        calendar_comparison=calendar,
        sampling=(SamplingMeta((), 1, 0) if sampled else None),
    )
    return MaterializedFrame(
        table,
        Roles("unit", "anchor_id", "t0", "treatment", (), ("y",), cluster_unit="unit"),
        {"y": ColumnMeta(None, "outcome", None, None, None, None)},
        provenance,
    )


def test_stacked_catt_and_joint_cluster_covariance_match_independent_reference():
    frame = _frame()
    result = stacked_event_study(
        frame, outcome="y", treatment="treatment", cohort="cohort", period="period", unit_weight="q"
    )
    reference = clustered_catt_reference(
        frame.table.to_pylist(),
        reference_period=-1,
        period_seconds=86400,
        comparison_periods=(0,),
        unit_weight="q",
    )
    assert result.effects == pytest.approx(reference.effects)
    assert np.asarray(result.covariance) == pytest.approx(np.asarray(reference.covariance))
    assert result.n_clusters == len({row["unit"] for row in frame.table.to_pylist()})


def test_stacked_catt_refuses_sampled_analysis_populations():
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            _frame(sampled=True),
            outcome="y",
            treatment="treatment",
            cohort="cohort",
            period="period",
        )
    assert error.value.code == "terrine.estimate.sampled_analysis_population"


def test_stacked_catt_refuses_legacy_sampled_control_weight_kind():
    from dataclasses import replace

    frame = _frame()
    provenance = replace(frame.provenance, weight_kind="sampled_control")
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            replace(frame, provenance=provenance),
            outcome="y",
            treatment="treatment",
            cohort="cohort",
            period="period",
        )
    assert error.value.code == "terrine.estimate.sampled_analysis_population"


def test_cohort_weights_define_effect_aggregation():
    result = stacked_event_study(
        _frame(),
        outcome="y",
        treatment="treatment",
        cohort="cohort",
        period="period",
        cohort_weights={"2025-01-04T00:00:00+00:00": 1.0, "2025-01-07T00:00:00+00:00": 3.0},
    )
    assert result.aggregate_effects[0] == pytest.approx(2.625)


def test_conditional_but_not_weighted_unconditional_trends_counterexample():
    from dataclasses import replace

    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["x"] = row["unit"].endswith("a")
        row["y"] = float(row["event_period"] == 0 and row["x"])
    frame = replace(
        frame,
        table=pa.Table.from_pylist(rows),
        roles=Roles("unit", "anchor_id", "t0", "treatment", ("x",), ("y",), cluster_unit="unit"),
    )
    result = stacked_event_study(
        frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
    )
    assert result.effects == pytest.approx((0.5, 0.5))


def test_stacked_estimator_refuses_outcome_windows_crossing_treatment():
    from dataclasses import replace

    frame = _frame()
    columns = dict(frame.columns)
    columns["y"] = ColumnMeta(Window("0d", "7d"), "outcome", None, None, None, None)
    frame = replace(frame, columns=columns)
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == "terrine.estimate.stacked_input"
    assert error.value.context["reason"] == "outcome_window_contaminates_reference"


def test_stacked_estimator_refuses_incomplete_treated_pairs():
    from dataclasses import replace

    frame = _frame()
    rows = [
        row
        for row in frame.table.to_pylist()
        if not (row["unit"] == "t0a" and row["event_period"] == -1)
    ]
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == "terrine.estimate.stacked_input"
    assert error.value.context["reason"] == "incomplete_treated_pair"


def test_stacked_estimator_refuses_incomplete_admissible_control_pairs():
    from dataclasses import replace

    frame = _frame()
    rows = [
        row
        for row in frame.table.to_pylist()
        if not (row["unit"] == "c1" and row["event_period"] == -1)
    ]
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == "terrine.estimate.stacked_input"
    assert error.value.context["reason"] == "incomplete_control_pair"


def test_stacked_estimator_refuses_missing_comparison_for_admissible_control():
    from dataclasses import replace

    frame = _frame()
    rows = [
        row
        for row in frame.table.to_pylist()
        if not (row["unit"] == "c1" and row["event_period"] == 0)
    ]
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == "terrine.estimate.stacked_input"
    assert error.value.context["reason"] == "incomplete_control_pair"


@pytest.mark.parametrize(
    ("outcomes", "expected_code"),
    [
        ([2**53, 2**53 + 1], "terrine.estimate.input_precision"),
        ([1.0, float("nan")], "terrine.estimate.nonfinite_data"),
    ],
)
def test_stacked_estimator_uses_checked_numeric_outcomes(outcomes, expected_code):
    from dataclasses import replace

    frame = _frame()
    rows = frame.table.to_pylist()
    for index, row in enumerate(rows):
        row["y"] = outcomes[index % len(outcomes)]
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    with pytest.raises(TerrineError) as error:
        stacked_event_study(
            frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
        )
    assert error.value.code == expected_code


def test_stacked_estimator_is_invariant_to_large_common_weight_scale():
    from dataclasses import replace

    frame = _frame()
    baseline = stacked_event_study(
        frame, outcome="y", treatment="treatment", cohort="cohort", period="period", unit_weight="q"
    )
    rows = frame.table.to_pylist()
    for row in rows:
        row["q"] = 5e307 if row["q"] == 1.0 else 1e308
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    result = stacked_event_study(
        frame, outcome="y", treatment="treatment", cohort="cohort", period="period", unit_weight="q"
    )
    assert np.isfinite(result.effects).all()
    assert result.effects == pytest.approx(baseline.effects)
    assert np.asarray(result.covariance) == pytest.approx(np.asarray(baseline.covariance))
    assert np.isfinite(np.asarray(result.covariance)).all()


def test_stacked_estimator_accepts_small_alpha():
    result = stacked_event_study(
        _frame(),
        outcome="y",
        treatment="treatment",
        cohort="cohort",
        period="period",
        alpha=1e-20,
    )
    assert result.provenance["confidence_multiplier"] > 9


def test_naive_staggered_twfe_request_returns_explicit_warning():
    from terrine.estimate import naive_staggered_twfe_warning

    diagnostic = naive_staggered_twfe_warning()
    assert diagnostic.name == "terrine.estimate.naive_staggered_twfe"
    assert diagnostic.status == "warn"
    assert diagnostic.metrics["robust"] is False


def test_stacked_estimator_computes_magnitude_scaled_weighted_means():
    from dataclasses import replace

    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        if row["event_period"] == -1 or not row["treatment"]:
            row["y"] = 0.0
        else:
            row["y"] = 1e308
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    result = stacked_event_study(
        frame, outcome="y", treatment="treatment", cohort="cohort", period="period"
    )
    assert result.effects == pytest.approx((1e308, 1e308))


def test_stacked_estimator_preserves_representable_mixed_magnitude_weights():
    from dataclasses import replace

    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        if row["unit"].endswith("a"):
            row["q"] = 5e-324
            row["y"] = 1e308 if row["event_period"] == 0 else 0.0
        elif row["unit"].endswith("b"):
            row["q"] = 1.0
            row["y"] = 1e-16 if row["event_period"] == 0 else 0.0
        else:
            row["y"] = 0.0
    frame = replace(frame, table=pa.Table.from_pylist(rows))
    result = stacked_event_study(
        frame,
        outcome="y",
        treatment="treatment",
        cohort="cohort",
        period="period",
        unit_weight="q",
    )
    expected = 5e-324 * 1e308 + 1e-16
    assert result.effects == pytest.approx((expected, expected), rel=1e-12, abs=0)


def test_stacked_estimator_accepts_control_removed_by_longer_declared_window():
    from dataclasses import replace

    frame = _frame()
    later = datetime(2025, 1, 10, tzinfo=UTC)
    rows = frame.table.to_pylist()
    rows = [row for row in rows if not (row["unit"] == "c1" and row["event_period"] == 0)]
    for row in rows:
        if row["unit"] == "c1":
            row["__terrine_treatment_initiation_time"] = later
    calendar = replace(frame.provenance.calendar_comparison, control="not_yet_treated")
    columns = dict(frame.columns)
    columns["long"] = ColumnMeta(Window("0d", "7d"), "outcome", None, None, None, None)
    frame = replace(
        frame,
        table=pa.Table.from_pylist(rows),
        columns=columns,
        provenance=replace(frame.provenance, calendar_comparison=calendar),
    )
    stacked_event_study(frame, outcome="y", treatment="treatment", cohort="cohort", period="period")
