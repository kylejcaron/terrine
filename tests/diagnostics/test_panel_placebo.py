import math
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from terrine.data import ExecutionSnapshot, MaterializedFrame, Provenance, Roles
from terrine.diagnostics import negative_control_test, placebo_test


def _frame(*, include_negative=True, planted=False):
    rows = []
    for unit in range(40):
        treated = unit < 20
        for period in (-2, -1, 0):
            rows.append(
                {
                    "unit": unit,
                    "anchor_id": (unit, period),
                    "t0": datetime(2025, 1, 1, tzinfo=UTC),
                    "treatment": treated,
                    "event_period": period,
                    "y": float(treated and planted and period == -2),
                    **({"negative": float(treated)} if include_negative else {}),
                }
            )
    return MaterializedFrame(
        pa.Table.from_pylist(rows),
        Roles("unit", "anchor_id", "t0", "treatment", (), ("y",), cluster_unit="unit"),
        {},
        Provenance(
            {},
            {},
            "design",
            "model",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "calendar_comparison",
            None,
        ),
    )


def test_placebo_effect_is_detected_when_planted():
    result = placebo_test(_frame(planted=True), outcome="y", placebo_periods=(-2,))
    assert result.status == "warn"
    assert result.metrics["n_periods"] == 1
    assert result.metrics["max_abs_effect"] == pytest.approx(1.0)
    assert result.metrics["any_reject"] is True
    assert (
        result.context["interpretation"]
        == "placebo_evidence_does_not_authorize_causal_interpretation"
    )


def test_missing_negative_control_is_unavailable_not_pass():
    result = negative_control_test(
        _frame(include_negative=False), outcome="y", negative_control="negative"
    )
    assert result.status == "unavailable"
    assert result.context["reason"] == "missing_negative_control"


def test_integer_binary_treatment_is_accepted():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["treatment"] = int(row["treatment"])
    result = placebo_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", placebo_periods=(-2,)
    )
    assert result.status == "warn"
    assert result.context["reason"] != "incomplete_or_missing_treatment_outcome"


def test_negative_control_counts_a_cluster_once_across_treatment_roles():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        if row["event_period"] == -1:
            row["treatment"] = not row["treatment"]
            row["negative"] = float(row["treatment"])
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.metrics["n_units"] == 40


def test_placebo_uses_requested_alpha():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        if row["event_period"] == -2:
            arm_sign = 1 if row["unit"] % 2 else -1
            row["y"] = (0.63 + arm_sign) if row["treatment"] else float(arm_sign)
    result = placebo_test(
        replace(frame, table=pa.Table.from_pylist(rows)),
        outcome="y",
        placebo_periods=(-2,),
        alpha=0.01,
    )
    assert result.metrics["any_reject"] is False


def test_large_finite_repeated_outcomes_have_finite_cluster_summary():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["negative"] = 1e308
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.metrics["effect"] == 0.0
    assert result.metrics["standard_error"] == 0.0
    assert result.metrics["n_units"] == 40


def test_boolean_outcome_matches_numeric_binary_outcome():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["negative"] = row["treatment"]
    boolean = replace(frame, table=pa.Table.from_pylist(rows))
    numeric_rows = [{**row, "negative": int(row["negative"])} for row in rows]
    numeric = replace(frame, table=pa.Table.from_pylist(numeric_rows))
    boolean_result = negative_control_test(boolean, outcome="y", negative_control="negative")
    numeric_result = negative_control_test(numeric, outcome="y", negative_control="negative")
    assert boolean_result.status == numeric_result.status == "warn"
    assert boolean_result.metrics["effect"] == numeric_result.metrics["effect"] == 1.0


def test_negative_control_mean_preserves_small_term_through_cancellation():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        if row["treatment"]:
            row["negative"] = {-2: 1e308, -1: 1e-16, 0: -1e308}[row["event_period"]]
        else:
            row["negative"] = 0.0
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.metrics["effect"] == pytest.approx(1e-16 / 3, rel=1e-12, abs=0.0)


def test_negative_control_preserves_small_difference_at_large_offset():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["negative"] = 1e16 + 2 if row["treatment"] else 1e16
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.metrics["effect"] == 2.0


def test_placebo_pvalue_does_not_square_extreme_finite_statistic():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["y"] = 1e200 if row["treatment"] and row["event_period"] == -2 else 0.0
    result = placebo_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", placebo_periods=(-2,)
    )
    assert result.metrics["any_reject"] is True


def test_tiny_nonzero_standardized_effect_keeps_two_sided_pvalue():
    frame = _frame()
    rows = [row for row in frame.table.to_pylist() if row["unit"] in (0, 1, 20, 21)]
    for row in rows:
        row["event_period"] = -2
        row["anchor_id"] = (row["unit"], -2)
        row["y"] = 1.0 if row["unit"] == 1 else 0.1 if row["unit"] == 20 else 0.0
    result = placebo_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", placebo_periods=(-2,)
    )
    assert result.metrics["any_reject"] is False


def test_repeated_subnormal_outcomes_remain_nonzero():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["negative"] = 5e-324 if row["treatment"] else 0.0
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.metrics["effect"] == 5e-324


def test_repeated_maximum_finite_outcomes_have_finite_mean():
    frame = _frame()
    rows = frame.table.to_pylist()
    for row in rows:
        row["negative"] = 1.7976931348623157e308 if row["treatment"] else 0.0
    result = negative_control_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", negative_control="negative"
    )
    assert result.status == "warn"
    assert result.metrics["effect"] == 1.7976931348623157e308


def test_overflowing_residual_can_have_finite_contrast_and_se():
    frame = _frame()
    rows = [row for row in frame.table.to_pylist() if row["unit"] in (0, 1, 2, 3, 20, 21, 22, 23)]
    treated_means = (-1e308, 1.6e308, 1.6e308, 1.6e308)
    for row in rows:
        row["event_period"] = -2
        row["anchor_id"] = (row["unit"], -2)
        row["y"] = treated_means[row["unit"]] if row["treatment"] else 0.0
    result = placebo_test(
        replace(frame, table=pa.Table.from_pylist(rows)), outcome="y", placebo_periods=(-2,)
    )
    assert result.status == "warn"
    effect = result.metrics["max_abs_effect"]
    assert isinstance(effect, float) and math.isfinite(effect)
