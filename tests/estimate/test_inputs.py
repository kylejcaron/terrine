import builtins
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pytest

from terrine.data import EligibilitySummary, ExpressionTiming, FeatureCutoff, MaterializedFrame
from terrine.errors import TerrineError
from terrine.estimate import LinearTerms, NuisanceSpec, aipw, regression_adjustment
from terrine.estimate._data import prepare


def changed(frame, name, values):
    return replace(
        frame, table=frame.table.set_column(frame.table.column_names.index(name), name, values)
    )


def refusal(frame, code, **kwargs):
    with pytest.raises(TerrineError) as exc:
        aipw(frame, **kwargs)
    assert exc.value.code == "terrine.estimate." + code
    assert "value" not in exc.value.context


def test_variable_origin_outcome_is_refused_before_baseline_estimation(frame):
    timing = ExpressionTiming(
        "event_time",
        "purchase",
        (0, 86400),
        (0, 86400),
        ("purchase",),
        {"purchase": (0, 86400)},
        "selected_time",
        None,
        None,
    )
    variable_origin = replace(
        frame,
        provenance=replace(frame.provenance, expression_timings={"y": timing}),
    )
    with pytest.raises(TerrineError) as error:
        prepare(variable_origin, outcome="y")
    assert error.value.code == "terrine.estimate.variable_origin_outcome"


def test_outcome_selection_is_explicit(frame, permissive):
    assert aipw(frame, folds=2, diagnostics=permissive).outcome == "y"
    for outcomes, selected in [((), None), (("y", "other"), None), (("y",), "other")]:
        refusal(
            replace(frame, roles=replace(frame.roles, outcomes=outcomes)),
            "invalid_outcome",
            outcome=selected,
        )


def test_metadata_and_measured_data_refuse_before_fitting(frame):
    cases = [
        (replace(frame, roles=replace(frame.roles, treatment=None)), "treatment_required"),
        (
            replace(
                frame, provenance=replace(frame.provenance, treatment_design="grace_initiation")
            ),
            "unsupported_design",
        ),
        (
            replace(frame, provenance=replace(frame.provenance, treatment_known_at_t0=False)),
            "unsupported_design",
        ),
        (
            replace(frame, provenance=replace(frame.provenance, feature_cutoffs={})),
            "postbaseline_covariate",
        ),
        (
            replace(
                frame,
                provenance=replace(
                    frame.provenance, feature_cutoffs={"x": FeatureCutoff(0, False, "recorded_at")}
                ),
            ),
            "postbaseline_covariate",
        ),
        (
            replace(
                frame,
                provenance=replace(
                    frame.provenance,
                    eligibility_cutoffs={"eligible": FeatureCutoff(1, True, "recorded_at")},
                ),
            ),
            "unsupported_design",
        ),
        (replace(frame, roles=replace(frame.roles, cluster_unit="a")), "unsupported_design"),
        (
            changed(
                frame,
                "t0",
                pa.array(
                    [frame.table["t0"][0].as_py() + timedelta(seconds=1)]
                    + frame.table["t0"].to_pylist()[1:]
                ),
            ),
            "unsupported_design",
        ),
        (
            replace(
                frame, columns={**frame.columns, "y": replace(frame.columns["y"], window=None)}
            ),
            "invalid_outcome",
        ),
        (changed(frame, "id", pa.array([0] * 120)), "duplicate_unit"),
        (changed(frame, "a", pa.array([2] * 120)), "unsupported_design"),
        (changed(frame, "a", pa.array([1] * 120)), "one_arm"),
        (changed(frame, "x", pa.array([None] * 120, type=pa.float64())), "missing_data"),
        (changed(frame, "x", pa.array([np.inf] * 120)), "nonfinite_data"),
        (changed(frame, "x", pa.array([b"binary"] * 120)), "unsupported_feature_dtype"),
        (changed(frame, "a", pa.array([None] * 120, type=pa.int64())), "missing_data"),
        (changed(frame, "y", pa.array([2**53 + 1] * 120, type=pa.int64())), "input_precision"),
        (changed(frame, "complete", pa.array([False] + [True] * 119)), "incomplete_followup"),
    ]
    for candidate, code in cases:
        refusal(candidate, code)
    refusal(frame, "unsupported_estimand", estimand="ATT")


def test_null_time_zero_is_refused(frame):
    values = pa.array(
        [None] + frame.table["t0"].to_pylist()[1:],
        type=frame.table["t0"].type,
    )
    refusal(changed(frame, "t0", values), "unsupported_design")


def test_nanosecond_common_time_zero_needs_no_pandas(frame, monkeypatch):
    values = pa.array([1735689600000000001] * 120, type=pa.int64()).cast(
        pa.timestamp("ns", tz="UTC")
    )
    measured = changed(frame, "t0", values)
    original_import = builtins.__import__

    def without_pandas(name, *args, **kwargs):
        if name.startswith("pandas"):
            raise ImportError("pandas is unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_pandas)
    assert prepare(measured).n == 120


def test_declared_eligibility_requires_resolved_cohort_summary(frame, permissive):
    cutoffs = {"eligible": FeatureCutoff(-1, True, "recorded_at")}
    for summary in (None, EligibilitySummary(121, 0, 1, 120), EligibilitySummary(119, 0, 0, 119)):
        refusal(
            replace(
                frame,
                provenance=replace(
                    frame.provenance, eligibility_cutoffs=cutoffs, eligibility_summary=summary
                ),
            ),
            "unsupported_design",
        )
    good = replace(
        frame,
        provenance=replace(
            frame.provenance,
            eligibility_cutoffs=cutoffs,
            eligibility_summary=EligibilitySummary(123, 3, 0, 120),
        ),
    )
    assert (
        aipw(MaterializedFrame.from_arrow(good.to_arrow()), folds=2, diagnostics=permissive).n
        == 120
    )


def test_target_weight_and_certificate_contract(frame, permissive):
    cutoff = FeatureCutoff(-1, True, "recorded_at")
    weighted = replace(
        frame,
        roles=replace(frame.roles, weight="q"),
        provenance=replace(
            frame.provenance,
            weight_kind="baseline_function",
            weight_cutoff=cutoff,
            weight_depends_on_treatment=False,
        ),
    )
    for p in (
        replace(weighted.provenance, weight_kind="sampled_control"),
        replace(weighted.provenance, weight_depends_on_treatment=True),
        replace(weighted.provenance, weight_cutoff=None),
    ):
        refusal(replace(weighted, provenance=p), "unsupported_weight_kind")
    refusal(changed(weighted, "q", pa.array([0.0] * 120)), "invalid_weight")
    columns = dict(frame.columns)
    columns["y"] = replace(
        columns["y"], numeric_policy="checked_float64_v1", absolute_error_column="bound"
    )
    certified = replace(
        frame, columns=columns, table=frame.table.append_column("bound", pa.array([0.001] * 120))
    )
    loaded = MaterializedFrame.from_json(certified.to_json())
    result = aipw(loaded, folds=2, diagnostics=permissive)
    assert result.provenance["input_precision"]["y"]["compiler_max_absolute_error"] == 0.001
    refusal(changed(certified, "bound", pa.array([-1.0] * 120)), "input_precision")
    refusal(replace(frame, columns=columns), "input_precision")
    decimal = changed(
        frame, "y", pa.array([Decimal("1.234567890123456789")] * 120, type=pa.decimal128(30, 18))
    )
    result = aipw(decimal, folds=2, diagnostics=permissive)
    expected_error = abs(
        Decimal.from_float(float(Decimal("1.234567890123456789"))) - Decimal("1.234567890123456789")
    )
    assert result.provenance["input_precision"]["y"]["max_conversion_error"] == pytest.approx(
        float(expected_error)
    )


def test_observation_exit_incomplete_outcome_refuses_before_fitting(frame):
    incomplete = changed(frame, "complete", pa.array([False] + [True] * 119))
    incomplete = changed(incomplete, "reason", pa.array(["incomplete_observation"] + [None] * 119))
    incomplete = changed(incomplete, "y", pa.array([None] + frame.table["y"].to_pylist()[1:]))
    restored = MaterializedFrame.from_json(incomplete.to_json())
    assert restored.table.num_rows == frame.table.num_rows
    assert restored.table["reason"][0].as_py() == "incomplete_observation"
    refusal(restored, "incomplete_followup")


def test_fail_has_diagnostics_but_no_effect_and_warn_retains_evidence(frame):
    from terrine.results import DiagnosticPolicy

    for method in (aipw, regression_adjustment):
        with pytest.raises(TerrineError) as exc:
            method(frame, folds=2, diagnostics=DiagnosticPolicy(min_arm_ess=1000))
        assert exc.value.code == "terrine.estimate.diagnostic_failure"
        assert "value" not in exc.value.context
        assert exc.value.context["diagnostics"][0]["status"] == "fail"
    result = aipw(
        frame, folds=2, diagnostics=DiagnosticPolicy(min_arm_ess=1, smd_warn=0, smd_fail=20)
    )
    assert any(item.status == "warn" for item in result.diagnostics)
    ra = regression_adjustment(
        frame,
        folds=2,
        nuisance=NuisanceSpec(propensity=LinearTerms(features=())),
        diagnostics=DiagnosticPolicy(min_arm_ess=1, smd_warn=0, smd_fail=1e-12),
    )
    assert ra.diagnostics[1].status == "not_applicable"
    assert ra.diagnostics[0].context["weight_role"] == "diagnostic_only_propensity_reference"


def test_native_imports_do_not_require_warehouse_apis():
    import subprocess
    import sys

    script = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        blocked = {"ibis", "increment"}
        native_modules = (
            "terrine.semantic",
            "terrine.compile",
            "terrine.model",
            "terrine.frame",
            "terrine.spec",
            "terrine.study",
        )
        if fullname.split(".")[0] in blocked or fullname.startswith(native_modules):
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from terrine.estimate import aipw, ipw, regression_adjustment
from terrine.diagnostics import overlap, balance
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True)


def test_native_float_rounding_bounds_include_top_binade(frame):
    value = np.finfo(np.float32).max
    measured = changed(frame, "y", pa.array(np.full(120, value, dtype=np.float32)))
    data = prepare(measured)
    assert data.precision["y"]["max_conversion_error"] == 0
    assert data.precision["y"]["max_input_rounding_bound"] == 2.0**103


def test_unavailable_diagnostic_is_not_a_pass_and_roundtrips(frame):
    from terrine.diagnostics import balance
    from terrine.results import Diagnostic

    diagnostic = balance(frame, log_treatment_weights=np.full(120, np.nan))
    assert diagnostic.status == "unavailable"
    assert diagnostic.context["reason"] == "invalid_complete_adjusted_log_weights"
    assert Diagnostic.from_dict(diagnostic.to_dict()) == diagnostic
