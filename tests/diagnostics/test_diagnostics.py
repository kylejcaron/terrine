from datetime import UTC, datetime

import numpy as np
import pyarrow as pa
import pytest

from terrine.data import ExecutionSnapshot, MaterializedFrame, Provenance, Roles
from terrine.diagnostics import balance, overlap
from terrine.results import DiagnosticPolicy


def diagnostic_frame(x=(0.0, 2.0, 1.0, 3.0), q=(1.0, 2.0, 3.0, 4.0)):
    return MaterializedFrame(
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "anchor": [1, 2, 3, 4],
                "t0": [datetime(2025, 1, 1, tzinfo=UTC)] * 4,
                "a": [0, 0, 1, 1],
                "x": x,
                "q": q,
            }
        ),
        Roles("id", "anchor", "t0", "a", ("x",), (), weight="q"),
        {},
        Provenance(
            {},
            {},
            "manual",
            "manual",
            {},
            ExecutionSnapshot(None, "arrow", {}, None, "unverified"),
            "cross_section",
            "baseline_binary",
        ),
    )


def test_overlap_uses_target_and_treatment_weights():
    frame = diagnostic_frame()
    e = np.array([0.2, 0.4, 0.6, 0.8])
    d = overlap(
        frame,
        log_propensity=np.log(e),
        log_complement=np.log1p(-e),
        policy=DiagnosticPolicy(min_arm_ess=1),
    )
    assert d.status == "pass"
    for arm, weights in [
        (0, np.array([1 / 0.8, 2 / 0.6])),
        (1, np.array([3 / 0.6, 4 / 0.8])),
    ]:
        assert d.metrics[f"arm{arm}_ess"] == pytest.approx(weights.sum() ** 2 / (weights @ weights))
    assert sum(d.context["histograms"]["arm0"]) == pytest.approx(1)
    assert d.metrics["fail_tail_share"] == 0
    eta = np.array([-1000.0, 0.0, 0.0, 1000.0])
    extreme = overlap(
        frame,
        log_propensity=-np.logaddexp(0, -eta),
        log_complement=-np.logaddexp(0, eta),
        policy=DiagnosticPolicy(min_arm_ess=1),
    )
    assert extreme.status == "fail"
    boundary = np.array([0.01, 0.05, 0.95, 0.99])
    d = overlap(
        frame,
        log_propensity=np.log(boundary),
        log_complement=np.log1p(-boundary),
        policy=DiagnosticPolicy(min_arm_ess=1),
    )
    assert d.metrics["fail_tail_share"] == 0
    assert d.status == "warn"


def test_balance_uses_fixed_preweight_scale_and_q_once():
    frame = diagnostic_frame()
    r = np.array([2.0, 1.0, 1.0, 2.0])
    d = balance(
        frame,
        log_treatment_weights=np.log(r),
        policy=DiagnosticPolicy(smd_warn=0.1, smd_fail=0.25),
    )
    before = (15 / 7 - 4 / 3) / np.sqrt(2)
    after = (7 / 3 - 2 / 3) / np.sqrt(2)
    assert d.metrics["x:before"] == pytest.approx(before)
    assert d.metrics["x:after"] == pytest.approx(after)
    assert d.status == "fail"
    raw = balance(frame)
    assert raw.status == "not_applicable" and raw.metrics["x:after"] is None
    assert raw.context["metric_reasons"]["x:after"] == "no_balance_adjustment"
    constant = balance(diagnostic_frame((0.0, 0.0, 1.0, 1.0)), log_treatment_weights=np.zeros(4))
    assert constant.status == "fail" and constant.metrics["x:after"] is None
    assert constant.context["metric_reasons"]["x:after"] == "zero_scale_unequal_means"
    equal = balance(diagnostic_frame((1.0, 1.0, 1.0, 1.0)), log_treatment_weights=np.zeros(4))
    assert equal.metrics["x:after"] == 0
    categories = balance(diagnostic_frame(("a", "b", "a", "b")), log_treatment_weights=np.zeros(4))
    assert {k for k in categories.metrics if k.endswith(":after")} == {
        'x[level="a"]:after',
        'x[level="b"]:after',
    }


def test_balance_gates_only_after_and_uses_strict_boundaries():
    frame = diagnostic_frame((0.0, 2.0, 0.0, 2.0), (1.0, 1.0, 1.0, 1.0))
    r = np.array([3.0, 1.0, 1.0, 3.0])
    value = 1 / np.sqrt(2)
    assert (
        balance(
            frame,
            log_treatment_weights=np.log(r),
            policy=DiagnosticPolicy(smd_warn=0.1, smd_fail=value),
        ).status
        == "warn"
    )
    assert (
        balance(
            frame,
            log_treatment_weights=np.log(r),
            policy=DiagnosticPolicy(smd_warn=value, smd_fail=1),
        ).status
        == "pass"
    )
    large_before = diagnostic_frame((0.0, 2.0, 0.0, 2.0), (3.0, 1.0, 1.0, 3.0))
    assert balance(large_before, log_treatment_weights=np.zeros(4)).status == "pass"


def test_balance_is_invariant_to_large_common_log_weight_offsets():
    frame = diagnostic_frame(q=(1.0, 1.0, 1.0, 1.0))
    policy = DiagnosticPolicy(smd_warn=0.8, smd_fail=1.0)
    for offset in (-1e308, 0.0, 1e308):
        result = balance(frame, log_treatment_weights=np.full(4, offset), policy=policy)
        assert result.metrics["x:after"] == pytest.approx(1 / np.sqrt(2))
        assert result.status == "pass"


def test_large_integer_feature_never_collapses_during_balance_conversion():
    frame = diagnostic_frame(
        pa.array([2**53, 2**53, 2**53 + 1, 2**53 + 1], type=pa.int64()),
        (1.0, 1.0, 1.0, 1.0),
    )
    result = balance(frame, log_treatment_weights=np.zeros(4))
    assert result.status == "unavailable"
    assert result.context["reason"] == "integer_outside_exact_float64_domain"


def test_low_arm_ess_fails_without_suppressing_diagnostic_evidence():
    d = overlap(
        diagnostic_frame(), log_propensity=np.log([0.5] * 4), log_complement=np.log([0.5] * 4)
    )
    assert d.status == "fail"
