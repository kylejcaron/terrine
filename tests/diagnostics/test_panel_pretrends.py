from dataclasses import replace
from datetime import UTC, datetime

import pytest

from terrine.diagnostics import pretrend_test
from terrine.estimate.stacked import StackedEventStudy


def _study(*, periods=(-3, -2, 0), effects=(0.0, 0.0, 1.0, 0.0, 0.0, 1.0), covariance=None):
    n = len(effects)
    return StackedEventStudy(
        cohorts=(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 2, 1, tzinfo=UTC)),
        periods=periods,
        effects=effects,
        covariance=covariance
        or tuple(tuple(0.04 if i == j else 0.0 for j in range(n)) for i in range(n)),
        standard_errors=tuple(0.2 for _ in effects),
        aggregate_effects={period: 0.0 for period in periods},
        aggregate_standard_errors={period: 0.2 for period in periods},
        cohort_weights={"2025-01-01T00:00:00+00:00": 0.5, "2025-02-01T00:00:00+00:00": 0.5},
        n=40,
        n_clusters=20,
        alpha=0.05,
        provenance={"reference_period": -4},
    )


def test_joint_pretrend_test_uses_all_declared_leads():
    # The second declared lead is individually unremarkable but contributes to
    # the joint test; covariance indexing must include both cohorts and leads.
    effects = (0.0, 0.0, 1.0, 0.0, 0.0, 1.0)
    covariance = tuple(tuple(0.04 if i == j else 0.0 for j in range(6)) for i in range(6))
    result = pretrend_test(_study(effects=effects, covariance=covariance))
    assert result.metrics["n_leads"] == 2
    assert result.metrics["n_cohorts"] == 2
    assert result.metrics["degrees_of_freedom"] == 4
    assert result.status == "warn"
    assert result.context["reason"] == "low_power"
    assert (
        result.context["interpretation"] == "failure_to_reject_is_not_evidence_of_parallel_trends"
    )


def test_diagnostic_status_and_reason_round_trip():
    result = pretrend_test(_study())
    restored = type(result).from_dict(result.to_dict())
    assert restored == result
    assert restored.context["reason"] == "low_power"


def test_insignificant_low_power_pretrend_is_not_pass_or_proof():
    result = pretrend_test(_study())
    assert result.status in {"warn", "unavailable"}
    p_value = result.metrics["p_value"]
    assert isinstance(p_value, float) and p_value > 0.05
    assert result.context["reason"] == "low_power"


def test_well_powered_null_pretrend_remains_warning_not_pass():
    base = _study(effects=(0.0,) * 15)
    result = pretrend_test(
        replace(
            base,
            cohorts=tuple(datetime(2025, month, 1, tzinfo=UTC) for month in range(1, 6)),
            covariance=tuple(tuple(0.04 if i == j else 0.0 for j in range(15)) for i in range(15)),
            standard_errors=(0.2,) * 15,
            n_clusters=80,
        )
    )
    assert result.status == "warn"
    assert result.context["reason"] == "not_rejected"


def test_rank_deficient_joint_covariance_is_unavailable():
    base = _study(effects=(0.0,) * 15)
    covariance = tuple(
        tuple(0.04 if i % 3 == j % 3 and i % 3 < 2 else 0.0 for j in range(15)) for i in range(15)
    )
    effects = (1.0, 0.0, 0.0, -1.0) + (0.0,) * 11
    result = pretrend_test(
        replace(
            base,
            cohorts=tuple(datetime(2025, month, 1, tzinfo=UTC) for month in range(1, 6)),
            effects=effects,
            covariance=covariance,
            standard_errors=(0.2,) * 15,
            n_clusters=80,
        )
    )
    assert result.status == "unavailable"
    assert result.context["reason"] == "singular_joint_covariance"


def test_pretrend_refuses_invalid_alpha():
    with pytest.raises(ValueError):
        pretrend_test(_study(), alpha=1.0)
