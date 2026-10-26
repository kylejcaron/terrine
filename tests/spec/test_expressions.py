from dataclasses import FrozenInstanceError

import pytest

from terrine.spec.expressions import (
    And,
    AttributeAsOf,
    Eq,
    Gt,
    IsNull,
    MetricAgg,
    agg,
    all_of,
    asof,
    eq,
    event_time,
    gt,
    is_null,
)


def test_durations_normalize_and_invalid_values_refuse():
    assert (
        agg("sessions", window=("60s", "1h")).window == agg("sessions", window=("1m", "60m")).window
    )
    assert agg("sessions", window=("-24h", "0d")).window.start == -86400
    for duration in ("1.5h", "1h30m", "1mo", "9223372036854775807w"):
        with pytest.raises(ValueError):
            agg("sessions", window=(duration, "0s"))


def test_asof_cutoff_and_predicate_constructors_are_typed_and_immutable():
    metric = agg("sessions", window=("-7d", "0d"))
    cutoff = asof("plan_tier", before="0d")
    assert isinstance(metric, MetricAgg)
    assert cutoff == AttributeAsOf(attribute="plan_tier", before=0)
    assert isinstance(gt(metric, 0), Gt)
    assert isinstance(eq(cutoff, "pro"), Eq)
    assert isinstance(is_null(cutoff), IsNull)
    assert all_of(gt(metric, 0), is_null(cutoff)) == And((gt(metric, 0), is_null(cutoff)))
    with pytest.raises((FrozenInstanceError, AttributeError)):
        metric.metric = "other"  # ty: ignore[invalid-assignment]


def test_predicates_reject_non_json_values_and_empty_and():
    with pytest.raises((TypeError, ValueError)):
        gt(agg("sessions", window=("-1d", "0d")), object())  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError):
        all_of()


def test_last_occurrence_rejects_n():
    with pytest.raises(ValueError):
        event_time("purchase", window=("-7d", "0d"), occurrence="last", n=1)
