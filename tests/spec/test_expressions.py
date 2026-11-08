from dataclasses import FrozenInstanceError

import pytest

from terrine.spec import (
    And,
    Eq,
    Gt,
    IsIn,
    IsNull,
    Ne,
    Not,
    Or,
    agg,
    all_of,
    any_of,
    asof,
    eq,
    gt,
    is_null,
    isin,
    lt,
    ne,
    not_,
)
from terrine.spec.expressions import AttributeAsOf, MetricAgg


def test_durations_normalize_and_invalid_values_refuse():
    assert (
        agg("sessions", window=("60s", "1h")).window
        == agg("sessions", window=("1min", "60min")).window
    )
    assert agg("sessions", window=("-24h", "0d")).window.start.seconds == -86400
    for duration in ("1.5h", "1h30m", "1mo", "9223372036854775807w"):
        with pytest.raises(ValueError):
            agg("sessions", window=(duration, "0s"))


def test_asof_cutoff_and_predicate_constructors_are_typed_and_immutable():
    metric = agg("sessions", window=("-7d", "0d"))
    cutoff = asof("plan_tier", before="0d")
    assert isinstance(metric, MetricAgg)
    assert cutoff == AttributeAsOf(attribute="plan_tier", before="0s")
    assert isinstance(gt(metric, 0), Gt)
    equal = eq(expr=cutoff, value="pro")
    assert equal == Eq(expr=cutoff, value="pro")
    assert equal.expr == cutoff
    assert isinstance(is_null(cutoff), IsNull)
    assert all_of(gt(metric, 0), is_null(cutoff)) == And((gt(metric, 0), is_null(cutoff)))
    with pytest.raises((FrozenInstanceError, AttributeError)):
        metric.metric = "other"  # ty: ignore[invalid-assignment]


def test_membership_and_boolean_predicates_normalize_their_operands():
    metric = agg("orders", window=("-30d", "0s"))
    plan = asof("plan_tier")
    assert isin(plan, ["pro", "plus"]) == IsIn(plan, ("pro", "plus"))
    # A set has no order; a stable one keeps equal designs hashing equally.
    assert isin(plan, {"pro", "plus"}) == isin(plan, frozenset({"plus", "pro"}))
    low = lt(metric, 3)
    assert all_of(not_(low), any_of(low, ne(plan, "free"))).terms == (
        Not(low),
        Or((low, Ne(plan, "free"))),
    )
