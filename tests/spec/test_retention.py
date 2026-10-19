import pytest

from terrine import retention


@pytest.mark.parametrize("thresholds", [(), (0,), (2, 1), (1, 1), (True,), (2**63,)])
def test_retention_thresholds_require_ordered_positive_int64_counts(thresholds):
    with pytest.raises((TypeError, ValueError)):
        retention(
            "occurrence",
            label_property="name",
            target_event="target",
            thresholds=thresholds,
            horizon="1d",
        )


@pytest.mark.parametrize("horizon", [0, -1, True, 1.25, "unknown"])
def test_retention_horizon_is_explicit_positive_elapsed_or_open(horizon):
    with pytest.raises((TypeError, ValueError)):
        retention(
            "occurrence",
            label_property="name",
            target_event="target",
            thresholds=(1,),
            horizon=horizon,
        )
