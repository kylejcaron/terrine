import pytest

from terrine import retention
from terrine.errors import TerrineError


@pytest.mark.parametrize(
    ("thresholds", "reason"),
    [
        ((), "empty"),
        ((0,), "not_positive_integer"),
        ((True,), "not_positive_integer"),
        ((2**63,), "not_positive_integer"),
        ((2, 1), "not_increasing"),
        ((1, 1), "not_increasing"),
    ],
)
def test_retention_thresholds_require_ordered_positive_int64_counts(thresholds, reason):
    with pytest.raises(TerrineError) as error:
        retention(
            "occurrence",
            label_property="name",
            target_event="target",
            thresholds=thresholds,
            horizon="1d",
        )
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context == {"reason": reason, "field": "thresholds"}


@pytest.mark.parametrize(
    ("horizon", "code"),
    [
        (0, "terrine.design.invalid"),
        (-1, "terrine.design.invalid"),
        (True, "terrine.window.invalid_duration"),
        (1.25, "terrine.window.invalid_duration"),
        ("unknown", "terrine.window.invalid_duration"),
    ],
)
def test_retention_horizon_is_explicit_positive_elapsed_or_open(horizon, code):
    with pytest.raises(TerrineError) as error:
        retention(
            "occurrence",
            label_property="name",
            target_event="target",
            thresholds=(1,),
            horizon=horizon,
        )
    assert error.value.code == code
    assert error.value.context["field"] == "horizon"
