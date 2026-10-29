from decimal import Decimal

import pytest

from terrine.spec import (
    BinnedMetricState,
    Design,
    Schedule,
    StateObservations,
    StatePanel,
    agg,
    from_json,
    from_yaml,
    to_json,
    to_yaml,
)


def test_observed_state_levels_and_absorbing_order_are_explicit():
    state = StateObservations("status", levels=("new", "active", "closed"), absorbing=("closed",))
    assert state.levels == ("new", "active", "closed")
    assert state.absorbing == ("closed",)
    with pytest.raises(ValueError):
        StateObservations("status", levels=("new", "new"))
    with pytest.raises(ValueError):
        StateObservations("status", levels=("new", "closed"), absorbing=("other",))


def test_binned_state_preserves_exact_numeric_variants_and_right_closed_contract():
    state = BinnedMetricState(
        "sessions",
        cutpoints=(2**60, Decimal(2**60) + Decimal("0.1")),
        levels=("low", "mid", "high"),
    )
    assert state.cutpoints[0] == 2**60
    assert state.cutpoints[1] == Decimal(2**60) + Decimal("0.1")
    with pytest.raises(ValueError):
        BinnedMetricState("sessions", cutpoints=(1, 1.0), levels=("a", "b", "c"))
    with pytest.raises(ValueError):
        BinnedMetricState("sessions", cutpoints=(float("inf"),), levels=("a", "b"))


def test_cutpoint_decoder_rejects_extra_tags_noncanonical_values_and_nonfinite():
    from terrine.spec.state_panel import decode_cutpoint

    for value in (
        {"type": "int", "value": "1", "extra": True},
        {"type": "int", "value": "01"},
        {"type": "float64", "value": "nan"},
        {"type": "decimal", "sign": 0, "digits": [1], "exponent": 0, "extra": 0},
    ):
        with pytest.raises(ValueError):
            decode_cutpoint(value)


def test_state_panel_design_round_trips_cutpoint_types_in_json_and_yaml():
    panel = StatePanel(
        BinnedMetricState(
            "weekly_sessions",
            cutpoints=(3, Decimal("9.00"), 10.5),
            levels=("inactive", "occasional", "regular", "power"),
        ),
        max_periods=12,
        baseline_features={"plan": agg("plan_size", window=("-1d", "0d"))},
    )
    design = Design("user", Schedule("1w", "2025-01-06", "2025-03-30"), shape=panel)
    for loader, serialized in ((from_json, to_json(design)), (from_yaml, to_yaml(design))):
        restored = loader(serialized)
        assert restored.spec_hash == design.spec_hash
        assert isinstance(restored.shape, StatePanel)
        assert isinstance(restored.shape.state, BinnedMetricState)
        assert isinstance(panel.state, BinnedMetricState)
        assert restored.shape.state.cutpoints == panel.state.cutpoints
        assert tuple(type(value) for value in restored.shape.state.cutpoints) == (
            int,
            Decimal,
            float,
        )


def test_state_panel_requires_positive_period_limit():
    with pytest.raises(ValueError):
        StatePanel(StateObservations("status", levels=("a", "b")), max_periods=0)


def test_observed_state_level_order_and_terminal_definition_round_trip():
    panel = StatePanel(
        StateObservations(
            "status",
            levels=("new", "active", "closed"),
            ordered=True,
            absorbing=("closed",),
        ),
        max_periods=3,
    )
    design = Design("user", Schedule("1d", "2025-01-01", "2025-01-03"), shape=panel)
    for loader, serialized in ((from_json, to_json(design)), (from_yaml, to_yaml(design))):
        restored = loader(serialized)
        assert isinstance(restored.shape, StatePanel)
        assert isinstance(restored.shape.state, StateObservations)
        assert restored.shape.state.levels == ("new", "active", "closed")
        assert restored.shape.state.absorbing == ("closed",)
        assert restored.spec_hash == design.spec_hash
