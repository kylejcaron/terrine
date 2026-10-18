from datetime import UTC, datetime, timedelta, timezone

import pytest

from terrine.errors import TerrineError
from terrine.spec import (
    Design,
    EventAnchor,
    EventPanel,
    EventPanelSpec,
    Initiation,
    MatchedT0Sample,
    PersistentAdoption,
    Schedule,
    Sequential,
    SequentialSpec,
    Treatment,
    TrialWeight,
    from_json,
    to_json,
)


def test_matched_t0_policy_is_serialized_and_in_design_hash():
    base = Design("user", Schedule("1d", "2025-01-01", "2025-01-03"))
    sample = MatchedT0Sample(("region", "channel"), controls_per_treated=2, seed=17)
    design = Design(
        base.unit,
        base.anchor,
        shape=SequentialSpec(
            Initiation("events", "started", datetime(2024, 1, 1, tzinfo=UTC)),
            TrialWeight("equal_trial"),
        ),
        sample=sample,
    )

    restored = from_json(to_json(design))

    assert restored == design
    assert restored.spec_hash == design.spec_hash
    assert restored.sample == sample


def test_sequential_schedule_spec_round_trips_canonically():
    design = Design("customer_id", Schedule(7, "2025-01-01", "2025-01-15"))
    treatment = Initiation("events", "started", history_start=datetime(2024, 1, 1, tzinfo=UTC))
    sequential = Sequential(design, treatment=treatment)

    assert from_json(to_json(sequential)) == sequential
    assert sequential.spec_hash == from_json(to_json(sequential)).spec_hash


def test_calendar_panel_is_canonical_sequential_composition():
    from terrine.spec import CalendarPanel

    design = Design("customer_id", Schedule(7, "2025-01-01", "2025-01-15"))
    treatment = Initiation("events", "started", history_start=datetime(2024, 1, 1, tzinfo=UTC))

    assert CalendarPanel(Sequential(design, treatment=treatment)) == Sequential(
        design, treatment=treatment
    )


def test_event_panel_emits_only_declared_relative_periods():
    base = Design("customer_id", EventAnchor("purchase", "first"))
    panel = EventPanel(base, periods=(-1, 0, 2), period_seconds=86_400)

    assert isinstance(panel.shape, EventPanelSpec)
    assert panel.shape.periods == (-1, 0, 2)


def test_treatment_initiation_and_persistent_state_are_distinct():
    initiation = Initiation("events", "started", history_start=datetime(2024, 1, 1, tzinfo=UTC))
    persistent = PersistentAdoption(
        "events",
        "started",
        adopted_value="yes",
        history_start=datetime(2024, 1, 1, tzinfo=UTC),
        persistence="absorbing",
    )

    assert initiation != persistent


def test_conflicting_design_inputs_refuse_before_hash():
    from terrine.errors import TerrineError
    from terrine.spec import Treatment

    base = Design(
        "customer_id", Schedule(7, "2025-01-01", "2025-01-15"), treatment=Treatment("started")
    )
    with pytest.raises(TerrineError) as error:
        Sequential(
            base,
            treatment=Initiation(
                "events", "started", history_start=datetime(2024, 1, 1, tzinfo=UTC)
            ),
        )

    assert error.value.code == "terrine.design.conflicting_treatment"


def test_explicit_trial_weights_normalize_without_overflow():
    weights = TrialWeight("explicit", {"a": 1e308, "b": 1e308})

    assert weights.values == {"a": 0.5, "b": 0.5}


def test_explicit_trial_weight_round_trips_in_design_hash():
    base = Design("user", Schedule("1d", "2025-01-01", "2025-01-03"))
    treatment = Initiation("events", "started", datetime(2024, 1, 1, tzinfo=UTC))
    design = Sequential(
        base,
        treatment=treatment,
        trial_weight=TrialWeight("explicit", {"later": 2.0, "earlier": 1.0}),
    )

    assert from_json(to_json(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash


def test_explicit_trial_weights_round_trip_without_mass_drift():
    base = Design("user", Schedule("1d", "2025-01-01", "2025-01-03"))
    design = Sequential(
        base,
        treatment=Initiation("events", "started", datetime(2024, 1, 1, tzinfo=UTC)),
        trial_weight=TrialWeight("explicit", {"a": 1, "b": 5, "c": 7}),
    )

    restored = from_json(to_json(design))

    assert restored == design
    assert restored.spec_hash == design.spec_hash


def test_explicit_trial_weights_refuse_nonfinite_values():
    with pytest.raises(ValueError):
        TrialWeight("explicit", {"a": float("nan")})


def test_treatment_history_instants_canonicalize_to_utc():
    first = Initiation(
        "events", "started", datetime(2025, 1, 1, tzinfo=timezone(timedelta(hours=2)))
    )
    second = Initiation("events", "started", datetime(2024, 12, 31, 22, tzinfo=UTC))

    assert first == second
    assert first.history_start.tzinfo is UTC


def test_direct_design_construction_cannot_conflict_with_sequential_treatment():
    history = Initiation("events", "started", datetime(2024, 1, 1, tzinfo=UTC))

    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            Schedule("1d", "2025-01-01", "2025-01-02"),
            treatment=Treatment("started"),
            shape=SequentialSpec(history),
        )

    assert getattr(error.value, "code", None) == "terrine.design.conflicting_treatment"


def test_design_json_without_optional_anchor_range_loads_with_sample_defaults():
    restored = from_json(
        '{\"schema_version\":1,\"design\":{\"unit\":\"user\",\"anchor\":'
        '{\"type\":\"FixedDate\",\"date\":{\"type\":\"date\",\"value\":\"2025-01-01\"}}}}'
    )

    assert restored.anchor_range is None
    assert restored.sample is None
