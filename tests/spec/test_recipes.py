from __future__ import annotations

import copy
import pickle
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

import terrine as tr
from terrine import recipes
from terrine.errors import TerrineError
from terrine.spec import (
    AnchorRange,
    BinnedMetricState,
    CohortSpec,
    Design,
    EventAnchor,
    EventPanel,
    EventSequence,
    Initiation,
    LifeTable,
    PersistentAdoption,
    Schedule,
    Sequential,
    SessionSpec,
    StateObservations,
    StatePanel,
    TimeToEvent,
    Trajectory,
    Treatment,
    TrialWeight,
    agg,
    asof,
    event_time,
    from_json,
    gt,
    to_json,
)
from terrine.windows import Window

_FEATURES = {"region": asof("region", before="-1d")}
_OUTCOMES = {"spend": agg("revenue", window=("0d", "7d"))}
_SHARED: dict[str, Any] = {
    "features": _FEATURES,
    "eligibility": {"active": gt(agg("visits", window=("-8d", "-1d")), 0)},
    "anchor_range": AnchorRange("2025-01-02", "2025-02-01"),
    "gap": "1d",
}
_SCHEDULE: dict[str, Any] = {"every": "7d", "start": date(2025, 1, 1), "end": "2025-02-01"}
_TREATMENT = Treatment("plan", at="-1d")
_INITIATION = Initiation("started", history_start=datetime(2024, 1, 1, tzinfo=UTC))
_CUTOFF = datetime(2025, 4, 1, tzinfo=UTC)
_REQUIRED: dict[str, dict[str, Any]] = {
    "time_series": _SCHEDULE,
    "trial_panel": {**_SCHEDULE, "treatment": _INITIATION},
    "event_panel": {"event": "purchase", "periods": (-1, 0, 2), "period": "2d"},
    "ltv": {"start_event": "purchase", "metric": "revenue", "period": "7d", "periods": 4},
    "survival": {"start_event": "signup", "end_event": "churn"},
    "life_table": {"start_event": "signup", "end_event": "churn", "bin": "7d"},
    "state_panel": {
        **_SCHEDULE,
        "state_source": StateObservations("health", ("well", "ill")),
        "periods": 5,
    },
    "event_sequence": {"event": "purchase", "horizon": "7d", "max_events": 9},
}


def _assert_equivalent(actual, expected):
    assert type(actual) is Design
    assert actual == expected
    assert actual.spec_hash == expected.spec_hash
    restored = from_json(to_json(actual))
    assert restored == expected
    assert restored.spec_hash == expected.spec_hash


def test_time_series_is_a_plain_scheduled_design_with_all_supported_roles():
    _assert_equivalent(
        recipes.time_series(
            "user", **_SCHEDULE, **_SHARED, outcomes=_OUTCOMES, treatment=_TREATMENT
        ),
        Design(
            "user",
            Schedule("7d", "2025-01-01", "2025-02-01"),
            **_SHARED,
            outcomes=_OUTCOMES,
            treatment=_TREATMENT,
        ),
    )


def test_time_series_accepts_typed_boolean_features_and_outcomes() -> None:
    feature = gt(agg("visits", window=("-7d", "-1d")), 0)
    outcome = gt(agg("revenue", window=("0d", "7d")), 0)
    design = recipes.time_series(
        "user",
        every="7d",
        start="2025-01-01",
        end="2025-02-01",
        features={"active": feature},
        outcomes={"converted": outcome},
    )
    assert design.features["active"] == feature
    assert design.outcomes["converted"] == outcome


@pytest.mark.parametrize(
    "treatment",
    [
        _INITIATION,
        PersistentAdoption(
            "plan",
            adopted_value="paid",
            history_start=datetime(2024, 1, 1, tzinfo=UTC),
            persistence="absorbing",
        ),
    ],
)
def test_trial_panel_matches_scheduled_sequential_design(treatment):
    weight = TrialWeight("equal_trial")
    _assert_equivalent(
        recipes.trial_panel(
            "user",
            **_SCHEDULE,
            **_SHARED,
            treatment=treatment,
            trial_weight=weight,
            anticipation="2d",
            reference_period="-3d",
            outcomes=_OUTCOMES,
        ),
        Sequential(
            Design("user", Schedule(**_SCHEDULE), **_SHARED, outcomes=_OUTCOMES),
            treatment=treatment,
            trial_weight=weight,
            anticipation="2d",
            reference_period="-3d",
        ),
    )


def test_event_panel_matches_event_anchor_and_relative_period_design():
    _assert_equivalent(
        recipes.event_panel(
            "user",
            **_REQUIRED["event_panel"],
            **_SHARED,
            occurrence="nth",
            n=2,
            reference_period=0,
            outcomes=_OUTCOMES,
        ),
        EventPanel(
            Design("user", EventAnchor("purchase", "nth", 2), **_SHARED, outcomes=_OUTCOMES),
            periods=(-1, 0, 2),
            period_seconds=172_800,
            reference_period=0,
        ),
    )


def test_ltv_matches_cumulative_cohort_design_with_origin_and_treatment():
    _assert_equivalent(
        recipes.ltv(
            "user",
            **_REQUIRED["ltv"],
            **_SHARED,
            treatment=_TREATMENT,
            administrative_cutoff=_CUTOFF,
        ),
        Design(
            "user",
            EventAnchor("purchase"),
            shape=CohortSpec("purchase", Trajectory("revenue", "7d", 4, cumulative=True)),
            **_SHARED,
            treatment=_TREATMENT,
            administrative_cutoff=_CUTOFF,
        ),
    )


def test_survival_matches_time_to_event_design_with_absolute_cutoff():
    _assert_equivalent(
        recipes.survival(
            "user",
            **_REQUIRED["survival"],
            **_SHARED,
            occurrence="nth",
            n=3,
            treatment=_TREATMENT,
            administrative_cutoff=_CUTOFF,
        ),
        Design(
            "user",
            EventAnchor("signup", "nth", 3),
            shape=TimeToEvent("churn"),
            **_SHARED,
            treatment=_TREATMENT,
            administrative_cutoff=_CUTOFF,
        ),
    )


def test_life_table_matches_explicit_feature_grouped_design():
    _assert_equivalent(
        recipes.life_table(
            "user",
            **_REQUIRED["life_table"],
            **_SHARED,
            group_by=("region",),
            occurrence="nth",
            n=2,
            administrative_cutoff=_CUTOFF,
        ),
        Design(
            "user",
            EventAnchor("signup", "nth", 2),
            shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
            **_SHARED,
            administrative_cutoff=_CUTOFF,
        ),
    )


@pytest.mark.parametrize(
    "source",
    [
        StateObservations("health", ("well", "ill")),
        BinnedMetricState("revenue", (0,), ("none", "positive")),
    ],
)
def test_state_panel_matches_bounded_scheduled_state_design(source):
    _assert_equivalent(
        recipes.state_panel("user", **_SCHEDULE, state_source=source, periods=5, **_SHARED),
        Design("user", Schedule(**_SCHEDULE), shape=StatePanel(source, max_periods=5), **_SHARED),
    )


def test_event_sequence_matches_bounded_session_design():
    _assert_equivalent(
        recipes.event_sequence(
            "user",
            **_REQUIRED["event_sequence"],
            **_SHARED,
            occurrence="each",
            session_gap="30m",
            outcomes=_OUTCOMES,
        ),
        Design(
            "user",
            EventAnchor("purchase", "each"),
            shape=EventSequence(
                "purchase", Window(0, "7d"), max_events=9, sessions=SessionSpec("30m")
            ),
            **_SHARED,
            outcomes=_OUTCOMES,
        ),
    )


@pytest.mark.parametrize("name", _REQUIRED)
def test_row_defining_arguments_cannot_be_omitted(name):
    constructor = getattr(recipes, name)
    arguments = {"unit": "user", **_REQUIRED[name]}
    for missing in arguments:
        supplied = {key: value for key, value in arguments.items() if key != missing}
        with pytest.raises(TypeError):
            constructor(**supplied)


@pytest.mark.parametrize(
    ("name", "unsupported"),
    [
        ("time_series", ("periods", "period", "administrative_cutoff")),
        ("trial_panel", ("periods", "period", "administrative_cutoff")),
        ("event_panel", ("treatment", "administrative_cutoff")),
        ("ltv", ("outcomes", "occurrence", "n")),
        ("survival", ("outcomes", "horizon", "periods")),
        ("life_table", ("outcomes", "treatment", "horizon")),
        ("state_panel", ("outcomes", "treatment", "administrative_cutoff")),
        ("event_sequence", ("treatment", "administrative_cutoff")),
    ],
)
def test_shape_unsupported_keywords_are_not_silently_dropped(name, unsupported):
    constructor = getattr(recipes, name)
    for keyword in (*unsupported, "unknown_option"):
        with pytest.raises(TypeError):
            constructor("user", **_REQUIRED[name], **{keyword: None})


@pytest.mark.parametrize("name", ["event_panel", "survival", "life_table", "event_sequence"])
def test_event_occurrence_defaults_to_first_and_nth_is_one_based(name):
    constructor = getattr(recipes, name)
    fact = _REQUIRED[name].get("event", _REQUIRED[name].get("start_event"))
    assert constructor("user", **_REQUIRED[name]).anchor == EventAnchor(fact, "first")
    assert constructor("user", **_REQUIRED[name], occurrence="nth", n=3).anchor == EventAnchor(
        fact, "nth", 3
    )
    for occurrence, n in (("nth", None), ("nth", 0), ("nth", True), ("first", 1), ("last", None)):
        with pytest.raises(ValueError):
            constructor("user", **_REQUIRED[name], occurrence=occurrence, n=n)


@pytest.mark.parametrize("name", ["event_panel", "event_sequence"])
def test_repeatable_event_shapes_accept_each_without_n(name):
    constructor = getattr(recipes, name)
    assert constructor("user", **_REQUIRED[name], occurrence="each").anchor == EventAnchor(
        "purchase", "each"
    )
    with pytest.raises(ValueError):
        constructor("user", **_REQUIRED[name], occurrence="each", n=1)


@pytest.mark.parametrize("name", ["survival", "life_table"])
def test_single_entry_shapes_refuse_each_with_portable_coded_context(name):
    with pytest.raises(TerrineError) as error:
        getattr(recipes, name)("user", **_REQUIRED[name], occurrence="each")
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context
    for restored in (copy.deepcopy(error.value), pickle.loads(pickle.dumps(error.value))):
        assert restored.code == error.value.code
        assert restored.context == error.value.context
    with pytest.raises(TypeError):
        cast(dict[str, Any], error.value.context)["reason"] = "changed"


@pytest.mark.parametrize(
    ("features", "groups"),
    [(_FEATURES, ()), (_FEATURES, ("region", "channel")), ({}, ("region",))],
)
def test_life_table_features_must_exactly_match_explicit_groups(features, groups):
    with pytest.raises(ValueError):
        recipes.life_table("user", **_REQUIRED["life_table"], features=features, group_by=groups)


@pytest.mark.parametrize("name", ["time_series", "trial_panel", "state_panel"])
def test_schedules_accept_calendar_dates_not_timestamps(name):
    constructor = getattr(recipes, name)
    for bound in ("start", "end"):
        for timestamp in (datetime(2025, 1, 10, tzinfo=UTC), "2025-01-10T00:00:00Z"):
            with pytest.raises((TypeError, ValueError)):
                constructor("user", **{**_REQUIRED[name], bound: timestamp})


def test_conditional_events_remain_low_level_expression_composition():
    selected = event_time("purchase", window=("-30d", "0d"), occurrence="last")
    conditional = event_time("refund", window=("0d", "7d"), relative_to=selected)
    design = Design(
        "user",
        EventAnchor("renewal"),
        outcomes={"refund_at": conditional},
    )
    restored = from_json(to_json(design))
    assert restored.outcomes["refund_at"] == conditional
    assert conditional.relative_to == selected
    assert restored.spec_hash == design.spec_hash


def test_recipes_module_is_exposed_without_individual_root_aliases():
    assert tr.recipes is recipes
    assert all(name not in tr.__all__ for name in _REQUIRED)
