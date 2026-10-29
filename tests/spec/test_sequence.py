import json
from typing import cast

import pytest

from terrine.errors import TerrineError
from terrine.spec import (
    Design,
    EventAnchor,
    EventSequence,
    EventTime,
    FunnelSpec,
    SessionSpec,
    from_json,
    from_yaml,
    to_json,
    to_yaml,
)
from terrine.windows import Window


def test_event_sequence_validates_positive_max_events_and_window():
    with pytest.raises(TerrineError) as error:
        EventSequence("purchase", Window("-1d", "0d"), max_events=0)
    assert error.value.code == "terrine.sequence.invalid_spec"
    with pytest.raises(TerrineError):
        EventSequence("purchase", Window("-1d", "0d"), max_events=True)


@pytest.mark.parametrize(
    "property_name",
    [
        "session_index",
        "session_event_count",
        "session_start_time",
        "session_end_time",
        "previous_event_id",
        "previous_event_time",
        "transition_reason",
        "transition",
    ],
)
def test_session_output_names_cannot_be_event_properties(property_name):
    with pytest.raises(TerrineError) as error:
        EventSequence(
            "purchase",
            Window("-1d", "0d"),
            max_events=5,
            properties=(property_name,),
            sessions=SessionSpec("1h"),
        )
    assert error.value.code == "terrine.sequence.invalid_spec"


def test_sequence_request_round_trips_through_json_yaml_and_spec_hash():
    shape = EventSequence(
        "purchase",
        Window("-1d", "0d"),
        max_events=17,
        properties=("amount",),
        sessions=SessionSpec("30m"),
        funnel=FunnelSpec("kind", ("view", "purchase"), within="2h"),
    )
    design = Design("user", EventAnchor("signup"), shape=shape)
    for serialized, decode in ((to_json(design), from_json), (to_yaml(design), from_yaml)):
        restored = decode(serialized)
        assert restored == design
        assert restored.spec_hash == design.spec_hash


def test_event_time_last_round_trips_through_json_yaml_and_spec_hash():
    selected = EventTime("purchase", Window("0s", "7d"), occurrence="last")
    design = Design("user", EventAnchor("signup"), features={"last_purchase": selected})
    for serialized, decode in ((to_json(design), from_json), (to_yaml(design), from_yaml)):
        restored = decode(serialized)
        assert restored == design
        assert restored.spec_hash == design.spec_hash


def test_occurrence_selector_needs_no_label_filter():
    assert EventTime("purchase", Window("0s", "1d")).label_property is None


def test_unknown_sequence_fields_refuse():
    shape = EventSequence("purchase", Window("-1d", "0d"), max_events=2)
    payload = json.loads(to_json(Design("user", EventAnchor("signup"), shape=shape)))
    payload["design"]["shape"]["unexpected"] = 3
    with pytest.raises(TerrineError) as error:
        from_json(json.dumps(payload))
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "malformed_document"


def test_funnel_steps_require_distinct_declared_string_values():
    with pytest.raises(TerrineError):
        FunnelSpec("kind", ("view", "view"))
    with pytest.raises(TerrineError):
        FunnelSpec("kind", cast(tuple[str, ...], ("view", 1)))


def test_session_gap_normalizes_to_integral_seconds():
    assert SessionSpec("30m").gap_seconds == 1800
    with pytest.raises(TerrineError):
        SessionSpec(True)


def test_sequence_property_names_cannot_collide_with_generated_columns():
    with pytest.raises(TerrineError) as error:
        EventSequence("purchase", Window("0s", "1h"), max_events=2, properties=("event_time",))
    assert error.value.code == "terrine.sequence.invalid_spec"
    with pytest.raises(TerrineError):
        EventSequence(
            "purchase", Window("0s", "1h"), max_events=2, properties=("value", "value_reason")
        )
    with pytest.raises(TerrineError):
        EventSequence("purchase", Window("0s", "1h"), max_events=2, properties=("sequence",))
