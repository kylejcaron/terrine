from datetime import datetime, timedelta, timezone

import pytest

from terrine.data import BoundCoverage, Budget, Coverage, UnitSource
from terrine.errors import TerrineError
from terrine.model import Model
from terrine.spec import Design, EventAnchor, EventSequence, agg
from terrine.windows import Window


def test_unused_label_source_readiness_is_not_a_model_requirement(native_inputs):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={"events": Coverage("2025-01-01", "2025-01-31")},
    )
    Design(
        "user", EventAnchor("session"), features={"past": agg("sessions", window=("-1d", "0d"))}
    ).build(model)
    with pytest.raises(TerrineError) as error:
        Design(
            "user", EventAnchor("session"), outcomes={"future": agg("churned", window=("0d", "1d"))}
        ).build(model)
    assert error.value.code == "terrine.readiness.missing_source"
    assert error.value.context["source"] == "labels"


def test_model_hash_tracks_observation_and_identity_bindings(native_inputs):
    catalog, con = native_inputs
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}

    def bind(**changes):
        return Model.from_catalog(
            catalog,
            con=con,
            readiness=changes.get("readiness", readiness),
            units=changes.get("units", {"user": UnitSource("users", "user_id")}),
            event_ids=changes.get("event_ids", {"events": "event_id"}),
        )

    base = bind().model_hash
    assert (
        bind(readiness={**readiness, "events": Coverage("2025-01-02", "2025-01-31")}).model_hash
        != base
    )
    assert bind(units={"user": UnitSource("users", "alt_id")}).model_hash != base
    assert bind(event_ids={"events": "alternate_event_id"}).model_hash != base
    offset = timezone(timedelta(hours=-5))
    equivalent = BoundCoverage(
        datetime(2024, 12, 31, 19, tzinfo=offset),
        datetime(2025, 1, 31, 19, tzinfo=offset),
    )
    assert bind(readiness={**readiness, "events": equivalent}).model_hash == base


@pytest.mark.parametrize("budget", [None, Budget(max_rows=1), Budget(max_output_bytes=1024)])
def test_sequence_materialization_requires_positive_row_and_byte_limits(native_inputs, budget):
    catalog, con = native_inputs
    model = Model.from_catalog(
        catalog,
        con=con,
        readiness={name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources},
        units={"user": UnitSource("users", "user_id")},
    )
    frame = Design(
        "user",
        EventAnchor("session"),
        shape=EventSequence("session", Window("0s", "1d"), max_events=2),
    ).build(model)
    with pytest.raises(TerrineError) as error:
        frame.materialize(budget)
    assert error.value.code == "terrine.sequence.budget_required"


def test_inferred_unit_frame_shares_the_explicit_design_identity(native_inputs):
    catalog, con = native_inputs
    readiness = {name: Coverage("2025-01-01", "2025-01-31") for name in catalog.sources}
    features = {"past": agg("sessions", window=("-1d", "0d"))}
    explicit = Design("user", EventAnchor("session"), features=features)
    inferred = Design(None, EventAnchor("session"), features=features)
    one = Model.from_catalog(
        catalog, con=con, readiness=readiness, units={"user": UnitSource("users", "user_id")}
    )
    result = inferred.build(one).materialize()
    assert result.provenance.design_hash == explicit.spec_hash
    assert result.table.equals(explicit.build(one).materialize().table)
    two = Model.from_catalog(
        catalog,
        con=con,
        readiness=readiness,
        units={"user": UnitSource("users", "user_id"), "account": UnitSource("users", "alt_id")},
    )
    with pytest.raises(TerrineError) as error:
        inferred.build(two)
    assert error.value.code == "terrine.model.ambiguous_unit"
    assert error.value.context["choices"] == ("account", "user")
    assert explicit.build(two).materialize().provenance.design_hash == explicit.spec_hash
