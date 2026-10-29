import json
from datetime import UTC, date, datetime
from typing import cast

import pandas as pd
import pytest

from terrine.errors import TerrineError
from terrine.spec.anchors import AnchorRange, EventAnchor, FixedDate, Schedule
from terrine.spec.design import Design, Treatment
from terrine.spec.expressions import agg, all_of, asof, eq, gt, is_null
from terrine.spec.io import from_json, from_yaml, to_json, to_yaml
from terrine.spec.trajectory import CohortGrouping, CohortSpec, Trajectory


def test_subday_duration_forms_share_canonical_spec_hashes():
    anchor = FixedDate("2025-03-01")
    for left, right in (("60s", "1m"), ("60m", "1h"), ("24h", "1d")):
        one = Design(
            unit="user",
            anchor=anchor,
            features={"visits": agg("sessions", window=(f"-{left}", "0s"))},
        )
        two = Design(
            unit="user",
            anchor=anchor,
            features={"visits": agg("sessions", window=(f"-{right}", "0d"))},
        )
        assert one.spec_hash == two.spec_hash


def test_anchor_variants_and_design_maps_round_trip_as_safe_yaml_and_json():
    anchors = (
        FixedDate("2025-03-01"),
        EventAnchor("feature_enabled", which="nth", n=2),
        Schedule(every="1w", start="2025-01-06", end="2025-02-02"),
    )
    for anchor in anchors:
        design = Design(
            unit="user",
            anchor=anchor,
            features={
                "visits": agg("sessions", window=("-28d", "0d")),
                "plan_before": asof("plan_tier", before="0s"),
            },
            outcomes={"future": agg("sessions", window=("0d", "7d"))},
            eligibility={
                "active_paid": all_of(
                    gt(agg("sessions", window=("-28d", "0d")), 0),
                    eq(asof("plan_tier", before="0d"), "plus"),
                    is_null(asof("plan_tier", before="0d"), negate=True),
                )
            },
            treatment=Treatment(attribute="assigned", known_at_t0=True),
        )
        assert from_yaml(to_yaml(design)) == design
        assert from_json(to_json(design)) == design
        assert from_yaml(to_yaml(design)).spec_hash == design.spec_hash


def test_unit_null_round_trips():
    design = Design(None, FixedDate("2025-01-01"))
    assert design.unit is None
    assert '"unit":null' in to_json(design)
    assert from_json(to_json(design)) == design
    assert from_yaml(to_yaml(design)).unit is None
    assert from_yaml(to_yaml(design)).spec_hash == design.spec_hash
    assert design.spec_hash != Design("user", FixedDate("2025-01-01")).spec_hash
    with pytest.raises(TerrineError) as error:
        from_json(to_json(design).replace('"unit":null', '"unit":1'))
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "malformed_document"


def _versioned(design, version):
    document = json.loads(to_json(design))
    document["schema_version"] = version
    return json.dumps(document)


def test_v1_design_document_refuses_with_a_coded_reason():
    with pytest.raises(TerrineError) as error:
        from_json(_versioned(Design("user", EventAnchor("purchase")), 1))
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "unsupported_schema_version"
    assert error.value.context["field"] == "schema_version"


def test_malformed_document_is_a_coded_refusal():
    for load, document in ((from_json, "{"), (from_yaml, "design: [\n")):
        with pytest.raises(TerrineError) as error:
            load(document)
        assert error.value.code == "terrine.design.invalid"
        assert error.value.context["reason"] == "malformed_document"


def test_event_anchor_offset_preserves_saved_design_identity_and_round_trips():
    saved = (
        '{"design":{"administrative_cutoff":null,"anchor":{"event":"purchase","n":2,'
        '"type":"EventAnchor","which":"nth"},"anchor_range":null,"eligibility":{},'
        '"features":{},"gap":0,"outcomes":{},"sample":null,"shape":"cross_section",'
        '"treatment":null,"unit":"user"},"schema_version":2}'
    )
    original = from_json(saved)
    assert to_json(original) == saved
    assert original.spec_hash == "220623d871a835f6579151a7503d759b018c2e538c89663ffed82b53517e0db6"
    assert Design("user", EventAnchor("purchase", "nth", 2, offset="0d")) == original

    shifted = Design("user", EventAnchor("purchase", "nth", 2, offset="1d"))
    assert isinstance(shifted.anchor, EventAnchor)
    assert shifted.anchor.offset == 86_400
    assert shifted.spec_hash != original.spec_hash
    assert shifted == Design("user", EventAnchor("purchase", "nth", 2, offset=86_400))
    assert json.loads(to_json(shifted))["design"]["anchor"]["offset"] == 86_400
    for restored in (from_json(to_json(shifted)), from_yaml(to_yaml(shifted))):
        assert restored == shifted
        assert restored.spec_hash == shifted.spec_hash


def test_predicate_outcome_round_trips_json():
    design = Design(
        "user",
        FixedDate("2025-01-01"),
        features={"missing": is_null(asof("tier", before=0))},
        outcomes={
            "converted_7d": gt(agg("orders", window=("0s", "7d")), 0),
            "churned_30d": eq(agg("orders", window=("0s", "30d")), 0),
            "both": all_of(
                gt(agg("orders", window=("0s", "7d")), 0),
                eq(agg("orders", window=("7d", "30d")), 0),
            ),
        },
    )
    for restored in (from_json(to_json(design)), from_yaml(to_yaml(design))):
        assert restored == design
        assert restored.spec_hash == design.spec_hash


def test_schedule_uses_positive_elapsed_cadence_and_ordered_dates():
    schedule = Schedule(every="1w", start="2025-01-06", end="2025-02-02")
    assert (schedule.every, schedule.months) == (7 * 86400, None)
    assert schedule.end == date(2025, 2, 2)
    assert Schedule(every="1d", start="2025-01-06", end="2025-01-06").end == date(2025, 1, 6)
    for every in ("0d", "0mo", "0y"):
        with pytest.raises(TerrineError) as error:
            Schedule(every=every, start="2025-01-06", end="2025-02-02")
        assert error.value.code == "terrine.design.invalid"
        assert (error.value.context["reason"], error.value.context["field"]) == (
            "not_positive",
            "every",
        )


def test_month_schedule_round_trips_months():
    design = Design("user", Schedule("3mo", "2025-01-31", "2025-12-31"))
    for restored in (from_json(to_json(design)), from_yaml(to_yaml(design))):
        assert isinstance(restored.anchor, Schedule)
        assert (restored.anchor.months, restored.anchor.every) == (3, None)
        assert restored.spec_hash == design.spec_hash
    document = json.loads(to_json(design))
    anchor = document["design"]["anchor"]
    assert (anchor["every"], anchor["months"]) == (None, 3)
    elapsed = json.loads(to_json(Design("user", Schedule("4w", "2025-01-31", "2025-12-31"))))
    assert (elapsed["design"]["anchor"]["every"], elapsed["design"]["anchor"]["months"]) == (
        4 * 7 * 86400,
        None,
    )
    for every, months in ((1, 1), (None, None), (0, None), (None, 0), (None, True)):
        malformed = {**document, "design": {**document["design"]}}
        malformed["design"]["anchor"] = {**anchor, "every": every, "months": months}
        with pytest.raises(TerrineError) as error:
            from_json(json.dumps(malformed))
        assert error.value.code == "terrine.design.invalid"


def test_design_named_maps_are_detached_and_read_only():
    features = {"visits": agg("sessions", window=("-1d", "0d"))}
    design = Design(unit="user", anchor=FixedDate("2025-03-01"), features=features)
    features.clear()
    assert "visits" in design.features
    with pytest.raises(TypeError):
        design.features["other"] = agg("other", window=("-1d", "0d"))  # ty: ignore[invalid-assignment]


def test_mapping_order_does_not_change_hash_and_is_canonical():
    first = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        features={
            "z": agg("z_metric", window=("-1d", "0d")),
            "a": agg("a_metric", window=("-1d", "0d")),
        },
    )
    second = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        features={
            "a": agg("a_metric", window=("-1d", "0d")),
            "z": agg("z_metric", window=("-1d", "0d")),
        },
    )
    assert first.spec_hash == second.spec_hash
    assert to_yaml(first) == to_yaml(second)
    assert to_json(first) == to_json(second)


def test_anchor_range_preserves_date_and_aware_instant_tags():
    spec = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        anchor_range=AnchorRange(start=datetime(2025, 3, 1, 1, tzinfo=UTC), end=date(2025, 3, 1)),
    )
    loaded = from_yaml(to_yaml(spec))
    assert loaded == spec
    assert loaded.anchor_range is not None
    assert loaded.anchor_range.start == datetime(2025, 3, 1, 1, tzinfo=UTC)
    assert isinstance(loaded.anchor_range.end, date) and not isinstance(
        loaded.anchor_range.end, datetime
    )


def test_fixed_date_preserves_intraday_aware_instants_in_safe_round_trips():
    instant = datetime(2025, 3, 1, 12, 34, 56, 123456, tzinfo=UTC)
    design = Design(unit="user", anchor=FixedDate(instant))

    for loaded in (from_json(to_json(design)), from_yaml(to_yaml(design))):
        assert loaded == design
        assert isinstance(loaded.anchor, FixedDate)
        assert loaded.anchor.date == instant

    with pytest.raises(TerrineError) as error:
        from_json(to_json(design).replace("123456+00:00", "123456789+00:00"))
    assert error.value.context["reason"] == "malformed_document"


def test_fixed_date_normalizes_offset_equivalent_instants_for_canonical_hashes():
    utc = FixedDate(datetime(2025, 3, 1, 12, 34, 56, 123456, tzinfo=UTC))
    offset = FixedDate("2025-03-01T14:34:56.123456000+02:00")
    first = Design(unit="user", anchor=utc)
    second = Design(unit="user", anchor=offset)

    assert utc.date == offset.date
    assert to_json(first) == to_json(second)
    assert first.spec_hash == second.spec_hash


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (datetime(2025, 3, 1, 12, 34), "naive_datetime"),
        ("2025-03-01T12:34:56.123456789Z", "invalid_instant"),
        ("2025-03-01T12:34:56", "invalid_instant"),
    ],
)
def test_fixed_date_rejects_naive_and_submicrosecond_instants(value, reason):
    with pytest.raises(TerrineError) as error:
        FixedDate(value)
    assert error.value.code == "terrine.design.invalid"
    assert (error.value.context["reason"], error.value.context["field"]) == (reason, "date")


def test_treatment_known_at_t0_metadata_is_explicit():
    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        treatment=Treatment(attribute="assigned", known_at_t0=True),
    )
    loaded = from_yaml(to_yaml(design))
    assert loaded.treatment is not None
    assert loaded.treatment.known_at_t0 is True


@pytest.mark.parametrize(
    ("load", "document", "reason"),
    [
        (from_yaml, "schema_version: 2\nschema_version: 2\ndesign: {}\n", "malformed_document"),
        (from_json, '{"schema_version":2,"schema_version":2,"design":{}}', "malformed_document"),
        (from_yaml, "schema_version: 99\ndesign: {}\n", "unsupported_schema_version"),
        (from_yaml, "schema_version: 2\ndesign: {type: FutureNode}\n", "malformed_document"),
        (
            from_json,
            to_json(Design(unit="user", anchor=FixedDate("2025-03-01"))).replace(
                '"unit":"user"', '"unit":"user","unexpected":1'
            ),
            "malformed_document",
        ),
    ],
)
def test_yaml_and_json_reject_duplicates_unknown_schema_nodes_and_extra_fields(
    load, document, reason
):
    with pytest.raises(TerrineError) as error:
        load(document)
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == reason


def test_trajectory_and_administrative_cutoff_round_trip_with_canonical_hash():
    design = Design(
        unit="user",
        anchor=EventAnchor("purchase", which="first"),
        shape=CohortSpec(
            origin="purchase",
            trajectory=Trajectory("net_revenue", "7d", periods=3, cumulative=True),
        ),
        administrative_cutoff=datetime(2025, 1, 22, tzinfo=UTC),
    )
    assert from_yaml(to_yaml(design)) == design
    assert from_json(to_json(design)).spec_hash == design.spec_hash


def test_time_to_event_design_round_trips_and_requires_matching_occurrence_anchor():
    from terrine.spec.trajectory import TimeToEvent

    design = Design(
        unit="user",
        anchor=EventAnchor("signup", which="first"),
        shape=TimeToEvent("churn", entry="enrolled", delayed_entry=True),
        administrative_cutoff=datetime(2025, 4, 1, tzinfo=UTC),
    )
    assert from_json(to_json(design)).spec_hash == design.spec_hash


def test_life_table_design_round_trips_and_binds_groups_to_features():
    from terrine.spec.trajectory import LifeTable, TimeToEvent

    design = Design(
        "user",
        EventAnchor("signup", which="first"),
        features={"region": agg("revenue", window=("-7d", "0d"))},
        shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
    )
    assert from_json(to_json(design)).spec_hash == design.spec_hash
    with pytest.raises(TerrineError) as error:
        Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
        )
    assert error.value.code == "terrine.design.invalid"
    assert error.value.context["reason"] == "undeclared_name"
    assert error.value.context["names"] == ("region",)


def test_submicrosecond_datetime_subclasses_are_rejected_at_construction():
    value = pd.Timestamp("2025-01-04T00:00:00.000000001Z")
    for build, field in (
        (
            lambda: Design(
                "user", FixedDate("2025-01-01"), administrative_cutoff=cast(datetime, value)
            ),
            "administrative_cutoff",
        ),
        (lambda: CohortGrouping("1d", bucket_origin=cast(datetime, value)), "bucket_origin"),
    ):
        with pytest.raises(TerrineError) as error:
            build()
        assert error.value.code == "terrine.design.invalid"
        assert (error.value.context["reason"], error.value.context["field"]) == (
            "invalid_instant",
            field,
        )


def test_microsecond_datetime_subclass_is_normalized_for_round_trip():
    value = pd.Timestamp("2025-01-04T00:00:00.123456Z")
    design = Design("user", FixedDate("2025-01-01"), administrative_cutoff=cast(datetime, value))
    assert type(design.administrative_cutoff) is datetime
    assert from_json(to_json(design)) == design
    grouping = CohortGrouping("1d", bucket_origin=cast(datetime, value))
    assert type(grouping.bucket_origin) is datetime
