from datetime import UTC, date, datetime
from typing import cast

import pandas as pd
import pytest

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
        Schedule(every="1w", start="2025-01-06", end="2025-02-03"),
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


def test_schedule_uses_positive_elapsed_cadence_and_exclusive_end():
    schedule = Schedule(every="1w", start="2025-01-06", end="2025-02-03")
    assert schedule.every == 7 * 86400
    assert schedule.end == date(2025, 2, 3)
    with pytest.raises(ValueError):
        Schedule(every="0d", start="2025-01-06", end="2025-02-03")
    with pytest.raises(ValueError):
        Schedule(every="1d", start="2025-01-06", end="2025-01-06")


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

    with pytest.raises(ValueError):
        from_json(to_json(design).replace("123456+00:00", "123456789+00:00"))


def test_fixed_date_normalizes_offset_equivalent_instants_for_canonical_hashes():
    utc = FixedDate(datetime(2025, 3, 1, 12, 34, 56, 123456, tzinfo=UTC))
    offset = FixedDate("2025-03-01T14:34:56.123456000+02:00")
    first = Design(unit="user", anchor=utc)
    second = Design(unit="user", anchor=offset)

    assert utc.date == offset.date
    assert to_json(first) == to_json(second)
    assert first.spec_hash == second.spec_hash


def test_fixed_date_rejects_none_and_naive_instants():
    with pytest.raises((TypeError, ValueError)):
        FixedDate(None)  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError):
        FixedDate(datetime(2025, 3, 1, 12, 34))
    with pytest.raises(ValueError):
        FixedDate("2025-03-01T12:34:56.123456789Z")


def test_anchor_range_requires_ordered_bounds_and_aware_instants():
    with pytest.raises(ValueError):
        AnchorRange()
    with pytest.raises(ValueError):
        AnchorRange(start="2025-04-01", end="2025-03-01")
    with pytest.raises(ValueError):
        AnchorRange(start=datetime(2025, 3, 1))


def test_treatment_known_at_t0_metadata_is_explicit():
    design = Design(
        unit="user",
        anchor=FixedDate("2025-03-01"),
        treatment=Treatment(attribute="assigned", known_at_t0=True),
    )
    loaded = from_yaml(to_yaml(design))
    assert loaded.treatment is not None
    assert loaded.treatment.known_at_t0 is True


def test_yaml_and_json_reject_duplicates_unknown_schema_nodes_and_extra_fields():
    with pytest.raises(ValueError):
        from_yaml("schema_version: 1\nschema_version: 1\ndesign: {}\n")
    with pytest.raises(ValueError):
        from_json('{"schema_version":1,"schema_version":1,"design":{}}')
    with pytest.raises(ValueError):
        from_yaml("schema_version: 99\ndesign: {}\n")
    with pytest.raises(ValueError):
        from_yaml("schema_version: 1\ndesign: {type: FutureNode}\n")
    with pytest.raises(ValueError):
        from_json('{"schema_version":1,"design":{"unit":"u","anchor":{"type":"FutureNode"}}}')
    with pytest.raises(ValueError):
        from_json(
            to_json(Design(unit="user", anchor=FixedDate("2025-03-01"))).replace(
                '"unit":"user"', '"unit":"user","unexpected":1'
            )
        )
    with pytest.raises((TypeError, ValueError)):
        Design(unit="user", anchor=FixedDate("2025-03-01"), features={"bad": lambda: 1})  # ty: ignore[invalid-argument-type]


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
    with pytest.raises(ValueError):
        Design(
            unit="user",
            anchor=Schedule("1d", "2025-03-01", "2025-03-04"),
            shape=TimeToEvent("churn"),
        )


def test_life_table_design_round_trips_and_binds_groups_to_features():
    from terrine.spec.trajectory import LifeTable, TimeToEvent

    design = Design(
        "user",
        EventAnchor("signup", which="first"),
        features={"region": agg("revenue", window=("-7d", "0d"))},
        shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
    )
    assert from_json(to_json(design)).spec_hash == design.spec_hash
    with pytest.raises(ValueError):
        Design(
            "user",
            EventAnchor("signup", which="first"),
            shape=LifeTable(TimeToEvent("churn"), "7d", group_by=("region",)),
        )


def test_life_table_rejects_group_names_that_collide_with_output_columns():
    from terrine.spec.trajectory import LifeTable, TimeToEvent

    with pytest.raises(ValueError):
        LifeTable(TimeToEvent("churn"), "1d", group_by=("time_bin",))


def test_submicrosecond_datetime_subclasses_are_rejected_at_construction():
    value = pd.Timestamp("2025-01-04T00:00:00.000000001Z")
    with pytest.raises(ValueError):
        Design("user", FixedDate("2025-01-01"), administrative_cutoff=cast(datetime, value))
    with pytest.raises(ValueError):
        CohortGrouping("1d", bucket_origin=cast(datetime, value))


def test_microsecond_datetime_subclass_is_normalized_for_round_trip():
    value = pd.Timestamp("2025-01-04T00:00:00.123456Z")
    design = Design("user", FixedDate("2025-01-01"), administrative_cutoff=cast(datetime, value))
    assert type(design.administrative_cutoff) is datetime
    assert from_json(to_json(design)) == design
    grouping = CohortGrouping("1d", bucket_origin=cast(datetime, value))
    assert type(grouping.bucket_origin) is datetime
